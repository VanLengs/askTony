# AskTony 研发效能：评分与反刷规则说明

本文档解释 AskTony 当前实现的三套规则：

1) **员工个人评分**（`asktony analyze active-employee-score`）  
2) **Line manager 评分**（`asktony analyze line-manager-dev-activity`）  
3) **反刷 commit 启发式**（`asktony analyze suspicious-committers`）  

目标：让业务/管理者能读懂“为什么某人/某团队分高或分低”，并能复盘每个维度的含义与边界。

---

## 1. 通用约定

### 1.1 时间窗口（months）

- 大多数分析命令采用 `--months N`，窗口起点约为：`now_utc - 30*N 天`
- SQL 过滤通常使用两层条件以提高性能与口径一致性：
  - `commit_month >= since_month`
  - `committed_at >= since_date`

> 说明：窗口是“最近 N 个月（近似 30*N 天）”，不是自然月滚动口径。

### 1.2 身份对齐（employee/member）

员工维度来自 HR 全名单（`gold.dim_member_enrichment`，要求 `full_name` 非空）。

commit 作者与员工的匹配采用“尽力而为”的三段式：

1) 直接 `member_key` 命中  
2) email（忽略大小写）命中  
3) username（忽略大小写）命中  

目的是提高员工与 commit 的对齐率（尤其是仓库成员列表不全或作者信息缺失时）。

### 1.3 排除 merge commit

所有“产出/反刷”统计默认**排除 merge commit**（`silver.commit_stats.is_merge`），避免 merge 造成无意义的数量膨胀。

### 1.4 分位得分（percent_rank）

很多 `score_xxx` 是用 DuckDB 的 `percent_rank()` 在同一窗口的群体内做分位归一化（0~100）：

- 值越大 → 分位越高 → 分数越高  
- 对于“风险/坏事”（例如单仓库集中度、低强度），会用 `1 - percent_rank()` 做反向得分

特点：

- 分数是**相对**群体的（同公司/同窗口的相对排名）
- 不是绝对 KPI（例如 90 分不等于“绝对好”，而是“比大多数人更好”）

### 1.5 角色差异与“加权变更量”

不同开发角色的“代码表达形式”差异很大：

- 数据开发（SQL）同等业务逻辑的 `changed_lines` 往往明显小于 Java/JS
- 算法开发的逻辑复杂度可能更高，同样行数的价值更大

为减少“跨角色直接比行数”的结构性偏差，AskTony 引入 **按角色加权的变更量**：

`weighted_changed_lines = changed_lines * role_weight`

其中 `role_weight` 由配置决定（见 `src/asktony/dim_admin.py` 的 `ROLE_CHANGE_WEIGHTS`）：

- 数据开发：1.8
- 算法开发：1.5
- 全栈开发：1.2
- Java 后台开发：1.1
- 其他角色：1.0

在员工/line manager 的“变更相关 score”（总量/强度/人均等）中会优先使用加权口径，以提升跨角色可比性；原始未加权的 `changed_lines` 仍会保留在导出结果中便于核对。

---

## 2. 员工个人评分（active-employee-score）

命令：

- `asktony analyze active-employee-score --months N --all --csv ...`

输出常用字段：

- `commit_count`：窗口内非 merge commit 数
- `repo_count`：窗口内参与的仓库数（distinct repo）
- `total_changed_lines`：窗口内 `changed_lines` 总和
- `total_weighted_changed_lines`：窗口内 `weighted_changed_lines` 总和（用于打分）
- `score_lines_total`：`ln(1 + total_changed_lines)` 的分位得分
- `score_repo_diversity`：`repo_count` 的分位得分
- `score_total`：最终总分（见下）

> 重要：`suspicious_score / score_integrity` 仍会输出用于统计/可视化，但 **不再参与员工 `score_total`**（避免反欺诈误伤排名）。

### 2.0 其他 score_xxx 的定位（诊断/雷达图维度）

`active-employee-score` 仍然会输出一批 `score_xxx`（例如 `score_active/score_lines_p50/score_message_quality/...`）。

这些字段主要用于：

- 雷达图的各维度展示与改进建议（“哪里短板”）
- 辅助解释为什么总分高/低（尤其是当 `score_total` 封顶到 100 时）

但它们**不一定都参与**当前的 `score_total`（总分公式以 2.5 为准）。

### 2.1 总分设计目标

你希望“重点关注一段时间内的总贡献量”，但同时：

- 不希望“只有寥寥几次提交”的人因为一次大提交而排名过高
- 想“稍微奖励”维护多个仓库的人，但不能过多

因此采用：

- **主指标**：总贡献量（`score_lines_total`）
- **门槛系数**：提交频率不足时更重惩罚；达到门槛后按原规则分段奖励
- **轻量奖励**：多仓维护加一点分，但封顶、且权重很小

### 2.2 “不饱和”门限（Under-saturated）

用于门槛系数的“不饱和”判断（业务口径）：

- **每月 6 次提交**视为一个基本“在循环中（in the loop）”信号  
- 对于 `--months N`，门限为：

`unsat_commit_min = 6 * N`

例如 `--months 2` 时：`unsat_commit_min = 12`

### 2.3 提交门槛系数（commit_gate）

为了对“不饱和”加大惩罚，同时保留原规则的分段奖励，门槛系数采用分段函数：

1) **不饱和区间（惩罚更重）**  
当 `commit_count < unsat_commit_min`：

`commit_gate = 0.5 + 0.3 * (commit_count / unsat_commit_min)`

含义：

- commit_count=0 时为 0.5（即使产出很大，也会被明显打折）
- commit_count 接近门限时，逐渐回到 0.8

2) **达到门限后（原规则分段奖励）**  
当 `commit_count >= unsat_commit_min`：

`commit_gate = 0.8 + 0.2 * min(1, commit_count / 20)`

含义：

- 达标后基础系数至少 0.8
- commit_count 达到 20 及以上时，系数封顶到 1.0（不再继续奖励“更多提交次数”）

### 2.4 多仓维护轻量奖励（repo_bonus）

为了“稍微奖励多仓维护，但不能过多”：

`repo_bonus = 0.05 * min(score_repo_diversity, 70)`

含义：

- 只使用分位分 `score_repo_diversity`，并在 70 分处封顶
- 最多加 `0.05 * 70 = 3.5` 分（非常轻量）

### 2.5 员工总分（score_total）

最终总分：

`score_total = min(100, score_lines_total * commit_gate + repo_bonus)`

其中 `score_lines_total` 使用的是 **加权后的** `total_weighted_changed_lines` 分位得分。

> 由于 `score_lines_total` 是分位分（0~100），当某些人处在极高分位且门槛也满额时，可能会触发 100 封顶。

---

## 3. Line manager 评分（line-manager-dev-activity）

命令：

- `asktony analyze line-manager-dev-activity --months N --all --csv ...`

人群范围：

- 仅统计“开发角色”员工（固定枚举）：
  - Java 后台开发、Web 前端开发、终端开发、算法开发、数据开发、全栈开发

输出常用字段（部分）：

- `dev_total/dev_active/dev_inactive`：该经理名下开发员工总数/活跃/不活跃
- `commits_total`：团队窗口内非 merge commit 总数
- `commits_per_dev`：`commits_total / dev_total`
- `changed_lines_total`：团队窗口内 `changed_lines` 总和
- `changed_lines_total_weighted`：团队窗口内 `weighted_changed_lines` 总和（用于打分）
- `repo_count`：团队窗口内涉及的仓库数（distinct repo）
- `score_lines_total`：团队 `ln(1 + changed_lines_total)` 的分位得分
- `score_total`：最终总分（见下）
- `score_integrity`、`suspicious_*`：反刷统计（**只展示，不进总分**）

### 3.0 Radar/诊断维度（score_xxx 如何理解）

Line manager 表里会输出多维 `score_xxx`，用于雷达图横向对比与改进建议（即使不进入 `score_total` 也会保留）：

- `score_active`：`active_pct` 的分位得分（团队活跃开发占比越高越好）
- `score_commits_p50`：活跃开发者 `commit_count` 的 P50 分位得分（“典型强度”）
- `score_commits_per_dev`：`ln(1 + commits_per_dev)` 的分位得分（人均产出）
- `score_lines_p50`：活跃开发者 `changed_lines_total` 的 P50 分位得分（典型变更强度）
- `score_lines_per_dev`：`ln(1 + lines_per_dev)` 的分位得分（人均变更）
- `score_concentration`：`1 - percent_rank(top1_commit_share_pct)`（依赖单核越少越好）
- `score_after_hours`：`after_hours_commit_share_pct` 的分位得分（“奋斗者文化”维度）
- `score_role_cover`：开发角色覆盖数的分位得分（角色越多越好）
- `score_dept_focus`：`1 - percent_rank(department_level2_cnt)`（组织越聚焦越好）
- `score_integrity`：团队“刷量风险”的反向分位得分（只展示，不进总分）

### 3.1 评分口径说明

Line manager 的总分同样以“总贡献量”为主，但避免：

- 团队只靠少量提交堆出大变更而“看起来很强”
- 过度奖励“维护很多仓库”

因此采用与员工类似的结构：

- 主指标：`score_lines_total`（团队总变更量的分位）
- 门槛系数：用 `commits_per_dev` 判断团队是否“参与度不饱和”
- 轻量奖励：`repo_count` 分位的轻量加分（封顶）

### 3.2 团队“不饱和”门限

团队按“人均提交次数”判断不饱和：

`unsat_commits_per_dev_min = 6 * N`

例如 `--months 2` 时门限为 12，含义是“2 个月人均提交 12 次（约每月 6 次）”。

### 3.3 团队门槛系数（team_commit_gate）

分段函数：

1) 不饱和区间（惩罚更重）  
当 `commits_per_dev < unsat_commits_per_dev_min`：

`team_commit_gate = 0.5 + 0.3 * (commits_per_dev / unsat_commits_per_dev_min)`

2) 达标后（原规则分段奖励）  
当 `commits_per_dev >= unsat_commits_per_dev_min`：

`team_commit_gate = 0.8 + 0.2 * min(1, commits_per_dev / 10)`

### 3.4 多仓维护轻量奖励（repo_bonus）

- 先算 `repo_count` 的分位分：`score_repo_count = 100 * percent_rank(repo_count)`
- 再封顶并加权：

`repo_bonus = 0.03 * min(score_repo_count, 70)`

最多加 `0.03 * 70 = 2.1` 分。

### 3.5 Line manager 总分（score_total）

`score_total = min(100, score_lines_total * team_commit_gate + repo_bonus)`

> `score_total_base` 目前保留为兼容/对照字段，便于后续回溯历史版本口径；以 `score_total` 为准。

---

## 4. 反刷 commit 启发式（suspicious-committers）

命令：

- `asktony analyze suspicious-committers --months N --all --csv ...`

含义：

- `score_total`：**越高越可疑**（0~100），用于“反刷”风险识别
- `tags`：触发了哪些可疑模式（便于解释/复盘）
- `under_saturated_flag`：是否“不饱和”（只标注，不影响可疑分）

> 重要：反刷分 **不参与** 员工/manager 的 `score_total`（只做统计和展示维度）。

### 4.1 输入数据与预处理

基于窗口内非 merge commit，构造每个员工的指标：

- 变更规模：
  - `p0_zero`：`changed_lines = 0` 占比
  - `p2_tiny`：`changed_lines <= 2` 占比
  - `p10_small`：`changed_lines <= 10` 占比（“微提交”分层，减少误伤）
  - `changed_lines_per_commit`：`sum(changed_lines)/count(commits)`
- burst / 高频：
  - `max_commits_10m`：任意 10 分钟 bucket 的最大 commit 数
  - `max_commits_1h`：任意 1 小时 bucket 的最大 commit 数
  - `median_inter_commit_seconds`：相邻提交间隔（秒）的中位数
- 新增≈删除对冲：
  - `p_balance_high`：满足下述条件的占比  
    - `changed_lines >= 50`
    - additions 与 deletions 非常接近（近似“新增删除对冲”）
- 单仓库集中：
  - `top1_repo_share`：单一 repo 的最大 commit 占比
- 提交信息质量：
  - `message_unique_ratio`：commit message 去重占比
  - `top1_message_share`：最常见 message 的占比
  - `short_message_ratio`：短 message（长度<=8）占比
  - `generic_message_ratio`：泛化/模板化前缀（如 fix/update/test/wip/tmp/merge/refactor）占比

### 4.2 “核心仓”与单仓库规则降权（减少职责型误伤）

为了避免“主仓职责型提交”被 `single_repo_grind` 误伤：

1) 先对每个 repo 统计窗口内：
   - `repo_person_cnt`：活跃贡献人数
   - `repo_commit_total`：commit 总数
2) 取两个阈值（P75）：
   - `repo_people_p75`
   - `repo_commits_p75`
3) 若某人的 top1 repo 满足：
   - `repo_person_cnt >= repo_people_p75` **或**
   - `repo_commit_total >= repo_commits_p75`

则认为其 top1 repo 为“核心仓”（`top1_repo_is_core=1`）：

- 单仓可疑子分会被折减（乘 0.6）
- 也不轻易打 `single_repo_grind` tag

### 4.3 可疑子分（score_xxx）

所有可疑子分都是分位得分（0~100），分高表示“更可疑”：

- `score_tiny`：`p2_tiny` 分位
- `score_small`：`p10_small` 分位
- `score_zero`：`p0_zero` 分位
- `score_burst`：`max_commits_10m` 分位
- `score_inter_commit`：`median_inter_commit_seconds` 的反向分位（越短越可疑）
- `score_balance`：`p_balance_high` 分位
- `score_message`：`message_unique_ratio` 的反向分位（越不唯一越可疑）
- `score_single_repo`：`top1_repo_share` 分位（若 top1 为核心仓则整体降权）
- `score_low_intensity`：`changed_lines_per_commit` 的反向分位（越低越可疑）

### 4.4 可疑总分（score_total_raw / score_total）

加权合成（权重之和为 1.0）：

- 0.18 * `score_tiny`
- 0.06 * `score_small`
- 0.10 * `score_zero`
- 0.12 * `score_burst`
- 0.06 * `score_inter_commit`
- 0.14 * `score_balance`
- 0.10 * `score_message`
- 0.10 * `score_single_repo`
- 0.14 * `score_low_intensity`

得到 `score_total_raw` 后，再做“保护/降权”（避免误伤真实高产/高协作）：

- `commit_count < 20`：乘 0.5（样本太小，不强判）
- 命中保护（满足任一）则乘 0.6：
  - `prod_rank >= 0.80`（总变更量分位高）
  - `intensity_rank >= 0.80`（单次强度分位高）
  - `repo_rank >= 0.80`（多仓协作分位高）
  - `msg_quality_rank >= 0.80`（message 唯一度分位高）
- 否则不降权

最终得到 `score_total`。

### 4.5 tags 触发规则（可解释输出）

为了“解释为什么可疑”，会输出 `tags`（以 `;` 分隔）：

- `zero_change_ratio_high`：`p0_zero >= P80`
- `tiny_commit_ratio_high`：`p2_tiny >= P80`
- `burst_commits`：`max_commits_10m >= P80`
- `add_del_flip`：`commit_count >= 20` 且 `p_balance_high >= P80`
- `single_repo_grind`：`top1_repo_is_core=0` 且 `top1_repo_share >= P80`
- `under_saturated`：开发角色且 `commit_count < 6*N`（每月 6 次门限）
- `template_messages`：同时满足
  - `msg_total >= 20`
  - `message_unique_ratio <= min(P15, 0.20)`
  - `top1_message_share >= 0.40`
  - 且（`generic_message_ratio >= 0.30` 或 `short_message_ratio >= 0.30`）
- `protected_high_output`：命中保护条件
- `low_sample_size`：`commit_count < 20`

---

## 5. 常见解读方式（业务侧）

### 5.1 员工分数

- 想看“谁贡献大”：看 `total_changed_lines`、`score_lines_total`、`score_total`
- 想避免“少量大提交躺赢”：看 `commit_count` 与 `commit_gate`（隐含在总分中）
- 想识别“多仓维护”：看 `repo_count`、`score_repo_diversity`

### 5.2 Line manager 分数

- 主看“团队交付量”：`changed_lines_total`、`score_lines_total`
- 看“参与度是否充足”：`commits_per_dev` 与门槛系数（隐含在总分中）
- 看“团队是否广泛维护”：`repo_count`（轻量加分，不是主导因子）
- 反刷只作侧面风险提示：`suspicious_dev_pct / suspicious_score_avg / score_integrity`

### 5.3 反刷分数

`suspicious-committers` 的 `score_total` 只表达“模式可疑”，建议作为：

- **人工核查优先级**
- **数据质量/行为模式观察**

而不是直接当作“绩效扣分”。
