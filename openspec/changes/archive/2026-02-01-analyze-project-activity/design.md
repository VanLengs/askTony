## Context

AskTony 当前的分析能力主要围绕 `repo / member / line_manager`，数据基础是：
- `gold.fact_commit`：commit 事实（含 `repo_id / committed_at / commit_month / author_username / author_email / member_key`）
- `gold.dim_member_enrichment`：员工主数据（来自 `import-dim-info`，以 `member_key` 为主键，但包含 HR 字段）
- 现有 active/inactive 的“员工口径”已经统一按 `employee_id`（若空则退化为 `member_key`）去重。

项目分析需要把“资源投入/产出”对齐到项目：
- 一个项目固定操作一个或多个 repo
- repo 允许多项目归属（同一时间段可按 weight 分摊）
- 员工以 `employee_id` 作为项目成员主键；员工可同时参与多个项目并有 allocation（FTE）
- 项目存在生命周期（交付项目/研发项目），但 MVP 先按“月”粒度完成资源与产出归属与指标。

约束：
- `project_id` 用项目名称的拼音（系统自带转写）生成；需可复现、可稳定
- 项目相关映射均需支持 `start_at / end_at` 时间区间（允许人员进出与 repo 归属变化）

## Goals / Non-Goals

**Goals:**
- 引入项目维度（`gold.dim_project`）与两类桥表（项目-仓库、项目-人员-角色），并提供 CSV 导入命令（带 dry-run 校验）。
- 在 gold 层建立项目月度聚合事实：
  - 将 commits 通过 repo->project 映射（含 weight）归属到项目
  - 将提交作者对齐到员工 `employee_id`（只统计已导入员工）
  - 提供 `project-month` 与 `project-person-month` 的基础指标，支持后续分析与可视化
- 新增 `asktony analyze project-activity`：按月窗口输出项目横向对比指标（表格/CSV）。

**Non-Goals:**
- 不在 MVP 中实现完整项目生命周期阶段建模（交付/研发阶段的状态机与阶段内 KPI），仅预留字段与扩展点。
- 不在 MVP 中强依赖外部系统（Jira/禅道/飞书）API 抓取；先以 CSV 作为输入。
- 不在 MVP 中做“按 PR/Issue/CI” 的质量指标（后续可拓展）。

## Decisions

### 1) `project_id` 生成规则：系统自带转写 + 稳定规范化

**Decision**
- 使用 macOS Foundation 的 transform（`Any-Latin` + `Latin-ASCII`）把项目名转为拼音，再做规范化：
  - 小写
  - 空白归一化为 `_`
  - 移除非 `[a-z0-9_]` 字符
  - 连续 `_` 折叠
  - 为空时报错

**Rationale**
- 不引入 `pypinyin` 依赖，保持环境简单；与既有部门拼音脚本一致。

**Alternative**
- 使用 `pypinyin`：跨平台更强，但引入依赖与版本差异。

### 2) 项目成员主键统一用 `employee_id`

**Decision**
- 项目成员桥表 `bridge_project_person_role` 使用 `employee_id` 作为 `person_id`（强制必填）。
- 项目分析的“员工口径”统一按 `employee_id` 聚合去重。

**Rationale**
- 避免同一员工多个账号/dummy key 造成重复统计或 active/inactive 冲突。

**Trade-off**
- 依赖 HR 数据完整性：缺失 `employee_id` 的人员不能作为项目成员导入。

### 3) repo↔project 多归属 + weight 分摊（按月）

**Decision**
- `bridge_project_repo` 支持：
  - 同一 repo 在同一时间段可以映射到多个 project（多归属）
  - 每条映射有 `weight`（0~1），用于将 repo 的提交分摊到 project
  - `start_at/end_at` 用于描述归属变化

**Rationale**
- repo 共用是常态；用 weight 能近似反映项目间资源/产出切分。

**校验规则（import 阶段）**
- `weight` 必须在 (0,1]（或允许 0 但无意义，建议禁止）
- 对同一 `repo_id` + 同一月的所有映射，`SUM(weight)` 建议接近 1（不强制阻断，先作为 warning 统计）
- 同一 `project_id+repo_id` 的区间允许多段，但不能重叠

### 4) allocation（FTE）用于“加权人力”和“加权产出”

**Decision**
- `bridge_project_person_role` 的 `allocation` 使用 0~1（FTE），缺省视为 1。
- 项目指标同时输出：
  - headcount（去重人数）
  - fte_sum（allocation 求和）
  - commits_per_fte（加权产出强度）

**Rationale**
- 解决一个人多项目贡献时的人力占用归属与强度对比。

### 5) 项目事实表：从 commit 事实推导（month grain）

**Decision**
- 新增：
  - `gold.dim_project`
  - `gold.bridge_project_repo`
  - `gold.bridge_project_person_role`
  - `gold.fact_project_member_month`（project_id, employee_id, commit_month, weighted_commit_count, weighted_changed_lines, repo_cnt, etc.）
  - `gold.fact_project_month`（project_id, commit_month, aggregated metrics）

**Commit -> employee_id 对齐**
- 复用现有对齐策略：以员工维度（导入员工）为全集，通过 `member_key` 优先、再用 `author_email/author_username` fallback 对齐到员工，再映射到 `employee_id`。
- 仅统计能对齐到员工且存在于项目成员表（employee_id）的提交（MVP 口径）。

**Why**
- 保持口径一致：项目绩效关注“投入的研发资源（项目成员）”与其产出。

### 6) 命令划分

- `asktony import-project-info`：CSV 导入 + 校验 + upsert（项目与桥表）
- `asktony model build`：在 build 阶段创建/刷新项目相关 gold 表与视图（或新增 `model-project-facts` 作为子步骤）
- `asktony analyze project-activity`：项目月度窗口的横向对比（支持 `--csv`）

## Risks / Trade-offs

- [项目名拼音冲突] → 允许在 CSV 中显式提供 `project_id` 覆盖自动生成；或提供冲突检测并建议手工后缀。
- [repo 多归属权重不规范导致指标失真] → import 时输出 warning 统计（repo-month 权重和偏离 1 的清单）。
- [成员 allocation 不维护/不准确] → 指标同时提供 headcount 与 fte_sum，避免单一口径误导。
- [提交作者无法对齐 employee_id] → 提供 “未对齐提交” 的 debug/报表（后续可扩展命令），用于推动 HR/账号补全。
- [阶段生命周期差异（交付 vs 研发）] → MVP 先按月统一口径；后续在 specs 中扩展“按阶段”指标与可视化。

