## Context

AskTony 已具备本地数仓（DuckDB / gold schema）以及现有分析命令（`active-repos`、`member-commits`、`repo-member-commits`），并且通过 `import-dim-info` 允许补全 `dim_member` 的 `full_name/department` 等字段。

本变更需要在既有 gold 模型之上新增一个分析命令，统一口径输出“最近 N 个月提交次数为 0”的成员列表，并支持 CSV 导出（仅导出存在 `full_name` 的成员）。

相关现状与可复用能力：
- gold 模型中已有 `gold.fact_commit`（commit 级事实，按 `commit_month` 分区）与 `gold.dim_member`（成员维度，包含 `full_name/department_level*_name`）。
- `gold.bridge_repo_member` 可用于限定“有效成员（members）”，现有 `active-repos` 与可视化报告中的部分统计也使用该桥表来过滤。
- CLI 侧已存在 `--months/--top/--all/--csv` 的通用交互模式与 CSV 写出函数（`src/asktony/commands/analyze.py`）。

## Goals / Non-Goals

**Goals:**
- 新增 `asktony analyze inactive-members` 子命令。
- 支持 `--months`（默认 2）、`--top`、`--all`、`--csv`，并与现有 analyze 命令保持一致。
- 结果列满足 spec：`member/full_name/department_level2_name/department_level3_name/commit_count(=0)`。
- `--csv` 导出时，只导出 `full_name` 非空/非空白的成员记录；终端输出不强制过滤。

**Non-Goals:**
- 不改动 gold 数据模型结构（不新增事实表/维表），仅基于现有 `gold.fact_commit`/`gold.dim_member`/`gold.bridge_repo_member` 查询。
- 不新增“部门维度统计/分组汇总”等更复杂报表。
- 不引入新依赖（保持 Python + DuckDB）。

## Decisions

### 1) 统计口径与成员基线

**Decision**：成员基线使用 `gold.bridge_repo_member` 的去重成员集合（`member_key`），以代表“有效成员（members）”；再 LEFT JOIN `gold.dim_member` 获取 `full_name/department` 等补充字段。

**Rationale**：
- `gold.bridge_repo_member` 来源于 `silver.members`（仓库成员列表），能避免把“仅在 commits/top_contributors 出现过的作者”误当作员工。
- `gold.dim_member` 的 `full_name/department` 依赖 `import-dim-info` 补齐；基线来自 `bridge_repo_member` 仍能在未补齐时正常输出（字段为空）。

**Alternative considered**：直接以 `gold.dim_member` 为基线（与 `visualize.py` 的 no_contrib 逻辑类似）。缺点是可能混入非成员作者；优点是覆盖面更大。本命令面向“员工”场景，优先选择更“干净”的成员集合。

### 2) 时间窗口过滤（性能与边界）

**Decision**：与现有分析保持一致，同时使用：
- `commit_month >= since_month`（用于分区裁剪）
- `committed_at >= since_ts`（用于天级边界准确）

其中 `since_ts = now_utc - 30*months days`，`since_month = STRFTIME(since_ts, '%Y-%m')`。

**Rationale**：`commit_month` 条件提升扫描性能，`committed_at` 条件避免“月边界”误差。

### 3) inactive 的计算方式（集合差）

**Decision**：先构建窗口期活跃成员集合 `active(member_key)`，再以成员基线做差集得到 inactive：

```sql
WITH active AS (
  SELECT DISTINCT c.member_key
  FROM gold.fact_commit c
  JOIN gold.bridge_repo_member br
    ON br.repo_id = c.repo_id
   AND br.member_key = c.member_key
  WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
),
members AS (
  SELECT DISTINCT member_key
  FROM gold.bridge_repo_member
  WHERE member_key IS NOT NULL AND member_key <> ''
)
SELECT
  COALESCE(NULLIF(TRIM(m.username), ''), NULLIF(TRIM(m.email), ''), b.member_key) AS member,
  COALESCE(m.full_name, '') AS full_name,
  COALESCE(m.department_level2_name, '') AS department_level2_name,
  COALESCE(m.department_level3_name, '') AS department_level3_name,
  0::BIGINT AS commit_count
FROM members b
LEFT JOIN active a ON a.member_key = b.member_key
LEFT JOIN gold.dim_member m ON m.member_key = b.member_key
WHERE a.member_key IS NULL
ORDER BY department_level2_name, department_level3_name, full_name, member
-- LIMIT ? (unless --all)
```

**Rationale**：差集思路可复用、易于验证（与 `visualize.py` 中“no contribution list”一致），且 inactive 的 `commit_count` 恒为 0，避免额外聚合。

### 4) `--csv` 的 `full_name` 过滤位置

**Decision**：在 CLI 导出路径中做过滤（Python 层），而不是改变默认查询口径。

**Rationale**：
- Spec 只要求“导出 CSV 时过滤”，并未要求终端展示也过滤。
- CLI 层过滤可以复用已有 `_write_csv`，并保持 `Warehouse` 方法输出与终端显示一致。

实现细节建议：
- Warehouse 方法返回 `columns, rows`（包含 `full_name` 列）。
- 当 `--csv` 指定时，对 `rows` 进行 `full_name.strip() != ''` 过滤后写出。

### 5) 代码落点

**Decision**：
- 新增 `Warehouse.inactive_members_data(months: int, top: int|None)` 与 `Warehouse.inactive_members(...)`（返回 rich table），对齐现有 analyze 代码风格。
- 在 `src/asktony/commands/analyze.py` 中新增命令处理，复用现有参数与 `_write_csv`。
- `README.md` 补充使用示例（与现有 analyze 命令列表一致）。

## Risks / Trade-offs

- [Risk] `bridge_repo_member` 的成员集合可能与“HR 员工名单”不完全一致（例如未加入任何 repo 的员工不会出现在结果中）。→ Mitigation：在 README/命令 help 中说明“基于仓库成员维度”；后续如需 HR 口径，可扩展为导入员工名单维表或提供 `--baseline dim_member` 选项。
- [Risk] `months` 采用 30*months 天近似，跨月边界可能与自然月口径略有偏差。→ Mitigation：保持与现有 analyze 命令一致；如业务需要可再引入“自然月窗口”选项（非本次范围）。
- [Risk] `full_name` 依赖 `import-dim-info` 补全，未补全时 CSV 可能为空。→ Mitigation：导出时打印 rows 数量，并在 README 提示先补全维度信息。

## Migration Plan

1. 实现 `Warehouse` 查询与 `analyze` 子命令（不改动数据模型、无需迁移）。
2. 更新 `README.md` 使用示例。
3. 回归验证：在有 commits 的环境验证 inactive 列表；在无 commits 的环境验证命令仍可运行并输出合理结果（可为空/全量）。
4. 回滚：删除新增子命令与相关 `Warehouse` 方法即可（无数据迁移）。

## Open Questions

- “员工”基线是否需要可配置：仅 repo members（当前设计） vs `dim_member` 全集？
- CSV 过滤规则中对 `full_name` 的“空白”定义：目前按 `strip()` 判空；是否需要同时排除 `NULL` 与 `'未知'` 等占位值？
