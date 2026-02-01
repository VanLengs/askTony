## Context

AskTony 已具备本地数仓（DuckDB / gold schema）以及现有分析命令（`active-repos`、`member-commits`、`repo-member-commits`、`inactive-members`）。本变更需要新增一个分析命令，用统一口径输出“最近 N 个月有提交（commit_count > 0）的成员”列表，并支持 CSV 导出（仅导出存在 `full_name` 的成员）。

相关现状与可复用能力：
- gold 模型中已有 `gold.fact_commit`（commit 级事实，按 `commit_month` 分区）与 `gold.dim_member`（成员维度，包含 `full_name/department_level*_name`）。
- `gold.bridge_repo_member` 可用于限定“有效成员（members）”，避免把偶发 author 误当作员工。
- CLI 侧已存在 `--months/--top/--all/--csv` 的交互模式与 CSV 写出函数（`src/asktony/commands/analyze.py`）。

## Goals / Non-Goals

**Goals:**
- 新增 `asktony analyze active-members` 子命令。
- 支持 `--months`（默认 2）、`--top`、`--all`、`--csv`，并与现有 analyze 命令保持一致。
- 结果列满足 spec：`member/full_name/department_level2_name/department_level3_name/commit_count(>0)`。
- `--csv` 导出时，只导出 `full_name` 非空/非空白的成员记录；终端输出不强制过滤。

**Non-Goals:**
- 不改动 gold 数据模型结构（不新增事实表/维表），仅基于现有 `gold.fact_commit`/`gold.dim_member`/`gold.bridge_repo_member` 查询。
- 不引入新依赖（保持 Python + DuckDB）。
- 不做跨部门/角色等多维汇总报表（只输出成员明细列表）。

## Decisions

### 1) 成员口径（仅“有效成员”）

**Decision**：统计口径仅包含“有效成员（members）”的提交，即在统计提交时要求 `gold.fact_commit` 与 `gold.bridge_repo_member` 以 `(repo_id, member_key)` 匹配。

**Rationale**：
- `bridge_repo_member` 来源于 `silver.members`（仓库成员列表），更贴近“员工”语义。
- 与现有 `active-repos` 的“有效成员提交”口径一致。

**Alternative considered**：不使用 `bridge_repo_member` 过滤（直接聚合 `gold.fact_commit`）。缺点是可能把外部贡献者/历史 author 混入员工列表。

### 2) 时间窗口过滤（性能与边界）

**Decision**：与现有分析保持一致，同时使用：
- `commit_month >= since_month`（用于分区裁剪）
- `committed_at >= since_ts`（用于天级边界准确）

其中 `since_ts = now_utc - 30*months days`，`since_month = STRFTIME(since_ts, '%Y-%m')`。

### 3) active 的计算方式（聚合 + HAVING）

**Decision**：按成员聚合提交数，并通过 `HAVING COUNT(*) > 0` 过滤：

```sql
SELECT
  COALESCE(NULLIF(TRIM(m.username), ''), NULLIF(TRIM(m.email), ''), c.member_key) AS member,
  COALESCE(m.full_name, '') AS full_name,
  COALESCE(m.department_level2_name, '') AS department_level2_name,
  COALESCE(m.department_level3_name, '') AS department_level3_name,
  COUNT(*)::BIGINT AS commit_count
FROM gold.fact_commit c
JOIN gold.bridge_repo_member br
  ON br.repo_id = c.repo_id
 AND br.member_key = c.member_key
LEFT JOIN gold.dim_member m ON m.member_key = c.member_key
WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
GROUP BY 1,2,3,4
HAVING COUNT(*) > 0
ORDER BY commit_count DESC
-- LIMIT ? (unless --all)
```

**Rationale**：聚合计算可直接得到 `commit_count`，并且排序可用于 Top N 输出。

### 4) `--csv` 的 `full_name` 过滤位置

**Decision**：在 CLI 导出路径中做过滤（Python 层），不改变默认查询口径。

**Rationale**：
- Spec 只要求“导出 CSV 时过滤”，并未要求终端展示也过滤。
- CLI 层过滤便于复用现有 `_write_csv`，并保持终端输出与查询结果一致。

### 5) 代码落点

**Decision**：
- 新增 `Warehouse.active_members_data(months: int, top: int|None)` 与 `Warehouse.active_members(...)`（返回 rich table），对齐现有 analyze 代码风格。
- 在 `src/asktony/commands/analyze.py` 中新增命令处理，复用现有参数与 `_write_csv`。
- `README.md` 补充使用示例。

## Risks / Trade-offs

- [Risk] `bridge_repo_member` 基线依赖仓库成员列表，未加入任何 repo 的员工无法被统计为 active。→ Mitigation：在命令/文档中说明“基于仓库成员口径”；如需 HR 口径可另引入员工名单维表。
- [Risk] `months` 使用 30*months 天近似，与自然月口径略有偏差。→ Mitigation：保持与现有 analyze 命令一致；如业务需要再增加自然月窗口参数（非本次范围）。
- [Risk] `full_name` 未补齐会导致 CSV 导出结果为空或偏少。→ Mitigation：README 明确需先通过 `import-dim-info` 补全姓名字段。

## Migration Plan

1. 实现 `Warehouse` 查询与 `analyze` 子命令（不改动数据模型、无需迁移）。
2. 更新 `README.md` 使用示例。
3. 回归验证：本地 DB 上确认 active 列表与 CSV 导出工作正常。
4. 回滚：删除新增子命令与相关 `Warehouse` 方法即可。

## Open Questions

- 是否需要输出额外字段（如 `member_key` 或 `changed_lines`）以便交叉核对？当前按需求仅输出必要字段。
