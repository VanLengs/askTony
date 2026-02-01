## Context

AskTony 通过 `import-dim-info` 将成员的 `full_name/department` 等信息写入 `gold.dim_member_enrichment`，并通过 `gold.dim_member` 视图对外暴露。我们已将 `active-members/inactive-members` 的口径定义为“员工口径”，即 `full_name` 非空的成员。

但在实际数据中，窗口期有大量作者有提交记录，却未补齐 `full_name`，导致员工统计与导出覆盖率偏低。需要一个定位工具来输出“窗口期活跃但 `full_name` 缺失”的作者清单，指导补全维度。

## Goals / Non-Goals

**Goals:**
- 新增 `asktony analyze missing-fullname-authors` 命令。
- 支持 `--months`（默认 2）、`--top`、`--all`、`--csv`，与现有 analyze 命令交互一致。
- 输出 `member_key/username/email/commit_count/repo_count`，按提交数降序，且仅包含 `full_name` 缺失的作者。

**Non-Goals:**
- 不自动写入/修正维表（仅输出清单）。
- 不引入新依赖或新表结构。

## Decisions

### 1) 作者归一化与数据源

**Decision**：以 `gold.dim_member` 作为作者信息源与全局 identity 映射来源。

**Rationale**：`gold.dim_member` 是 union 维表，包含 members/top_contributors/commits authors，能够覆盖窗口内所有作者（用于对齐 `member_key`、拿到 `username/email`），同时 `full_name` 为空代表尚未通过 `import-dim-info` 补全。

### 2) 过滤逻辑

**Decision**：只输出 `NULLIF(TRIM(full_name),'') IS NULL` 的作者，并按窗口期聚合 `commit_count` 与 `repo_count`（distinct repo_id）。

### 3) 时间窗口与性能

**Decision**：沿用现有分析口径，使用 `commit_month >= since_month` 与 `committed_at >= since_ts` 双条件裁剪。

## Risks / Trade-offs

- [Risk] `gold.dim_member` 包含所有 commit authors，输出可能混入外部贡献者。→ Mitigation：该命令目的就是“找需要补全 full_name 的候选”，由业务方在补全时筛选；同时可按 email 域名/用户名进一步筛选（后续可扩展）。
- [Risk] 同一作者可能存在多个 `member_key`（历史/字段变化）。→ Mitigation：输出 `member_key` + `username/email` 供人工确认；需要时可后续引入更强的 canonical id 映射。

## Migration Plan

1. 添加 `Warehouse.missing_fullname_authors_data` 查询方法与 `analyze` 子命令。
2. 更新 `README.md` 示例。
3. 验证：在本地 DB 上运行命令并导出 CSV，检查结果可用于补全。
