## Why

目前 `active-members/inactive-members` 已采用“员工口径”（`full_name` 非空），但实际数据中仍存在大量窗口期有提交、却未补全 `full_name` 的作者，导致员工统计覆盖率偏低且难以定位需要补全的名单。

## What Changes

- 新增分析命令：`asktony analyze missing-fullname-authors`（窗口默认近 2 个月，支持 `--months/--top/--all/--csv`）。
- 统计并列出最近 N 个月内有提交记录、但成员维度 `full_name` 为空/空白的作者（用于补齐 `dim_member_enrichment.full_name`）。
- 支持导出 CSV，便于直接作为补全清单进行处理。

## Capabilities

### New Capabilities

- `analyze-missing-fullname-authors`: 基于本地数仓，输出“窗口期有提交但 `full_name` 缺失”的作者清单，并支持 CSV 导出。

### Modified Capabilities

<!-- None -->

## Impact

- CLI：`src/asktony/commands/analyze.py` 增加新子命令，复用现有 CSV 输出能力。
- Warehouse：`src/asktony/warehouse.py` 增加查询方法，基于 `gold.fact_commit` + `gold.dim_member`（作者 union 维表）统计缺失 `full_name` 的作者。
- 文档：`README.md` 增加该命令使用示例，并说明其用途（为 `import-dim-info` 补全提供线索）。
