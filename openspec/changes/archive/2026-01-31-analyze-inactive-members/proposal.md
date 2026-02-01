## Why

目前 `asktony analyze` 已能查看活跃仓库/成员提交等，但缺少一个“快速识别近两个月无提交（0 commit）成员”的标准口径，导致需要手工筛选、难以复用与导出给业务方（HR/管理者）做跟进。

## What Changes

- 新增分析命令：`asktony analyze inactive-members`（窗口默认近 2 个月，可沿用现有 `--months`/`--top`/`--all` 交互习惯）。
- 输出字段包含：`member`、`full_name`、`department_*_name`（部门相关字段），以及用于核对的窗口信息（如 `months` / `commit_count=0`）。
- 支持导出 CSV（沿用现有 `--csv <path>` 方式）；并且**只导出**存在 `full_name` 的成员记录（`full_name` 为空/缺失的行不出现在 CSV 中）。

## Capabilities

### New Capabilities

- `analyze-inactive-members`: 在本地数仓（DuckDB/Gold 模型）基础上，统计近 N 个月提交数为 0 的成员，并支持 CSV 导出（仅包含有 `full_name` 的成员）。

### Modified Capabilities

<!-- None -->

## Impact

- CLI：`src/asktony/commands/analyze.py` 增加子命令与参数解析，复用现有 `--csv/--top/--all/--months` 行为。
- 数据依赖：依赖已构建的提交事实与成员维表（需要先完成 `ingest` + `model build`，并通过 `import-dim-info` 补全 `full_name/department` 才能导出更多维度信息）。
- 文档：`README.md` 的“常用分析与导出”需要补充该命令示例。
