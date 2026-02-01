## Why

在日常管理与协作中，除了识别“无提交成员”，也需要快速识别“最近两个月有提交（> 0 commit）成员”，用于衡量团队参与度、做活跃人员名单同步，以及支持按姓名/部门维度的导出分析。

## What Changes

- 新增分析命令：`asktony analyze active-members`（窗口默认近 2 个月，支持 `--months` 调整统计窗口，沿用现有 `--top/--all/--csv` 交互习惯）。
- 输出字段包含：`member`、`full_name`、`department_level2_name`、`department_level3_name`、`commit_count`（>0）。
- 支持导出 CSV（`--csv <path>`），并且**只导出** `full_name` 非空（含非空白）的成员记录。

## Capabilities

### New Capabilities

- `analyze-active-members`: 基于本地数仓（DuckDB/gold）统计最近 N 个月提交数 `> 0` 的成员列表，支持终端展示与 CSV 导出（导出仅包含 `full_name` 非空成员）。

### Modified Capabilities

<!-- None -->

## Impact

- CLI：`src/asktony/commands/analyze.py` 增加 `active-members` 子命令，复用现有 CSV 导出能力。
- 数据依赖：依赖 `gold.fact_commit` 与 `gold.dim_member`（`full_name/department` 由 `import-dim-info` 补齐；导出 CSV 会过滤掉未补齐 `full_name` 的记录）。
- 文档：`README.md` 的“常用分析与导出”补充该命令示例。
