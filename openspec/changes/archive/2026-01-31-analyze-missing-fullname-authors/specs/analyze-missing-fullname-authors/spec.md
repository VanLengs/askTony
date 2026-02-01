## ADDED Requirements

### Requirement: Analyze missing full_name authors command
系统 SHALL 提供一个新的分析命令 `asktony analyze missing-fullname-authors`，用于列出最近 N 个月内有提交记录、但成员维度 `full_name` 为空/空白的作者清单。

#### Scenario: User runs the command
- **WHEN** 用户执行 `asktony analyze missing-fullname-authors`
- **THEN** 系统输出“最近 2 个月有提交且 `full_name` 缺失的作者”列表（默认 `months=2`）

### Requirement: Time window
系统 MUST 支持通过 `--months` 指定时间窗口（最近 N 个月，N>=1），并仅统计该窗口内的提交记录。

#### Scenario: User changes the analysis window
- **WHEN** 用户执行 `asktony analyze missing-fullname-authors --months 3`
- **THEN** 系统使用最近 3 个月作为统计窗口进行计算

### Requirement: Output columns
系统 MUST 输出以下字段（列名一致、可用于 CSV 表头）：
- `member_key`：可用于 `import-dim-info` 补全 `full_name` 的主键
- `username`：成员用户名（缺失时为空）
- `email`：成员邮箱（缺失时为空）
- `commit_count`：统计窗口内提交次数（MUST 为正整数）
- `repo_count`：统计窗口内提交覆盖的仓库数量（MUST 为正整数）

#### Scenario: User inspects returned columns
- **WHEN** 用户查看命令输出
- **THEN** 输出包含 `member_key/username/email/commit_count/repo_count` 这些列

### Requirement: Filtering logic
系统 MUST 仅返回满足以下条件的作者：
- 在统计窗口内 `commit_count > 0`
- `full_name` 为空或仅包含空白字符

#### Scenario: Only missing full_name rows are returned
- **WHEN** 用户执行 `asktony analyze missing-fullname-authors`
- **THEN** 输出中不包含 `full_name` 已补全的作者

### Requirement: CSV export
系统 MUST 支持通过 `--csv <path>` 将结果导出为 UTF-8 CSV，首行为表头，后续行为数据；导出内容 MUST 与该命令的列定义一致，并且不截断字符串字段。

#### Scenario: User exports CSV
- **WHEN** 用户执行 `asktony analyze missing-fullname-authors --months 2 --all --csv /output/missing_fullname_authors.csv`
- **THEN** 系统生成 `/output/missing_fullname_authors.csv`，并写入表头与数据行（字符串不截断）

### Requirement: Top and all options
系统 MUST 支持 `--top N` 与 `--all` 参数，并与现有 analyze 命令保持一致：默认返回 Top N；当指定 `--all` 时 MUST 返回全量结果且不做 Top 限制。

#### Scenario: User requests all results
- **WHEN** 用户执行 `asktony analyze missing-fullname-authors --all`
- **THEN** 系统返回全量缺失 `full_name` 的作者结果（不受 Top 限制）
