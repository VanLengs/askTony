## ADDED Requirements

### Requirement: Analyze active members command
系统 SHALL 提供一个新的分析命令 `asktony analyze active-members`，用于识别在最近 N 个月内提交次数 `> 0` 的成员，并在终端展示结果或导出为 CSV。

#### Scenario: User runs the command
- **WHEN** 用户执行 `asktony analyze active-members`
- **THEN** 系统输出“最近 2 个月提交次数 > 0 的成员”列表（默认 `months=2`）

### Requirement: Active definition and time window
系统 MUST 支持通过 `--months` 指定时间窗口（最近 N 个月，N>=1）。在该窗口内，成员的 `commit_count` 统计值 `> 0` 时，成员 MUST 被判定为 active 并出现在结果中。

#### Scenario: User changes the analysis window
- **WHEN** 用户执行 `asktony analyze active-members --months 3`
- **THEN** 系统使用最近 3 个月作为统计窗口，并仅返回在该窗口内 `commit_count>0` 的成员

### Requirement: Output columns
系统 MUST 在结果中提供以下字段（列名一致、可用于 CSV 表头）：
- `member`：成员标识（与维表/数据仓库中的 member 字段一致）
- `full_name`：成员姓名（来自成员维表；缺失时为空）
- `department_level2_name`：成员二级部门名称（缺失时为空）
- `department_level3_name`：成员三级分组名称（缺失时为空）
- `commit_count`：统计窗口内提交次数（MUST 为正整数）

#### Scenario: User inspects the returned columns
- **WHEN** 用户查看 `asktony analyze active-members` 的输出
- **THEN** 输出中包含 `member/full_name/department_level2_name/department_level3_name/commit_count` 这些列

### Requirement: CSV export
系统 MUST 支持通过 `--csv <path>` 将结果导出为 UTF-8 CSV，首行为表头，后续行为数据；导出内容 MUST 与该命令的列定义一致，并且不截断字符串字段。

#### Scenario: User exports CSV
- **WHEN** 用户执行 `asktony analyze active-members --months 2 --all --csv /output/active_members.csv`
- **THEN** 系统生成 `/output/active_members.csv`，并写入表头与数据行（字符串不截断）

### Requirement: CSV includes only members with full_name
在启用 `--csv` 导出时，系统 MUST 只导出 `full_name` 非空的记录；`full_name` 为空或仅包含空白字符的记录 MUST 被排除在 CSV 之外。

#### Scenario: CSV excludes missing full_name rows
- **WHEN** 用户执行 `asktony analyze active-members --csv /output/active_members.csv`
- **THEN** 生成的 CSV 中不包含 `full_name` 为空/空白的成员记录

### Requirement: Top and all options
系统 MUST 支持 `--top N` 与 `--all` 参数，并与现有 analyze 命令保持一致：默认返回 Top N；当指定 `--all` 时 MUST 返回全量结果且不做 Top 限制。

#### Scenario: User requests all results
- **WHEN** 用户执行 `asktony analyze active-members --all`
- **THEN** 系统返回全量 active 成员结果（不受 Top 限制）
