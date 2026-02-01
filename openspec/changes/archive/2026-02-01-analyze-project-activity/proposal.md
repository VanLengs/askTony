## Why

目前 AskTony 已支持按 `repo / member / line_manager` 维度做研发效能分析，但公司实际资源投入与管理考核往往以“项目”为单位：同一研发人员可能同时贡献多个项目、一个项目也可能跨多个仓库与阶段。缺少项目维度会导致资源占用与产出无法对齐，难以做横向对比与改进激励。

## What Changes

- 新增“项目”维度与两类桥表（项目-仓库、项目-人员-角色），支持时间区间、多人多项目、多仓库多项目、权重分摊与人员 allocation（FTE）。
- 新增项目主数据与映射信息的导入能力（CSV），并做数据质量校验（权重/时间区间/重复/缺失等）。
- 在 gold 层建立项目相关的月度聚合事实，用于按月衡量项目活跃度/产出/集中度/人力结构等指标。
- 新增按项目维度的分析命令（先按月），用于项目横向对比与绩效改进激励。

## Capabilities

### New Capabilities

- `import-project-info`: 导入项目主数据与映射（`dim_project`、`bridge_project_repo`、`bridge_project_person_role`），并校验：
  - `project_id` 使用项目名称的汉语拼音（系统自带转写），确保稳定可复现
  - `repo->project` 支持多归属 + `weight`（同月可分摊到多个项目）
  - `employee_id` 作为人员主键（项目成员统一用 `employee_id`），支持 `allocation`
  - 映射的 `start_at/end_at` 时间区间与冲突/重叠校验
- `model-project-facts`: 在 gold 层新增项目相关模型（维表 + 桥表 + 月度聚合事实），用于后续分析与可视化：
  - 以 `commit_month` 为粒度，将 `fact_commit` 通过 `bridge_project_repo`（含 `weight`）归属到项目
  - 通过员工维表将提交作者对齐到 `employee_id`（仅统计已导入的员工名单）
- `analyze-project-activity`: 新增 `asktony analyze project-activity`（按月窗口）导出项目指标（表格/CSV），用于横向对比与改进激励。

### Modified Capabilities

<!-- None -->

## Impact

- 新增 CLI 命令（项目导入、项目分析），并在数仓 gold 层新增项目相关表/视图与聚合事实。
- 依赖数据输入：项目清单、项目-仓库映射（含权重与时间区间）、项目-人员-角色映射（含 allocation 与时间区间）。
- 项目分析将依赖现有员工口径（`dim_member_enrichment`）与 `employee_id` 主键，确保同一员工不会因多个账号/占位 key 被重复计数。
