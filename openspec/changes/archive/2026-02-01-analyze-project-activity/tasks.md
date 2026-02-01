## 1. 数据模型（gold）

- [ ] 1.1 增加项目相关表：`gold.dim_project` / `gold.bridge_project_repo` / `gold.bridge_project_person_role`
- [ ] 1.2 在 `Warehouse.build()` 中创建/刷新项目相关视图与事实：`fact_project_employee_month`、`fact_project_month`
- [ ] 1.3 实现 commit->employee_id 对齐（复用现有员工口径逻辑），并仅统计能对齐到员工且属于项目成员的提交
- [ ] 1.4 实现 repo->project 多归属的按月映射与 `weight` 分摊（commit_count/changed_lines 等按 weight 加权）

## 2. 项目导入命令（import-project-info）

- [ ] 2.1 增加 CLI 命令 `asktony import-project-info`（支持 `--project-file/--project-repo-file/--project-member-file` 与 `--dry-run`）
- [ ] 2.2 实现 `project_id` 的系统自带拼音转写 + 规范化（小写、下划线、去特殊字符），并支持 CSV 显式 project_id 覆盖
- [ ] 2.3 实现 `bridge_project_repo` 校验：必填字段、weight 范围、(project_id, repo_id) 区间重叠检测、repo-month 权重和偏离 1 的 warning 统计
- [ ] 2.4 实现 `bridge_project_person_role` 校验：employee_id 必填、allocation 范围、(project_id, employee_id, role) 区间重叠检测
- [ ] 2.5 实现 upsert 写入与 stats 输出（projects_upserted / mappings_upserted / members_upserted / warnings_count 等）
- [ ] 2.6 增加模板导出或在 README 中补充三类 CSV 的 header 与示例

## 3. 项目分析命令（analyze project-activity）

- [ ] 3.1 增加 `asktony analyze project-activity`（支持 `--months/--top/--all/--csv`）
- [ ] 3.2 输出核心指标：project_id/name、dev_headcount、dev_fte_sum、active_dev/inactive_dev、active_pct、weighted_commits_total、weighted_commits_per_fte
- [ ] 3.3 输出风险与治理信号：top1_share（集中度）、role_coverage（PO/TO/SM/TL 是否齐全）、repo_count（项目涉及仓库数）
- [ ] 3.4 增加一个 debug/补全辅助输出（可选）：未能对齐到 employee_id 的窗口期提交作者清单（便于补齐 HR/账号）

## 4. 文档与回归

- [ ] 4.1 更新 README：项目 CSV 格式、导入命令、分析命令示例
- [ ] 4.2 运行最小化语法校验（compileall）并确保 CLI help 可用
