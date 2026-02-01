【已实现】

现在需要对 dim_member和 dim_repo中的数据做一些标准化：
1）增加两个辅助命令用于导出 dim_member和dim_repo 的信息采集模板（csv 格式）
2）线下采集补充信息
    2.1）用户根据导出的 csv 模板，补充 dim_member 和 dim_repo 中缺失的信息（例如：成员的姓名、成员的二级部门、成员的三级分组、成员的角色，仓库所属二级部门、仓库所属三级分组）
    2.2）命令行工具读取补充信息的 csv 文件，并将补充信息写入到数据仓库对应的维度表中
    2.3）把二级部门、三级分组作为两个独立的维度表进行管理（dim_department_level2, dim_department_level3）
    2.4) 对两个 examples csv文件进行预填充，其中dim_member_template.csv 中预先填充好所有 member （取自 dim_member)的member_key、username、emai字段，其他字段填空。role字段根据质检要求，做一个下拉选项
    2.5) dim_repo_template的预填充命令，不用填充所有 repo，只需要预填充 active repos（最近3个月有 commit 记录的 repo），填充 member_key、repo_id、repo_name字段，其他字段填空
3）更新维度表结构
    3.1）dim_member 表增加以下字段：
        - full_name (成员的全名)
        - department_level2_id (成员所属二级部门ID，关联 dim_department_level2 表)
        - department_level3_id (成员所属三级分组ID，关联 dim_department_level3 表)
        - role (成员在仓库中的角色，例如：Owner, Maintainer, Developer, Reporter, Guest)
    3.2）dim_repo 表增加以下字段：
        - department_level2_id (仓库所属二级部门ID，关联 dim_department_level2 表)
        - department_level3_id (仓库所属三级分组ID，关联 dim_department_level3 表)
4）数据质量检查
    4.1）在导入补充信息时，进行数据质量检查，确保必填字段不为空，且关联的二级部门和三级分组在对应的维度表中存在
    4.2）提供数据质量报告，列出导入过程中发现的问题，供用户修正后重新导入
5）更新文档
    5.1）更新命令行工具的使用文档，说明如何导出模板、补充信息以及导入数据的步骤
    5.2）提供示例 csv 模板，帮助用户理解需要补充的信息格式  
6）技术栈和要求保持与之前一致
    - Python（3.13）+ducklake
    - cli界面友好
    - 工具（项目）名称：AskTony
    - duckdb名称： asktonydb
7）命令行工具示例
    - 导出 dim_member 模板： `asktony export-member-template --output dim_member_template.csv`
    - 导出 dim_repo 模板： `asktony export-repo-template --output dim_repo_template.csv`
    - 导入补充信息： `asktony import-dim-info --member-file dim_member_filled.csv --repo-file dim_repo_filled.csv`
8）基于新增的维度和标准化的字段（成员的姓名、部门、角色等），后续可以支持更丰富的分析命令，例如：
    - 按部门统计成员的提交量
    - 按角色分析成员在仓库中的活跃度
    - 按部门和角色细分的仓库活跃情况
请帮我实现上述功能（openspec）。