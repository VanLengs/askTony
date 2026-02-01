【已实现】

你是一名全栈开发工程师，请帮我实现以下需求：

1） 产品的功能是实现一个命令行工具（参照 claud code 或 open code 的命令行形式）
2） 该命令行工具支持：通过配置一个代码仓库 cnb（类似于 git）的用户名和 token，完成初始的用户配置工作
3） 该命令行工具支持：从 cnb 的 openapi 中采集数据到数据仓库（基于 datalake 的分层架构），采集的数据包括
- 3.1 组织下访问用户有权限查看到仓库（https://api.cnb.cool/#/operations/GetGroupSubRepos）
- 3.2 每个仓库的top活跃用户（https://api.cnb.cool/#/operations/TopContributors）
- 3.3 每个仓库的仓库内有效成员列表（https://api.cnb.cool/#/operations/ListAllMembers）
- 3.4 每个仓库的查询 commit 列表（https://api.cnb.cool/#/operations/ListCommits）
4）该命令行工具支持：生成数据仓库分层模型，包括
- 4.1 成员列表维度
- 4.2 仓库维度
- 4.3 分析一定时间内成员的提交数量、代码行数等相关事实表
5）该命令行工具支持：一系列分析命令，相关示例场景如下：
- 分析最近n个月内活跃的仓库
- 分析最近n个月内成员在所有仓库的提交量
- 分析最近n个月内活跃仓库内的成员提交量
- 等

要求：
1） 技术栈：Python（3.13）+ducklake
2） cli界面友好
3）工具（项目）名称：AskTony
4）duckdb名称： asktonydb
5）如果你能设置cli的启动asicc字符图，可以使用一个理发师或理发剪刀