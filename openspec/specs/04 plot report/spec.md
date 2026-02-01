[TODO]！！tonken 用光了，暂未实现

# Role
你是一名资深的数据仓库工程师与数据可视化专家，精通 Python 生态（DuckDB, Pandas, Matplotlib/Seaborn）。

# Task
请编写一个名为 `visualize.py` 的 CLI 工具。该工具需连接本地的 `asktonydb.duckdb` 数仓，执行 SQL 查询提取数据，并生成一份**“Grok 风格”**（Modern UI、圆角、极简）的**长图表分析报告**。

# 1. Data Layer (数据层与 SQL)
* **Source:** 使用 `duckdb` 库连接 `asktonydb.duckdb`。
* **Schema Assumption:** 假设数仓 Gold/Silver 层包含以下基础视图（如果不存在，请在代码中用 CTE 临时构建）：
    * `dim_repos` (id, name)
    * `dim_users` (id, name, department, group_name)
    * `fact_commits` (commit_id, repo_id, author_id, commit_date)
* **Query Logic:** 代码需根据输入的周期参数（`weeks=1` 等），动态生成 SQL 的 `WHERE commit_date >= ...` 条件。

# 2. Functional Requirements (功能逻辑)
脚本应使用 `argparse` 接收参数：`--period` (week/month/bimonth), `--topn` (默认10), `--bottomn` (默认3)。

数据处理需包含以下 Pandas 逻辑：
1.  **活跃仓库分析 (Repos):**
    * Top N & Bottom N: 按 Commit Count 排序。
    * **分布桶 (Bucketing):** 使用 `pd.cut` 将仓库分为：`>1000`, `500-1000`, `100-500`, `10-100`, `<10` 五个区间，统计每个区间的仓库数量。
2.  **小组贡献分析 (Groups):**
    * Top N & Bottom N: 按 Commit Count 排序。
    * **百分位分布:** 使用 `pd.qcut` 或 `rank(pct=True)` 统计 `Top 5%`, `5%-30%`, `30%-70%`, `Bottom 30%` 的提交量占比或组数分布。
3.  **贡献者分析 (Committers):**
    * Top N & Bottom N: 按 Commit Count 排序。
    * **百分位分布:** 同上，按个人维度统计分布。
    * **零贡献清单:** 找出 commit count = 0 的活跃用户（基于 dim_users 但在 fact_commits 无记录的人员）。

# 3. Visualization Layout (布局策略)
由于包含 3 个大类共 9+ 个子模块，**必须**使用 `matplotlib.gridspec` 创建一个 **3行 x 3列** 的布局（Figure Size 建议 20x24 英寸）：

* **Row 1 (Repos):** [Col 1: Top N Bar] | [Col 2: Bottom N Bar] | [Col 3: Distribution Bar/Donut]
* **Row 2 (Groups):** [Col 1: Top N Bar] | [Col 2: Bottom N Bar] | [Col 3: Distribution Bar/Donut]
* **Row 3 (Users):** [Col 1: Top N Bar] | [Col 2: Bottom N Bar] | [Col 3: Distribution Bar/Donut]
* **Footer (Text):** 在图表最底部留白，使用 `plt.text` 或 `plt.table` 列出“无贡献人员清单” (若人数过多仅显示前 20 名 + "..." )。

# 4. Visual Style Requirements (Grok UI 风格)
* **核心组件 - 圆角柱体:** 所有的柱状图 (Bar Chart) **必须**使用 `matplotlib.patches.FancyBboxPatch` 绘制完全圆润的顶部（Capsule style）。严禁使用默认矩形。
* **配色 (Pastel Palette):**
    * Row 1 (Repos) 主色调: 柔和粉色 (Pastel Red/Pink)
    * Row 2 (Groups) 主色调: 柔和青色 (Teal/Cyan)
    * Row 3 (Users) 主色调: 柔和紫色 (Purple/Lavender)
* **极简主义:**
    * **隐藏 Y 轴:** `ax.yaxis.set_visible(False)`，移除所有 Y 轴脊柱、刻度和标签。
    * **去边框:** `ax.spines['top/right/left'].set_visible(False)`。
    * **网格:** 仅保留极淡的水平虚线网格 (`alpha=0.2`, `zorder=0`)。
* **数据直读:** 必须在每个圆角柱体的**正上方**标注数值（Font: Bold, Color: Black）。

# 5. Code Structure (代码架构)
请遵循面向对象或模块化设计：
1.  `class DuckDBLoader`: 负责 SQL 连接、时间计算、DataFrame 提取。
2.  `class DataProcessor`: 负责 Pandas 的 groupby, nlargest, cut/qcut 分桶逻辑。
3.  `class GrokVisualizer`:
    * `draw_rounded_bar(ax, x, y, color)`: 核心绘图函数。
    * `draw_distribution(ax, data, color)`: 绘制分布图（建议使用水平条形图或环形图）。
    * `render_dashboard(...)`: 组装 GridSpec 并保存图片。
4.  `main()`: CLI 入口。

请生成完整的、可运行的 Python 代码。