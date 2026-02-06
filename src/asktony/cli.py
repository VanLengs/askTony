from __future__ import annotations

import typer
from rich.console import Console

from asktony.commands.analyze import analyze_app
from asktony.commands.config import config_app
from asktony.commands.critic import critic_app
from asktony.commands.ingest import ingest_app
from asktony.commands.model import model_app
from asktony.commands.visualize import visualize_app
from asktony.commands.universe import universe_app
from asktony.dim_admin import DimAdmin, issues_to_table
from asktony.project_admin import ProjectAdmin, issues_to_table as project_issues_to_table
from pathlib import Path
from asktony.project_templates import export_project_collection_xlsx

app = typer.Typer(
    name="asktony",
    help="AskTony - CNB analytics CLI (DuckDB + DuckLake).",
    add_completion=False,
    no_args_is_help=True,
)

app.add_typer(config_app, name="config")
app.add_typer(ingest_app, name="ingest")
app.add_typer(model_app, name="model")
app.add_typer(analyze_app, name="analyze")
app.add_typer(visualize_app, name="visualize")
app.add_typer(universe_app, name="universe")
app.add_typer(critic_app, name="critic")

console = Console()

BARBER = r'''
                    _.-""-._
                 .'  _    _  '.
                /   (_)  (_)    \
               |  ,           ,  |
               |  \`.       .`/  |    
                \  '.`'---'`.'  /
                 '.  `"---"`  .'
                   '-._____.-'
             ( ✂️✂️✂️✂️✂️✂️✂️✂️✂️✂️✂️✂️ )
'''


@app.callback()
def _root_callback(ctx: typer.Context) -> None:
    # Print once per invocation (before subcommand execution)
    if ctx.invoked_subcommand:
        console.print(f"[bold cyan]{BARBER}[/bold cyan]")


@app.command("export-member-template")
def export_member_template(
    output: str = typer.Option("dim_member_template.csv", "--output", "-o", help="输出 CSV 文件路径"),
    blank: bool = typer.Option(True, help="仅预填 member_key/username/email，其余字段置空"),
) -> None:
    from asktony.config import load_config
    from asktony.db import DB

    cfg = load_config()
    admin = DimAdmin(DB(cfg.db_path_resolved))
    admin.export_member_template(Path(output), blank=blank)
    console.print({"output": output})


@app.command("export-repo-template")
def export_repo_template(
    output: str = typer.Option("dim_repo_template.csv", "--output", "-o", help="输出 CSV 文件路径"),
    blank: bool = typer.Option(True, help="仅预填 repo_id/repo_name/repo_path，其余字段置空"),
    active_only: bool = typer.Option(True, "--active-only/--all", help="仅导出活跃仓库（默认）；或导出全部仓库"),
    months: int = typer.Option(2, min=1, max=60, help="活跃窗口：最近 N 个月（仅在 --active-only 时生效）"),
) -> None:
    from asktony.config import load_config
    from asktony.db import DB

    cfg = load_config()
    admin = DimAdmin(DB(cfg.db_path_resolved))
    admin.export_repo_template(Path(output), blank=blank, active_only=active_only, months=months)
    console.print({"output": output})


@app.command("import-dim-info")
def import_dim_info(
    member_file: str | None = typer.Option(None, "--member-file", help="补充后的 dim_member CSV"),
    repo_file: str | None = typer.Option(None, "--repo-file", help="补充后的 dim_repo CSV"),
    dry_run: bool = typer.Option(False, help="只做校验与报告，不写入数据库"),
    auto_create_departments: bool = typer.Option(True, help="自动创建 CSV 中出现的新部门/分组"),
) -> None:
    from asktony.config import load_config
    from asktony.db import DB

    if member_file is None and repo_file is None:
        raise typer.BadParameter("至少提供 --member-file 或 --repo-file")

    cfg = load_config()
    admin = DimAdmin(DB(cfg.db_path_resolved))
    issues, stats = admin.import_dim_info(
        member_file=Path(member_file) if member_file else None,
        repo_file=Path(repo_file) if repo_file else None,
        auto_create_departments=auto_create_departments,
        dry_run=dry_run,
    )
    if issues:
        console.print(issues_to_table(issues))
        raise SystemExit(1)
    console.print(stats)


@app.command("import-project-info")
def import_project_info(
    input_xlsx: str | None = typer.Option(
        None,
        "--input",
        "-i",
        help="项目采集 XLSX（推荐：export-project-collection 的输出；包含 dim_project/bridge_project_repo/bridge_project_person_role 三个 sheet）",
    ),
    project_file: str | None = typer.Option(None, "--project-file", help="(deprecated) 项目主数据 CSV", hidden=True),
    project_repo_file: str | None = typer.Option(None, "--project-repo-file", help="(deprecated) 项目-仓库映射 CSV", hidden=True),
    project_member_file: str | None = typer.Option(None, "--project-member-file", help="(deprecated) 项目成员 CSV", hidden=True),
    dry_run: bool = typer.Option(False, help="只做校验与报告，不写入数据库"),
) -> None:
    from asktony.config import load_config
    from asktony.db import DB

    if input_xlsx is None and project_file is None and project_repo_file is None and project_member_file is None:
        raise typer.BadParameter("请提供 --input <xlsx>（推荐）")

    cfg = load_config()
    admin = ProjectAdmin(DB(cfg.db_path_resolved))
    issues, stats = admin.import_project_info(
        xlsx_file=Path(input_xlsx) if input_xlsx else None,
        project_file=Path(project_file) if project_file else None,
        project_repo_file=Path(project_repo_file) if project_repo_file else None,
        project_member_file=Path(project_member_file) if project_member_file else None,
        dry_run=dry_run,
    )
    if issues:
        console.print(project_issues_to_table(issues))
        raise SystemExit(1)
    console.print(stats)


@app.command("export-project-collection")
def export_project_collection(
    output: str = typer.Option("project_info_collection.xlsx", "--output", "-o", help="输出 XLSX 文件路径"),
) -> None:
    """
    导出“项目维度信息采集”Excel：填写后可分别另存为 CSV，再用 import-project-info 导入。
    """
    from asktony.config import load_config

    cfg = load_config()
    out, info = export_project_collection_xlsx(Path(output), db_path=cfg.db_path_resolved)
    console.print({"output": str(out), **info})
