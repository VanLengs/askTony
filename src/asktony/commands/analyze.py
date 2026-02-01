from __future__ import annotations

import csv
from pathlib import Path

import typer
from rich.console import Console

from asktony.config import load_config
from asktony.warehouse import Warehouse

analyze_app = typer.Typer(help="基于 gold 层的分析命令。", add_completion=False)
console = Console()

def _write_csv(path: Path, columns: list[str], rows: list[tuple]) -> None:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for r in rows:
            # No truncation: write full string values.
            w.writerow(["" if v is None else str(v) for v in r])


@analyze_app.command("active-repos")
def active_repos(
    months: int = typer.Option(3, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(20, min=1, max=2000, help="返回 Top N"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
) -> None:
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.active_repos_data(months=months, top=top_n)
    if csv_path is not None:
        _write_csv(csv_path, columns, rows)
        console.print({"csv": str(csv_path), "rows": len(rows)})
        return
    console.print(wh.active_repos(months=months, top=top_n or 2000))


@analyze_app.command("member-commits")
def member_commits(
    months: int = typer.Option(3, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(50, min=1, max=20000, help="返回 Top N"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
) -> None:
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.member_commits_all_repos_data(months=months, top=top_n)
    if csv_path is not None:
        _write_csv(csv_path, columns, rows)
        console.print({"csv": str(csv_path), "rows": len(rows)})
        return
    console.print(wh.member_commits_all_repos(months=months, top=top_n or 20000))


@analyze_app.command("repo-member-commits")
def repo_member_commits(
    months: int = typer.Option(3, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(100, min=1, max=200000, help="返回 Top N"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
) -> None:
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.repo_member_commits_data(months=months, top=top_n)
    if csv_path is not None:
        _write_csv(csv_path, columns, rows)
        console.print({"csv": str(csv_path), "rows": len(rows)})
        return
    console.print(wh.repo_member_commits(months=months, top=top_n or 200000))


@analyze_app.command("inactive-members")
def inactive_members(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(2000, min=1, max=200000, help="返回 Top N"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
    all_fields: bool = typer.Option(False, "--all-fields", help="输出全字段集合（默认最小字段集合）"),
) -> None:
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.inactive_members_data(months=months, top=top_n, all_fields=all_fields)
    if csv_path is not None:
        full_name_idx = columns.index("full_name")
        filtered_rows = [
            r for r in rows if str(r[full_name_idx] or "").strip() != ""
        ]
        _write_csv(csv_path, columns, filtered_rows)
        console.print({"csv": str(csv_path), "rows": len(filtered_rows)})
        return
    console.print(wh.inactive_members(months=months, top=top_n, all_fields=all_fields))


@analyze_app.command("active-members")
def active_members(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(2000, min=1, max=200000, help="返回 Top N"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
    all_fields: bool = typer.Option(False, "--all-fields", help="输出全字段集合（默认最小字段集合）"),
) -> None:
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.active_members_data(months=months, top=top_n, all_fields=all_fields)
    if csv_path is not None:
        full_name_idx = columns.index("full_name")
        filtered_rows = [r for r in rows if str(r[full_name_idx] or "").strip() != ""]
        _write_csv(csv_path, columns, filtered_rows)
        console.print({"csv": str(csv_path), "rows": len(filtered_rows)})
        return
    console.print(wh.active_members(months=months, top=top_n, all_fields=all_fields))


@analyze_app.command("active-employee-score")
def active_employee_score(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(200, min=1, max=200000, help="返回 Top N（按 score_total 倒序）"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
) -> None:
    """
    活跃员工综合评分榜单（用于报告/雷达图统计口径）。
    """
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.active_employee_score_data(months=months, top=top_n)
    if csv_path is not None:
        _write_csv(csv_path, columns, rows)
        console.print({"csv": str(csv_path), "rows": len(rows)})
        return
    console.print(wh.active_employee_score(months=months, top=top_n))


@analyze_app.command("missing-fullname-authors")
def missing_fullname_authors(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(200, min=1, max=200000, help="返回 Top N"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
) -> None:
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.missing_fullname_authors_data(months=months, top=top_n)
    if csv_path is not None:
        _write_csv(csv_path, columns, rows)
        console.print({"csv": str(csv_path), "rows": len(rows)})
        return
    console.print(wh.missing_fullname_authors(months=months, top=top_n))


@analyze_app.command("project-activity")
def project_activity(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(200, min=1, max=200000, help="返回 Top N"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
) -> None:
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.project_activity_data(months=months, top=top_n)
    if csv_path is not None:
        _write_csv(csv_path, columns, rows)
        console.print({"csv": str(csv_path), "rows": len(rows)})
        return
    console.print(wh.project_activity(months=months, top=top_n))


@analyze_app.command("line-manager-dev-activity")
def line_manager_dev_activity(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(200, min=1, max=200000, help="返回 Top N"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
) -> None:
    """
    统计 line_manager 下开发角色员工的活跃度（active/inactive），并提供辅助指标。
    """
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.line_manager_dev_activity_data(months=months, top=top_n)
    if csv_path is not None:
        _write_csv(csv_path, columns, rows)
        console.print({"csv": str(csv_path), "rows": len(rows)})
        return
    console.print(wh.line_manager_dev_activity(months=months, top=top_n))


@analyze_app.command("suspicious-committers")
def suspicious_committers(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月"),
    top: int = typer.Option(200, min=1, max=200000, help="返回 Top N（按可疑分倒序）"),
    all: bool = typer.Option(False, "--all", help="返回全量（不做 Top 限制）"),
    csv_path: Path | None = typer.Option(None, "--csv", help="输出到 CSV 文件"),
) -> None:
    """
    反刷 commit 启发式：识别“数量很好看，但价值/协作/复杂度很可疑”的模式。
    """
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    top_n = None if all else top
    columns, rows = wh.suspicious_committers_data(months=months, top=top_n)
    if csv_path is not None:
        _write_csv(csv_path, columns, rows)
        console.print({"csv": str(csv_path), "rows": len(rows)})
        return
    console.print(wh.suspicious_committers(months=months, top=top_n))


@analyze_app.command("debug-active-repos")
def debug_active_repos(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月"),
) -> None:
    """
    排查 active-repos 结果为空的原因（fact_commit/bridge_repo_member/join 命中情况）。
    """
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    info = wh.debug_active_repos(months=months)
    console.print(info)
