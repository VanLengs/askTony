from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import typer
from rich.console import Console

from asktony.config import load_config
from asktony.warehouse import Warehouse

visualize_app = typer.Typer(help="数据可视化报告（Grok 风格）。", add_completion=False)
console = Console()


def _since_from_period(period: str) -> dt.timedelta:
    p = period.strip().lower()
    if p in {"week", "w", "7d"}:
        return dt.timedelta(days=7)
    if p in {"month", "m", "30d"}:
        return dt.timedelta(days=30)
    if p in {"bimonth", "2m", "60d"}:
        return dt.timedelta(days=60)
    raise typer.BadParameter("period must be one of: week, month, bimonth")


@visualize_app.command("report")
def report(
    period: str = typer.Option("bimonth", help="榜单周期：week/month/bimonth"),
    top_n: int = typer.Option(10, min=1, max=200, help="Top N（默认 10）"),
    bottom_n: int = typer.Option(10, min=0, max=50, help="Bottom N（默认 10）"),
    output: Path = typer.Option(Path("output/report.png"), help="输出图片路径（png）"),
    dpi: int = typer.Option(180, min=72, max=600, help="输出 DPI"),
) -> None:
    from asktony.visualize import plot_report

    cfg = load_config()
    window = _since_from_period(period)
    since_dt = dt.datetime.now(dt.timezone.utc) - window

    out = output.expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)

    plot_report(
        db_path=cfg.db_path_resolved,
        since_dt=since_dt,
        top_n=top_n,
        bottom_n=bottom_n,
        output=out,
        dpi=dpi,
    )
    console.print({"output": str(out), "since": since_dt.isoformat(), "top_n": top_n, "bottom_n": bottom_n})


@visualize_app.command("line-manager-dev-activity")
def line_manager_dev_activity(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月（与 analyze 口径一致）"),
    top_n: int = typer.Option(12, min=1, max=60, help="展示 Top N（按 score_total）"),
    bottom_n: int = typer.Option(6, min=0, max=60, help="附带 Bottom N（用于对比）"),
    input_csv: Path | None = typer.Option(None, "--input", help="读取 analyze 导出的 CSV（可选）"),
    output: Path = typer.Option(Path("output/line_manager_dev_activity_radar.png"), help="输出图片路径（png）"),
    dpi: int = typer.Option(200, min=72, max=600, help="输出 DPI"),
) -> None:
    """
    针对 line_manager 的综合评分模型做横向对比（雷达图 + 改进建议）。
    """
    from asktony.visualize import plot_line_manager_dev_activity_radar

    cfg = load_config()
    out = output.expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)

    if input_csv is not None:
        df = pd.read_csv(input_csv.expanduser())
    else:
        wh = Warehouse.from_config(cfg)
        columns, rows = wh.line_manager_dev_activity_data(months=months, top=None)
        df = pd.DataFrame(rows, columns=columns)

    plot_line_manager_dev_activity_radar(df=df, output=out, dpi=dpi, top_n=top_n, bottom_n=bottom_n)
    console.print(
        {
            "output": str(out),
            "months": months,
            "top_n": top_n,
            "bottom_n": bottom_n,
            "rows": int(len(df)),
        }
    )


@visualize_app.command("active-employee-score")
def active_employee_score(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月（与 analyze 口径一致）"),
    top_n: int = typer.Option(10, min=1, max=60, help="展示 Top N（按 score_total）"),
    bottom_n: int = typer.Option(10, min=0, max=60, help="附带 Bottom N（用于对比）"),
    input_csv: Path | None = typer.Option(None, "--input", help="读取 analyze 导出的 CSV（可选）"),
    output: Path = typer.Option(Path("output/active_employee_score_radar.png"), help="输出图片路径（png）"),
    dpi: int = typer.Option(200, min=72, max=600, help="输出 DPI"),
) -> None:
    """
    针对活跃员工综合评分做横向对比（雷达图）。
    """
    from asktony.visualize import plot_active_employee_score_radar

    cfg = load_config()
    out = output.expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)

    if input_csv is not None:
        df = pd.read_csv(input_csv.expanduser())
    else:
        wh = Warehouse.from_config(cfg)
        columns, rows = wh.active_employee_score_data(months=months, top=None)
        df = pd.DataFrame(rows, columns=columns)

    plot_active_employee_score_radar(df=df, output=out, dpi=dpi, top_n=top_n, bottom_n=bottom_n)
    console.print(
        {
            "output": str(out),
            "months": months,
            "top_n": top_n,
            "bottom_n": bottom_n,
            "rows": int(len(df)),
        }
    )


@visualize_app.command("anti-fraud-report")
def anti_fraud_report(
    months: int = typer.Option(2, min=1, max=60, help="最近 N 个月（与 analyze 口径一致）"),
    top_n: int = typer.Option(10, min=1, max=50, help="Top N 可疑员工（默认 10）"),
    output: Path = typer.Option(Path("output/anti_fraud_report.png"), help="输出图片路径（png）"),
    dpi: int = typer.Option(200, min=72, max=600, help="输出 DPI"),
) -> None:
    """
    反欺诈（反刷 commit）可视化报表：Top 可疑员工 + tag 分布 + manager 分布等。
    """
    from asktony.visualize import plot_anti_fraud_report

    cfg = load_config()
    wh = Warehouse.from_config(cfg)

    out = output.expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)

    columns, rows = wh.suspicious_committers_data(months=months, top=None)
    df = pd.DataFrame(rows, columns=columns)
    plot_anti_fraud_report(df=df, output=out, dpi=dpi, top_n=top_n)
    console.print({"output": str(out), "months": months, "top_n": top_n, "rows": int(len(df))})
