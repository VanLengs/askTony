from __future__ import annotations

import typer
from rich.console import Console

from asktony.config import AskTonyConfig, load_config, save_config

config_app = typer.Typer(help="管理 CNB 认证与本地配置。", add_completion=False)
console = Console()


@config_app.command("set")
def config_set(
    username: str = typer.Option(..., help="CNB 用户名"),
    token: str = typer.Option(..., help="CNB Access Token（会写入本地配置）"),
    group: str = typer.Option(..., help="组织/群组标识（id 或 path，按 CNB API 要求）"),
    base_url: str = typer.Option("https://api.cnb.cool", help="CNB API Base URL"),
    auth_header: str = typer.Option("Authorization", help="鉴权 Header 名（如 Authorization / Private-Token）"),
    auth_prefix: str = typer.Option("Bearer", help="鉴权前缀（如 Bearer；若不需要可传空字符串）"),
    lake_dir: str | None = typer.Option(None, help="本地数据湖目录（默认 ~/.asktony/lake）"),
    db_path: str | None = typer.Option(None, help="DuckDB 文件路径（默认 ~/.asktony/asktonydb.duckdb）"),
) -> None:
    cfg = AskTonyConfig(
        cnb_username=username,
        cnb_token=token,
        cnb_group=group,
        cnb_base_url=base_url.rstrip("/"),
        cnb_auth_header=auth_header,
        cnb_auth_prefix=auth_prefix,
        lake_dir=lake_dir,
        db_path=db_path,
    )
    save_config(cfg)
    console.print("[green]已保存配置：[/green]")
    console.print(load_config().masked_dict())


@config_app.command("show")
def config_show() -> None:
    cfg = load_config()
    console.print(cfg.masked_dict())


@config_app.command("path")
def config_path() -> None:
    cfg = load_config()
    console.print(
        {
            "config": str(cfg.config_path),
            "lake_dir": str(cfg.lake_dir_path),
            "db_path": str(cfg.db_path_resolved),
        }
    )
