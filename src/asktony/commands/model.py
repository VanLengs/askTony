from __future__ import annotations

import typer
from rich.console import Console

from asktony.config import load_config
from asktony.warehouse import Warehouse

model_app = typer.Typer(help="生成数据仓库分层模型（dims/facts）。", add_completion=False)
console = Console()


@model_app.command("build")
def model_build() -> None:
    cfg = load_config()
    wh = Warehouse.from_config(cfg)
    wh.build()
    console.print("[green]已生成 gold 模型（dim/fact）。[/green]")

