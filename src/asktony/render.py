from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from rich.table import Table


def to_rich_table(columns: Sequence[str], rows: Sequence[Sequence[Any]], title: str | None = None) -> Table:
    t = Table(title=title, show_lines=False)
    for c in columns:
        t.add_column(str(c))
    for r in rows:
        t.add_row(*[("" if v is None else str(v)) for v in r])
    return t

