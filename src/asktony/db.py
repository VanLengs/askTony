from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


@dataclass(frozen=True)
class DB:
    path: Path
    name: str = "asktonydb"

    def connect(self) -> "duckdb.DuckDBPyConnection":
        try:
            import duckdb  # type: ignore[import-not-found]
        except ModuleNotFoundError as e:  # pragma: no cover
            raise ModuleNotFoundError(
                "Missing dependency 'duckdb'. Install it (e.g. `pip install duckdb`) to run AskTony."
            ) from e

        conn = duckdb.connect(str(self.path))
        conn.execute("PRAGMA threads=4")
        conn.execute("PRAGMA enable_progress_bar=false")
        self._try_load_ducklake(conn)
        return conn

    @staticmethod
    def _try_load_ducklake(conn: "duckdb.DuckDBPyConnection") -> None:
        # DuckLake 可能需要 INSTALL（依赖网络/环境）；这里尽力而为，不强依赖。
        try:
            conn.execute("LOAD ducklake")
            return
        except Exception:  # noqa: BLE001
            pass

        try:
            conn.execute("INSTALL ducklake")
            conn.execute("LOAD ducklake")
        except Exception:  # noqa: BLE001
            # Fallback to plain DuckDB.
            return
