from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomllib


def _default_home() -> Path:
    return Path(os.environ.get("ASKTONY_HOME", Path.home() / ".asktony")).expanduser()


def _mask_secret(value: str, keep: int = 4) -> str:
    if not value:
        return value
    if len(value) <= keep:
        return "*" * len(value)
    return "*" * (len(value) - keep) + value[-keep:]


def _safe_toml_str(value: str) -> str:
    # Minimal TOML string escaping for our config needs.
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


@dataclass(frozen=True)
class AskTonyConfig:
    cnb_username: str
    cnb_token: str
    cnb_group: str
    cnb_base_url: str = "https://api.cnb.cool"
    cnb_auth_header: str = "Authorization"
    cnb_auth_prefix: str = "Bearer"
    lake_dir: str | None = None
    db_path: str | None = None

    @property
    def home_dir(self) -> Path:
        return _default_home()

    @property
    def config_path(self) -> Path:
        return self.home_dir / "config.toml"

    @property
    def lake_dir_path(self) -> Path:
        if self.lake_dir:
            return Path(self.lake_dir).expanduser()
        return self.home_dir / "lake"

    @property
    def db_path_resolved(self) -> Path:
        if self.db_path:
            p = Path(self.db_path).expanduser()
            if p.is_dir():
                return p / "asktonydb.duckdb"
            return p
        return self.home_dir / "asktonydb.duckdb"

    def masked_dict(self) -> dict[str, Any]:
        return {
            "cnb_username": self.cnb_username,
            "cnb_token": _mask_secret(self.cnb_token),
            "cnb_group": self.cnb_group,
            "cnb_base_url": self.cnb_base_url,
            "cnb_auth_header": self.cnb_auth_header,
            "cnb_auth_prefix": self.cnb_auth_prefix,
            "lake_dir": str(self.lake_dir_path),
            "db_path": str(self.db_path_resolved),
            "ASKTONY_HOME": str(self.home_dir),
        }


def _ensure_dirs(cfg: AskTonyConfig) -> None:
    cfg.home_dir.mkdir(parents=True, exist_ok=True)
    cfg.lake_dir_path.mkdir(parents=True, exist_ok=True)
    (cfg.lake_dir_path / "bronze").mkdir(parents=True, exist_ok=True)
    (cfg.lake_dir_path / "silver").mkdir(parents=True, exist_ok=True)
    (cfg.lake_dir_path / "gold").mkdir(parents=True, exist_ok=True)


def load_config() -> AskTonyConfig:
    home = _default_home()
    path = home / "config.toml"
    if not path.exists():
        raise SystemExit(
            f"未找到配置文件：{path}\n"
            "请先运行：asktony config set --username ... --token ... --group ..."
        )
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    cfg = AskTonyConfig(
        cnb_username=str(data["cnb_username"]),
        cnb_token=str(data["cnb_token"]),
        cnb_group=str(data["cnb_group"]),
        cnb_base_url=str(data.get("cnb_base_url", "https://api.cnb.cool")).rstrip("/"),
        cnb_auth_header=str(data.get("cnb_auth_header", "Authorization")),
        cnb_auth_prefix=str(data.get("cnb_auth_prefix", "Bearer")),
        lake_dir=data.get("lake_dir"),
        db_path=data.get("db_path"),
    )
    _ensure_dirs(cfg)
    return cfg


def save_config(cfg: AskTonyConfig) -> None:
    _ensure_dirs(cfg)
    lines: list[str] = []
    for key, value in {
        "cnb_username": cfg.cnb_username,
        "cnb_token": cfg.cnb_token,
        "cnb_group": cfg.cnb_group,
        "cnb_base_url": cfg.cnb_base_url.rstrip("/"),
        "cnb_auth_header": cfg.cnb_auth_header,
        "cnb_auth_prefix": cfg.cnb_auth_prefix,
        "lake_dir": cfg.lake_dir or "",
        "db_path": cfg.db_path or "",
    }.items():
        if value == "":
            continue
        if not re.fullmatch(r"[a-zA-Z0-9_]+", key):
            raise ValueError(f"Invalid config key: {key}")
        lines.append(f"{key} = {_safe_toml_str(str(value))}")

    cfg.config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

