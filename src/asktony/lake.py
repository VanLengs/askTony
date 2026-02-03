from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from asktony.config import AskTonyConfig
from asktony.db import DB


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ts_slug(t: dt.datetime) -> str:
    return t.strftime("%Y%m%dT%H%M%SZ")


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def _isoformat_z(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_datetime_loose(value: Any) -> dt.datetime | None:
    if value is None:
        return None

    # epoch seconds/ms
    if isinstance(value, (int, float)):
        # Heuristic: ms timestamps are usually > 1e12
        seconds = float(value) / 1000.0 if value > 1_000_000_000_000 else float(value)
        try:
            return dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc)
        except Exception:  # noqa: BLE001
            return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Common "Z" suffix
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Numeric string epoch
        if s.isdigit():
            try:
                n = int(s)
                return _parse_datetime_loose(n)
            except Exception:  # noqa: BLE001
                return None
        try:
            return dt.datetime.fromisoformat(s)
        except Exception:  # noqa: BLE001
            return None

    return None


def extract_committed_at_str(commit_obj: dict[str, Any]) -> str:
    """
    尽可能从 CNB commit 返回结构中提取提交时间，并规范化为 ISO8601（UTC, ...Z）。
    """
    candidates: list[Any] = []

    # Flat/common fields
    for k in ("committed_at", "committedAt", "committedAtUtc", "createdAt", "created_at", "date", "timestamp"):
        if k in commit_obj:
            candidates.append(commit_obj.get(k))

    # GitHub-like nested structure: commit.committer.date / commit.author.date
    commit_block = commit_obj.get("commit")
    if isinstance(commit_block, dict):
        committer = commit_block.get("committer")
        author = commit_block.get("author")
        if isinstance(committer, dict):
            candidates.append(committer.get("date"))
        if isinstance(author, dict):
            candidates.append(author.get("date"))

    # CNB-like nested structure (best-effort): committer.date / author.date
    committer = commit_obj.get("committer")
    author = commit_obj.get("author")
    if isinstance(committer, dict):
        candidates.append(committer.get("date"))
    if isinstance(author, dict):
        candidates.append(author.get("date"))

    for v in candidates:
        dtv = _parse_datetime_loose(v)
        if dtv is not None:
            return _isoformat_z(dtv)

    return ""


def _get_nested(d: dict[str, Any], *keys: str) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def extract_author_identity(commit_obj: dict[str, Any]) -> tuple[str, str, str]:
    """
    Returns (author_id, author_username, author_email).
    Best-effort across CNB/Git-like shapes.
    """
    author_id = ""
    author_username = ""
    author_email = ""

    author = commit_obj.get("author")
    if isinstance(author, dict):
        author_id = str(author.get("id") or author.get("userId") or author.get("uid") or "")
        author_username = str(author.get("username") or author.get("name") or author.get("login") or "")
        author_email = str(author.get("email") or "")

    if not author_username:
        v = _get_nested(commit_obj, "commit", "author", "name")
        if isinstance(v, str):
            author_username = v
    if not author_email:
        v = _get_nested(commit_obj, "commit", "author", "email")
        if isinstance(v, str):
            author_email = v
    if not author_email:
        v = _get_nested(commit_obj, "commit", "committer", "email")
        if isinstance(v, str):
            author_email = v

    return author_id, author_username, author_email


def extract_member_identity(member_obj: dict[str, Any]) -> tuple[str, str, str]:
    """
    Returns (user_id, username, email) for repo member record (best-effort).
    """
    user = member_obj.get("user") if isinstance(member_obj.get("user"), dict) else member_obj
    user_id = str(user.get("id") or user.get("userId") or user.get("uid") or "")
    username = str(user.get("username") or user.get("name") or user.get("login") or "")
    email = str(user.get("email") or "")
    if not email:
        v = member_obj.get("email")
        if isinstance(v, str):
            email = v
    return user_id, username, email


def _company_username_from_email(email: str) -> str | None:
    e = (email or "").strip().lower()
    if not e:
        return None
    # Corporate email policy:
    # - Regular employee: aa.bb@clife.cn
    # - Contractor: 801495@clife.cn (numeric local-part)
    if not re.fullmatch(r"(?:[a-z0-9]+\.[a-z0-9]+|[0-9]+)@clife\.cn", e):
        return None
    local = e.split("@", 1)[0]
    if re.fullmatch(r"[0-9]+", local):
        return f"partner-{local}"
    return local
def _parse_iso_dt(value: str) -> dt.datetime | None:
    return _parse_datetime_loose(value)


@dataclass(frozen=True)
class Lake:
    root: Path
    db: DB

    @classmethod
    def from_config(cls, cfg: AskTonyConfig) -> "Lake":
        return cls(root=cfg.lake_dir_path, db=DB(cfg.db_path_resolved))

    @property
    def bronze_dir(self) -> Path:
        return self.root / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.root / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.root / "gold"

    def write_bronze(self, dataset: str, payload: Any) -> Path:
        t = _utc_now()
        out_dir = self.bronze_dir / dataset
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{_ts_slug(t)}.jsonl"
        # jsonl: 1 line per record (we store a single record envelope per run)
        path.write_text(_json_dumps({"ingested_at": t.isoformat(), "payload": payload}) + "\n", encoding="utf-8")
        return path

    def _init_silver(self) -> None:
        with self.db.connect() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
            conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta.repo_watermark (
                  repo_id TEXT PRIMARY KEY,
                  -- Store as TEXT to avoid DuckDB Python TIMESTAMPTZ -> pytz dependency at runtime.
                  last_committed_at TEXT,
                  updated_at TIMESTAMPTZ
                )
                """
            )
            # Best-effort migration from older TIMESTAMPTZ type.
            try:
                conn.execute("ALTER TABLE meta.repo_watermark ALTER COLUMN last_committed_at SET DATA TYPE TEXT")
            except Exception:  # noqa: BLE001
                pass
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS silver.repos (
                  repo_id TEXT,
                  repo_name TEXT,
                  repo_path TEXT,
                  group_id TEXT,
                  raw JSON,
                  ingested_at TIMESTAMPTZ
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS silver.top_contributors (
                  repo_id TEXT,
                  user_id TEXT,
                  username TEXT,
                  contributions BIGINT,
                  raw JSON,
                  ingested_at TIMESTAMPTZ
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS silver.members (
                  repo_id TEXT,
                  user_id TEXT,
                  username TEXT,
                  email TEXT,
                  role TEXT,
                  state TEXT,
                  raw JSON,
                  ingested_at TIMESTAMPTZ
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS silver.commits (
                  repo_id TEXT,
                  sha TEXT,
                  author_id TEXT,
                  author_username TEXT,
                  author_email TEXT,
                  committed_at TIMESTAMPTZ,
                  additions BIGINT,
                  deletions BIGINT,
                  raw JSON,
                  ingested_at TIMESTAMPTZ
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS silver.commit_stats (
                  repo_id TEXT,
                  sha TEXT,
                  base_sha TEXT,
                  additions BIGINT,
                  deletions BIGINT,
                  changed_lines BIGINT,
                  is_merge BOOLEAN,
                  computed_at TIMESTAMPTZ,
                  raw JSON,
                  PRIMARY KEY (repo_id, sha)
                )
                """
            )
            # Migrate older schemas
            try:
                conn.execute("ALTER TABLE silver.members ADD COLUMN IF NOT EXISTS email TEXT")
            except Exception:  # noqa: BLE001
                pass
            try:
                conn.execute("ALTER TABLE silver.commits ADD COLUMN IF NOT EXISTS author_email TEXT")
            except Exception:  # noqa: BLE001
                pass
            # Best-effort migration for commit_stats (older versions may not exist)
            try:
                conn.execute("ALTER TABLE silver.commit_stats ADD COLUMN IF NOT EXISTS base_sha TEXT")
                conn.execute("ALTER TABLE silver.commit_stats ADD COLUMN IF NOT EXISTS additions BIGINT")
                conn.execute("ALTER TABLE silver.commit_stats ADD COLUMN IF NOT EXISTS deletions BIGINT")
                conn.execute("ALTER TABLE silver.commit_stats ADD COLUMN IF NOT EXISTS changed_lines BIGINT")
                conn.execute("ALTER TABLE silver.commit_stats ADD COLUMN IF NOT EXISTS is_merge BOOLEAN")
                conn.execute("ALTER TABLE silver.commit_stats ADD COLUMN IF NOT EXISTS computed_at TIMESTAMPTZ")
                conn.execute("ALTER TABLE silver.commit_stats ADD COLUMN IF NOT EXISTS raw JSON")
            except Exception:  # noqa: BLE001
                pass

    def init_silver(self) -> None:
        self._init_silver()

    def get_repo_watermark(self, repo: str) -> dt.datetime | None:
        self._init_silver()
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT last_committed_at FROM meta.repo_watermark WHERE repo_id = ?",
                [str(repo)],
            ).fetchone()
            if not row:
                return None
            value = row[0]
            if value is None:
                return None
            if isinstance(value, str):
                return _parse_iso_dt(value)
            # Defensive fallback (shouldn't happen with TEXT schema)
            return _parse_datetime_loose(str(value))

    def set_repo_watermark(self, repo: str, last_committed_at: dt.datetime | str | None) -> None:
        self._init_silver()
        if last_committed_at is None:
            return
        if isinstance(last_committed_at, dt.datetime):
            last_committed_at_str = _isoformat_z(last_committed_at)
        else:
            last_committed_at_str = str(last_committed_at)
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO meta.repo_watermark(repo_id, last_committed_at, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (repo_id)
                DO UPDATE SET
                  last_committed_at = EXCLUDED.last_committed_at,
                  updated_at = EXCLUDED.updated_at
                """,
                [str(repo), last_committed_at_str, _utc_now()],
            )

    def update_repo_watermark_from_silver(self, repo: str) -> None:
        self._init_silver()
        # Avoid fetching TIMESTAMPTZ to Python (which can require pytz). Keep this in SQL.
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO meta.repo_watermark(repo_id, last_committed_at, updated_at)
                SELECT
                  ? AS repo_id,
                  CAST(MAX(committed_at) AS VARCHAR) AS last_committed_at,
                  ? AS updated_at
                FROM silver.commits
                WHERE repo_id = ?
                HAVING MAX(committed_at) IS NOT NULL
                ON CONFLICT (repo_id)
                DO UPDATE SET
                  last_committed_at = EXCLUDED.last_committed_at,
                  updated_at = EXCLUDED.updated_at
                """,
                [str(repo), _utc_now(), str(repo)],
            )

    def upsert_silver_repos(self, repos: list[dict[str, Any]], group: str) -> None:
        self._init_silver()
        t = _utc_now()
        rows: list[tuple[Any, ...]] = []
        for r in repos:
            # CNB 的 repo slug/path（通常形如 <org>/<repo>）更适合作为跨表 join 的稳定主键，
            # 同时也是多数 API 的 path 参数形式。
            repo_path = str(
                r.get("path")
                or r.get("fullPath")
                or r.get("full_path")
                or r.get("pathWithNamespace")
                or r.get("path_with_namespace")
                or r.get("fullName")
                or r.get("full_name")
                or ""
            )
            repo_id_raw = str(r.get("id") or r.get("repoId") or "")
            repo_id = repo_path or repo_id_raw or str(r.get("name") or "")
            repo_name = str(r.get("name") or r.get("repoName") or repo_path or repo_id)
            repo_path = repo_path or repo_id
            rows.append((repo_id, repo_name, repo_path, str(group), _json_dumps(r), t))

        with self.db.connect() as conn:
            conn.execute("DELETE FROM silver.repos WHERE group_id = ?", [str(group)])
            if rows:
                conn.executemany(
                    "INSERT INTO silver.repos VALUES (?, ?, ?, ?, CAST(? AS JSON), ?)",
                    rows,
                )
            self._materialize_parquet(conn, "silver.repos", self.silver_dir / "repos.parquet")

    def upsert_silver_top_contributors(self, repo: str, items: list[dict[str, Any]]) -> None:
        self._init_silver()
        t = _utc_now()
        rows: list[tuple[Any, ...]] = []
        for it in items:
            user = it.get("user") if isinstance(it.get("user"), dict) else it
            user_id = str(user.get("id") or user.get("userId") or "")
            username = str(user.get("username") or user.get("name") or user.get("login") or "")
            contributions = int(it.get("contributions") or it.get("count") or it.get("commits") or 0)
            rows.append((str(repo), user_id, username, contributions, _json_dumps(it), t))

        with self.db.connect() as conn:
            conn.execute("DELETE FROM silver.top_contributors WHERE repo_id = ?", [str(repo)])
            if rows:
                conn.executemany(
                    "INSERT INTO silver.top_contributors VALUES (?, ?, ?, ?, CAST(? AS JSON), ?)",
                    rows,
                )
            self._materialize_parquet(
                conn, "silver.top_contributors", self.silver_dir / "top_contributors.parquet"
            )

    def upsert_silver_members(self, repo: str, items: list[dict[str, Any]]) -> None:
        self._init_silver()
        t = _utc_now()
        rows: list[tuple[Any, ...]] = []
        for it in items:
            user_id, username, email = extract_member_identity(it)
            role = str(it.get("role") or it.get("access") or it.get("permission") or "")
            state = str(it.get("state") or it.get("status") or "")
            rows.append((str(repo), user_id, username, email, role, state, _json_dumps(it), t))

        with self.db.connect() as conn:
            conn.execute("DELETE FROM silver.members WHERE repo_id = ?", [str(repo)])
            if rows:
                conn.executemany(
                    """
                    INSERT INTO silver.members(repo_id, user_id, username, email, role, state, raw, ingested_at)
                    VALUES (?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?)
                    """,
                    rows,
                )
            self._materialize_parquet(conn, "silver.members", self.silver_dir / "members.parquet")

    def upsert_silver_commits(self, repo: str, items: list[dict[str, Any]]) -> None:
        self._init_silver()
        t = _utc_now()
        rows: list[tuple[Any, ...]] = []
        for it in items:
            sha = str(it.get("sha") or it.get("id") or it.get("commitId") or "")
            author_id, author_username, author_email = extract_author_identity(it)
            if not author_username and isinstance(it.get("authorName"), str):
                author_username = str(it.get("authorName"))
            company_user = _company_username_from_email(author_email)
            if company_user:
                author_username = company_user

            committed_at_str = extract_committed_at_str(it)

            stats = it.get("stats") if isinstance(it.get("stats"), dict) else {}
            additions = int(stats.get("additions") or it.get("additions") or 0)
            deletions = int(stats.get("deletions") or it.get("deletions") or 0)
            rows.append(
                (
                    str(repo),
                    sha,
                    author_id,
                    author_username,
                    author_email,
                    committed_at_str,
                    additions,
                    deletions,
                    _json_dumps(it),
                    t,
                )
            )

        with self.db.connect() as conn:
            conn.execute("DELETE FROM silver.commits WHERE repo_id = ?", [str(repo)])
            if rows:
                conn.executemany(
                    """
                    INSERT INTO silver.commits(
                      repo_id, sha, author_id, author_username, author_email,
                      committed_at, additions, deletions, raw, ingested_at
                    )
                    VALUES (?, ?, ?, ?, ?, TRY_CAST(? AS TIMESTAMPTZ), ?, ?, CAST(? AS JSON), ?)
                    """,
                    rows,
                )
            self._materialize_parquet(conn, "silver.commits", self.silver_dir / "commits.parquet")
            # 覆盖式刷新成功后，更新 watermark
            self.update_repo_watermark_from_silver(repo)

    def upsert_silver_commits_incremental(self, repo: str, items: list[dict[str, Any]]) -> int:
        """
        增量写入 commits：按 (repo_id, sha) 去重插入；不会清空旧数据。
        返回本次插入的行数（近似：去重后插入的记录数）。
        """
        self._init_silver()
        t = _utc_now()
        rows: list[tuple[Any, ...]] = []
        for it in items:
            sha = str(it.get("sha") or it.get("id") or it.get("commitId") or "")
            if not sha:
                continue
            author_id, author_username, author_email = extract_author_identity(it)
            if not author_username and isinstance(it.get("authorName"), str):
                author_username = str(it.get("authorName"))
            company_user = _company_username_from_email(author_email)
            if company_user:
                author_username = company_user
            committed_at_str = extract_committed_at_str(it)
            stats = it.get("stats") if isinstance(it.get("stats"), dict) else {}
            additions = int(stats.get("additions") or it.get("additions") or 0)
            deletions = int(stats.get("deletions") or it.get("deletions") or 0)
            rows.append(
                (
                    str(repo),
                    sha,
                    author_id,
                    author_username,
                    author_email,
                    committed_at_str,
                    additions,
                    deletions,
                    _json_dumps(it),
                    t,
                )
            )

        if not rows:
            return 0

        with self.db.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE tmp_new_commits (
                  repo_id TEXT,
                  sha TEXT,
                  author_id TEXT,
                  author_username TEXT,
                  author_email TEXT,
                  committed_at_str TEXT,
                  additions BIGINT,
                  deletions BIGINT,
                  raw_str TEXT,
                  ingested_at TIMESTAMPTZ
                )
                """
            )
            conn.executemany(
                "INSERT INTO tmp_new_commits VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

            before = conn.execute("SELECT COUNT(*) FROM silver.commits WHERE repo_id = ?", [str(repo)]).fetchone()[0]
            conn.execute(
                """
                INSERT INTO silver.commits
                SELECT
                  n.repo_id,
                  n.sha,
                  n.author_id,
                  n.author_username,
                  n.author_email,
                  TRY_CAST(n.committed_at_str AS TIMESTAMPTZ),
                  n.additions,
                  n.deletions,
                  CAST(n.raw_str AS JSON),
                  n.ingested_at
                FROM tmp_new_commits n
                LEFT JOIN silver.commits s
                  ON s.repo_id = n.repo_id AND s.sha = n.sha
                WHERE s.sha IS NULL
                """
            )
            after = conn.execute("SELECT COUNT(*) FROM silver.commits WHERE repo_id = ?", [str(repo)]).fetchone()[0]

            # 物化（全表 parquet 仍会重写；后续可改为分区物化）
            self._materialize_parquet(conn, "silver.commits", self.silver_dir / "commits.parquet")

        # 仅在增量写入成功后更新 watermark
        self.update_repo_watermark_from_silver(repo)
        return int(after - before)

    def upsert_silver_commit_stats(
        self,
        rows: list[tuple[str, str, str, int, int, int, bool, dt.datetime, str]],
    ) -> None:
        """
        rows: (repo_id, sha, base_sha, additions, deletions, changed_lines, is_merge, computed_at, raw_json_str)
        """
        if not rows:
            return
        self._init_silver()
        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT INTO silver.commit_stats(
                  repo_id, sha, base_sha, additions, deletions, changed_lines, is_merge, computed_at, raw
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON))
                ON CONFLICT (repo_id, sha)
                DO UPDATE SET
                  base_sha = EXCLUDED.base_sha,
                  additions = EXCLUDED.additions,
                  deletions = EXCLUDED.deletions,
                  changed_lines = EXCLUDED.changed_lines,
                  is_merge = EXCLUDED.is_merge,
                  computed_at = EXCLUDED.computed_at,
                  raw = EXCLUDED.raw
                """,
                rows,
            )
            self._materialize_parquet(conn, "silver.commit_stats", self.silver_dir / "commit_stats.parquet")

    @staticmethod
    def _materialize_parquet(conn, table: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(
            f"""
            COPY (SELECT * FROM {table})
            TO '{str(path).replace("'", "''")}'
            (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
            """
        )
