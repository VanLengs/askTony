from __future__ import annotations

import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import typer
import httpx
from rich.console import Console

from asktony.config import load_config
from asktony.cnb_client import CNBClient
from asktony.lake import Lake

ingest_app = typer.Typer(help="从 CNB OpenAPI 采集数据到数据湖。", add_completion=False)
console = Console()

ApiResult = dict[str, object]

def _repo_key(repo_obj: dict) -> str:
    return str(
        repo_obj.get("path")
        or repo_obj.get("fullPath")
        or repo_obj.get("full_path")
        or repo_obj.get("pathWithNamespace")
        or repo_obj.get("path_with_namespace")
        or repo_obj.get("fullName")
        or repo_obj.get("full_name")
        or repo_obj.get("id")
        or repo_obj.get("repoId")
        or repo_obj.get("name")
        or ""
    )

def _hint_for_status(status_code: int | None) -> str:
    if status_code == 404:
        return (
            "检查点：repo 参数需要是仓库 slug/path（通常 <org>/<repo> 或带子组的 <org>/<sub>/<repo>），"
            "而不是纯数字 id；另外 path 里的 `/` 不能被编码成 %2F。若仅 commits 404，可能是 CNB 的"
            " commits 路由不同（AskTony 已做多路径 fallback），或该仓库为空（没有任何提交），"
            "或该 path 实际不是仓库。"
        )
    if status_code == 401:
        return "检查点：鉴权失败（401），请确认 token、auth-header/auth-prefix 是否正确。"
    if status_code == 403:
        return (
            "检查点：权限不足（403），该账号对该仓库/该接口无权限；请更换具备权限的 token，"
            "或仅采集你有权限的组织/仓库。"
        )
    return "检查点：请确认 token 权限与接口路径是否正确。"


def _safe_api_call(fn, *, label: str, repo: str, verbose: bool = False) -> ApiResult:
    try:
        items = fn()
        return {"ok": True, "status": 200, "items": items}
    except httpx.HTTPStatusError as e:
        status = e.response.status_code if e.response is not None else None
        console.print(f"[yellow]{label} 跳过：repo={repo} HTTP {status}[/yellow]")
        if verbose:
            console.print(f"[yellow]{_hint_for_status(status)}[/yellow]")
        return {"ok": False, "status": status, "error": str(e), "items": None}
    except Exception as e:  # noqa: BLE001
        console.print(f"[yellow]{label} 跳过：repo={repo} error={e}[/yellow]")
        return {"ok": False, "status": None, "error": str(e), "items": None}


def _items_or_none(result: ApiResult) -> list[dict] | None:
    if bool(result.get("ok")):
        items = result.get("items")
        if isinstance(items, list):
            return items
    return None


def _items_or_empty_when_commits_404(result: ApiResult, *, repo_exists_hint: bool) -> list[dict] | None:
    # 某些仓库可能是空仓库（没有任何提交），在 CNB 上可能表现为 commits 接口 404。
    # 如果我们已经能从其他接口（members/top_contributors）拿到 200，则把 commits 视作空列表更合理。
    if bool(result.get("ok")):
        return _items_or_none(result)
    if repo_exists_hint and result.get("status") == 404:
        return []
    return None


def _parse_iso_dt(value: str) -> dt.datetime:
    # Handles ...Z
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value)


def _read_one_jsonl(path: Path) -> dict:
    line = path.read_text(encoding="utf-8").splitlines()[0]
    obj = json.loads(line)
    if not isinstance(obj, dict):
        raise ValueError("Invalid jsonl record")
    return obj


def _extract_repo_and_items(envelope: dict) -> tuple[str | None, list[dict] | None]:
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return None, None
    repo = payload.get("repo")
    if repo is not None:
        repo = str(repo)

    # New format: payload.result = { ok, status, items }
    result = payload.get("result")
    if isinstance(result, dict):
        if bool(result.get("ok")):
            items = result.get("items")
            return repo, items if isinstance(items, list) else None
        return repo, None

    # Old format: payload.items = [...]
    items = payload.get("items")
    if isinstance(items, list):
        return repo, items
    return repo, None


@ingest_app.command("repos")
def ingest_repos() -> None:
    cfg = load_config()
    client = CNBClient.from_config(cfg)
    lake = Lake.from_config(cfg)

    repos = client.get_group_sub_repos(cfg.cnb_group)
    lake.write_bronze("group_sub_repos", {"group": cfg.cnb_group, "repos": repos})
    lake.upsert_silver_repos(repos, group=cfg.cnb_group)
    console.print(f"[green]已采集仓库数：{len(repos)}[/green]")


@ingest_app.command("repo")
def ingest_repo(
    repo: str = typer.Argument(..., help="仓库 slug/path（通常为 <org>/<repo>）"),
    months: int = typer.Option(6, min=1, max=60, help="仅采集最近 N 个月的提交"),
    verbose: bool = typer.Option(False, help="输出更详细的错误提示"),
) -> None:
    cfg = load_config()
    client = CNBClient.from_config(cfg)
    lake = Lake.from_config(cfg)

    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)

    if repo.isdigit():
        console.print(
            "[yellow]提示：CNB 的多数仓库接口参数是 repo 的 slug/path（如 org/repo），"
            "如果你传的是数字 id 可能会 404。[/yellow]"
        )

    top_r = _safe_api_call(lambda: client.top_contributors(repo), label="top_contributors", repo=repo, verbose=verbose)
    members_r = _safe_api_call(lambda: client.list_all_members(repo), label="members", repo=repo, verbose=verbose)
    commits_r = _safe_api_call(lambda: client.list_commits(repo, since=since), label="commits", repo=repo, verbose=verbose)

    lake.write_bronze("top_contributors", {"repo": repo, "result": top_r})
    lake.write_bronze("members", {"repo": repo, "result": members_r})
    lake.write_bronze("commits", {"repo": repo, "result": commits_r})

    top = _items_or_none(top_r)
    members = _items_or_none(members_r)
    repo_exists_hint = top is not None or members is not None
    commits = _items_or_empty_when_commits_404(commits_r, repo_exists_hint=repo_exists_hint)

    # 失败（403/401/404/网络等）时不覆盖 silver，避免把历史数据删掉写空。
    if top is not None:
        lake.upsert_silver_top_contributors(repo, top)
    if members is not None:
        lake.upsert_silver_members(repo, members)
    if commits is not None:
        lake.upsert_silver_commits(repo, commits)

    console.print(
        f"[green]已采集 repo={repo}："
        f"contributors={len(top or [])} members={len(members or [])} commits={len(commits or [])}[/green]"
    )


@ingest_app.command("all")
def ingest_all(
    months: int = typer.Option(6, min=1, max=60, help="仅采集最近 N 个月的提交"),
    limit: int | None = typer.Option(None, min=1, help="限制采集仓库数（调试用）"),
    verbose: bool = typer.Option(False, help="输出更详细的错误提示"),
) -> None:
    cfg = load_config()
    client = CNBClient.from_config(cfg)
    lake = Lake.from_config(cfg)

    repos = client.get_group_sub_repos(cfg.cnb_group)
    if limit is not None:
        repos = repos[:limit]

    lake.write_bronze("group_sub_repos", {"group": cfg.cnb_group, "repos": repos})
    lake.upsert_silver_repos(repos, group=cfg.cnb_group)

    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
    ok = 0
    for r in repos:
        repo_id = _repo_key(r)
        if not repo_id:
            continue

        top_r = _safe_api_call(
            lambda: client.top_contributors(repo_id), label="top_contributors", repo=repo_id, verbose=verbose
        )
        members_r = _safe_api_call(
            lambda: client.list_all_members(repo_id), label="members", repo=repo_id, verbose=verbose
        )
        commits_r = _safe_api_call(
            lambda: client.list_commits(repo_id, since=since), label="commits", repo=repo_id, verbose=verbose
        )

        lake.write_bronze("top_contributors", {"repo": repo_id, "result": top_r})
        lake.write_bronze("members", {"repo": repo_id, "result": members_r})
        lake.write_bronze("commits", {"repo": repo_id, "result": commits_r})

        top = _items_or_none(top_r)
        members = _items_or_none(members_r)
        repo_exists_hint = top is not None or members is not None
        commits = _items_or_empty_when_commits_404(commits_r, repo_exists_hint=repo_exists_hint)

        if top is not None:
            lake.upsert_silver_top_contributors(repo_id, top)
        if members is not None:
            lake.upsert_silver_members(repo_id, members)
        if commits is not None:
            lake.upsert_silver_commits(repo_id, commits)

        ok += 1

    console.print(f"[green]已完成采集仓库：{ok}/{len(repos)}[/green]")


@ingest_app.command("incremental")
def ingest_incremental(
    overlap_days: int = typer.Option(1, min=0, max=30, help="回看 N 天，避免边界漏数（会自动去重）"),
    bootstrap_months: int = typer.Option(2, min=1, max=60, help="首次无 watermark 时回溯 N 个月"),
    limit: int | None = typer.Option(None, min=1, help="限制采集仓库数（调试用）"),
    verbose: bool = typer.Option(False, help="输出更详细的错误提示"),
) -> None:
    """
    按仓库 watermark 增量采集 commits（不定时执行也可），并写入 silver.commits。
    """
    cfg = load_config()
    client = CNBClient.from_config(cfg)
    lake = Lake.from_config(cfg)

    repos = client.get_group_sub_repos(cfg.cnb_group)
    if limit is not None:
        repos = repos[:limit]
    lake.upsert_silver_repos(repos, group=cfg.cnb_group)

    now = dt.datetime.now(dt.timezone.utc)
    bootstrap_since = now - dt.timedelta(days=30 * bootstrap_months)
    overlap = dt.timedelta(days=overlap_days)

    total_inserted = 0
    processed = 0
    for r in repos:
        repo_id = _repo_key(r)
        if not repo_id:
            continue

        wm = lake.get_repo_watermark(repo_id)
        since = (wm - overlap) if wm is not None else bootstrap_since

        commits_r = _safe_api_call(
            lambda: client.list_commits(repo_id, since=since),
            label="commits",
            repo=repo_id,
            verbose=verbose,
        )
        lake.write_bronze("commits", {"repo": repo_id, "result": commits_r})
        commits = _items_or_none(commits_r)
        if commits is None:
            continue

        inserted = lake.upsert_silver_commits_incremental(repo_id, commits)
        total_inserted += inserted
        processed += 1

    console.print(f"[green]增量采集完成：repos={processed} inserted_commits≈{total_inserted}[/green]")


def _extract_first_parent_sha(raw_str: str) -> tuple[str, bool]:
    try:
        obj = json.loads(raw_str) if raw_str else {}
    except Exception:  # noqa: BLE001
        return "", False
    parents = obj.get("parents")
    if not isinstance(parents, list) or not parents:
        return "", False
    is_merge = len(parents) > 1
    first = parents[0]
    if isinstance(first, dict):
        return str(first.get("sha") or first.get("id") or ""), is_merge
    if isinstance(first, str):
        return first, is_merge
    return "", is_merge


def _sum_add_del_from_compare(resp: object) -> tuple[int, int, int]:
    if not isinstance(resp, dict):
        return 0, 0, 0
    files = resp.get("files") or resp.get("diffs") or resp.get("changes") or []
    if not isinstance(files, list):
        return 0, 0, 0
    additions = 0
    deletions = 0
    for f in files:
        if not isinstance(f, dict):
            continue
        try:
            additions += int(f.get("additions") or 0)
        except Exception:  # noqa: BLE001
            pass
        try:
            deletions += int(f.get("deletions") or 0)
        except Exception:  # noqa: BLE001
            pass
    return additions, deletions, additions + deletions


@ingest_app.command("enrich-commit-stats")
def enrich_commit_stats(
    months: int = typer.Option(2, min=1, max=60, help="仅回填最近 N 个月的提交"),
    repo: list[str] = typer.Option([], "--repo", help="仅处理指定 repo（可重复传入）"),
    max_commits: int = typer.Option(5000, min=1, help="最多回填 N 条 commit（避免误操作）"),
    concurrency: int = typer.Option(4, min=1, max=16, help="并发请求数（注意 API 限流）"),
    force: bool = typer.Option(False, help="即使已存在 commit_stats 也重新计算（仍受 max_commits 限制）"),
    dry_run: bool = typer.Option(False, help="只统计将会回填多少，不写入数据库"),
    verbose: bool = typer.Option(False, help="输出更详细的错误提示"),
) -> None:
    """
    回填每个 commit 的 additions/deletions/changed_lines。
    CNB 的 commits 列表接口不包含 stats，因此需要用 compare(base...head) 计算。
    """
    cfg = load_config()
    client = CNBClient.from_config(cfg)
    lake = Lake.from_config(cfg)
    lake.init_silver()

    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)

    repo_filter_sql = ""
    repo_args: list[object] = []
    if repo:
        repo_filter_sql = f" AND c.repo_id IN ({','.join(['?'] * len(repo))})"
        repo_args = [str(r) for r in repo]

    with lake.db.connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              c.repo_id,
              c.sha,
              CAST(c.raw AS VARCHAR) AS raw_str
            FROM silver.commits c
            LEFT JOIN silver.commit_stats s
              ON s.repo_id = c.repo_id AND s.sha = c.sha
            WHERE c.repo_id IS NOT NULL AND c.repo_id <> ''
              AND c.sha IS NOT NULL AND c.sha <> ''
              AND c.committed_at IS NOT NULL
              AND c.committed_at >= ?
              AND ({'TRUE' if force else 's.sha IS NULL'})
              {repo_filter_sql}
            ORDER BY c.committed_at DESC
            LIMIT ?
            """,
            [since, *repo_args, int(max_commits)],
        ).fetchall()

    candidates: list[tuple[str, str, str, bool]] = []
    skipped_no_parent = 0
    for repo_id, sha, raw_str in rows:
        base_sha, is_merge = _extract_first_parent_sha(str(raw_str or ""))
        if not base_sha:
            skipped_no_parent += 1
            continue
        candidates.append((str(repo_id), str(sha), base_sha, bool(is_merge)))

    out = {
        "months": months,
        "since_ts": since.strftime("%Y-%m-%d"),
        "max_commits": max_commits,
        "selected_rows": len(rows),
        "candidates": len(candidates),
        "skipped_no_parent": skipped_no_parent,
        "dry_run": dry_run,
        "force": force,
    }

    if dry_run or not candidates:
        console.print(out)
        return

    ok = 0
    failed_http = 0
    failed_other = 0
    merge_commits = 0
    batch: list[tuple[str, str, str, int, int, int, bool, dt.datetime, str]] = []
    computed_at = dt.datetime.now(dt.timezone.utc)

    def _compute_one(repo_id: str, sha: str, base_sha: str, is_merge: bool) -> tuple[str, str, str, int, int, int, bool, str]:
        resp = client.compare_commits(repo_id, base_sha, sha)
        additions, deletions, changed_lines = _sum_add_del_from_compare(resp)
        files = resp.get("files") if isinstance(resp, dict) else None
        raw_small = json.dumps(
            {"source": "compare", "file_count": len(files) if isinstance(files, list) else None},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return repo_id, sha, base_sha, additions, deletions, changed_lines, is_merge, raw_small

    with ThreadPoolExecutor(max_workers=int(concurrency)) as ex:
        futs = [
            ex.submit(_compute_one, repo_id, sha, base_sha, is_merge) for (repo_id, sha, base_sha, is_merge) in candidates
        ]
        for fut in as_completed(futs):
            try:
                repo_id, sha, base_sha, additions, deletions, changed_lines, is_merge, raw_small = fut.result()
                if is_merge:
                    merge_commits += 1
                batch.append(
                    (
                        repo_id,
                        sha,
                        base_sha,
                        int(additions),
                        int(deletions),
                        int(changed_lines),
                        bool(is_merge),
                        computed_at,
                        raw_small,
                    )
                )
                ok += 1
            except httpx.HTTPStatusError as e:
                failed_http += 1
                status = e.response.status_code if e.response is not None else None
                console.print(f"[yellow]compare 跳过：HTTP {status}[/yellow]")
                if verbose:
                    console.print(f"[yellow]{_hint_for_status(status)}[/yellow]")
            except Exception as e:  # noqa: BLE001
                failed_other += 1
                console.print(f"[yellow]compare 跳过：error={e}[/yellow]")

            if len(batch) >= 500:
                lake.upsert_silver_commit_stats(batch)
                batch = []

    if batch:
        lake.upsert_silver_commit_stats(batch)

    out.update(
        {
            "ok": ok,
            "failed_http": failed_http,
            "failed_other": failed_other,
            "merge_commits": merge_commits,
        }
    )
    console.print(out)


@ingest_app.command("rebuild-silver-commits")
def rebuild_silver_commits(
    latest_only: bool = typer.Option(True, help="同一 repo 仅使用最新一份 bronze 快照"),
    truncate: bool = typer.Option(True, help="重建前清空 silver.commits（推荐）"),
    batch_size: int = typer.Option(5000, min=500, max=100_000, help="批量写入行数"),
) -> None:
    """
    使用 bronze/commits 的已落地数据重建 silver.commits，并生成 silver/commits.parquet。
    """
    cfg = load_config()
    lake = Lake.from_config(cfg)
    bronze_dir = lake.bronze_dir / "commits"
    if not bronze_dir.exists():
        raise SystemExit(f"未找到 bronze commits 目录：{bronze_dir}")

    paths = sorted(bronze_dir.glob("*.jsonl"))
    if not paths:
        raise SystemExit(f"未找到任何 bronze commits 文件：{bronze_dir}")

    # Pass 1: pick latest file per repo (or keep all)
    selected: list[Path] = []
    if latest_only:
        latest_by_repo: dict[str, tuple[dt.datetime, Path]] = {}
        for p in paths:
            env = _read_one_jsonl(p)
            ingested_at = env.get("ingested_at")
            if not isinstance(ingested_at, str):
                continue
            payload = env.get("payload")
            if not isinstance(payload, dict):
                continue
            repo = payload.get("repo")
            if repo is None:
                continue
            repo = str(repo)
            ts = _parse_iso_dt(ingested_at)
            prev = latest_by_repo.get(repo)
            if prev is None or ts > prev[0]:
                latest_by_repo[repo] = (ts, p)
        selected = [p for _, p in latest_by_repo.values()]
    else:
        selected = paths

    selected.sort()
    console.print(f"[cyan]准备重建 silver.commits：bronze 文件数={len(selected)}[/cyan]")

    lake._init_silver()  # noqa: SLF001
    import datetime as _dt  # local alias to avoid shadowing
    from asktony.lake import _json_dumps as _json_dumps  # noqa: PLC0415
    from asktony.lake import extract_committed_at_str as _extract_committed_at_str  # noqa: PLC0415
    from asktony.lake import extract_author_identity as _extract_author_identity  # noqa: PLC0415

    def commit_rows(repo_id: str, items: list[dict], ingested_at: dt.datetime) -> list[tuple]:
        rows: list[tuple] = []
        for it in items:
            sha = str(it.get("sha") or it.get("id") or it.get("commitId") or "")
            if not sha:
                continue
            author_id, author_username, author_email = _extract_author_identity(it)
            if not author_username and isinstance(it.get("authorName"), str):
                author_username = str(it.get("authorName"))
            committed_at_str = _extract_committed_at_str(it)
            stats = it.get("stats") if isinstance(it.get("stats"), dict) else {}
            additions = int(stats.get("additions") or it.get("additions") or 0)
            deletions = int(stats.get("deletions") or it.get("deletions") or 0)
            rows.append(
                (
                    repo_id,
                    sha,
                    author_id,
                    author_username,
                    author_email,
                    committed_at_str,
                    additions,
                    deletions,
                    _json_dumps(it),
                    ingested_at,
                )
            )
        return rows

    inserted = 0
    with lake.db.connect() as conn:
        if truncate:
            conn.execute("DELETE FROM silver.commits")

        buf: list[tuple] = []
        for p in selected:
            env = _read_one_jsonl(p)
            ingested_at_raw = env.get("ingested_at")
            ingested_at = _parse_iso_dt(ingested_at_raw) if isinstance(ingested_at_raw, str) else _dt.datetime.now(_dt.timezone.utc)
            repo, items = _extract_repo_and_items(env)
            if not repo or not items:
                continue

            if not truncate:
                conn.execute("DELETE FROM silver.commits WHERE repo_id = ?", [repo])

            buf.extend(commit_rows(repo, items, ingested_at))
            if len(buf) >= batch_size:
                conn.executemany(
                    """
                    INSERT INTO silver.commits(
                      repo_id, sha, author_id, author_username, author_email,
                      committed_at, additions, deletions, raw, ingested_at
                    )
                    VALUES (?, ?, ?, ?, ?, TRY_CAST(? AS TIMESTAMPTZ), ?, ?, CAST(? AS JSON), ?)
                    """,
                    buf,
                )
                inserted += len(buf)
                buf.clear()

        if buf:
            conn.executemany(
                """
                INSERT INTO silver.commits(
                  repo_id, sha, author_id, author_username, author_email,
                  committed_at, additions, deletions, raw, ingested_at
                )
                VALUES (?, ?, ?, ?, ?, TRY_CAST(? AS TIMESTAMPTZ), ?, ?, CAST(? AS JSON), ?)
                """,
                buf,
            )
            inserted += len(buf)

        lake._materialize_parquet(conn, "silver.commits", lake.silver_dir / "commits.parquet")  # noqa: SLF001

    # 从重建后的 silver 表刷新所有 repo watermark
    with lake.db.connect() as conn:
        repos = [r[0] for r in conn.execute("SELECT DISTINCT repo_id FROM silver.commits").fetchall()]
    for repo in repos:
        lake.update_repo_watermark_from_silver(repo)

    console.print(f"[green]已重建 silver.commits，并生成 commits.parquet；写入行数={inserted}[/green]")


@ingest_app.command("status")
def ingest_status() -> None:
    """
    查看 silver 层表行数与 parquet 文件大小，用于排查物化是否生效。
    """
    cfg = load_config()
    lake = Lake.from_config(cfg)

    def _fmt_size(p: Path) -> str:
        if not p.exists():
            return "(missing)"
        n = p.stat().st_size
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if n < 1024 or unit == "TB":
                return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
            n /= 1024
        return f"{n:.1f}TB"

    with lake.db.connect() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        rows = conn.execute(
            """
            SELECT 'silver.repos' AS t, COUNT(*) AS n FROM silver.repos
            UNION ALL SELECT 'silver.members' AS t, COUNT(*) AS n FROM silver.members
            UNION ALL SELECT 'silver.top_contributors' AS t, COUNT(*) AS n FROM silver.top_contributors
            UNION ALL SELECT 'silver.commits' AS t, COUNT(*) AS n FROM silver.commits
            ORDER BY t
            """
        ).fetchall()

    console.print(
        {
            "parquet": {
                "repos": {"path": str(lake.silver_dir / "repos.parquet"), "size": _fmt_size(lake.silver_dir / "repos.parquet")},
                "members": {
                    "path": str(lake.silver_dir / "members.parquet"),
                    "size": _fmt_size(lake.silver_dir / "members.parquet"),
                },
                "top_contributors": {
                    "path": str(lake.silver_dir / "top_contributors.parquet"),
                    "size": _fmt_size(lake.silver_dir / "top_contributors.parquet"),
                },
                "commits": {
                    "path": str(lake.silver_dir / "commits.parquet"),
                    "size": _fmt_size(lake.silver_dir / "commits.parquet"),
                },
            },
            "tables": {t: int(n) for (t, n) in rows},
        }
    )


@ingest_app.command("prune-bronze-commits")
def prune_bronze_commits(
    keep_days: int = typer.Option(30, min=0, max=3650, help="保留最近 N 天的 bronze commits 文件"),
    keep_latest_per_repo: bool = typer.Option(True, help="每个 repo 额外保留最新一份快照"),
    archive_dir: str | None = typer.Option(
        None,
        help="将要删除的文件移动到该目录（可用于归档）；不填则直接删除",
    ),
    yes: bool = typer.Option(False, "--yes", help="不提示确认，直接执行"),
    dry_run: bool = typer.Option(False, help="只展示将删除/归档的文件数量，不实际操作"),
) -> None:
    """
    清理 bronze/commits 下的历史 jsonl 文件，避免长期运行导致文件数量过多。

    建议策略：保留最近 N 天 + 每仓库最新 1 份，其余归档或删除。
    """
    cfg = load_config()
    lake = Lake.from_config(cfg)
    bronze_dir = lake.bronze_dir / "commits"
    if not bronze_dir.exists():
        raise SystemExit(f"未找到目录：{bronze_dir}")

    paths = sorted(bronze_dir.glob("*.jsonl"))
    if not paths:
        console.print(f"[yellow]未找到文件：{bronze_dir}[/yellow]")
        return

    now = dt.datetime.now(dt.timezone.utc)
    keep_cutoff = now - dt.timedelta(days=keep_days)

    latest_by_repo: dict[str, tuple[dt.datetime, Path]] = {}
    keep_set: set[Path] = set()

    for p in paths:
        try:
            env = _read_one_jsonl(p)
        except Exception:  # noqa: BLE001
            # 无法解析的文件默认保留，避免误删
            keep_set.add(p)
            continue

        ingested_at_raw = env.get("ingested_at")
        ingested_at = None
        if isinstance(ingested_at_raw, str):
            try:
                ingested_at = _parse_iso_dt(ingested_at_raw)
            except Exception:  # noqa: BLE001
                ingested_at = None

        repo, _items = _extract_repo_and_items(env)
        if ingested_at is not None and ingested_at >= keep_cutoff:
            keep_set.add(p)

        if keep_latest_per_repo and repo and ingested_at is not None:
            prev = latest_by_repo.get(repo)
            if prev is None or ingested_at > prev[0]:
                latest_by_repo[repo] = (ingested_at, p)

    if keep_latest_per_repo:
        keep_set.update([p for _, p in latest_by_repo.values()])

    to_remove = [p for p in paths if p not in keep_set]
    if not to_remove:
        console.print("[green]无需清理：没有可删除/归档的 bronze commits 文件。[/green]")
        return

    archive_path = Path(archive_dir).expanduser() if archive_dir else None
    action = "归档" if archive_path else "删除"
    console.print(
        f"[cyan]bronze commits 文件总数={len(paths)}，保留={len(keep_set)}，将{action}={len(to_remove)}[/cyan]"
    )

    if dry_run:
        return

    if not yes:
        if not typer.confirm(f"确认要{action}这 {len(to_remove)} 个文件吗？"):
            console.print("[yellow]已取消。[/yellow]")
            return

    if archive_path:
        archive_path.mkdir(parents=True, exist_ok=True)
        moved = 0
        for p in to_remove:
            # 确保只操作 bronze_dir 下的文件
            p_rel = p.relative_to(bronze_dir)
            target = archive_path / p_rel.name
            p.rename(target)
            moved += 1
        console.print(f"[green]已归档文件数：{moved} -> {archive_path}[/green]")
    else:
        deleted = 0
        for p in to_remove:
            p.unlink(missing_ok=True)
            deleted += 1
        console.print(f"[green]已删除文件数：{deleted}[/green]")
