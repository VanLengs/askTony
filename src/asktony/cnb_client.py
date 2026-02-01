from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from asktony.config import AskTonyConfig


def _to_iso8601(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class CNBClient:
    base_url: str
    username: str
    token: str
    auth_header: str = "Authorization"
    auth_prefix: str = "Bearer"
    timeout_s: float = 30.0

    @classmethod
    def from_config(cls, cfg: AskTonyConfig) -> "CNBClient":
        return cls(
            base_url=cfg.cnb_base_url,
            username=cfg.cnb_username,
            token=cfg.cnb_token,
            auth_header=cfg.cnb_auth_header,
            auth_prefix=cfg.cnb_auth_prefix,
        )

    def _headers(self) -> dict[str, str]:
        # CNB 的具体鉴权 header 形式可能因 token 类型而不同，因此提供可配置的 header/prefix。
        value = self.token if not self.auth_prefix else f"{self.auth_prefix} {self.token}"
        return {
            self.auth_header: value,
            "Accept": "application/json",
            "User-Agent": "AskTony/0.1",
        }

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, headers=self._headers(), timeout=self.timeout_s)

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        with self._client() as c:
            r = c.get(path, params=params)
            r.raise_for_status()
            return r.json()

    def _paged_list(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = dict(params or {})
        page = int(params.pop("page", 1))
        # CNB OpenAPI 常见字段为 page_size；兼容 per_page
        page_size = int(params.pop("page_size", params.pop("per_page", 100)))
        out: list[dict[str, Any]] = []

        while True:
            resp = self._get_json(path, params={**params, "page": page, "page_size": page_size})
            items = resp
            if isinstance(resp, dict):
                # 兼容 { items: [...] } / { data: [...] } 等风格
                items = resp.get("items") or resp.get("data") or resp.get("list") or []

            if not isinstance(items, list):
                raise ValueError(f"Unexpected paging response: {type(resp)}")

            out.extend(items)
            if len(items) < page_size:
                break
            page += 1

        return out

    def _paged_list_fallback(
        self,
        paths: list[str],
        *,
        params: dict[str, Any] | None = None,
        fallback_on_status: set[int] | None = None,
    ) -> list[dict[str, Any]]:
        fallback_on_status = fallback_on_status or {404}
        last_exc: Exception | None = None
        for p in paths:
            try:
                return self._paged_list(p, params=params)
            except httpx.HTTPStatusError as e:
                last_exc = e
                if e.response is not None and e.response.status_code in fallback_on_status:
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        return []

    # 3.1 组织下可见仓库
    def get_group_sub_repos(self, group: str) -> list[dict[str, Any]]:
        # 参考：GetGroupSubRepos => GET /{slug}/-/repos
        # 注意：repo/slug 参数本身可能包含 `/`（如 clife/old-xxx），这里必须保留 `/`，
        # 否则编码成 %2F 可能导致路由 404。
        slug_q = quote(group, safe="/")
        return self._paged_list(f"/{slug_q}/-/repos")

    # 3.2 每个仓库的 top 活跃用户
    def top_contributors(self, repo: str) -> list[dict[str, Any]]:
        # 参考：TopContributors => GET /{repo}/-/top-activity-users
        repo_q = quote(repo, safe="/")
        resp = self._get_json(f"/{repo_q}/-/top-activity-users")
        if isinstance(resp, list):
            return resp
        if isinstance(resp, dict):
            items = resp.get("items") or resp.get("data") or resp.get("list") or []
            return items if isinstance(items, list) else []
        return []

    # 3.3 每个仓库的有效成员列表
    def list_all_members(self, repo: str) -> list[dict[str, Any]]:
        # 参考：ListAllMembers => GET /{slug}/-/list-members
        slug_q = quote(repo, safe="/")
        return self._paged_list(f"/{slug_q}/-/list-members")

    # 3.4 每个仓库的 commit 列表
    def list_commits(self, repo: str, since: dt.datetime | None = None) -> list[dict[str, Any]]:
        # 参考：ListCommits => GET /{repo}/-/git/commits
        repo_q = quote(repo, safe="/")
        params: dict[str, Any] = {}
        if since is not None:
            params["since"] = _to_iso8601(since)
        # CNB 不同版本/路由可能存在多个 commits 路径，404 时尝试 fallback。
        return self._paged_list_fallback(
            [
                f"/{repo_q}/-/git/commits",
                f"/{repo_q}/-/commits",
                f"/{repo_q}/-/repository/commits",
                f"/repos/{repo_q}/commits",
            ],
            params=params,
            fallback_on_status={404},
        )

    # 3.5 Compare commits（用于获取每个 commit 的增删行）
    def compare_commits(self, repo: str, base: str, head: str) -> dict[str, Any]:
        # 参考：Compare => GET /{repo}/-/git/compare/{base_head}
        # base_head 形如 "<base>...<head>"，其中 "..." 和 "." 必须保留不编码。
        repo_q = quote(repo, safe="/")
        base_head_q = quote(f"{base}...{head}", safe="/.")
        resp = self._get_json(f"/{repo_q}/-/git/compare/{base_head_q}")
        return resp if isinstance(resp, dict) else {"data": resp}
