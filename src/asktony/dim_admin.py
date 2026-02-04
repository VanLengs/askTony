from __future__ import annotations

import csv
import datetime as dt
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.table import Table

from asktony.db import DB


ROLE_OPTIONS = [
    "Java 后台开发",
    "Web 前端开发",
    "终端开发",
    "算法开发",
    "数据开发",
    "全栈开发",
    "产测运项管",
    "管理层",
    "其他",
]

# 变更量（changed_lines）按角色加权，用于跨角色对比时减少“语言/表达方式”带来的偏差。
# 未显式列出的角色默认权重为 1.0。
ROLE_CHANGE_WEIGHTS: dict[str, float] = {
    "管理层": 1.9,
    "数据开发": 1.8,
    "算法开发": 1.5,
    "全栈开发": 1.2,
    "Java 后台开发": 1.1,
    "Web 前端开发": 1.0,
    "终端开发": 1.0,
}


def role_options_cell() -> str:
    # CSV 本身无法做“下拉”，这里提供一个可用于 Excel/Sheets 数据验证的候选列表字符串。
    # 用户可把这一列复制到一个单独区域并设置 Data Validation -> List。
    return " | ".join(ROLE_OPTIONS)


def _stable_id(prefix: str, *parts: str, length: int = 12) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(p.strip().lower().encode("utf-8"))
        h.update(b"\x1f")
    return f"{prefix}_{h.hexdigest()[:length]}"


def _norm_key(value: str) -> str:
    return value.strip().lower()

def _split_identities(value: str) -> list[str]:
    """
    Split a user-provided identity field (username/email) into candidates.
    Supports common separators used in Excel exports.
    """
    raw = (value or "").strip()
    if not raw:
        return []
    for sep in [";", "；", ",", "，", "|", "、", " "]:
        raw = raw.replace(sep, ";")
    return [x.strip() for x in raw.split(";") if x.strip()]

def _is_company_email(value: str) -> bool:
    v = (value or "").strip().lower()
    if not v:
        return False
    # Corporate email policy:
    # - Regular employee: aa.bb@clife.cn
    # - Contractor: 801495@clife.cn (numeric local-part)
    return bool(re.fullmatch(r"(?:[a-z0-9]+\.[a-z0-9]+|[0-9]+)@clife\.cn", v))


def _company_username_from_email(value: str) -> str | None:
    v = (value or "").strip().lower()
    if not _is_company_email(v):
        return None
    local = v.split("@", 1)[0]
    if re.fullmatch(r"[0-9]+", local):
        return f"partner-{local}"
    return local


# 允许的岗位/角色（不区分大小写）
ALLOWED_ROLES = {_norm_key(x) for x in ROLE_OPTIONS}


@dataclass(frozen=True)
class ImportIssue:
    file: str
    row: int
    key: str
    field: str
    message: str


class DimAdmin:
    def __init__(self, db: DB) -> None:
        self.db = db

    def ensure_schema(self) -> None:
        with self.db.connect() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.dim_department_level2 (
                  department_level2_id TEXT PRIMARY KEY,
                  name TEXT UNIQUE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.dim_department_level3 (
                  department_level3_id TEXT PRIMARY KEY,
                  department_level2_id TEXT,
                  name TEXT,
                  UNIQUE(department_level2_id, name)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.dim_member_enrichment (
                  member_key TEXT PRIMARY KEY,
                  full_name TEXT,
                  department_level2_id TEXT,
                  department_level3_id TEXT,
                  role TEXT,
                  employee_id TEXT,
                  employee_type TEXT,
                  department_level1_name TEXT,
                  position TEXT,
                  in_date TEXT,
                  gender TEXT,
                  age BIGINT,
                  years_of_service DOUBLE,
                  job_sequence TEXT,
                  job_rank TEXT,
                  line_manager TEXT,
                  education_level TEXT,
                  collodge TEXT,
                  major TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.bridge_employee_identity (
                  kind TEXT,
                  identity_l TEXT,
                  employee_id TEXT,
                  source TEXT,
                  updated_at TIMESTAMPTZ,
                  PRIMARY KEY(kind, identity_l)
                )
                """
            )
            # Backward compatible schema evolution (existing installs may have fewer columns).
            for ddl in [
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS employee_id TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS employee_type TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS department_level1_name TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS position TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS in_date TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS gender TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS age BIGINT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS years_of_service DOUBLE",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS job_sequence TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS job_rank TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS line_manager TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS education_level TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS collodge TEXT",
                "ALTER TABLE gold.dim_member_enrichment ADD COLUMN IF NOT EXISTS major TEXT",
            ]:
                conn.execute(ddl)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.dim_repo_enrichment (
                  repo_id TEXT PRIMARY KEY,
                  department_level2_id TEXT,
                  department_level3_id TEXT
                )
                """
            )

    def export_member_template(self, output: Path, *, blank: bool = False) -> None:
        self.ensure_schema()
        output = output.expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        with self.db.connect() as conn:
            # Requires model build to have created gold.dim_member / gold.dim_repo views.
            if blank:
                rows = conn.execute(
                    """
                    SELECT
                      m.email,
                      '' AS email_aliases,
                      '' AS full_name,
                      '' AS department_level2_id,
                      '' AS department_level2_name,
                      '' AS department_level3_id,
                      '' AS department_level3_name,
                      '' AS role,
                      '' AS employee_id,
                      '' AS employee_type,
                      '' AS department_level1_name,
                      '' AS position,
                      '' AS in_date,
                      '' AS gender,
                      NULL::BIGINT AS age,
                      NULL::DOUBLE AS years_of_service,
                      '' AS job_sequence,
                      '' AS job_rank,
                      '' AS line_manager,
                      '' AS education_level,
                      '' AS collodge,
                      '' AS major
                    FROM gold.dim_member m
                    ORDER BY m.member_key
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                      m.email,
                      '' AS email_aliases,
                      e.full_name,
                      e.department_level2_id,
                      d2.name AS department_level2_name,
                      e.department_level3_id,
                      d3.name AS department_level3_name,
                      e.role,
                      e.employee_id,
                      e.employee_type,
                      e.department_level1_name,
                      e.position,
                      e.in_date,
                      e.gender,
                      e.age,
                      e.years_of_service,
                      e.job_sequence,
                      e.job_rank,
                      e.line_manager,
                      e.education_level,
                      e.collodge,
                      e.major
                    FROM gold.dim_member m
                    LEFT JOIN gold.dim_member_enrichment e ON e.member_key = m.member_key
                    LEFT JOIN gold.dim_department_level2 d2 ON d2.department_level2_id = e.department_level2_id
                    LEFT JOIN gold.dim_department_level3 d3 ON d3.department_level3_id = e.department_level3_id
                    ORDER BY m.member_key
                    """
                ).fetchall()

        header = [
            "email",
            "email_aliases",
            "full_name",
            "department_level2_id",
            "department_level2_name",
            "department_level3_id",
            "department_level3_name",
            "role",
            "employee_id",
            "employee_type",
            "department_level1_name",
            "position",
            "in_date",
            "gender",
            "age",
            "years_of_service",
            "job_sequence",
            "job_rank",
            "line_manager",
            "education_level",
            "collodge",
            "major",
            "role_options",
            "member_key_rule",
            "fill_guide",
        ]
        member_key_rule = "member_key = lower(coalesce(username, email, user_id))"
        fill_guide = (
            "无需填写 member_key/username；仅填写 employee_id/full_name/部门/role 及 email/email_aliases。"
            "import-dim-info 会基于 email/email_aliases 自动匹配或生成 member_key，并把提交统一映射到 employee_id。"
        )
        with output.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            for r in rows:
                w.writerow(
                    ["" if v is None else str(v) for v in r] + [role_options_cell(), member_key_rule, fill_guide]
                )

    def export_repo_template(
        self,
        output: Path,
        *,
        blank: bool = False,
        active_only: bool = True,
        months: int = 2,
    ) -> None:
        self.ensure_schema()
        output = output.expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        with self.db.connect() as conn:
            where_active = ""
            params: list[object] = []
            if active_only:
                since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
                since_ts = since_dt.date().isoformat()
                since_month = since_dt.strftime("%Y-%m")
                where_active = """
                WHERE r.repo_id IN (
                  SELECT DISTINCT repo_id
                  FROM gold.fact_commit
                  WHERE commit_month >= ? AND committed_at >= ?::TIMESTAMPTZ
                )
                """
                params = [since_month, since_ts]

            if blank:
                rows = conn.execute(
                    f"""
                    SELECT
                      r.repo_id,
                      r.repo_name,
                      r.repo_path,
                      '' AS department_level2_id,
                      '' AS department_level2_name,
                      '' AS department_level3_id,
                      '' AS department_level3_name
                    FROM gold.dim_repo r
                    {where_active}
                    ORDER BY r.repo_id
                    """,
                    params,
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT
                      r.repo_id,
                      r.repo_name,
                      r.repo_path,
                      e.department_level2_id,
                      d2.name AS department_level2_name,
                      e.department_level3_id,
                      d3.name AS department_level3_name
                    FROM gold.dim_repo r
                    LEFT JOIN gold.dim_repo_enrichment e ON e.repo_id = r.repo_id
                    LEFT JOIN gold.dim_department_level2 d2 ON d2.department_level2_id = e.department_level2_id
                    LEFT JOIN gold.dim_department_level3 d3 ON d3.department_level3_id = e.department_level3_id
                    {where_active}
                    ORDER BY r.repo_id
                    """,
                    params,
                ).fetchall()

        header = [
            "repo_id",
            "repo_name",
            "repo_path",
            "department_level2_id",
            "department_level2_name",
            "department_level3_id",
            "department_level3_name",
        ]
        with output.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            for r in rows:
                w.writerow(["" if v is None else str(v) for v in r])

    def import_dim_info(
        self,
        *,
        member_file: Path | None,
        repo_file: Path | None,
        auto_create_departments: bool = True,
        dry_run: bool = False,
    ) -> tuple[list[ImportIssue], dict[str, int]]:
        self.ensure_schema()
        issues: list[ImportIssue] = []
        stats = {
            "departments_level2_upserted": 0,
            "departments_level3_upserted": 0,
            "member_rows_upserted": 0,
            "member_rows_resolved_by_email": 0,
            "member_rows_unresolved_identity": 0,
            "member_rows_created_dummy_member_key": 0,
            "member_rows_migrated_member_key": 0,
            "member_rows_skipped_missing_member_key": 0,
            "member_rows_dummy_member_key": 0,
            "employee_identity_rows_upserted": 0,
            "repo_rows_upserted": 0,
        }

        dept2_to_id: dict[str, str] = {}
        dept3_to_id: dict[tuple[str, str], str] = {}
        email_to_member_key: dict[str, str] = {}
        existing_identity_to_employee: dict[tuple[str, str], str] = {}
        employee_id_to_member_key: dict[str, str] = {}
        member_key_to_employee_id: dict[str, str] = {}

        def resolve_dept2(row_idx: int, key: str, dept2_id: str, dept2_name: str) -> str | None:
            # Strict: if name exists in DB, the provided id (if any) must match.
            if dept2_id:
                return dept2_id
            if dept2_name:
                existing = dept2_to_id.get(_norm_key(dept2_name))
                if existing:
                    return existing
                return _stable_id("d2", dept2_name)
            issues.append(ImportIssue("member/repo", row_idx, key, "department_level2", "missing department level2"))
            return None

        def resolve_dept3(
            row_idx: int, key: str, dept2_id: str, dept3_id: str, dept3_name: str
        ) -> str | None:
            # Strict: if (dept2_id, name) exists in DB, the provided id (if any) must match.
            if dept3_id:
                return dept3_id
            if dept3_name:
                key3 = (dept2_id, _norm_key(dept3_name))
                existing = dept3_to_id.get(key3)
                if existing:
                    return existing
                return _stable_id("d3", dept2_id, dept3_name)
            return None

        def load_existing_departments() -> None:
            with self.db.connect() as conn:
                for did, name in conn.execute(
                    "SELECT department_level2_id, name FROM gold.dim_department_level2"
                ).fetchall():
                    if name:
                        dept2_to_id[_norm_key(name)] = str(did)
                for did, d2id, name in conn.execute(
                    "SELECT department_level3_id, department_level2_id, name FROM gold.dim_department_level3"
                ).fetchall():
                    if d2id and name:
                        dept3_to_id[(str(d2id), _norm_key(str(name)))] = str(did)
                # Best-effort identity maps for resolving member_key when the CSV leaves it blank.
                # Requires `asktony model build` to have created gold.dim_member_base.
                try:
                    for email_l, mk in conn.execute(
                        """
                        SELECT
                          LOWER(NULLIF(email,'')) AS email_l,
                          MIN(member_key) AS member_key
                        FROM gold.dim_member_base
                        WHERE email IS NOT NULL AND email <> ''
                        GROUP BY 1
                        """
                    ).fetchall():
                        if email_l and mk:
                            email_to_member_key[str(email_l)] = str(mk)
                except Exception:  # noqa: BLE001
                    # Model not built yet (or old schema): skip auto-resolve maps.
                    email_to_member_key.clear()
                try:
                    for kind, identity_l, employee_id in conn.execute(
                        "SELECT kind, identity_l, employee_id FROM gold.bridge_employee_identity"
                    ).fetchall():
                        if kind and identity_l and employee_id:
                            existing_identity_to_employee[(str(kind), str(identity_l))] = str(employee_id)
                except Exception:  # noqa: BLE001
                    existing_identity_to_employee.clear()
                try:
                    for mk, emp in conn.execute(
                        """
                        SELECT member_key, employee_id
                        FROM gold.dim_member_enrichment
                        WHERE employee_id IS NOT NULL AND employee_id <> ''
                        """
                    ).fetchall():
                        mk_s = str(mk or "").strip()
                        emp_s = str(emp or "").strip()
                        if not mk_s or not emp_s:
                            continue
                        member_key_to_employee_id[_norm_key(mk_s)] = emp_s
                        # Prefer non-dummy member_key when multiple rows exist.
                        prev = employee_id_to_member_key.get(emp_s)
                        if prev is None:
                            employee_id_to_member_key[emp_s] = _norm_key(mk_s)
                        elif prev.startswith("dummy_") and not _norm_key(mk_s).startswith("dummy_"):
                            employee_id_to_member_key[emp_s] = _norm_key(mk_s)
                except Exception:  # noqa: BLE001
                    employee_id_to_member_key.clear()
                    member_key_to_employee_id.clear()

        load_existing_departments()

        # Stage 1: validate + prepare department upserts
        dept2_upserts: dict[str, str] = {}  # id -> name
        dept3_upserts: dict[str, tuple[str, str]] = {}  # id -> (d2id, name)
        # Track duplicates inside the same import file to avoid DB UNIQUE constraint failures.
        dept2_name_seen: dict[str, str] = {}  # normalized name -> id
        dept3_name_seen: dict[tuple[str, str], str] = {}  # (dept2_id, normalized name) -> id
        member_upserts: list[
            tuple[
                str,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
                int | None,
                float | None,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
            ]
        ] = []
        repo_upserts: list[tuple[str, str | None, str | None]] = []
        identity_upserts: list[tuple[str, str, str, str]] = []  # (kind, identity_l, employee_id, source)
        member_keys_to_delete: list[str] = []

        if member_file is not None:
            p = member_file.expanduser()
            with p.open("r", encoding="utf-8-sig", newline="") as f:
                r = csv.DictReader(f)
                fieldnames = set(r.fieldnames or [])
                required = {"employee_id", "full_name"}
                if not required.issubset(fieldnames):
                    raise ValueError(f"member_file missing required columns: {sorted(required)}")
                if not ({"email", "email_aliases"} & fieldnames):
                    raise ValueError("member_file must include at least one of: email, email_aliases")
                for idx, row in enumerate(r, start=2):
                    email_raw = str(row.get("email") or "").strip()
                    employee_id_raw = str(row.get("employee_id") or "").strip()
                    email_aliases_raw = str(row.get("email_aliases") or "").strip()

                    if not employee_id_raw:
                        issues.append(ImportIssue("member", idx, "", "employee_id", "missing employee_id"))
                        continue
                    full_name_peek = str(row.get("full_name") or "").strip()
                    if not full_name_peek:
                        issues.append(ImportIssue("member", idx, employee_id_raw, "full_name", "missing full_name"))
                        continue
                    if not email_raw:
                        issues.append(ImportIssue("member", idx, employee_id_raw, "email", "missing email"))
                        continue

                    # Corporate policy: email must be aa.bb@clife.cn for employees.
                    company_user = _company_username_from_email(email_raw)
                    if not company_user:
                        issues.append(
                            ImportIssue(
                                "member",
                                idx,
                                employee_id_raw,
                                "email",
                                "invalid corporate email (expected aa.bb@clife.cn or 801495@clife.cn)",
                            )
                        )
                        continue

                    canonical_member_key = _norm_key(company_user)
                    # Conflict check: a member_key must map to exactly one employee_id.
                    prev_emp_for_mk = member_key_to_employee_id.get(canonical_member_key)
                    if prev_emp_for_mk and prev_emp_for_mk != employee_id_raw:
                        issues.append(
                            ImportIssue(
                                "member",
                                idx,
                                employee_id_raw,
                                "email",
                                f"canonical member_key '{canonical_member_key}' already used by employee_id={prev_emp_for_mk}",
                            )
                        )
                        continue

                    # Stage A: employee_id-scoped identity aliases (best-effort, independent of member_key).
                    # This improves commit->employee mapping for renamed usernames/emails.
                    emails = [_norm_key(email_raw)] + [_norm_key(x) for x in _split_identities(email_aliases_raw)]
                    # Dedupe + drop blanks; also auto-drop aliases identical to primary email.
                    emails_dedup: list[str] = []
                    seen: set[str] = set()
                    for e in emails:
                        if not e:
                            continue
                        if e in seen:
                            continue
                        seen.add(e)
                        emails_dedup.append(e)

                    for identity_l in emails_dedup:
                        key = ("email", identity_l)
                        prev_emp = existing_identity_to_employee.get(key)
                        if prev_emp and prev_emp != employee_id_raw:
                            issues.append(
                                ImportIssue(
                                    "member",
                                    idx,
                                    employee_id_raw,
                                    "email_aliases",
                                    f"identity '{identity_l}' already mapped to employee_id={prev_emp}",
                                )
                            )
                        else:
                            identity_upserts.append(("email", identity_l, employee_id_raw, "import"))
                            existing_identity_to_employee[key] = employee_id_raw

                    # Member key policy: always use corporate username (aa.bb) derived from email.
                    # If this employee_id already exists under a different member_key, migrate it.
                    existing_mk = employee_id_to_member_key.get(employee_id_raw)
                    if existing_mk and existing_mk != canonical_member_key:
                        stats["member_rows_migrated_member_key"] += 1
                        member_keys_to_delete.append(existing_mk)
                    member_key = canonical_member_key
                    if member_key.startswith("dummy_"):
                        stats["member_rows_dummy_member_key"] += 1

                    full_name = full_name_peek or None
                    role_raw = str(row.get("role") or "").strip()
                    role = role_raw or None
                    if role_raw and role_raw.strip().lower() not in ALLOWED_ROLES:
                        issues.append(
                            ImportIssue(
                                "member",
                                idx,
                                member_key,
                                "role",
                                f"invalid role '{role_raw}' (allowed: {sorted(ALLOWED_ROLES)})",
                            )
                        )

                    dept2_id = str(row.get("department_level2_id") or "").strip()
                    dept2_name = str(row.get("department_level2_name") or "").strip()
                    dept3_id = str(row.get("department_level3_id") or "").strip()
                    dept3_name = str(row.get("department_level3_name") or "").strip()

                    def to_int(value: str) -> int | None:
                        s = (value or "").strip()
                        if not s:
                            return None
                        try:
                            return int(float(s))
                        except Exception:  # noqa: BLE001
                            issues.append(ImportIssue("member", idx, member_key, "age", f"invalid int '{value}'"))
                            return None

                    def to_float(value: str) -> float | None:
                        s = (value or "").strip()
                        if not s:
                            return None
                        try:
                            return float(s)
                        except Exception:  # noqa: BLE001
                            issues.append(
                                ImportIssue("member", idx, member_key, "years_of_service", f"invalid float '{value}'")
                            )
                            return None

                    employee_id = employee_id_raw or None
                    employee_type = str(row.get("employee_type") or "").strip() or None
                    department_level1_name = str(row.get("department_level1_name") or "").strip() or None
                    position = str(row.get("position") or "").strip() or None
                    in_date = str(row.get("in_date") or "").strip() or None
                    gender = str(row.get("gender") or "").strip() or None
                    age = to_int(str(row.get("age") or ""))
                    years_of_service = to_float(str(row.get("years_of_service") or ""))
                    job_sequence = str(row.get("job_sequence") or "").strip() or None
                    job_rank = str(row.get("job_rank") or "").strip() or None
                    line_manager = str(row.get("line_manager") or "").strip() or None
                    education_level = str(row.get("education_level") or "").strip() or None
                    collodge = str(row.get("collodge") or "").strip() or None
                    major = str(row.get("major") or "").strip() or None

                    # If any dept fields present, require dept2 and dept3 name/id consistency.
                    has_dept = bool(dept2_id or dept2_name or dept3_id or dept3_name)
                    resolved_dept2_id: str | None = None
                    resolved_dept3_id: str | None = None
                    if has_dept:
                        resolved_dept2_id = resolve_dept2(idx, member_key, dept2_id, dept2_name)
                        if resolved_dept2_id:
                            if dept2_name:
                                norm = _norm_key(dept2_name)
                                existing = dept2_to_id.get(norm)
                                if existing and existing != resolved_dept2_id:
                                    issues.append(
                                        ImportIssue(
                                            "member",
                                            idx,
                                            member_key,
                                            "department_level2",
                                            f"department_level2_name '{dept2_name}' already exists with id={existing}",
                                        )
                                    )
                                else:
                                    seen_id = dept2_name_seen.get(norm)
                                    if seen_id and seen_id != resolved_dept2_id:
                                        issues.append(
                                            ImportIssue(
                                                "member",
                                                idx,
                                                member_key,
                                                "department_level2",
                                                f"duplicate department_level2_name '{dept2_name}' in import (ids: {seen_id} vs {resolved_dept2_id})",
                                            )
                                        )
                                    else:
                                        dept2_name_seen[norm] = resolved_dept2_id
                                        if norm not in dept2_to_id:
                                            dept2_upserts[resolved_dept2_id] = dept2_name
                            if dept3_id or dept3_name:
                                resolved_dept3_id = resolve_dept3(
                                    idx, member_key, resolved_dept2_id, dept3_id, dept3_name
                                )
                                if resolved_dept3_id and dept3_name:
                                    norm3 = _norm_key(dept3_name)
                                    key3 = (resolved_dept2_id, norm3)
                                    existing3 = dept3_to_id.get(key3)
                                    if existing3 and existing3 != resolved_dept3_id:
                                        issues.append(
                                            ImportIssue(
                                                "member",
                                                idx,
                                                member_key,
                                                "department_level3",
                                                f"department_level3_name '{dept3_name}' already exists under {resolved_dept2_id} with id={existing3}",
                                            )
                                        )
                                    else:
                                        seen3 = dept3_name_seen.get(key3)
                                        if seen3 and seen3 != resolved_dept3_id:
                                            issues.append(
                                                ImportIssue(
                                                    "member",
                                                    idx,
                                                    member_key,
                                                    "department_level3",
                                                    f"duplicate department_level3_name '{dept3_name}' under {resolved_dept2_id} in import (ids: {seen3} vs {resolved_dept3_id})",
                                                )
                                            )
                                        else:
                                            dept3_name_seen[key3] = resolved_dept3_id
                                            if key3 not in dept3_to_id:
                                                dept3_upserts[resolved_dept3_id] = (resolved_dept2_id, dept3_name)

                    member_upserts.append(
                        (
                            member_key,
                            full_name,
                            resolved_dept2_id,
                            resolved_dept3_id,
                            role,
                            employee_id,
                            employee_type,
                            department_level1_name,
                            position,
                            in_date,
                            gender,
                            age,
                            years_of_service,
                            job_sequence,
                            job_rank,
                            line_manager,
                            education_level,
                            collodge,
                            major,
                        )
                    )

        if repo_file is not None:
            p = repo_file.expanduser()
            with p.open("r", encoding="utf-8-sig", newline="") as f:
                r = csv.DictReader(f)
                required = {"repo_id"}
                if not required.issubset(set(r.fieldnames or [])):
                    raise ValueError(f"repo_file missing required columns: {sorted(required)}")
                for idx, row in enumerate(r, start=2):
                    repo_id = str(row.get("repo_id") or "").strip()
                    if not repo_id:
                        issues.append(ImportIssue("repo", idx, "", "repo_id", "missing repo_id"))
                        continue

                    dept2_id = str(row.get("department_level2_id") or "").strip()
                    dept2_name = str(row.get("department_level2_name") or "").strip()
                    dept3_id = str(row.get("department_level3_id") or "").strip()
                    dept3_name = str(row.get("department_level3_name") or "").strip()

                    has_dept = bool(dept2_id or dept2_name or dept3_id or dept3_name)
                    resolved_dept2_id: str | None = None
                    resolved_dept3_id: str | None = None
                    if has_dept:
                        resolved_dept2_id = resolve_dept2(idx, repo_id, dept2_id, dept2_name)
                        if resolved_dept2_id:
                            if dept2_name:
                                norm = _norm_key(dept2_name)
                                existing = dept2_to_id.get(norm)
                                if existing and existing != resolved_dept2_id:
                                    issues.append(
                                        ImportIssue(
                                            "repo",
                                            idx,
                                            repo_id,
                                            "department_level2",
                                            f"department_level2_name '{dept2_name}' already exists with id={existing}",
                                        )
                                    )
                                else:
                                    seen_id = dept2_name_seen.get(norm)
                                    if seen_id and seen_id != resolved_dept2_id:
                                        issues.append(
                                            ImportIssue(
                                                "repo",
                                                idx,
                                                repo_id,
                                                "department_level2",
                                                f"duplicate department_level2_name '{dept2_name}' in import (ids: {seen_id} vs {resolved_dept2_id})",
                                            )
                                        )
                                    else:
                                        dept2_name_seen[norm] = resolved_dept2_id
                                        if norm not in dept2_to_id:
                                            dept2_upserts[resolved_dept2_id] = dept2_name
                            if dept3_id or dept3_name:
                                resolved_dept3_id = resolve_dept3(
                                    idx, repo_id, resolved_dept2_id, dept3_id, dept3_name
                                )
                                if resolved_dept3_id and dept3_name:
                                    norm3 = _norm_key(dept3_name)
                                    key3 = (resolved_dept2_id, norm3)
                                    existing3 = dept3_to_id.get(key3)
                                    if existing3 and existing3 != resolved_dept3_id:
                                        issues.append(
                                            ImportIssue(
                                                "repo",
                                                idx,
                                                repo_id,
                                                "department_level3",
                                                f"department_level3_name '{dept3_name}' already exists under {resolved_dept2_id} with id={existing3}",
                                            )
                                        )
                                    else:
                                        seen3 = dept3_name_seen.get(key3)
                                        if seen3 and seen3 != resolved_dept3_id:
                                            issues.append(
                                                ImportIssue(
                                                    "repo",
                                                    idx,
                                                    repo_id,
                                                    "department_level3",
                                                    f"duplicate department_level3_name '{dept3_name}' under {resolved_dept2_id} in import (ids: {seen3} vs {resolved_dept3_id})",
                                                )
                                            )
                                        else:
                                            dept3_name_seen[key3] = resolved_dept3_id
                                            if key3 not in dept3_to_id:
                                                dept3_upserts[resolved_dept3_id] = (resolved_dept2_id, dept3_name)

                    repo_upserts.append((repo_id, resolved_dept2_id, resolved_dept3_id))

        # Validate department existence if not auto-create
        if not auto_create_departments:
            for did, name in dept2_upserts.items():
                issues.append(
                    ImportIssue("departments", 0, did, "department_level2", f"department '{name}' not found")
                )
            for did, (d2id, name) in dept3_upserts.items():
                issues.append(
                    ImportIssue(
                        "departments", 0, did, "department_level3", f"department '{name}' not found under {d2id}"
                    )
                )

        if issues:
            return issues, stats

        if dry_run:
            return issues, stats

        # Stage 2: write to DB
        with self.db.connect() as conn:
            # Best-effort migration: if an employee_id was previously stored under a different member_key,
            # delete the old enrichment row so the canonical member_key can take over.
            if member_keys_to_delete:
                conn.executemany(
                    "DELETE FROM gold.dim_member_enrichment WHERE member_key = ?",
                    [(mk,) for mk in member_keys_to_delete],
                )
            if dept2_upserts:
                rows = [(did, name) for did, name in dept2_upserts.items()]
                conn.executemany(
                    """
                    INSERT INTO gold.dim_department_level2(department_level2_id, name)
                    VALUES (?, ?)
                    ON CONFLICT (department_level2_id) DO UPDATE SET name = EXCLUDED.name
                    """,
                    rows,
                )
                stats["departments_level2_upserted"] += len(rows)

            if dept3_upserts:
                rows = [(did, d2id, name) for did, (d2id, name) in dept3_upserts.items()]
                conn.executemany(
                    """
                    INSERT INTO gold.dim_department_level3(department_level3_id, department_level2_id, name)
                    VALUES (?, ?, ?)
                    ON CONFLICT (department_level3_id) DO UPDATE SET
                      department_level2_id = EXCLUDED.department_level2_id,
                      name = EXCLUDED.name
                    """,
                    rows,
                )
                stats["departments_level3_upserted"] += len(rows)

            if member_upserts:
                conn.executemany(
                    """
                    INSERT INTO gold.dim_member_enrichment(
                      member_key,
                      full_name,
                      department_level2_id,
                      department_level3_id,
                      role,
                      employee_id,
                      employee_type,
                      department_level1_name,
                      position,
                      in_date,
                      gender,
                      age,
                      years_of_service,
                      job_sequence,
                      job_rank,
                      line_manager,
                      education_level,
                      collodge,
                      major
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (member_key) DO UPDATE SET
                      full_name = EXCLUDED.full_name,
                      department_level2_id = EXCLUDED.department_level2_id,
                      department_level3_id = EXCLUDED.department_level3_id,
                      role = EXCLUDED.role
                      ,employee_id = EXCLUDED.employee_id
                      ,employee_type = EXCLUDED.employee_type
                      ,department_level1_name = EXCLUDED.department_level1_name
                      ,position = EXCLUDED.position
                      ,in_date = EXCLUDED.in_date
                      ,gender = EXCLUDED.gender
                      ,age = EXCLUDED.age
                      ,years_of_service = EXCLUDED.years_of_service
                      ,job_sequence = EXCLUDED.job_sequence
                      ,job_rank = EXCLUDED.job_rank
                      ,line_manager = EXCLUDED.line_manager
                      ,education_level = EXCLUDED.education_level
                      ,collodge = EXCLUDED.collodge
                      ,major = EXCLUDED.major
                    """,
                    member_upserts,
                )
                stats["member_rows_upserted"] += len(member_upserts)

            if identity_upserts:
                now = dt.datetime.now(dt.timezone.utc)
                rows = [(k, ident, emp, src, now) for (k, ident, emp, src) in identity_upserts]
                conn.executemany(
                    """
                    INSERT INTO gold.bridge_employee_identity(kind, identity_l, employee_id, source, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (kind, identity_l) DO UPDATE SET
                      employee_id = EXCLUDED.employee_id,
                      source = EXCLUDED.source,
                      updated_at = EXCLUDED.updated_at
                    """,
                    rows,
                )
                stats["employee_identity_rows_upserted"] += len(rows)

            if repo_upserts:
                conn.executemany(
                    """
                    INSERT INTO gold.dim_repo_enrichment(
                      repo_id, department_level2_id, department_level3_id
                    )
                    VALUES (?, ?, ?)
                    ON CONFLICT (repo_id) DO UPDATE SET
                      department_level2_id = EXCLUDED.department_level2_id,
                      department_level3_id = EXCLUDED.department_level3_id
                    """,
                    repo_upserts,
                )
                stats["repo_rows_upserted"] += len(repo_upserts)

        return issues, stats


def issues_to_table(issues: list[ImportIssue], limit: int = 50) -> Table:
    t = Table(title=f"Data Quality Issues (showing up to {limit})", show_lines=False)
    t.add_column("file")
    t.add_column("row")
    t.add_column("key")
    t.add_column("field")
    t.add_column("message")
    for it in issues[:limit]:
        t.add_row(it.file, str(it.row), it.key, it.field, it.message)
    return t
