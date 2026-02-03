from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

from asktony.config import AskTonyConfig
from asktony.db import DB
from asktony.dim_admin import ROLE_CHANGE_WEIGHTS
from asktony.render import to_rich_table


@dataclass(frozen=True)
class Warehouse:
    root: Path
    db: DB

    @classmethod
    def from_config(cls, cfg: AskTonyConfig) -> "Warehouse":
        return cls(root=cfg.lake_dir_path, db=DB(cfg.db_path_resolved))

    @property
    def gold_dir(self) -> Path:
        return self.root / "gold"

    @staticmethod
    def _valid_member_join_condition(commit_alias: str, bridge_alias: str) -> str:
        # Match commits to "valid members" with best-effort identity alignment:
        # - Primary: member_key equality
        # - Fallbacks: username/email equality (case-insensitive)
        return (
            f"{bridge_alias}.member_key = {commit_alias}.member_key "
            f"OR LOWER(NULLIF({bridge_alias}.username,'')) = LOWER(NULLIF({commit_alias}.author_username,'')) "
            f"OR LOWER(NULLIF({bridge_alias}.email,'')) = LOWER(NULLIF({commit_alias}.author_email,''))"
        )

    @staticmethod
    def _global_member_identity_maps_cte_sql() -> str:
        # Global (cross-repo) member maps.
        #
        # Use gold.dim_member as the source because it is the union of:
        # - silver.members (repo members)
        # - silver.top_contributors
        # - silver.commits (authors)
        #
        # This improves matching for employees who have commits but are missing from
        # repo member lists for that specific repo.
        return """
        member_keys AS (
          SELECT DISTINCT member_key
          FROM gold.dim_member
          WHERE member_key IS NOT NULL AND member_key <> ''
        ),
        email_map AS (
          SELECT
            LOWER(NULLIF(email,'')) AS email_l,
            MIN(member_key) AS member_key
          FROM gold.dim_member
          WHERE email IS NOT NULL AND email <> ''
          GROUP BY 1
        ),
        username_map AS (
          SELECT
            LOWER(NULLIF(username,'')) AS username_l,
            MIN(member_key) AS member_key
          FROM gold.dim_member
          WHERE username IS NOT NULL AND username <> ''
          GROUP BY 1
        )
        """

    @staticmethod
    def _role_change_weight_case_sql(role_sql: str) -> str:
        """
        Build a SQL CASE expression that maps role -> weight (default 1.0).
        Used for role-weighted changed_lines so cross-role comparisons are fairer.
        """
        clauses: list[str] = []
        for role, weight in ROLE_CHANGE_WEIGHTS.items():
            # role strings are controlled (ROLE_OPTIONS), so simple quoting is ok.
            clauses.append(f"WHEN {role_sql} = '{role}' THEN {float(weight)}")
        when_sql = " ".join(clauses)
        return f"(CASE {when_sql} ELSE 1.0 END)"

    @staticmethod
    def _global_member_key_expr(*, mk_alias: str = "mk", em_alias: str = "em", um_alias: str = "um") -> str:
        # Prefer member_key match, then email, then username.
        return f"COALESCE({mk_alias}.member_key, {em_alias}.member_key, {um_alias}.member_key)"

    @staticmethod
    def _employees_cte_sql() -> str:
        # Employees are defined as imported enrichment rows with a non-empty full_name (HR full list).
        # This allows "dummy_*" placeholder member_key employees to exist even if they never appear in
        # repo membership lists or commits (i.e., not present in dim_member_base).
        return """
        employees AS (
          SELECT
            e.member_key,
            -- One-ID strategy for employee analytics: employee_id is the ultimate unique person id.
            -- Fallback to stable synthetic ids only if employee_id is missing (should be rare if HR list is complete).
            COALESCE(NULLIF(TRIM(e.employee_id), ''), COALESCE(b.one_id, 'mk:' || e.member_key)) AS one_id,
            b.user_id,
            b.username,
            b.email,
            e.full_name,
            e.department_level1_name,
            e.department_level2_id,
            e.department_level3_id,
            e.role,
            e.employee_id,
            e.employee_type,
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
            e.major,
            d2.name AS department_level2_name,
            d3.name AS department_level3_name
          FROM gold.dim_member_enrichment e
          LEFT JOIN gold.dim_member_base b ON b.member_key = e.member_key
          LEFT JOIN gold.dim_department_level3 d3 ON d3.department_level3_id = e.department_level3_id
          LEFT JOIN gold.dim_department_level2 d2
            ON d2.department_level2_id = COALESCE(e.department_level2_id, d3.department_level2_id)
          WHERE NULLIF(TRIM(e.full_name), '') IS NOT NULL
            AND NULLIF(TRIM(e.employee_id), '') IS NOT NULL
        ),
        emp_email_map AS (
          SELECT
            LOWER(NULLIF(email,'')) AS email_l,
            MIN(member_key) AS member_key
          FROM employees
          WHERE email IS NOT NULL AND email <> ''
          GROUP BY 1
        ),
        emp_username_map AS (
          SELECT
            LOWER(NULLIF(username,'')) AS username_l,
            MIN(member_key) AS member_key
          FROM employees
          WHERE username IS NOT NULL AND username <> ''
          GROUP BY 1
        )
        """

    def build(self) -> None:
        with self.db.connect() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

            # 4.2 仓库维度
            # 兼容历史版本：dim_repo 可能是 TABLE 或 VIEW
            conn.execute("DROP VIEW IF EXISTS gold.dim_repo")
            conn.execute("DROP TABLE IF EXISTS gold.dim_repo")
            conn.execute("DROP TABLE IF EXISTS gold.dim_repo_base")
            conn.execute(
                """
                CREATE TABLE gold.dim_repo_base AS
                SELECT
                  repo_id,
                  repo_name,
                  repo_path,
                  group_id
                FROM silver.repos
                WHERE repo_id IS NOT NULL AND repo_id <> ''
                """
            )
            # Enrichment & departments (do not drop, keep user-edited data)
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
                CREATE TABLE IF NOT EXISTS gold.dim_repo_enrichment (
                  repo_id TEXT PRIMARY KEY,
                  department_level2_id TEXT,
                  department_level3_id TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE VIEW gold.dim_repo AS
                SELECT
                  b.repo_id,
                  b.repo_name,
                  b.repo_path,
                  b.group_id,
                  e.department_level2_id,
                  e.department_level3_id,
                  d2.name AS department_level2_name,
                  d3.name AS department_level3_name
                FROM gold.dim_repo_base b
                LEFT JOIN gold.dim_repo_enrichment e ON e.repo_id = b.repo_id
                LEFT JOIN gold.dim_department_level3 d3 ON d3.department_level3_id = e.department_level3_id
                LEFT JOIN gold.dim_department_level2 d2
                  ON d2.department_level2_id = COALESCE(e.department_level2_id, d3.department_level2_id)
                """
            )

            # 4.1 成员列表维度：来自 members / top_contributors / commits author
            # 兼容历史版本：dim_member 可能是 TABLE 或 VIEW
            conn.execute("DROP VIEW IF EXISTS gold.dim_member")
            conn.execute("DROP TABLE IF EXISTS gold.dim_member")
            conn.execute("DROP TABLE IF EXISTS gold.dim_member_base")
            conn.execute(
                """
                CREATE TABLE gold.dim_member_base AS
                WITH u AS (
                  SELECT DISTINCT NULLIF(user_id,'') AS user_id, NULLIF(username,'') AS username, NULLIF(email,'') AS email
                  FROM silver.members
                  UNION ALL
                  SELECT DISTINCT NULLIF(user_id,'') AS user_id, NULLIF(username,'') AS username, CAST(NULL AS TEXT) AS email
                  FROM silver.top_contributors
                  UNION ALL
                  SELECT DISTINCT NULLIF(author_id,'') AS user_id, NULLIF(author_username,'') AS username, NULLIF(author_email,'') AS email
                  FROM silver.commits
                ),
                normalized AS (
                  SELECT
                    CASE
                      WHEN regexp_matches(LOWER(NULLIF(email,'')), '^(?:[a-z0-9]+\\.[a-z0-9]+|[0-9]+)@clife\\.cn$')
                        THEN (
                          CASE
                            WHEN regexp_matches(split_part(LOWER(email), '@', 1), '^[0-9]+$')
                              THEN 'partner-' || split_part(LOWER(email), '@', 1)
                            ELSE split_part(LOWER(email), '@', 1)
                          END
                        )
                      WHEN regexp_matches(LOWER(NULLIF(username,'')), '^(?:[a-z0-9]+\\.[a-z0-9]+|[0-9]+|partner-[0-9]+)$')
                        THEN LOWER(username)
                      ELSE LOWER(COALESCE(username, email, user_id))
                    END AS member_key,
                    user_id,
                    username,
                    email
                  FROM u
                )
                SELECT
                  member_key,
                  CASE
                    WHEN NULLIF(MAX(user_id), '') IS NOT NULL THEN 'uid:' || MAX(user_id)
                    ELSE 'mk:' || member_key
                  END AS one_id,
                  MAX(user_id) AS user_id,
                  MAX(username) AS username,
                  MAX(email) AS email
                FROM normalized
                WHERE user_id IS NOT NULL OR username IS NOT NULL OR email IS NOT NULL
                GROUP BY 1
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
                CREATE VIEW gold.dim_member AS
                SELECT
                  b.member_key,
                  b.one_id,
                  b.user_id,
                  b.username,
                  b.email,
                  e.full_name,
                  e.department_level2_id,
                  e.department_level3_id,
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
                  e.major,
                  d2.name AS department_level2_name,
                  d3.name AS department_level3_name
                FROM gold.dim_member_base b
                LEFT JOIN gold.dim_member_enrichment e ON e.member_key = b.member_key
                LEFT JOIN gold.dim_department_level3 d3 ON d3.department_level3_id = e.department_level3_id
                LEFT JOIN gold.dim_department_level2 d2
                  ON d2.department_level2_id = COALESCE(e.department_level2_id, d3.department_level2_id)
                """
            )

            # repo 成员桥表（可用于过滤“有效成员”）
            conn.execute("DROP TABLE IF EXISTS gold.bridge_repo_member")
            conn.execute(
                """
                CREATE TABLE gold.bridge_repo_member AS
                SELECT DISTINCT
                  repo_id,
                  CASE
                    WHEN regexp_matches(LOWER(NULLIF(email,'')), '^(?:[a-z0-9]+\\.[a-z0-9]+|[0-9]+)@clife\\.cn$')
                      THEN (
                        CASE
                          WHEN regexp_matches(split_part(LOWER(email), '@', 1), '^[0-9]+$')
                            THEN 'partner-' || split_part(LOWER(email), '@', 1)
                          ELSE split_part(LOWER(email), '@', 1)
                        END
                      )
                    WHEN regexp_matches(LOWER(NULLIF(username,'')), '^(?:[a-z0-9]+\\.[a-z0-9]+|[0-9]+|partner-[0-9]+)$')
                      THEN LOWER(username)
                    ELSE LOWER(COALESCE(NULLIF(username,''), NULLIF(email,''), NULLIF(user_id,'')))
                  END AS member_key,
                  CASE
                    WHEN NULLIF(user_id,'') IS NOT NULL THEN 'uid:' || user_id
                    ELSE 'mk:' || LOWER(COALESCE(NULLIF(username,''), NULLIF(email,''), NULLIF(user_id,'')))
                  END AS one_id,
                  NULLIF(username,'') AS username,
                  NULLIF(email,'') AS email,
                  NULLIF(role,'') AS role,
                  NULLIF(state,'') AS state
                FROM silver.members
                WHERE repo_id IS NOT NULL AND repo_id <> ''
                """
            )

            # 4.3 提交事实表：commit 级（数据量大：按 YYYY-MM 分区落地到 parquet）
            #
            # 说明：
            # - 物理数据：gold/fact_commit/commit_month=YYYY-MM/*.parquet（hive 分区）
            # - DuckDB 对外：gold.fact_commit 视图（read_parquet + hive_partitioning）
            fact_commit_dir = self.gold_dir / "fact_commit"
            fact_commit_dir.mkdir(parents=True, exist_ok=True)
            conn.execute("DROP VIEW IF EXISTS gold.fact_commit")
            conn.execute("DROP TABLE IF EXISTS gold.fact_commit")
            conn.execute(
                f"""
	                COPY (
	                  WITH corp_email AS (
	                    SELECT
	                      employee_id,
	                      MIN(identity_l) AS company_email
	                    FROM gold.bridge_employee_identity
	                    WHERE kind = 'email'
	                      AND regexp_matches(identity_l, '^(?:[a-z0-9]+\\.[a-z0-9]+|[0-9]+)@clife\\.cn$')
	                    GROUP BY 1
	                  ),
	                  base_commits AS (
	                    SELECT * FROM silver.commits
	                  ),
	                  picked_id AS (
	                    -- Commit-side identity unification: rely on author_email as the primary key.
	                    SELECT c.repo_id, c.sha, m.employee_id
	                    FROM base_commits c
	                    JOIN gold.bridge_employee_identity m
	                      ON m.kind = 'email' AND m.identity_l = LOWER(NULLIF(c.author_email,''))
	                  ),
	                  commit_employee AS (
	                    SELECT
	                      c.*,
	                      p.employee_id AS employee_id,
	                      ce.company_email AS company_email
	                    FROM base_commits c
	                    LEFT JOIN picked_id p ON p.repo_id = c.repo_id AND p.sha = c.sha
	                    LEFT JOIN corp_email ce ON ce.employee_id = p.employee_id
	                  )
	                  SELECT
	                    c.repo_id,
	                    c.sha AS commit_sha,
	                    CASE
	                      WHEN regexp_matches(LOWER(NULLIF(c.author_email,'')), '^(?:[a-z0-9]+\\.[a-z0-9]+|[0-9]+)@clife\\.cn$')
	                        THEN (
	                          CASE
	                            WHEN regexp_matches(split_part(LOWER(c.author_email), '@', 1), '^[0-9]+$')
	                              THEN 'partner-' || split_part(LOWER(c.author_email), '@', 1)
	                            ELSE split_part(LOWER(c.author_email), '@', 1)
	                          END
	                        )
	                      WHEN c.company_email IS NOT NULL
	                        THEN (
	                          CASE
	                            WHEN regexp_matches(split_part(c.company_email, '@', 1), '^[0-9]+$')
	                              THEN 'partner-' || split_part(c.company_email, '@', 1)
	                            ELSE split_part(c.company_email, '@', 1)
	                          END
	                        )
	                      -- External commits: use the commit's author_email as member_key.
	                      ELSE COALESCE(LOWER(NULLIF(c.author_email,'')), LOWER(NULLIF(c.author_username,'')))
	                    END AS member_key,
	                    COALESCE(
	                      NULLIF(TRIM(c.employee_id), ''),
	                      CASE
	                        WHEN NULLIF(c.author_id,'') IS NOT NULL THEN 'uid:' || c.author_id
	                        ELSE 'mk:' || LOWER(COALESCE(NULLIF(c.author_username,''), NULLIF(c.author_email,''), NULLIF(c.author_id,'')))
	                      END
	                    ) AS one_id,
	                    CASE
	                      WHEN regexp_matches(LOWER(NULLIF(c.author_email,'')), '^(?:[a-z0-9]+\\.[a-z0-9]+|[0-9]+)@clife\\.cn$')
	                        THEN (
	                          CASE
	                            WHEN regexp_matches(split_part(LOWER(c.author_email), '@', 1), '^[0-9]+$')
	                              THEN 'partner-' || split_part(LOWER(c.author_email), '@', 1)
	                            ELSE split_part(LOWER(c.author_email), '@', 1)
	                          END
	                        )
	                      WHEN c.company_email IS NOT NULL
	                        THEN (
	                          CASE
	                            WHEN regexp_matches(split_part(c.company_email, '@', 1), '^[0-9]+$')
	                              THEN 'partner-' || split_part(c.company_email, '@', 1)
	                            ELSE split_part(c.company_email, '@', 1)
	                          END
	                        )
	                      ELSE c.author_username
	                    END AS author_username,
	                    c.author_email,
	                    c.committed_at,
	                    STRFTIME(c.committed_at, '%Y-%m') AS commit_month,
	                    COALESCE(s.additions, c.additions, 0) AS additions,
	                    COALESCE(s.deletions, c.deletions, 0) AS deletions,
	                    COALESCE(s.additions, c.additions, 0) + COALESCE(s.deletions, c.deletions, 0) AS changed_lines
	                  FROM commit_employee c
	                  LEFT JOIN silver.commit_stats s
	                    ON s.repo_id = c.repo_id AND s.sha = c.sha
	                  WHERE c.sha IS NOT NULL AND c.sha <> '' AND c.committed_at IS NOT NULL
	                )
	                TO '{str(fact_commit_dir).replace("'", "''")}'
	                (FORMAT PARQUET, PARTITION_BY (commit_month), OVERWRITE_OR_IGNORE TRUE)
	                """
	            )
            # DuckDB 的 glob 对 ** 支持不稳定；这里使用单层匹配（commit_month=YYYY-MM/*.parquet）。
            parquet_glob = fact_commit_dir / "*" / "*.parquet"
            if any(fact_commit_dir.glob("*/*.parquet")):
                conn.execute(
                    f"""
                    CREATE VIEW gold.fact_commit AS
                    SELECT *
                    FROM read_parquet(
                      '{str(parquet_glob).replace("'", "''")}',
                      hive_partitioning=1
                    )
                    """
                )
            else:
                # 没有任何 commits（或都无 committed_at）时，创建一个空视图，保证后续模型可正常 build。
                conn.execute(
                    """
	                    CREATE VIEW gold.fact_commit AS
	                    SELECT
	                      CAST(NULL AS TEXT) AS repo_id,
	                      CAST(NULL AS TEXT) AS commit_sha,
	                      CAST(NULL AS TEXT) AS member_key,
	                      CAST(NULL AS TEXT) AS one_id,
	                      CAST(NULL AS TEXT) AS author_username,
	                      CAST(NULL AS TEXT) AS author_email,
	                      CAST(NULL AS TIMESTAMPTZ) AS committed_at,
	                      CAST(NULL AS TEXT) AS commit_month,
	                      CAST(NULL AS BIGINT) AS additions,
                      CAST(NULL AS BIGINT) AS deletions,
                      CAST(NULL AS BIGINT) AS changed_lines
                    WHERE 1=0
                    """
                )

            # 月度聚合事实：方便分析
            conn.execute("DROP TABLE IF EXISTS gold.fact_member_repo_month")
            conn.execute(
                """
                CREATE TABLE gold.fact_member_repo_month AS
                SELECT
                  repo_id,
                  member_key,
                  commit_month,
                  COUNT(*) AS commit_count,
                  SUM(changed_lines) AS changed_lines,
                  SUM(additions) AS additions,
                  SUM(deletions) AS deletions
                FROM gold.fact_commit
                WHERE member_key IS NOT NULL
                GROUP BY 1,2,3
                """
            )

            # Commit -> employee_id 统一口径视图（employee analytics 的事实入口）
            #
            # Notes:
            # - This does not rewrite the physical parquet; it is a resolving view on top of gold.fact_commit.
            # - If an employee has multiple member_key/username/email values, all commits can still be unified to
            #   the same employee_id as long as one of the identifiers matches.
            conn.execute("DROP VIEW IF EXISTS gold.fact_commit_employee")
            conn.execute(
                f"""
                CREATE VIEW gold.fact_commit_employee AS
                WITH {self._employees_cte_sql()},
                employee_dim AS (
                  SELECT
                    one_id AS employee_id,
                    arg_min(member_key, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS member_key,
                    arg_min(user_id, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS user_id,
                    arg_min(username, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS username,
                    arg_min(email, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS email,
                    arg_min(full_name, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS full_name,
                    arg_min(department_level2_name, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS department_level2_name,
                    arg_min(department_level3_name, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS department_level3_name,
                    arg_min(role, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS role
                  FROM employees
                  GROUP BY 1
                ),
                identity_map AS (
                  -- Employee mapping: rely on email identities only (commit policy enforces author_email).
                  SELECT 'email' AS kind, LOWER(NULLIF(email,'')) AS identity_l, one_id AS employee_id
                  FROM employees
                  WHERE NULLIF(email,'') IS NOT NULL
                  UNION ALL
                  SELECT kind, identity_l, employee_id
                  FROM gold.bridge_employee_identity
                  WHERE kind = 'email'
                ),
                candidates AS (
                  SELECT
                    fc.*,
                    m.employee_id AS employee_id,
                    d.full_name AS full_name,
                    d.department_level2_name AS department_level2_name,
                    d.department_level3_name AS department_level3_name,
                    d.role AS role,
                    CASE
                      WHEN m.kind = 'email' AND m.identity_l = LOWER(NULLIF(fc.author_email,'')) THEN 1
                      ELSE 100
                    END AS match_rank
                  FROM gold.fact_commit fc
                  JOIN identity_map m
                    ON (m.kind = 'email' AND m.identity_l = LOWER(NULLIF(fc.author_email,'')))
                  JOIN employee_dim d ON d.employee_id = m.employee_id
                ),
                picked AS (
                  SELECT *
                  FROM (
                    SELECT
                      *,
                      ROW_NUMBER() OVER (
                        PARTITION BY repo_id, commit_sha
                        ORDER BY match_rank ASC, employee_id ASC
                      ) AS rn
                    FROM candidates
                  )
                  WHERE rn = 1
                )
                SELECT
                  repo_id,
                  commit_sha,
                  member_key,
                  one_id,
                  author_username,
                  author_email,
                  committed_at,
                  commit_month,
                  additions,
                  deletions,
                  changed_lines,
                  employee_id,
                  full_name,
                  department_level2_name,
                  department_level3_name,
                  role
                FROM picked
                """
            )

            # 项目维度与事实（不 drop 项目主数据/桥表，保留用户维护内容）
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.dim_project (
                  project_id TEXT PRIMARY KEY,
                  project_name TEXT,
                  project_type TEXT,
                  status TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.bridge_project_repo (
                  project_id TEXT,
                  repo_id TEXT,
                  start_at DATE,
                  end_at DATE,
                  weight DOUBLE,
                  PRIMARY KEY(project_id, repo_id, start_at)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.bridge_project_person_role (
                  project_id TEXT,
                  employee_id TEXT,
                  project_role TEXT,
                  start_at DATE,
                  end_at DATE,
                  allocation DOUBLE,
                  PRIMARY KEY(project_id, employee_id, project_role, start_at)
                )
                """
            )

            # 项目事实：按月聚合（commit -> employee_id -> project(weight)），仅统计项目成员且为开发角色的员工
            dev_roles = [
                "Java 后台开发",
                "Web 前端开发",
                "终端开发",
                "算法开发",
                "数据开发",
                "全栈开发",
            ]
            roles_sql = ", ".join(["?"] * len(dev_roles))

            conn.execute("DROP TABLE IF EXISTS gold.fact_project_employee_month")
            conn.execute(
                f"""
                CREATE TABLE gold.fact_project_employee_month AS
                WITH employee_hr AS (
                  SELECT
                    NULLIF(TRIM(employee_id), '') AS employee_id,
                    MIN(NULLIF(TRIM(role), '')) AS role
                  FROM gold.dim_member_enrichment
                  WHERE employee_id IS NOT NULL AND employee_id <> ''
                  GROUP BY 1
                ),
                dev_employees AS (
                  SELECT employee_id
                  FROM employee_hr
                  WHERE role IN ({roles_sql})
                ),
                emp_by_member_key AS (
                  SELECT
                    member_key,
                    MIN(NULLIF(TRIM(employee_id), '')) AS employee_id
                  FROM gold.dim_member
                  WHERE employee_id IS NOT NULL AND employee_id <> ''
                  GROUP BY 1
                ),
                emp_by_email AS (
                  SELECT
                    LOWER(NULLIF(email,'')) AS email_l,
                    MIN(NULLIF(TRIM(employee_id), '')) AS employee_id
                  FROM gold.dim_member
                  WHERE email IS NOT NULL AND email <> ''
                    AND employee_id IS NOT NULL AND employee_id <> ''
                  GROUP BY 1
                ),
                emp_by_username AS (
                  SELECT
                    LOWER(NULLIF(username,'')) AS username_l,
                    MIN(NULLIF(TRIM(employee_id), '')) AS employee_id
                  FROM gold.dim_member
                  WHERE username IS NOT NULL AND username <> ''
                    AND employee_id IS NOT NULL AND employee_id <> ''
                  GROUP BY 1
                ),
                resolved_commits AS (
                  SELECT
                    c.repo_id,
                    c.commit_month,
                    c.committed_at,
                    c.changed_lines,
                    COALESCE(mk.employee_id, em.employee_id, um.employee_id) AS employee_id
                  FROM gold.fact_commit c
                  LEFT JOIN emp_by_member_key mk ON mk.member_key = c.member_key
                  LEFT JOIN emp_by_email em
                    ON em.email_l = LOWER(NULLIF(c.author_email,''))
                   AND mk.employee_id IS NULL
                  LEFT JOIN emp_by_username um
                    ON um.username_l = LOWER(NULLIF(c.author_username,''))
                   AND mk.employee_id IS NULL
                   AND em.employee_id IS NULL
                  WHERE c.repo_id IS NOT NULL AND c.repo_id <> ''
                ),
                attributed AS (
                  SELECT
                    pr.project_id,
                    rc.repo_id,
                    rc.commit_month,
                    rc.committed_at,
                    rc.employee_id,
                    COALESCE(pr.weight, 1.0) AS weight,
                    rc.changed_lines
                  FROM resolved_commits rc
                  JOIN dev_employees de ON de.employee_id = rc.employee_id
                  JOIN gold.bridge_project_repo pr
                    ON pr.repo_id = rc.repo_id
                   AND CAST(rc.committed_at AS DATE) >= pr.start_at
                   AND (pr.end_at IS NULL OR CAST(rc.committed_at AS DATE) <= pr.end_at)
                  JOIN gold.bridge_project_person_role pm
                    ON pm.project_id = pr.project_id
                   AND pm.employee_id = rc.employee_id
                   AND CAST(rc.committed_at AS DATE) >= pm.start_at
                   AND (pm.end_at IS NULL OR CAST(rc.committed_at AS DATE) <= pm.end_at)
                  WHERE rc.employee_id IS NOT NULL AND rc.employee_id <> ''
                )
                SELECT
                  project_id,
                  employee_id,
                  commit_month,
                  SUM(weight)::DOUBLE AS weighted_commit_count,
                  SUM(weight * changed_lines)::DOUBLE AS weighted_changed_lines,
                  COUNT(DISTINCT repo_id)::BIGINT AS repo_count,
                  MIN(committed_at) AS committed_at_min,
                  MAX(committed_at) AS committed_at_max
                FROM attributed
                GROUP BY 1,2,3
                """,
                dev_roles,
            )

            conn.execute("DROP TABLE IF EXISTS gold.fact_project_month")
            conn.execute(
                f"""
                CREATE TABLE gold.fact_project_month AS
                WITH employee_hr AS (
                  SELECT
                    NULLIF(TRIM(employee_id), '') AS employee_id,
                    MIN(NULLIF(TRIM(role), '')) AS role
                  FROM gold.dim_member_enrichment
                  WHERE employee_id IS NOT NULL AND employee_id <> ''
                  GROUP BY 1
                ),
                dev_employees AS (
                  SELECT employee_id
                  FROM employee_hr
                  WHERE role IN ({roles_sql})
                ),
                emp_by_member_key AS (
                  SELECT
                    member_key,
                    MIN(NULLIF(TRIM(employee_id), '')) AS employee_id
                  FROM gold.dim_member
                  WHERE employee_id IS NOT NULL AND employee_id <> ''
                  GROUP BY 1
                ),
                emp_by_email AS (
                  SELECT
                    LOWER(NULLIF(email,'')) AS email_l,
                    MIN(NULLIF(TRIM(employee_id), '')) AS employee_id
                  FROM gold.dim_member
                  WHERE email IS NOT NULL AND email <> ''
                    AND employee_id IS NOT NULL AND employee_id <> ''
                  GROUP BY 1
                ),
                emp_by_username AS (
                  SELECT
                    LOWER(NULLIF(username,'')) AS username_l,
                    MIN(NULLIF(TRIM(employee_id), '')) AS employee_id
                  FROM gold.dim_member
                  WHERE username IS NOT NULL AND username <> ''
                    AND employee_id IS NOT NULL AND employee_id <> ''
                  GROUP BY 1
                ),
                resolved_commits AS (
                  SELECT
                    c.repo_id,
                    c.commit_month,
                    c.committed_at,
                    c.changed_lines,
                    COALESCE(mk.employee_id, em.employee_id, um.employee_id) AS employee_id
                  FROM gold.fact_commit c
                  LEFT JOIN emp_by_member_key mk ON mk.member_key = c.member_key
                  LEFT JOIN emp_by_email em
                    ON em.email_l = LOWER(NULLIF(c.author_email,''))
                   AND mk.employee_id IS NULL
                  LEFT JOIN emp_by_username um
                    ON um.username_l = LOWER(NULLIF(c.author_username,''))
                   AND mk.employee_id IS NULL
                   AND em.employee_id IS NULL
                  WHERE c.repo_id IS NOT NULL AND c.repo_id <> ''
                ),
                attributed AS (
                  SELECT
                    pr.project_id,
                    rc.repo_id,
                    rc.commit_month,
                    rc.committed_at,
                    rc.employee_id,
                    COALESCE(pr.weight, 1.0) AS weight,
                    rc.changed_lines
                  FROM resolved_commits rc
                  JOIN dev_employees de ON de.employee_id = rc.employee_id
                  JOIN gold.bridge_project_repo pr
                    ON pr.repo_id = rc.repo_id
                   AND CAST(rc.committed_at AS DATE) >= pr.start_at
                   AND (pr.end_at IS NULL OR CAST(rc.committed_at AS DATE) <= pr.end_at)
                  JOIN gold.bridge_project_person_role pm
                    ON pm.project_id = pr.project_id
                   AND pm.employee_id = rc.employee_id
                   AND CAST(rc.committed_at AS DATE) >= pm.start_at
                   AND (pm.end_at IS NULL OR CAST(rc.committed_at AS DATE) <= pm.end_at)
                  WHERE rc.employee_id IS NOT NULL AND rc.employee_id <> ''
                )
                SELECT
                  project_id,
                  commit_month,
                  SUM(weight)::DOUBLE AS weighted_commit_count,
                  SUM(weight * changed_lines)::DOUBLE AS weighted_changed_lines,
                  COUNT(DISTINCT employee_id)::BIGINT AS active_dev,
                  COUNT(DISTINCT repo_id)::BIGINT AS repo_count,
                  MIN(committed_at) AS committed_at_min,
                  MAX(committed_at) AS committed_at_max
                FROM attributed
                GROUP BY 1,2
                """,
                dev_roles,
            )

            self._materialize(conn, "gold.dim_repo", self.gold_dir / "dim_repo.parquet")
            self._materialize(conn, "gold.dim_member", self.gold_dir / "dim_member.parquet")
            self._materialize(conn, "gold.bridge_repo_member", self.gold_dir / "bridge_repo_member.parquet")
            self._materialize(conn, "gold.fact_member_repo_month", self.gold_dir / "fact_member_repo_month.parquet")
            self._materialize(conn, "gold.dim_project", self.gold_dir / "dim_project.parquet")
            self._materialize(conn, "gold.bridge_project_repo", self.gold_dir / "bridge_project_repo.parquet")
            self._materialize(conn, "gold.bridge_project_person_role", self.gold_dir / "bridge_project_person_role.parquet")
            self._materialize(conn, "gold.fact_project_employee_month", self.gold_dir / "fact_project_employee_month.parquet")
            self._materialize(conn, "gold.fact_project_month", self.gold_dir / "fact_project_month.parquet")

    def active_repos(self, months: int, top: int = 20):
        columns, rows = self.active_repos_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def active_repos_data(self, months: int, top: int | None = 20) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        # Unsaturated threshold (business framing): >= 6 commits/month is a basic "in the loop" signal.
        unsat_commit_min = max(1, int(months) * 6)
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                  r.repo_id,
                  r.repo_name,
                  COALESCE(r.department_level2_name, 'Unassigned') AS department_level2_name,
                  COALESCE(r.department_level3_name, 'Unassigned') AS department_level3_name,
                  COUNT(c.commit_sha) AS commit_count,
                  SUM(c.changed_lines) AS changed_lines
                FROM gold.dim_repo r
                JOIN gold.fact_commit c ON c.repo_id = r.repo_id
                -- 仅统计“有效成员（members）”在仓库内的提交
                JOIN gold.bridge_repo_member br
                  ON br.repo_id = c.repo_id
                 AND ({self._valid_member_join_condition("c", "br")})
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                GROUP BY 1,2,3,4
                ORDER BY commit_count DESC
                {limit_sql}
                """,
                params,
            ).fetchall()
        return [
            "repo_id",
            "repo_name",
            "department_level2_name",
            "department_level3_name",
            "commit_count",
            "changed_lines",
        ], rows

    def member_commits_all_repos(self, months: int, top: int = 50):
        columns, rows = self.member_commits_all_repos_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def member_commits_all_repos_data(self, months: int, top: int | None = 50) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        # Unsaturated threshold (business framing): >= 6 commits/month is a basic "in the loop" signal.
        unsat_commit_min = max(1, int(months) * 6)
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                  COALESCE(m.full_name, e.full_name, '') AS full_name,
                  COALESCE(m.username, c.author_username, c.member_key) AS member,
                  c.member_key,
                  arg_max(COALESCE(NULLIF(c.author_email,''), NULLIF(m.email,''), ''), c.committed_at) AS author_email,
                  COALESCE(m.department_level2_name, d2.name, 'Unassigned') AS department_level2_name,
                  COALESCE(m.department_level3_name, d3.name, 'Unassigned') AS department_level3_name,
                  COUNT(*) AS commit_count,
                  SUM(c.changed_lines) AS changed_lines
                FROM gold.fact_commit c
                LEFT JOIN gold.dim_member m ON m.member_key = c.member_key
                LEFT JOIN gold.bridge_employee_identity b
                  ON b.kind = 'email' AND b.identity_l = LOWER(NULLIF(c.author_email,''))
                LEFT JOIN gold.dim_member_enrichment e ON e.employee_id = b.employee_id
                LEFT JOIN gold.dim_department_level3 d3 ON d3.department_level3_id = e.department_level3_id
                LEFT JOIN gold.dim_department_level2 d2
                  ON d2.department_level2_id = COALESCE(e.department_level2_id, d3.department_level2_id)
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                GROUP BY 1,2,3,5,6
                ORDER BY commit_count DESC
                {limit_sql}
                """,
                params,
            ).fetchall()
        return [
            "full_name",
            "member",
            "member_key",
            "author_email",
            "department_level2_name",
            "department_level3_name",
            "commit_count",
            "changed_lines",
        ], rows

    def employee_commits_all_repos(self, months: int, top: int = 50):
        columns, rows = self.employee_commits_all_repos_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def employee_commits_all_repos_data(self, months: int, top: int | None = 50) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                  COALESCE(full_name, '') AS full_name,
                  COALESCE(employee_id, '') AS employee_id,
                  COALESCE(department_level2_name, 'Unassigned') AS department_level2_name,
                  COALESCE(department_level3_name, 'Unassigned') AS department_level3_name,
                  COALESCE(role, '') AS role,
                  COUNT(*) AS commit_count,
                  SUM(changed_lines) AS changed_lines
                FROM gold.fact_commit_employee
                WHERE commit_month >= ? AND committed_at >= ?::TIMESTAMPTZ
                GROUP BY 1,2,3,4,5
                ORDER BY commit_count DESC
                {limit_sql}
                """,
                params,
            ).fetchall()
        return [
            "full_name",
            "employee_id",
            "department_level2_name",
            "department_level3_name",
            "role",
            "commit_count",
            "changed_lines",
        ], rows

    def repo_member_commits(self, months: int, top: int = 100):
        columns, rows = self.repo_member_commits_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def repo_member_commits_data(self, months: int, top: int | None = 100) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        # Unsaturated threshold (business framing): >= 6 commits/month is a basic "in the loop" signal.
        unsat_commit_min = max(1, int(months) * 6)
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                  r.repo_name,
                  COALESCE(r.department_level2_name, 'Unassigned') AS department_level2_name,
                  COALESCE(r.department_level3_name, 'Unassigned') AS department_level3_name,
                  COALESCE(m.full_name, e.full_name, '') AS full_name,
                  COALESCE(m.username, c.author_username, c.member_key) AS member,
                  c.member_key,
                  arg_max(COALESCE(NULLIF(c.author_email,''), NULLIF(m.email,''), ''), c.committed_at) AS author_email,
                  COALESCE(m.department_level2_name, d2.name, 'Unassigned') AS member_department_level2_name,
                  COALESCE(m.department_level3_name, d3.name, 'Unassigned') AS member_department_level3_name,
                  COUNT(*) AS commit_count,
                  SUM(c.changed_lines) AS changed_lines
                FROM gold.fact_commit c
                JOIN gold.dim_repo r ON r.repo_id = c.repo_id
                LEFT JOIN gold.dim_member m ON m.member_key = c.member_key
                LEFT JOIN gold.bridge_employee_identity b
                  ON b.kind = 'email' AND b.identity_l = LOWER(NULLIF(c.author_email,''))
                LEFT JOIN gold.dim_member_enrichment e ON e.employee_id = b.employee_id
                LEFT JOIN gold.dim_department_level3 d3 ON d3.department_level3_id = e.department_level3_id
                LEFT JOIN gold.dim_department_level2 d2
                  ON d2.department_level2_id = COALESCE(e.department_level2_id, d3.department_level2_id)
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                GROUP BY 1,2,3,4,5,6,8,9
                ORDER BY commit_count DESC
                {limit_sql}
                """,
                params,
            ).fetchall()
        return [
            "repo",
            "repo_department_level2_name",
            "repo_department_level3_name",
            "full_name",
            "member",
            "member_key",
            "author_email",
            "member_department_level2_name",
            "member_department_level3_name",
            "commit_count",
            "changed_lines",
        ], rows

    def repo_employee_commits(self, months: int, top: int = 100):
        columns, rows = self.repo_employee_commits_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def repo_employee_commits_data(self, months: int, top: int | None = 100) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                  r.repo_name,
                  COALESCE(r.department_level2_name, 'Unassigned') AS repo_department_level2_name,
                  COALESCE(r.department_level3_name, 'Unassigned') AS repo_department_level3_name,
                  COALESCE(c.full_name, '') AS full_name,
                  COALESCE(c.employee_id, '') AS employee_id,
                  COALESCE(c.department_level2_name, 'Unassigned') AS department_level2_name,
                  COALESCE(c.department_level3_name, 'Unassigned') AS department_level3_name,
                  COALESCE(c.role, '') AS role,
                  COUNT(*) AS commit_count,
                  SUM(c.changed_lines) AS changed_lines
                FROM gold.fact_commit_employee c
                JOIN gold.dim_repo r ON r.repo_id = c.repo_id
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                GROUP BY 1,2,3,4,5,6,7,8
                ORDER BY commit_count DESC
                {limit_sql}
                """,
                params,
            ).fetchall()
        return [
            "repo",
            "repo_department_level2_name",
            "repo_department_level3_name",
            "full_name",
            "employee_id",
            "department_level2_name",
            "department_level3_name",
            "role",
            "commit_count",
            "changed_lines",
        ], rows

    def external_committers(self, months: int, top: int = 200):
        columns, rows = self.external_committers_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def external_committers_data(self, months: int, top: int | None = 200) -> tuple[list[str], list[tuple]]:
        """
        Export commit authors (by author_email) who have commits in the window but cannot be mapped to employee_id.
        Mapping uses gold.bridge_employee_identity with kind='email'.
        """
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                WITH window_commits AS (
                  SELECT *
                  FROM gold.fact_commit
                  WHERE commit_month >= ? AND committed_at >= ?::TIMESTAMPTZ
                    AND NULLIF(author_email,'') IS NOT NULL
                ),
                unmapped_commits AS (
                  SELECT c.*
                  FROM window_commits c
                  LEFT JOIN gold.bridge_employee_identity b
                    ON b.kind = 'email' AND b.identity_l = LOWER(NULLIF(c.author_email,''))
                  WHERE b.employee_id IS NULL
                ),
                per_repo AS (
                  SELECT
                    LOWER(NULLIF(c.author_email,'')) AS author_email_l,
                    c.repo_id,
                    COUNT(*)::BIGINT AS commit_count_repo,
                    SUM(c.changed_lines)::BIGINT AS changed_lines_repo,
                    MAX(c.committed_at) AS last_committed_at_repo
                  FROM unmapped_commits c
                  GROUP BY 1,2
                ),
                top_repo AS (
                  SELECT author_email_l, repo_id
                  FROM (
                    SELECT
                      *,
                      ROW_NUMBER() OVER (
                        PARTITION BY author_email_l
                        ORDER BY commit_count_repo DESC, changed_lines_repo DESC, last_committed_at_repo DESC, repo_id ASC
                      ) AS rn
                    FROM per_repo
                  )
                  WHERE rn = 1
                ),
                per_author AS (
                  SELECT
                    LOWER(NULLIF(c.author_email,'')) AS author_email_l,
                    arg_max(NULLIF(c.author_email,''), c.committed_at) AS author_email,
                    CASE
                      WHEN regexp_matches(LOWER(NULLIF(c.author_email,'')), '^(?:[a-z0-9]+\\.[a-z0-9]+|[0-9]+)@clife\\.cn$')
                        THEN 1 ELSE 0 END AS is_clife_email,
                    COUNT(*)::BIGINT AS commit_count,
                    COUNT(DISTINCT c.repo_id)::BIGINT AS repo_count,
                    SUM(c.changed_lines)::BIGINT AS changed_lines,
                    CAST(MIN(c.committed_at) AS VARCHAR) AS first_committed_at,
                    CAST(MAX(c.committed_at) AS VARCHAR) AS last_committed_at
                  FROM unmapped_commits c
                  GROUP BY 1,3
                )
                SELECT
                  a.author_email_l,
                  a.author_email,
                  a.is_clife_email,
                  a.commit_count,
                  a.repo_count,
                  a.changed_lines,
                  a.first_committed_at,
                  a.last_committed_at,
                  tr.repo_id AS main_repo_id,
                  COALESCE(r.repo_name, '') AS main_repo_name,
                  COALESCE(r.department_level2_name, 'Unassigned') AS main_repo_department_level2_name,
                  COALESCE(r.department_level3_name, 'Unassigned') AS main_repo_department_level3_name
                FROM per_author a
                LEFT JOIN top_repo tr ON tr.author_email_l = a.author_email_l
                LEFT JOIN gold.dim_repo r ON r.repo_id = tr.repo_id
                ORDER BY a.commit_count DESC
                {limit_sql}
                """,
                params,
            ).fetchall()
        return [
            "author_email_l",
            "author_email",
            "is_clife_email",
            "commit_count",
            "repo_count",
            "changed_lines",
            "first_committed_at",
            "last_committed_at",
            "main_repo_id",
            "main_repo_name",
            "main_repo_department_level2_name",
            "main_repo_department_level3_name",
        ], rows

    def inactive_members(self, months: int, top: int | None = 100, *, all_fields: bool = False):
        columns, rows = self.inactive_members_data(months=months, top=top, all_fields=all_fields)
        return to_rich_table(columns, rows)

    def inactive_members_data(
        self, months: int, top: int | None = 100, *, all_fields: bool = False
    ) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                WITH {self._employees_cte_sql()},
                employee_one_id AS (
                  SELECT
                    one_id,
                    MAX(NULLIF(TRIM(employee_id), '')) AS employee_id_any
                  FROM employees
                  GROUP BY 1
                ),
                resolved AS (
                  SELECT
                    COALESCE(e0.member_key, em.member_key, eu.member_key) AS member_key
                  FROM gold.fact_commit c
                  LEFT JOIN employees e0 ON e0.member_key = c.member_key
                  LEFT JOIN emp_email_map em
                    ON em.email_l = LOWER(NULLIF(c.author_email,''))
                   AND e0.member_key IS NULL
                  LEFT JOIN emp_username_map eu
                    ON eu.username_l = LOWER(NULLIF(c.author_username,''))
                   AND e0.member_key IS NULL
                   AND em.member_key IS NULL
                  WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                    AND COALESCE(e0.member_key, em.member_key, eu.member_key) IS NOT NULL
                    AND COALESCE(e0.member_key, em.member_key, eu.member_key) <> ''
                ),
                active_people AS (
                  SELECT DISTINCT
                    COALESCE(o.employee_id_any, e.one_id) AS person_id
                  FROM resolved r
                  JOIN employees e ON e.member_key = r.member_key
                  LEFT JOIN employee_one_id o ON o.one_id = e.one_id
                ),
                employee_people AS (
                  SELECT
                    COALESCE(o.employee_id_any, e.one_id) AS person_id,
                    arg_min(
                      COALESCE(NULLIF(TRIM(e.username), ''), NULLIF(TRIM(e.email), ''), e.member_key),
                      CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END
                    ) AS member,
                    arg_min(e.full_name, CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS full_name,
                    arg_min(COALESCE(e.department_level1_name, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS department_level1_name,
                    arg_min(COALESCE(e.department_level2_id, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS department_level2_id,
                    arg_min(COALESCE(e.department_level2_name, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS department_level2_name,
                    arg_min(COALESCE(e.department_level3_id, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS department_level3_id,
                    arg_min(COALESCE(e.department_level3_name, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS department_level3_name,
                    arg_min(COALESCE(e.role, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS role,
                    arg_min(COALESCE(o.employee_id_any, COALESCE(e.employee_id, '')), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS employee_id,
                    arg_min(COALESCE(e.employee_type, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS employee_type,
                    arg_min(COALESCE(e.position, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS position,
                    arg_min(COALESCE(e.in_date, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS in_date,
                    arg_min(COALESCE(e.gender, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS gender,
                    arg_min(e.age, CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS age,
                    arg_min(e.years_of_service, CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS years_of_service,
                    arg_min(COALESCE(e.job_sequence, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS job_sequence,
                    arg_min(COALESCE(e.job_rank, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS job_rank,
                    arg_min(COALESCE(e.line_manager, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS line_manager,
                    arg_min(COALESCE(e.education_level, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS education_level,
                    arg_min(COALESCE(e.collodge, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS collodge,
                    arg_min(COALESCE(e.major, ''), CASE WHEN e.member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS major
                  FROM employees e
                  LEFT JOIN employee_one_id o ON o.one_id = e.one_id
                  GROUP BY 1
                )
                SELECT
                  ep.member AS member,
                  {"""
                  ep.employee_id AS employee_id,
                  ep.full_name AS full_name,
                  ep.department_level1_name AS department_level1_name,
                  ep.department_level2_id AS department_level2_id,
                  ep.department_level2_name AS department_level2_name,
                  ep.department_level3_id AS department_level3_id,
                  ep.department_level3_name AS department_level3_name,
                  ep.role AS role,
                  ep.employee_type AS employee_type,
                  ep.position AS position,
                  ep.in_date AS in_date,
                  ep.gender AS gender,
                  ep.age AS age,
                  ep.years_of_service AS years_of_service,
                  ep.job_sequence AS job_sequence,
                  ep.job_rank AS job_rank,
                  ep.line_manager AS line_manager,
                  ep.education_level AS education_level,
                  ep.collodge AS collodge,
                  ep.major AS major,
                  0::BIGINT AS commit_count
                  """ if all_fields else """
                  ep.full_name AS full_name,
                  ep.department_level2_name AS department_level2_name,
                  ep.department_level3_name AS department_level3_name,
                  0::BIGINT AS commit_count
                  """}
                FROM employee_people ep
                LEFT JOIN active_people ap ON ap.person_id = ep.person_id
                WHERE ap.person_id IS NULL
                ORDER BY department_level2_name, department_level3_name, full_name, member
                {limit_sql}
                """,
                params,
            ).fetchall()
        if all_fields:
            return [
                "member",
                "employee_id",
                "full_name",
                "department_level1_name",
                "department_level2_id",
                "department_level2_name",
                "department_level3_id",
                "department_level3_name",
                "role",
                "employee_type",
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
                "commit_count",
            ], rows
        return ["member", "full_name", "department_level2_name", "department_level3_name", "commit_count"], rows

    def active_members(self, months: int, top: int | None = 2000, *, all_fields: bool = False):
        columns, rows = self.active_members_data(months=months, top=top, all_fields=all_fields)
        return to_rich_table(columns, rows)

    def active_members_data(
        self, months: int, top: int | None = 2000, *, all_fields: bool = False
    ) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                WITH {self._employees_cte_sql()},
                employee_one_id AS (
                  SELECT
                    one_id,
                    MAX(NULLIF(TRIM(employee_id), '')) AS employee_id_any
                  FROM employees
                  GROUP BY 1
                ),
                resolved AS (
                  SELECT
                    COALESCE(e0.member_key, em.member_key, eu.member_key) AS member_key
                  FROM gold.fact_commit c
                  LEFT JOIN employees e0 ON e0.member_key = c.member_key
                  LEFT JOIN emp_email_map em
                    ON em.email_l = LOWER(NULLIF(c.author_email,''))
                   AND e0.member_key IS NULL
                  LEFT JOIN emp_username_map eu
                    ON eu.username_l = LOWER(NULLIF(c.author_username,''))
                   AND e0.member_key IS NULL
                   AND em.member_key IS NULL
                  WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                    AND COALESCE(e0.member_key, em.member_key, eu.member_key) IS NOT NULL
                    AND COALESCE(e0.member_key, em.member_key, eu.member_key) <> ''
                ),
                per_member AS (
                  SELECT
                    COALESCE(o.employee_id_any, e.one_id) AS person_id,
                    e.member_key,
                    COALESCE(NULLIF(TRIM(e.username), ''), NULLIF(TRIM(e.email), ''), e.member_key) AS member,
                    e.full_name,
                    COALESCE(e.department_level1_name, '') AS department_level1_name,
                    COALESCE(e.department_level2_id, '') AS department_level2_id,
                    COALESCE(e.department_level2_name, '') AS department_level2_name,
                    COALESCE(e.department_level3_id, '') AS department_level3_id,
                    COALESCE(e.department_level3_name, '') AS department_level3_name,
                    COALESCE(e.role, '') AS role,
                    COALESCE(o.employee_id_any, COALESCE(e.employee_id, '')) AS employee_id,
                    COALESCE(e.employee_type, '') AS employee_type,
                    COALESCE(e.position, '') AS position,
                    COALESCE(e.in_date, '') AS in_date,
                    COALESCE(e.gender, '') AS gender,
                    e.age AS age,
                    e.years_of_service AS years_of_service,
                    COALESCE(e.job_sequence, '') AS job_sequence,
                    COALESCE(e.job_rank, '') AS job_rank,
                    COALESCE(e.line_manager, '') AS line_manager,
                    COALESCE(e.education_level, '') AS education_level,
                    COALESCE(e.collodge, '') AS collodge,
                    COALESCE(e.major, '') AS major,
                    COUNT(*)::BIGINT AS commit_count
                  FROM resolved
                  JOIN employees e ON e.member_key = resolved.member_key
                  LEFT JOIN employee_one_id o ON o.one_id = e.one_id
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
                ),
                per_person AS (
                  SELECT
                    person_id,
                    arg_max(member, commit_count) AS member,
                    arg_max(employee_id, commit_count) AS employee_id,
                    arg_max(full_name, commit_count) AS full_name,
                    arg_max(department_level1_name, commit_count) AS department_level1_name,
                    arg_max(department_level2_id, commit_count) AS department_level2_id,
                    arg_max(department_level2_name, commit_count) AS department_level2_name,
                    arg_max(department_level3_id, commit_count) AS department_level3_id,
                    arg_max(department_level3_name, commit_count) AS department_level3_name,
                    arg_max(role, commit_count) AS role,
                    arg_max(employee_type, commit_count) AS employee_type,
                    arg_max(position, commit_count) AS position,
                    arg_max(in_date, commit_count) AS in_date,
                    arg_max(gender, commit_count) AS gender,
                    arg_max(age, commit_count) AS age,
                    arg_max(years_of_service, commit_count) AS years_of_service,
                    arg_max(job_sequence, commit_count) AS job_sequence,
                    arg_max(job_rank, commit_count) AS job_rank,
                    arg_max(line_manager, commit_count) AS line_manager,
                    arg_max(education_level, commit_count) AS education_level,
                    arg_max(collodge, commit_count) AS collodge,
                    arg_max(major, commit_count) AS major,
                    SUM(commit_count)::BIGINT AS commit_count
                  FROM per_member
                  GROUP BY 1
                )
                SELECT
                  p.member AS member,
                  {"""
                  p.employee_id AS employee_id,
                  p.full_name AS full_name,
                  p.department_level1_name AS department_level1_name,
                  p.department_level2_id AS department_level2_id,
                  p.department_level2_name AS department_level2_name,
                  p.department_level3_id AS department_level3_id,
                  p.department_level3_name AS department_level3_name,
                  p.role AS role,
                  p.employee_type AS employee_type,
                  p.position AS position,
                  p.in_date AS in_date,
                  p.gender AS gender,
                  p.age AS age,
                  p.years_of_service AS years_of_service,
                  p.job_sequence AS job_sequence,
                  p.job_rank AS job_rank,
                  p.line_manager AS line_manager,
                  p.education_level AS education_level,
                  p.collodge AS collodge,
                  p.major AS major,
                  p.commit_count AS commit_count
                  """ if all_fields else """
                  p.full_name AS full_name,
                  p.department_level2_name AS department_level2_name,
                  p.department_level3_name AS department_level3_name,
                  p.commit_count AS commit_count
                  """}
                FROM per_person p
                WHERE p.commit_count > 0
                ORDER BY p.commit_count DESC
                {limit_sql}
                """,
                params,
            ).fetchall()
        if all_fields:
            return [
                "member",
                "employee_id",
                "full_name",
                "department_level1_name",
                "department_level2_id",
                "department_level2_name",
                "department_level3_id",
                "department_level3_name",
                "role",
                "employee_type",
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
                "commit_count",
            ], rows
        return ["member", "full_name", "department_level2_name", "department_level3_name", "commit_count"], rows

    def missing_fullname_authors(self, months: int, top: int | None = 200):
        columns, rows = self.missing_fullname_authors_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def missing_fullname_authors_data(self, months: int, top: int | None = 200) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                WITH {self._global_member_identity_maps_cte_sql()},
                resolved AS (
                  SELECT
                    {self._global_member_key_expr()} AS member_key,
                    c.repo_id
                  FROM gold.fact_commit c
                  LEFT JOIN member_keys mk ON mk.member_key = c.member_key
                  LEFT JOIN email_map em
                    ON em.email_l = LOWER(NULLIF(c.author_email,''))
                   AND mk.member_key IS NULL
                  LEFT JOIN username_map um
                    ON um.username_l = LOWER(NULLIF(c.author_username,''))
                   AND mk.member_key IS NULL
                   AND em.member_key IS NULL
                  WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                )
                SELECT
                  r.member_key,
                  COALESCE(m.username, '') AS username,
                  COALESCE(m.email, '') AS email,
                  COUNT(*)::BIGINT AS commit_count,
                  COUNT(DISTINCT r.repo_id)::BIGINT AS repo_count
                FROM resolved r
                JOIN gold.dim_member m ON m.member_key = r.member_key
                WHERE r.member_key IS NOT NULL AND r.member_key <> ''
                  AND NULLIF(TRIM(m.full_name), '') IS NULL
                GROUP BY 1,2,3
                ORDER BY commit_count DESC
                {limit_sql}
                """,
                params,
            ).fetchall()
        return ["member_key", "username", "email", "commit_count", "repo_count"], rows

    def line_manager_dev_activity(self, months: int, top: int | None = 200):
        columns, rows = self.line_manager_dev_activity_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def suspicious_committers(self, months: int, top: int | None = 200):
        columns, rows = self.suspicious_committers_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def active_employee_score(self, months: int, top: int | None = 200):
        columns, rows = self.active_employee_score_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def active_employee_score_data(
        self,
        months: int,
        top: int | None = 200,
    ) -> tuple[list[str], list[tuple]]:
        """
        活跃员工综合评分（用于榜单/雷达图/报告）。
        评分口径：活跃、贡献强度、变更强度、人均产出、人均变更为主；
        诚信(反刷)、奋斗者文化次之；其他维度权重较低。
        """
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        # Unsaturated threshold (business framing): >= 6 commits/month is a basic "in the loop" signal.
        # Used in score_total gating for stronger penalties when under-saturated.
        unsat_commit_min = max(1, int(months) * 6)
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))

        role_weight_sql = self._role_change_weight_case_sql("e.role")

        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                WITH {self._employees_cte_sql()},
                window_commits AS (
                  SELECT
                    c.repo_id,
                    c.commit_sha,
                    c.member_key,
                    c.author_username,
                    c.author_email,
                    c.committed_at,
                    c.additions,
                    c.deletions,
                    c.changed_lines,
                    COALESCE(cs.is_merge, FALSE) AS is_merge,
                    COALESCE(
                      NULLIF(json_extract_string(sc.raw, '$.commit.message'), ''),
                      NULLIF(json_extract_string(sc.raw, '$.message'), ''),
                      ''
                    ) AS message
                  FROM gold.fact_commit c
                  LEFT JOIN silver.commit_stats cs
                    ON cs.repo_id = c.repo_id AND cs.sha = c.commit_sha
                  LEFT JOIN silver.commits sc
                    ON sc.repo_id = c.repo_id AND sc.sha = c.commit_sha
                  WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                ),
                resolved AS (
                  SELECT
                    COALESCE(e0.member_key, em.member_key, eu.member_key) AS emp_member_key,
                    wc.*
                  FROM window_commits wc
                  LEFT JOIN employees e0 ON e0.member_key = wc.member_key
                  LEFT JOIN emp_email_map em
                    ON em.email_l = LOWER(NULLIF(wc.author_email,''))
                   AND e0.member_key IS NULL
                  LEFT JOIN emp_username_map eu
                    ON eu.username_l = LOWER(NULLIF(wc.author_username,''))
                   AND e0.member_key IS NULL
                   AND em.member_key IS NULL
                  WHERE COALESCE(e0.member_key, em.member_key, eu.member_key) IS NOT NULL
                    AND COALESCE(e0.member_key, em.member_key, eu.member_key) <> ''
                ),
                commit_enriched AS (
                  SELECT
                    COALESCE(NULLIF(TRIM(e.employee_id), ''), e.member_key) AS person_id,
                    e.employee_id,
                    e.full_name,
                    e.department_level2_name,
                    e.department_level3_name,
                    e.role,
                    COALESCE(NULLIF(TRIM(e.line_manager), ''), 'Unassigned') AS line_manager,
                    {role_weight_sql} AS role_change_weight,
                    r.repo_id,
                    r.commit_sha,
                    r.committed_at,
                    r.additions,
                    r.deletions,
                    r.changed_lines,
                    (COALESCE(r.changed_lines, 0)::DOUBLE * {role_weight_sql}) AS weighted_changed_lines,
                    r.is_merge,
                    regexp_replace(LOWER(TRIM(COALESCE(r.message, ''))), '\\\\s+', ' ', 'g') AS message_norm,
                    CASE
                      WHEN (date_part('isodow', r.committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 1 AND 5)
                       AND (date_part('hour', r.committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 9 AND 18)
                      THEN 0 ELSE 1 END AS is_after_hours
                  FROM resolved r
                  JOIN employees e ON e.member_key = r.emp_member_key
                ),
                non_merge_commits AS (
                  SELECT * FROM commit_enriched WHERE NOT is_merge
                ),
                per_person_base AS (
                  SELECT
                    person_id,
                    MIN(NULLIF(TRIM(employee_id), '')) AS employee_id,
                    MIN(full_name) AS full_name,
                    MIN(department_level2_name) AS department_level2_name,
                    MIN(department_level3_name) AS department_level3_name,
                    MIN(role) AS role,
                    MIN(line_manager) AS line_manager,
                    COUNT(*)::BIGINT AS commit_count,
                    COUNT(DISTINCT repo_id)::BIGINT AS repo_count,
                    SUM(changed_lines)::BIGINT AS total_changed_lines,
                    SUM(weighted_changed_lines)::DOUBLE AS total_weighted_changed_lines,
                    (SUM(changed_lines)::DOUBLE / NULLIF(COUNT(*), 0)) AS changed_lines_per_commit,
                    (SUM(weighted_changed_lines)::DOUBLE / NULLIF(COUNT(*), 0)) AS weighted_changed_lines_per_commit,
                    quantile_cont(changed_lines, 0.5) AS median_changed_lines,
                    quantile_cont(weighted_changed_lines, 0.5) AS median_weighted_changed_lines,
                    SUM(is_after_hours)::BIGINT AS after_hours_commit_count,
                    (SUM(is_after_hours)::DOUBLE / NULLIF(COUNT(*), 0)) AS after_hours_ratio,
                    (SUM(CASE WHEN changed_lines = 0 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0)) AS p0_zero,
                    (SUM(CASE WHEN changed_lines <= 2 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0)) AS p2_tiny,
                    (SUM(
                      CASE
                        WHEN changed_lines >= 50
                         AND (1 - (ABS(COALESCE(additions,0) - COALESCE(deletions,0))::DOUBLE / NULLIF(COALESCE(additions,0) + COALESCE(deletions,0), 0))) >= 0.9
                        THEN 1 ELSE 0 END
                    )::DOUBLE / NULLIF(COUNT(*), 0)) AS p_balance_high
                  FROM non_merge_commits
                  GROUP BY 1
                ),
                per_person_repo AS (
                  SELECT
                    person_id,
                    MAX(repo_commits)::DOUBLE / NULLIF(SUM(repo_commits), 0) AS top1_repo_share
                  FROM (
                    SELECT person_id, repo_id, COUNT(*)::BIGINT AS repo_commits
                    FROM non_merge_commits
                    GROUP BY 1,2
                  ) t
                  GROUP BY 1
                ),
                per_person_burst AS (
                  SELECT
                    person_id,
                    MAX(cnt_10m)::BIGINT AS max_commits_10m
                  FROM (
                    SELECT
                      person_id,
                      CAST(FLOOR(EPOCH(committed_at) / 600) AS BIGINT) AS b10m,
                      COUNT(*)::BIGINT AS cnt_10m
                    FROM non_merge_commits
                    GROUP BY 1,2
                  ) t
                  GROUP BY 1
                ),
                per_person_msg AS (
                  SELECT
                    person_id,
                    COUNT(*)::BIGINT AS msg_total,
                    COUNT(DISTINCT NULLIF(message_norm, ''))::BIGINT AS msg_unique,
                    MAX(msg_cnt)::BIGINT AS msg_top1_cnt
                  FROM (
                    SELECT
                      person_id,
                      message_norm,
                      COUNT(*)::BIGINT AS msg_cnt
                    FROM non_merge_commits
                    WHERE NULLIF(message_norm, '') IS NOT NULL
                    GROUP BY 1,2
                  ) x
                  GROUP BY 1
                ),
                joined AS (
                  SELECT
                    b.*,
                    COALESCE(r.top1_repo_share, 0) AS top1_repo_share,
                    COALESCE(bt.max_commits_10m, 0) AS max_commits_10m,
                    COALESCE(m.msg_total, 0) AS msg_total,
                    COALESCE(m.msg_unique, 0) AS msg_unique,
                    COALESCE(m.msg_top1_cnt, 0) AS msg_top1_cnt,
                    (COALESCE(m.msg_unique, 0)::DOUBLE / NULLIF(COALESCE(m.msg_total, 0), 0)) AS message_unique_ratio,
                    (COALESCE(m.msg_top1_cnt, 0)::DOUBLE / NULLIF(COALESCE(m.msg_total, 0), 0)) AS top1_message_share
                  FROM per_person_base b
                  LEFT JOIN per_person_repo r ON r.person_id = b.person_id
                  LEFT JOIN per_person_burst bt ON bt.person_id = b.person_id
                  LEFT JOIN per_person_msg m ON m.person_id = b.person_id
                ),
                suspicious_ranked AS (
                  SELECT
                    j.*,
                    (100 * percent_rank() OVER (ORDER BY j.p2_tiny)) AS s_tiny,
                    (100 * percent_rank() OVER (ORDER BY j.p0_zero)) AS s_zero,
                    (100 * percent_rank() OVER (ORDER BY j.max_commits_10m)) AS s_burst,
                    (100 * percent_rank() OVER (ORDER BY j.p_balance_high)) AS s_balance,
                    (100 * (1 - percent_rank() OVER (ORDER BY j.message_unique_ratio))) AS s_template,
                    (100 * percent_rank() OVER (ORDER BY j.top1_repo_share)) AS s_single_repo,
                    (100 * (1 - percent_rank() OVER (ORDER BY j.changed_lines_per_commit))) AS s_low_intensity
                  FROM joined j
                ),
                suspicious_scored AS (
                  SELECT
                    r.*,
                    ROUND(
                      0.25 * r.s_tiny +
                      0.12 * r.s_zero +
                      0.18 * r.s_burst +
                      0.14 * r.s_balance +
                      0.10 * r.s_template +
                      0.13 * r.s_single_repo +
                      0.08 * r.s_low_intensity,
                      2
                    ) AS suspicious_score
                  FROM suspicious_ranked r
                ),
                scored AS (
                  SELECT
                    s.*,
                    ROUND(100 * percent_rank() OVER (ORDER BY s.commit_count), 2) AS score_active,
                    ROUND(100 * percent_rank() OVER (ORDER BY LN(1 + COALESCE(s.total_weighted_changed_lines, 0))), 2) AS score_lines_total,
                    ROUND(100 * percent_rank() OVER (ORDER BY LN(1 + COALESCE(s.median_weighted_changed_lines, 0))), 2) AS score_lines_p50,
                    ROUND(100 * percent_rank() OVER (ORDER BY LN(1 + COALESCE(s.weighted_changed_lines_per_commit, 0))), 2) AS score_lines_per_commit,
                    ROUND(100 * percent_rank() OVER (ORDER BY s.repo_count), 2) AS score_repo_diversity,
                    ROUND(100 * (1 - percent_rank() OVER (ORDER BY s.top1_repo_share)), 2) AS score_concentration,
                    ROUND(100 * percent_rank() OVER (ORDER BY s.message_unique_ratio), 2) AS score_message_quality,
                    ROUND(100 * percent_rank() OVER (ORDER BY s.after_hours_ratio), 2) AS score_after_hours,
                    ROUND(100 * (1 - percent_rank() OVER (ORDER BY s.suspicious_score)), 2) AS score_integrity
                  FROM suspicious_scored s
                )
                SELECT
                  employee_id,
                  person_id,
                  full_name,
                  department_level2_name,
                  department_level3_name,
                  role,
                  line_manager,
                  commit_count,
                  repo_count,
                  total_changed_lines,
                  ROUND(total_weighted_changed_lines, 2) AS total_weighted_changed_lines,
                  ROUND(changed_lines_per_commit, 2) AS changed_lines_per_commit,
                  ROUND(weighted_changed_lines_per_commit, 2) AS weighted_changed_lines_per_commit,
                  median_changed_lines,
                  ROUND(median_weighted_changed_lines, 2) AS median_weighted_changed_lines,
                  ROUND(after_hours_ratio, 4) AS after_hours_ratio,
                  ROUND(message_unique_ratio, 4) AS message_unique_ratio,
                  ROUND(top1_repo_share, 4) AS top1_repo_share,
	                  -- Total score focuses on contribution quantity, but avoids "few giant commits" bias:
	                  -- - Primary: score_lines_total
	                  -- - Gate: commit_count (extra penalty when under-saturated, then follow original staged rewards)
	                  -- - Small bonus: multi-repo maintenance (capped)
	                  ROUND(
	                    LEAST(
	                      100.0,
	                      score_lines_total
	                      * (
	                        CASE
	                          WHEN commit_count < {unsat_commit_min} THEN (0.5 + 0.3 * (commit_count::DOUBLE / {unsat_commit_min}))
	                          ELSE (0.8 + 0.2 * LEAST(1.0, commit_count::DOUBLE / 20.0))
	                        END
	                      )
	                      + (0.05 * LEAST(score_repo_diversity, 70.0))
	                    ),
	                    2
	                  ) AS score_total,
                  ROUND(score_active, 2) AS score_active,
                  ROUND(score_lines_total, 2) AS score_lines_total,
                  ROUND(score_lines_p50, 2) AS score_lines_p50,
                  ROUND(score_lines_per_commit, 2) AS score_lines_per_commit,
                  ROUND(score_repo_diversity, 2) AS score_repo_diversity,
                  ROUND(score_message_quality, 2) AS score_message_quality,
                  ROUND(score_integrity, 2) AS score_integrity,
                  ROUND(score_after_hours, 2) AS score_after_hours,
                  ROUND(score_concentration, 2) AS score_concentration,
                  ROUND(suspicious_score, 2) AS suspicious_score
                FROM scored
                WHERE commit_count > 0
                ORDER BY score_total DESC, total_changed_lines DESC, commit_count DESC, full_name
                {limit_sql}
                """,
                params,
            ).fetchall()

        return [
            "employee_id",
            "person_id",
            "full_name",
            "department_level2_name",
            "department_level3_name",
            "role",
            "line_manager",
            "commit_count",
            "repo_count",
            "total_changed_lines",
            "total_weighted_changed_lines",
            "changed_lines_per_commit",
            "weighted_changed_lines_per_commit",
            "median_changed_lines",
            "median_weighted_changed_lines",
            "after_hours_ratio",
            "message_unique_ratio",
            "top1_repo_share",
            "score_total",
            "score_active",
            "score_lines_total",
            "score_lines_p50",
            "score_lines_per_commit",
            "score_repo_diversity",
            "score_message_quality",
            "score_integrity",
            "score_after_hours",
            "score_concentration",
            "suspicious_score",
        ], rows

    def suspicious_committers_data(
        self,
        months: int,
        top: int | None = 200,
    ) -> tuple[list[str], list[tuple]]:
        """
        反刷 commit 启发式：识别“数量很好看，但价值/协作/复杂度很可疑”的模式。
        仅使用现有 commit 采集字段，不依赖 PR/Issue。
        """
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        # Unsaturated threshold (business framing):
        # Use a simple monthly floor to flag "under_saturated" participation for dev roles.
        # For months=2, this is 12 commits (6 commits/month).
        unsat_commit_min = max(1, int(months) * 6)
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))

        dev_roles_sql = (
            "('Java 后台开发','Web 前端开发','终端开发','算法开发','数据开发','全栈开发')"
        )

        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                WITH {self._employees_cte_sql()},
                window_commits AS (
                  SELECT
                    c.repo_id,
                    c.commit_sha,
                    c.member_key,
                    c.author_username,
                    c.author_email,
                    c.committed_at,
                    c.additions,
                    c.deletions,
                    c.changed_lines,
                    COALESCE(cs.is_merge, FALSE) AS is_merge,
                    COALESCE(
                      NULLIF(json_extract_string(sc.raw, '$.commit.message'), ''),
                      NULLIF(json_extract_string(sc.raw, '$.message'), ''),
                      NULLIF(json_extract_string(sc.raw, '$.commit.title'), ''),
                      ''
                    ) AS message
                  FROM gold.fact_commit c
                  LEFT JOIN silver.commit_stats cs
                    ON cs.repo_id = c.repo_id AND cs.sha = c.commit_sha
                  LEFT JOIN silver.commits sc
                    ON sc.repo_id = c.repo_id AND sc.sha = c.commit_sha
                  WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                ),
                resolved AS (
                  SELECT
                    COALESCE(e0.member_key, em.member_key, eu.member_key) AS emp_member_key,
                    wc.*
                  FROM window_commits wc
                  LEFT JOIN employees e0 ON e0.member_key = wc.member_key
                  LEFT JOIN emp_email_map em
                    ON em.email_l = LOWER(NULLIF(wc.author_email,''))
                   AND e0.member_key IS NULL
                  LEFT JOIN emp_username_map eu
                    ON eu.username_l = LOWER(NULLIF(wc.author_username,''))
                   AND e0.member_key IS NULL
                   AND em.member_key IS NULL
                  WHERE COALESCE(e0.member_key, em.member_key, eu.member_key) IS NOT NULL
                    AND COALESCE(e0.member_key, em.member_key, eu.member_key) <> ''
                ),
                commit_enriched AS (
                  SELECT
                    COALESCE(NULLIF(TRIM(e.employee_id), ''), e.member_key) AS person_id,
                    e.employee_id,
                    e.full_name,
                    e.department_level2_name,
                    e.department_level3_name,
                    e.role,
                    COALESCE(NULLIF(TRIM(e.line_manager), ''), 'Unassigned') AS line_manager,
                    r.repo_id,
                    r.commit_sha,
                    r.committed_at,
                    r.additions,
                    r.deletions,
                    r.changed_lines,
                    r.is_merge,
                    regexp_replace(LOWER(TRIM(COALESCE(r.message, ''))), '\\\\s+', ' ', 'g') AS message_norm
                  FROM resolved r
                  JOIN employees e ON e.member_key = r.emp_member_key
                ),
                non_merge_commits AS (
                  SELECT * FROM commit_enriched WHERE NOT is_merge
                ),
                per_person_base AS (
                  SELECT
                    person_id,
                    MIN(NULLIF(TRIM(employee_id), '')) AS employee_id,
                    MIN(full_name) AS full_name,
                    MIN(department_level2_name) AS department_level2_name,
                    MIN(department_level3_name) AS department_level3_name,
                    MIN(role) AS role,
                    MIN(line_manager) AS line_manager,
                    COUNT(*)::BIGINT AS commit_count,
                    COUNT(DISTINCT repo_id)::BIGINT AS repo_count,
                    SUM(changed_lines)::BIGINT AS total_changed_lines,
                    (SUM(changed_lines)::DOUBLE / NULLIF(COUNT(*), 0)) AS changed_lines_per_commit,
                    quantile_cont(changed_lines, 0.5) AS median_changed_lines,
                    (SUM(CASE WHEN changed_lines = 0 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0)) AS p0_zero,
                    (SUM(CASE WHEN changed_lines <= 2 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0)) AS p2_tiny,
                    (SUM(CASE WHEN changed_lines <= 10 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0)) AS p10_small,
                    (SUM(
                      CASE
                        WHEN changed_lines >= 50
                         AND (1 - (ABS(COALESCE(additions,0) - COALESCE(deletions,0))::DOUBLE / NULLIF(COALESCE(additions,0) + COALESCE(deletions,0), 0))) >= 0.9
                        THEN 1 ELSE 0 END
                    )::DOUBLE / NULLIF(COUNT(*), 0)) AS p_balance_high,
                    (SUM(
                      CASE
                        WHEN (date_part('isodow', committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 1 AND 5)
                         AND (date_part('hour', committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 9 AND 18)
                        THEN 0 ELSE 1 END
                    )::DOUBLE / NULLIF(COUNT(*), 0)) AS off_hours_ratio
                  FROM non_merge_commits
                  GROUP BY 1
                ),
                per_repo_stats AS (
                  SELECT
                    repo_id,
                    COUNT(*)::BIGINT AS repo_commit_total,
                    COUNT(DISTINCT person_id)::BIGINT AS repo_person_cnt
                  FROM non_merge_commits
                  GROUP BY 1
                ),
                repo_thresholds AS (
                  SELECT
                    quantile_cont(repo_person_cnt, 0.75) AS repo_people_p75,
                    quantile_cont(repo_commit_total, 0.75) AS repo_commits_p75
                  FROM per_repo_stats
                ),
                per_person_repo AS (
                  SELECT
                    person_id,
                    arg_max(repo_id, repo_commits) AS top1_repo_id,
                    MAX(repo_commits)::DOUBLE / NULLIF(SUM(repo_commits), 0) AS top1_repo_share
                  FROM (
                    SELECT person_id, repo_id, COUNT(*)::BIGINT AS repo_commits
                    FROM non_merge_commits
                    GROUP BY 1,2
                  ) t
                  GROUP BY 1
                ),
                per_person_msg AS (
                  SELECT
                    person_id,
                    COUNT(*)::BIGINT AS msg_total,
                    COUNT(DISTINCT NULLIF(message_norm, ''))::BIGINT AS msg_unique,
                    MAX(msg_cnt)::BIGINT AS msg_top1_cnt
                  FROM (
                    SELECT
                      person_id,
                      message_norm,
                      COUNT(*)::BIGINT AS msg_cnt
                    FROM non_merge_commits
                    WHERE NULLIF(message_norm, '') IS NOT NULL
                    GROUP BY 1,2
                  ) x
                  GROUP BY 1
                ),
                per_person_msg2 AS (
                  SELECT
                    c.person_id,
                    (SUM(CASE WHEN LENGTH(NULLIF(c.message_norm, '')) <= 8 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(m.msg_total, 0)) AS short_message_ratio,
                    (SUM(
                      CASE WHEN regexp_matches(c.message_norm, '^(fix|update|test|wip|tmp|merge|refactor)(:|$)')
                      THEN 1 ELSE 0 END
                    )::DOUBLE / NULLIF(m.msg_total, 0)) AS generic_message_ratio
                  FROM non_merge_commits c
                  JOIN per_person_msg m ON m.person_id = c.person_id
                  WHERE NULLIF(c.message_norm, '') IS NOT NULL
                  GROUP BY 1, m.msg_total
                ),
                per_person_burst AS (
                  SELECT
                    person_id,
                    MAX(cnt_10m)::BIGINT AS max_commits_10m,
                    MAX(cnt_1h)::BIGINT AS max_commits_1h
                  FROM (
                    SELECT
                      person_id,
                      CAST(FLOOR(EPOCH(committed_at) / 600) AS BIGINT) AS b10m,
                      COUNT(*)::BIGINT AS cnt_10m,
                      CAST(FLOOR(EPOCH(committed_at) / 3600) AS BIGINT) AS b1h,
                      COUNT(*)::BIGINT AS cnt_1h
                    FROM non_merge_commits
                    GROUP BY 1,2,4
                  ) t
                  GROUP BY 1
                ),
                per_person_delta AS (
                  SELECT
                    person_id,
                    quantile_cont(delta_s, 0.5) AS median_inter_commit_seconds
                  FROM (
                    SELECT
                      person_id,
                      EPOCH(committed_at) - LAG(EPOCH(committed_at)) OVER (PARTITION BY person_id ORDER BY committed_at) AS delta_s
                    FROM non_merge_commits
                  ) d
                  WHERE delta_s IS NOT NULL AND delta_s > 0
                  GROUP BY 1
                ),
                joined AS (
                  SELECT
                    b.*,
                    r.top1_repo_id,
                    COALESCE(r.top1_repo_share, 0) AS top1_repo_share,
                    COALESCE(rs.repo_person_cnt, 0) AS top1_repo_person_cnt,
                    COALESCE(rs.repo_commit_total, 0) AS top1_repo_commit_total,
                    CASE
                      WHEN COALESCE(rs.repo_person_cnt, 0) >= rt.repo_people_p75
                        OR COALESCE(rs.repo_commit_total, 0) >= rt.repo_commits_p75
                      THEN 1 ELSE 0 END AS top1_repo_is_core,
                    COALESCE(m.msg_total, 0) AS msg_total,
                    COALESCE(m.msg_unique, 0) AS msg_unique,
                    COALESCE(m.msg_top1_cnt, 0) AS msg_top1_cnt,
                    (COALESCE(m.msg_unique, 0)::DOUBLE / NULLIF(COALESCE(m.msg_total, 0), 0)) AS message_unique_ratio,
                    (COALESCE(m.msg_top1_cnt, 0)::DOUBLE / NULLIF(COALESCE(m.msg_total, 0), 0)) AS top1_message_share,
                    COALESCE(m2.short_message_ratio, 0) AS short_message_ratio,
                    COALESCE(m2.generic_message_ratio, 0) AS generic_message_ratio,
                    COALESCE(bt.max_commits_10m, 0) AS max_commits_10m,
                    COALESCE(bt.max_commits_1h, 0) AS max_commits_1h,
                    COALESCE(d.median_inter_commit_seconds, NULL) AS median_inter_commit_seconds
                  FROM per_person_base b
                  LEFT JOIN per_person_repo r ON r.person_id = b.person_id
                  LEFT JOIN per_repo_stats rs ON rs.repo_id = r.top1_repo_id
                  CROSS JOIN repo_thresholds rt
                  LEFT JOIN per_person_msg m ON m.person_id = b.person_id
                  LEFT JOIN per_person_msg2 m2 ON m2.person_id = b.person_id
                  LEFT JOIN per_person_burst bt ON bt.person_id = b.person_id
                  LEFT JOIN per_person_delta d ON d.person_id = b.person_id
                ),
                thresholds AS (
                  SELECT
                    quantile_cont(p0_zero, 0.8) AS p0_p80,
                    quantile_cont(p2_tiny, 0.8) AS p2_p80,
                    quantile_cont(max_commits_10m, 0.8) AS burst_p80,
                    quantile_cont(p_balance_high, 0.8) AS balance_p80,
                    quantile_cont(top1_repo_share, 0.8) AS repo_p80,
                    quantile_cont(message_unique_ratio, 0.15) AS msg_unique_p15,
                    quantile_cont(off_hours_ratio, 0.8) AS off_p80
                  FROM joined
                ),
                ranked AS (
                  SELECT
                    j.*,
                    (100 * percent_rank() OVER (ORDER BY j.p2_tiny)) AS score_tiny,
                    (100 * percent_rank() OVER (ORDER BY j.p10_small)) AS score_small,
                    (100 * percent_rank() OVER (ORDER BY j.p0_zero)) AS score_zero,
                    (100 * percent_rank() OVER (ORDER BY j.max_commits_10m)) AS score_burst,
                    (100 * (1 - percent_rank() OVER (ORDER BY COALESCE(j.median_inter_commit_seconds, 999999999)))) AS score_inter_commit,
                    (100 * percent_rank() OVER (ORDER BY j.p_balance_high)) AS score_balance,
                    (100 * (1 - percent_rank() OVER (ORDER BY j.message_unique_ratio))) AS score_message,
                    (100 * percent_rank() OVER (ORDER BY j.top1_repo_share))
                      * CASE WHEN j.top1_repo_is_core = 1 THEN 0.6 ELSE 1.0 END AS score_single_repo,
                    (100 * percent_rank() OVER (ORDER BY j.off_hours_ratio)) AS score_off_hours,
                    (100 * (1 - percent_rank() OVER (ORDER BY j.changed_lines_per_commit))) AS score_low_intensity,
                    percent_rank() OVER (ORDER BY j.total_changed_lines) AS prod_rank,
                    percent_rank() OVER (ORDER BY j.changed_lines_per_commit) AS intensity_rank,
                    percent_rank() OVER (ORDER BY j.repo_count) AS repo_rank,
                    percent_rank() OVER (ORDER BY j.message_unique_ratio) AS msg_quality_rank
                  FROM joined j
                ),
                scored AS (
                  SELECT
                    r.*,
                    ROUND(
                      0.18 * r.score_tiny +
                      0.06 * r.score_small +
                      0.10 * r.score_zero +
                      0.12 * r.score_burst +
                      0.06 * r.score_inter_commit +
                      0.14 * r.score_balance +
                      0.10 * r.score_message +
                      0.10 * r.score_single_repo +
                      0.14 * r.score_low_intensity,
                      2
                    ) AS score_total_raw,
                    ROUND(
                      CASE
                        WHEN r.commit_count < 20 THEN (0.18 * r.score_tiny +
                          0.06 * r.score_small +
                          0.10 * r.score_zero +
                          0.12 * r.score_burst +
                          0.06 * r.score_inter_commit +
                          0.14 * r.score_balance +
                          0.10 * r.score_message +
                          0.10 * r.score_single_repo +
                          0.14 * r.score_low_intensity) * 0.5
                        WHEN r.prod_rank >= 0.80 OR r.intensity_rank >= 0.80 OR r.repo_rank >= 0.80 OR r.msg_quality_rank >= 0.80 THEN (
                          0.18 * r.score_tiny +
                          0.06 * r.score_small +
                          0.10 * r.score_zero +
                          0.12 * r.score_burst +
                          0.06 * r.score_inter_commit +
                          0.14 * r.score_balance +
                          0.10 * r.score_message +
                          0.10 * r.score_single_repo +
                          0.14 * r.score_low_intensity
                        ) * 0.6
                        ELSE (0.18 * r.score_tiny +
                          0.06 * r.score_small +
                          0.10 * r.score_zero +
                          0.12 * r.score_burst +
                          0.06 * r.score_inter_commit +
                          0.14 * r.score_balance +
                          0.10 * r.score_message +
                          0.10 * r.score_single_repo +
                          0.14 * r.score_low_intensity)
                      END,
                      2
                    ) AS score_total,
                    TRIM(BOTH ';' FROM
                      (
                        CASE WHEN r.p0_zero >= t.p0_p80 THEN 'zero_change_ratio_high;' ELSE '' END ||
                        CASE WHEN r.p2_tiny >= t.p2_p80 THEN 'tiny_commit_ratio_high;' ELSE '' END ||
                        CASE WHEN r.max_commits_10m >= t.burst_p80 THEN 'burst_commits;' ELSE '' END ||
                        CASE WHEN r.commit_count >= 20 AND r.p_balance_high >= t.balance_p80 THEN 'add_del_flip;' ELSE '' END ||
                        CASE WHEN r.top1_repo_is_core = 0 AND r.top1_repo_share >= t.repo_p80 THEN 'single_repo_grind;' ELSE '' END ||
                        CASE
                          WHEN r.role IN {dev_roles_sql}
                           AND r.commit_count < {unsat_commit_min}
                          THEN 'under_saturated;'
                          ELSE ''
                        END ||
                        CASE
                          WHEN r.msg_total >= 20
                           AND r.message_unique_ratio <= LEAST(t.msg_unique_p15, 0.20)
                           AND r.top1_message_share >= 0.40
                           AND (r.generic_message_ratio >= 0.30 OR r.short_message_ratio >= 0.30)
                          THEN 'template_messages;'
                          ELSE ''
                        END ||
                        CASE WHEN r.prod_rank >= 0.80 OR r.intensity_rank >= 0.80 OR r.repo_rank >= 0.80 OR r.msg_quality_rank >= 0.80 THEN 'protected_high_output;' ELSE '' END ||
                        CASE WHEN r.commit_count < 20 THEN 'low_sample_size;' ELSE '' END
                      )
                    ) AS tags
                  FROM ranked r
                  CROSS JOIN thresholds t
                )
                SELECT
                  employee_id,
                  person_id,
                  full_name,
                  department_level2_name,
                  department_level3_name,
                  role,
                  line_manager,
                  commit_count,
                  CASE WHEN role IN {dev_roles_sql} AND commit_count < {unsat_commit_min} THEN 1 ELSE 0 END AS under_saturated_flag,
                  repo_count,
                  total_changed_lines,
                  ROUND(changed_lines_per_commit, 2) AS changed_lines_per_commit,
                  median_changed_lines,
                  ROUND(p0_zero, 4) AS p0_zero,
                  ROUND(p2_tiny, 4) AS p2_tiny,
                  ROUND(p10_small, 4) AS p10_small,
                  ROUND(p_balance_high, 4) AS p_balance_high,
                  ROUND(off_hours_ratio, 4) AS off_hours_ratio,
                  ROUND(top1_repo_share, 4) AS top1_repo_share,
                  ROUND(message_unique_ratio, 4) AS message_unique_ratio,
                  ROUND(top1_message_share, 4) AS top1_message_share,
                  ROUND(short_message_ratio, 4) AS short_message_ratio,
                  ROUND(generic_message_ratio, 4) AS generic_message_ratio,
                  max_commits_10m,
                  max_commits_1h,
                  median_inter_commit_seconds,
                  ROUND(score_total, 2) AS score_total,
                  ROUND(score_total_raw, 2) AS score_total_raw,
                  ROUND(score_tiny, 2) AS score_tiny,
                  ROUND(score_small, 2) AS score_small,
                  ROUND(score_zero, 2) AS score_zero,
                  ROUND(score_burst, 2) AS score_burst,
                  ROUND(score_inter_commit, 2) AS score_inter_commit,
                  ROUND(score_balance, 2) AS score_balance,
                  ROUND(score_message, 2) AS score_message,
                  ROUND(score_single_repo, 2) AS score_single_repo,
                  ROUND(score_low_intensity, 2) AS score_low_intensity,
                  tags
                FROM scored
                WHERE commit_count > 0
                ORDER BY score_total DESC, commit_count DESC, total_changed_lines DESC, full_name
                {limit_sql}
                """,
                params,
            ).fetchall()

        return [
            "employee_id",
            "person_id",
            "full_name",
            "department_level2_name",
            "department_level3_name",
            "role",
            "line_manager",
            "commit_count",
            "under_saturated_flag",
            "repo_count",
            "total_changed_lines",
            "changed_lines_per_commit",
            "median_changed_lines",
            "p0_zero",
            "p2_tiny",
            "p10_small",
            "p_balance_high",
            "off_hours_ratio",
            "top1_repo_share",
            "message_unique_ratio",
            "top1_message_share",
            "short_message_ratio",
            "generic_message_ratio",
            "max_commits_10m",
            "max_commits_1h",
            "median_inter_commit_seconds",
            "score_total",
            "score_total_raw",
            "score_tiny",
            "score_small",
            "score_zero",
            "score_burst",
            "score_inter_commit",
            "score_balance",
            "score_message",
            "score_single_repo",
            "score_low_intensity",
            "tags",
        ], rows

    def line_manager_dev_activity_data(
        self,
        months: int,
        top: int | None = 200,
    ) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        # Unsaturated threshold for a team: >= 6 commits/month/dev on average.
        unsat_commits_per_dev_min = max(1, int(months) * 6)
        limit_sql = "" if top is None else "LIMIT ?"
        window_params: list[object] = [since_month, since]
        params: list[object] = [*window_params]
        if top is not None:
            params.append(int(top))

        dev_roles = [
            "Java 后台开发",
            "Web 前端开发",
            "终端开发",
            "算法开发",
            "数据开发",
            "全栈开发",
        ]
        roles_sql = ", ".join(["?"] * len(dev_roles))
        role_weight_sql = self._role_change_weight_case_sql("e.role")

        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                WITH {self._employees_cte_sql()},
                dev_employees AS (
                  SELECT
                    *,
                    COALESCE(NULLIF(TRIM(line_manager), ''), 'Unassigned') AS manager,
                    COALESCE(NULLIF(TRIM(employee_id), ''), member_key) AS person_id
                  FROM employees
                  WHERE role IN ({roles_sql})
                ),
                dev_people AS (
                  -- de-duplicate employees under the same person_id
                  SELECT
                    manager,
                    person_id,
                    arg_min(full_name, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS full_name,
                    arg_min(COALESCE(NULLIF(TRIM(username), ''), NULLIF(TRIM(email), ''), member_key), CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS member,
                    arg_min(COALESCE(department_level2_name, ''), CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS department_level2_name,
                    arg_min(COALESCE(department_level3_name, ''), CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS department_level3_name,
                    arg_min(COALESCE(role, ''), CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS role,
                    arg_min(employee_id, CASE WHEN member_key LIKE 'dummy_%' THEN 1 ELSE 0 END) AS employee_id,
                    AVG(years_of_service) AS years_of_service_avg
                  FROM dev_employees
                  GROUP BY 1,2
                ),
                resolved AS (
                  SELECT
                    COALESCE(e0.member_key, em.member_key, eu.member_key) AS member_key,
                    c.repo_id,
                    c.committed_at,
                    c.changed_lines,
                    CASE
                      WHEN (date_part('isodow', c.committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 1 AND 5)
                       AND (date_part('hour', c.committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 9 AND 18)
                      THEN 0 ELSE 1 END AS is_after_hours
                  FROM gold.fact_commit c
                  LEFT JOIN dev_employees e0 ON e0.member_key = c.member_key
                  LEFT JOIN emp_email_map em
                    ON em.email_l = LOWER(NULLIF(c.author_email,''))
                   AND e0.member_key IS NULL
                  LEFT JOIN emp_username_map eu
                    ON eu.username_l = LOWER(NULLIF(c.author_username,''))
                   AND e0.member_key IS NULL
                   AND em.member_key IS NULL
                  WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                    AND COALESCE(e0.member_key, em.member_key, eu.member_key) IS NOT NULL
                    AND COALESCE(e0.member_key, em.member_key, eu.member_key) <> ''
                ),
                per_member AS (
                  SELECT
                    e.manager,
                    e.person_id,
                    e.member_key,
                    COUNT(*)::BIGINT AS commit_count,
                    SUM(r.is_after_hours)::BIGINT AS after_hours_commit_count,
                    SUM(r.changed_lines)::BIGINT AS changed_lines_total,
                    SUM(COALESCE(r.changed_lines, 0)::DOUBLE * {role_weight_sql})::DOUBLE AS changed_lines_total_weighted
                  FROM resolved r
                  JOIN dev_employees e ON e.member_key = r.member_key
                  GROUP BY 1,2,3
                ),
                per_person AS (
                  SELECT
                    manager,
                    person_id,
                    SUM(commit_count)::BIGINT AS commit_count,
                    SUM(after_hours_commit_count)::BIGINT AS after_hours_commit_count,
                    SUM(changed_lines_total)::BIGINT AS changed_lines_total,
                    SUM(changed_lines_total_weighted)::DOUBLE AS changed_lines_total_weighted
                  FROM per_member
                  GROUP BY 1,2
                ),
                manager_repo AS (
                  SELECT
                    e.manager AS line_manager,
                    COUNT(DISTINCT r.repo_id)::BIGINT AS repo_count
                  FROM resolved r
                  JOIN dev_employees e ON e.member_key = r.member_key
                  GROUP BY 1
                ),
                manager_rollup AS (
                  SELECT
                    p.manager AS line_manager,
                    COUNT(*)::BIGINT AS dev_total,
                    SUM(CASE WHEN COALESCE(c.commit_count, 0) > 0 THEN 1 ELSE 0 END)::BIGINT AS dev_active,
                    SUM(CASE WHEN COALESCE(c.commit_count, 0) = 0 THEN 1 ELSE 0 END)::BIGINT AS dev_inactive,
                    SUM(COALESCE(c.commit_count, 0))::BIGINT AS commits_total,
                    SUM(COALESCE(c.after_hours_commit_count, 0))::BIGINT AS after_hours_commits_total,
                    SUM(COALESCE(c.changed_lines_total, 0))::BIGINT AS changed_lines_total,
                    SUM(COALESCE(c.changed_lines_total_weighted, 0))::DOUBLE AS changed_lines_total_weighted,
                    AVG(NULLIF(COALESCE(c.commit_count, 0), 0)) FILTER (WHERE COALESCE(c.commit_count, 0) > 0) AS commits_active_avg,
                    quantile_cont(COALESCE(c.commit_count, 0), 0.5) FILTER (WHERE COALESCE(c.commit_count, 0) > 0) AS commits_active_p50,
                    MAX(COALESCE(c.commit_count, 0))::BIGINT AS commits_active_max,
                    AVG(NULLIF(COALESCE(c.changed_lines_total, 0), 0)) FILTER (WHERE COALESCE(c.commit_count, 0) > 0) AS changed_lines_active_avg,
                    quantile_cont(COALESCE(c.changed_lines_total, 0), 0.5) FILTER (WHERE COALESCE(c.commit_count, 0) > 0) AS changed_lines_active_p50,
                    MAX(COALESCE(c.changed_lines_total, 0))::BIGINT AS changed_lines_active_max,
                    quantile_cont(COALESCE(c.changed_lines_total_weighted, 0), 0.5) FILTER (WHERE COALESCE(c.commit_count, 0) > 0) AS changed_lines_active_p50_weighted,
                    COUNT(DISTINCT NULLIF(p.department_level2_name, ''))::BIGINT AS department_level2_cnt,
                    COUNT(DISTINCT NULLIF(p.role, ''))::BIGINT AS dev_role_cnt,
                    AVG(p.years_of_service_avg) AS years_of_service_avg
                  FROM dev_people p
                  LEFT JOIN per_person c ON c.person_id = p.person_id AND c.manager = p.manager
                  GROUP BY 1
                ),
                derived AS (
                  SELECT
                    *,
                    ROUND(100.0 * dev_active / NULLIF(dev_total, 0), 2) AS active_pct,
                    CAST(dev_active AS VARCHAR) || '/' || CAST(dev_total AS VARCHAR) AS active_fraction,
                    ROUND(100.0 * commits_active_max / NULLIF(commits_total, 0), 2) AS top1_commit_share_pct,
                    (commits_total::DOUBLE / NULLIF(dev_total, 0)) AS commits_per_dev,
                    ROUND(100.0 * after_hours_commits_total / NULLIF(commits_total, 0), 2) AS after_hours_commit_share_pct
                  FROM manager_rollup
                ),
                thresholds AS (
                  SELECT
                    quantile_cont(active_pct, 0.25) AS active_pct_p25,
                    quantile_cont(commits_active_p50, 0.25) AS commits_active_p50_p25,
                    quantile_cont(top1_commit_share_pct, 0.75) AS top1_commit_share_pct_p75
                  FROM derived
                ),
                scored AS (
                  SELECT
                    d.*,
                    ROUND(100 * percent_rank() OVER (ORDER BY d.active_pct), 2) AS score_active,
                    ROUND(100 * percent_rank() OVER (ORDER BY d.commits_active_p50), 2) AS score_commits_p50,
                    ROUND(100 * percent_rank() OVER (ORDER BY LN(1 + d.commits_per_dev)), 2) AS score_commits_per_dev,
                    ROUND(100 * (1 - percent_rank() OVER (ORDER BY d.top1_commit_share_pct)), 2) AS score_concentration,
                    ROUND(100 * percent_rank() OVER (ORDER BY d.after_hours_commit_share_pct), 2) AS score_after_hours,
                    ROUND(
                      100 * percent_rank()
                      OVER (ORDER BY LN(1 + (COALESCE(d.changed_lines_total_weighted, 0)::DOUBLE / NULLIF(d.dev_total, 0)))),
                      2
                    ) AS score_lines_per_dev,
                    ROUND(100 * percent_rank() OVER (ORDER BY LN(1 + COALESCE(d.changed_lines_active_p50_weighted, 0))), 2) AS score_lines_p50,
                    ROUND(100 * percent_rank() OVER (ORDER BY LN(1 + COALESCE(d.changed_lines_total_weighted, 0))), 2) AS score_lines_total,
                    ROUND(100 * percent_rank() OVER (ORDER BY d.dev_role_cnt), 2) AS score_role_cover,
                    ROUND(100 * (1 - percent_rank() OVER (ORDER BY d.department_level2_cnt)), 2) AS score_dept_focus,
                    NULL::DOUBLE AS _reserved
                  FROM derived d
                )
                SELECT
                  s.line_manager AS line_manager,
                  dev_total,
                  dev_active,
                  dev_inactive,
                  active_pct,
                  active_fraction,
                  commits_total,
                  commits_active_avg,
                  commits_active_p50,
                  commits_active_max,
                  top1_commit_share_pct,
                  commits_per_dev,
                  after_hours_commits_total,
                  after_hours_commit_share_pct,
                  changed_lines_total,
                  ROUND(changed_lines_total_weighted, 2) AS changed_lines_total_weighted,
                  changed_lines_active_avg,
                  changed_lines_active_p50,
                  changed_lines_active_max,
                  (changed_lines_total::DOUBLE / NULLIF(dev_total, 0)) AS changed_lines_per_dev,
                  (changed_lines_total_weighted::DOUBLE / NULLIF(dev_total, 0)) AS changed_lines_per_dev_weighted,
                  department_level2_cnt,
                  dev_role_cnt,
                  years_of_service_avg,
                  COALESCE(suspicious_dev_cnt, 0)::BIGINT AS suspicious_dev_cnt,
                  ROUND(100.0 * COALESCE(suspicious_dev_cnt, 0) / NULLIF(dev_total, 0), 2) AS suspicious_dev_pct,
                  ROUND(COALESCE(suspicious_score_avg, 0), 2) AS suspicious_score_avg,
                  ROUND(
                    0.5 * (100 * (1 - percent_rank() OVER (ORDER BY (COALESCE(suspicious_dev_cnt, 0)::DOUBLE / NULLIF(dev_total, 0))))) +
                    0.5 * (100 * (1 - percent_rank() OVER (ORDER BY COALESCE(suspicious_score_avg, 0)))),
                    2
                  ) AS score_integrity,
                  ROUND(
                    LEAST(
                      100.0,
	                      -- Focus on total contribution, but avoid "few commits" bias:
	                      -- - Primary: score_lines_total (team total changed_lines)
	                      -- - Gate: commits_per_dev (extra penalty when under-saturated, then follow original staged rewards)
	                      score_lines_total
	                      * (
	                        CASE
	                          WHEN COALESCE(commits_per_dev, 0) < {unsat_commits_per_dev_min} THEN (
	                            0.5 + 0.3 * (COALESCE(commits_per_dev, 0) / {unsat_commits_per_dev_min})
	                          )
	                          ELSE (0.8 + 0.2 * LEAST(1.0, COALESCE(commits_per_dev, 0) / 10.0))
	                        END
	                      )
	                      -- Small bonus for maintaining multiple repos (capped, low weight)
	                      + (0.03 * LEAST(100.0 * percent_rank() OVER (ORDER BY COALESCE(r.repo_count, 0)), 70.0))
                    ),
                    2
                  ) AS score_total,
                  -- Do NOT blend anti-gaming into overall score_total (avoid mis-ranking due to heuristic false positives).
                  -- Keep `score_integrity`/suspicious_* for stats and visualization only.
                  ROUND(
                    LEAST(
	                      100.0,
	                      score_lines_total
	                      * (
	                        CASE
	                          WHEN COALESCE(commits_per_dev, 0) < {unsat_commits_per_dev_min} THEN (
	                            0.5 + 0.3 * (COALESCE(commits_per_dev, 0) / {unsat_commits_per_dev_min})
	                          )
	                          ELSE (0.8 + 0.2 * LEAST(1.0, COALESCE(commits_per_dev, 0) / 10.0))
	                        END
	                      )
	                      + (0.03 * LEAST(100.0 * percent_rank() OVER (ORDER BY COALESCE(r.repo_count, 0)), 70.0))
	                    ),
                    2
                  ) AS score_total_base,
                  score_active,
                  score_commits_p50,
                  score_commits_per_dev,
                  score_concentration,
                  score_after_hours,
                  score_lines_p50,
                  score_lines_per_dev,
                  score_lines_total,
                  score_role_cover,
                  score_dept_focus,
                  TRIM(BOTH ';' FROM
                    (
                      CASE WHEN active_pct < t.active_pct_p25 THEN '活跃风险;' ELSE '' END ||
                      CASE WHEN commits_active_p50 IS NOT NULL AND commits_active_p50 < t.commits_active_p50_p25 THEN '强度不足;' ELSE '' END ||
                      CASE WHEN top1_commit_share_pct > t.top1_commit_share_pct_p75 THEN '依赖单核;' ELSE '' END ||
                      CASE WHEN COALESCE(suspicious_dev_cnt, 0) >= 2 AND (100.0 * COALESCE(suspicious_dev_cnt, 0) / NULLIF(dev_total, 0)) >= 30 THEN '刷量风险;' ELSE '' END
                    )
                  ) AS tags
                FROM scored s
                LEFT JOIN manager_repo r ON r.line_manager = s.line_manager
                LEFT JOIN (
                  -- Anti-gaming rollup for dev employees under each manager (same months window).
                  WITH window_commits AS (
                    SELECT
                      c.repo_id,
                      c.commit_sha,
                      c.member_key,
                      c.author_username,
                      c.author_email,
                      c.committed_at,
                      c.additions,
                      c.deletions,
                      c.changed_lines,
                      COALESCE(cs.is_merge, FALSE) AS is_merge,
                      COALESCE(
                        NULLIF(json_extract_string(sc.raw, '$.commit.message'), ''),
                        NULLIF(json_extract_string(sc.raw, '$.message'), ''),
                        ''
                      ) AS message
                    FROM gold.fact_commit c
                    LEFT JOIN silver.commit_stats cs
                      ON cs.repo_id = c.repo_id AND cs.sha = c.commit_sha
                    LEFT JOIN silver.commits sc
                      ON sc.repo_id = c.repo_id AND sc.sha = c.commit_sha
                    WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                  ),
                  resolved_commits AS (
                    SELECT
                      COALESCE(e0.member_key, em.member_key, eu.member_key) AS emp_member_key,
                      wc.*
                    FROM window_commits wc
                    LEFT JOIN dev_employees e0 ON e0.member_key = wc.member_key
                    LEFT JOIN emp_email_map em
                      ON em.email_l = LOWER(NULLIF(wc.author_email,''))
                     AND e0.member_key IS NULL
                    LEFT JOIN emp_username_map eu
                      ON eu.username_l = LOWER(NULLIF(wc.author_username,''))
                     AND e0.member_key IS NULL
                     AND em.member_key IS NULL
                    WHERE COALESCE(e0.member_key, em.member_key, eu.member_key) IS NOT NULL
                      AND COALESCE(e0.member_key, em.member_key, eu.member_key) <> ''
                  ),
                  ce AS (
                    SELECT
                      de.manager,
                      de.person_id,
                      rc.repo_id,
                      rc.committed_at,
                      rc.additions,
                      rc.deletions,
                      rc.changed_lines,
                      rc.is_merge,
                      regexp_replace(LOWER(TRIM(COALESCE(rc.message, ''))), '\\\\s+', ' ', 'g') AS message_norm
                    FROM resolved_commits rc
                    JOIN dev_employees de ON de.member_key = rc.emp_member_key
                    WHERE NOT rc.is_merge
                  ),
                  per_person AS (
                    SELECT
                      manager,
                      person_id,
                      COUNT(*)::BIGINT AS commit_count,
                      SUM(changed_lines)::BIGINT AS total_changed_lines,
                      (SUM(changed_lines)::DOUBLE / NULLIF(COUNT(*), 0)) AS changed_lines_per_commit,
                      (SUM(CASE WHEN changed_lines = 0 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0)) AS p0_zero,
                      (SUM(CASE WHEN changed_lines <= 2 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0)) AS p2_tiny,
                      (SUM(
                        CASE
                          WHEN changed_lines >= 50
                           AND (1 - (ABS(COALESCE(additions,0) - COALESCE(deletions,0))::DOUBLE / NULLIF(COALESCE(additions,0) + COALESCE(deletions,0), 0))) >= 0.9
                          THEN 1 ELSE 0 END
                      )::DOUBLE / NULLIF(COUNT(*), 0)) AS p_balance_high,
                      (SUM(
                        CASE
                          WHEN (date_part('isodow', committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 1 AND 5)
                           AND (date_part('hour', committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 9 AND 18)
                          THEN 0 ELSE 1 END
                      )::DOUBLE / NULLIF(COUNT(*), 0)) AS off_hours_ratio
                    FROM ce
                    GROUP BY 1,2
                  ),
                  per_person_repo AS (
                    SELECT
                      manager,
                      person_id,
                      MAX(repo_commits)::DOUBLE / NULLIF(SUM(repo_commits), 0) AS top1_repo_share
                    FROM (
                      SELECT manager, person_id, repo_id, COUNT(*)::BIGINT AS repo_commits
                      FROM ce
                      GROUP BY 1,2,3
                    ) t
                    GROUP BY 1,2
                  ),
                  per_person_burst AS (
                    SELECT
                      manager,
                      person_id,
                      MAX(cnt_10m)::BIGINT AS max_commits_10m
                    FROM (
                      SELECT
                        manager,
                        person_id,
                        CAST(FLOOR(EPOCH(committed_at) / 600) AS BIGINT) AS b10m,
                        COUNT(*)::BIGINT AS cnt_10m
                      FROM ce
                      GROUP BY 1,2,3
                    ) t
                    GROUP BY 1,2
                  ),
                  per_person_msg AS (
                    SELECT
                      manager,
                      person_id,
                      COUNT(*)::BIGINT AS msg_total,
                      COUNT(DISTINCT NULLIF(message_norm, ''))::BIGINT AS msg_unique
                    FROM ce
                    WHERE NULLIF(message_norm, '') IS NOT NULL
                    GROUP BY 1,2
                  ),
                  j AS (
                    SELECT
                      p.*,
                      COALESCE(r.top1_repo_share, 0) AS top1_repo_share,
                      COALESCE(b.max_commits_10m, 0) AS max_commits_10m,
                      (COALESCE(m.msg_unique, 0)::DOUBLE / NULLIF(COALESCE(m.msg_total, 0), 0)) AS message_unique_ratio
                    FROM per_person p
                    LEFT JOIN per_person_repo r ON r.manager = p.manager AND r.person_id = p.person_id
                    LEFT JOIN per_person_burst b ON b.manager = p.manager AND b.person_id = p.person_id
                    LEFT JOIN per_person_msg m ON m.manager = p.manager AND m.person_id = p.person_id
                  ),
                  ranked AS (
                    SELECT
                      j.*,
                      (100 * percent_rank() OVER (ORDER BY j.p2_tiny)) AS score_tiny,
                      (100 * percent_rank() OVER (ORDER BY j.p0_zero)) AS score_zero,
                      (100 * percent_rank() OVER (ORDER BY j.max_commits_10m)) AS score_burst,
                      (100 * percent_rank() OVER (ORDER BY j.p_balance_high)) AS score_balance,
                      (100 * (1 - percent_rank() OVER (ORDER BY j.message_unique_ratio))) AS score_message,
                      (100 * percent_rank() OVER (ORDER BY j.top1_repo_share)) AS score_single_repo,
                      (100 * percent_rank() OVER (ORDER BY j.off_hours_ratio)) AS score_off_hours,
                      (100 * (1 - percent_rank() OVER (ORDER BY j.changed_lines_per_commit))) AS score_low_intensity,
                      percent_rank() OVER (ORDER BY j.total_changed_lines) AS prod_rank,
                      percent_rank() OVER (ORDER BY j.changed_lines_per_commit) AS intensity_rank
                    FROM j
                  ),
                  scored AS (
                    SELECT
                      *,
                      CASE
                        WHEN commit_count < 20 THEN (
                          0.22 * score_tiny +
                          0.12 * score_zero +
                          0.18 * score_burst +
                          0.14 * score_balance +
                          0.10 * score_message +
                          0.10 * score_single_repo +
                          0.06 * score_off_hours +
                          0.08 * score_low_intensity
                        ) * 0.5
                        WHEN prod_rank >= 0.80 OR intensity_rank >= 0.80 THEN (
                          0.22 * score_tiny +
                          0.12 * score_zero +
                          0.18 * score_burst +
                          0.14 * score_balance +
                          0.10 * score_message +
                          0.10 * score_single_repo +
                          0.06 * score_off_hours +
                          0.08 * score_low_intensity
                        ) * 0.6
                        ELSE (
                          0.22 * score_tiny +
                          0.12 * score_zero +
                          0.18 * score_burst +
                          0.14 * score_balance +
                          0.10 * score_message +
                          0.10 * score_single_repo +
                          0.06 * score_off_hours +
                          0.08 * score_low_intensity
                        )
                      END AS score_total
                    FROM ranked
                  )
                  SELECT
                    manager AS line_manager,
                    SUM(CASE WHEN score_total >= 70 THEN 1 ELSE 0 END)::BIGINT AS suspicious_dev_cnt,
                    AVG(score_total) AS suspicious_score_avg
                  FROM scored
                  GROUP BY 1
                ) risk
                  ON risk.line_manager = s.line_manager
                CROSS JOIN thresholds t
                ORDER BY score_total DESC, dev_total DESC, s.line_manager
                {limit_sql}
                """,
                [*dev_roles, *params, *window_params],
            ).fetchall()

        return [
            "line_manager",
            "dev_total",
            "dev_active",
            "dev_inactive",
            "active_pct",
            "active_fraction",
            "commits_total",
            "commits_active_avg",
            "commits_active_p50",
            "commits_active_max",
            "top1_commit_share_pct",
            "commits_per_dev",
            "after_hours_commits_total",
            "after_hours_commit_share_pct",
            "changed_lines_total",
            "changed_lines_total_weighted",
            "changed_lines_active_avg",
            "changed_lines_active_p50",
            "changed_lines_active_max",
            "changed_lines_per_dev",
            "changed_lines_per_dev_weighted",
            "department_level2_cnt",
            "dev_role_cnt",
            "years_of_service_avg",
            "suspicious_dev_cnt",
            "suspicious_dev_pct",
            "suspicious_score_avg",
            "score_integrity",
            "score_total",
            "score_total_base",
            "score_active",
            "score_commits_p50",
            "score_commits_per_dev",
            "score_concentration",
            "score_after_hours",
            "score_lines_p50",
            "score_lines_per_dev",
            "score_lines_total",
            "score_role_cover",
            "score_dept_focus",
            "tags",
        ], rows

    def project_activity(self, months: int, top: int | None = 200):
        columns, rows = self.project_activity_data(months=months, top=top)
        return to_rich_table(columns, rows)

    def project_activity_data(self, months: int, top: int | None = 200) -> tuple[list[str], list[tuple]]:
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        limit_sql = "" if top is None else "LIMIT ?"
        params: list[object] = [since_month, since]
        if top is not None:
            params.append(int(top))

        dev_roles = [
            "Java 后台开发",
            "Web 前端开发",
            "终端开发",
            "算法开发",
            "数据开发",
            "全栈开发",
        ]
        roles_sql = ", ".join(["?"] * len(dev_roles))

        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                WITH employee_hr AS (
                  SELECT
                    NULLIF(TRIM(employee_id), '') AS employee_id,
                    MIN(NULLIF(TRIM(full_name), '')) AS full_name,
                    MIN(NULLIF(TRIM(role), '')) AS role
                  FROM gold.dim_member_enrichment
                  WHERE employee_id IS NOT NULL AND employee_id <> ''
                  GROUP BY 1
                ),
                dev_employees AS (
                  SELECT employee_id
                  FROM employee_hr
                  WHERE role IN ({roles_sql})
                ),
                roster_raw AS (
                  SELECT
                    pr.project_id,
                    pr.employee_id,
                    MAX(COALESCE(pr.allocation, 1.0)) AS allocation
                  FROM gold.bridge_project_person_role pr
                  JOIN dev_employees de ON de.employee_id = pr.employee_id
                  WHERE pr.start_at <= CURRENT_DATE
                    AND (pr.end_at IS NULL OR pr.end_at >= ?::DATE)
                  GROUP BY 1,2
                ),
                roster AS (
                  SELECT
                    project_id,
                    COUNT(DISTINCT employee_id)::BIGINT AS dev_headcount,
                    SUM(allocation)::DOUBLE AS dev_fte_sum
                  FROM roster_raw
                  GROUP BY 1
                ),
                window_employee_commits AS (
                  SELECT
                    project_id,
                    employee_id,
                    SUM(weighted_commit_count)::DOUBLE AS weighted_commits_total,
                    SUM(weighted_changed_lines)::DOUBLE AS weighted_changed_lines_total
                  FROM gold.fact_project_employee_month
                  WHERE commit_month >= ?
                    AND committed_at_max >= ?::TIMESTAMPTZ
                  GROUP BY 1,2
                ),
                window_project_commits AS (
                  SELECT
                    project_id,
                    SUM(weighted_commits_total)::DOUBLE AS weighted_commits_total,
                    SUM(weighted_changed_lines_total)::DOUBLE AS weighted_changed_lines_total,
                    MAX(weighted_commits_total)::DOUBLE AS top1_weighted_commits,
                    COUNT(DISTINCT employee_id)::BIGINT AS active_dev
                  FROM window_employee_commits
                  GROUP BY 1
                ),
                repo_cnt AS (
                  SELECT
                    project_id,
                    COUNT(DISTINCT repo_id)::BIGINT AS repo_count
                  FROM gold.bridge_project_repo
                  WHERE start_at <= CURRENT_DATE
                    AND (end_at IS NULL OR end_at >= ?::DATE)
                  GROUP BY 1
                ),
                role_cov AS (
                  SELECT
                    project_id,
                    MAX(UPPER(TRIM(project_role)) = 'PO')::BIGINT AS has_po,
                    MAX(UPPER(TRIM(project_role)) = 'TO')::BIGINT AS has_to,
                    MAX(UPPER(TRIM(project_role)) = 'SM')::BIGINT AS has_sm,
                    MAX(UPPER(TRIM(project_role)) = 'TL')::BIGINT AS has_tl
                  FROM gold.bridge_project_person_role
                  WHERE start_at <= CURRENT_DATE
                    AND (end_at IS NULL OR end_at >= ?::DATE)
                  GROUP BY 1
                )
                SELECT
                  p.project_id,
                  COALESCE(p.project_name, '') AS project_name,
                  COALESCE(p.project_type, '') AS project_type,
                  COALESCE(p.status, '') AS status,
                  COALESCE(r.dev_headcount, 0)::BIGINT AS dev_headcount,
                  COALESCE(r.dev_fte_sum, 0)::DOUBLE AS dev_fte_sum,
                  COALESCE(c.active_dev, 0)::BIGINT AS active_dev,
                  (COALESCE(r.dev_headcount, 0) - COALESCE(c.active_dev, 0))::BIGINT AS inactive_dev,
                  ROUND(100.0 * COALESCE(c.active_dev, 0) / NULLIF(COALESCE(r.dev_headcount, 0), 0), 2) AS active_pct,
                  CAST(COALESCE(c.active_dev, 0) AS VARCHAR) || '/' || CAST(COALESCE(r.dev_headcount, 0) AS VARCHAR) AS active_fraction,
                  COALESCE(c.weighted_commits_total, 0)::DOUBLE AS weighted_commits_total,
                  COALESCE(c.weighted_changed_lines_total, 0)::DOUBLE AS weighted_changed_lines_total,
                  ROUND(COALESCE(c.weighted_commits_total, 0) / NULLIF(COALESCE(r.dev_fte_sum, 0), 0), 3) AS commits_per_fte,
                  ROUND(100.0 * COALESCE(c.top1_weighted_commits, 0) / NULLIF(COALESCE(c.weighted_commits_total, 0), 0), 2) AS top1_share_pct,
                  COALESCE(rc.repo_count, 0)::BIGINT AS repo_count,
                  (COALESCE(cv.has_po, 0) + COALESCE(cv.has_to, 0) + COALESCE(cv.has_sm, 0) + COALESCE(cv.has_tl, 0))::BIGINT AS core_role_coverage_cnt,
                  TRIM(BOTH ',' FROM (
                    CASE WHEN COALESCE(cv.has_po, 0) = 1 THEN 'PO,' ELSE '' END ||
                    CASE WHEN COALESCE(cv.has_to, 0) = 1 THEN 'TO,' ELSE '' END ||
                    CASE WHEN COALESCE(cv.has_sm, 0) = 1 THEN 'SM,' ELSE '' END ||
                    CASE WHEN COALESCE(cv.has_tl, 0) = 1 THEN 'TL,' ELSE '' END
                  )) AS core_roles_present
                FROM gold.dim_project p
                LEFT JOIN roster r ON r.project_id = p.project_id
                LEFT JOIN window_project_commits c ON c.project_id = p.project_id
                LEFT JOIN repo_cnt rc ON rc.project_id = p.project_id
                LEFT JOIN role_cov cv ON cv.project_id = p.project_id
                ORDER BY active_pct DESC, weighted_commits_total DESC, dev_headcount DESC, project_id
                {limit_sql}
                """,
                [*dev_roles, since, since_month, since, since, since, *([] if top is None else [int(top)])],
            ).fetchall()

        return [
            "project_id",
            "project_name",
            "project_type",
            "status",
            "dev_headcount",
            "dev_fte_sum",
            "active_dev",
            "inactive_dev",
            "active_pct",
            "active_fraction",
            "weighted_commits_total",
            "weighted_changed_lines_total",
            "commits_per_fte",
            "top1_share_pct",
            "repo_count",
            "core_role_coverage_cnt",
            "core_roles_present",
        ], rows


    def debug_active_repos(self, months: int):
        since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30 * months)
        since = since_dt.date().isoformat()
        since_month = since_dt.strftime("%Y-%m")
        with self.db.connect() as conn:
            out = {}
            out["since_month"] = since_month
            out["since_ts"] = since
            out["silver_commits_total"] = conn.execute("SELECT COUNT(*) FROM silver.commits").fetchone()[0]
            out["silver_commits_null_committed_at"] = conn.execute(
                "SELECT SUM(committed_at IS NULL)::BIGINT FROM silver.commits"
            ).fetchone()[0]
            out["gold_fact_commit_total"] = conn.execute("SELECT COUNT(*) FROM gold.fact_commit").fetchone()[0]
            out["gold_bridge_repo_member_total"] = conn.execute(
                "SELECT COUNT(*) FROM gold.bridge_repo_member"
            ).fetchone()[0]
            out["window_fact_commits"] = conn.execute(
                """
                SELECT COUNT(*) FROM gold.fact_commit
                WHERE commit_month >= ? AND committed_at >= ?::TIMESTAMPTZ
                """,
                [since_month, since],
            ).fetchone()[0]
            out["window_distinct_repos_in_fact"] = conn.execute(
                """
                SELECT COUNT(DISTINCT repo_id) FROM gold.fact_commit
                WHERE commit_month >= ? AND committed_at >= ?::TIMESTAMPTZ
                """,
                [since_month, since],
            ).fetchone()[0]
            out["window_distinct_repos_after_member_join"] = conn.execute(
                """
                SELECT COUNT(DISTINCT c.repo_id)
                FROM gold.fact_commit c
                JOIN gold.bridge_repo_member br
                  ON br.repo_id = c.repo_id
                 AND br.member_key = c.member_key
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                """,
                [since_month, since],
            ).fetchone()[0]
            out["window_commits_after_member_join"] = conn.execute(
                """
                SELECT COUNT(*)
                FROM gold.fact_commit c
                JOIN gold.bridge_repo_member br
                  ON br.repo_id = c.repo_id
                 AND br.member_key = c.member_key
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                """,
                [since_month, since],
            ).fetchone()[0]
            cond = self._valid_member_join_condition("c", "br")
            out["window_distinct_repos_after_relaxed_member_join"] = conn.execute(
                f"""
                SELECT COUNT(DISTINCT c.repo_id)
                FROM gold.fact_commit c
                JOIN gold.bridge_repo_member br
                  ON br.repo_id = c.repo_id
                 AND ({cond})
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                """,
                [since_month, since],
            ).fetchone()[0]
            out["window_commits_after_relaxed_member_join"] = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM gold.fact_commit c
                JOIN gold.bridge_repo_member br
                  ON br.repo_id = c.repo_id
                 AND ({cond})
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                """,
                [since_month, since],
            ).fetchone()[0]

            maps_bridge = """
            member_keys AS (
              SELECT DISTINCT member_key
              FROM gold.bridge_repo_member
              WHERE member_key IS NOT NULL AND member_key <> ''
            ),
            email_map AS (
              SELECT
                LOWER(NULLIF(email,'')) AS email_l,
                MIN(member_key) AS member_key
              FROM gold.bridge_repo_member
              WHERE email IS NOT NULL AND email <> ''
              GROUP BY 1
            ),
            username_map AS (
              SELECT
                LOWER(NULLIF(username,'')) AS username_l,
                MIN(member_key) AS member_key
              FROM gold.bridge_repo_member
              WHERE username IS NOT NULL AND username <> ''
              GROUP BY 1
            )
            """
            member_key_expr = self._global_member_key_expr()
            out["window_commits_after_global_bridge_member_match"] = conn.execute(
                f"""
                WITH {maps_bridge}
                SELECT COUNT(*)::BIGINT
                FROM gold.fact_commit c
                LEFT JOIN member_keys mk ON mk.member_key = c.member_key
                LEFT JOIN email_map em
                  ON em.email_l = LOWER(NULLIF(c.author_email,''))
                 AND mk.member_key IS NULL
                LEFT JOIN username_map um
                  ON um.username_l = LOWER(NULLIF(c.author_username,''))
                 AND mk.member_key IS NULL
                 AND em.member_key IS NULL
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                  AND {member_key_expr} IS NOT NULL AND {member_key_expr} <> ''
                """,
                [since_month, since],
            ).fetchone()[0]
            out["window_distinct_repos_after_global_bridge_member_match"] = conn.execute(
                f"""
                WITH {maps_bridge}
                SELECT COUNT(DISTINCT c.repo_id)::BIGINT
                FROM gold.fact_commit c
                LEFT JOIN member_keys mk ON mk.member_key = c.member_key
                LEFT JOIN email_map em
                  ON em.email_l = LOWER(NULLIF(c.author_email,''))
                 AND mk.member_key IS NULL
                LEFT JOIN username_map um
                  ON um.username_l = LOWER(NULLIF(c.author_username,''))
                 AND mk.member_key IS NULL
                 AND em.member_key IS NULL
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                  AND {member_key_expr} IS NOT NULL AND {member_key_expr} <> ''
                """,
                [since_month, since],
            ).fetchone()[0]

            maps_dim = self._global_member_identity_maps_cte_sql()
            out["window_commits_after_global_dim_member_match"] = conn.execute(
                f"""
                WITH {maps_dim}
                SELECT COUNT(*)::BIGINT
                FROM gold.fact_commit c
                LEFT JOIN member_keys mk ON mk.member_key = c.member_key
                LEFT JOIN email_map em
                  ON em.email_l = LOWER(NULLIF(c.author_email,''))
                 AND mk.member_key IS NULL
                LEFT JOIN username_map um
                  ON um.username_l = LOWER(NULLIF(c.author_username,''))
                 AND mk.member_key IS NULL
                 AND em.member_key IS NULL
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                  AND {member_key_expr} IS NOT NULL AND {member_key_expr} <> ''
                """,
                [since_month, since],
            ).fetchone()[0]
            out["window_distinct_repos_after_global_dim_member_match"] = conn.execute(
                f"""
                WITH {maps_dim}
                SELECT COUNT(DISTINCT c.repo_id)::BIGINT
                FROM gold.fact_commit c
                LEFT JOIN member_keys mk ON mk.member_key = c.member_key
                LEFT JOIN email_map em
                  ON em.email_l = LOWER(NULLIF(c.author_email,''))
                 AND mk.member_key IS NULL
                LEFT JOIN username_map um
                  ON um.username_l = LOWER(NULLIF(c.author_username,''))
                 AND mk.member_key IS NULL
                 AND em.member_key IS NULL
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                  AND {member_key_expr} IS NOT NULL AND {member_key_expr} <> ''
                """,
                [since_month, since],
            ).fetchone()[0]

            employees = self._employees_cte_sql()
            out["window_commits_after_employee_match"] = conn.execute(
                f"""
                WITH {employees}
                SELECT COUNT(*)::BIGINT
                FROM gold.fact_commit c
                LEFT JOIN employees e0 ON e0.member_key = c.member_key
                LEFT JOIN emp_email_map em
                  ON em.email_l = LOWER(NULLIF(c.author_email,''))
                 AND e0.member_key IS NULL
                LEFT JOIN emp_username_map eu
                  ON eu.username_l = LOWER(NULLIF(c.author_username,''))
                 AND e0.member_key IS NULL
                 AND em.member_key IS NULL
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                  AND COALESCE(e0.member_key, em.member_key, eu.member_key) IS NOT NULL
                """,
                [since_month, since],
            ).fetchone()[0]
            out["window_distinct_repos_after_employee_match"] = conn.execute(
                f"""
                WITH {employees}
                SELECT COUNT(DISTINCT c.repo_id)::BIGINT
                FROM gold.fact_commit c
                LEFT JOIN employees e0 ON e0.member_key = c.member_key
                LEFT JOIN emp_email_map em
                  ON em.email_l = LOWER(NULLIF(c.author_email,''))
                 AND e0.member_key IS NULL
                LEFT JOIN emp_username_map eu
                  ON eu.username_l = LOWER(NULLIF(c.author_username,''))
                 AND e0.member_key IS NULL
                 AND em.member_key IS NULL
                WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
                  AND COALESCE(e0.member_key, em.member_key, eu.member_key) IS NOT NULL
                """,
                [since_month, since],
            ).fetchone()[0]
        return out

    @staticmethod
    def _materialize(conn, table: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(
            f"""
            COPY (SELECT * FROM {table})
            TO '{str(path).replace("'", "''")}'
            (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
            """
        )
