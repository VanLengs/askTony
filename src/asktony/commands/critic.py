from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import typer
from rich.console import Console

from asktony.config import load_config
from asktony.warehouse import Warehouse

critic_app = typer.Typer(help="绩效/考核 critic。", add_completion=False)
console = Console()


GRADE_ORDER = ["A", "B+", "B", "B-", "C", "C-", "D"]
GRADE_RANK = {g: i for i, g in enumerate(GRADE_ORDER)}  # smaller is better


@dataclass(frozen=True)
class GradeBands:
    a_min: float = 85.0
    bplus_min: float = 75.0
    b_min: float = 60.0
    bminus_min: float = 45.0
    c_min: float = 30.0
    cminus_min: float = 15.0


def _norm_col(s: str) -> str:
    # Normalize column names to improve auto-detection across Excel exports:
    # - remove whitespace/underscores/dashes
    # - drop punctuation like "*"
    # - keep letters/numbers/Chinese characters
    s = str(s or "").strip().lower()
    s = re.sub(r"[\s_\-]+", "", s)
    s = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", s)
    return s


def _pick_column(df: pd.DataFrame, *, preferred: list[str]) -> str | None:
    if df.columns.empty:
        return None
    by_norm = {_norm_col(c): str(c) for c in df.columns}
    for cand in preferred:
        hit = by_norm.get(_norm_col(cand))
        if hit is not None:
            return hit
    return None


def _normalize_grade(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip().upper()
    if s == "" or s in {"NAN", "NONE"}:
        return None

    # Common variants: "A：优秀", "优秀(A)", "B +", "B＋"
    s = s.replace("＋", "+").replace(" ", "")

    # Prefer explicit letter forms.
    if "B+" in s:
        return "B+"
    if "B-" in s:
        return "B-"
    if "C-" in s:
        return "C-"
    if s.startswith("A"):
        return "A"
    if s.startswith("B"):
        return "B"
    if s.startswith("C"):
        return "C"
    if s.startswith("D"):
        return "D"

    # Chinese-only variants.
    if "优秀" in s:
        return "A"
    if "良好" in s:
        return "B+"
    if "符合预期" in s:
        return "B"
    if "不符合预期" in s:
        # "严重不符合预期" is a stronger category; default it to C.
        if "严重" in s:
            return "C"
        return "B-"
    if "严重" in s:
        return "C"

    return None


def _expected_grade_from_score_total(score_total: float, bands: GradeBands) -> str:
    if score_total >= bands.a_min:
        return "A"
    if score_total >= bands.bplus_min:
        return "B+"
    if score_total >= bands.b_min:
        return "B"
    if score_total >= bands.bminus_min:
        return "B-"
    if score_total >= bands.c_min:
        return "C"
    if score_total >= bands.cminus_min:
        return "C-"
    return "D"


def _expected_grade_from_percentile(pctl: float) -> str:
    # Higher is better; pctl in [0, 100].
    if pctl >= 90:
        return "A"
    if pctl >= 75:
        return "B+"
    if pctl >= 40:
        return "B"
    if pctl >= 20:
        return "B-"
    if pctl >= 8:
        return "C"
    if pctl >= 3:
        return "C-"
    return "D"

def _classify_missing_keys(wh: Warehouse, keys: set[str]) -> dict[str, str]:
    """
    For keys not found in active-employee-score, try to distinguish:
    - present in dim_member_enrichment (likely no activity in window / excluded)
    - not present (likely input typo / not imported into dims yet)
    """
    keys = {str(k).strip() for k in keys if str(k or "").strip() != ""}
    if not keys:
        return {}

    found: set[str] = set()

    # DuckDB has practical limits for giant IN lists; batch to be safe.
    batch_size = 500
    key_list = sorted(keys)

    with wh.db.connect() as conn:
        for i in range(0, len(key_list), batch_size):
            batch = key_list[i : i + batch_size]
            placeholders = ",".join(["?"] * len(batch))
            # Match against employee_id, member_key, full_name.
            rows = conn.execute(
                f"""
                SELECT
                  COALESCE(NULLIF(TRIM(employee_id), ''), member_key) AS person_id,
                  employee_id,
                  full_name,
                  member_key
                FROM gold.dim_member_enrichment
                WHERE employee_id IN ({placeholders})
                   OR member_key IN ({placeholders})
                   OR full_name IN ({placeholders})
                """,
                batch + batch + batch,
            ).fetchall()
            for person_id, employee_id, full_name, member_key in rows:
                for v in [person_id, employee_id, full_name, member_key]:
                    vv = str(v or "").strip()
                    if vv != "":
                        found.add(vv)

    out: dict[str, str] = {}
    for k in keys:
        out[k] = "IN_DIM_MEMBER" if k in found else "NOT_IN_DIM_MEMBER"
    return out


def _ref_lookup(df_ref: pd.DataFrame) -> dict[str, dict[str, object]]:
    """
    Build a lookup of multiple keys -> best reference row.
    If multiple rows map to the same key, keep the one with higher score_total.
    """
    out: dict[str, dict[str, object]] = {}
    if df_ref.empty:
        return out

    def upsert(key: str, row: dict[str, object]) -> None:
        key = str(key or "").strip()
        if key == "":
            return
        prev = out.get(key)
        if prev is None:
            out[key] = row
            return
        try:
            prev_score = float(prev.get("score_total") or 0)
            cur_score = float(row.get("score_total") or 0)
        except Exception:  # noqa: BLE001
            out[key] = row
            return
        if cur_score > prev_score:
            out[key] = row

    for _, r in df_ref.iterrows():
        row = r.to_dict()
        upsert(row.get("employee_id", ""), row)
        upsert(row.get("person_id", ""), row)
        upsert(row.get("full_name", ""), row)

    return out


def _build_inactive_dev_keys(wh: Warehouse, months: int) -> set[str]:
    columns, rows = wh.inactive_members_data(months=months, top=None, all_fields=True)
    if not rows:
        return set()
    df = pd.DataFrame(rows, columns=columns)
    if "role" not in df.columns:
        return set()

    df["role"] = df["role"].astype("string").fillna("").map(lambda x: str(x).strip())
    dev = df[df["role"].str.contains("开发", na=False)]
    keys: set[str] = set()
    for col in ["employee_id", "full_name", "member"]:
        if col not in dev.columns:
            continue
        for v in dev[col].astype("string").fillna("").tolist():
            vv = str(v or "").strip()
            if vv:
                keys.add(vv)
    return keys


def _apply_critic(
    df: pd.DataFrame,
    *,
    source_sheet: str,
    ref_lut: dict[str, dict[str, object]],
    missing_key_class: dict[str, str],
    inactive_dev_keys: set[str],
    id_col: str,
    name_col: str | None,
    grade_col: str,
    expected_from: str,
    bands: GradeBands,
    tolerance_levels: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_out = df.copy()
    df_out.insert(0, "source_sheet", source_sheet)

    df_out["critic_lm_grade_raw"] = df_out[grade_col]
    df_out["critic_lm_grade"] = df_out[grade_col].map(_normalize_grade)

    # Match key as string (id preferred; fallback to name if id is blank).
    df_out["critic_match_key"] = df_out[id_col].astype("string").fillna("").map(lambda x: str(x).strip())
    if name_col is not None and name_col in df_out.columns:
        name_series = df_out[name_col].astype("string").fillna("").map(lambda x: str(x).strip())
        df_out.loc[df_out["critic_match_key"] == "", "critic_match_key"] = name_series

    # Enrich from reference.
    ref_employee_id: list[object] = []
    ref_person_id: list[object] = []
    ref_full_name: list[object] = []
    ref_score_total: list[object] = []
    ref_pctl: list[object] = []
    expected_grade: list[object] = []
    flag: list[str] = []
    reason: list[str] = []
    delta_levels: list[object] = []

    expected_from_norm = expected_from.strip().lower()
    if expected_from_norm not in {"score_total", "percentile"}:
        raise typer.BadParameter("--expected-from must be one of: score_total, percentile")

    for _, row in df_out.iterrows():
        key = str(row.get("critic_match_key", "") or "").strip()
        lm_g = row.get("critic_lm_grade", None)

        ref_row = ref_lut.get(key)
        if ref_row is None:
            # Not in reference.
            ref_employee_id.append(pd.NA)
            ref_person_id.append(pd.NA)
            ref_full_name.append(pd.NA)
            ref_score_total.append(pd.NA)
            ref_pctl.append(pd.NA)
            expected_grade.append(pd.NA)
            delta_levels.append(pd.NA)

            if lm_g is None:
                flag.append("MISSING_GRADE")
                reason.append("绩效等级为空，且员工未命中 active-employee-score")
            else:
                cls = missing_key_class.get(key, "UNKNOWN")
                if cls == "IN_DIM_MEMBER" and key in inactive_dev_keys:
                    # Rule: dev-role inactive employees should be rated very low.
                    exp_g = "D"
                    expected_grade[-1] = exp_g
                    ref_score_total[-1] = 0.0
                    ref_pctl[-1] = 0.0
                    try:
                        d = GRADE_RANK[lm_g] - GRADE_RANK[exp_g]
                    except Exception:  # noqa: BLE001
                        flag.append("INVALID_GRADE")
                        reason.append(f"无法识别的绩效等级：{row.get('critic_lm_grade_raw')}")
                        continue
                    delta_levels[-1] = int(d)
                    if abs(d) <= tolerance_levels:
                        flag.append("OK")
                        reason.append("")
                    else:
                        flag.append("ABNORMAL")
                        reason.append(
                            f"员工在 inactive-members 且角色为开发（role 含“开发”），期望很低≈{exp_g}；当前 LM={lm_g}"
                        )
                    continue
                if cls == "IN_DIM_MEMBER":
                    flag.append("NO_ACTIVITY_IN_WINDOW")
                    reason.append("员工存在于员工维表，但未进入 active-employee-score（可能窗口期无提交/无活跃行为）")
                elif cls == "NOT_IN_DIM_MEMBER":
                    flag.append("UNKNOWN_EMPLOYEE")
                    reason.append("员工未在员工维表中找到（可能工号/姓名填写错误，或尚未 import-dim-info 导入）")
                else:
                    flag.append("REF_MISSING")
                    reason.append(
                        "员工未命中 active-employee-score；建议核对工号/姓名，并检查是否已 import-dim-info 导入员工维表"
                    )
            continue

        ref_employee_id.append(ref_row.get("employee_id", pd.NA))
        ref_person_id.append(ref_row.get("person_id", pd.NA))
        ref_full_name.append(ref_row.get("full_name", pd.NA))
        ref_score_total.append(ref_row.get("score_total", pd.NA))
        ref_pctl.append(ref_row.get("score_total_pctl", pd.NA))

        if lm_g is None:
            expected_grade.append(pd.NA)
            delta_levels.append(pd.NA)
            flag.append("MISSING_GRADE")
            reason.append("绩效等级为空")
            continue

        if lm_g not in GRADE_RANK:
            expected_grade.append(pd.NA)
            delta_levels.append(pd.NA)
            flag.append("INVALID_GRADE")
            reason.append(f"无法识别的绩效等级：{row.get('critic_lm_grade_raw')}")
            continue

        st = ref_row.get("score_total", None)
        pctl = ref_row.get("score_total_pctl", None)

        exp_g: str
        try:
            if expected_from_norm == "percentile":
                exp_g = _expected_grade_from_percentile(float(pctl))
            else:
                exp_g = _expected_grade_from_score_total(float(st), bands)
        except Exception:  # noqa: BLE001
            expected_grade.append(pd.NA)
            delta_levels.append(pd.NA)
            flag.append("REF_BAD")
            reason.append("参考标准中 score_total/percentile 异常，无法计算期望等级")
            continue

        expected_grade.append(exp_g)
        d = GRADE_RANK[lm_g] - GRADE_RANK[exp_g]
        delta_levels.append(int(d))

        if abs(d) <= tolerance_levels:
            flag.append("OK")
            reason.append("")
        else:
            dir_txt = "偏高" if d < 0 else "偏低"
            flag.append("ABNORMAL")
            if expected_from_norm == "percentile":
                reason.append(f"相对 active-employee-score(Percentile={pctl})，打分{dir_txt}（LM={lm_g}, 期望≈{exp_g}）")
            else:
                reason.append(f"相对 active-employee-score(score_total={st})，打分{dir_txt}（LM={lm_g}, 期望≈{exp_g}）")

    df_out["ref_employee_id"] = ref_employee_id
    df_out["ref_person_id"] = ref_person_id
    df_out["ref_full_name"] = ref_full_name
    df_out["ref_score_total"] = ref_score_total
    df_out["ref_score_total_percentile"] = ref_pctl
    df_out["critic_expected_grade"] = expected_grade
    df_out["critic_delta_levels"] = delta_levels
    df_out["critic_flag"] = flag
    df_out["critic_reason"] = reason

    anomalies = df_out[df_out["critic_flag"] != "OK"].copy()
    return df_out, anomalies


@critic_app.command("monthly-assessment")
def monthly_assessment(
    input_xlsx: Path = typer.Option(..., "--input", "-i", exists=True, dir_okay=False, help="输入 XLSX（包含正式员工/外包两个 sheet）"),
    output: Path = typer.Option(Path("output/monthly_assessment_critic.xlsx"), "--output", "-o", help="输出 XLSX 文件路径"),
    months: int = typer.Option(2, min=1, max=60, help="参考 active-employee-score 的分析窗口：最近 N 个月"),
    formal_sheet: str = typer.Option("正式", help="输入 Excel 的正式员工 sheet 名"),
    contractor_sheet: str = typer.Option("外包", help="输入 Excel 的外包 sheet 名"),
    expected_from: str = typer.Option("score_total", "--expected-from", help="期望等级映射依据：score_total 或 percentile"),
    tolerance_levels: int = typer.Option(1, min=0, max=6, help="允许与期望等级相差多少档仍视为正常"),
    formal_id_column: str | None = typer.Option(None, help="正式员工：员工标识列名（默认自动识别：工号/employee_id/person_id/...）"),
    formal_grade_column: str | None = typer.Option(None, help="正式员工：绩效等级列名（默认自动识别：绩效/绩效等级/grade/...）"),
    contractor_id_column: str | None = typer.Option(None, help="外包：员工标识列名（默认自动识别）"),
    contractor_grade_column: str | None = typer.Option(None, help="外包：绩效等级列名（默认自动识别）"),
    slim: bool = typer.Option(
        False,
        "--slim/--no-slim",
        help="精简导出字段：移除邮箱/部门/流程等列，以及部分 critic 辅助列（对所有 sheet 生效）",
    ),
) -> None:
    """
    对 line manager 的“月度绩效等级表”做 critic：
    - 参考标准：运行当下的 active-employee-score（默认窗口最近 2 个月）
    - 输出：原两张 sheet 增加参考分/期望等级/critic 结果；另生成“异常”sheet 汇总异常与原因
    """
    if input_xlsx.suffix.lower() not in {".xlsx", ".xlsm"}:
        raise typer.BadParameter("--input must be an .xlsx/.xlsm file")

    cfg = load_config()
    wh = Warehouse.from_config(cfg)

    columns, rows = wh.active_employee_score_data(months=months, top=None)
    df_ref = pd.DataFrame(rows, columns=columns)
    if not df_ref.empty:
        for c in ["employee_id", "person_id", "full_name"]:
            if c in df_ref.columns:
                df_ref[c] = df_ref[c].astype("string").fillna("").map(lambda x: str(x).strip())
        if "score_total" in df_ref.columns:
            df_ref["score_total"] = pd.to_numeric(df_ref["score_total"], errors="coerce")
            df_ref["score_total_pctl"] = (df_ref["score_total"].rank(pct=True, ascending=True) * 100).round(2)
        else:
            df_ref["score_total_pctl"] = pd.NA

    ref_lut = _ref_lookup(df_ref)

    try:
        xls = pd.ExcelFile(input_xlsx)  # noqa: PD901
    except Exception as e:  # noqa: BLE001
        raise typer.BadParameter(f"无法读取输入 Excel：{input_xlsx}") from e

    available = set(xls.sheet_names)
    missing = [s for s in [formal_sheet, contractor_sheet] if s not in available]
    if missing:
        raise typer.BadParameter(f"找不到 sheet：{missing}；可用 sheets: {xls.sheet_names}")

    df_formal = pd.read_excel(xls, sheet_name=formal_sheet)
    df_contractor = pd.read_excel(xls, sheet_name=contractor_sheet)

    id_candidates = [
        "employee_id",
        "工号",
        "员工工号",
        "人员工号",
        "person_id",
        "member_key",
        "memberkey",
        "邮箱",
        "email",
        "姓名",
        "full_name",
    ]
    grade_candidates = ["总等级", "绩效", "绩效等级", "绩效评分", "绩效分", "评级", "grade", "rating"]
    name_candidates = ["*员工", "员工", "full_name", "姓名", "员工姓名", "name"]

    def resolve_cols(df: pd.DataFrame, id_override: str | None, grade_override: str | None) -> tuple[str, str | None, str]:
        id_col = id_override or _pick_column(df, preferred=id_candidates)
        if id_col is None:
            raise typer.BadParameter(f"无法识别员工标识列；请用 --*-id-column 指定。columns={list(df.columns)}")
        grade_col = grade_override or _pick_column(df, preferred=grade_candidates)
        if grade_col is None:
            raise typer.BadParameter(f"无法识别绩效等级列；请用 --*-grade-column 指定。columns={list(df.columns)}")
        name_col = _pick_column(df, preferred=name_candidates)
        return id_col, name_col, grade_col

    formal_id_col, formal_name_col, formal_grade_col = resolve_cols(df_formal, formal_id_column, formal_grade_column)
    contractor_id_col, contractor_name_col, contractor_grade_col = resolve_cols(
        df_contractor, contractor_id_column, contractor_grade_column
    )

    bands = GradeBands()

    def compute_missing_keys(df: pd.DataFrame, id_col: str, name_col: str | None) -> set[str]:
        ids = df[id_col].astype("string").fillna("").map(lambda x: str(x).strip())
        if name_col is not None and name_col in df.columns:
            names = df[name_col].astype("string").fillna("").map(lambda x: str(x).strip())
            ids = ids.mask(ids == "", names)
        return {k for k in ids.unique().tolist() if str(k or "").strip() != "" and str(k).strip() not in ref_lut}

    missing_keys = set()
    missing_keys |= compute_missing_keys(df_formal, formal_id_col, formal_name_col)
    missing_keys |= compute_missing_keys(df_contractor, contractor_id_col, contractor_name_col)
    missing_key_class = _classify_missing_keys(wh, missing_keys)
    inactive_dev_keys = _build_inactive_dev_keys(wh, months=months)

    formal_out, formal_anom = _apply_critic(
        df_formal,
        source_sheet=formal_sheet,
        ref_lut=ref_lut,
        missing_key_class=missing_key_class,
        inactive_dev_keys=inactive_dev_keys,
        id_col=formal_id_col,
        name_col=formal_name_col,
        grade_col=formal_grade_col,
        expected_from=expected_from,
        bands=bands,
        tolerance_levels=tolerance_levels,
    )
    contractor_out, contractor_anom = _apply_critic(
        df_contractor,
        source_sheet=contractor_sheet,
        ref_lut=ref_lut,
        missing_key_class=missing_key_class,
        inactive_dev_keys=inactive_dev_keys,
        id_col=contractor_id_col,
        name_col=contractor_name_col,
        grade_col=contractor_grade_col,
        expected_from=expected_from,
        bands=bands,
        tolerance_levels=tolerance_levels,
    )

    anomalies = pd.concat([formal_anom, contractor_anom], ignore_index=True)

    out = output.expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)

    slim_drop_columns = {
        "*员工邮箱",
        "职务",
        "一级部门",
        "二级部门",
        "模板",
        "当前步骤",
        "当前执行人",
        "当前执行人邮箱",
        "状态",
        "人员状态",
        "项目指标得分",
        "指标模板",
        "入职时间",
        "工号",
        "critic_match_key",
        "ref_employee_id",
        "ref_full_name",
    }

    def maybe_slim(df: pd.DataFrame) -> pd.DataFrame:
        if not slim or df.empty:
            return df
        cols = [c for c in df.columns if str(c) not in slim_drop_columns]
        return df.loc[:, cols]

    def sheet_name(name: str, used: set[str]) -> str:
        base = str(name).strip() or "sheet"
        base = re.sub(r"[\[\]\*\?:/\\]+", "_", base)
        base = base[:31]
        if base == "":
            base = "sheet"
        if base not in used:
            used.add(base)
            return base
        for i in range(2, 1000):
            cand = f"{base[: (31 - len(str(i)) - 1)]}_{i}"
            if cand not in used:
                used.add(cand)
                return cand
        raise RuntimeError("too many sheets with the same name")

    used_sheets: set[str] = set()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        maybe_slim(formal_out).to_excel(w, sheet_name=sheet_name(formal_sheet, used_sheets), index=False)
        maybe_slim(contractor_out).to_excel(w, sheet_name=sheet_name(contractor_sheet, used_sheets), index=False)

        # Split anomalies by critic_flag; ABNORMAL is further split by direction.
        abnormal = anomalies[anomalies["critic_flag"] == "ABNORMAL"].copy()
        abnormal_high = abnormal[pd.to_numeric(abnormal["critic_delta_levels"], errors="coerce") < 0].copy()
        abnormal_low = abnormal[pd.to_numeric(abnormal["critic_delta_levels"], errors="coerce") > 0].copy()

        maybe_slim(abnormal_high).to_excel(
            w, sheet_name=sheet_name("ABNORMAL_评价过高", used_sheets), index=False
        )
        maybe_slim(abnormal_low).to_excel(
            w, sheet_name=sheet_name("ABNORMAL_评价过低", used_sheets), index=False
        )

        other = anomalies[anomalies["critic_flag"] != "ABNORMAL"].copy()
        for flag in sorted({str(x) for x in other["critic_flag"].dropna().unique().tolist()}):
            df_flag = other[other["critic_flag"] == flag].copy()
            maybe_slim(df_flag).to_excel(w, sheet_name=sheet_name(flag, used_sheets), index=False)

    console.print(
        {
            "output": str(out),
            "reference_rows": int(len(df_ref)),
            "formal_rows": int(len(df_formal)),
            "contractor_rows": int(len(df_contractor)),
            "anomalies": int(len(anomalies)),
            "abnormal_high": int(len(abnormal_high)),
            "abnormal_low": int(len(abnormal_low)),
            "expected_from": expected_from,
            "tolerance_levels": tolerance_levels,
        }
    )
