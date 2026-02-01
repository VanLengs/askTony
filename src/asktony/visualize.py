from __future__ import annotations

import datetime as dt
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import gridspec
from matplotlib import font_manager
from matplotlib import patheffects
from matplotlib.patches import FancyBboxPatch
from matplotlib.text import Text
import seaborn as sns


PALETTE = {
    "repos": "#F4A3B4",  # soft pink
    "groups": "#86D7D0",  # soft teal
    "people": "#B6A6FF",  # soft purple
    "text": "#1C1C1E",
    "muted": "#6B7280",
    "grid": "#D1D5DB",
}


@dataclass(frozen=True)
class Window:
    since_dt: dt.datetime
    since_month: str
    since_ts: str


def _window(since_dt: dt.datetime) -> Window:
    if since_dt.tzinfo is None:
        since_dt = since_dt.replace(tzinfo=dt.timezone.utc)
    return Window(
        since_dt=since_dt,
        since_month=since_dt.strftime("%Y-%m"),
        since_ts=since_dt.date().isoformat(),
    )


def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path))


def _query_df(conn: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> pd.DataFrame:
    return conn.execute(sql, params).df()


def _wrap_label(s: str, width: int = 14) -> str:
    s = _sanitize_text(str(s))
    if len(s) <= width:
        return s
    # soft wrap
    return "\n".join([s[i : i + width] for i in range(0, len(s), width)])

def _sanitize_text(s: str) -> str:
    """
    Remove invisible / problematic Unicode characters that can trigger Matplotlib
    "missing glyph" warnings (e.g. U+200B ZERO WIDTH SPACE).
    """
    # Defensive: inputs may be NA/None/float('nan') from pandas.
    if s is None:
        return ""
    try:
        if pd.isna(s):
            return ""
    except Exception:  # noqa: BLE001
        pass
    if not s:
        return ""
    if not isinstance(s, str):
        s = str(s)
    # Common invisibles / BOM
    return (
        s.replace("\u200b", "")  # ZERO WIDTH SPACE
        .replace("\ufeff", "")  # BOM
        .replace("\u2060", "")  # WORD JOINER
    )


def _suggestions_for_manager(row: dict[str, Any]) -> str:
    tags = _sanitize_text(str(row.get("tags") or ""))
    suggestions: list[str] = []

    if "活跃风险" in tags:
        suggestions.append("提升活跃：明确迭代目标/任务拆分/节奏跟进")
    if "强度不足" in tags:
        suggestions.append("提升产出：减少阻塞/提升交付频率/结对辅导")
    if "依赖单核" in tags:
        suggestions.append("降低单核依赖：轮值/结对/知识共享/Code Ownership")
    if "刷量风险" in tags:
        suggestions.append("核查刷量：聚焦有效交付/减少微提交/强化评审与拆分")

    subs = {
        "活跃": float(row.get("score_active") or 0),
        "贡献强度(Commit P50)": float(row.get("score_commits_p50") or 0),
        "变更强度(Lines P50)": float(row.get("score_lines_p50") or 0),
        "人均产出(Commit)": float(row.get("score_commits_per_dev") or 0),
        "人均变更(Lines)": float(row.get("score_lines_per_dev") or 0),
        "集中度风险": float(row.get("score_concentration") or 0),
        "诚信(反刷)": float(row.get("score_integrity") or 0),
        "奋斗者文化": float(row.get("score_after_hours") or 0),
        "角色覆盖": float(row.get("score_role_cover") or 0),
        "组织聚焦": float(row.get("score_dept_focus") or 0),
    }
    weakest = sorted(subs.items(), key=lambda kv: kv[1])[:2]
    for k, _v in weakest:
        if k == "集中度风险":
            msg = "分散关键任务：扩大贡献面/提升可替代性"
        elif k == "诚信(反刷)":
            msg = "提升可信度：减少微提交/强调有效拆分/加强评审与交付闭环"
        elif k == "变更强度(Lines P50)":
            msg = "提升交付深度：推动中位贡献提升/减少碎片化工作"
        elif k == "人均变更(Lines)":
            msg = "提升人均有效产出：减少上下文切换/聚焦关键需求"
        elif k == "奋斗者文化":
            msg = "优化节奏：在不牺牲健康的前提下提升连续交付"
        elif k == "角色覆盖":
            msg = "补齐能力结构：明确角色分工/招聘或培养"
        elif k == "组织聚焦":
            msg = "优化协作边界：减少跨部门混编或强化接口机制"
        elif k == "活跃":
            msg = "提高参与度：设定提交/PR节奏与可视化目标"
        elif k == "贡献强度(Commit P50)":
            msg = "提升中位贡献：辅导与代码评审，拉齐工程实践"
        else:  # 人均产出(Commit)
            msg = "提升人均产出：WIP 限制/减少上下文切换/清障"
        if msg not in suggestions:
            suggestions.append(msg)

    # keep short
    return "；".join(suggestions[:2])


def _radar(
    ax: plt.Axes,
    labels: list[str],
    values: list[float],
    *,
    color: str,
    fontproperties: font_manager.FontProperties | None = None,
) -> None:
    n = len(labels)
    angles = [i / n * 2 * math.pi for i in range(n)]
    angles += angles[:1]
    vals = values + values[:1]

    ax.set_theta_offset(math.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_ylim(0, 100)
    ax.set_yticks([20, 40, 60, 80, 100])
    ax.set_yticklabels(["20", "40", "60", "80", "100"], fontsize=8, color=PALETTE["muted"])
    ax.grid(color=PALETTE["grid"], alpha=0.35)

    ax.plot(angles, vals, color=color, linewidth=2)
    ax.fill(angles, vals, color=color, alpha=0.18)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels([_wrap_label(x, 8) for x in labels], fontsize=9, color=PALETTE["text"])
    if fontproperties is not None:
        for t in ax.get_xticklabels():
            t.set_fontproperties(fontproperties)


def plot_line_manager_dev_activity_radar(
    *,
    df: pd.DataFrame,
    output: Path,
    dpi: int = 200,
    top_n: int = 12,
    bottom_n: int = 6,
) -> None:
    """
    Radar (spider) charts for line_manager scoring, ordered by score_total.
    Includes lightweight improvement suggestions per manager.
    """
    if df.empty:
        raise ValueError("empty dataframe")

    # normalize column names (csv may be read as strings)
    df = df.copy()
    # IMPORTANT: Do not recompute/override `score_total` here.
    # The radar title + ranking must match the exported `score_total` from `analyze line-manager-dev-activity`.
    df["line_manager"] = df["line_manager"].astype(str).map(_sanitize_text)
    if "score_total" not in df.columns:
        raise ValueError("missing required column: score_total")
    for c in [
        "score_total",
        "score_active",
        "score_commits_p50",
        "score_commits_per_dev",
        "score_concentration",
        "score_after_hours",
        "score_lines_p50",
        "score_lines_per_dev",
        "score_integrity",
        "score_role_cover",
        "score_dept_focus",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values(["score_total", "dev_total"], ascending=[False, False])
    head = df.head(top_n)
    tail = df.tail(bottom_n) if bottom_n > 0 else df.iloc[0:0]
    show = pd.concat([head, tail], ignore_index=True).drop_duplicates(subset=["line_manager"])
    show = show.sort_values(["score_total", "dev_total"], ascending=[False, False]).reset_index(drop=True)

    labels = [
        "活跃",
        "贡献强度(Commit P50)",
        "变更强度(Lines P50)",
        "人均产出(Commit)",
        "人均变更(Lines)",
        "集中度风险",
        "诚信(反刷)",
        "奋斗者文化",
        "角色覆盖",
        "组织聚焦",
    ]
    value_cols = [
        "score_active",
        "score_commits_p50",
        "score_lines_p50",
        "score_commits_per_dev",
        "score_lines_per_dev",
        "score_concentration",
        "score_integrity",
        "score_after_hours",
        "score_role_cover",
        "score_dept_focus",
    ]

    font = _cjk_font()
    sns.set_theme(style="whitegrid")

    n = len(show)
    cols = 3
    rows = max(1, math.ceil(n / cols))
    # Give extra vertical room for per-chart suggestions; avoid tight_layout (polar axes are not compatible).
    fig = plt.figure(figsize=(cols * 6.3, rows * 6.0), dpi=dpi)
    gs = gridspec.GridSpec(rows, cols, figure=fig, wspace=0.30, hspace=0.85)

    for i, (_idx, r) in enumerate(show.iterrows()):
        ax = fig.add_subplot(gs[i // cols, i % cols], projection="polar")
        vals = [float(r.get(c) or 0) for c in value_cols]
        _radar(ax, labels, vals, color=PALETTE["people"], fontproperties=font)

        manager = r.get("line_manager") or "Unassigned"
        score_total = float(r.get("score_total") or 0)
        active_fraction = str(r.get("active_fraction") or "")
        title = f"{i+1}. {manager}  score={score_total:.1f}  active={active_fraction}"
        ax.set_title(_sanitize_text(title), fontsize=12, fontweight="bold", pad=18, fontproperties=font)

        suggestion = _suggestions_for_manager(r.to_dict())
        if suggestion:
            suggestion_wrapped = _wrap_label(suggestion, 18)
            ax.text(
                0.5,
                -0.32,
                _sanitize_text(suggestion_wrapped),
                transform=ax.transAxes,
                ha="center",
                va="top",
                fontsize=9,
                color=PALETTE["muted"],
                fontproperties=font,
                bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.9, "boxstyle": "round,pad=0.25"},
            )

    fig.suptitle(
        "Line Manager Dev Activity Score Radar (Top/Bottom)",
        fontsize=16,
        fontweight="bold",
        y=0.99,
        color=PALETTE["text"],
        fontproperties=font,
    )
    fig.subplots_adjust(top=0.92, bottom=0.06)
    output = output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _suggestions_for_employee(row: dict[str, Any]) -> str:
    suggestions: list[str] = []
    # Use weakest dimensions to generate compact suggestions.
    subs = {
        "活跃": float(row.get("score_active") or 0),
        "变更强度": float(row.get("score_lines_p50") or 0),
        "贡献总量": float(row.get("score_lines_total") or 0),
        "人均变更": float(row.get("score_lines_per_commit") or 0),
        "协作广度": float(row.get("score_repo_diversity") or 0),
        "信息质量": float(row.get("score_message_quality") or 0),
        "诚信(反刷)": float(row.get("score_integrity") or 0),
        "奋斗者文化": float(row.get("score_after_hours") or 0),
        "集中度风险": float(row.get("score_concentration") or 0),
    }
    weakest = sorted(subs.items(), key=lambda kv: kv[1])[:2]
    for k, _v in weakest:
        if k == "诚信(反刷)":
            msg = "提升可信度：减少微提交/强调有效拆分/可复盘交付"
        elif k == "信息质量":
            msg = "提升信息：commit message 更具体/避免模板化"
        elif k == "协作广度":
            msg = "扩大协作：参与更多模块/减少单点刷量嫌疑"
        elif k == "集中度风险":
            msg = "降低集中：避免长期只在单一仓库高频提交"
        elif k == "活跃":
            msg = "提升节奏：保持稳定交付频率"
        elif k == "人均变更":
            msg = "提升单次交付：减少过碎提交/提升每次有效变更"
        elif k == "变更强度":
            msg = "提升深度：提高中位变更强度/聚焦关键需求"
        else:  # 贡献总量/奋斗者文化
            msg = "提升贡献：聚焦主线交付/减少碎片化工作"
        if msg not in suggestions:
            suggestions.append(msg)
    return "；".join(suggestions[:2])


def plot_active_employee_score_radar(
    *,
    df: pd.DataFrame,
    output: Path,
    dpi: int = 200,
    top_n: int = 10,
    bottom_n: int = 10,
) -> None:
    """
    Radar charts for active employee score, ordered by score_total.
    """
    if df.empty:
        raise ValueError("empty dataframe")

    df = df.copy()
    for c in [
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
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Use a stable, de-duplicated display label to avoid ambiguous names (and to make Top/Bottom de-overlap safe).
    if "full_name" in df.columns:
        df["full_name"] = df["full_name"].astype(str).map(_sanitize_text)
    if "employee_id" in df.columns:
        df["employee_id"] = df["employee_id"].astype(str).map(_sanitize_text)

    if "full_name" in df.columns:
        name_col = "_display_name"
        emp_id = df["employee_id"] if "employee_id" in df.columns else ""
        emp_id = emp_id.fillna("").astype(str)
        full_name = df["full_name"].fillna("").astype(str)
        df[name_col] = full_name.where(emp_id.str.strip() == "", full_name + "(" + emp_id + ")")
    else:
        name_col = "person_id"
        df[name_col] = df[name_col].astype(str).map(_sanitize_text)

    df = df.sort_values(["score_total", "total_changed_lines"], ascending=[False, False]).reset_index(drop=True)
    top_df = df.head(top_n)
    bottom_df = df.sort_values(["score_total", "total_changed_lines"], ascending=[True, True]).head(bottom_n) if bottom_n > 0 else df.iloc[0:0]
    # Avoid overlap (rare but possible when dataset is small).
    if not top_df.empty and not bottom_df.empty:
        bottom_df = bottom_df[~bottom_df[name_col].isin(top_df[name_col])]
    # Bottom numbering should reflect "Bottom1..N" (worst -> better among bottom).
    bottom_df = bottom_df.sort_values(["score_total", "total_changed_lines"], ascending=[True, True]).reset_index(drop=True)

    labels = [
        "活跃",
        "贡献总量(Lines)",
        "变更强度(Lines P50)",
        "人均变更(Lines/Commit)",
        "协作广度(Repos)",
        "信息质量",
        "诚信(反刷)",
        "奋斗者文化",
        "集中度风险",
    ]
    value_cols = [
        "score_active",
        "score_lines_total",
        "score_lines_p50",
        "score_lines_per_commit",
        "score_repo_diversity",
        "score_message_quality",
        "score_integrity",
        "score_after_hours",
        "score_concentration",
    ]

    font = _cjk_font()
    sns.set_theme(style="whitegrid")

    n_top = int(len(top_df))
    n_bottom = int(len(bottom_df))
    n = n_top + n_bottom
    cols = 3
    rows_top = max(1, math.ceil(max(n_top, 1) / cols)) if n_top else 0
    rows_bottom = max(1, math.ceil(max(n_bottom, 1) / cols)) if n_bottom else 0
    rows = max(1, rows_top + rows_bottom)
    fig = plt.figure(figsize=(cols * 6.3, rows * 6.0), dpi=dpi)
    gs = gridspec.GridSpec(rows, cols, figure=fig, wspace=0.30, hspace=0.95)

    axes_top: list[plt.Axes] = []
    axes_bottom: list[plt.Axes] = []

    # Top section
    for i, (_idx, r) in enumerate(top_df.iterrows()):
        ax = fig.add_subplot(gs[i // cols, i % cols], projection="polar")
        axes_top.append(ax)
        vals = [float(r.get(c) or 0) for c in value_cols]
        _radar(ax, labels, vals, color=PALETTE["people"], fontproperties=font)

        name = r.get(name_col) or "Unknown"
        score_total = float(r.get("score_total") or 0)
        commits = int(float(r.get("commit_count") or 0))
        lines = int(float(r.get("total_changed_lines") or 0))
        title = f"Top{i+1}. {name}  score={score_total:.1f}  commits={commits}  lines={lines}"
        ax.set_title(_sanitize_text(title), fontsize=12, fontweight="bold", pad=18, fontproperties=font)

        suggestion = _suggestions_for_employee(r.to_dict())
        if suggestion:
            suggestion_wrapped = _wrap_label(suggestion, 18)
            ax.text(
                0.5,
                -0.32,
                _sanitize_text(suggestion_wrapped),
                transform=ax.transAxes,
                ha="center",
                va="top",
                fontsize=9,
                color=PALETTE["muted"],
                fontproperties=font,
                bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.9, "boxstyle": "round,pad=0.25"},
            )

    # Bottom section
    bottom_color = "#FB7185"  # soft rose
    for j, (_idx, r) in enumerate(bottom_df.iterrows()):
        k = n_top + j
        ax = fig.add_subplot(gs[k // cols, k % cols], projection="polar")
        axes_bottom.append(ax)
        vals = [float(r.get(c) or 0) for c in value_cols]
        _radar(ax, labels, vals, color=bottom_color, fontproperties=font)

        name = r.get(name_col) or "Unknown"
        score_total = float(r.get("score_total") or 0)
        commits = int(float(r.get("commit_count") or 0))
        lines = int(float(r.get("total_changed_lines") or 0))
        title = f"Bottom{j+1}. {name}  score={score_total:.1f}  commits={commits}  lines={lines}"
        ax.set_title(_sanitize_text(title), fontsize=12, fontweight="bold", pad=18, fontproperties=font)

        suggestion = _suggestions_for_employee(r.to_dict())
        if suggestion:
            suggestion_wrapped = _wrap_label(suggestion, 18)
            ax.text(
                0.5,
                -0.32,
                _sanitize_text(suggestion_wrapped),
                transform=ax.transAxes,
                ha="center",
                va="top",
                fontsize=9,
                color=PALETTE["muted"],
                fontproperties=font,
                bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.9, "boxstyle": "round,pad=0.25"},
            )

    fig.suptitle(
        "Active Employee Score Radar (Top/Bottom)",
        fontsize=16,
        fontweight="bold",
        y=0.99,
        color=PALETTE["text"],
        fontproperties=font,
    )
    # Add a clear divider between Top and Bottom sections.
    if axes_top and axes_bottom:
        try:
            fig.canvas.draw()
            y_top_bottom = min(ax.get_position().y0 for ax in axes_top[-cols:])  # last row of top section
            y_bottom_top = max(ax.get_position().y1 for ax in axes_bottom[:cols])  # first row of bottom section
            y_mid = (y_top_bottom + y_bottom_top) / 2.0
            fig.add_artist(
                plt.Line2D(
                    [0.06, 0.94],
                    [y_mid, y_mid],
                    transform=fig.transFigure,
                    color=PALETTE["grid"],
                    alpha=0.55,
                    linewidth=1.2,
                )
            )
            fig.text(
                0.5,
                y_mid + 0.01,
                _sanitize_text("— Bottom 10 —"),
                ha="center",
                va="bottom",
                fontsize=12,
                fontweight="bold",
                color=PALETTE["muted"],
                fontproperties=font,
            )
        except Exception:  # noqa: BLE001
            pass
    fig.subplots_adjust(top=0.92, bottom=0.06)
    output = output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_anti_fraud_report(
    *,
    df: pd.DataFrame,
    output: Path,
    dpi: int = 200,
    top_n: int = 10,
) -> None:
    """
    Anti-fraud report (commit gaming heuristics): Top suspicious employees + tag distribution + manager distribution.
    Expects df from `Warehouse.suspicious_committers_data`.
    """
    if df.empty:
        raise ValueError("empty dataframe")

    df = df.copy()
    for c in ["score_total", "commit_count", "changed_lines_per_commit"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["full_name"] = df.get("full_name", "").astype(str).map(_sanitize_text)
    df["line_manager"] = df.get("line_manager", "Unassigned").astype(str).map(_sanitize_text)
    df["tags"] = df.get("tags", "").astype(str)

    font = _cjk_font()
    sns.set_theme(style="whitegrid")

    fig = plt.figure(figsize=(20, 16), dpi=dpi, layout="constrained")
    gs = fig.add_gridspec(2, 2, width_ratios=[1.2, 1.0], height_ratios=[1.0, 1.0])

    # Top suspicious
    ax_top = fig.add_subplot(gs[0, 0])
    top_df = df.sort_values("score_total", ascending=False).head(top_n)
    labels = [_wrap_label(x, 14) for x in top_df["full_name"].astype(str).tolist()]
    heights = top_df["score_total"].fillna(0).astype(float).tolist()
    _minimal_axes(ax_top)
    draw_rounded_bar(
        ax_top,
        list(range(len(labels))),
        heights,
        labels,
        color="#F87171",  # soft red
        fontproperties=font,
        label_rotation=45,
        label_ha="right",
        value_fmt="{:,.1f}",
    )
    ax_top.set_title(_sanitize_text(f"Top {top_n} 可疑员工（反刷评分）"), fontsize=14, fontweight="bold", fontproperties=font)

    # Tag distribution
    ax_tags = fig.add_subplot(gs[0, 1])
    _minimal_axes(ax_tags)
    tag_counts: dict[str, int] = {}
    for t in df["tags"].astype(str).tolist():
        for part in [p.strip() for p in t.split(";") if p.strip()]:
            tag_counts[part] = tag_counts.get(part, 0) + 1
    tag_items = sorted(tag_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
    tag_labels = [_wrap_label(k, 14) for k, _ in tag_items]
    tag_vals = [float(v) for _k, v in tag_items]
    draw_rounded_bar(
        ax_tags,
        list(range(len(tag_labels))),
        tag_vals,
        tag_labels,
        color="#FBBF24",  # amber
        fontproperties=font,
        label_rotation=45,
        label_ha="right",
        value_fmt="{:,.0f}",
    )
    ax_tags.set_title(_sanitize_text("反刷标签分布（Top 10）"), fontsize=14, fontweight="bold", fontproperties=font)

    # Manager distribution (suspicious count)
    ax_mgr = fig.add_subplot(gs[1, 0])
    _minimal_axes(ax_mgr)
    mgr_df = df[df["score_total"].fillna(0) >= 70].groupby("line_manager", as_index=False).size()
    mgr_df = mgr_df.sort_values("size", ascending=False).head(12)
    mgr_labels = [_wrap_label(x, 14) for x in mgr_df["line_manager"].astype(str).tolist()]
    mgr_vals = mgr_df["size"].astype(float).tolist()
    draw_rounded_bar(
        ax_mgr,
        list(range(len(mgr_labels))),
        mgr_vals,
        mgr_labels,
        color="#60A5FA",  # blue
        fontproperties=font,
        label_rotation=45,
        label_ha="right",
        value_fmt="{:,.0f}",
    )
    ax_mgr.set_title(_sanitize_text("可疑员工人数最多的 Line Manager（Top 12）"), fontsize=14, fontweight="bold", fontproperties=font)

    # Scatter: suspicion vs productivity
    ax_sc = fig.add_subplot(gs[1, 1])
    ax_sc.set_facecolor("white")
    ax_sc.grid(axis="both", linestyle="--", alpha=0.2, color=PALETTE["grid"])
    ax_sc.set_axisbelow(True)
    x = df["commit_count"].fillna(0).astype(float)
    y = df["changed_lines_per_commit"].fillna(0).astype(float)
    c = df["score_total"].fillna(0).astype(float)
    sc = ax_sc.scatter(x, y, c=c, cmap="Reds", alpha=0.65, s=18)
    ax_sc.set_xlabel("commit_count", fontproperties=font)
    ax_sc.set_ylabel("changed_lines_per_commit", fontproperties=font)
    ax_sc.set_title(_sanitize_text("可疑分 vs 产出形态"), fontsize=14, fontweight="bold", fontproperties=font)
    cb = fig.colorbar(sc, ax=ax_sc)
    cb.set_label("suspicious_score", fontproperties=font)

    fig.suptitle("Anti-Fraud (Commit Gaming) Report", fontsize=18, fontweight="bold", y=1.02, fontproperties=font)
    output = output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _cjk_font() -> font_manager.FontProperties | None:
    """
    Ensure a CJK-capable font is available; otherwise Matplotlib may fall back to
    DejaVu Sans which misses many Chinese glyphs (tables show as squares).
    """
    candidates = [
        "/System/Library/Fonts/PingFang.ttc",  # macOS
        "/System/Library/Fonts/STHeiti Medium.ttc",  # macOS
        "/System/Library/Fonts/STHeiti Light.ttc",  # macOS
        "/Library/Fonts/Arial Unicode.ttf",
        "/Library/Fonts/Arial Unicode MS.ttf",
    ]
    for p in candidates:
        try:
            if os.path.exists(p):
                font_manager.fontManager.addfont(p)
                return font_manager.FontProperties(fname=p)
        except Exception:  # noqa: BLE001
            continue
    return None


def draw_rounded_bar(
    ax,
    x: list[float],
    heights: list[float],
    labels: list[str],
    *,
    color: str,
    fontproperties: font_manager.FontProperties | None = None,
    bar_width: float = 0.75,
    radius: float | None = None,
    value_fmt: str = "{:,.0f}",
    label_rotation: int = 45,
    label_ha: str = "right",
    alpha: float = 0.95,
    value_fontsize: int = 10,
    tick_fontsize: int = 9,
) -> None:
    """
    Draw capsule-like rounded vertical bars using FancyBboxPatch.
    """
    if radius is None:
        radius = bar_width / 2.0

    max_h = max(heights) if heights else 0.0
    for xi, h, lab in zip(x, heights, labels, strict=False):
        if h < 0:
            h = 0
        patch = FancyBboxPatch(
            (xi - bar_width / 2.0, 0),
            bar_width,
            h,
            boxstyle=f"round,pad=0,rounding_size={radius}",
            linewidth=0,
            facecolor=color,
            alpha=alpha,
            zorder=3,
        )
        ax.add_patch(patch)

        # direct labeling
        ax.text(
            xi,
            h + (max_h * 0.03 + 0.5),
            value_fmt.format(h),
            ha="center",
            va="bottom",
            fontsize=value_fontsize,
            color=PALETTE["text"],
            fontweight="bold",
            fontproperties=fontproperties,
            zorder=4,
            path_effects=[patheffects.withStroke(linewidth=3, foreground="white", alpha=0.9)],
        )

    ax.set_xticks(x)
    ax.set_xticklabels(
        labels,
        rotation=label_rotation,
        ha=label_ha,
        rotation_mode="anchor",
        fontsize=tick_fontsize,
        color=PALETTE["text"],
    )
    if fontproperties is not None:
        for t in ax.get_xticklabels():
            t.set_fontproperties(fontproperties)
    ax.set_xlim(min(x) - 0.6, max(x) + 0.6)
    ax.set_ylim(0, max_h * 1.25 + 1)

    # minimalist axes
    ax.spines["left"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.tick_params(axis="y", left=False, labelleft=False)
    ax.tick_params(axis="x", length=0)
    ax.grid(axis="y", linestyle="--", alpha=0.25, color=PALETTE["grid"], zorder=0)
    ax.set_axisbelow(True)


def _minimal_axes(ax) -> None:
    ax.set_facecolor("white")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.yaxis.set_visible(False)
    ax.tick_params(axis="x", length=0)
    ax.grid(axis="y", linestyle="--", alpha=0.2, color=PALETTE["grid"], zorder=0)
    ax.set_axisbelow(True)


def _draw_row_titles(
    fig: plt.Figure,
    axes: list[plt.Axes],
    titles: list[str],
    *,
    pad: float = 0.01,
    fontproperties: font_manager.FontProperties | None = None,
) -> None:
    """
    V9: Lock all subplot titles to a single figure-level Y per row, so donuts
    (aspect='equal') cannot shift the title baseline.
    """
    if not axes:
        return
    y = max(ax.get_position().y1 for ax in axes) + pad
    for ax, title in zip(axes, titles, strict=False):
        x = ax.get_position().x0
        fig.text(
            x,
            y,
            title,
            ha="left",
            va="bottom",
            fontsize=14,
            fontweight="bold",
            color=PALETTE["text"],
            fontproperties=fontproperties,
        )


def _shade(hex_color: str, factor: float) -> str:
    # factor in [0..1], closer to 1 = original; smaller = lighter
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    r2 = int(255 - (255 - r) * factor)
    g2 = int(255 - (255 - g) * factor)
    b2 = int(255 - (255 - b) * factor)
    return f"#{r2:02x}{g2:02x}{b2:02x}"


def _donut(
    ax,
    labels: list[str],
    values: list[int],
    base_color: str,
    detail_text: str,
    *,
    fontproperties: font_manager.FontProperties | None = None,
) -> None:
    # V9: keep donut centered; titles are drawn at figure-level (row-locked).
    ax.set_anchor("C")
    ax.set_aspect("equal", adjustable="box")
    ax.set_axis_off()
    if not values or sum(values) == 0:
        ax.text(
            0.5,
            0.55,
            "无数据",
            ha="center",
            va="center",
            color=PALETTE["muted"],
            fontproperties=fontproperties,
        )
        return
    factors = [0.95, 0.80, 0.65, 0.50, 0.40]
    colors = [_shade(base_color, factors[i % len(factors)]) for i in range(len(values))]
    wedges, _ = ax.pie(
        values,
        labels=None,
        colors=colors,
        startangle=90,
        counterclock=False,
        wedgeprops=dict(width=0.35, edgecolor="white"),
    )
    # Legend must not overlap the donut.
    legend_labels = [f"{l}: {v}" for l, v in zip(labels, values, strict=False)]
    ax.legend(
        wedges,
        legend_labels,
        frameon=False,
        fontsize=9,
        bbox_to_anchor=(1.05, 1.0),
        loc="upper left",
        prop=fontproperties,
    )
    # Details: keep it short and put inside donut hole to avoid overlapping the ring.
    if detail_text:
        raw_lines = [_sanitize_text(ln.strip()) for ln in detail_text.splitlines() if ln.strip()]
        lines: list[str] = []
        for ln in raw_lines:
            # hard wrap to keep within donut hole
            if len(ln) > 34:
                ln = ln[:31].rstrip() + "..."
            lines.append(ln)
        if len(lines) > 2:
            lines = lines[:2] + ["..."]
        ax.text(
            0.0,
            0.0,
            "\n".join(lines),
            ha="center",
            va="center",
            fontsize=7.5,
            color=PALETTE["muted"],
            clip_on=True,
            fontproperties=fontproperties,
            bbox=dict(boxstyle="round,pad=0.32,rounding_size=0.6", fc="white", ec="none", alpha=0.94),
        )
    return


def _repo_bucket_stats(df: pd.DataFrame, label_col: str, value_col: str) -> tuple[list[str], list[int], str]:
    buckets = [
        (90, math.inf, ">=90 分"),
        (75, 89.999, "75-90 分"),
        (60, 74.999, "60-75 分"),
        (40, 59.999, "40-60 分"),
        (0, 39.999, "<40 分"),
    ]
    labels: list[str] = []
    values: list[int] = []
    lines: list[str] = []
    for lo, hi, name in buckets:
        if hi == math.inf:
            sub = df[df[value_col] >= lo]
        else:
            sub = df[(df[value_col] >= lo) & (df[value_col] <= hi)]
        items = [_sanitize_text(x) for x in sub[label_col].astype(str).tolist()]
        labels.append(name)
        values.append(len(items))
        # Keep details short; only show a few examples.
        preview = ", ".join(items[:3]) + (" ..." if len(items) > 3 else "")
        if preview:
            lines.append(f"{name} 示例: {preview}".rstrip())
    detail = "\n".join(lines[:4])
    return labels, values, detail


def _percentile_stats(df: pd.DataFrame, label_col: str, value_col: str) -> tuple[list[str], list[int], str]:
    if df.empty:
        return ["前 5%", "5%-30%", "30%-70%", "后 30%"], [0, 0, 0, 0], "无数据"
    df2 = df.sort_values(value_col, ascending=False).reset_index(drop=True)
    n = len(df2)

    def seg(a: float, b: float) -> pd.DataFrame:
        i0 = int(math.floor(a * n))
        i1 = int(math.floor(b * n))
        return df2.iloc[i0:i1]

    segments = [
        ("前 5%", seg(0.0, 0.05)),
        ("5%-30%", seg(0.05, 0.30)),
        ("30%-70%", seg(0.30, 0.70)),
        ("后 30%", seg(0.70, 1.0)),
    ]
    labels: list[str] = []
    values: list[int] = []
    lines: list[str] = []
    for name, s in segments:
        items = [_sanitize_text(x) for x in s[label_col].astype(str).tolist()]
        labels.append(name)
        values.append(len(items))
        preview = ", ".join(items[:3]) + (" ..." if len(items) > 3 else "")
        if preview:
            lines.append(f"{name} 示例: {preview}".rstrip())
    detail = "\n".join(lines[:4])
    return labels, values, detail


def _top_bottom(df: pd.DataFrame, top_n: int, bottom_n: int, value_col: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df2 = df.sort_values(value_col, ascending=False)
    top_df = df2.head(top_n)
    # Bottom榜：按分数从低到高展示（最差的排在最前面），更符合“末位”语义。
    bottom_df = df2.tail(bottom_n).sort_values(value_col, ascending=True) if bottom_n > 0 else df2.iloc[0:0]
    return top_df, bottom_df


def plot_report(
    *,
    db_path: Path,
    since_dt: dt.datetime,
    top_n: int = 10,
    bottom_n: int = 10,
    output: Path,
    dpi: int = 180,
) -> None:
    w = _window(since_dt)

    # IMPORTANT:
    # The report must use the same employee `score_total` definition as:
    # - `asktony analyze active-employee-score`
    # Otherwise users will see mismatched rankings between report.png and the exported score CSV.
    #
    # For standard periods (month/bimonth/...), we reuse Warehouse scoring directly.
    # For non-standard windows (e.g. week), fall back to the window-based SQL.
    now = dt.datetime.now(dt.timezone.utc)
    window_days = max(1, int((now - w.since_dt).days))
    months_guess = max(1, int(round(window_days / 30)))
    use_warehouse_score = abs(window_days - 30 * months_guess) <= 3
    unsat_commit_min = max(1, months_guess * 6)

    if use_warehouse_score:
        from asktony.db import DB
        from asktony.warehouse import Warehouse

        wh = Warehouse(root=Path("."), db=DB(db_path))
        cols, rows = wh.active_employee_score_data(months=months_guess, top=None)
        raw_df = pd.DataFrame(rows, columns=cols)
        # Align report columns.
        grp = raw_df["department_level3_name"].fillna("").astype(str)
        grp = grp.where(grp.str.strip() != "", raw_df["department_level2_name"].fillna("").astype(str))
        grp = grp.where(grp.str.strip() != "", "未分配")
        person_label = raw_df["full_name"].astype(str)
        emp_id = raw_df["employee_id"].fillna("").astype(str)
        person_label = person_label.where(emp_id.str.strip() == "", person_label + "(" + emp_id + ")")
        people_df = pd.DataFrame(
            {
                "person_id": raw_df["person_id"],
                "person": person_label,
                "grp": grp,
                "commit_count": raw_df["commit_count"],
                "total_changed_lines": raw_df["total_changed_lines"],
                "score_total": raw_df["score_total"],
            }
        )
    else:
        with _connect(db_path) as conn:
            # Score-based report (active employees). Window-based fallback.
            people_df = _query_df(
                conn,
                """
            WITH employees AS (
              SELECT
                e.member_key,
                b.username,
                b.email,
                COALESCE(NULLIF(TRIM(e.employee_id), ''), e.member_key) AS person_id,
                e.full_name,
                COALESCE(d3.name, d2.name, '未分配') AS grp
              FROM gold.dim_member_enrichment e
              LEFT JOIN gold.dim_member_base b ON b.member_key = e.member_key
              LEFT JOIN gold.dim_department_level3 d3 ON d3.department_level3_id = e.department_level3_id
              LEFT JOIN gold.dim_department_level2 d2
                ON d2.department_level2_id = COALESCE(e.department_level2_id, d3.department_level2_id)
              WHERE NULLIF(TRIM(e.full_name), '') IS NOT NULL
            ),
            emp_email_map AS (
              SELECT LOWER(NULLIF(email,'')) AS email_l, MIN(member_key) AS member_key
              FROM employees
              WHERE email IS NOT NULL AND email <> ''
              GROUP BY 1
            ),
            emp_username_map AS (
              SELECT LOWER(NULLIF(username,'')) AS username_l, MIN(member_key) AS member_key
              FROM employees
              WHERE username IS NOT NULL AND username <> ''
              GROUP BY 1
            ),
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
                e.person_id,
                e.full_name || CASE WHEN e.person_id <> e.member_key THEN '(' || e.person_id || ')' ELSE '' END AS person,
                e.grp,
                r.repo_id,
                r.committed_at,
                r.additions,
                r.deletions,
                r.changed_lines,
                r.is_merge,
                regexp_replace(LOWER(TRIM(COALESCE(r.message, ''))), '\\\\s+', ' ', 'g') AS message_norm,
                CASE
                  WHEN (date_part('isodow', r.committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 1 AND 5)
                   AND (date_part('hour', r.committed_at AT TIME ZONE 'Asia/Shanghai') BETWEEN 9 AND 18)
                  THEN 0 ELSE 1 END AS is_after_hours
              FROM resolved r
              JOIN employees e ON e.member_key = r.emp_member_key
              WHERE NOT r.is_merge
            ),
            per_person_base AS (
              SELECT
                person_id,
                MIN(person) AS person,
                MIN(grp) AS grp,
                COUNT(*)::BIGINT AS commit_count,
                COUNT(DISTINCT repo_id)::BIGINT AS repo_count,
                SUM(changed_lines)::BIGINT AS total_changed_lines,
                (SUM(changed_lines)::DOUBLE / NULLIF(COUNT(*), 0)) AS changed_lines_per_commit,
                quantile_cont(changed_lines, 0.5) AS median_changed_lines,
                (SUM(is_after_hours)::DOUBLE / NULLIF(COUNT(*), 0)) AS after_hours_ratio,
                (SUM(CASE WHEN changed_lines = 0 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0)) AS p0_zero,
                (SUM(CASE WHEN changed_lines <= 2 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0)) AS p2_tiny,
                (SUM(
                  CASE
                    WHEN changed_lines >= 50
                     AND (1 - (ABS(COALESCE(additions,0) - COALESCE(deletions,0))::DOUBLE / NULLIF(COALESCE(additions,0) + COALESCE(deletions,0), 0))) >= 0.9
                    THEN 1 ELSE 0 END
                )::DOUBLE / NULLIF(COUNT(*), 0)) AS p_balance_high
              FROM commit_enriched
              GROUP BY 1
            ),
            per_person_repo AS (
              SELECT
                person_id,
                MAX(repo_commits)::DOUBLE / NULLIF(SUM(repo_commits), 0) AS top1_repo_share
              FROM (
                SELECT person_id, repo_id, COUNT(*)::BIGINT AS repo_commits
                FROM commit_enriched
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
                FROM commit_enriched
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
                FROM commit_enriched
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
            anti_fraud_risk_scored AS (
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
                ) AS anti_fraud_risk_score
              FROM suspicious_ranked r
            ),
            scored AS (
              SELECT
                s.*,
                ROUND(100 * percent_rank() OVER (ORDER BY s.commit_count), 2) AS score_active,
                ROUND(100 * percent_rank() OVER (ORDER BY LN(1 + s.total_changed_lines)), 2) AS score_lines_total,
                ROUND(100 * percent_rank() OVER (ORDER BY LN(1 + COALESCE(s.median_changed_lines, 0))), 2) AS score_lines_p50,
                ROUND(100 * percent_rank() OVER (ORDER BY LN(1 + s.changed_lines_per_commit)), 2) AS score_lines_per_commit,
                ROUND(100 * percent_rank() OVER (ORDER BY s.repo_count), 2) AS score_repo_diversity,
                ROUND(100 * percent_rank() OVER (ORDER BY s.message_unique_ratio), 2) AS score_message_quality,
                ROUND(100 * percent_rank() OVER (ORDER BY s.after_hours_ratio), 2) AS score_after_hours,
                ROUND(100 * (1 - percent_rank() OVER (ORDER BY s.anti_fraud_risk_score)), 2) AS score_integrity,
                ROUND(100 * (1 - percent_rank() OVER (ORDER BY s.top1_repo_share)), 2) AS score_concentration
              FROM anti_fraud_risk_scored s
            )
            SELECT
              person_id,
              person,
              grp,
              commit_count,
              total_changed_lines,
              ROUND(
                0.22 * score_active +
                0.16 * score_lines_total +
                0.16 * score_lines_p50 +
                0.12 * score_lines_per_commit +
                0.10 * score_integrity +
                0.06 * score_after_hours +
                0.06 * score_repo_diversity +
                0.06 * score_message_quality +
                0.06 * score_concentration,
                2
              ) AS score_total
            FROM scored
            """,
                [w.since_month, w.since_ts],
            )

            person_repo_df = _query_df(
                conn,
                """
            WITH person_repo AS (
              SELECT DISTINCT
                ce.repo_id,
                ce.person_id
              FROM (
                WITH employees AS (
                  SELECT
                    e.member_key,
                    b.username,
                    b.email,
                    COALESCE(NULLIF(TRIM(e.employee_id), ''), e.member_key) AS person_id,
                    e.full_name
                  FROM gold.dim_member_enrichment e
                  LEFT JOIN gold.dim_member_base b ON b.member_key = e.member_key
                  WHERE NULLIF(TRIM(e.full_name), '') IS NOT NULL
                ),
                emp_email_map AS (
                  SELECT LOWER(NULLIF(email,'')) AS email_l, MIN(member_key) AS member_key
                  FROM employees
                  WHERE email IS NOT NULL AND email <> ''
                  GROUP BY 1
                ),
                emp_username_map AS (
                  SELECT LOWER(NULLIF(username,'')) AS username_l, MIN(member_key) AS member_key
                  FROM employees
                  WHERE username IS NOT NULL AND username <> ''
                  GROUP BY 1
                ),
                window_commits AS (
                  SELECT
                    c.repo_id,
                    c.member_key,
                    c.author_username,
                    c.author_email,
                    c.committed_at,
                    c.changed_lines,
                    COALESCE(cs.is_merge, FALSE) AS is_merge
                  FROM gold.fact_commit c
                  LEFT JOIN silver.commit_stats cs
                    ON cs.repo_id = c.repo_id AND cs.sha = c.commit_sha
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
                )
                SELECT
                  r.repo_id,
                  e.person_id,
                  r.is_merge
                FROM resolved r
                JOIN employees e ON e.member_key = r.emp_member_key
              ) ce
              WHERE NOT ce.is_merge
            )
            SELECT
              pr.repo_id,
              r.repo_name,
              pr.person_id
            FROM person_repo pr
            JOIN gold.dim_repo r ON r.repo_id = pr.repo_id
            """,
                [w.since_month, w.since_ts],
            )

            # Under-saturated list (dev roles with commits >0 but below threshold)
            under_sat_df = _query_df(
                conn,
                """
            WITH employees AS (
              SELECT
                e.member_key,
                b.username,
                b.email,
                e.full_name,
                e.role
              FROM gold.dim_member_enrichment e
              LEFT JOIN gold.dim_member_base b ON b.member_key = e.member_key
              WHERE NULLIF(TRIM(e.full_name), '') IS NOT NULL
            ),
            emp_email_map AS (
              SELECT LOWER(NULLIF(email,'')) AS email_l, MIN(member_key) AS member_key
              FROM employees
              WHERE email IS NOT NULL AND email <> ''
              GROUP BY 1
            ),
            emp_username_map AS (
              SELECT LOWER(NULLIF(username,'')) AS username_l, MIN(member_key) AS member_key
              FROM employees
              WHERE username IS NOT NULL AND username <> ''
              GROUP BY 1
            ),
            window_commits AS (
              SELECT
                c.repo_id,
                c.member_key,
                c.author_username,
                c.author_email,
                COALESCE(cs.is_merge, FALSE) AS is_merge
              FROM gold.fact_commit c
              LEFT JOIN silver.commit_stats cs
                ON cs.repo_id = c.repo_id AND cs.sha = c.commit_sha
              WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
            ),
            resolved AS (
              SELECT
                COALESCE(e0.member_key, em.member_key, eu.member_key) AS member_key,
                wc.is_merge
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
            per_member AS (
              SELECT
                member_key,
                COUNT(*)::BIGINT AS commit_count
              FROM resolved
              WHERE NOT is_merge
              GROUP BY 1
            )
            SELECT
              COALESCE(NULLIF(TRIM(e.full_name), ''), '未知') AS person
            FROM employees e
            JOIN per_member pm ON pm.member_key = e.member_key
            WHERE e.role IN (
              'Java 后台开发','Web 前端开发','终端开发','算法开发','数据开发','全栈开发'
            )
              AND pm.commit_count > 0
              AND pm.commit_count < ?
            ORDER BY 1
            """,
                [w.since_month, w.since_ts, unsat_commit_min],
            )

            # No contribution list (employees list minus window activity)
            no_contrib_df = _query_df(
                conn,
                """
            WITH employees AS (
              SELECT
                e.member_key,
                b.username,
                b.email,
                e.full_name
              FROM gold.dim_member_enrichment e
              LEFT JOIN gold.dim_member_base b ON b.member_key = e.member_key
              WHERE NULLIF(TRIM(e.full_name), '') IS NOT NULL
            ),
            emp_email_map AS (
              SELECT LOWER(NULLIF(email,'')) AS email_l, MIN(member_key) AS member_key
              FROM employees
              WHERE email IS NOT NULL AND email <> ''
              GROUP BY 1
            ),
            emp_username_map AS (
              SELECT LOWER(NULLIF(username,'')) AS username_l, MIN(member_key) AS member_key
              FROM employees
              WHERE username IS NOT NULL AND username <> ''
              GROUP BY 1
            ),
            active AS (
              SELECT DISTINCT
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
            )
            SELECT COALESCE(NULLIF(TRIM(e.full_name), ''), '未知') AS person
            FROM employees e
            LEFT JOIN active a ON a.member_key = e.member_key
            WHERE a.member_key IS NULL
            ORDER BY 1
            """,
                [w.since_month, w.since_ts],
            )
    if use_warehouse_score:
        with _connect(db_path) as conn:
            person_repo_df = _query_df(
                conn,
                """
            WITH person_repo AS (
              SELECT DISTINCT
                ce.repo_id,
                ce.person_id
              FROM (
                WITH employees AS (
                  SELECT
                    e.member_key,
                    b.username,
                    b.email,
                    COALESCE(NULLIF(TRIM(e.employee_id), ''), e.member_key) AS person_id,
                    e.full_name
                  FROM gold.dim_member_enrichment e
                  LEFT JOIN gold.dim_member_base b ON b.member_key = e.member_key
                  WHERE NULLIF(TRIM(e.full_name), '') IS NOT NULL
                ),
                emp_email_map AS (
                  SELECT LOWER(NULLIF(email,'')) AS email_l, MIN(member_key) AS member_key
                  FROM employees
                  WHERE email IS NOT NULL AND email <> ''
                  GROUP BY 1
                ),
                emp_username_map AS (
                  SELECT LOWER(NULLIF(username,'')) AS username_l, MIN(member_key) AS member_key
                  FROM employees
                  WHERE username IS NOT NULL AND username <> ''
                  GROUP BY 1
                ),
                window_commits AS (
                  SELECT
                    c.repo_id,
                    c.member_key,
                    c.author_username,
                    c.author_email,
                    c.committed_at,
                    c.changed_lines,
                    COALESCE(cs.is_merge, FALSE) AS is_merge
                  FROM gold.fact_commit c
                  LEFT JOIN silver.commit_stats cs
                    ON cs.repo_id = c.repo_id AND cs.sha = c.commit_sha
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
                )
                SELECT
                  r.repo_id,
                  e.person_id,
                  r.is_merge
                FROM resolved r
                JOIN employees e ON e.member_key = r.emp_member_key
              ) ce
              WHERE NOT ce.is_merge
            )
            SELECT
              pr.repo_id,
              r.repo_name,
              pr.person_id
            FROM person_repo pr
            JOIN gold.dim_repo r ON r.repo_id = pr.repo_id
            """,
                [w.since_month, w.since_ts],
            )
            no_contrib_df = _query_df(
                conn,
                """
            WITH employees AS (
              SELECT
                e.member_key,
                b.username,
                b.email,
                e.full_name
              FROM gold.dim_member_enrichment e
              LEFT JOIN gold.dim_member_base b ON b.member_key = e.member_key
              WHERE NULLIF(TRIM(e.full_name), '') IS NOT NULL
            ),
            emp_email_map AS (
              SELECT LOWER(NULLIF(email,'')) AS email_l, MIN(member_key) AS member_key
              FROM employees
              WHERE email IS NOT NULL AND email <> ''
              GROUP BY 1
            ),
            emp_username_map AS (
              SELECT LOWER(NULLIF(username,'')) AS username_l, MIN(member_key) AS member_key
              FROM employees
              WHERE username IS NOT NULL AND username <> ''
              GROUP BY 1
            ),
            active AS (
              SELECT DISTINCT
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
            )
            SELECT COALESCE(NULLIF(TRIM(e.full_name), ''), '未知') AS person
            FROM employees e
            LEFT JOIN active a ON a.member_key = e.member_key
            WHERE a.member_key IS NULL
            ORDER BY 1
            """,
                [w.since_month, w.since_ts],
            )
            under_sat_df = _query_df(
                conn,
                """
            WITH employees AS (
              SELECT
                e.member_key,
                b.username,
                b.email,
                e.full_name,
                e.role
              FROM gold.dim_member_enrichment e
              LEFT JOIN gold.dim_member_base b ON b.member_key = e.member_key
              WHERE NULLIF(TRIM(e.full_name), '') IS NOT NULL
            ),
            emp_email_map AS (
              SELECT LOWER(NULLIF(email,'')) AS email_l, MIN(member_key) AS member_key
              FROM employees
              WHERE email IS NOT NULL AND email <> ''
              GROUP BY 1
            ),
            emp_username_map AS (
              SELECT LOWER(NULLIF(username,'')) AS username_l, MIN(member_key) AS member_key
              FROM employees
              WHERE username IS NOT NULL AND username <> ''
              GROUP BY 1
            ),
            window_commits AS (
              SELECT
                c.repo_id,
                c.member_key,
                c.author_username,
                c.author_email,
                COALESCE(cs.is_merge, FALSE) AS is_merge
              FROM gold.fact_commit c
              LEFT JOIN silver.commit_stats cs
                ON cs.repo_id = c.repo_id AND cs.sha = c.commit_sha
              WHERE c.commit_month >= ? AND c.committed_at >= ?::TIMESTAMPTZ
            ),
            resolved AS (
              SELECT
                COALESCE(e0.member_key, em.member_key, eu.member_key) AS member_key,
                wc.is_merge
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
            per_member AS (
              SELECT
                member_key,
                COUNT(*)::BIGINT AS commit_count
              FROM resolved
              WHERE NOT is_merge
              GROUP BY 1
            )
            SELECT
              COALESCE(NULLIF(TRIM(e.full_name), ''), '未知') AS person
            FROM employees e
            JOIN per_member pm ON pm.member_key = e.member_key
            WHERE e.role IN (
              'Java 后台开发','Web 前端开发','终端开发','算法开发','数据开发','全栈开发'
            )
              AND pm.commit_count > 0
              AND pm.commit_count < ?
            ORDER BY 1
            """,
                [w.since_month, w.since_ts, unsat_commit_min],
            )

    # Aggregate repos/groups using employee score as the metric.
    groups_df = (
        people_df.groupby("grp", as_index=False)
        .agg(score_avg=("score_total", "mean"), people_cnt=("person_id", "nunique"))
        .sort_values("score_avg", ascending=False)
        .reset_index(drop=True)
    )
    repos_df = (
        person_repo_df.merge(people_df[["person_id", "score_total"]], on="person_id", how="left")
        .groupby("repo_name", as_index=False)
        .agg(score_avg=("score_total", "mean"), people_cnt=("person_id", "nunique"))
        .sort_values("score_avg", ascending=False)
        .reset_index(drop=True)
        .rename(columns={"repo_name": "repo"})
    )

    repos_top, repos_bottom = _top_bottom(repos_df, top_n, bottom_n, "score_avg")
    groups_top, groups_bottom = _top_bottom(groups_df, top_n, bottom_n, "score_avg")
    people_top, people_bottom = _top_bottom(people_df, top_n, bottom_n, "score_total")

    # Figure (4x3 gridspec + footer table)
    cjk_fp = _cjk_font()
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "font.size": 11,
            # Prefer CJK-capable fonts; fall back gracefully.
            "font.sans-serif": [
                "PingFang SC",
                "Hiragino Sans GB",
                "Microsoft YaHei",
                "Noto Sans CJK SC",
                "Arial Unicode MS",
                "DejaVu Sans",
            ],
            "axes.unicode_minus": False,
        }
    )
    if cjk_fp is not None:
        try:
            plt.rcParams.update({"font.family": cjk_fp.get_name()})
        except Exception:  # noqa: BLE001
            pass
    # Tighter layout: use constrained layout but keep paddings small.
    # NOTE: use fig.add_gridspec so rect (top/bottom/left/right) is respected under constrained layout.
    # V5.0: tall canvas for better spacing and readable footer table.
    fig = plt.figure(figsize=(20, 30), layout="constrained")
    fig.set_constrained_layout_pads(w_pad=0.006, h_pad=0.006, wspace=0.006, hspace=0.006)
    gs = fig.add_gridspec(
        4,
        3,
        width_ratios=[3.5, 1.5, 2],
        height_ratios=[3, 3, 3, 2],
        wspace=0.04,
        hspace=0.04,
        # Reserve header/footer space so global title/subtitle won't collide with row 0 titles.
        top=0.90,
        bottom=0.05,
        left=0.05,
        right=0.95,
    )

    subtitle = f"统计周期：自 {w.since_ts} (UTC) • Top {top_n} / Bottom {bottom_n}"
    fig.suptitle(
        _sanitize_text("研发代码报告"),
        fontsize=24,
        weight="bold",
        x=0.5,
        ha="center",
        color=PALETTE["text"],
        y=1.09,
        va="top",
        fontproperties=cjk_fp,
    )
    # V5.0: header uses figure-level text to avoid overlap with axes titles.
    fig.text(
        0.5,
        1.07,
        _sanitize_text(subtitle),
        fontsize=14,
        ha="center",
        va="top",
        color=PALETTE["muted"],
        fontproperties=cjk_fp,
    )
    # subtle separator line between header and body
    fig.add_artist(
        plt.Line2D(
            [0.05, 0.95],
            [1.06, 1.06],
            transform=fig.transFigure,
            color=PALETTE["grid"],
            alpha=0.25,
            linewidth=1.0,
        )
    )

    # Row 1: repos
    ax_r_top = fig.add_subplot(gs[0, 0])
    ax_r_bot = fig.add_subplot(gs[0, 1])
    ax_r_dist = fig.add_subplot(gs[0, 2])
    _minimal_axes(ax_r_top)
    _minimal_axes(ax_r_bot)
    top_xlim = None
    if not repos_top.empty:
        labels = [_wrap_label(_sanitize_text(x), 16) for x in repos_top["repo"].astype(str).tolist()]
        heights = repos_top["score_avg"].astype(float).tolist()
        draw_rounded_bar(
            ax_r_top,
            list(range(len(labels))),
            heights,
            labels,
            color=PALETTE["repos"],
            fontproperties=cjk_fp,
            label_rotation=45,
            label_ha="right",
            alpha=1.0,
            value_fmt="{:,.1f}",
        )
        # Keep a consistent x-range based on requested top_n to prevent fat bars when data is small.
        if top_n and top_n > len(labels):
            ax_r_top.set_xlim(-0.6, top_n - 0.4)
        top_xlim = ax_r_top.get_xlim()
    if not repos_bottom.empty:
        labels = [_wrap_label(_sanitize_text(x), 16) for x in repos_bottom["repo"].astype(str).tolist()]
        heights = repos_bottom["score_avg"].astype(float).tolist()
        draw_rounded_bar(
            ax_r_bot,
            list(range(len(labels))),
            heights,
            labels,
            color=PALETTE["repos"],
            fontproperties=cjk_fp,
            bar_width=0.75,
            label_rotation=45,
            label_ha="right",
            alpha=0.4,
            value_fontsize=9,
            tick_fontsize=8,
            value_fmt="{:,.1f}",
        )
    # V9: enforce xlim sync after drawing (fat bar fix).
    if top_xlim is not None:
        ax_r_bot.set_xlim(top_xlim)
    repo_b_labels, repo_b_vals, repo_detail = _repo_bucket_stats(repos_df, "repo", "score_avg")
    _donut(ax_r_dist, repo_b_labels, repo_b_vals, PALETTE["repos"], repo_detail, fontproperties=cjk_fp)

    # Row 2: groups
    ax_g_top = fig.add_subplot(gs[1, 0])
    ax_g_bot = fig.add_subplot(gs[1, 1])
    ax_g_dist = fig.add_subplot(gs[1, 2])
    _minimal_axes(ax_g_top)
    _minimal_axes(ax_g_bot)
    top_xlim = None
    if not groups_top.empty:
        labels = [_wrap_label(_sanitize_text(x), 16) for x in groups_top["grp"].astype(str).tolist()]
        heights = groups_top["score_avg"].astype(float).tolist()
        draw_rounded_bar(
            ax_g_top,
            list(range(len(labels))),
            heights,
            labels,
            color=PALETTE["groups"],
            fontproperties=cjk_fp,
            label_rotation=45,
            label_ha="right",
            alpha=1.0,
            value_fmt="{:,.1f}",
        )
        if top_n and top_n > len(labels):
            ax_g_top.set_xlim(-0.6, top_n - 0.4)
        top_xlim = ax_g_top.get_xlim()
    if not groups_bottom.empty:
        labels = [_wrap_label(_sanitize_text(x), 16) for x in groups_bottom["grp"].astype(str).tolist()]
        heights = groups_bottom["score_avg"].astype(float).tolist()
        draw_rounded_bar(
            ax_g_bot,
            list(range(len(labels))),
            heights,
            labels,
            color=PALETTE["groups"],
            fontproperties=cjk_fp,
            bar_width=0.75,
            label_rotation=45,
            label_ha="right",
            alpha=0.4,
            value_fontsize=9,
            tick_fontsize=8,
            value_fmt="{:,.1f}",
        )
    if top_xlim is not None:
        ax_g_bot.set_xlim(top_xlim)
    g_labels, g_vals, g_detail = _percentile_stats(groups_df, "grp", "score_avg")
    _donut(ax_g_dist, g_labels, g_vals, PALETTE["groups"], g_detail, fontproperties=cjk_fp)

    # Row 3: people
    ax_u_top = fig.add_subplot(gs[2, 0])
    ax_u_bot = fig.add_subplot(gs[2, 1])
    ax_u_dist = fig.add_subplot(gs[2, 2])
    _minimal_axes(ax_u_top)
    _minimal_axes(ax_u_bot)
    top_xlim = None
    if not people_top.empty:
        labels = [_wrap_label(_sanitize_text(x), 16) for x in people_top["person"].astype(str).tolist()]
        heights = people_top["score_total"].astype(float).tolist()
        draw_rounded_bar(
            ax_u_top,
            list(range(len(labels))),
            heights,
            labels,
            color=PALETTE["people"],
            fontproperties=cjk_fp,
            label_rotation=45,
            label_ha="right",
            alpha=1.0,
            value_fmt="{:,.1f}",
        )
        if top_n and top_n > len(labels):
            ax_u_top.set_xlim(-0.6, top_n - 0.4)
        top_xlim = ax_u_top.get_xlim()
    if not people_bottom.empty:
        labels = [_wrap_label(_sanitize_text(x), 16) for x in people_bottom["person"].astype(str).tolist()]
        heights = people_bottom["score_total"].astype(float).tolist()
        draw_rounded_bar(
            ax_u_bot,
            list(range(len(labels))),
            heights,
            labels,
            color=PALETTE["people"],
            fontproperties=cjk_fp,
            bar_width=0.75,
            label_rotation=45,
            label_ha="right",
            alpha=0.4,
            value_fontsize=9,
            tick_fontsize=8,
            value_fmt="{:,.1f}",
        )
    if top_xlim is not None:
        ax_u_bot.set_xlim(top_xlim)
    u_labels, u_vals, u_detail = _percentile_stats(people_df, "person", "score_total")
    _donut(ax_u_dist, u_labels, u_vals, PALETTE["people"], u_detail, fontproperties=cjk_fp)

    # V9: draw titles after layout is finalized; lock y per row using axes bbox.ymax.
    fig.canvas.draw()
    _draw_row_titles(
        fig,
        [ax_r_top, ax_r_bot, ax_r_dist],
        ["仓库活跃员工得分 Top 榜", "仓库活跃员工得分 Bottom 榜", "仓库得分分布"],
        pad=0.008,
        fontproperties=cjk_fp,
    )
    _draw_row_titles(
        fig,
        [ax_g_top, ax_g_bot, ax_g_dist],
        ["分组活跃员工得分 Top 榜", "分组活跃员工得分 Bottom 榜", "分组得分分布"],
        pad=0.008,
        fontproperties=cjk_fp,
    )
    _draw_row_titles(
        fig,
        [ax_u_top, ax_u_bot, ax_u_dist],
        ["活跃员工得分 Top 榜", "活跃员工得分 Bottom 榜", "员工得分分布"],
        pad=0.008,
        fontproperties=cjk_fp,
    )

    # Footer: pretty table
    ax_footer = fig.add_subplot(gs[3, :])
    ax_footer.axis("off")
    ax_footer.set_facecolor("white")

    # Table layout constants
    max_show = 30
    cols = 5

    # Under-saturated list (dev roles with commits >0 but below threshold)
    sat_people = [_sanitize_text(x) for x in under_sat_df["person"].astype(str).tolist()]
    sat_names_all = [n for n in sat_people if n and n.strip() and n.strip() != "未知"]
    sat_unknown_count = len(sat_people) - len(sat_names_all)
    sat_truncated = len(sat_names_all) > max_show
    sat_names = sat_names_all[:max_show] if sat_truncated else sat_names_all

    sat_rows_n = int(math.ceil(len(sat_names) / cols)) if sat_names else 1
    sat_grid: list[list[str]] = []
    for i in range(sat_rows_n):
        row: list[str] = []
        for j in range(cols):
            k = i * cols + j
            row.append(sat_names[k] if k < len(sat_names) else "")
        sat_grid.append(row)

    ax_footer.text(
        0.0,
        1.05,
        (
            f"不饱和个人（Under Saturated） . Total: {len(sat_names_all)}（门限<{unsat_commit_min} commits）"
            + (f"（另有“未知” {sat_unknown_count} 人未展示）" if sat_unknown_count else "")
            + (f"（仅展示前 {max_show}）" if sat_truncated else "")
        ),
        transform=ax_footer.transAxes,
        fontsize=12,
        fontweight="bold",
        color=PALETTE["text"],
        va="bottom",
        fontproperties=cjk_fp,
    )

    sat_table = ax_footer.table(
        cellText=sat_grid,
        colLabels=[f"{i}" for i in range(1, cols + 1)],
        cellLoc="center",
        colLoc="center",
        loc="upper left",
        bbox=[0.0, 0.56, 1.0, 0.36],
    )
    sat_table.auto_set_font_size(False)
    sat_table.set_fontsize(14)
    sat_table.scale(1.0, 2.5)
    for (r, c), cell in sat_table.get_celld().items():
        cell.visible_edges = "horizontal"
        cell.set_edgecolor("#DDDDDD")
        cell.set_linewidth(0.6)
        if r == 0:
            cell.set_facecolor("#F3F4F6")
            cell.set_text_props(weight="bold", color=PALETTE["text"], fontproperties=cjk_fp)
        else:
            cell.set_facecolor("#FFFFFF" if r % 2 == 1 else "#FAFAFB")
            cell.set_text_props(color=PALETTE["text"], fontproperties=cjk_fp)

    all_people = [_sanitize_text(x) for x in no_contrib_df["person"].astype(str).tolist()]
    # 优先展示有姓名的成员；“未知”不展示在表格里（避免噪声）。
    names_all = [n for n in all_people if n and n.strip() and n.strip() != "未知"]
    unknown_count = len(all_people) - len(names_all)
    # V5.0: keep table readable; show top rows if too many.
    truncated = len(names_all) > max_show
    names = names_all[:max_show] if truncated else names_all
    # 5 columns x N rows
    rows_n = int(math.ceil(len(names) / cols)) if names else 1
    grid: list[list[str]] = []
    for i in range(rows_n):
        row: list[str] = []
        for j in range(cols):
            k = i * cols + j
            row.append(names[k] if k < len(names) else "")
        grid.append(row)

    ax_footer.text(
        0.0,
        0.50,
        (
            f"0 提交个人（0 Commits） . Total: {len(names_all)}"
            + (f"（另有“未知” {unknown_count} 人未展示）" if unknown_count else "")
            + (f"（仅展示前 {max_show}）" if truncated else "")
        ),
        transform=ax_footer.transAxes,
        fontsize=12,
        fontweight="bold",
        color=PALETTE["text"],
        va="bottom",
        fontproperties=cjk_fp,
    )

    table = ax_footer.table(
        cellText=grid,
        colLabels=[f"{i}" for i in range(1, cols + 1)],
        cellLoc="center",
        colLoc="center",
        loc="upper left",
        bbox=[0.0, 0.05, 1.0, 0.36],
    )
    table.auto_set_font_size(False)
    # V9: larger font + taller rows for readability.
    table.set_fontsize(14)
    table.scale(1.0, 2.5)
    for (r, c), cell in table.get_celld().items():
        # "Open" table style: only horizontal edges, very light.
        cell.visible_edges = "horizontal"
        cell.set_edgecolor("#DDDDDD")
        cell.set_linewidth(0.6)
        if r == 0:
            cell.set_facecolor("#F3F4F6")
            cell.set_text_props(weight="bold", color=PALETTE["text"], fontproperties=cjk_fp)
        else:
            cell.set_facecolor("#FFFFFF" if r % 2 == 1 else "#FAFAFB")
            cell.set_text_props(color=PALETTE["text"], fontproperties=cjk_fp)

    fig.savefig(str(output), dpi=dpi, bbox_inches="tight")
    plt.close(fig)
