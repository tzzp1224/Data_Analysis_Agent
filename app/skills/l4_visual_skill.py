from __future__ import annotations

import re
from typing import Dict, Optional

import pandas as pd
import plotly.express as px

from app.services.semantic_contract import ensure_semantic_contract
from app.services.semantic_infer import SemanticInferenceResult, infer_dataframe_semantics
from app.skills.column_guard import (
    RequiredColumnSpec,
    build_missing_columns_message,
    resolve_required_columns,
)
from app.skills.contracts import (
    ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
    ERROR_TYPE_RUNTIME,
    ERROR_TYPE_TABLE_SELECTION,
    SkillResult,
)
from app.utils.tools import AuditLogger


DIMENSION_HINT_TOKENS = ("区域", "地区", "region", "渠道", "channel", "品类", "category", "城市", "大区")


def _pick_target_table(
    dfs_context: Dict[str, pd.DataFrame],
    contract: Dict[str, SemanticInferenceResult],
) -> tuple[Optional[str], Optional[pd.DataFrame], Optional[SemanticInferenceResult]]:
    candidates = []
    for name, df in (dfs_context or {}).items():
        if str(name).startswith("__"):
            continue
        sem = contract.get(str(name))
        if sem is None:
            sem = infer_dataframe_semantics(df, table_name=str(name))
            contract[str(name)] = sem
        has_date = any(c.label == "date" and c.confidence >= 0.55 for c in sem.columns)
        has_amount = any(c.label == "amount" and c.confidence >= 0.6 for c in sem.columns)
        score = 0.0
        score += 1.2 if has_date else 0.0
        score += 1.2 if has_amount else 0.0
        if re.search(r"gmv|经营|月度|trend|sales|revenue", str(name).lower()):
            score += 0.6
        candidates.append((str(name), df, sem, score))

    if not candidates:
        return None, None, None
    candidates.sort(key=lambda x: x[3], reverse=True)
    name, df, sem, _ = candidates[0]
    return name, df, sem


def _pick_dimension_column(df: pd.DataFrame, sem: SemanticInferenceResult, reserved: set[str]) -> Optional[str]:
    text_candidates = [
        c for c in sem.columns if c.label == "text" and c.confidence >= 0.5 and c.name in df.columns
    ]
    text_candidates.sort(
        key=lambda c: (
            1 if any(tok in str(c.name).lower() for tok in DIMENSION_HINT_TOKENS) else 0,
            c.confidence,
        ),
        reverse=True,
    )
    for candidate in text_candidates:
        if candidate.name not in reserved:
            return candidate.name
    for col in df.columns:
        if col in reserved:
            continue
        if any(tok in str(col).lower() for tok in DIMENSION_HINT_TOKENS):
            return str(col)
    return None


def _coerce_amount(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("¥", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce")


def run_l4_visual_skill(
    dfs_context: Dict[str, pd.DataFrame],
    instruction: str,
    semantic_contract: Optional[Dict[str, SemanticInferenceResult]] = None,
) -> SkillResult:
    audit = AuditLogger()
    try:
        contract = semantic_contract or ensure_semantic_contract(
            dfs_context,
            user_instruction=instruction,
        )
        table_name, source_df, sem = _pick_target_table(dfs_context, contract)
        if source_df is None or sem is None or table_name is None:
            return SkillResult(
                handled=True,
                response_text="L4已阻断：当前没有可用于趋势分析的业务表。",
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_TABLE_SELECTION,
            )

        required_specs = [
            RequiredColumnSpec(
                key="date_col",
                display_name="日期列",
                semantic_labels=("date",),
                min_confidence=0.55,
                name_tokens=("日期", "时间", "月份", "date", "month"),
            ),
            RequiredColumnSpec(
                key="amount_col",
                display_name="金额列",
                semantic_labels=("amount",),
                min_confidence=0.6,
                name_tokens=("金额", "销售额", "GMV", "revenue", "amount"),
            ),
        ]
        resolved, missing = resolve_required_columns(source_df, sem, required_specs)
        if missing:
            block_msg = build_missing_columns_message(
                skill_name="L4 趋势分析",
                table_name=table_name,
                df=source_df,
                sem=sem,
                missing_specs=missing,
                guidance="请补充日期列和金额列后再生成趋势图。",
            )
            audit.info("L4阻断", block_msg.splitlines()[0], affected_rows=0)
            return SkillResult(
                handled=True,
                response_text=block_msg,
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
            )

        date_col = resolved["date_col"]
        amount_col = resolved["amount_col"]
        dimension_col = _pick_dimension_column(source_df, sem, reserved={date_col, amount_col})

        work_df = source_df.copy()
        work_df[date_col] = pd.to_datetime(work_df[date_col], errors="coerce")
        work_df[amount_col] = _coerce_amount(work_df[amount_col])
        work_df = work_df.dropna(subset=[date_col, amount_col]).copy()
        if work_df.empty:
            return SkillResult(
                handled=True,
                response_text="L4已阻断：日期或金额列无法解析为有效数据，无法生成趋势图。",
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
            )

        work_df["__month__"] = work_df[date_col].dt.to_period("M").dt.to_timestamp()
        if dimension_col and dimension_col in work_df.columns:
            group_cols = ["__month__", dimension_col]
            grouped = (
                work_df.groupby(group_cols, as_index=False)[amount_col]
                .sum()
                .rename(columns={amount_col: "value"})
            )
            # 维度过多时仅保留 top6，防止图表不可读。
            top_dims = (
                grouped.groupby(dimension_col)["value"].sum().sort_values(ascending=False).head(6).index
            )
            grouped = grouped[grouped[dimension_col].isin(top_dims)].copy()
            fig = px.line(
                grouped,
                x="__month__",
                y="value",
                color=dimension_col,
                markers=True,
                title=f"{table_name} 月度趋势",
            )
        else:
            grouped = work_df.groupby("__month__", as_index=False)[amount_col].sum().rename(
                columns={amount_col: "value"}
            )
            fig = px.line(
                grouped,
                x="__month__",
                y="value",
                markers=True,
                title=f"{table_name} 月度趋势",
            )

        if grouped.empty:
            return SkillResult(
                handled=True,
                response_text="L4已阻断：可用于聚合的趋势数据为空，请检查日期/金额/维度列质量。",
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
            )

        summary_df = grouped.sort_values("__month__").copy()
        monthly_total = summary_df.groupby("__month__", as_index=False)["value"].sum()
        if monthly_total.empty:
            return SkillResult(
                handled=True,
                response_text="L4已阻断：月度聚合后无有效结果，无法产出趋势结论。",
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
            )
        peak_row = monthly_total.sort_values("value", ascending=False).iloc[0]
        low_row = monthly_total.sort_values("value", ascending=True).iloc[0]
        audit.info(
            "L4趋势分析",
            f"基于 `{table_name}` 生成趋势图，样本点={len(summary_df)}。",
            affected_rows=len(summary_df),
        )

        response_text = (
            "### 💡 分析结论\n\n"
            f"已执行确定性 L4 趋势分析（表：{table_name}）。\n\n"
            f"- 峰值月份：{peak_row['__month__']:%Y-%m}，金额 {peak_row['value']:.2f}\n"
            f"- 低谷月份：{low_row['__month__']:%Y-%m}，金额 {low_row['value']:.2f}\n"
            f"- 指标列：`{amount_col}`，时间列：`{date_col}`"
        )
        if dimension_col:
            response_text += f"\n- 分组维度：`{dimension_col}`"

        export_df = summary_df.rename(columns={"__month__": "月份", "value": "金额"}).copy()
        return SkillResult(
            handled=True,
            response_text=response_text,
            result_df=export_df,
            chart_jsons=[fig.to_json()],
            audit=audit,
        )
    except Exception as exc:
        return SkillResult(
            handled=True,
            audit=audit,
            error=f"{type(exc).__name__}: {exc}",
            error_type=ERROR_TYPE_RUNTIME,
        )
