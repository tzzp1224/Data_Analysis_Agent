from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

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
        score = (1.2 if has_date else 0.0) + (1.2 if has_amount else 0.0)
        candidates.append((str(name), df, sem, score))

    if not candidates:
        return None, None, None
    candidates.sort(key=lambda x: x[3], reverse=True)
    name, df, sem, _ = candidates[0]
    if _ < 1.2:
        return None, None, None
    return name, df, sem


def _coerce_amount(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("¥", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce")


def run_l5_anomaly_skill(
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
                response_text="L5已阻断：当前没有可用于异常检测的业务表。",
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
                skill_name="L5 异常检测",
                table_name=table_name,
                df=source_df,
                sem=sem,
                missing_specs=missing,
                guidance="请补充日期列和金额列后再执行异常检测。",
            )
            return SkillResult(
                handled=True,
                response_text=block_msg,
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
            )

        date_col = resolved["date_col"]
        amount_col = resolved["amount_col"]

        work_df = source_df.copy()
        work_df[date_col] = pd.to_datetime(work_df[date_col], errors="coerce")
        work_df[amount_col] = _coerce_amount(work_df[amount_col])
        work_df = work_df.dropna(subset=[date_col, amount_col]).copy()
        if work_df.empty:
            return SkillResult(
                handled=True,
                response_text="L5已阻断：日期或金额列无法解析为有效数据。",
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
            )

        work_df["__period__"] = work_df[date_col].dt.to_period("M").dt.to_timestamp()
        grouped = (
            work_df.groupby("__period__", as_index=False)[amount_col]
            .sum()
            .rename(columns={amount_col: "value"})
            .sort_values("__period__")
            .reset_index(drop=True)
        )
        if len(grouped) < 3:
            return SkillResult(
                handled=True,
                response_text="L5已阻断：有效时间点不足（至少3个周期）无法判断异常。",
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
            )

        mean = float(grouped["value"].mean())
        std = float(grouped["value"].std(ddof=0))
        if std <= 1e-9:
            grouped["z_score"] = 0.0
        else:
            grouped["z_score"] = (grouped["value"] - mean) / std

        grouped["pct_change"] = grouped["value"].pct_change().fillna(0.0)
        grouped["is_anomaly"] = grouped["z_score"].abs() >= 2.0
        if not bool(grouped["is_anomaly"].any()):
            grouped["is_anomaly"] = grouped["pct_change"].abs() >= 0.5

        anomalies = grouped[grouped["is_anomaly"]].copy()
        anomaly_count = int(len(anomalies))
        audit.info(
            "L5异常检测",
            f"基于 `{table_name}` 完成异常检测，周期点={len(grouped)}，异常点={anomaly_count}。",
            affected_rows=anomaly_count,
        )

        if anomalies.empty:
            response_text = (
                "### 💡 分析结论\n\n"
                f"已完成 L5 异常检测（表：{table_name}），未识别到显著异常波动。"
            )
        else:
            top_anomaly = anomalies.sort_values("z_score", key=lambda s: s.abs(), ascending=False).iloc[0]
            response_text = (
                "### 💡 分析结论\n\n"
                f"已完成 L5 异常检测（表：{table_name}）。\n\n"
                f"检测到 {anomaly_count} 个异常周期，最显著异常出现在 {top_anomaly['__period__']:%Y-%m}，"
                f"金额 {top_anomaly['value']:.2f}，z-score={top_anomaly['z_score']:.2f}。"
            )

        export_df = grouped.rename(columns={"__period__": "周期", "value": "金额"}).copy()
        return SkillResult(
            handled=True,
            response_text=response_text,
            result_df=export_df,
            audit=audit,
        )
    except Exception as exc:
        return SkillResult(
            handled=True,
            audit=audit,
            error=f"{type(exc).__name__}: {exc}",
            error_type=ERROR_TYPE_RUNTIME,
        )
