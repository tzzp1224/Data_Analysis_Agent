from __future__ import annotations

import re
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd

from app.services.semantic_infer import ColumnSemantic, SemanticInferenceResult, infer_dataframe_semantics
from app.skills.contracts import SkillResult
from app.utils.tools import AuditLogger, smart_reconcile


SYS_KEY_TOKENS = ("外部流水号", "外部流水", "流水号", "交易号", "订单号")
SYS_AMOUNT_TOKENS = ("应收金额", "金额", "应收", "总额", "amount")
BANK_KEY_TOKENS = ("交易流水", "银行流水", "流水号", "外部流水号", "参考号")
BANK_AMOUNT_TOKENS = ("到账金额", "入账金额", "金额", "交易金额", "amount")


def _find_column(columns: Iterable[str], tokens: Iterable[str]) -> Optional[str]:
    text_cols = [str(col) for col in columns]
    for token in tokens:
        for col in text_cols:
            if token.lower() in col.lower():
                return col
    return None


def _clean_amount(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("¥", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.strip(),
        errors="coerce",
    ).fillna(0.0)


def _parse_tolerance(instruction: str) -> float:
    text = str(instruction or "")
    match = re.search(r"容差\s*([0-9]+(?:\.[0-9]+)?)", text)
    if match:
        return float(match.group(1))
    yuan_match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*元", text)
    if yuan_match and ("容差" in text or "忽略" in text):
        return float(yuan_match.group(1))
    return 0.01


def _pick_semantic_column(
    sem: SemanticInferenceResult,
    desired_label: str,
    min_confidence: float,
) -> Optional[str]:
    candidates = [c for c in sem.columns if c.label == desired_label and c.confidence >= min_confidence]
    if not candidates:
        return None
    best = sorted(candidates, key=lambda x: x.confidence, reverse=True)[0]
    return best.name


def _table_role_score(table_name: str, sem: SemanticInferenceResult) -> Tuple[float, float]:
    name = table_name.lower()
    sys_score = 0.0
    bank_score = 0.0
    if re.search(r"系统|ledger|erp", name):
        sys_score += 1.0
    if re.search(r"银行|bank|statement", name):
        bank_score += 1.0

    has_amount = any(c.label == "amount" and c.confidence >= 0.6 for c in sem.columns)
    has_id = any(c.label == "id" and c.confidence >= 0.55 for c in sem.columns)
    if has_amount:
        sys_score += 0.8
        bank_score += 0.8
    if has_id:
        sys_score += 0.5
        bank_score += 0.5
    return sys_score, bank_score


def _pick_tables(
    dfs_context: Dict[str, pd.DataFrame],
    instruction: str,
) -> Tuple[
    Optional[str],
    Optional[pd.DataFrame],
    Optional[SemanticInferenceResult],
    Optional[str],
    Optional[pd.DataFrame],
    Optional[SemanticInferenceResult],
]:
    business_tables = [
        (str(name), df)
        for name, df in (dfs_context or {}).items()
        if not str(name).startswith("__")
    ]
    if len(business_tables) < 2:
        return None, None, None, None, None, None

    semantics = {
        name: infer_dataframe_semantics(df, table_name=name, user_instruction=instruction)
        for name, df in business_tables
    }
    scored = []
    for name, df in business_tables:
        sys_score, bank_score = _table_role_score(name, semantics[name])
        scored.append((name, df, semantics[name], sys_score, bank_score))

    sys_sorted = sorted(scored, key=lambda x: x[3], reverse=True)
    bank_sorted = sorted(scored, key=lambda x: x[4], reverse=True)
    sys_name, sys_df, sys_sem, _, _ = sys_sorted[0]
    bank_name, bank_df, bank_sem, _, _ = bank_sorted[0]
    if sys_name == bank_name and len(bank_sorted) > 1:
        bank_name, bank_df, bank_sem, _, _ = bank_sorted[1]
    if sys_name == bank_name:
        return None, None, None, None, None, None
    return sys_name, sys_df, sys_sem, bank_name, bank_df, bank_sem


def _format_missing_column_message(
    table_name: str,
    missing_type: str,
    df: pd.DataFrame,
    sem: SemanticInferenceResult,
) -> str:
    columns_preview = ", ".join([str(c) for c in list(df.columns)[:12]])
    sem_preview = ", ".join([f"{c.name}:{c.label}({c.confidence:.2f})" for c in sem.columns[:8]])
    return (
        f"无法执行对账：文件 `{table_name}` 未识别到可用的{missing_type}列。\n\n"
        f"当前列: {columns_preview or '无'}\n\n"
        f"语义识别结果(前8列): {sem_preview or '无'}\n\n"
        "请补充/明确该列后再执行对账（例如在列名中包含金额或交易流水语义）。"
    )


def run_l3_reconcile_skill(dfs_context: Dict[str, pd.DataFrame], instruction: str) -> SkillResult:
    audit = AuditLogger()
    try:
        sys_name, sys_df, sys_sem, bank_name, bank_df, bank_sem = _pick_tables(dfs_context, instruction=instruction)
        if sys_df is None or bank_df is None or sys_sem is None or bank_sem is None:
            return SkillResult(
                handled=True,
                response_text="无法执行对账：未找到两张可区分的业务明细表。",
                audit=audit,
            )

        semantic_notes: list[str] = []
        for sem_name, sem in ((sys_name, sys_sem), (bank_name, bank_sem)):
            if sem.used_fallback:
                note = f"{sem_name}: 语义判定回退启发式，已保守处理。"
                semantic_notes.append(note)
                audit.info("语义回退", note, affected_rows=0)
            for warning in sem.warnings[:2]:
                semantic_notes.append(f"{sem_name}: {warning}")

        sys_key = _pick_semantic_column(sys_sem, "id", min_confidence=0.55) or _find_column(sys_df.columns, SYS_KEY_TOKENS)
        bank_key = _pick_semantic_column(bank_sem, "id", min_confidence=0.55) or _find_column(bank_df.columns, BANK_KEY_TOKENS)
        sys_amount = _pick_semantic_column(sys_sem, "amount", min_confidence=0.6) or _find_column(sys_df.columns, SYS_AMOUNT_TOKENS)
        bank_amount = _pick_semantic_column(bank_sem, "amount", min_confidence=0.6) or _find_column(bank_df.columns, BANK_AMOUNT_TOKENS)

        if not sys_amount:
            return SkillResult(
                handled=True,
                response_text=_format_missing_column_message(sys_name, "金额", sys_df, sys_sem),
                audit=audit,
            )
        if not bank_amount:
            return SkillResult(
                handled=True,
                response_text=_format_missing_column_message(bank_name, "金额", bank_df, bank_sem),
                audit=audit,
            )
        if not sys_key:
            return SkillResult(
                handled=True,
                response_text=_format_missing_column_message(sys_name, "主键/流水", sys_df, sys_sem),
                audit=audit,
            )
        if not bank_key:
            return SkillResult(
                handled=True,
                response_text=_format_missing_column_message(bank_name, "主键/流水", bank_df, bank_sem),
                audit=audit,
            )

        tolerance = _parse_tolerance(instruction)
        audit.info("L3Skill", f"命中语义增强对账流程，容差={tolerance}", affected_rows=0)

        sys_norm = sys_df.copy()
        bank_norm = bank_df.copy()
        sys_norm[sys_key] = sys_norm[sys_key].astype(str).str.strip()
        bank_norm[bank_key] = bank_norm[bank_key].astype(str).str.strip()
        sys_norm[sys_amount] = _clean_amount(sys_norm[sys_amount])
        bank_norm[bank_amount] = _clean_amount(bank_norm[bank_amount])

        before_rows = len(sys_norm)
        sys_grouped = sys_norm.groupby(sys_key, as_index=False)[sys_amount].sum()
        reduced_rows = max(0, before_rows - len(sys_grouped))
        audit.info(
            "多对一聚合",
            f"系统表按 {sys_key} 聚合：{before_rows} -> {len(sys_grouped)}",
            affected_rows=reduced_rows,
        )

        result_df = smart_reconcile(
            sys_grouped,
            bank_norm,
            sys_key=sys_key,
            bank_key=bank_key,
            sys_amount=sys_amount,
            bank_amount=bank_amount,
            tolerance=tolerance,
            logger=audit,
        )

        status_summary = "无状态字段"
        if "对账状态" in result_df.columns:
            counts = result_df["对账状态"].value_counts().to_dict()
            status_summary = "，".join([f"{k}:{v}" for k, v in counts.items()])

        response_text = (
            "### 💡 分析结论\n\n"
            f"已执行语义增强 L3 对账流程（容差={tolerance:g}）。\n\n"
            f"系统表：{sys_name}；银行表：{bank_name}。\n\n"
            f"系统聚合后 {len(sys_grouped)} 条，对账结果 {len(result_df)} 条。\n\n"
            f"状态分布：{status_summary}"
        )
        if semantic_notes:
            response_text += "\n\n⚠️ 语义提示:\n" + "\n".join(f"- {note}" for note in semantic_notes[:5])
        return SkillResult(
            handled=True,
            response_text=response_text,
            result_df=result_df,
            audit=audit,
        )
    except Exception as exc:
        return SkillResult(
            handled=True,
            audit=audit,
            error=f"{type(exc).__name__}: {exc}",
        )
