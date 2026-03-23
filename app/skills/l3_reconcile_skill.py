from __future__ import annotations

import re
from typing import Dict, Optional, Tuple

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
from app.utils.tools import AuditLogger, smart_reconcile


SYS_KEY_TOKENS = ("外部流水号", "外部流水", "流水号", "交易号", "订单号")
SYS_AMOUNT_TOKENS = ("应收金额", "金额", "应收", "总额", "amount")
BANK_KEY_TOKENS = ("交易流水", "银行流水", "流水号", "外部流水号", "参考号")
BANK_AMOUNT_TOKENS = ("到账金额", "入账金额", "金额", "交易金额", "amount")

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
    semantic_contract: Optional[Dict[str, SemanticInferenceResult]] = None,
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

    contract = semantic_contract or ensure_semantic_contract(
        dfs_context,
        user_instruction=instruction,
    )
    semantics = {}
    for name, df in business_tables:
        sem = contract.get(name)
        if sem is None:
            sem = infer_dataframe_semantics(df, table_name=name, user_instruction=instruction)
            contract[name] = sem
        semantics[name] = sem
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

def run_l3_reconcile_skill(
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
        sys_name, sys_df, sys_sem, bank_name, bank_df, bank_sem = _pick_tables(
            dfs_context,
            instruction=instruction,
            semantic_contract=contract,
        )
        if sys_df is None or bank_df is None or sys_sem is None or bank_sem is None:
            return SkillResult(
                handled=True,
                response_text="无法执行对账：未找到两张可区分的业务明细表。",
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_TABLE_SELECTION,
            )

        semantic_notes: list[str] = []
        for sem_name, sem in ((sys_name, sys_sem), (bank_name, bank_sem)):
            if sem.used_fallback:
                note = f"{sem_name}: 语义判定回退启发式，已保守处理。"
                semantic_notes.append(note)
                audit.info("语义回退", note, affected_rows=0)
            for warning in sem.warnings[:2]:
                semantic_notes.append(f"{sem_name}: {warning}")

        sys_specs = [
            RequiredColumnSpec(
                key="reconcile_key",
                display_name="系统侧流水/主键列",
                semantic_labels=("id",),
                min_confidence=0.55,
                name_tokens=SYS_KEY_TOKENS,
            ),
            RequiredColumnSpec(
                key="reconcile_amount",
                display_name="系统侧金额列",
                semantic_labels=("amount",),
                min_confidence=0.6,
                name_tokens=SYS_AMOUNT_TOKENS,
            ),
        ]
        bank_specs = [
            RequiredColumnSpec(
                key="reconcile_key",
                display_name="银行侧流水/主键列",
                semantic_labels=("id",),
                min_confidence=0.55,
                name_tokens=BANK_KEY_TOKENS,
            ),
            RequiredColumnSpec(
                key="reconcile_amount",
                display_name="银行侧金额列",
                semantic_labels=("amount",),
                min_confidence=0.6,
                name_tokens=BANK_AMOUNT_TOKENS,
            ),
        ]
        sys_resolved, sys_missing = resolve_required_columns(sys_df, sys_sem, sys_specs)
        if sys_missing:
            return SkillResult(
                handled=True,
                response_text=build_missing_columns_message(
                    skill_name="L3 财务对账",
                    table_name=sys_name,
                    df=sys_df,
                    sem=sys_sem,
                    missing_specs=sys_missing,
                    guidance="请补充系统侧流水号与金额列后再执行对账。",
                ),
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
            )
        bank_resolved, bank_missing = resolve_required_columns(bank_df, bank_sem, bank_specs)
        if bank_missing:
            return SkillResult(
                handled=True,
                response_text=build_missing_columns_message(
                    skill_name="L3 财务对账",
                    table_name=bank_name,
                    df=bank_df,
                    sem=bank_sem,
                    missing_specs=bank_missing,
                    guidance="请补充银行侧流水号与金额列后再执行对账。",
                ),
                audit=audit,
                blocked=True,
                error_type=ERROR_TYPE_MISSING_REQUIRED_COLUMNS,
            )

        sys_key = sys_resolved["reconcile_key"]
        bank_key = bank_resolved["reconcile_key"]
        sys_amount = sys_resolved["reconcile_amount"]
        bank_amount = bank_resolved["reconcile_amount"]

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
            error_type=ERROR_TYPE_RUNTIME,
        )
