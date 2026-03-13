from __future__ import annotations

from typing import Dict, Iterable, Optional

import pandas as pd


L3_KEYWORDS = ("对账", "核对流水", "银行流水", "系统日记账", "差异", "容差")
L1_KEYWORDS = ("清洗", "体检", "去重")
L2_KEYWORDS = ("对齐", "合并", "主数据", "关联")
SALES_NAME_TOKENS = ("客户名称", "客户", "company", "client")
MASTER_NAME_TOKENS = ("标准公司名", "公司名称", "主数据", "master")


def _has_token(columns: Iterable[str], tokens: Iterable[str]) -> bool:
    text_cols = [str(col) for col in columns]
    for token in tokens:
        if any(token in col for col in text_cols):
            return True
    return False


def route_skill(user_instruction: str, dfs_context: Dict[str, pd.DataFrame]) -> Optional[str]:
    text = str(user_instruction or "")
    business_tables = [
        df for name, df in (dfs_context or {}).items() if not str(name).startswith("__")
    ]
    if any(keyword in text for keyword in L3_KEYWORDS) and len(business_tables) >= 2:
        return "l3_reconcile"

    if not business_tables:
        return None

    has_l1_intent = any(keyword in text for keyword in L1_KEYWORDS)
    has_l2_intent = any(keyword in text for keyword in L2_KEYWORDS)
    if not (has_l1_intent or has_l2_intent):
        return None

    if len(business_tables) == 1:
        return "l1_hygiene"

    has_sales_like = False
    has_master_like = False
    for df in business_tables:
        cols = list(df.columns)
        if _has_token(cols, SALES_NAME_TOKENS):
            has_sales_like = True
        if _has_token(cols, MASTER_NAME_TOKENS):
            has_master_like = True
    if has_sales_like and has_master_like and (has_l1_intent or has_l2_intent):
        return "l1_l2_hygiene_merge"
    return None
