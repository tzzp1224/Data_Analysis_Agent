from __future__ import annotations

from typing import Dict, Optional

import pandas as pd


L3_KEYWORDS = ("对账", "核对流水", "银行流水", "系统日记账", "差异", "容差")
L1_KEYWORDS = ("清洗", "体检", "去重")
L2_KEYWORDS = ("对齐", "合并", "主数据", "关联")
L4_KEYWORDS = ("趋势", "图表", "可视化", "波动", "gmv", "plot", "chart", "trend")


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
    has_l4_intent = any(keyword.lower() in text.lower() for keyword in L4_KEYWORDS)

    if has_l4_intent:
        return "l4_visual"
    if has_l2_intent and len(business_tables) >= 2:
        return "l1_l2_hygiene_merge"
    if has_l1_intent:
        return "l1_hygiene"
    return None
