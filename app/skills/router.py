from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from app.skills.catalog_registry import is_catalog_available, route_worker_from_catalog


L3_KEYWORDS = ("对账", "核对流水", "银行流水", "系统日记账", "差异", "容差")
L1_KEYWORDS = ("清洗", "体检", "去重")
L2_KEYWORDS = ("对齐", "合并", "主数据", "关联")
L4_KEYWORDS = ("趋势", "图表", "可视化", "波动", "gmv", "plot", "chart", "trend")
L5_KEYWORDS = ("异常", "波动异常", "anomaly", "outlier")


def route_skill(user_instruction: str, dfs_context: Dict[str, pd.DataFrame]) -> Optional[str]:
    text = str(user_instruction or "")
    text_lower = text.lower()
    business_tables = [
        df for name, df in (dfs_context or {}).items() if not str(name).startswith("__")
    ]
    min_tables = len(business_tables)

    # P2.3: catalog-first routing with dynamic SKILL.md loading.
    catalog_available = is_catalog_available()
    worker = route_worker_from_catalog(
        text,
        min_tables=min_tables,
    )
    if worker:
        return worker
    if catalog_available:
        return None

    # Legacy fallback is only used when catalog is unavailable.
    if any(keyword in text for keyword in L3_KEYWORDS) and len(business_tables) >= 2:
        return "l3_reconcile"

    if not business_tables:
        return None

    has_l1_intent = any(keyword in text for keyword in L1_KEYWORDS)
    has_l2_intent = any(keyword in text for keyword in L2_KEYWORDS)
    has_l4_intent = any(keyword.lower() in text_lower for keyword in L4_KEYWORDS)
    has_l5_intent = any(keyword.lower() in text_lower for keyword in L5_KEYWORDS)

    if has_l4_intent:
        return "l4_visual"
    if has_l5_intent:
        return "l5_anomaly"
    if has_l2_intent and len(business_tables) >= 2:
        return "l2_merge"
    if has_l1_intent:
        return "l1_hygiene"
    return None
