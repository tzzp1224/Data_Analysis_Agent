from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from app.services.semantic_contract import ensure_semantic_contract
from app.skills.contracts import SkillResult
from app.skills.l1_hygiene_skill import (
    run_l2_merge_skill,
    run_l1_hygiene_skill,
)
from app.skills.l3_reconcile_skill import run_l3_reconcile_skill
from app.skills.l4_visual_skill import run_l4_visual_skill
from app.skills.l5_anomaly_skill import run_l5_anomaly_skill


SUPPORTED_SKILL_WORKERS = (
    "l1_hygiene",
    "l2_merge",
    "l3_reconcile",
    "l4_visual",
    "l5_anomaly",
)


def execute_skill(
    skill_name: str,
    dfs_context: Dict[str, pd.DataFrame],
    instruction: str,
) -> Optional[SkillResult]:
    semantic_contract = ensure_semantic_contract(dfs_context, user_instruction=instruction)
    if skill_name == "l3_reconcile":
        return run_l3_reconcile_skill(
            dfs_context,
            instruction,
            semantic_contract=semantic_contract,
        )
    if skill_name == "l1_hygiene":
        return run_l1_hygiene_skill(
            dfs_context,
            user_instruction=instruction,
            semantic_contract=semantic_contract,
        )
    if skill_name == "l2_merge":
        return run_l2_merge_skill(
            dfs_context,
            user_instruction=instruction,
            semantic_contract=semantic_contract,
        )
    if skill_name == "l4_visual":
        return run_l4_visual_skill(
            dfs_context,
            instruction=instruction,
            semantic_contract=semantic_contract,
        )
    if skill_name == "l5_anomaly":
        return run_l5_anomaly_skill(
            dfs_context,
            instruction=instruction,
            semantic_contract=semantic_contract,
        )
    return None
