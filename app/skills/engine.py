from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from app.services.semantic_contract import ensure_semantic_contract
from app.skills.contracts import SkillResult, SkillResultEnvelope
from app.skills.expert_excel_skill import run_expert_excel_skill
from app.skills.l1_hygiene_skill import (
    run_l2_merge_skill,
    run_l1_hygiene_skill,
)
from app.skills.l3_reconcile_skill import run_l3_reconcile_skill
from app.skills.l4_visual_skill import run_l4_visual_skill
from app.skills.l5_anomaly_skill import run_l5_anomaly_skill


SUPPORTED_SKILL_WORKERS = (
    "expert_excel",
    "l1_hygiene",
    "l2_merge",
    "l3_reconcile",
    "l4_visual",
    "l5_anomaly",
)


def _business_table_count(dfs_context: Dict[str, pd.DataFrame]) -> int:
    return len([name for name in (dfs_context or {}).keys() if not str(name).startswith("__")])


def _precheck(skill_name: str, dfs_context: Dict[str, pd.DataFrame]) -> dict:
    table_count = _business_table_count(dfs_context)
    ok = table_count >= 1
    if skill_name in {"l2_merge", "l3_reconcile"}:
        ok = table_count >= 2
    return {
        "ok": ok,
        "table_count": table_count,
        "skill": skill_name,
    }


def _postcheck(result: Optional[SkillResult]) -> dict:
    if result is None:
        return {"ok": False, "reason": "skill_result_missing"}
    return {
        "ok": bool(result.handled and not result.blocked and not result.error),
        "blocked": bool(result.blocked),
        "error_type": str(result.error_type or ""),
        "has_result_df": bool(result.result_df is not None),
        "chart_count": len(result.chart_jsons or []),
    }


def _wrap_envelope(skill_name: str, result: SkillResult, precheck: dict, postcheck: dict) -> SkillResultEnvelope:
    evidence = dict(result.evidence or {})
    if "skill_postcheck" not in evidence:
        evidence["skill_postcheck"] = dict(postcheck)
    return SkillResultEnvelope(
        skill_name=skill_name,
        result=result,
        precheck=precheck,
        postcheck=postcheck,
        evidence=evidence,
        change_summary=result.change_summary or "",
    )


def execute_skill(
    skill_name: str,
    dfs_context: Dict[str, pd.DataFrame],
    instruction: str,
) -> Optional[SkillResultEnvelope]:
    precheck = _precheck(skill_name, dfs_context)
    if not precheck.get("ok"):
        blocked = SkillResult(
            handled=True,
            blocked=True,
            response_text=f"`{skill_name}` 前置检查未通过：业务表数量不足。",
            error_type="table_selection_failed",
            evidence={"precheck": precheck},
        )
        postcheck = _postcheck(blocked)
        return _wrap_envelope(skill_name, blocked, precheck, postcheck)

    semantic_contract = ensure_semantic_contract(dfs_context, user_instruction=instruction)
    result: Optional[SkillResult] = None

    if skill_name == "expert_excel":
        result = run_expert_excel_skill(
            dfs_context,
            instruction=instruction,
            semantic_contract=semantic_contract,
        )
    if skill_name == "l3_reconcile":
        result = run_l3_reconcile_skill(
            dfs_context,
            instruction,
            semantic_contract=semantic_contract,
        )
    if skill_name == "l1_hygiene":
        result = run_l1_hygiene_skill(
            dfs_context,
            user_instruction=instruction,
            semantic_contract=semantic_contract,
        )
    if skill_name == "l2_merge":
        result = run_l2_merge_skill(
            dfs_context,
            user_instruction=instruction,
            semantic_contract=semantic_contract,
        )
    if skill_name == "l4_visual":
        result = run_l4_visual_skill(
            dfs_context,
            instruction=instruction,
            semantic_contract=semantic_contract,
        )
    if skill_name == "l5_anomaly":
        result = run_l5_anomaly_skill(
            dfs_context,
            instruction=instruction,
            semantic_contract=semantic_contract,
        )

    if result is None:
        return None
    postcheck = _postcheck(result)
    return _wrap_envelope(skill_name, result, precheck, postcheck)
