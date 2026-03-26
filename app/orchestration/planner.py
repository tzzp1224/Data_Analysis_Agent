from __future__ import annotations

import json
import re
import uuid
from typing import Any, Dict

import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from app.orchestration.contracts import (
    ALLOWED_WORKERS,
    PlanStep,
    TaskPlanV2,
    WORKER_GOALS,
    WORKER_ORDER,
)
from app.orchestration.prompts import planner_system_prompt
from app.services.llm_factory import get_llm
from app.skills.catalog_registry import collect_intent_hits
from app.skills.router import route_skill


_MAX_STEPS = 6
_MULTI_STEP_HINTS = (
    "先",
    "然后",
    "再",
    "最后",
    "接着",
    "step",
    "first",
    "then",
    "finally",
)


def _extract_json(raw_text: str) -> dict | None:
    text = str(raw_text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except Exception:
        return None


def _table_brief(dfs_context: Dict[str, pd.DataFrame]) -> list[dict]:
    payload = []
    for name, df in dfs_context.items():
        if str(name).startswith("__"):
            continue
        payload.append(
            {
                "name": str(name),
                "rows": int(len(df)),
                "columns": [str(c) for c in list(df.columns)[:16]],
            }
        )
    return payload[:8]


def _normalize_worker(value: Any) -> str:
    worker = str(value or "").strip().lower()
    alias = {
        "agent": "agent_worker",
        "worker": "agent_worker",
        "l1": "l1_hygiene",
        "l2": "l2_merge",
        "l3": "l3_reconcile",
        "l4": "l4_visual",
        "l5": "l5_anomaly",
        "hygiene": "l1_hygiene",
        "merge": "l2_merge",
        "reconcile": "l3_reconcile",
        "visual": "l4_visual",
        "anomaly": "l5_anomaly",
    }
    return alias.get(worker, worker)


def _default_retry_policy(_worker: str) -> dict[str, Any]:
    return {
        "runtime_error_max_retries": 2,
        "non_retryable_error_types": [
            "missing_required_columns",
            "merge_key_invalid",
            "table_selection_failed",
        ],
    }


def _business_table_count(dfs_context: Dict[str, pd.DataFrame]) -> int:
    return len([name for name in dfs_context.keys() if not str(name).startswith("__")])


def _is_worker_applicable(worker: str, table_count: int) -> bool:
    if worker in {"l2_merge", "l3_reconcile"}:
        return table_count >= 2
    if worker in {"l1_hygiene", "l4_visual", "l5_anomaly", "agent_worker"}:
        return table_count >= 1
    return False


def _make_step(worker: str, *, idx: int, goal: str = "") -> PlanStep:
    selected_worker = _normalize_worker(worker)
    return PlanStep(
        step_id=f"step_{idx:02d}",
        goal=goal or WORKER_GOALS.get(selected_worker, "执行任务步骤"),
        candidate_workers=[selected_worker],
        selected_worker=selected_worker,
        retry_policy=_default_retry_policy(selected_worker),
    )


def _build_fallback_steps(user_instruction: str, dfs_context: Dict[str, pd.DataFrame]) -> list[PlanStep]:
    instruction = str(user_instruction or "")
    instruction_lower = instruction.lower()
    table_count = _business_table_count(dfs_context)
    intent_hits = collect_intent_hits(instruction)

    explicit_multi = any(token in instruction_lower for token in _MULTI_STEP_HINTS)
    if explicit_multi:
        workers = [worker for worker in WORKER_ORDER if worker in intent_hits]
        # For merge/reconcile chains, prepend hygiene if user asks for multi-step processing.
        if any(w in workers for w in {"l2_merge", "l3_reconcile"}) and "l1_hygiene" not in workers:
            workers.insert(0, "l1_hygiene")
    else:
        ranked = sorted(
            intent_hits.items(),
            key=lambda item: (-item[1], WORKER_ORDER.index(item[0]) if item[0] in WORKER_ORDER else 99),
        )
        workers = [ranked[0][0]] if ranked else []

    if not workers:
        routed = route_skill(instruction, dfs_context)
        if routed:
            workers = [routed]

    workers = [w for w in workers if _is_worker_applicable(w, table_count)]
    if not workers and table_count >= 1:
        workers = ["agent_worker"]

    deduped: list[str] = []
    for worker in workers:
        if deduped and deduped[-1] == worker:
            continue
        deduped.append(worker)

    return [_make_step(worker, idx=i) for i, worker in enumerate(deduped[:_MAX_STEPS], start=1)]


def _parse_llm_steps(payload: dict, table_count: int) -> list[PlanStep]:
    raw_steps = payload.get("steps", []) if isinstance(payload, dict) else []
    steps: list[PlanStep] = []

    for idx, item in enumerate(raw_steps[:_MAX_STEPS], start=1):
        if not isinstance(item, dict):
            continue

        goal = str(item.get("goal", "")).strip()
        raw_candidates = item.get("candidate_workers") or []
        candidates = [_normalize_worker(v) for v in raw_candidates if _normalize_worker(v) in ALLOWED_WORKERS]

        selected = _normalize_worker(item.get("selected_worker"))
        if selected not in ALLOWED_WORKERS:
            selected = candidates[0] if candidates else ""

        if not selected:
            continue
        if not _is_worker_applicable(selected, table_count):
            continue

        if selected not in candidates:
            candidates = [selected] + candidates

        retry_policy = item.get("retry_policy") if isinstance(item.get("retry_policy"), dict) else {}
        merged_retry = _default_retry_policy(selected)
        merged_retry.update(retry_policy)

        steps.append(
            PlanStep(
                step_id=f"step_{idx:02d}",
                goal=goal or WORKER_GOALS.get(selected, "执行任务步骤"),
                candidate_workers=candidates,
                selected_worker=selected,
                retry_policy=merged_retry,
            )
        )

    deduped: list[PlanStep] = []
    for step in steps:
        if deduped and deduped[-1].selected_worker == step.selected_worker:
            continue
        deduped.append(step)
    return deduped


def _fallback_plan_v2(user_instruction: str, dfs_context: Dict[str, pd.DataFrame]) -> TaskPlanV2:
    return TaskPlanV2(
        plan_id=f"plan_{uuid.uuid4().hex[:10]}",
        steps=_build_fallback_steps(user_instruction, dfs_context),
        reason="Fallback deterministic planning from SKILL catalog and intent rules.",
        used_fallback=True,
    )


def build_task_plan_v2(user_instruction: str, dfs_context: Dict[str, pd.DataFrame]) -> TaskPlanV2:
    try:
        llm = get_llm(temperature=0)
    except Exception:
        return _fallback_plan_v2(user_instruction=user_instruction, dfs_context=dfs_context)

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", planner_system_prompt()),
            (
                "human",
                """
用户指令:
{instruction}

当前表概览:
{tables}
""".strip(),
            ),
        ]
    )

    chain = prompt | llm | StrOutputParser()
    table_count = _business_table_count(dfs_context)

    try:
        raw = chain.invoke(
            {
                "instruction": str(user_instruction or ""),
                "tables": json.dumps(_table_brief(dfs_context), ensure_ascii=False),
            }
        )
        payload = _extract_json(raw)
        if payload:
            parsed_steps = _parse_llm_steps(payload, table_count=table_count)
            if parsed_steps:
                return TaskPlanV2(
                    plan_id=f"plan_{uuid.uuid4().hex[:10]}",
                    steps=parsed_steps,
                    reason=str(payload.get("reason", "")).strip() or "LLM planner",
                    used_fallback=False,
                )
    except Exception:
        pass

    return _fallback_plan_v2(user_instruction=user_instruction, dfs_context=dfs_context)
