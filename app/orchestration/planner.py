from __future__ import annotations

import json
import re
from typing import Any, Dict

import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from app.orchestration.contracts import ALLOWED_WORKERS, TaskPlan, TaskStep
from app.services.llm_factory import get_llm
from app.skills.router import route_skill


_MAX_STEPS = 4


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
        "l1": "l1_hygiene",
        "l2": "l2_merge",
        "l3": "l3_reconcile",
        "l4": "l4_visual",
        "hygiene": "l1_hygiene",
        "merge": "l2_merge",
        "reconcile": "l3_reconcile",
        "visual": "l4_visual",
    }
    worker = alias.get(worker, worker)
    return worker


def _fallback_plan(user_instruction: str, dfs_context: Dict[str, pd.DataFrame]) -> TaskPlan:
    routed = route_skill(user_instruction, dfs_context)
    if routed == "l2_merge":
        return TaskPlan(
            steps=[
                TaskStep(worker="l1_hygiene", goal="先完成数据体检与基础清洗"),
                TaskStep(worker="l2_merge", goal="执行主数据对齐与合并"),
            ],
            reason="Fallback: deterministic router mapped to L1 + L2 merge chain.",
            used_fallback=True,
        )
    if routed == "l1_hygiene":
        return TaskPlan(
            steps=[TaskStep(worker="l1_hygiene", goal="执行数据体检与清洗")],
            reason="Fallback: deterministic router mapped to L1.",
            used_fallback=True,
        )
    if routed == "l3_reconcile":
        return TaskPlan(
            steps=[TaskStep(worker="l3_reconcile", goal="执行财务对账")],
            reason="Fallback: deterministic router mapped to L3.",
            used_fallback=True,
        )
    if routed == "l4_visual":
        return TaskPlan(
            steps=[TaskStep(worker="l4_visual", goal="执行趋势分析与可视化")],
            reason="Fallback: deterministic router mapped to L4.",
            used_fallback=True,
        )
    return TaskPlan(
        steps=[],
        reason="Fallback: no deterministic skill matched, continue workflow fallback.",
        used_fallback=True,
    )


def _parse_llm_plan(payload: dict) -> TaskPlan:
    steps: list[TaskStep] = []
    raw_steps = payload.get("steps", []) if isinstance(payload, dict) else []
    for item in raw_steps[:_MAX_STEPS]:
        if not isinstance(item, dict):
            continue
        worker = _normalize_worker(item.get("worker"))
        if worker not in ALLOWED_WORKERS:
            continue
        goal = str(item.get("goal", "")).strip()
        steps.append(TaskStep(worker=worker, goal=goal))
    # Drop adjacent duplicates to keep execution concise.
    deduped: list[TaskStep] = []
    for step in steps:
        if deduped and deduped[-1].worker == step.worker:
            continue
        deduped.append(step)
    reason = str(payload.get("reason", "")).strip() if isinstance(payload, dict) else ""
    return TaskPlan(steps=deduped, reason=reason, used_fallback=False)


def build_task_plan(user_instruction: str, dfs_context: Dict[str, pd.DataFrame]) -> TaskPlan:
    try:
        llm = get_llm(temperature=0)
    except Exception:
        return _fallback_plan(user_instruction=user_instruction, dfs_context=dfs_context)
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
你是 Supervisor，仅负责拆解任务并路由到 worker，不执行数据处理。

可选 worker:
- l1_hygiene: 数据体检/清洗
- l2_merge: 多表实体对齐与合并
- l3_reconcile: 财务对账
- l4_visual: 趋势分析与可视化

要求:
1. 支持多步计划，按执行顺序给 steps。
2. 只输出 JSON，不要解释。
3. 只可使用允许的 worker。
4. 若用户是闲聊或无可执行任务，返回空 steps。

输出 JSON:
{{
  "steps": [{{"worker":"l1_hygiene","goal":"一句话目标"}}],
  "reason": "简短原因"
}}
""".strip(),
            ),
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
    try:
        raw = chain.invoke(
            {
                "instruction": str(user_instruction or ""),
                "tables": json.dumps(_table_brief(dfs_context), ensure_ascii=False),
            }
        )
        payload = _extract_json(raw)
        if payload:
            plan = _parse_llm_plan(payload)
            if plan.steps:
                return plan
    except Exception:
        pass
    return _fallback_plan(user_instruction=user_instruction, dfs_context=dfs_context)
