from __future__ import annotations

from typing import Dict

import pandas as pd

from app.orchestration.contracts import OrchestrationResult, TaskStep
from app.orchestration.planner import build_task_plan
from app.services.semantic_contract import ensure_semantic_contract
from app.skills.engine import execute_skill
from app.utils.tools import AuditLogger


def _merge_audit(target: AuditLogger, source: AuditLogger | None) -> None:
    if source is None:
        return
    for entry in source.logs:
        target.logs.append(dict(entry))
    for key, df in source.excluded_data.items():
        safe_key = f"{key}_{len(target.excluded_data)}"
        target.excluded_data[safe_key] = df

def _execute_step(
    step: TaskStep,
    dfs_context: Dict[str, pd.DataFrame],
    instruction: str,
):
    return execute_skill(step.worker, dfs_context, instruction)


def run_supervisor_orchestration(
    dfs_context: Dict[str, pd.DataFrame],
    instruction: str,
) -> OrchestrationResult:
    plan = build_task_plan(instruction, dfs_context)
    if not plan.steps:
        return OrchestrationResult(
            handled=False,
            fallback_to_workflow=True,
            fallback_reason=plan.reason or "No executable step from supervisor plan.",
        )

    ensure_semantic_contract(dfs_context, user_instruction=instruction)
    merged_audit = AuditLogger()
    merged_audit.info(
        "Supervisor",
        f"执行计划: {' -> '.join(step.worker for step in plan.steps)}",
        affected_rows=0,
    )

    combined_texts: list[str] = []
    combined_charts: list[str] = []
    last_result_df: pd.DataFrame | None = None
    for idx, step in enumerate(plan.steps, start=1):
        step_result = _execute_step(step, dfs_context, instruction)
        if not step_result or not step_result.handled:
            return OrchestrationResult(
                handled=False,
                fallback_to_workflow=True,
                fallback_reason=f"Step {idx} `{step.worker}` not handled.",
            )

        _merge_audit(merged_audit, step_result.audit)
        if step_result.error:
            error_type = step_result.error_type or "unknown_error"
            return OrchestrationResult(
                handled=False,
                fallback_to_workflow=True,
                fallback_reason=f"Step {idx} `{step.worker}` failed ({error_type}): {step_result.error}",
            )
        if step_result.blocked:
            block_type = step_result.error_type or "blocked"
            return OrchestrationResult(
                handled=False,
                fallback_to_workflow=True,
                fallback_reason=f"Step {idx} `{step.worker}` blocked ({block_type}).",
            )

        if step_result.response_text.strip():
            combined_texts.append(step_result.response_text.strip())
        if step_result.chart_jsons:
            combined_charts.extend(step_result.chart_jsons)
        if step_result.result_df is not None:
            last_result_df = step_result.result_df

    plan_lines = [f"{i}. `{step.worker}` {('- ' + step.goal) if step.goal else ''}" for i, step in enumerate(plan.steps, start=1)]
    response_text = "### 🧭 Supervisor 计划\n\n" + "\n".join(plan_lines)
    if combined_texts:
        response_text += "\n\n---\n\n" + "\n\n".join(combined_texts)

    return OrchestrationResult(
        handled=True,
        response_text=response_text,
        result_df=last_result_df,
        chart_jsons=combined_charts,
        audit=merged_audit,
    )
