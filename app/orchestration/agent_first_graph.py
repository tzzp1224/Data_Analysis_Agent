from __future__ import annotations

import json
import operator
import time
from functools import partial
from typing import Annotated, Any, Dict, List, Optional, TypedDict

import pandas as pd
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langgraph.graph import END, MessagesState, StateGraph
from pydantic import BaseModel, Field

from app.core.config import settings
from app.orchestration.agent_worker_runtime import (
    build_context_packet,
    clean_code_string,
    execute_code,
    python_worker_node,
)
from app.orchestration.contracts import ALLOWED_WORKERS, StepScratchpad, WORKER_CAPABILITIES
from app.orchestration.planner import build_task_plan_v2
from app.orchestration.prompts import PROMPT_VERSION, router_system_prompt
from app.services.llm_factory import get_llm
from app.skills.catalog_registry import ensure_registry_ready, get_skill_registry
from app.skills.engine import execute_skill
from app.utils.tools import AuditLogger

try:
    from langgraph.prebuilt import create_supervisor as langgraph_prebuilt_create_supervisor
except Exception:  # pragma: no cover
    langgraph_prebuilt_create_supervisor = None

try:
    from langgraph_supervisor import create_supervisor as langgraph_package_create_supervisor
except Exception:  # pragma: no cover
    langgraph_package_create_supervisor = None


NON_RETRYABLE_DEFAULT = {
    "missing_required_columns",
    "merge_key_invalid",
    "table_selection_failed",
}
AGENT_WORKER_MAX_ATTEMPTS = 2


def get_official_supervisor_backend() -> Optional[str]:
    if langgraph_prebuilt_create_supervisor is not None:
        return "langgraph.prebuilt.create_supervisor"
    if langgraph_package_create_supervisor is not None:
        return "langgraph_supervisor.create_supervisor"
    return None


def _get_supervisor_factory():
    if langgraph_prebuilt_create_supervisor is not None:
        return langgraph_prebuilt_create_supervisor
    if langgraph_package_create_supervisor is not None:
        return langgraph_package_create_supervisor
    return None


class WorkerRouteDecision(BaseModel):
    route: str = Field(default="agent_worker", description="Worker chosen for current step.")
    reason: str = Field(default="", description="Concise rationale")


def _build_route_worker(worker_name: str, capability: str):
    workflow = StateGraph(MessagesState)

    def respond_node(_: MessagesState):
        return {"messages": [AIMessage(content=f"{worker_name}: {capability}")]}

    workflow.add_node("respond", respond_node)
    workflow.set_entry_point("respond")
    workflow.add_edge("respond", END)
    return workflow.compile(name=worker_name)


def _normalize_structured_payload(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "model_dump"):
        try:
            return dict(value.model_dump())
        except Exception:
            return {}
    return {}


def _create_official_supervisor_router_app() -> tuple[Optional[Any], str]:
    factory = _get_supervisor_factory()
    if factory is None:
        return None, "official supervisor factory unavailable"

    try:
        model = get_llm(temperature=0)
    except Exception as exc:
        return None, f"llm init failed: {exc}"

    prompt = router_system_prompt()

    try:
        workers = [
            _build_route_worker(worker, WORKER_CAPABILITIES.get(worker, worker))
            for worker in ALLOWED_WORKERS
        ]
        graph = factory(
            workers,
            model=model,
            prompt=prompt,
            response_format=WorkerRouteDecision,
            supervisor_name="task_supervisor",
        )
        return graph.compile(name="official_supervisor_router_v2"), ""
    except Exception as exc:
        return None, f"official supervisor compile failed: {exc}"


def _route_with_official_supervisor(
    supervisor_app,
    *,
    instruction: str,
    goal: str,
    candidates: list[str],
    default_worker: str,
) -> tuple[str, str]:
    if supervisor_app is None:
        return default_worker, "official supervisor app unavailable"

    safe_candidates = [worker for worker in candidates if worker in ALLOWED_WORKERS]
    if not safe_candidates:
        return default_worker, "no valid candidates"

    payload = {
        "instruction": instruction,
        "goal": goal,
        "candidates": safe_candidates,
        "default_worker": default_worker,
    }
    try:
        result = supervisor_app.invoke(
            {"messages": [HumanMessage(content=json.dumps(payload, ensure_ascii=False))]},
            config={"recursion_limit": 20},
        )
        structured = _normalize_structured_payload(result.get("structured_response"))
        proposed = str(structured.get("route", "")).strip()
        reason = str(structured.get("reason", "")).strip()
        if proposed in safe_candidates:
            return proposed, reason
    except Exception as exc:
        return default_worker, f"official routing failed: {exc}"

    return default_worker, "official route missing/invalid"


class AgentFirstState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]
    user_instruction: str
    human_action: str
    resume_plan_id: str
    pending_state: Dict[str, Any]

    plan_id: str
    plan_steps: List[dict]
    current_step_idx: int
    dispatch_worker: str
    step_execution: Dict[str, Any]

    step_results: Annotated[List[dict], operator.add]

    router_decision: str
    execution_status: str
    status: str
    reply: str
    next_action: str
    pending_hitl: Dict[str, Any]

    route_trace: Annotated[List[dict], operator.add]
    audit_envelope: Annotated[List[dict], operator.add]
    chart_jsons: Annotated[List[str], operator.add]


def _event(stage: str, action: str, detail: str, **extra: Any) -> dict:
    payload = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "stage": stage,
        "action": action,
        "detail": detail,
        "prompt_version": PROMPT_VERSION,
    }
    payload.update(extra)
    return payload


def _merge_audit(target: AuditLogger, source: AuditLogger | None) -> None:
    if source is None:
        return
    for entry in source.logs:
        target.logs.append(dict(entry))
    for key, df in source.excluded_data.items():
        target.excluded_data[f"{key}_{len(target.excluded_data)}"] = df


def merge_audit_envelope(base_audit: Optional[AuditLogger], envelope: list[dict]) -> Optional[AuditLogger]:
    if not envelope and not base_audit:
        return None
    merged = AuditLogger()
    for idx, item in enumerate(envelope or [], start=1):
        step = f"RouteTrace-{idx:02d}"
        detail = f"{item.get('stage','?')}::{item.get('action','?')} - {item.get('detail','')}"
        merged.info(step, detail, affected_rows=0)
    _merge_audit(merged, base_audit)
    return merged


def _coerce_step_dict(step: dict, fallback_id: str) -> dict:
    selected = str(step.get("selected_worker", "")).strip()
    candidates = [str(item).strip() for item in step.get("candidate_workers", []) if str(item).strip()]
    if selected and selected not in candidates:
        candidates = [selected] + candidates
    return {
        "step_id": str(step.get("step_id", fallback_id)).strip() or fallback_id,
        "goal": str(step.get("goal", "")).strip(),
        "candidate_workers": candidates,
        "selected_worker": selected,
        "retry_policy": dict(step.get("retry_policy") or {}),
        "status": str(step.get("status", "pending")).strip() or "pending",
        "retry_count": int(step.get("retry_count", 0) or 0),
        "error_type": str(step.get("error_type", "")).strip(),
        "summary": str(step.get("summary", "")).strip(),
    }


def _normalize_plan_steps(raw_steps: list[dict]) -> list[dict]:
    steps: list[dict] = []
    for idx, step in enumerate(raw_steps or [], start=1):
        if not isinstance(step, dict):
            continue
        normalized = _coerce_step_dict(step, fallback_id=f"step_{idx:02d}")
        if normalized["selected_worker"] not in ALLOWED_WORKERS:
            continue
        if not normalized["candidate_workers"]:
            normalized["candidate_workers"] = [normalized["selected_worker"]]
        steps.append(normalized)
    return steps


def _build_plan_summary(steps: list[dict]) -> str:
    lines = [
        f"{idx}. {step.get('selected_worker')} - {step.get('goal') or '执行步骤'}"
        for idx, step in enumerate(steps, start=1)
    ]
    return "\n".join(lines)


def _build_done_reply(plan_steps: list[dict], step_results: list[dict]) -> str:
    completed = [step for step in plan_steps if step.get("status") == "completed"]
    blocked = [step for step in plan_steps if step.get("status") in {"blocked", "failed"}]
    lines = ["### ✅ 执行完成"]
    lines.append("")
    lines.append(f"- 完成步骤: {len(completed)}/{len(plan_steps)}")
    if blocked:
        lines.append(f"- 阻断步骤: {len(blocked)}")
    if step_results:
        latest = step_results[-1]
        if latest.get("summary"):
            lines.append(f"- 最后一步: {latest.get('summary')}")
    return "\n".join(lines)


def supervisor_plan_node(state: AgentFirstState, dfs_context: Dict[str, pd.DataFrame]):
    instruction = str(state.get("user_instruction", "")).strip()
    human_action = str(state.get("human_action", "")).strip().lower()
    resume_plan_id = str(state.get("resume_plan_id", "")).strip()
    pending = state.get("pending_state") or {}

    if pending:
        pending_plan_id = str(pending.get("plan_id", "")).strip()
        if resume_plan_id and pending_plan_id and resume_plan_id != pending_plan_id:
            event = _event("supervisor_plan", "resume_plan_mismatch", "resume_plan_id 与待续跑计划不一致。")
            return {
                "router_decision": "finalize",
                "status": "blocked",
                "execution_status": "blocked",
                "reply": "resume_plan_id 与当前待续跑计划不一致，已拒绝续跑。",
                "route_trace": [event],
                "audit_envelope": [event],
            }

        if not human_action:
            reason = str(pending.get("pending_hitl", {}).get("reason", "任务等待人工确认。")).strip()
            event = _event("supervisor_plan", "await_human", reason, plan_id=pending_plan_id)
            return {
                "router_decision": "finalize",
                "status": "awaiting_human",
                "execution_status": "awaiting_human",
                "reply": f"🧑‍⚖️ 任务等待人工确认。\n\n原因：{reason}",
                "next_action": "请提供 human_action=approve|reject|revise。",
                "route_trace": [event],
                "audit_envelope": [event],
                "plan_id": pending_plan_id,
                "plan_steps": list(pending.get("plan_steps") or []),
                "current_step_idx": int(pending.get("current_step_idx", 0) or 0),
                "step_results": list(pending.get("step_results") or []),
            }

        if human_action == "reject":
            event = _event("supervisor_plan", "human_reject", "人工拒绝继续执行。", plan_id=pending_plan_id)
            return {
                "router_decision": "finalize",
                "status": "blocked",
                "execution_status": "blocked",
                "reply": "已按人工指令终止执行。",
                "next_action": "可提交新指令重新规划。",
                "route_trace": [event],
                "audit_envelope": [event],
                "pending_hitl": {},
                "plan_id": pending_plan_id,
                "plan_steps": list(pending.get("plan_steps") or []),
                "current_step_idx": int(pending.get("current_step_idx", 0) or 0),
                "step_results": list(pending.get("step_results") or []),
            }

        if human_action == "revise":
            event = _event("supervisor_plan", "human_revise", "人工要求按新指令重规划。")
            pending = {}
        elif human_action == "approve":
            plan_steps = _normalize_plan_steps(list(pending.get("plan_steps") or []))
            idx = int(pending.get("current_step_idx", 0) or 0)
            event = _event("supervisor_plan", "human_approve", "人工确认后恢复执行。", plan_id=pending_plan_id)
            return {
                "router_decision": "supervisor_dispatch",
                "status": "running",
                "execution_status": "running",
                "pending_hitl": {},
                "plan_id": pending_plan_id,
                "plan_steps": plan_steps,
                "current_step_idx": idx,
                "step_results": list(pending.get("step_results") or []),
                "next_action": "",
                "route_trace": [event],
                "audit_envelope": [event],
            }
        else:
            event = _event("supervisor_plan", "invalid_human_action", f"无效 human_action={human_action}")
            return {
                "router_decision": "finalize",
                "status": "awaiting_human",
                "execution_status": "awaiting_human",
                "reply": "无效 human_action。请使用 approve / reject / revise。",
                "next_action": "重试并提供有效 human_action。",
                "route_trace": [event],
                "audit_envelope": [event],
                "plan_id": pending_plan_id,
                "plan_steps": list(pending.get("plan_steps") or []),
                "current_step_idx": int(pending.get("current_step_idx", 0) or 0),
                "step_results": list(pending.get("step_results") or []),
            }

    plan = build_task_plan_v2(instruction, dfs_context)
    steps = _normalize_plan_steps([step.to_dict() for step in plan.steps])
    if not steps:
        event = _event("supervisor_plan", "no_step", "未生成可执行步骤。")
        return {
            "router_decision": "finalize",
            "status": "blocked",
            "execution_status": "blocked",
            "reply": "未识别可执行步骤，请补充更明确指令。",
            "next_action": "可改为：先清洗，再合并，再对账。",
            "route_trace": [event],
            "audit_envelope": [event],
            "plan_id": plan.plan_id,
            "plan_steps": [],
            "current_step_idx": 0,
        }

    summary = _build_plan_summary(steps)
    event = _event(
        "supervisor_plan",
        "planned",
        f"已生成 {len(steps)} 个步骤。",
        plan_id=plan.plan_id,
        used_fallback=plan.used_fallback,
    )
    return {
        "router_decision": "supervisor_dispatch",
        "status": "running",
        "execution_status": "running",
        "plan_id": plan.plan_id,
        "plan_steps": steps,
        "current_step_idx": 0,
        "reply": f"### 🧭 Supervisor 计划\n\n{summary}",
        "step_results": [],
        "next_action": "",
        "route_trace": [event],
        "audit_envelope": [event],
    }


def supervisor_dispatch_node(
    state: AgentFirstState,
    official_supervisor_app=None,
):
    steps = list(state.get("plan_steps") or [])
    idx = int(state.get("current_step_idx", 0) or 0)

    if idx < 0 or idx >= len(steps):
        event = _event("supervisor_dispatch", "all_done", "所有步骤已完成。")
        return {
            "router_decision": "finalize",
            "status": "done",
            "execution_status": "done",
            "route_trace": [event],
            "audit_envelope": [event],
        }

    step = _coerce_step_dict(steps[idx], fallback_id=f"step_{idx + 1:02d}")
    candidates = [worker for worker in step.get("candidate_workers", []) if worker in ALLOWED_WORKERS]
    selected_worker = step.get("selected_worker")
    if selected_worker not in ALLOWED_WORKERS:
        selected_worker = candidates[0] if candidates else "agent_worker"
    if selected_worker not in candidates:
        candidates = [selected_worker] + candidates

    chosen_worker, route_reason = _route_with_official_supervisor(
        official_supervisor_app,
        instruction=str(state.get("user_instruction", "")),
        goal=str(step.get("goal", "")),
        candidates=candidates,
        default_worker=selected_worker,
    )
    step["selected_worker"] = chosen_worker
    step["candidate_workers"] = candidates
    step["status"] = "running"
    steps[idx] = step

    event = _event(
        "supervisor_dispatch",
        "dispatch",
        f"step={step.get('step_id')} worker={chosen_worker}",
        reason=route_reason,
        current_step_idx=idx,
    )
    return {
        "router_decision": "worker_execute",
        "dispatch_worker": chosen_worker,
        "plan_steps": steps,
        "status": "running",
        "execution_status": "running",
        "route_trace": [event],
        "audit_envelope": [event],
    }


def _run_agent_worker(
    state: AgentFirstState,
    dfs_context: Dict[str, pd.DataFrame],
    backups_context: Optional[Dict[str, pd.DataFrame]] = None,
) -> dict[str, Any]:
    error_feedback = ""
    chart_jsons: list[str] = []

    for attempt in range(1, AGENT_WORKER_MAX_ATTEMPTS + 1):
        context_packet = build_context_packet(
            state,
            dfs_context,
            attempt=attempt,
            error_feedback=error_feedback,
        )
        context_fingerprint = context_packet.fingerprint()
        scratchpad = StepScratchpad(
            attempt=attempt,
            reasoning=f"基于当前 step goal 与上下文执行第 {attempt} 次尝试。",
        )

        worker_resp = python_worker_node(
            state,
            dfs_context,
            mode="custom",
            context_packet=context_packet,
            error_feedback=error_feedback,
            attempt=attempt,
        )
        ai_message = worker_resp["messages"][0]
        code = str(ai_message.content)
        clean_code = clean_code_string(code)
        scratchpad.action_code_preview = clean_code[:220]

        result = execute_code(dfs_context, code, backups_context=backups_context)
        if isinstance(result.get("dfs"), dict):
            dfs_context.clear()
            dfs_context.update(result["dfs"])

        chart_jsons = list(result.get("chart_jsons") or chart_jsons)
        log_text = str(result.get("log", "")).strip()
        done_signal = "WORKER_DONE" in log_text or "WORKER_DONE" in clean_code

        if result.get("success"):
            if result.get("audit_logger"):
                dfs_context["__last_audit__"] = result["audit_logger"]
            if result.get("result_df") is not None:
                dfs_context["__last_result_df__"] = result["result_df"]

            validation_status = "passed" if done_signal else "soft_pass_no_worker_done"
            observation = (
                f"attempt={attempt}, status=success, log_excerpt={log_text[:180]}"
                if log_text
                else f"attempt={attempt}, status=success"
            )
            scratchpad.observation = observation
            scratchpad.status = "done"
            detail = f"agent 执行成功，代码前80字符: {clean_code[:80]}"
            success_signal = "(Signal: WORKER_DONE)" if done_signal else "(Signal missing: WORKER_DONE)"
            return {
                "step_execution": {
                    "handled": True,
                    "blocked": False,
                    "error_type": "",
                    "summary": detail,
                    "response_text": log_text,
                    "worker_log": log_text,
                    "attempt": attempt,
                    "observation": observation,
                    "validation_status": validation_status,
                    "context_fingerprint": context_fingerprint,
                    "scratchpad": scratchpad.to_dict(),
                },
                "messages": [ai_message, HumanMessage(content=f"✅ 成功:\n{log_text}\n{success_signal}")],
                "chart_jsons": chart_jsons,
            }

        error_feedback = log_text or "❌ Runtime Error"
        observation = (
            f"attempt={attempt}, status=failed, error_excerpt={error_feedback[:220]}"
            if error_feedback
            else f"attempt={attempt}, status=failed"
        )
        scratchpad.observation = observation
        scratchpad.status = "failed"

        if attempt < AGENT_WORKER_MAX_ATTEMPTS:
            continue

        error_text = error_feedback or "❌ Runtime Error"
        return {
            "step_execution": {
                "handled": False,
                "blocked": True,
                "error_type": "runtime_error",
                "summary": error_text,
                "response_text": error_text,
                "worker_log": error_text,
                "attempt": attempt,
                "observation": observation,
                "validation_status": "retry_exhausted",
                "context_fingerprint": context_fingerprint,
                "scratchpad": scratchpad.to_dict(),
            },
            "messages": [ai_message, HumanMessage(content=error_text)],
            "chart_jsons": chart_jsons,
        }

    # Defensive fallback (should never be reached).
    return {
        "step_execution": {
            "handled": False,
            "blocked": True,
            "error_type": "runtime_error",
            "summary": "agent_worker 内部状态异常，未获得执行结果。",
            "response_text": "agent_worker 内部状态异常，未获得执行结果。",
            "worker_log": "",
            "attempt": AGENT_WORKER_MAX_ATTEMPTS,
            "observation": "react_loop_unreachable",
            "validation_status": "failed",
            "context_fingerprint": "",
        },
        "chart_jsons": chart_jsons,
    }


def _run_skill_worker(
    worker: str,
    instruction: str,
    dfs_context: Dict[str, pd.DataFrame],
) -> dict[str, Any]:
    result = execute_skill(worker, dfs_context, instruction)

    base_audit = dfs_context.get("__last_audit__")
    if result and result.audit:
        merged = AuditLogger()
        _merge_audit(merged, base_audit)
        _merge_audit(merged, result.audit)
        dfs_context["__last_audit__"] = merged
    elif base_audit is not None:
        dfs_context["__last_audit__"] = base_audit

    if result and result.result_df is not None:
        dfs_context["__last_result_df__"] = result.result_df

    charts = list(result.chart_jsons) if result and result.chart_jsons else []

    if result and result.handled and not result.error and not result.blocked:
        return {
            "step_execution": {
                "handled": True,
                "blocked": False,
                "error_type": "",
                "summary": result.response_text or f"{worker} 执行成功",
                "response_text": result.response_text or "",
                "worker_log": "",
                "attempt": 1,
                "observation": f"deterministic skill `{worker}` succeeded.",
                "validation_status": "passed",
                "context_fingerprint": "",
            },
            "chart_jsons": charts,
        }

    error_type = "runtime_error"
    if result and result.error_type:
        error_type = result.error_type

    summary = "worker 执行失败"
    if result:
        summary = (result.error or result.response_text or summary).strip()

    return {
        "step_execution": {
            "handled": False,
            "blocked": bool(result and result.blocked),
            "error_type": error_type,
            "summary": summary,
            "response_text": result.response_text if result else "",
            "worker_log": result.error if result else summary,
            "attempt": 1,
            "observation": f"deterministic skill `{worker}` failed: {summary[:180]}",
            "validation_status": "failed",
            "context_fingerprint": "",
        },
        "chart_jsons": charts,
    }


def worker_execute_node(
    state: AgentFirstState,
    dfs_context: Dict[str, pd.DataFrame],
    backups_context: Optional[Dict[str, pd.DataFrame]] = None,
):
    worker = str(state.get("dispatch_worker", "")).strip()
    instruction = str(state.get("user_instruction", ""))

    if worker not in ALLOWED_WORKERS:
        payload = {
            "step_execution": {
                "handled": False,
                "blocked": True,
                "error_type": "worker_not_allowed",
                "summary": f"不支持的 worker: {worker}",
                "response_text": "",
                "worker_log": "",
                "attempt": 1,
                "observation": f"invalid worker `{worker}`",
                "validation_status": "failed",
                "context_fingerprint": "",
            }
        }
        event = _event("worker_execute", "invalid_worker", payload["step_execution"]["summary"])
        payload["route_trace"] = [event]
        payload["audit_envelope"] = [event]
        return payload

    if worker == "agent_worker":
        payload = _run_agent_worker(state, dfs_context, backups_context=backups_context)
    else:
        payload = _run_skill_worker(worker, instruction, dfs_context)

    step_execution = dict(payload.get("step_execution") or {})
    handled = bool(step_execution.get("handled"))
    detail = str(step_execution.get("summary", "")).strip()
    event = _event(
        "worker_execute",
        "success" if handled else "failed",
        detail,
        worker=worker,
        attempt=int(step_execution.get("attempt", 1) or 1),
        context_fingerprint=str(step_execution.get("context_fingerprint", "")),
    )
    payload["route_trace"] = [event]
    payload["audit_envelope"] = [event]
    return payload


def supervisor_review_node(state: AgentFirstState):
    steps = list(state.get("plan_steps") or [])
    idx = int(state.get("current_step_idx", 0) or 0)
    execution = dict(state.get("step_execution") or {})

    if idx < 0 or idx >= len(steps):
        event = _event("supervisor_review", "out_of_range", "步骤索引越界。")
        return {
            "router_decision": "finalize",
            "status": "blocked",
            "execution_status": "blocked",
            "reply": "步骤索引异常，流程终止。",
            "route_trace": [event],
            "audit_envelope": [event],
        }

    step = _coerce_step_dict(steps[idx], fallback_id=f"step_{idx + 1:02d}")
    worker = str(step.get("selected_worker", "")).strip()

    handled = bool(execution.get("handled"))
    blocked = bool(execution.get("blocked"))
    error_type = str(execution.get("error_type", "")).strip()
    summary = str(execution.get("summary", "")).strip() or "步骤执行结束"

    step_result = {
        "step_id": step.get("step_id"),
        "worker": worker,
        "handled": handled,
        "blocked": blocked,
        "error_type": error_type,
        "summary": summary,
        "response_text": str(execution.get("response_text", "")).strip(),
        "retry_count": int(step.get("retry_count", 0) or 0),
        "attempt": int(execution.get("attempt", 1) or 1),
        "observation": str(execution.get("observation", "")).strip(),
        "validation_status": str(execution.get("validation_status", "")).strip(),
    }

    if handled:
        step["status"] = "completed"
        step["error_type"] = ""
        step["summary"] = summary
        steps[idx] = step

        if idx + 1 >= len(steps):
            done_reply = _build_done_reply(steps, list(state.get("step_results") or []) + [step_result])
            event = _event("supervisor_review", "done", "全部步骤执行完成。")
            return {
                "router_decision": "finalize",
                "status": "done",
                "execution_status": "done",
                "reply": done_reply,
                "plan_steps": steps,
                "current_step_idx": idx,
                "step_results": [step_result],
                "route_trace": [event],
                "audit_envelope": [event],
            }

        next_idx = idx + 1
        event = _event(
            "supervisor_review",
            "next_step",
            f"step={step.get('step_id')} 完成，进入下一步。",
            next_step_idx=next_idx,
        )
        return {
            "router_decision": "supervisor_dispatch",
            "status": "running",
            "execution_status": "running",
            "plan_steps": steps,
            "current_step_idx": next_idx,
            "step_results": [step_result],
            "route_trace": [event],
            "audit_envelope": [event],
        }

    retry_policy = dict(step.get("retry_policy") or {})
    runtime_max = int(retry_policy.get("runtime_error_max_retries", 2) or 2)
    non_retryable = set(retry_policy.get("non_retryable_error_types") or []) | NON_RETRYABLE_DEFAULT
    retry_count = int(step.get("retry_count", 0) or 0)

    can_retry = (
        (error_type == "runtime_error")
        and (error_type not in non_retryable)
        and (not blocked)
        and (retry_count < runtime_max)
    )

    if can_retry:
        step["retry_count"] = retry_count + 1
        step["status"] = "pending"
        step["error_type"] = error_type
        step["summary"] = summary
        steps[idx] = step

        event = _event(
            "supervisor_review",
            "retry",
            f"{worker} 运行失败，重试 {step['retry_count']}/{runtime_max}",
            error_type=error_type,
        )
        return {
            "router_decision": "supervisor_dispatch",
            "status": "running",
            "execution_status": "running",
            "plan_steps": steps,
            "current_step_idx": idx,
            "step_results": [step_result],
            "route_trace": [event],
            "audit_envelope": [event],
        }

    step["status"] = "blocked" if blocked else "failed"
    step["error_type"] = error_type or "runtime_error"
    step["summary"] = summary
    steps[idx] = step

    reason = summary
    pending_hitl = {
        "reason": reason,
        "error_type": error_type,
    }

    event = _event(
        "supervisor_review",
        "to_hitl",
        reason,
        error_type=error_type,
        step_id=step.get("step_id"),
    )
    return {
        "router_decision": "finalize",
        "status": "awaiting_human",
        "execution_status": "awaiting_human",
        "pending_hitl": pending_hitl,
        "reply": f"🧑‍⚖️ 步骤 `{step.get('step_id')}` 执行失败并进入人工确认。\n\n原因：{reason}",
        "next_action": "human_action=approve|reject|revise",
        "plan_steps": steps,
        "current_step_idx": idx,
        "step_results": [step_result],
        "route_trace": [event],
        "audit_envelope": [event],
    }


def finalize_node(state: AgentFirstState):
    status = str(state.get("status", "")).strip() or "done"
    execution_status = str(state.get("execution_status", "")).strip() or status
    reply = str(state.get("reply", "")).strip()
    if not reply:
        if status == "done":
            reply = "执行完成。"
        elif status == "awaiting_human":
            reply = "任务等待人工确认。"
        elif status == "blocked":
            reply = "任务已阻断。"
        else:
            reply = "流程结束。"

    event = _event("finalize", status, "流程结束。")
    return {
        "status": status,
        "execution_status": execution_status,
        "reply": reply,
        "route_trace": [event],
        "audit_envelope": [event],
    }


def plan_router(state: AgentFirstState):
    decision = str(state.get("router_decision", "supervisor_dispatch")).strip()
    if decision in {"supervisor_dispatch", "finalize"}:
        return decision
    return "supervisor_dispatch"


def review_router(state: AgentFirstState):
    decision = str(state.get("router_decision", "finalize")).strip()
    if decision in {"supervisor_dispatch", "finalize"}:
        return decision
    return "finalize"


def create_agent_first_workflow(
    dfs_context: Dict[str, pd.DataFrame],
    backups_context: Optional[Dict[str, pd.DataFrame]] = None,
):
    ensure_registry_ready()
    registry_errors = get_skill_registry().get_errors()
    if registry_errors:
        formatted = "; ".join(f"{key}: {value}" for key, value in sorted(registry_errors.items()))
        raise RuntimeError(f"Skill catalog contains invalid entries: {formatted}")

    backend = get_official_supervisor_backend()
    if settings.SUPERVISOR_REQUIRE_OFFICIAL and backend is None:
        raise RuntimeError(
            "SUPERVISOR_REQUIRE_OFFICIAL=true but no official supervisor API is available. "
            "Install `langgraph-supervisor` or use a LangGraph build that exposes `langgraph.prebuilt.create_supervisor`."
        )

    official_router_app, init_error = _create_official_supervisor_router_app()
    if settings.SUPERVISOR_REQUIRE_OFFICIAL and official_router_app is None:
        raise RuntimeError(
            "Official supervisor detected but router app initialization failed: "
            f"{init_error or 'unknown error'}"
        )

    workflow = StateGraph(AgentFirstState)
    workflow.add_node("supervisor_plan", partial(supervisor_plan_node, dfs_context=dfs_context))
    workflow.add_node(
        "supervisor_dispatch",
        partial(supervisor_dispatch_node, official_supervisor_app=official_router_app),
    )
    workflow.add_node(
        "worker_execute",
        partial(worker_execute_node, dfs_context=dfs_context, backups_context=backups_context),
    )
    workflow.add_node("supervisor_review", supervisor_review_node)
    workflow.add_node("finalize", finalize_node)

    workflow.set_entry_point("supervisor_plan")
    workflow.add_conditional_edges(
        "supervisor_plan",
        plan_router,
        {
            "supervisor_dispatch": "supervisor_dispatch",
            "finalize": "finalize",
        },
    )
    workflow.add_edge("supervisor_dispatch", "worker_execute")
    workflow.add_edge("worker_execute", "supervisor_review")
    workflow.add_conditional_edges(
        "supervisor_review",
        review_router,
        {
            "supervisor_dispatch": "supervisor_dispatch",
            "finalize": "finalize",
        },
    )
    workflow.add_edge("finalize", END)

    return workflow.compile()


def run_agent_first_workflow(
    app,
    *,
    user_instruction: str,
    human_action: str = "",
    pending_state: Optional[dict] = None,
    resume_plan_id: str = "",
) -> dict:
    initial_state: AgentFirstState = {
        "messages": [],
        "user_instruction": str(user_instruction or ""),
        "human_action": str(human_action or ""),
        "resume_plan_id": str(resume_plan_id or ""),
        "pending_state": dict(pending_state or {}),
        "plan_id": "",
        "plan_steps": [],
        "current_step_idx": 0,
        "dispatch_worker": "",
        "step_execution": {},
        "step_results": [],
        "router_decision": "",
        "execution_status": "running",
        "status": "running",
        "reply": "",
        "next_action": "",
        "pending_hitl": {},
        "route_trace": [],
        "audit_envelope": [],
        "chart_jsons": [],
    }
    return app.invoke(initial_state, config={"recursion_limit": 120})
