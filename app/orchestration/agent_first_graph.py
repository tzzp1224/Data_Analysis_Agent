from __future__ import annotations

import json
import operator
import time
from functools import partial
from typing import Annotated, Any, Dict, List, Literal, Optional, TypedDict

import pandas as pd
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langgraph.graph import END, MessagesState, StateGraph
from pydantic import BaseModel, Field

from app.core.config import settings
from app.services.llm_factory import get_llm
from app.services.workflow import clean_code_string, execute_code, python_worker_node
from app.skills.engine import execute_skill
from app.skills.router import route_skill
from app.utils.tools import AuditLogger

try:
    # Available on some LangGraph distributions.
    from langgraph.prebuilt import create_supervisor as langgraph_prebuilt_create_supervisor
except Exception:  # pragma: no cover - import availability depends on runtime package version.
    langgraph_prebuilt_create_supervisor = None

try:
    # Official supervisor package fallback.
    from langgraph_supervisor import create_supervisor as langgraph_package_create_supervisor
except Exception:  # pragma: no cover - optional dependency
    langgraph_package_create_supervisor = None


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


class OfficialRouteDecision(BaseModel):
    route: Literal["agent_worker", "skill_repair"] = Field(
        default="agent_worker",
        description="选择当前轮应先执行的 worker。",
    )
    reason: str = Field(default="", description="简短理由。")
    risk_level: Literal["low", "medium", "high"] = Field(default="medium")


def _build_route_worker(worker_name: str, capability: str):
    workflow = StateGraph(MessagesState)

    def respond_node(_: MessagesState):
        # Worker 本身不执行业务逻辑，只暴露能力说明给官方 supervisor 进行路由。
        return {"messages": [AIMessage(content=f"{worker_name}: {capability}")]}

    workflow.add_node("respond", respond_node)
    workflow.set_entry_point("respond")
    workflow.add_edge("respond", END)
    return workflow.compile(name=worker_name)


def _create_official_supervisor_router_app() -> tuple[Optional[Any], str]:
    factory = _get_supervisor_factory()
    if factory is None:
        return None, "official supervisor factory unavailable"
    try:
        model = get_llm(temperature=0)
    except Exception as e:
        return None, f"llm init failed: {e}"

    prompt = (
        "You are the routing supervisor for a finance reconciliation system.\n"
        "Choose exactly one worker for this round:\n"
        "- agent_worker: default for dirty real-world tasks and exploratory execution.\n"
        "- skill_repair: only for deterministic low-risk template repair.\n"
        "Rules:\n"
        "1) Agent-first by default.\n"
        "2) Select skill_repair only when allow_direct_skill=true.\n"
        "3) Return concise rationale.\n"
    )
    try:
        graph = factory(
            [
                _build_route_worker(
                    "agent_worker",
                    "General agent execution for dirty headers, entity alias mismatch, and complex finance logic.",
                ),
                _build_route_worker(
                    "skill_repair",
                    "Deterministic repair path for low-risk template fixes and controlled fallback.",
                ),
            ],
            model=model,
            prompt=prompt,
            response_format=OfficialRouteDecision,
            supervisor_name="task_supervisor",
        )
        return graph.compile(name="official_supervisor_router"), ""
    except Exception as e:
        return None, f"official supervisor compile failed: {e}"


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


def _route_with_official_supervisor(
    supervisor_app,
    *,
    instruction: str,
    routed_skill: str,
    risk: str,
    allow_direct_skill: bool,
) -> tuple[Optional[dict[str, str]], str]:
    if supervisor_app is None:
        return None, "official supervisor app unavailable"
    payload = {
        "instruction": instruction,
        "routed_skill": routed_skill or "none",
        "risk_hint": risk,
        "allow_direct_skill": bool(allow_direct_skill),
    }
    try:
        result = supervisor_app.invoke(
            {"messages": [HumanMessage(content=json.dumps(payload, ensure_ascii=False))]},
            config={"recursion_limit": 20},
        )
        structured = _normalize_structured_payload(result.get("structured_response"))
        route = str(structured.get("route", "")).strip()
        if route not in {"agent_worker", "skill_repair"}:
            return None, "missing valid structured route"
        reason = str(structured.get("reason", "")).strip()
        return {"route": route, "reason": reason}, ""
    except Exception as e:
        return None, str(e)


class AgentFirstState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]
    user_instruction: str
    human_action: str
    pending_hitl: Dict[str, Any]
    router_decision: str
    selected_skill: str
    primary: str
    fallback_chain: List[str]
    risk_level: str
    reason: str
    error_count: int
    max_retries: int
    status: str
    reply: str
    next_action: str
    route_trace: Annotated[List[dict], operator.add]
    audit_envelope: Annotated[List[dict], operator.add]
    chart_jsons: Annotated[List[str], operator.add]
    worker_log: str
    last_worker_success: bool


def _event(stage: str, action: str, detail: str, **extra: Any) -> dict:
    payload = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "stage": stage,
        "action": action,
        "detail": detail,
    }
    payload.update(extra)
    return payload


def _risk_level(user_instruction: str, routed_skill: Optional[str]) -> str:
    text = str(user_instruction or "")
    if routed_skill in {"l2_merge", "l3_reconcile"}:
        return "high"
    if any(token in text for token in ("对账", "流水", "容差", "核对", "reconcile")):
        return "high"
    if any(token in text for token in ("合并", "关联", "对齐", "merge")):
        return "medium"
    return "low"


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


def supervisor_node(
    state: AgentFirstState,
    dfs_context: Dict[str, pd.DataFrame],
    official_supervisor_app=None,
):
    instruction = str(state.get("user_instruction", "")).strip()
    pending_hitl = state.get("pending_hitl") or {}
    human_action = str(state.get("human_action", "")).strip().lower()

    if pending_hitl:
        if not human_action:
            reason = str(pending_hitl.get("reason", "上一轮触发阻断，需要人工确认。")).strip()
            event = _event("supervisor", "await_human", reason)
            return {
                "router_decision": "finalize",
                "status": "awaiting_human",
                "reply": f"🧑‍⚖️ 任务已进入人工确认。\n\n原因：{reason}",
                "next_action": "请带上 `human_action` 重试：approve（继续建议路径）/reject（终止）/revise（按新指令重试agent）。",
                "route_trace": [event],
                "audit_envelope": [event],
            }

        if human_action == "reject":
            event = _event("supervisor", "human_reject", "人工拒绝自动续跑。")
            return {
                "router_decision": "finalize",
                "status": "blocked",
                "reply": "已按人工指令终止自动执行。你可以给出新指令重新开始。",
                "next_action": "如需继续，请提交新的业务指令。",
                "pending_hitl": {},
                "route_trace": [event],
                "audit_envelope": [event],
            }

        if human_action == "approve":
            suggested = str(pending_hitl.get("suggested_next", "skill_repair")).strip() or "skill_repair"
            if suggested not in {"agent_worker", "skill_repair"}:
                suggested = "skill_repair"
            event = _event("supervisor", "human_approve", f"人工确认后转入 {suggested}。")
            return {
                "router_decision": suggested,
                "status": "running",
                "pending_hitl": {},
                "selected_skill": str(pending_hitl.get("skill", "")).strip(),
                "route_trace": [event],
                "audit_envelope": [event],
            }

        if human_action == "revise":
            event = _event("supervisor", "human_revise", "人工要求按新指令重跑 agent。")
            return {
                "router_decision": "agent_worker",
                "status": "running",
                "pending_hitl": {},
                "route_trace": [event],
                "audit_envelope": [event],
            }

        event = _event("supervisor", "human_action_invalid", f"无效 human_action={human_action}")
        return {
            "router_decision": "finalize",
            "status": "awaiting_human",
            "reply": "收到无效的 human_action。请使用 approve / reject / revise。",
            "next_action": "重试并提供有效 human_action。",
            "route_trace": [event],
            "audit_envelope": [event],
        }

    routed_skill = route_skill(instruction, dfs_context)
    risk = _risk_level(instruction, routed_skill)
    direct_whitelist = set(settings.SUPERVISOR_DIRECT_SKILL_WHITELIST)
    allow_direct_skill = bool(routed_skill and routed_skill in direct_whitelist and risk == "low")
    policy_default = "skill_repair" if allow_direct_skill else "agent_worker"
    decision = policy_default
    route_source = "policy_default"
    reason_parts = [
        f"agent-first 默认主路径；routed_skill={routed_skill or 'none'}，risk={risk}。",
        "命中低风险白名单，允许直达 skill。" if allow_direct_skill else "先走 agent_worker。",
    ]

    official_route, official_error = _route_with_official_supervisor(
        official_supervisor_app,
        instruction=instruction,
        routed_skill=routed_skill or "",
        risk=risk,
        allow_direct_skill=allow_direct_skill,
    )
    if official_route is not None:
        proposed = official_route["route"]
        if proposed == "skill_repair" and not allow_direct_skill:
            route_source = "official_overridden_to_policy"
            reason_parts.append("官方 supervisor 建议 skill_repair，但不满足白名单，按 agent-first 策略覆盖。")
        else:
            decision = proposed
            route_source = "official_supervisor"
        if official_route.get("reason"):
            reason_parts.append(f"官方 supervisor 理由: {official_route['reason']}")
    elif official_error and official_error != "official supervisor app unavailable":
        route_source = "official_error_fallback"
        reason_parts.append(f"官方 supervisor 调用失败，回退策略路由: {official_error}")

    reason = " ".join(reason_parts).strip()
    event = _event(
        "supervisor",
        "plan",
        reason,
        source=route_source,
        primary="agent_worker",
        fallback_chain=["skill_repair", "human_gate"],
        risk_level=risk,
        selected_skill=routed_skill or "",
    )
    return {
        "router_decision": decision,
        "primary": "agent_worker",
        "fallback_chain": ["skill_repair", "human_gate"],
        "risk_level": risk,
        "reason": reason,
        "selected_skill": routed_skill or "",
        "route_trace": [event],
        "audit_envelope": [event],
    }


def agent_worker_node(
    state: AgentFirstState,
    dfs_context: Dict[str, pd.DataFrame],
    backups_context: Optional[Dict[str, pd.DataFrame]] = None,
):
    worker_resp = python_worker_node(state, dfs_context, mode="custom")
    ai_message = worker_resp["messages"][0]
    code = str(ai_message.content)
    result = execute_code(dfs_context, code, backups_context=backups_context)

    updates: dict[str, Any] = {
        "messages": [ai_message],
        "worker_log": str(result.get("log", "")),
    }

    if isinstance(result.get("dfs"), dict):
        # P0: 回写沙箱返回的 dfs，保证跨轮状态一致。
        dfs_context.clear()
        dfs_context.update(result["dfs"])

    if result.get("success"):
        if result.get("audit_logger"):
            dfs_context["__last_audit__"] = result["audit_logger"]
        if result.get("result_df") is not None:
            dfs_context["__last_result_df__"] = result["result_df"]
        log = str(result.get("log", ""))
        success_msg = HumanMessage(content=f"✅ 成功:\n{log}\n(Signal: WORKER_DONE)")
        updates["messages"].append(success_msg)
        updates["error_count"] = 0
        updates["last_worker_success"] = True
        if result.get("chart_jsons"):
            updates["chart_jsons"] = list(result["chart_jsons"])
        detail = f"agent 执行成功，代码前80字符: {clean_code_string(code)[:80]}"
        event = _event("agent_worker", "success", detail)
    else:
        fail_msg = HumanMessage(content=str(result.get("log", "❌ Runtime Error")))
        updates["messages"].append(fail_msg)
        updates["error_count"] = int(state.get("error_count", 0)) + 1
        updates["last_worker_success"] = False
        detail = f"agent 执行失败，error_count={updates['error_count']}"
        event = _event("agent_worker", "runtime_error", detail)

    updates["route_trace"] = [event]
    updates["audit_envelope"] = [event]
    return updates


def validator_node(state: AgentFirstState, dfs_context: Dict[str, pd.DataFrame]):
    instruction = str(state.get("user_instruction", ""))
    risk = str(state.get("risk_level", "medium"))
    error_count = int(state.get("error_count", 0))
    max_retries = int(state.get("max_retries", 2))
    last_success = bool(state.get("last_worker_success", False))

    if not last_success:
        if error_count <= max_retries:
            detail = f"agent 执行失败，进入重试 {error_count}/{max_retries}"
            event = _event("validator", "retry_agent", detail)
            return {
                "router_decision": "agent_worker",
                "status": "running",
                "route_trace": [event],
                "audit_envelope": [event],
            }
        detail = f"agent 超过最大重试({max_retries})，转 skill_repair"
        event = _event("validator", "to_skill_repair", detail)
        return {
            "router_decision": "skill_repair",
            "status": "running",
            "reply": "检测到多次执行失败，转入 skill 修复路径。",
            "route_trace": [event],
            "audit_envelope": [event],
        }

    # 高风险财务任务必须通过更严格验证后才可交付。
    last_result = dfs_context.get("__last_result_df__")
    last_audit = dfs_context.get("__last_audit__")
    is_reconcile_like = (
        "对账" in instruction
        or "reconcile" in instruction.lower()
        or state.get("selected_skill") == "l3_reconcile"
    )
    audit_ok = bool(last_audit and not last_audit.get_log_df().empty)
    result_ok = isinstance(last_result, pd.DataFrame)
    reconcile_ok = bool(
        (not is_reconcile_like)
        or (isinstance(last_result, pd.DataFrame) and "对账状态" in last_result.columns)
    )

    if risk == "high" and (not result_ok or not audit_ok or not reconcile_ok):
        detail = (
            "高风险任务校验未通过："
            f"result_ok={result_ok}, audit_ok={audit_ok}, reconcile_ok={reconcile_ok}"
        )
        event = _event("validator", "to_skill_repair", detail)
        return {
            "router_decision": "skill_repair",
            "status": "running",
            "reply": "高风险校验未通过，自动转入 skill 修复路径。",
            "route_trace": [event],
            "audit_envelope": [event],
        }

    event = _event("validator", "pass", "验证通过，进入 finalize。")
    return {
        "router_decision": "finalize",
        "status": "done",
        "route_trace": [event],
        "audit_envelope": [event],
    }


def skill_repair_node(state: AgentFirstState, dfs_context: Dict[str, pd.DataFrame]):
    instruction = str(state.get("user_instruction", ""))
    skill_name = str(state.get("selected_skill", "")).strip() or (route_skill(instruction, dfs_context) or "")
    if not skill_name:
        reason = "未能识别可用的修复 skill。"
        event = _event("skill_repair", "to_human_gate", reason)
        return {
            "router_decision": "human_gate",
            "status": "awaiting_human",
            "pending_hitl": {
                "reason": reason,
                "suggested_next": "agent_worker",
            },
            "route_trace": [event],
            "audit_envelope": [event],
        }

    result = execute_skill(skill_name, dfs_context, instruction)
    base_audit = dfs_context.get("__last_audit__")
    if result and result.audit:
        merged = AuditLogger()
        _merge_audit(merged, base_audit)
        _merge_audit(merged, result.audit)
        dfs_context["__last_audit__"] = merged
    elif base_audit is not None:
        dfs_context["__last_audit__"] = base_audit

    updates: dict[str, Any] = {
        "selected_skill": skill_name,
    }
    if result and result.result_df is not None:
        dfs_context["__last_result_df__"] = result.result_df
    if result and result.chart_jsons:
        updates["chart_jsons"] = list(result.chart_jsons)

    if result and result.handled and not result.error and not result.blocked:
        detail = f"skill 修复成功: {skill_name}"
        event = _event("skill_repair", "success", detail, skill=skill_name)
        updates.update(
            {
                "router_decision": "finalize",
                "status": "done",
                "reply": result.response_text or "skill 修复已完成。",
                "route_trace": [event],
                "audit_envelope": [event],
            }
        )
        return updates

    reason = "skill 执行失败或被阻断。"
    if result:
        reason = (result.error or result.response_text or reason).strip()
    event = _event("skill_repair", "to_human_gate", reason, skill=skill_name)
    updates.update(
        {
            "router_decision": "human_gate",
            "status": "awaiting_human",
            "pending_hitl": {
                "reason": reason,
                "skill": skill_name,
                "suggested_next": "skill_repair" if result and result.blocked else "agent_worker",
            },
            "route_trace": [event],
            "audit_envelope": [event],
        }
    )
    return updates


def human_gate_node(state: AgentFirstState):
    pending = state.get("pending_hitl") or {}
    reason = str(pending.get("reason", "任务被阻断，需要人工确认。")).strip()
    reply = (
        "🧑‍⚖️ 已进入人工确认(HITL)。\n\n"
        f"阻断原因：{reason}\n\n"
        "请在下一次请求中携带 `human_action`：approve / reject / revise。"
    )
    event = _event("human_gate", "awaiting_human", reason)
    return {
        "status": "awaiting_human",
        "next_action": "approve=继续建议路径；reject=终止；revise=按新指令重跑agent。",
        "reply": reply,
        "route_trace": [event],
        "audit_envelope": [event],
    }


def finalize_node(state: AgentFirstState):
    status = str(state.get("status", "")).strip() or "done"
    reply = str(state.get("reply", "")).strip()
    if not reply:
        if status == "done":
            reply = "执行完成。"
        elif status == "awaiting_human":
            reply = "任务等待人工确认。"
        elif status == "blocked":
            reply = "任务已阻断。"
        else:
            reply = "执行结束。"
    event = _event("finalize", status, "流程结束。")
    return {
        "status": status,
        "reply": reply,
        "route_trace": [event],
        "audit_envelope": [event],
    }


def supervisor_router(state: AgentFirstState):
    decision = state.get("router_decision", "agent_worker")
    if decision in {"agent_worker", "skill_repair", "finalize"}:
        return decision
    return "agent_worker"


def validator_router(state: AgentFirstState):
    decision = state.get("router_decision", "finalize")
    if decision in {"agent_worker", "skill_repair", "finalize"}:
        return decision
    return "finalize"


def skill_router(state: AgentFirstState):
    decision = state.get("router_decision", "finalize")
    if decision in {"human_gate", "finalize"}:
        return decision
    return "finalize"


def create_agent_first_workflow(
    dfs_context: Dict[str, pd.DataFrame],
    backups_context: Optional[Dict[str, pd.DataFrame]] = None,
):
    backend = get_official_supervisor_backend()
    if settings.SUPERVISOR_REQUIRE_OFFICIAL and backend is None:
        raise RuntimeError(
            "SUPERVISOR_REQUIRE_OFFICIAL=true but no official supervisor API is available. "
            "Install `langgraph-supervisor` or use a LangGraph build that exposes `langgraph.prebuilt.create_supervisor`."
        )
    official_supervisor_app, init_error = _create_official_supervisor_router_app()
    if settings.SUPERVISOR_REQUIRE_OFFICIAL and official_supervisor_app is None:
        raise RuntimeError(
            "Official supervisor detected but router app initialization failed: "
            f"{init_error or 'unknown error'}"
        )

    workflow = StateGraph(AgentFirstState)
    workflow.add_node(
        "supervisor",
        partial(
            supervisor_node,
            dfs_context=dfs_context,
            official_supervisor_app=official_supervisor_app,
        ),
    )
    workflow.add_node(
        "agent_worker",
        partial(agent_worker_node, dfs_context=dfs_context, backups_context=backups_context),
    )
    workflow.add_node("validator", partial(validator_node, dfs_context=dfs_context))
    workflow.add_node("skill_repair", partial(skill_repair_node, dfs_context=dfs_context))
    workflow.add_node("human_gate", human_gate_node)
    workflow.add_node("finalize", finalize_node)

    workflow.set_entry_point("supervisor")
    workflow.add_conditional_edges(
        "supervisor",
        supervisor_router,
        {
            "agent_worker": "agent_worker",
            "skill_repair": "skill_repair",
            "finalize": "finalize",
        },
    )
    workflow.add_edge("agent_worker", "validator")
    workflow.add_conditional_edges(
        "validator",
        validator_router,
        {
            "agent_worker": "agent_worker",
            "skill_repair": "skill_repair",
            "finalize": "finalize",
        },
    )
    workflow.add_conditional_edges(
        "skill_repair",
        skill_router,
        {
            "human_gate": "human_gate",
            "finalize": "finalize",
        },
    )
    workflow.add_edge("human_gate", "finalize")
    workflow.add_edge("finalize", END)
    return workflow.compile()


def run_agent_first_workflow(
    app,
    *,
    user_instruction: str,
    human_action: str = "",
    pending_hitl: Optional[dict] = None,
) -> dict:
    initial_state: AgentFirstState = {
        "messages": [],
        "user_instruction": str(user_instruction or ""),
        "human_action": str(human_action or ""),
        "pending_hitl": dict(pending_hitl or {}),
        "router_decision": "",
        "selected_skill": "",
        "primary": "agent_worker",
        "fallback_chain": ["skill_repair", "human_gate"],
        "risk_level": "low",
        "reason": "",
        "error_count": 0,
        "max_retries": int(settings.SUPERVISOR_MAX_AGENT_RETRIES),
        "status": "running",
        "reply": "",
        "next_action": "",
        "route_trace": [],
        "audit_envelope": [],
        "chart_jsons": [],
        "worker_log": "",
        "last_worker_success": False,
    }
    return app.invoke(initial_state, config={"recursion_limit": 40})
