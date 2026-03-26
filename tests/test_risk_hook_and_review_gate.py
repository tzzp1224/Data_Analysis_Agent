from __future__ import annotations

import pandas as pd

from app.orchestration.agent_first_graph import review_gate_node, supervisor_dispatch_node
from app.utils.tools import AuditLogger


def test_dispatch_triggers_hook_on_low_confidence_merge():
    dfs_context = {
        "sales.xlsx": pd.DataFrame({"客户": ["A"], "金额": [1]}),
        "master.xlsx": pd.DataFrame({"客户": ["A"], "等级": ["VIP"]}),
    }
    state = {
        "user_instruction": "请合并两张表",
        "plan_steps": [
            {
                "step_id": "step_01",
                "goal": "合并",
                "candidate_workers": ["l2_merge", "agent_worker"],
                "selected_worker": "l2_merge",
                "fallback_worker": "agent_worker",
                "fallback_attempted": False,
                "retry_policy": {},
                "confidence": 0.45,
                "risk_level": "high",
                "status": "pending",
                "retry_count": 0,
                "error_type": "",
                "summary": "",
            }
        ],
        "current_step_idx": 0,
        "step_results": [],
    }

    payload = supervisor_dispatch_node(state, dfs_context=dfs_context, official_supervisor_app=None)

    assert payload["status"] == "awaiting_human"
    assert payload["pending_hook"]["hook_type"] in {"select_option", "approve"}
    assert payload["router_decision"] == "finalize"


def test_review_gate_requires_human_for_high_risk_without_audit():
    state = {
        "reply": "done",
        "user_instruction": "请严格执行对账",
        "plan_steps": [
            {"step_id": "step_01", "selected_worker": "l3_reconcile", "requires_review": True}
        ],
        "step_results": [{"step_id": "step_01", "handled": True}],
        "route_trace": [],
        "chart_jsons": [],
    }

    payload = review_gate_node(state, dfs_context={})

    assert payload["status"] == "awaiting_human"
    assert payload["pending_hook"]["hook_type"] == "approve"
    assert payload["router_decision"] == "finalize"


def test_review_gate_passes_when_audit_exists():
    state = {
        "reply": "done",
        "user_instruction": "请执行对账",
        "plan_steps": [
            {"step_id": "step_01", "selected_worker": "l3_reconcile", "requires_review": True}
        ],
        "step_results": [{"step_id": "step_01", "handled": True}],
        "route_trace": [],
        "chart_jsons": [],
    }

    payload = review_gate_node(state, dfs_context={"__last_audit__": AuditLogger()})

    assert payload["status"] == "done"
    assert payload["router_decision"] == "finalize"
