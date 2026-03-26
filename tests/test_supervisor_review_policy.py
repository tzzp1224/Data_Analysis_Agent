from __future__ import annotations

from app.orchestration.agent_first_graph import supervisor_review_node
from app.orchestration.prompts import PROMPT_VERSION


def _base_state(
    error_type: str,
    blocked: bool = False,
    *,
    selected_worker: str = "l3_reconcile",
    fallback_worker: str = "",
    fallback_attempted: bool = False,
    validation_status: str = "",
):
    return {
        "plan_steps": [
            {
                "step_id": "step_01",
                "goal": "test",
                "candidate_workers": [selected_worker],
                "selected_worker": selected_worker,
                "fallback_worker": fallback_worker,
                "fallback_attempted": fallback_attempted,
                "retry_policy": {
                    "runtime_error_max_retries": 2,
                    "non_retryable_error_types": [
                        "missing_required_columns",
                        "merge_key_invalid",
                        "table_selection_failed",
                    ],
                },
                "status": "running",
                "retry_count": 0,
            }
        ],
        "current_step_idx": 0,
        "step_results": [],
        "step_execution": {
            "handled": False,
            "blocked": blocked,
            "error_type": error_type,
            "summary": "failed",
            "response_text": "",
            "validation_status": validation_status,
        },
    }


def test_runtime_error_retries():
    state = _base_state("runtime_error", blocked=False)
    result = supervisor_review_node(state)

    assert result["router_decision"] == "supervisor_dispatch"
    assert result["plan_steps"][0]["retry_count"] == 1
    assert result["status"] == "running"
    assert result["route_trace"][0]["prompt_version"] == PROMPT_VERSION


def test_non_retryable_error_goes_hitl():
    state = _base_state("missing_required_columns", blocked=True)
    result = supervisor_review_node(state)

    assert result["router_decision"] == "finalize"
    assert result["status"] == "awaiting_human"
    assert "pending_hitl" in result
    assert result["route_trace"][0]["prompt_version"] == PROMPT_VERSION


def test_blocked_runtime_error_goes_hitl_without_supervisor_retry():
    state = _base_state("runtime_error", blocked=True)
    result = supervisor_review_node(state)

    assert result["router_decision"] == "finalize"
    assert result["status"] == "awaiting_human"
    assert result["plan_steps"][0]["retry_count"] == 0


def test_agent_retry_exhausted_switches_to_deterministic_fallback():
    state = _base_state(
        "runtime_error",
        blocked=True,
        selected_worker="agent_worker",
        fallback_worker="l3_reconcile",
        fallback_attempted=False,
        validation_status="retry_exhausted",
    )
    result = supervisor_review_node(state)

    assert result["router_decision"] == "supervisor_dispatch"
    assert result["status"] == "running"
    assert result["plan_steps"][0]["selected_worker"] == "l3_reconcile"
    assert result["plan_steps"][0]["fallback_attempted"] is True
    assert result["route_trace"][0]["action"] == "fallback_to_deterministic"
    assert result["route_trace"][0]["prompt_version"] == PROMPT_VERSION


def test_agent_retry_exhausted_after_fallback_attempt_goes_hitl():
    state = _base_state(
        "runtime_error",
        blocked=True,
        selected_worker="agent_worker",
        fallback_worker="l3_reconcile",
        fallback_attempted=True,
        validation_status="retry_exhausted",
    )
    result = supervisor_review_node(state)

    assert result["router_decision"] == "finalize"
    assert result["status"] == "awaiting_human"
