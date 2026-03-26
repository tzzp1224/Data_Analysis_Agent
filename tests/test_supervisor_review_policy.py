from __future__ import annotations

from app.orchestration.agent_first_graph import supervisor_review_node
from app.orchestration.prompts import PROMPT_VERSION


def _base_state(error_type: str, blocked: bool = False):
    return {
        "plan_steps": [
            {
                "step_id": "step_01",
                "goal": "test",
                "candidate_workers": ["l3_reconcile"],
                "selected_worker": "l3_reconcile",
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
