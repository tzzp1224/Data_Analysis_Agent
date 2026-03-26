from __future__ import annotations

from langchain_core.messages import AIMessage

import app.orchestration.agent_first_graph as graph
from app.orchestration.contracts import ContextPacket


def _state() -> dict:
    return {
        "messages": [],
        "user_instruction": "请执行复杂分析",
        "plan_id": "plan_react",
        "plan_steps": [
            {
                "step_id": "step_01",
                "goal": "执行通用 agent 任务",
                "selected_worker": "agent_worker",
                "candidate_workers": ["agent_worker"],
                "retry_policy": {"runtime_error_max_retries": 2, "non_retryable_error_types": []},
                "status": "running",
                "retry_count": 0,
            }
        ],
        "current_step_idx": 0,
        "step_results": [],
    }


def test_run_agent_worker_react_retry_then_success(monkeypatch):
    calls = {"execute": 0}

    def fake_build_context_packet(_state, _dfs, *, attempt, error_feedback):
        return ContextPacket(
            system_invariants=["safe"],
            plan_slice={"attempt": attempt},
            schema_digest={"table": {"columns": ["a"]}},
            memory_slice={},
            error_feedback=error_feedback,
            attempt=attempt,
        )

    def fake_python_worker_node(_state, _dfs, mode="custom", **kwargs):
        attempt = int(kwargs.get("attempt", 1) or 1)
        return {"messages": [AIMessage(content=f"print('attempt={attempt}')\\nprint('WORKER_DONE')")]}

    def fake_execute_code(_dfs, _code, backups_context=None):
        del backups_context
        calls["execute"] += 1
        if calls["execute"] == 1:
            return {
                "success": False,
                "dfs": _dfs,
                "chart_jsons": [],
                "result_df": None,
                "audit_logger": None,
                "log": "❌ Runtime Error: first failure",
            }
        return {
            "success": True,
            "dfs": _dfs,
            "chart_jsons": [],
            "result_df": None,
            "audit_logger": None,
            "log": "WORKER_DONE\nall good",
        }

    monkeypatch.setattr(graph, "build_context_packet", fake_build_context_packet)
    monkeypatch.setattr(graph, "python_worker_node", fake_python_worker_node)
    monkeypatch.setattr(graph, "execute_code", fake_execute_code)

    result = graph._run_agent_worker(_state(), {"table.xlsx": {}})

    assert calls["execute"] == 2
    assert result["step_execution"]["handled"] is True
    assert result["step_execution"]["attempt"] == 2
    assert result["step_execution"]["validation_status"] == "passed"
    assert result["step_execution"]["context_fingerprint"]


def test_run_agent_worker_react_retry_exhausted(monkeypatch):
    calls = {"execute": 0}

    def fake_build_context_packet(_state, _dfs, *, attempt, error_feedback):
        return ContextPacket(
            system_invariants=["safe"],
            plan_slice={"attempt": attempt},
            schema_digest={"table": {"columns": ["a"]}},
            memory_slice={},
            error_feedback=error_feedback,
            attempt=attempt,
        )

    def fake_python_worker_node(_state, _dfs, mode="custom", **kwargs):
        attempt = int(kwargs.get("attempt", 1) or 1)
        return {"messages": [AIMessage(content=f"print('attempt={attempt}')")]} 

    def fake_execute_code(_dfs, _code, backups_context=None):
        del backups_context
        calls["execute"] += 1
        return {
            "success": False,
            "dfs": _dfs,
            "chart_jsons": [],
            "result_df": None,
            "audit_logger": None,
            "log": f"❌ Runtime Error: failure #{calls['execute']}",
        }

    monkeypatch.setattr(graph, "build_context_packet", fake_build_context_packet)
    monkeypatch.setattr(graph, "python_worker_node", fake_python_worker_node)
    monkeypatch.setattr(graph, "execute_code", fake_execute_code)

    result = graph._run_agent_worker(_state(), {"table.xlsx": {}})

    assert calls["execute"] == graph.AGENT_WORKER_MAX_ATTEMPTS
    assert result["step_execution"]["handled"] is False
    assert result["step_execution"]["blocked"] is True
    assert result["step_execution"]["error_type"] == "runtime_error"
    assert result["step_execution"]["attempt"] == graph.AGENT_WORKER_MAX_ATTEMPTS
    assert result["step_execution"]["validation_status"] == "retry_exhausted"


def test_worker_execute_route_trace_contains_attempt_and_context(monkeypatch):
    monkeypatch.setattr(
        graph,
        "_run_agent_worker",
        lambda *_args, **_kwargs: {
            "step_execution": {
                "handled": True,
                "blocked": False,
                "error_type": "",
                "summary": "ok",
                "response_text": "",
                "worker_log": "",
                "attempt": 2,
                "observation": "done",
                "validation_status": "passed",
                "context_fingerprint": "fp123",
            },
            "chart_jsons": [],
        },
    )

    payload = graph.worker_execute_node(
        {"dispatch_worker": "agent_worker", "user_instruction": "x"},
        {},
        backups_context={},
    )

    event = payload["route_trace"][0]
    assert event["attempt"] == 2
    assert event["context_fingerprint"] == "fp123"
