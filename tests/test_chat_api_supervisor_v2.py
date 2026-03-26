from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

import app.server as server
from app.orchestration.prompts import PROMPT_VERSION


def _seed_session(session_id: str) -> server.SessionData:
    session = server.SessionData()
    session.agent_graph_app = object()
    server.sessions[session_id] = session
    return session


def _minimal_step(step_id: str, worker: str, status: str = "pending") -> dict[str, Any]:
    return {
        "step_id": step_id,
        "goal": "test",
        "candidate_workers": [worker],
        "selected_worker": worker,
        "retry_policy": {},
        "status": status,
        "retry_count": 0,
        "error_type": "",
        "summary": "",
    }


def _minimal_trace(stage: str, action: str) -> dict[str, Any]:
    return {
        "timestamp": "2026-03-26 10:00:00",
        "stage": stage,
        "action": action,
        "detail": "ok",
        "prompt_version": PROMPT_VERSION,
    }


def test_chat_multi_step_execution_payload(monkeypatch):
    server.sessions.clear()
    sid = "s-multi-step"
    _seed_session(sid)

    monkeypatch.setattr(server, "ensure_semantic_contract", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(server, "build_export_response", lambda *_args, **_kwargs: (None, None))

    def fake_run(*_args, **_kwargs):
        return {
            "status": "done",
            "execution_status": "done",
            "reply": "### ✅ 执行完成",
            "plan_id": "plan_abc",
            "plan_steps": [
                _minimal_step("step_01", "l1_hygiene", status="completed"),
                _minimal_step("step_02", "l2_merge", status="completed"),
                _minimal_step("step_03", "l3_reconcile", status="completed"),
                _minimal_step("step_04", "l4_visual", status="completed"),
            ],
            "current_step_idx": 3,
            "step_results": [
                {"step_id": "step_01", "worker": "l1_hygiene", "handled": True, "blocked": False, "error_type": "", "summary": "ok", "response_text": "", "retry_count": 0},
                {"step_id": "step_02", "worker": "l2_merge", "handled": True, "blocked": False, "error_type": "", "summary": "ok", "response_text": "", "retry_count": 0},
                {"step_id": "step_03", "worker": "l3_reconcile", "handled": True, "blocked": False, "error_type": "", "summary": "ok", "response_text": "", "retry_count": 0},
                {"step_id": "step_04", "worker": "l4_visual", "handled": True, "blocked": False, "error_type": "", "summary": "ok", "response_text": "", "retry_count": 0},
            ],
            "route_trace": [
                _minimal_trace("supervisor_plan", "planned"),
                _minimal_trace("supervisor_dispatch", "dispatch"),
                _minimal_trace("worker_execute", "success"),
                _minimal_trace("supervisor_review", "next_step"),
            ],
            "audit_envelope": [],
            "chart_jsons": [],
            "next_action": "",
            "pending_hitl": {},
        }

    monkeypatch.setattr(server, "run_agent_first_workflow", fake_run)

    with TestClient(server.app) as client:
        resp = client.post("/chat", json={"session_id": sid, "message": "先清洗再合并再对账再可视化"})
    data = resp.json()

    assert resp.status_code == 200
    assert data["status"] == "done"
    assert data["execution"]["plan_id"] == "plan_abc"
    assert data["execution"]["current_step_idx"] == 3
    assert len(data["execution"]["plan_steps"]) == 4
    assert len(data["execution"]["step_results"]) == 4
    assert data["execution"]["route_trace"][0]["prompt_version"] == PROMPT_VERSION
    assert "execution" in data and "artifacts" in data


def test_chat_retry_hitl_and_approve_resume(monkeypatch):
    server.sessions.clear()
    sid = "s-hitl"
    session = _seed_session(sid)

    monkeypatch.setattr(server, "ensure_semantic_contract", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(server, "build_export_response", lambda *_args, **_kwargs: (None, None))

    observed_calls: list[dict[str, Any]] = []

    def fake_run(*_args, **kwargs):
        observed_calls.append(
            {
                "human_action": kwargs.get("human_action", ""),
                "pending_state": kwargs.get("pending_state"),
                "resume_plan_id": kwargs.get("resume_plan_id", ""),
            }
        )
        if not kwargs.get("human_action"):
            return {
                "status": "awaiting_human",
                "execution_status": "awaiting_human",
                "reply": "需要人工确认",
                "next_action": "human_action=approve|reject|revise",
                "plan_id": "plan_hitl_1",
                "plan_steps": [
                    _minimal_step("step_01", "l1_hygiene", status="completed"),
                    _minimal_step("step_02", "agent_worker", status="failed"),
                ],
                "current_step_idx": 1,
                "step_results": [
                    {"step_id": "step_01", "worker": "l1_hygiene", "handled": True, "blocked": False, "error_type": "", "summary": "ok", "response_text": "", "retry_count": 0},
                    {"step_id": "step_02", "worker": "agent_worker", "handled": False, "blocked": False, "error_type": "runtime_error", "summary": "retry exhausted", "response_text": "", "retry_count": 2},
                ],
                "pending_hitl": {"reason": "retry exhausted", "error_type": "runtime_error"},
                "route_trace": [
                    _minimal_trace("supervisor_review", "retry"),
                    _minimal_trace("supervisor_review", "to_hitl"),
                ],
                "audit_envelope": [],
                "chart_jsons": [],
            }

        assert kwargs.get("human_action") == "approve"
        assert kwargs.get("resume_plan_id") == "plan_hitl_1"
        pending_state = kwargs.get("pending_state") or {}
        assert pending_state.get("current_step_idx") == 1

        return {
            "status": "done",
            "execution_status": "done",
            "reply": "续跑完成",
            "next_action": "",
            "plan_id": "plan_hitl_1",
            "plan_steps": [
                _minimal_step("step_01", "l1_hygiene", status="completed"),
                _minimal_step("step_02", "agent_worker", status="completed"),
            ],
            "current_step_idx": 1,
            "step_results": [
                {"step_id": "step_02", "worker": "agent_worker", "handled": True, "blocked": False, "error_type": "", "summary": "ok", "response_text": "", "retry_count": 2},
            ],
            "pending_hitl": {},
            "route_trace": [_minimal_trace("supervisor_review", "done")],
            "audit_envelope": [],
            "chart_jsons": [],
        }

    monkeypatch.setattr(server, "run_agent_first_workflow", fake_run)

    with TestClient(server.app) as client:
        resp1 = client.post("/chat", json={"session_id": sid, "message": "执行任务"})
        assert resp1.status_code == 200
        assert resp1.json()["status"] == "awaiting_human"
        assert session.pending_execution_state is not None

        resp2 = client.post(
            "/chat",
            json={
                "session_id": sid,
                "message": "approve",
                "human_action": "approve",
                "resume_plan_id": "plan_hitl_1",
            },
        )

    assert resp2.status_code == 200
    assert resp2.json()["status"] == "done"
    assert session.pending_execution_state is None
    assert len(observed_calls) == 2
    assert observed_calls[1]["pending_state"]["current_step_idx"] == 1


def test_chat_resume_plan_id_mismatch_is_blocked(monkeypatch):
    server.sessions.clear()
    sid = "s-resume-mismatch"
    session = _seed_session(sid)

    monkeypatch.setattr(server, "ensure_semantic_contract", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(server, "build_export_response", lambda *_args, **_kwargs: (None, None))

    def fake_run(*_args, **kwargs):
        pending_state = kwargs.get("pending_state") or {}
        resume_plan_id = str(kwargs.get("resume_plan_id") or "")
        if not pending_state:
            return {
                "status": "awaiting_human",
                "execution_status": "awaiting_human",
                "reply": "等待人工操作",
                "next_action": "human_action=approve|reject|revise",
                "plan_id": "plan_resume_1",
                "plan_steps": [_minimal_step("step_01", "l1_hygiene", status="failed")],
                "current_step_idx": 0,
                "step_results": [],
                "pending_hitl": {"reason": "blocked", "error_type": "runtime_error"},
                "route_trace": [_minimal_trace("supervisor_review", "to_hitl")],
                "audit_envelope": [],
                "chart_jsons": [],
            }

        if resume_plan_id != str(pending_state.get("plan_id", "")):
            return {
                "status": "blocked",
                "execution_status": "blocked",
                "reply": "resume_plan_id 与当前待续跑计划不一致，已拒绝续跑。",
                "next_action": "提交正确 plan_id 或发起新指令。",
                "plan_id": str(pending_state.get("plan_id", "")),
                "plan_steps": list(pending_state.get("plan_steps") or []),
                "current_step_idx": int(pending_state.get("current_step_idx", 0) or 0),
                "step_results": list(pending_state.get("step_results") or []),
                "pending_hitl": {},
                "route_trace": [_minimal_trace("supervisor_plan", "resume_plan_mismatch")],
                "audit_envelope": [],
                "chart_jsons": [],
            }

        raise AssertionError("Expected mismatch branch")

    monkeypatch.setattr(server, "run_agent_first_workflow", fake_run)

    with TestClient(server.app) as client:
        resp1 = client.post("/chat", json={"session_id": sid, "message": "执行任务"})
        assert resp1.status_code == 200
        assert resp1.json()["status"] == "awaiting_human"
        assert session.pending_execution_state is not None

        resp2 = client.post(
            "/chat",
            json={
                "session_id": sid,
                "message": "approve",
                "human_action": "approve",
                "resume_plan_id": "wrong-plan-id",
            },
        )

    assert resp2.status_code == 200
    assert resp2.json()["status"] == "blocked"
    assert session.pending_execution_state is None
