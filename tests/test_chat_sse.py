from __future__ import annotations

import json
from typing import Any

from fastapi.testclient import TestClient

import app.server as server
from app.orchestration.prompts import PROMPT_VERSION


def _seed_session(session_id: str) -> server.SessionData:
    session = server.SessionData()
    session.agent_graph_app = object()
    server.sessions[session_id] = session
    return session


def _minimal_trace(stage: str, action: str, detail: str = "ok") -> dict[str, Any]:
    return {
        "timestamp": "2026-03-26 10:00:00",
        "stage": stage,
        "action": action,
        "detail": detail,
        "prompt_version": PROMPT_VERSION,
    }


def _parse_sse_events(raw: str) -> list[tuple[str, dict[str, Any]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    event_name = "message"
    data_lines: list[str] = []

    for line in raw.splitlines():
        if line == "":
            if data_lines:
                payload = json.loads("\n".join(data_lines))
                events.append((event_name, payload))
            event_name = "message"
            data_lines = []
            continue

        if line.startswith("event:"):
            event_name = line.split(":", 1)[1].strip() or "message"
        elif line.startswith("data:"):
            data_lines.append(line.split(":", 1)[1].strip())

    if data_lines:
        payload = json.loads("\n".join(data_lines))
        events.append((event_name, payload))
    return events


def test_chat_stream_emits_ordered_events_and_final_payload(monkeypatch):
    server.sessions.clear()
    sid = "s-sse-01"
    _seed_session(sid)

    monkeypatch.setattr(server, "ensure_semantic_contract", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(server, "build_export_response", lambda *_args, **_kwargs: (None, None))

    def fake_run(*_args, **_kwargs):
        return {
            "status": "done",
            "execution_status": "done",
            "reply": "执行完成",
            "plan_id": "plan_sse_1",
            "plan_steps": [{"step_id": "step_01", "selected_worker": "l1_hygiene", "status": "completed"}],
            "current_step_idx": 0,
            "step_results": [{"step_id": "step_01", "worker": "l1_hygiene", "handled": True, "blocked": False}],
            "route_trace": [
                _minimal_trace("supervisor_plan", "planned", "create plan"),
                _minimal_trace("supervisor_dispatch", "dispatch", "step=step_01 worker=l1_hygiene"),
                _minimal_trace("worker_execute", "success", "worker ok"),
                _minimal_trace("finalize", "done", "done"),
            ],
            "audit_envelope": [],
            "chart_jsons": [],
            "pending_hitl": {},
        }

    monkeypatch.setattr(server, "run_agent_first_workflow", fake_run)

    with TestClient(server.app) as client:
        resp = client.post(
            "/chat/stream",
            json={
                "session_id": sid,
                "message": "执行任务",
            },
        )

    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers.get("content-type", "")
    events = _parse_sse_events(resp.text)
    names = [name for name, _ in events]
    assert names[0] == "connected"
    assert names[1] == "workflow_started"
    assert "plan_created" in names
    assert "route_selected" in names
    assert "worker_started" in names
    assert "worker_finished" in names
    assert "final" in names
    assert names[-1] == "done"

    final_payload = next(payload for name, payload in events if name == "final")
    response = dict(final_payload.get("response") or {})
    assert response.get("status") == "done"
    assert response.get("execution", {}).get("plan_id") == "plan_sse_1"
