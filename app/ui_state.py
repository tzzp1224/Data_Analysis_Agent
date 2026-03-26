from __future__ import annotations

import uuid
from typing import Any

import streamlit as st


def ensure_state() -> None:
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "files_uploaded" not in st.session_state:
        st.session_state.files_uploaded = False
    if "pending_plan_id" not in st.session_state:
        st.session_state.pending_plan_id = None
    if "pending_hook" not in st.session_state:
        st.session_state.pending_hook = {}
    if "data_readiness" not in st.session_state:
        st.session_state.data_readiness = {}
    if "timeline_events" not in st.session_state:
        st.session_state.timeline_events = []


def append_message(
    *,
    role: str,
    content: str,
    charts: list[str] | None = None,
    download: str | None = None,
    events: list[dict[str, Any]] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "role": role,
        "content": content,
    }
    if charts:
        payload["charts"] = list(charts)
    if download:
        payload["download"] = str(download)
    if events:
        payload["events"] = list(events)
    st.session_state.messages.append(payload)


def set_execution_state(status: str, execution: dict[str, Any]) -> None:
    exec_payload = dict(execution or {})
    if str(status) == "awaiting_human":
        st.session_state.pending_plan_id = exec_payload.get("plan_id")
        st.session_state.pending_hook = dict(exec_payload.get("pending_hook") or {})
    else:
        st.session_state.pending_plan_id = None
        st.session_state.pending_hook = {}
    st.session_state.timeline_events = list(exec_payload.get("events") or [])


def set_data_readiness(payload: dict[str, Any]) -> None:
    st.session_state.data_readiness = dict(payload or {})
