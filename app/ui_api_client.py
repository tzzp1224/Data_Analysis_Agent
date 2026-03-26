from __future__ import annotations

import json
from typing import Any, Iterator

import requests


def upload_files(api_url: str, session_id: str, uploaded_files: list[Any]) -> dict[str, Any]:
    files_data = [("files", (item.name, item, item.type or "application/octet-stream")) for item in uploaded_files]
    payload = {"session_id": session_id}
    response = requests.post(f"{api_url}/upload", data=payload, files=files_data, timeout=120)
    response.raise_for_status()
    return dict(response.json() or {})


def chat_sync(api_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(f"{api_url}/chat", json=payload, timeout=300)
    response.raise_for_status()
    return dict(response.json() or {})


def _flush_sse_event(event_name: str, data_lines: list[str]) -> tuple[str, dict[str, Any]] | None:
    if not data_lines:
        return None
    raw = "\n".join(data_lines).strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        payload = {"raw": raw}
    return event_name, payload


def chat_stream(api_url: str, payload: dict[str, Any]) -> Iterator[tuple[str, dict[str, Any]]]:
    with requests.post(
        f"{api_url}/chat/stream",
        json=payload,
        stream=True,
        timeout=300,
        headers={"Accept": "text/event-stream"},
    ) as response:
        response.raise_for_status()
        event_name = "message"
        data_lines: list[str] = []

        for raw_line in response.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue
            line = str(raw_line).strip("\r")
            if line == "":
                event = _flush_sse_event(event_name, data_lines)
                if event is not None:
                    yield event
                event_name = "message"
                data_lines = []
                continue
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event_name = line.split(":", 1)[1].strip() or "message"
                continue
            if line.startswith("data:"):
                data_lines.append(line.split(":", 1)[1].strip())

        event = _flush_sse_event(event_name, data_lines)
        if event is not None:
            yield event
