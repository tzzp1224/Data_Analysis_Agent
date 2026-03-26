from __future__ import annotations

import json
import os
from typing import Any


class JsonMemoryStore:
    """Simple local persistent memory store for session-level preferences and audit events."""

    def __init__(self, root_dir: str = "data/session_memory"):
        self.root_dir = root_dir
        os.makedirs(self.root_dir, exist_ok=True)

    def _path(self, session_id: str) -> str:
        safe = "".join(ch for ch in str(session_id) if ch.isalnum() or ch in {"_", "-"})
        if not safe:
            safe = "session"
        return os.path.join(self.root_dir, f"{safe}.json")

    def load(self, session_id: str) -> dict[str, Any]:
        path = self._path(session_id)
        if not os.path.exists(path):
            return {
                "preferences": {},
                "confirmed_mappings": {},
                "audit_events": [],
            }
        try:
            with open(path, "r", encoding="utf-8") as fp:
                payload = json.load(fp)
            if not isinstance(payload, dict):
                raise ValueError("invalid payload")
            payload.setdefault("preferences", {})
            payload.setdefault("confirmed_mappings", {})
            payload.setdefault("audit_events", [])
            return payload
        except Exception:
            return {
                "preferences": {},
                "confirmed_mappings": {},
                "audit_events": [],
            }

    def save(self, session_id: str, payload: dict[str, Any]) -> None:
        path = self._path(session_id)
        temp_path = f"{path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
        os.replace(temp_path, path)

    def append_event(self, session_id: str, event: dict[str, Any], max_events: int = 500) -> None:
        payload = self.load(session_id)
        events = list(payload.get("audit_events") or [])
        events.append(dict(event))
        payload["audit_events"] = events[-max_events:]
        self.save(session_id, payload)
