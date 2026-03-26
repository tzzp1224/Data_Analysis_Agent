from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExecutionMemory:
    plan_id: str = ""
    plan_steps: list[dict[str, Any]] = field(default_factory=list)
    current_step_idx: int = 0
    step_results: list[dict[str, Any]] = field(default_factory=list)
    pending_hitl: dict[str, Any] = field(default_factory=dict)
    pending_hook: dict[str, Any] = field(default_factory=dict)
    hook_decisions: dict[str, Any] = field(default_factory=dict)

    def to_pending_state(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "plan_steps": list(self.plan_steps),
            "current_step_idx": int(self.current_step_idx),
            "step_results": list(self.step_results),
            "pending_hitl": dict(self.pending_hitl),
            "pending_hook": dict(self.pending_hook),
            "hook_decisions": dict(self.hook_decisions),
        }

    @classmethod
    def from_pending_state(cls, payload: dict[str, Any] | None) -> "ExecutionMemory | None":
        if not payload:
            return None
        return cls(
            plan_id=str(payload.get("plan_id", "")).strip(),
            plan_steps=list(payload.get("plan_steps") or []),
            current_step_idx=int(payload.get("current_step_idx", 0) or 0),
            step_results=list(payload.get("step_results") or []),
            pending_hitl=dict(payload.get("pending_hitl") or {}),
            pending_hook=dict(payload.get("pending_hook") or {}),
            hook_decisions=dict(payload.get("hook_decisions") or {}),
        )
