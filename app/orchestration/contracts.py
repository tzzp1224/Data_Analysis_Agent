from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any

ALLOWED_WORKERS = (
    "agent_worker",
    "expert_excel",
    "l1_hygiene",
    "l2_merge",
    "l3_reconcile",
    "l4_visual",
    "l5_anomaly",
)

WORKER_ORDER = (
    "l1_hygiene",
    "l2_merge",
    "l3_reconcile",
    "l4_visual",
    "l5_anomaly",
    "expert_excel",
    "agent_worker",
)

WORKER_GOALS: dict[str, str] = {
    "agent_worker": "执行通用 agent 任务",
    "expert_excel": "执行复杂自然语言 Excel 操作的结构化兜底",
    "l1_hygiene": "执行数据体检与语义增强清洗",
    "l2_merge": "执行主数据对齐与合并",
    "l3_reconcile": "执行财务对账",
    "l4_visual": "执行趋势分析与可视化",
    "l5_anomaly": "执行异常波动检测",
}

WORKER_CAPABILITIES: dict[str, str] = {
    "agent_worker": "General-purpose agent execution for uncategorized requests.",
    "expert_excel": "Expert fallback for complex or low-confidence Excel instructions.",
    "l1_hygiene": "Data hygiene and semantic cleaning.",
    "l2_merge": "Entity alignment and table merge.",
    "l3_reconcile": "Finance reconciliation.",
    "l4_visual": "Trend visualization.",
    "l5_anomaly": "Anomaly detection for time-series metrics.",
}


@dataclass(frozen=True)
class ToolSpec:
    name: str
    input_contract: str
    output_contract: str
    deterministic: bool = True


INTERNAL_TOOL_SPECS: tuple[ToolSpec, ...] = (
    ToolSpec(
        name="audit",
        input_contract="operation/reason/rows",
        output_contract="structured audit log entries",
        deterministic=True,
    ),
    ToolSpec(
        name="smart_merge",
        input_contract="left/right dataframe + key columns",
        output_contract="merged dataframe with alignment logging",
        deterministic=True,
    ),
    ToolSpec(
        name="smart_reconcile",
        input_contract="system/bank dataframe + keys + amount columns + tolerance",
        output_contract="reconciliation dataframe",
        deterministic=True,
    ),
    ToolSpec(
        name="reload_data",
        input_contract="filename",
        output_contract="restored dataframe from in-memory backup",
        deterministic=True,
    ),
)


@dataclass
class PlanStep:
    step_id: str
    goal: str
    candidate_workers: list[str] = field(default_factory=list)
    selected_worker: str = ""
    fallback_worker: str = ""
    fallback_attempted: bool = False
    retry_policy: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.5
    risk_level: str = "low"
    intent_source: str = "planner"
    requires_review: bool = False
    status: str = "pending"
    retry_count: int = 0
    error_type: str = ""
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TaskPlanV2:
    plan_id: str
    steps: list[PlanStep] = field(default_factory=list)
    reason: str = ""
    used_fallback: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "reason": self.reason,
            "used_fallback": self.used_fallback,
            "steps": [step.to_dict() for step in self.steps],
        }


@dataclass
class StepResult:
    step_id: str
    worker: str
    handled: bool
    blocked: bool = False
    error_type: str = ""
    summary: str = ""
    response_text: str = ""
    retry_count: int = 0
    attempt: int = 1
    observation: str = ""
    validation_status: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class StepScratchpad:
    attempt: int = 1
    reasoning: str = ""
    action_code_preview: str = ""
    observation: str = ""
    status: str = "running"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ContextPacket:
    system_invariants: list[str] = field(default_factory=list)
    plan_slice: dict[str, Any] = field(default_factory=dict)
    schema_digest: dict[str, Any] = field(default_factory=dict)
    memory_slice: dict[str, Any] = field(default_factory=dict)
    error_feedback: str = ""
    attempt: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def fingerprint(self) -> str:
        payload = json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)
        return hashlib.md5(payload.encode("utf-8")).hexdigest()[:16]
