from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from app.utils.tools import AuditLogger


ALLOWED_WORKERS = (
    "l1_hygiene",
    "l2_merge",
    "l3_reconcile",
    "l4_visual",
)


@dataclass(frozen=True)
class TaskStep:
    worker: str
    goal: str = ""


@dataclass
class TaskPlan:
    steps: list[TaskStep] = field(default_factory=list)
    reason: str = ""
    used_fallback: bool = False


@dataclass
class OrchestrationResult:
    handled: bool
    response_text: str = ""
    result_df: Optional[pd.DataFrame] = None
    chart_jsons: list[str] = field(default_factory=list)
    audit: Optional[AuditLogger] = None
    fallback_to_workflow: bool = False
    fallback_reason: str = ""
