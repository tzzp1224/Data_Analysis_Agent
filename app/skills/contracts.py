from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from app.utils.tools import AuditLogger


ERROR_TYPE_RUNTIME = "runtime_error"
ERROR_TYPE_TABLE_SELECTION = "table_selection_failed"
ERROR_TYPE_MISSING_REQUIRED_COLUMNS = "missing_required_columns"
ERROR_TYPE_MERGE_KEY_INVALID = "merge_key_invalid"


@dataclass
class SkillResult:
    handled: bool
    response_text: str = ""
    result_df: Optional[pd.DataFrame] = None
    chart_jsons: list[str] = field(default_factory=list)
    audit: Optional[AuditLogger] = None
    error: Optional[str] = None
    blocked: bool = False
    error_type: Optional[str] = None
    evidence: dict = field(default_factory=dict)
    change_summary: str = ""


@dataclass
class SkillResultEnvelope:
    skill_name: str
    result: SkillResult
    precheck: dict = field(default_factory=dict)
    postcheck: dict = field(default_factory=dict)
    evidence: dict = field(default_factory=dict)
    change_summary: str = ""
