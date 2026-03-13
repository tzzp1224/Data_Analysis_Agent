from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from app.utils.tools import AuditLogger


@dataclass
class SkillResult:
    handled: bool
    response_text: str = ""
    result_df: Optional[pd.DataFrame] = None
    chart_jsons: list[str] = field(default_factory=list)
    audit: Optional[AuditLogger] = None
    error: Optional[str] = None

