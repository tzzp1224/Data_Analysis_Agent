from __future__ import annotations

import pandas as pd

from app.services.data_readiness import assess_data_readiness


def test_data_readiness_ready_for_structured_table():
    dfs = {"sales.xlsx": pd.DataFrame({"日期": ["2026-01-01", "2026-01-02"], "金额": [10, 20], "客户": ["A", "B"]})}
    report = assess_data_readiness(dfs).to_dict()

    assert report["status"] in {"ready", "recoverable"}
    assert report["score"] > 0.35


def test_data_readiness_blocked_for_empty_context():
    report = assess_data_readiness({}).to_dict()

    assert report["status"] == "blocked"
    assert report["score"] == 0.0
