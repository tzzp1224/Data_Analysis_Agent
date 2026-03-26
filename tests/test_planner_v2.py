from __future__ import annotations

import pandas as pd

import app.orchestration.planner as planner


def test_build_task_plan_v2_multi_step_fallback(monkeypatch):
    def _raise_llm(*_args, **_kwargs):
        raise RuntimeError("llm unavailable")

    monkeypatch.setattr(planner, "get_llm", _raise_llm)

    dfs_context = {
        "销售台账.xlsx": pd.DataFrame({"客户": ["A"], "金额": [1]}),
        "银行流水.xlsx": pd.DataFrame({"流水号": ["x"], "到账金额": [1]}),
    }
    instruction = "先清洗再合并再对账再可视化"

    plan = planner.build_task_plan_v2(instruction, dfs_context)
    workers = [step.selected_worker for step in plan.steps]

    assert workers == ["l1_hygiene", "l2_merge", "l3_reconcile", "l4_visual"]
    for step in plan.steps:
        assert step.retry_policy["runtime_error_max_retries"] == 2
        assert "missing_required_columns" in step.retry_policy["non_retryable_error_types"]
