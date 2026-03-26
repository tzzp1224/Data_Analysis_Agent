from __future__ import annotations

import pandas as pd

import app.orchestration.planner as planner
from app.skills.engine import execute_skill


def test_planner_fallback_routes_to_expert_when_intent_unknown(monkeypatch):
    def _raise_llm(*_args, **_kwargs):
        raise RuntimeError("llm unavailable")

    monkeypatch.setattr(planner, "get_llm", _raise_llm)

    dfs_context = {"sales.xlsx": pd.DataFrame({"字段A": ["x"], "字段B": [1]})}
    plan = planner.build_task_plan_v2("请按复杂业务规则做行列函数计算", dfs_context)

    assert len(plan.steps) == 1
    step = plan.steps[0]
    assert step.selected_worker == "expert_excel"
    assert step.fallback_worker == "agent_worker"


def test_execute_skill_returns_envelope_for_expert():
    dfs_context = {"sales.xlsx": pd.DataFrame({"金额": [10, 20], "区域": ["华东", "华南"]})}
    envelope = execute_skill("expert_excel", dfs_context, "请计算金额总计")

    assert envelope is not None
    assert envelope.skill_name == "expert_excel"
    assert envelope.result.handled is True
    assert envelope.postcheck["ok"] in {True, False}
