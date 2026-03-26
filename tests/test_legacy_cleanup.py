from __future__ import annotations

import app.orchestration.planner as planner
import app.services.workflow as legacy_workflow


def test_legacy_create_workflow_is_supervisor_v2_shim(monkeypatch):
    sentinel = object()
    observed = {}

    def fake_create_agent_first_workflow(dfs_context, backups_context=None):
        observed["dfs"] = dfs_context
        observed["backups"] = backups_context
        return sentinel

    monkeypatch.setattr(
        legacy_workflow,
        "create_agent_first_workflow",
        fake_create_agent_first_workflow,
    )

    dfs = {"sales.xlsx": "df"}
    backups = {"sales.xlsx": "backup"}
    result = legacy_workflow.create_workflow(dfs, backups)

    assert result is sentinel
    assert observed["dfs"] == dfs
    assert observed["backups"] == backups


def test_legacy_build_task_plan_adapter_removed():
    assert not hasattr(planner, "build_task_plan")
