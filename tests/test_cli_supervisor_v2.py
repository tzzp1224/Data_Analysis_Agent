from __future__ import annotations

import inspect

import app.main as cli_main


def test_parse_cli_action():
    assert cli_main._parse_cli_action("/approve") == ("approve", "/approve")
    assert cli_main._parse_cli_action("approve") == ("approve", "approve")
    assert cli_main._parse_cli_action("/reject") == ("reject", "/reject")
    assert cli_main._parse_cli_action("/revise 先清洗再对账") == ("revise", "先清洗再对账")
    assert cli_main._parse_cli_action("普通任务") == ("", "普通任务")


def test_cli_source_uses_supervisor_v2_flow():
    source = inspect.getsource(cli_main)
    assert "create_agent_first_workflow" in source
    assert "run_agent_first_workflow" in source
    assert "create_workflow(" not in source
