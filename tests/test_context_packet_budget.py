from __future__ import annotations

import pandas as pd

import app.orchestration.agent_worker_runtime as runtime


def test_context_packet_budget_and_stability(monkeypatch):
    monkeypatch.setattr(runtime, "get_semantic_contract", lambda _dfs: {})

    wide_df = pd.DataFrame(
        {
            f"col_{idx:02d}": [f"value_{idx}", f"value_{idx + 1}", f"value_{idx + 2}"]
            for idx in range(40)
        }
    )
    dfs_context = {
        "table_a.xlsx": wide_df,
        "table_b.xlsx": wide_df.copy(),
        "table_c.xlsx": wide_df.copy(),
        "table_d.xlsx": wide_df.copy(),
        "table_e.xlsx": wide_df.copy(),
    }

    state = {
        "plan_id": "plan_budget",
        "current_step_idx": 2,
        "plan_steps": [
            {"step_id": "step_01", "goal": "清洗", "selected_worker": "l1_hygiene", "status": "completed"},
            {"step_id": "step_02", "goal": "合并", "selected_worker": "l2_merge", "status": "completed"},
            {"step_id": "step_03", "goal": "对账" * 120, "selected_worker": "agent_worker", "status": "running"},
        ],
        "step_results": [
            {
                "step_id": "step_01",
                "worker": "l1_hygiene",
                "handled": True,
                "error_type": "",
                "summary": "ok",
            },
            {
                "step_id": "step_02",
                "worker": "l2_merge",
                "handled": True,
                "error_type": "",
                "summary": "ok",
            },
        ],
    }

    long_error = "runtime error: " + ("x" * 5000)
    packet_a = runtime.build_context_packet(state, dfs_context, attempt=2, error_feedback=long_error)
    packet_b = runtime.build_context_packet(state, dfs_context, attempt=2, error_feedback=long_error)

    assert packet_a.to_dict() == packet_b.to_dict()
    assert packet_a.fingerprint() == packet_b.fingerprint()

    assert len(packet_a.schema_digest) == runtime.MAX_CONTEXT_TABLES
    for table_payload in packet_a.schema_digest.values():
        assert len(table_payload.get("columns", [])) <= runtime.MAX_CONTEXT_COLUMNS
        assert len(table_payload.get("sample_rows", [])) <= runtime.MAX_CONTEXT_SAMPLE_ROWS

    assert len(packet_a.error_feedback) <= runtime.MAX_ERROR_CHARS
    assert packet_a.plan_slice["current_step"]["goal"].endswith("...")
