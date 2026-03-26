from __future__ import annotations

import pandas as pd

import app.skills.router as router


def test_route_skill_catalog_first_without_legacy_fallback(monkeypatch):
    monkeypatch.setattr(router, "is_catalog_available", lambda: True)
    monkeypatch.setattr(router, "route_worker_from_catalog", lambda *_args, **_kwargs: None)

    dfs_context = {"sales.xlsx": pd.DataFrame({"x": [1]})}
    # "图表" would match legacy fallback, but should not route when catalog is available.
    routed = router.route_skill("请帮我做图表分析", dfs_context)
    assert routed is None


def test_route_skill_uses_legacy_fallback_only_when_catalog_unavailable(monkeypatch):
    monkeypatch.setattr(router, "is_catalog_available", lambda: False)
    monkeypatch.setattr(router, "route_worker_from_catalog", lambda *_args, **_kwargs: None)

    dfs_context = {"sales.xlsx": pd.DataFrame({"x": [1]})}
    routed = router.route_skill("请帮我做图表分析", dfs_context)
    assert routed == "l4_visual"

