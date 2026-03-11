# Changelog

All notable changes to the golden dataset are documented in this file.

## [v1.0.1] - 2026-03-11
- Added automated benchmark runner: `golden_dataset/run_evaluation.py`.
- Added evaluator modules with clean boundaries:
  - API client: `golden_dataset/evaluator/http_api.py`
  - XLSX snapshot inspector: `golden_dataset/evaluator/xlsx_inspector.py`
  - Assertion engine: `golden_dataset/evaluator/assertions.py`
- Added case-level regression snapshot config: `golden_dataset/expected_snapshots.json`.
- Upgraded scorecard schema with assertion and chart metrics.
- Added dataset version file `golden_dataset/VERSION` and `runs/` output convention.

## [v1.0.0] - 2026-03-11
- Initial golden Excel dataset with 4 benchmark scenarios and manifest.

## Planned [v1.1.0]
- Add new enterprise scenarios (cross-currency reconciliation, tax invoice alignment, payment channel split-merge).
- Add stricter structured assertions on output workbook content and business KPI checks.
- Introduce baseline comparison command (`--compare-with <run_id>`) for trend reporting.
