# Changelog

All notable changes to the golden dataset are documented in this file.

## [v1.1.0] - 2026-03-26
- Added multi-step supervisor regression case:
  - New case `MULTI_STEP_SUPERVISOR_CHAIN` validates sequential routing (`clean -> merge -> reconcile -> visual`).
  - Added corresponding expectations in `expected_snapshots.json`.
- Updated dataset version files to `v1.1.0`.
- Goal: verify supervisor-worker ordered execution and HITL-continuation readiness.

## [v1.0.9] - 2026-03-13
- Introduced explicit cleaning-policy layer in semantic L1 pipeline:
  - Default mode is `conservative` to reduce accidental data loss on unknown real-world files.
  - `strict` mode is supported via user instruction keywords for aggressive anomaly removal.
- Unified destructive-action gating:
  - Negative amount/quantity removal now depends on semantic confidence + policy, not hardcoded behavior.
  - Summary/metadata row removal also depends on policy, with audit trace for every exclusion/warning.
- Goal: optimize for objective data safety and explainability rather than benchmark-specific tuning.

## [v1.0.8] - 2026-03-13
- Fixed ingestion-case over-cleaning mismatch:
  - Negative values in quantity-like columns (`数量/qty/count`) are now treated as warnings (kept in table), not hard deletions.
  - Negative values in amount/price-like columns remain hard exclusions.
- Goal: preserve potentially business-valid reverse-flow rows (returns/adjustments) while keeping financial-risk fields strict.

## [v1.0.7] - 2026-03-13
- Refined deterministic skill architecture:
  - Added `app/skills/engine.py` as single dispatch entry to keep API layer thin and decoupled.
  - Split L1 and L1+L2 into dedicated skill functions instead of runtime flag switching.
- Fixed over-cleaning behavior in deterministic L1 hygiene:
  - Logic mismatch (`单价 * 数量 != 总额`) now records audit warning instead of direct row deletion.
  - Prevents false data loss on ingestion benchmark while preserving anomaly observability.

## [v1.0.6] - 2026-03-11
- Added minimal deterministic L1/L2 skill path to avoid long-running free-form generation on cleaning/merge benchmark prompts.
- Skill router now supports:
  - `l1_hygiene` for single-table cleaning intents.
  - `l1_l2_hygiene_merge` for cleaning + master-data alignment intents.
- This keeps workflow fallback intact while reducing evaluation timeout risk on high-structure cases.

## [v1.0.5] - 2026-03-11
- P0.6 minimal reliability and consistency updates:
  - Improved backend runtime-error extraction to return the concrete exception line instead of generic traceback header.
  - Kept backups out of business `dfs_context` and wired reload through separate backup context to prevent accidental processing of backup tables.
- P1-A initial deterministic skill path:
  - Added lightweight skill router (`l3_reconcile`) and deterministic L3 reconciliation skill execution path.
  - Integrated skill-first execution in API chat endpoint with workflow fallback for non-matching cases.

## [v1.0.4] - 2026-03-11
- P0.5 reliability fixes in runtime pipeline:
  - On macOS, trusted executor now defaults to multiprocessing `spawn` to avoid `fork` crashes with torch/MPS stack.
  - Trusted execution timeout is now configurable via `TRUSTED_EXEC_TIMEOUT_SECONDS` (default: 30s), reducing false timeouts on complex tasks.
  - Added runtime-error summary propagation in API response when execution never succeeds, so evaluator can record the true root cause.
  - `smart_merge` now lazily loads vector model only when vector recall is actually needed.

## [v1.0.3] - 2026-03-11
- Added evaluator preflight check (`GET /health`) to fail fast when API is unreachable or `GOOGLE_API_KEY` is not ready.
- Added backend error extraction in benchmark runner, so scorecard/console shows real root cause instead of generic FAIL.
- Added backend `/health` endpoint (`status`, `llm_ready`, `model`, `active_sessions`) for CI and local diagnostics.
- Tightened workflow completion routing: successful execution now ends directly to avoid unnecessary loops and long latency.
- Added `temp_uploads/` and `temp_outputs/` to root `.gitignore` to keep runtime artifacts out of commits.

## [v1.0.2] - 2026-03-11
- Fixed sandbox-policy mismatch that caused repeated retries:
  - Removed ambiguous blocked attribute calls (`replace`, `rename`, `remove` family) from `trusted_exec`.
- Added retry guard in workflow executor router (`MAX_EXEC_RETRIES=3`) to avoid recursion-limit loops.
- Fixed export trigger logic to only export business tables / result / audit, and added exporter fallback sheet.
- Preserved original uploaded filename as logical table key (storage path remains sanitized) for assertion consistency.
- Improved benchmark CLI observability: print first failure reason inline for each failed case.
- Relaxed brittle text-keyword snapshot assertions to reduce non-deterministic false negatives.

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

## Planned [v1.2.0]
- Add new enterprise scenarios (cross-currency reconciliation, tax invoice alignment, payment channel split-merge).
- Add stricter structured assertions on output workbook content and business KPI checks.
- Introduce baseline comparison command (`--compare-with <run_id>`) for trend reporting.
