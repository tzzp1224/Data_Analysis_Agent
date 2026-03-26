# Agentic Finance: Enterprise Data Analyst Agent

[![Language](https://img.shields.io/badge/Lang-简体中文-red.svg)](README_CN.md)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-green.svg)](https://www.python.org/)

**Agentic Finance** is an autonomous data analysis system designed for enterprise financial scenarios. Leveraging **LangGraph** for orchestration and **Google Gemini** for reasoning, it automates the workflow of data ingestion, schema inference, entity alignment, and financial reconciliation.

The system implements a **Supervisor-Worker architecture**, featuring self-healing code execution and comprehensive audit logging.

## System Architecture

The core logic is driven by a state machine that orchestrates interactions between the Supervisor (decision maker) and the Python Worker (executor).

[![](https://mermaid.ink/img/pako:eNp1Ul1v2jAU_SuWnymQlI-Qh0qIsq4Ta1FDV2mhD15ySaIlNvLHVkr477uOoc0k8IPtc3Xuuedee08TkQINaSbZNier2zUnuJ4VyNhupEciLYFVZaHJ8_0rubq6qb-uVsvet-jxoSbT5X38hSmNJ4lA_gH56hRswHIjUKoQvCYvQv7elOJvvGA8u2uqnULHFLcr88t5WdNpBlyTR5nkoLRkGnXW1LHsiswWKxZKyPjzSh6wH2fzSRgN0lXGdpY7nQt-RI52Tuu_1FnOdHwHHCQrG9BKOQpZOjLIDAVrMn-DxGh0ZOEHamWdQq4MKFPq3lxK8eHzAjUySYKjrFtOHRN4emF8KyHKgmdkwXYoS88LkyXjKVOxO8icZwWHC36JVVTxzCgtKgdazAY3tIUXT02KH2YhsgzkeY4f_4DECn9nGh_4Aus6jiomNXmCRPCkKE_WsGvawU9bpDTU0kCHViArZiHdW8qa6hwqWNMQrylsGA7azuCAaVvGfwpRnTKlMFlOww0rFSKzTZmG24LhGD8pWA_kTBiuaThsFGi4p2809Hyv2_f84WA8nviBHwTXHbrD8KA7HkwmQTCaeL4_GAWjQ4e-N0X73WA87LeWd_gHOXceUQ?type=png)](https://mermaid.live/edit#pako:eNp1Ul1v2jAU_SuWnymQlI-Qh0qIsq4Ta1FDV2mhD15ySaIlNvLHVkr477uOoc0k8IPtc3Xuuedee08TkQINaSbZNier2zUnuJ4VyNhupEciLYFVZaHJ8_0rubq6qb-uVsvet-jxoSbT5X38hSmNJ4lA_gH56hRswHIjUKoQvCYvQv7elOJvvGA8u2uqnULHFLcr88t5WdNpBlyTR5nkoLRkGnXW1LHsiswWKxZKyPjzSh6wH2fzSRgN0lXGdpY7nQt-RI52Tuu_1FnOdHwHHCQrG9BKOQpZOjLIDAVrMn-DxGh0ZOEHamWdQq4MKFPq3lxK8eHzAjUySYKjrFtOHRN4emF8KyHKgmdkwXYoS88LkyXjKVOxO8icZwWHC36JVVTxzCgtKgdazAY3tIUXT02KH2YhsgzkeY4f_4DECn9nGh_4Aus6jiomNXmCRPCkKE_WsGvawU9bpDTU0kCHViArZiHdW8qa6hwqWNMQrylsGA7azuCAaVvGfwpRnTKlMFlOww0rFSKzTZmG24LhGD8pWA_kTBiuaThsFGi4p2809Hyv2_f84WA8nviBHwTXHbrD8KA7HkwmQTCaeL4_GAWjQ4e-N0X73WA87LeWd_gHOXceUQ)

## Key Capabilities

The system categorizes capabilities into four levels (L1-L4):

### L1: Intelligent Hygiene

- **Schema Inference:** Automatically detects header rows and sheet names using LLM-based inspection.
- **Data Cleaning:** Identifies and handles duplicates, null values, and outliers.
- **Audit Logging:** Tracks all data modifications (drops, fills, exclusions) in a dedicated audit log for compliance.

### L2: Semantic Entity Alignment

- **Problem:** Resolves inconsistencies in entity names across datasets (e.g., "ByteDance" vs. "字节跳动").
- **Solution:** Hybrid matching approach combining **RapidFuzz** (string similarity) and **Sentence-Transformers** (vector embeddings), validated by an LLM Judge.

### L3: Financial Reconciliation

- **Tolerance Matching:** Supports monetary reconciliation with configurable tolerance thresholds (e.g., ignoring differences < 0.01).
- **Many-to-One Aggregation:** Automatically handles scenarios where multiple system records correspond to a single bank transaction.
- **Status Classification:** Categorizes records into "Matched", "Tolerance Matched", or "Unilateral" (System/Bank only).

### L4: Interactive Visualization

- Generates interactive charts (Plotly) based on natural language queries.
- Provides automated insights and trend analysis alongside visual outputs.

## Stage A Security Hardening (Trusted Execution)

This repo now includes a focused P0 hardening pass on execution and file delivery:

- **Trusted Executor (`app/services/trusted_exec.py`)**
  - Runs generated Python code in a dedicated subprocess with timeout control.
  - Applies AST-based security validation before execution.
  - Blocks dangerous operations (`exec/eval/open/__import__`, OS/process calls, direct file I/O APIs).
  - Uses restricted builtins and a narrow import allowlist (`pandas`, `numpy`, `re`, `plotly`, `warnings`).

- **Prompt Injection Reduction**
  - Replaces raw `df.head().to_string()` prompt context with sanitized structured schema snapshots.
  - Treats dataset content as untrusted input in the worker system prompt.

- **Upload/Download Security**
  - Upload path now enforces filename sanitization, extension allowlist, and size limit.
  - Download endpoint now requires `session_id + token` binding instead of filename-only access.

## P0 Usability Stabilization (Completed)

This repository now includes a focused P0 pass for reliability and engineering hygiene:

- **Decoupled Export Service**
  - Export logic moved from API layer to `app/services/exporter.py`.
  - CLI and FastAPI now share the same export implementation, avoiding duplicated/fragile paths.

- **CSV-Compatible Ingestion**
  - Ingestion now branches by file type (`excel` / `csv`) instead of assuming Excel only.
  - CSV mode now supports delimiter sniffing, encoding fallback, and automatic header row detection (LLM-first + heuristic fallback).

- **Workflow Prompt/Tool Consistency**
  - Removed reference to unavailable `vector_match` in worker prompt.
  - Worker guidance now aligns with available tools (`smart_merge`, `smart_reconcile`).

- **Audit Persistence Fix**
  - Audit context is now persisted even when no `result_df` is explicitly produced.
  - Exported reports consistently include audit trail when operations occurred.

- **Session Runtime Hygiene**
  - Added in-memory session TTL cleanup (default 4 hours).
  - Expired sessions now clean associated upload/output temp files.

## P0.5 Runtime Reliability (Completed)

- **macOS Process Safety**
  - Trusted executor now defaults to multiprocessing `spawn` on macOS to avoid `fork` crashes with torch/MPS runtime.

- **Configurable Execution Timeout**
  - Trusted execution timeout is configurable via `TRUSTED_EXEC_TIMEOUT_SECONDS` (default `30`).
  - This reduces false execution failures for heavier reconciliation/merge code paths.

- **Failure Observability**
  - When execution never succeeds, API now returns a concise `❌ Runtime Error` summary in `response_text`.
  - Evaluator can now capture root-cause failure directly instead of only downstream assertion failures.

## P1 Skillization (In Progress)

- Added lightweight deterministic skill routes for:
  - `L1` hygiene
  - `L2` merge-only worker (typically composed as `L1 -> L2`)
  - `L3` reconciliation
  - `L4` trend visualization
- Introduced `app/skills/engine.py` as the single dispatch boundary to keep API orchestration and skill logic decoupled.
- Agent-first path now runs by default; deterministic skills are used as repair/fallback nodes.
- Added semantic layer for tabular robustness:
  - `app/services/semantic_taxonomy.py`: extensible column/row type taxonomy.
  - `app/services/semantic_profile.py`: column name + value-distribution profiling.
  - `app/services/semantic_infer.py`: LLM-first semantic inference with heuristic fallback.
- Added shared semantic contract cache:
  - `app/services/semantic_contract.py` builds/reuses one semantic contract per request.
  - Skills and workflow now consume the same semantic contract, avoiding duplicated inference.
- Fallback policy is explicit:
  - If semantic inference falls back to heuristics, audit logs include warning and cleaning strategy becomes conservative.
- Cleaning policy is explicit and user-steerable:
  - Default `conservative` mode prioritizes data preservation (warn-first).
  - `strict` mode can be triggered by user instruction for aggressive anomaly removal.
- Required-column guardrails:
  - Before `L2`/`L3`/`L4`, system checks critical semantic columns and blocks execution when missing.
  - Blocked responses include current columns + semantic evidence to guide user correction.
- L2 merge safety policy:
  - For merge tasks, system proposes join keys first (LLM + key quality checks).
  - If no reliable key is found, merge is blocked with actionable guidance instead of forced execution.
  - If key type is `entity_name`, alias alignment uses LLM-assisted matching; otherwise deterministic merge is used.
- Added Supervisor v2 multi-step graph:
  - `POST /chat` now runs a single LangGraph state machine:
    `supervisor_plan -> supervisor_dispatch -> worker_execute -> supervisor_review -> finalize`.
  - Supervisor first builds a full step plan, then routes workers step-by-step in order.
  - Each worker completion returns to supervisor before dispatching the next step.
  - `ChatRequest` now supports optional:
    `human_action=approve|reject|revise`, `resume_plan_id=<plan_id>`.
  - `ChatResponse` is execution-oriented:
    top-level `status/message/next_action`,
    `execution={plan_id,current_step_idx,plan_steps,step_results,route_trace}`,
    `artifacts={download_url,chart_jsons,audit_summary}`.
  - Official supervisor API availability is still enforced via `SUPERVISOR_REQUIRE_OFFICIAL` and exposed in `/health`.

## Current Execution Chain (Supervisor v2, 2026-03)

1. **Upload stage (`POST /upload`)**
   - Files are validated/sanitized, then loaded through `load_file` (`propose_ingestion_config` + `apply_ingestion`).
   - Session context/backups are refreshed and the agent-first graph is (re)compiled.

2. **Request stage (`POST /chat`)**
   - Server prebuilds semantic contract for current context.
   - Supervisor generates a global task plan (`plan_steps`) before execution.
   - Each step is dispatched to one worker and reviewed before moving to the next step.
   - Runtime now uses a single Supervisor v2 chain; legacy `orchestrator/build_task_plan` adapters were removed.

3. **Failure policy + HITL**
   - `runtime_error`: automatic retry (max 2 per step).
   - `missing_required_columns` / `merge_key_invalid` / `table_selection_failed`: no retry, direct HITL.
   - `approve` resumes from blocked step; `revise` rebuilds plan with new instruction; `reject` terminates execution.

4. **Delivery stage**
   - Only `status=done` requests produce downloadable outputs.
   - Export remains unified via `save_full_context_excel`, with `session_id + token` download binding.

5. **HITL continuation**
   - When `status=awaiting_human`, client can continue with `human_action=approve|reject|revise` and optional `resume_plan_id`.
   - Streamlit client supports shortcut commands: `/approve`, `/reject`, `/revise <new instruction>`.

## Installation

### Prerequisites

- Python 3.9+
- Google Gemini API Key

### Setup

1. **Clone the repository**

   Bash

   ```
   git clone [https://github.com/your-username/agentic-finance.git](https://github.com/your-username/agentic-finance.git)
   cd agentic-finance
   ```

2. **Install dependencies**

   Bash

   ```
   pip install -r requirements.txt
   ```

3. **Configure Environment** Create a `.env` file in the root directory:

   Bash

   ```
   GOOGLE_API_KEY=your_api_key_here
   # Optional runtime flags
   AGENT_FIRST_ENABLED=true
   # If true (default), startup fails when official supervisor API is unavailable
   SUPERVISOR_REQUIRE_OFFICIAL=true
   ```

## Official Supervisor Setup (Step-by-Step)

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Verify official supervisor import:

   ```bash
   python - <<'PY'
backend = None
try:
    from langgraph.prebuilt import create_supervisor  # noqa: F401
    backend = "langgraph.prebuilt.create_supervisor"
except Exception:
    pass
if backend is None:
    try:
        from langgraph_supervisor import create_supervisor  # noqa: F401
        backend = "langgraph_supervisor.create_supervisor"
    except Exception:
        pass
print(backend or "unavailable")
PY
   ```

   Expected output:
   - `langgraph_supervisor.create_supervisor`, or
   - `langgraph.prebuilt.create_supervisor`

3. Enable hard gate in `.env`:

   ```bash
   SUPERVISOR_REQUIRE_OFFICIAL=true
   ```

4. Start backend and check health:

   ```bash
   uvicorn app.server:app --reload --host 0.0.0.0 --port 8000
   curl http://localhost:8000/health
   ```

   Confirm:
   - `official_supervisor_ready=true`
   - `supervisor_backend` is not `unavailable`

## Usage

The system requires both the backend API and frontend UI to be running.

**1. Start the Backend (FastAPI)**

Bash

```
uvicorn app.server:app --reload --host 0.0.0.0 --port 8000
```

**2. Start the Frontend (Streamlit)**

Bash

```
streamlit run app/ui.py
```

Access the web interface at `http://localhost:8501`.

## Golden Dataset

For regression and optimization benchmarking, a reusable golden dataset is included:

- Folder: `golden_dataset/`
- Manifest: `golden_dataset/manifest.json`
- Expected snapshots: `golden_dataset/expected_snapshots.json`
- Cases: `golden_dataset/cases/` (cleaning/merge, reconciliation, ingestion, visualization)
- Scorecard template: `golden_dataset/scorecard_template.csv`
- Changelog: `golden_dataset/CHANGELOG.md`

Regenerate all Excel files deterministically with:

```bash
python golden_dataset/build_golden_dataset.py
```

Run automated benchmark (batch by manifest + snapshot assertions):

```bash
python golden_dataset/run_evaluation.py --api-url http://localhost:8000
```

The evaluator now performs a preflight check on `GET /health` before running cases.
If `GOOGLE_API_KEY` is missing in the backend environment, evaluation exits early with a clear error.

### Troubleshooting

- `python: can't open file .../golden_dataset/run_evaluation.py`:
  Run command from project root: `/Users/dexter/Documents/Dexter_Work/Data_Analysis_Agent`.
- All cases fail with `latency=0.00s`:
  Backend API likely unreachable. Confirm `uvicorn app.server:app --reload --port 8000` is running.
- Preflight reports `LLM key is not ready`:
  Set `GOOGLE_API_KEY` in the same shell/session where `uvicorn` is started.

## Roadmap (Single Source of Truth)

- **Current stage: P2.5 (Completed / Baseline)**
  - API and CLI now share the same Supervisor v2 orchestration chain.
  - Old workflow graph logic is retired into a minimal legacy shim (`app/services/workflow.py`).
  - `agent_worker` now runs a lightweight ReAct loop (`max_attempts=2`) with context packets and attempt-level observability.
  - Context engineering is standardized via `ContextPacket` (`system_invariants/plan_slice/schema_digest/memory_slice/error_feedback`).
  - Skills remain declarative (`SKILL.md`) and deterministic; no external plugin execution or MCP introduced.
- **Top priority: P2.6**
  - Expand deterministic reconciliation templates (many-to-one aggregation, tolerance policy, layered diff attribution).
  - Improve production observability dashboards (step latency, retry profile, blocker taxonomy).
- **P3**
  - Add production-grade persistence (Redis + SQL/Object Storage) and authn/authz controls.
  - Build evaluation and observability layer (success rate, latency, error-type profile).

## License

This project is licensed under the MIT License.
