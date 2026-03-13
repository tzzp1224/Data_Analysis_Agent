#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json
import re
import subprocess
import time
import uuid
import sys


if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from golden_dataset.evaluator.assertions import evaluate_case_assertions
from golden_dataset.evaluator.http_api import AgentApiClient, ApiClientError
from golden_dataset.evaluator.xlsx_inspector import inspect_xlsx_bytes


ROOT = Path(__file__).resolve().parent


@dataclass
class CaseRunRow:
    run_id: str
    run_date: str
    branch_or_tag: str
    model: str
    dataset_version: str
    case_id: str
    success: int
    retry_count: int
    latency_seconds: float
    output_file_generated: int
    audit_log_present: int
    chart_count: int
    assertion_passed: int
    assertion_failed_count: int
    notes: str

    def to_csv_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_date": self.run_date,
            "branch_or_tag": self.branch_or_tag,
            "model": self.model,
            "dataset_version": self.dataset_version,
            "case_id": self.case_id,
            "success": self.success,
            "retry_count": self.retry_count,
            "latency_seconds": f"{self.latency_seconds:.3f}",
            "output_file_generated": self.output_file_generated,
            "audit_log_present": self.audit_log_present,
            "chart_count": self.chart_count,
            "assertion_passed": self.assertion_passed,
            "assertion_failed_count": self.assertion_failed_count,
            "notes": self.notes,
        }


CSV_FIELDS = [
    "run_id",
    "run_date",
    "branch_or_tag",
    "model",
    "dataset_version",
    "case_id",
    "success",
    "retry_count",
    "latency_seconds",
    "output_file_generated",
    "audit_log_present",
    "chart_count",
    "assertion_passed",
    "assertion_failed_count",
    "notes",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def default_run_id() -> str:
    return utc_now().strftime("run-%Y%m%d-%H%M%S")


def infer_branch_or_tag(project_root: Path) -> str:
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=project_root,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        branch = output.strip()
        return branch or "unknown"
    except Exception:
        return "unknown"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_dataset_version(default: str = "v0.0.0") -> str:
    version_path = ROOT / "VERSION"
    if not version_path.exists():
        return default
    return version_path.read_text(encoding="utf-8").strip() or default


def merge_expectations(
    default_expectations: dict[str, Any],
    case_expectations: dict[str, Any] | None,
) -> dict[str, Any]:
    merged: dict[str, Any] = dict(default_expectations or {})
    if not case_expectations:
        return merged

    for key, value in case_expectations.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            inner = dict(merged[key])
            inner.update(value)
            merged[key] = inner
        else:
            merged[key] = value
    return merged


def extract_backend_error(response_text: str) -> str | None:
    if not response_text:
        return None

    system_error_match = re.search(r"系统异常:\s*(.+)", response_text)
    if system_error_match:
        return system_error_match.group(1).strip()

    runtime_error_match = re.search(r"❌ Runtime Error:\s*(.+)", response_text, re.DOTALL)
    if runtime_error_match:
        lines = [line.strip() for line in runtime_error_match.group(1).strip().splitlines() if line.strip()]
        for line in reversed(lines):
            if line.startswith("Traceback"):
                continue
            if line.startswith("File "):
                continue
            return line[:300]
        if lines:
            return lines[-1][:300]

    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run batch regression benchmark from manifest.")
    parser.add_argument("--api-url", default="http://localhost:8000", help="Backend API base URL.")
    parser.add_argument(
        "--manifest",
        default=str(ROOT / "manifest.json"),
        help="Path to manifest.json",
    )
    parser.add_argument(
        "--expected",
        default=str(ROOT / "expected_snapshots.json"),
        help="Path to expected_snapshots.json",
    )
    parser.add_argument("--run-id", default=default_run_id(), help="Run ID for scorecard.")
    parser.add_argument("--branch-or-tag", default="", help="Git branch or release tag label.")
    parser.add_argument(
        "--model",
        default="gemini-3-flash-preview",
        help="Model label to write into scorecard.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=300,
        help="HTTP timeout for each request.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and generate placeholder scorecard without API calls.",
    )
    parser.add_argument(
        "--output-csv",
        default="",
        help="Output scorecard CSV path. Default: golden_dataset/runs/scorecard_<run_id>.csv",
    )
    parser.add_argument(
        "--output-summary",
        default="",
        help="Output summary JSON path. Default: golden_dataset/runs/summary_<run_id>.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = ROOT.parent
    manifest_path = Path(args.manifest).resolve()
    expected_path = Path(args.expected).resolve()

    manifest = load_json(manifest_path)
    expected = load_json(expected_path)

    run_id = args.run_id
    run_date = utc_now().strftime("%Y-%m-%d")
    branch_or_tag = args.branch_or_tag or infer_branch_or_tag(project_root)
    dataset_version = manifest.get("version") or load_dataset_version()
    model = args.model

    runs_dir = ROOT / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    output_csv = Path(args.output_csv).resolve() if args.output_csv else runs_dir / f"scorecard_{run_id}.csv"
    output_summary = (
        Path(args.output_summary).resolve() if args.output_summary else runs_dir / f"summary_{run_id}.json"
    )

    cases = manifest.get("cases", [])
    expected_cases: dict[str, Any] = expected.get("cases", {})
    default_expectations: dict[str, Any] = expected.get("default_expectations", {})

    rows: list[CaseRunRow] = []
    summary_rows: list[dict[str, Any]] = []

    if args.dry_run:
        print("ℹ️ Dry-run mode: API calls are skipped.")

    client = AgentApiClient(args.api_url, timeout_seconds=args.timeout_seconds)
    if not args.dry_run:
        try:
            health = client.healthcheck()
        except ApiClientError as exc:
            raise SystemExit(f"Preflight failed: cannot reach API /health. {exc}") from exc

        if health.get("status") != "ok":
            raise SystemExit(f"Preflight failed: unexpected health status: {health}")
        if not bool(health.get("llm_ready")):
            model = health.get("model", "unknown")
            raise SystemExit(
                f"Preflight failed: API is up but LLM key is not ready "
                f"(model={model}). Please check GOOGLE_API_KEY in server environment."
            )
        print(
            "✅ Preflight passed: "
            f"api={args.api_url} model={health.get('model', 'unknown')} "
            f"active_sessions={health.get('active_sessions', 'n/a')}"
        )

    for case in cases:
        case_id = case["case_id"]
        prompt = case["recommended_prompt"]
        case_files = [ROOT / rel_path for rel_path in case["files"]]
        case_expect = merge_expectations(default_expectations, expected_cases.get(case_id))

        if args.dry_run:
            row = CaseRunRow(
                run_id=run_id,
                run_date=run_date,
                branch_or_tag=branch_or_tag,
                model=model,
                dataset_version=dataset_version,
                case_id=case_id,
                success=0,
                retry_count=0,
                latency_seconds=0.0,
                output_file_generated=0,
                audit_log_present=0,
                chart_count=0,
                assertion_passed=0,
                assertion_failed_count=0,
                notes="dry-run: API execution skipped",
            )
            rows.append(row)
            summary_rows.append({"case_id": case_id, "status": "dry-run"})
            continue

        session_id = uuid.uuid4().hex
        case_notes: list[str] = []
        retry_count = 0
        latency = 0.0
        output_generated = False
        audit_present = False
        chart_count = 0
        assertion_passed = False
        assertion_failures: list[str] = []

        try:
            for file_path in case_files:
                if not file_path.exists():
                    raise FileNotFoundError(f"Case file not found: {file_path}")

            client.upload_files(session_id=session_id, file_paths=case_files)

            started = time.perf_counter()
            chat_result = client.chat(session_id=session_id, prompt=prompt)
            latency = time.perf_counter() - started

            retry_count = chat_result.response_text.count("自愈")
            chart_count = len(chat_result.chart_jsons)
            output_generated = bool(chat_result.download_url)
            audit_present = bool(chat_result.audit_summary)

            workbook_snapshot = None
            if chat_result.download_url:
                workbook_bytes = client.download_file(chat_result.download_url)
                workbook_snapshot = inspect_xlsx_bytes(workbook_bytes)

            observed = {
                "response_text": chat_result.response_text,
                "chart_count": chart_count,
                "output_file_generated": output_generated,
                "audit_log_present": audit_present,
                "audit_summary": chat_result.audit_summary,
                "workbook_snapshot": workbook_snapshot,
            }
            assertion_passed, assertion_failures = evaluate_case_assertions(
                expectations=case_expect,
                observed=observed,
            )
            backend_error = extract_backend_error(chat_result.response_text)
            if backend_error:
                assertion_failures.insert(0, f"Backend error: {backend_error}")
                assertion_passed = False
            case_notes.extend(assertion_failures)

            summary_rows.append(
                {
                    "case_id": case_id,
                    "status": "passed" if assertion_passed else "failed",
                    "latency_seconds": round(latency, 3),
                    "assertion_failures": assertion_failures,
                    "chart_count": chart_count,
                    "retry_count": retry_count,
                }
            )
        except (ApiClientError, FileNotFoundError, RuntimeError, ValueError) as exc:
            case_notes.append(str(exc))
            summary_rows.append(
                {
                    "case_id": case_id,
                    "status": "error",
                    "error": str(exc),
                }
            )
            assertion_passed = False
        except Exception as exc:  # pragma: no cover - defensive guard
            case_notes.append(f"Unhandled exception: {exc}")
            summary_rows.append(
                {
                    "case_id": case_id,
                    "status": "error",
                    "error": f"Unhandled exception: {exc}",
                }
            )
            assertion_passed = False

        row = CaseRunRow(
            run_id=run_id,
            run_date=run_date,
            branch_or_tag=branch_or_tag,
            model=model,
            dataset_version=dataset_version,
            case_id=case_id,
            success=1 if assertion_passed else 0,
            retry_count=retry_count,
            latency_seconds=latency,
            output_file_generated=1 if output_generated else 0,
            audit_log_present=1 if audit_present else 0,
            chart_count=chart_count,
            assertion_passed=1 if assertion_passed else 0,
            assertion_failed_count=len(assertion_failures),
            notes=" | ".join(case_notes),
        )
        rows.append(row)

        status_text = "PASS" if assertion_passed else "FAIL"
        if assertion_passed:
            print(f"[{status_text}] {case_id} latency={latency:.2f}s retries={retry_count} charts={chart_count}")
        else:
            first_reason = case_notes[0] if case_notes else "Unknown failure"
            print(
                f"[{status_text}] {case_id} latency={latency:.2f}s retries={retry_count} "
                f"charts={chart_count} reason={first_reason}"
            )

    with output_csv.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_dict())

    latest_csv = ROOT / "scorecard_latest.csv"
    latest_csv.write_text(output_csv.read_text(encoding="utf-8"), encoding="utf-8")

    total_cases = len(rows)
    pass_count = sum(row.success for row in rows)
    pass_rate = (pass_count / total_cases) if total_cases else 0.0
    summary = {
        "run_id": run_id,
        "run_date_utc": utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset_version": dataset_version,
        "manifest_path": str(manifest_path),
        "expected_path": str(expected_path),
        "api_url": args.api_url,
        "total_cases": total_cases,
        "pass_count": pass_count,
        "pass_rate": round(pass_rate, 4),
        "dry_run": bool(args.dry_run),
        "cases": summary_rows,
        "scorecard_csv": str(output_csv),
    }
    output_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"\nScorecard written: {output_csv}")
    print(f"Summary written:   {output_summary}")
    print(f"Pass rate: {pass_count}/{total_cases} = {pass_rate:.2%}")


if __name__ == "__main__":
    main()
