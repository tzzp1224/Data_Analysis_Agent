from __future__ import annotations

from typing import Any
import re

from golden_dataset.evaluator.xlsx_inspector import WorkbookSnapshot


def _contains_any(text: str, keywords: list[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def _contains_all(text: str, keywords: list[str]) -> bool:
    return all(keyword in text for keyword in keywords)


def parse_audit_summary(summary: str | None) -> tuple[int | None, int | None]:
    if not summary:
        return None, None
    match = re.search(r"执行\s*(\d+)\s*步操作,\s*剔除\s*(\d+)\s*次", summary)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def evaluate_case_assertions(
    expectations: dict[str, Any],
    observed: dict[str, Any],
) -> tuple[bool, list[str]]:
    failures: list[str] = []

    response_text = observed.get("response_text", "") or ""
    chart_count = int(observed.get("chart_count", 0) or 0)
    output_file_generated = bool(observed.get("output_file_generated"))
    audit_log_present = bool(observed.get("audit_log_present"))
    workbook_snapshot: WorkbookSnapshot | None = observed.get("workbook_snapshot")
    audit_summary = observed.get("audit_summary")
    op_count, ex_count = parse_audit_summary(audit_summary)

    if expectations.get("require_download", False) and not output_file_generated:
        failures.append("Expected download_url but got none.")

    if expectations.get("require_audit", False) and not audit_log_present:
        failures.append("Expected audit_summary but got none.")

    min_chart_count = int(expectations.get("min_chart_count", 0) or 0)
    if chart_count < min_chart_count:
        failures.append(f"Expected at least {min_chart_count} chart(s), got {chart_count}.")

    response_keywords_any = expectations.get("response_keywords_any") or []
    if response_keywords_any and not _contains_any(response_text, response_keywords_any):
        failures.append(f"Response missing any of keywords: {response_keywords_any}")

    response_keywords_all = expectations.get("response_keywords_all") or []
    if response_keywords_all and not _contains_all(response_text, response_keywords_all):
        failures.append(f"Response missing required keywords: {response_keywords_all}")

    audit_expectations = expectations.get("audit_summary") or {}
    min_operations = audit_expectations.get("min_operations")
    min_exclusions = audit_expectations.get("min_exclusions")
    if min_operations is not None:
        if op_count is None or op_count < int(min_operations):
            failures.append(
                f"Expected audit operations >= {min_operations}, got {op_count}."
            )
    if min_exclusions is not None:
        if ex_count is None or ex_count < int(min_exclusions):
            failures.append(
                f"Expected audit exclusions >= {min_exclusions}, got {ex_count}."
            )

    xlsx_expectations = expectations.get("xlsx") or {}
    if xlsx_expectations:
        if workbook_snapshot is None:
            failures.append("Expected downloadable workbook snapshot, got none.")
        else:
            min_sheet_count = xlsx_expectations.get("min_sheet_count")
            if min_sheet_count is not None and workbook_snapshot.sheet_count < int(min_sheet_count):
                failures.append(
                    f"Expected sheet_count >= {min_sheet_count}, got {workbook_snapshot.sheet_count}."
                )

            required_sheet_tokens = xlsx_expectations.get("required_sheet_name_contains") or []
            for token in required_sheet_tokens:
                if workbook_snapshot.find_sheet(token) is None:
                    failures.append(f"Expected a sheet containing token '{token}', but not found.")

            min_data_rows_by_token = xlsx_expectations.get("min_data_rows_by_sheet") or {}
            for token, min_rows in min_data_rows_by_token.items():
                sheet = workbook_snapshot.find_sheet(token)
                if sheet is None:
                    failures.append(f"Sheet token '{token}' not found for row assertion.")
                    continue
                if sheet.data_rows < int(min_rows):
                    failures.append(
                        f"Sheet '{sheet.name}' expected data_rows >= {min_rows}, got {sheet.data_rows}."
                    )

    return len(failures) == 0, failures
