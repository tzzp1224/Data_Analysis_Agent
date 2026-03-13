import os
from typing import Dict, Optional

import pandas as pd

from app.utils.tools import AuditLogger


def _unique_sheet_name(base_name: str, existing_names: set[str], max_len: int = 31) -> str:
    normalized = (base_name or "sheet").strip()[:max_len]
    if not normalized:
        normalized = "sheet"
    if normalized not in existing_names:
        return normalized

    counter = 1
    while True:
        suffix = f"_{counter}"
        candidate = f"{normalized[: max_len - len(suffix)]}{suffix}"
        if candidate not in existing_names:
            return candidate
        counter += 1


def save_full_context_excel(
    result_df: Optional[pd.DataFrame],
    dfs_context: Dict[str, pd.DataFrame],
    audit: Optional[AuditLogger],
    output_path: str,
) -> None:
    """
    Save all current tables plus audit trail into one Excel workbook.
    """
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        saved_sheets: set[str] = set()

        for name, df in (dfs_context or {}).items():
            if name.startswith("__"):
                continue
            sheet_base = os.path.splitext(name)[0][:30]
            sheet_name = _unique_sheet_name(sheet_base, saved_sheets)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            saved_sheets.add(sheet_name)

        # Keep compatibility: when caller only has an aggregated result_df,
        # persist it as a separate sheet if no data table sheet exists.
        if result_df is not None and not saved_sheets:
            result_sheet = _unique_sheet_name("analysis_result", saved_sheets)
            result_df.to_excel(writer, sheet_name=result_sheet, index=False)
            saved_sheets.add(result_sheet)

        if audit:
            log_df = audit.get_log_df()
            if not log_df.empty:
                log_sheet = _unique_sheet_name("处理日志(Audit)", saved_sheets)
                log_df.to_excel(writer, sheet_name=log_sheet, index=False)
                saved_sheets.add(log_sheet)

            for name, excluded_df in audit.excluded_data.items():
                excluded_base = f"剔除_{os.path.splitext(name)[0][:10]}"
                excluded_sheet = _unique_sheet_name(excluded_base, saved_sheets)
                excluded_df.to_excel(writer, sheet_name=excluded_sheet, index=False)
                saved_sheets.add(excluded_sheet)

        # Defensive fallback: ensure workbook always has at least one visible sheet.
        if not saved_sheets:
            fallback_sheet = _unique_sheet_name("meta", saved_sheets)
            pd.DataFrame(
                {"message": ["No exportable business tables or audit records produced."]}
            ).to_excel(writer, sheet_name=fallback_sheet, index=False)
            saved_sheets.add(fallback_sheet)


def save_result_with_audit(
    result_df: Optional[pd.DataFrame],
    audit: Optional[AuditLogger],
    output_path: str,
) -> None:
    """
    Backward-compatible helper for legacy call sites.
    """
    context: Dict[str, pd.DataFrame] = {}
    if result_df is not None:
        context["analysis_result.xlsx"] = result_df
    save_full_context_excel(result_df, context, audit, output_path)
