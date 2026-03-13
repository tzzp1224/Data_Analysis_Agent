from __future__ import annotations

import re
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz, process

from app.services.semantic_infer import ColumnSemantic, RowSemantic, infer_dataframe_semantics
from app.skills.contracts import SkillResult
from app.utils.tools import AuditLogger


NUMERIC_COL_TOKENS = ("金额", "单价", "数量", "总额", "应收", "到账", "price", "qty", "amount", "total")
HARD_NEGATIVE_DROP_TOKENS = ("金额", "单价", "总额", "应收", "到账", "price", "amount", "total")
SOFT_NEGATIVE_WARN_TOKENS = ("数量", "qty", "count")
SALES_NAME_TOKENS = ("客户名称", "客户", "company", "client")
MASTER_NAME_TOKENS = ("标准公司名", "公司名称", "客户主数据", "master")


def _find_column(columns: Iterable[str], tokens: Iterable[str]) -> Optional[str]:
    text_cols = [str(col) for col in columns]
    for token in tokens:
        for col in text_cols:
            if token.lower() in col.lower():
                return col
    return None


def _looks_numeric_column(col_name: str, series: pd.Series) -> bool:
    if any(token.lower() in str(col_name).lower() for token in NUMERIC_COL_TOKENS):
        return True
    sample = series.dropna().astype(str).head(5).tolist()
    if not sample:
        return False
    digit_like = sum(bool(re.search(r"\d", val)) for val in sample)
    return digit_like >= max(1, len(sample) // 2)


def _coerce_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("¥", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _find_semantic(column_semantics: Dict[str, ColumnSemantic], col_name: str) -> Optional[ColumnSemantic]:
    if col_name in column_semantics:
        return column_semantics[col_name]
    for key, sem in column_semantics.items():
        if str(key) == str(col_name):
            return sem
    return None


def _apply_row_type_plan(
    name: str,
    work_df: pd.DataFrame,
    row_semantics: Dict[int, RowSemantic],
    audit: AuditLogger,
) -> pd.DataFrame:
    removable = {"summary_row", "metadata_row", "empty_row"}
    drop_indices = [
        idx
        for idx, sem in row_semantics.items()
        if sem.label in removable and sem.confidence >= 0.9 and idx in work_df.index
    ]
    if drop_indices:
        excluded = work_df.loc[drop_indices].copy()
        audit.log_exclusion(
            f"行语义剔除-{name}",
            "识别为高置信 summary/metadata/empty 行",
            excluded,
        )
        work_df = work_df.drop(index=drop_indices)
    return work_df


def _apply_hygiene_to_table(
    name: str,
    df: pd.DataFrame,
    audit: AuditLogger,
    column_semantics: Dict[str, ColumnSemantic],
    row_semantics: Dict[int, RowSemantic],
) -> pd.DataFrame:
    work_df = df.copy()
    audit.info("L1体检", f"{name} 开始体检", affected_rows=0)
    work_df = _apply_row_type_plan(name, work_df, row_semantics, audit)

    if work_df.duplicated().any():
        dupes = work_df[work_df.duplicated()]
        audit.log_exclusion(f"重复剔除-{name}", "完全重复行", dupes)
        work_df = work_df.drop_duplicates()

    for col in work_df.columns:
        sem = _find_semantic(column_semantics, str(col))
        col_series = work_df[col]
        should_numeric = False
        if sem and sem.label in {"amount", "quantity"} and sem.confidence >= 0.55:
            should_numeric = True
        elif col_series.dtype == "object" and _looks_numeric_column(str(col), col_series):
            should_numeric = True

        if should_numeric:
            converted = _coerce_numeric(col_series)
            valid = int(converted.notna().sum())
            if valid >= max(1, int(0.5 * max(1, col_series.notna().sum()))):
                work_df[col] = converted

    num_cols = list(work_df.select_dtypes(include="number").columns)
    for col in num_cols:
        col_name = str(col).lower()
        sem = _find_semantic(column_semantics, str(col))
        sem_label = sem.label if sem else "unknown"
        sem_conf = sem.confidence if sem else 0.0
        neg_mask = work_df[col] < 0
        if bool(neg_mask.any()):
            if sem_label == "amount" and sem_conf >= 0.6:
                audit.log_exclusion(f"负数异常-{name}", f"{col} 为负数", work_df[neg_mask])
                work_df = work_df[~neg_mask]
            elif sem_label == "quantity" and sem_conf >= 0.6:
                neg_count = int(neg_mask.sum())
                audit.info(
                    f"负数告警-{name}",
                    f"{col} 发现 {neg_count} 条负数，按业务语义保留供复核（可能为退货/冲销）。",
                    affected_rows=neg_count,
                )
            elif any(token.lower() in col_name for token in HARD_NEGATIVE_DROP_TOKENS):
                audit.log_exclusion(f"负数异常-{name}", f"{col} 为负数", work_df[neg_mask])
                work_df = work_df[~neg_mask]
            elif any(token.lower() in col_name for token in SOFT_NEGATIVE_WARN_TOKENS):
                neg_count = int(neg_mask.sum())
                audit.info(
                    f"负数告警-{name}",
                    f"{col} 发现 {neg_count} 条负数，启发式保留供复核。",
                    affected_rows=neg_count,
                )

        quantity_like = (sem_label == "quantity" and sem_conf >= 0.55) or "数量" in str(col) or "qty" in col_name
        if quantity_like:
            huge_mask = work_df[col] > 100000
            if bool(huge_mask.any()):
                audit.log_exclusion(f"极端值-{name}", f"{col} 过大", work_df[huge_mask])
                work_df = work_df[~huge_mask]

    p_col = _find_column(work_df.columns, ("单价", "price"))
    q_col = _find_column(work_df.columns, ("数量", "qty"))
    t_col = _find_column(work_df.columns, ("总金额", "总额", "total", "amount"))
    if p_col and q_col and t_col and p_col in work_df.columns and q_col in work_df.columns and t_col in work_df.columns:
        try:
            expected = work_df[p_col] * work_df[q_col]
            logic_mask = (expected - work_df[t_col]).abs() > 1.0
            if bool(logic_mask.any()):
                logic_count = int(logic_mask.sum())
                audit.info(
                    f"逻辑校验告警-{name}",
                    f"发现 {logic_count} 条单价*数量 与总额不一致记录，保留原始数据供人工复核。",
                    affected_rows=logic_count,
                )
        except Exception:
            pass

    audit.info("L1体检完成", f"{name} 处理后 {len(work_df)} 行", affected_rows=max(0, len(df) - len(work_df)))
    return work_df


def _pick_sales_master_tables(dfs_context: Dict[str, pd.DataFrame]) -> Tuple[Optional[str], Optional[str]]:
    sales_name = None
    master_name = None
    for name, df in (dfs_context or {}).items():
        if str(name).startswith("__"):
            continue
        cols = [str(c) for c in df.columns]
        if sales_name is None and _find_column(cols, SALES_NAME_TOKENS):
            sales_name = str(name)
        if master_name is None and _find_column(cols, MASTER_NAME_TOKENS):
            master_name = str(name)
    if sales_name == master_name:
        return None, None
    return sales_name, master_name


def _fuzzy_map_entities(values: pd.Series, standards: pd.Series) -> pd.Series:
    left_values = values.fillna("").astype(str)
    right_values = standards.dropna().astype(str).unique().tolist()
    if not right_values:
        return pd.Series([None] * len(left_values), index=left_values.index)

    mapped = []
    for item in left_values:
        if not item:
            mapped.append(None)
            continue
        result = process.extractOne(item, right_values, scorer=fuzz.WRatio)
        if result and result[1] >= 70:
            mapped.append(result[0])
        else:
            mapped.append(None)
    return pd.Series(mapped, index=left_values.index)


def _run_l1_hygiene(
    dfs_context: Dict[str, pd.DataFrame],
    audit: AuditLogger,
    user_instruction: str = "",
) -> tuple[list[str], list[str]]:
    business_tables = [name for name in dfs_context.keys() if not str(name).startswith("__")]
    semantic_notes: list[str] = []
    for name in business_tables:
        sem = infer_dataframe_semantics(
            dfs_context[name],
            table_name=name,
            user_instruction=user_instruction,
        )
        if sem.used_fallback:
            note = f"{name}: 语义判定回退启发式，已自动进入保守清洗策略。"
            semantic_notes.append(note)
            audit.info("语义回退", note, affected_rows=0)
        for warning in sem.warnings:
            semantic_notes.append(f"{name}: {warning}")

        col_map = sem.column_map()
        row_map = {row.row_index: row for row in sem.rows}
        dfs_context[name] = _apply_hygiene_to_table(name, dfs_context[name], audit, col_map, row_map)
    return business_tables, semantic_notes


def run_l1_hygiene_skill(dfs_context: Dict[str, pd.DataFrame], user_instruction: str = "") -> SkillResult:
    audit = AuditLogger()
    try:
        business_tables, semantic_notes = _run_l1_hygiene(dfs_context, audit, user_instruction=user_instruction)
        if not business_tables:
            return SkillResult(handled=False, audit=audit, error="No business tables for L1 hygiene skill.")

        response_lines = [
            "### 💡 分析结论",
            "",
            "已执行语义增强的 L1 数据体检流程（列名 + 值分布 + LLM 判定），并输出审计日志。",
            f"共处理 {len(business_tables)} 张业务表。",
        ]
        if semantic_notes:
            response_lines.append("⚠️ 语义提示:\n" + "\n".join(f"- {note}" for note in semantic_notes[:5]))
        return SkillResult(handled=True, response_text="\n\n".join(response_lines), result_df=None, audit=audit)
    except Exception as exc:
        return SkillResult(handled=True, audit=audit, error=f"{type(exc).__name__}: {exc}")


def run_l1_l2_hygiene_merge_skill(
    dfs_context: Dict[str, pd.DataFrame],
    user_instruction: str = "",
) -> SkillResult:
    audit = AuditLogger()
    try:
        business_tables, semantic_notes = _run_l1_hygiene(dfs_context, audit, user_instruction=user_instruction)
        if not business_tables:
            return SkillResult(handled=False, audit=audit, error="No business tables for L1/L2 skill.")

        merged_df = None
        sales_name, master_name = _pick_sales_master_tables(dfs_context)
        if sales_name and master_name:
            sales_df = dfs_context[sales_name].copy()
            master_df = dfs_context[master_name].copy()
            sales_key = _find_column(sales_df.columns, SALES_NAME_TOKENS)
            master_key = _find_column(master_df.columns, MASTER_NAME_TOKENS)
            if sales_key and master_key:
                mapped = _fuzzy_map_entities(sales_df[sales_key], master_df[master_key])
                sales_df["_matched_master_name"] = mapped
                merged_df = pd.merge(
                    sales_df,
                    master_df,
                    left_on="_matched_master_name",
                    right_on=master_key,
                    how="left",
                )
                dfs_context["销售客户对齐结果.xlsx"] = merged_df
                match_count = int(mapped.notna().sum())
                audit.info("L2实体对齐", f"销售客户匹配 {match_count}/{len(mapped)}", affected_rows=match_count)

        response_lines = [
            "### 💡 分析结论",
            "",
            "已执行语义增强的 L1 数据体检流程，并输出审计日志。",
        ]
        if merged_df is not None:
            response_lines.append("已执行 L2 客户主数据对齐，生成 `销售客户对齐结果.xlsx`。")
        else:
            response_lines.append("未识别到可对齐的销售表/主数据表，已仅完成 L1 处理。")
        if semantic_notes:
            response_lines.append("⚠️ 语义提示:\n" + "\n".join(f"- {note}" for note in semantic_notes[:5]))

        return SkillResult(
            handled=True,
            response_text="\n\n".join(response_lines),
            result_df=merged_df,
            audit=audit,
        )
    except Exception as exc:
        return SkillResult(handled=True, audit=audit, error=f"{type(exc).__name__}: {exc}")

