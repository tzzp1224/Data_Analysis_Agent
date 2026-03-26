from __future__ import annotations

import json
import re
from typing import Dict, Optional

import pandas as pd

from app.services.semantic_contract import ensure_semantic_contract
from app.services.semantic_infer import SemanticInferenceResult
from app.skills.contracts import (
    ERROR_TYPE_RUNTIME,
    ERROR_TYPE_TABLE_SELECTION,
    SkillResult,
)
from app.utils.tools import AuditLogger


def _pick_table(dfs_context: Dict[str, pd.DataFrame]) -> tuple[Optional[str], Optional[pd.DataFrame]]:
    for name, df in (dfs_context or {}).items():
        if str(name).startswith("__"):
            continue
        if isinstance(df, pd.DataFrame):
            return str(name), df
    return None, None


def _coerce_numeric(series: pd.Series) -> pd.Series:
    text = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("¥", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(text, errors="coerce")


def _best_amount_column(df: pd.DataFrame, sem: Optional[SemanticInferenceResult]) -> Optional[str]:
    if sem is not None:
        candidates = [c for c in sem.columns if c.label == "amount" and c.name in df.columns]
        if candidates:
            candidates.sort(key=lambda x: x.confidence, reverse=True)
            return str(candidates[0].name)

    for col in df.columns:
        name = str(col).lower()
        if any(tok in name for tok in ("金额", "amount", "gmv", "revenue", "total")):
            return str(col)

    numeric_cols = []
    for col in df.columns:
        ratio = float(_coerce_numeric(df[col]).notna().mean())
        if ratio >= 0.6:
            numeric_cols.append((str(col), ratio))
    if numeric_cols:
        numeric_cols.sort(key=lambda x: x[1], reverse=True)
        return numeric_cols[0][0]
    return None


def _best_dim_column(df: pd.DataFrame, amount_col: Optional[str]) -> Optional[str]:
    for col in df.columns:
        col_name = str(col)
        if amount_col and col_name == amount_col:
            continue
        if df[col].dtype == "object":
            return col_name
    for col in df.columns:
        col_name = str(col)
        if amount_col and col_name == amount_col:
            continue
        return col_name
    return None


def _extract_top_n(instruction: str, default_n: int = 10) -> int:
    text = str(instruction or "")
    match = re.search(r"top\s*([0-9]+)", text, re.IGNORECASE)
    if match:
        return max(1, int(match.group(1)))
    cn = re.search(r"前\s*([0-9]+)", text)
    if cn:
        return max(1, int(cn.group(1)))
    return default_n


def run_expert_excel_skill(
    dfs_context: Dict[str, pd.DataFrame],
    instruction: str,
    semantic_contract: Optional[Dict[str, SemanticInferenceResult]] = None,
) -> SkillResult:
    audit = AuditLogger()
    try:
        table_name, df = _pick_table(dfs_context)
        if table_name is None or df is None:
            return SkillResult(
                handled=True,
                blocked=True,
                error_type=ERROR_TYPE_TABLE_SELECTION,
                response_text="Expert 已阻断：当前没有可处理的业务表。",
                audit=audit,
            )

        contract = semantic_contract or ensure_semantic_contract(dfs_context, user_instruction=instruction)
        sem = contract.get(table_name)

        amount_col = _best_amount_column(df, sem)
        dim_col = _best_dim_column(df, amount_col)
        top_n = _extract_top_n(instruction)

        instruction_text = str(instruction or "").lower()
        execution_spec: dict = {
            "worker": "expert_excel",
            "table": table_name,
            "instruction": instruction,
            "selected_columns": {
                "amount": amount_col,
                "dimension": dim_col,
            },
            "operation": "profile",
            "top_n": top_n,
        }

        if amount_col and any(token in instruction_text for token in ["sum", "总计", "求和", "合计"]):
            execution_spec["operation"] = "sum"
            values = _coerce_numeric(df[amount_col]).dropna()
            total = float(values.sum()) if not values.empty else 0.0
            result_df = pd.DataFrame(
                [
                    {
                        "指标": f"{amount_col}总计",
                        "数值": round(total, 4),
                        "样本行数": int(len(values)),
                    }
                ]
            )
            audit.info("Expert执行", f"按金额列 `{amount_col}` 进行求和。", affected_rows=len(result_df))
        elif amount_col and dim_col and any(token in instruction_text for token in ["group", "分组", "按"]):
            execution_spec["operation"] = "groupby_sum"
            temp_df = df.copy()
            temp_df[amount_col] = _coerce_numeric(temp_df[amount_col])
            result_df = (
                temp_df.dropna(subset=[amount_col])
                .groupby(dim_col, as_index=False)[amount_col]
                .sum()
                .sort_values(amount_col, ascending=False)
                .head(top_n)
                .reset_index(drop=True)
            )
            audit.info(
                "Expert执行",
                f"按 `{dim_col}` 分组并聚合 `{amount_col}`，返回 Top{top_n}。",
                affected_rows=len(result_df),
            )
        elif amount_col and any(token in instruction_text for token in ["top", "前", "排序", "largest"]):
            execution_spec["operation"] = "top_n"
            temp_df = df.copy()
            temp_df[amount_col] = _coerce_numeric(temp_df[amount_col])
            result_df = temp_df.sort_values(amount_col, ascending=False).head(top_n).reset_index(drop=True)
            audit.info("Expert执行", f"按 `{amount_col}` 取 Top{top_n}。", affected_rows=len(result_df))
        else:
            execution_spec["operation"] = "schema_guided_profile"
            profile_rows = [
                {
                    "列名": str(col),
                    "dtype": str(df[col].dtype),
                    "非空率": round(float(df[col].notna().mean()), 3),
                }
                for col in list(df.columns)[:20]
            ]
            result_df = pd.DataFrame(profile_rows)
            audit.info("Expert执行", "未命中明确计算意图，返回结构化画像建议。", affected_rows=len(result_df))

        response_text = (
            "### 💡 Expert 兜底执行\n\n"
            f"已根据自然语言生成结构化执行规格（Execution Spec），并用确定性路径执行。\n\n"
            f"- 表: `{table_name}`\n"
            f"- 操作: `{execution_spec['operation']}`\n"
            f"- 关键列: amount=`{amount_col}` dimension=`{dim_col}`\n\n"
            f"```json\n{json.dumps(execution_spec, ensure_ascii=False, indent=2)}\n```"
        )

        return SkillResult(
            handled=True,
            response_text=response_text,
            result_df=result_df,
            audit=audit,
            evidence={"execution_spec": execution_spec},
            change_summary=f"expert:{execution_spec['operation']}",
        )
    except Exception as exc:
        return SkillResult(
            handled=True,
            audit=audit,
            error=f"{type(exc).__name__}: {exc}",
            error_type=ERROR_TYPE_RUNTIME,
        )
