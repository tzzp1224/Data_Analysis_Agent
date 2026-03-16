from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import json
import os
import re

import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from app.services.llm_factory import get_llm
from app.services.semantic_profile import build_dataframe_profile
from app.services.semantic_taxonomy import COLUMN_TYPE_SPECS, ROW_TYPE_SPECS


ALLOWED_COLUMN_LABELS = set(COLUMN_TYPE_SPECS.keys())
ALLOWED_ROW_LABELS = set(ROW_TYPE_SPECS.keys())
DEFAULT_COLUMN_LABEL = "unknown"
DEFAULT_ROW_LABEL = "unknown"
LLM_TIMEOUT_HINT = os.getenv("SEMANTIC_LLM_TIMEOUT_HINT_SECONDS", "8")


@dataclass
class ColumnSemantic:
    name: str
    label: str
    confidence: float
    reason: str
    source: str


@dataclass
class RowSemantic:
    row_index: int
    label: str
    confidence: float
    reason: str
    source: str


@dataclass
class SemanticInferenceResult:
    columns: List[ColumnSemantic]
    rows: List[RowSemantic]
    warnings: List[str]
    used_fallback: bool

    def column_map(self) -> Dict[str, ColumnSemantic]:
        return {item.name: item for item in self.columns}


def _clamp_confidence(value: Any, default: float = 0.5) -> float:
    try:
        num = float(value)
    except Exception:
        return default
    if num < 0:
        return 0.0
    if num > 1:
        return 1.0
    return num


def _extract_json(raw_text: str) -> Optional[dict]:
    text = str(raw_text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except Exception:
        return None


def _score_amount(column_name: str, profile: dict) -> float:
    name = str(column_name).lower()
    score = 0.0
    if re.search(r"金额|amount|amt|price|fee|cost|total|应收|到账|gmv|revenue|支付", name):
        score += 0.45
    score += 0.30 * float(profile.get("numeric_parse_ratio", 0))
    score += 0.15 * float(profile.get("currency_symbol_ratio", 0))
    score += 0.10 * (1.0 - float(profile.get("integer_like_ratio", 0)))
    return min(score, 1.0)


def _score_quantity(column_name: str, profile: dict) -> float:
    name = str(column_name).lower()
    score = 0.0
    if re.search(r"数量|qty|count|件数|volume|num", name):
        score += 0.50
    score += 0.30 * float(profile.get("numeric_parse_ratio", 0))
    score += 0.20 * float(profile.get("integer_like_ratio", 0))
    return min(score, 1.0)


def _score_date(column_name: str, profile: dict) -> float:
    name = str(column_name).lower()
    score = 0.0
    if re.search(r"日期|时间|date|month|day|period", name):
        score += 0.45
    score += 0.55 * float(profile.get("date_parse_ratio", 0))
    return min(score, 1.0)


def _score_id(column_name: str, profile: dict) -> float:
    name = str(column_name).lower()
    score = 0.0
    if re.search(r"id|编号|编码|流水|单号|order|trx|ref|key", name):
        score += 0.50
    score += 0.25 * float(profile.get("unique_ratio", 0))
    score += 0.15 * float(profile.get("non_null_ratio", 0))
    score += 0.10 * (1.0 - float(profile.get("numeric_parse_ratio", 0)))
    return min(score, 1.0)


def _heuristic_column_semantics(profile_payload: dict) -> List[ColumnSemantic]:
    results: List[ColumnSemantic] = []
    for col in profile_payload.get("columns", []):
        col_name = str(col.get("name", ""))
        amount = _score_amount(col_name, col)
        quantity = _score_quantity(col_name, col)
        date_score = _score_date(col_name, col)
        id_score = _score_id(col_name, col)
        text_score = 0.35 + 0.35 * (1 - float(col.get("numeric_parse_ratio", 0)))

        candidates = {
            "amount": amount,
            "quantity": quantity,
            "date": date_score,
            "id": id_score,
            "text": min(text_score, 0.8),
        }
        label = max(candidates, key=candidates.get)
        confidence = candidates[label]
        if confidence < 0.45:
            label = DEFAULT_COLUMN_LABEL
            confidence = max(confidence, 0.35)

        results.append(
            ColumnSemantic(
                name=col_name,
                label=label,
                confidence=confidence,
                reason=f"heuristic score={confidence:.2f}",
                source="heuristic",
            )
        )
    return results


def _heuristic_row_semantics(profile_payload: dict) -> List[RowSemantic]:
    rows: List[RowSemantic] = []
    for row in profile_payload.get("rows", []):
        idx = int(row.get("row_index", 0))
        first_text = str(row.get("first_text_cell", "")).lower()
        non_null_ratio = float(row.get("non_null_ratio", 0))
        numeric_like_ratio = float(row.get("numeric_like_ratio", 0))

        label = "data_row"
        conf = 0.65
        reason = "default data row"
        if non_null_ratio <= 0.05:
            label, conf, reason = "empty_row", 0.95, "mostly empty"
        elif re.search(r"合计|小计|total|summary|汇总", first_text):
            label, conf, reason = "summary_row", 0.92, "summary keyword"
        elif non_null_ratio < 0.4 and numeric_like_ratio < 0.3:
            label, conf, reason = "metadata_row", 0.75, "sparse text-like row"

        rows.append(
            RowSemantic(
                row_index=idx,
                label=label,
                confidence=conf,
                reason=reason,
                source="heuristic",
            )
        )
    return rows


def _infer_with_llm(profile_payload: dict, user_instruction: str) -> Optional[dict]:
    llm = get_llm(temperature=0)
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
你是资深数据语义建模专家。请同时利用列名（含中英文）与列值统计特征进行判断。
严禁只看 token 下结论。

列类型可选：
- amount, quantity, date, id, text, unknown

行类型可选：
- data_row, summary_row, metadata_row, empty_row, unknown

返回 JSON（不要解释）:
{{
  "columns": [{{"name":"列名","label":"amount","confidence":0.0,"reason":"简短理由"}}],
  "rows": [{{"row_index":0,"label":"data_row","confidence":0.0,"reason":"简短理由"}}],
  "warnings": ["可选风险提示"]
}}
""".strip(),
            ),
            (
                "human",
                """
用户指令:
{instruction}

语义类型定义:
columns={column_specs}
rows={row_specs}

数据画像:
{profile}

要求:
1. 每个列都必须输出一条 columns 结果。
2. 置信度在 [0,1]。
3. 如果不确定，label 用 unknown 并写原因。
4. 优先准确，不要为了覆盖率强行猜测。
""".strip(),
            ),
        ]
    )
    chain = prompt | llm | StrOutputParser()
    raw = chain.invoke(
        {
            "instruction": user_instruction or "",
            "column_specs": json.dumps(
                {k: v.description for k, v in COLUMN_TYPE_SPECS.items()},
                ensure_ascii=False,
            ),
            "row_specs": json.dumps(
                {k: v.description for k, v in ROW_TYPE_SPECS.items()},
                ensure_ascii=False,
            ),
            "profile": json.dumps(profile_payload, ensure_ascii=False),
            "timeout_hint": LLM_TIMEOUT_HINT,
        }
    )
    return _extract_json(raw)


def _merge_llm_and_heuristic(profile_payload: dict, llm_json: dict) -> SemanticInferenceResult:
    warnings: List[str] = []
    columns_by_name = {c.name: c for c in _heuristic_column_semantics(profile_payload)}
    row_by_idx = {r.row_index: r for r in _heuristic_row_semantics(profile_payload)}

    raw_columns = llm_json.get("columns", []) if isinstance(llm_json, dict) else []
    for item in raw_columns:
        name = str(item.get("name", ""))
        if name not in columns_by_name:
            continue
        label = str(item.get("label", DEFAULT_COLUMN_LABEL)).strip().lower()
        if label not in ALLOWED_COLUMN_LABELS:
            label = DEFAULT_COLUMN_LABEL
        llm_conf = _clamp_confidence(item.get("confidence"), default=0.5)
        base = columns_by_name[name]
        fused_conf = _clamp_confidence(0.7 * llm_conf + 0.3 * base.confidence, default=base.confidence)
        columns_by_name[name] = ColumnSemantic(
            name=name,
            label=label,
            confidence=fused_conf,
            reason=str(item.get("reason", ""))[:160] or base.reason,
            source="llm+heuristic",
        )

    raw_rows = llm_json.get("rows", []) if isinstance(llm_json, dict) else []
    for item in raw_rows:
        try:
            idx = int(item.get("row_index"))
        except Exception:
            continue
        if idx not in row_by_idx:
            continue
        label = str(item.get("label", DEFAULT_ROW_LABEL)).strip().lower()
        if label not in ALLOWED_ROW_LABELS:
            label = DEFAULT_ROW_LABEL
        llm_conf = _clamp_confidence(item.get("confidence"), default=0.5)
        base = row_by_idx[idx]
        fused_conf = _clamp_confidence(0.7 * llm_conf + 0.3 * base.confidence, default=base.confidence)
        row_by_idx[idx] = RowSemantic(
            row_index=idx,
            label=label,
            confidence=fused_conf,
            reason=str(item.get("reason", ""))[:160] or base.reason,
            source="llm+heuristic",
        )

    for warning in llm_json.get("warnings", []) if isinstance(llm_json, dict) else []:
        text = str(warning).strip()
        if text:
            warnings.append(text[:180])

    return SemanticInferenceResult(
        columns=list(columns_by_name.values()),
        rows=list(row_by_idx.values()),
        warnings=warnings,
        used_fallback=False,
    )


def infer_dataframe_semantics(
    df: pd.DataFrame,
    table_name: str,
    user_instruction: str = "",
) -> SemanticInferenceResult:
    profile_payload = build_dataframe_profile(df, table_name=table_name)
    heuristic_columns = _heuristic_column_semantics(profile_payload)
    heuristic_rows = _heuristic_row_semantics(profile_payload)

    try:
        llm_json = _infer_with_llm(profile_payload=profile_payload, user_instruction=user_instruction)
        if not llm_json:
            raise ValueError("empty llm semantic response")
        merged = _merge_llm_and_heuristic(profile_payload, llm_json)
        return merged
    except Exception as exc:
        return SemanticInferenceResult(
            columns=heuristic_columns,
            rows=heuristic_rows,
            warnings=[f"语义判定回退启发式: {type(exc).__name__}: {exc}"],
            used_fallback=True,
        )
