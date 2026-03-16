from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Dict, Iterable, Optional

import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from app.services.llm_factory import get_llm
from app.services.semantic_infer import SemanticInferenceResult


ALLOWED_KEY_TYPES = {"entity_name", "deterministic", "none"}
NAME_HINT_TOKENS = ("客户", "公司", "企业", "主体", "name", "client", "company")


@dataclass(frozen=True)
class KeyQuality:
    non_null_ratio: float
    unique_ratio: float


@dataclass(frozen=True)
class MergePlan:
    left_key: str
    right_key: str
    key_type: str
    confidence: float
    reason: str
    left_quality: KeyQuality
    right_quality: KeyQuality
    overlap_ratio: float
    valid: bool
    validation_notes: tuple[str, ...]

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["validation_notes"] = list(self.validation_notes)
        return payload

    @classmethod
    def from_dict(cls, payload: dict) -> "MergePlan":
        return cls(
            left_key=str(payload.get("left_key", "")),
            right_key=str(payload.get("right_key", "")),
            key_type=str(payload.get("key_type", "none")),
            confidence=float(payload.get("confidence", 0.0)),
            reason=str(payload.get("reason", "")),
            left_quality=KeyQuality(**payload.get("left_quality", {"non_null_ratio": 0.0, "unique_ratio": 0.0})),
            right_quality=KeyQuality(**payload.get("right_quality", {"non_null_ratio": 0.0, "unique_ratio": 0.0})),
            overlap_ratio=float(payload.get("overlap_ratio", 0.0)),
            valid=bool(payload.get("valid", False)),
            validation_notes=tuple(str(x) for x in payload.get("validation_notes", [])),
        )


def _safe_json(raw: str) -> Optional[dict]:
    text = str(raw or "").strip()
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


def _preview_records(df: pd.DataFrame, max_rows: int = 5, max_cols: int = 12) -> list[dict]:
    if df.empty:
        return []
    cols = [str(c) for c in list(df.columns)[:max_cols]]
    rows = []
    for _, row in df.head(max_rows).iterrows():
        rows.append({c: str(row.get(c, ""))[:80] for c in cols})
    return rows


def _sem_brief(sem: Optional[SemanticInferenceResult], max_cols: int = 12) -> list[dict]:
    if sem is None:
        return []
    brief = []
    for col in sem.columns[:max_cols]:
        brief.append(
            {
                "name": col.name,
                "label": col.label,
                "confidence": round(float(col.confidence), 3),
            }
        )
    return brief


def _find_hint_col(columns: Iterable[str], tokens: Iterable[str]) -> Optional[str]:
    cols = [str(c) for c in columns]
    for token in tokens:
        token_lower = str(token).lower()
        for col in cols:
            if token_lower in col.lower():
                return col
    return None


def _heuristic_plan(left_df: pd.DataFrame, right_df: pd.DataFrame) -> dict:
    left_key = _find_hint_col(left_df.columns, NAME_HINT_TOKENS)
    right_key = _find_hint_col(right_df.columns, NAME_HINT_TOKENS)
    if left_key and right_key:
        return {
            "left_key": left_key,
            "right_key": right_key,
            "key_type": "entity_name",
            "confidence": 0.55,
            "reason": "启发式识别到双方均存在主体名称类字段。",
        }
    shared = [c for c in left_df.columns if c in right_df.columns]
    if shared:
        key = str(shared[0])
        return {
            "left_key": key,
            "right_key": key,
            "key_type": "deterministic",
            "confidence": 0.45,
            "reason": "启发式回退为同名字段匹配。",
        }
    return {
        "left_key": "",
        "right_key": "",
        "key_type": "none",
        "confidence": 0.0,
        "reason": "未找到可靠主键候选。",
    }


def _column_quality(df: pd.DataFrame, col: str) -> KeyQuality:
    if col not in df.columns:
        return KeyQuality(non_null_ratio=0.0, unique_ratio=0.0)
    series = df[col]
    non_null = float(series.notna().mean()) if len(series) else 0.0
    non_empty_series = series.fillna("").astype(str).str.strip()
    non_empty = non_empty_series[non_empty_series != ""]
    if non_empty.empty:
        return KeyQuality(non_null_ratio=non_null, unique_ratio=0.0)
    unique_ratio = float(non_empty.nunique() / max(1, len(non_empty)))
    return KeyQuality(non_null_ratio=non_null, unique_ratio=unique_ratio)


def _exact_overlap_ratio(left_df: pd.DataFrame, right_df: pd.DataFrame, left_key: str, right_key: str) -> float:
    if left_key not in left_df.columns or right_key not in right_df.columns:
        return 0.0
    left_set = set(
        left_df[left_key]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    right_set = set(
        right_df[right_key]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    if not left_set or not right_set:
        return 0.0
    inter = len(left_set & right_set)
    return float(inter / max(1, min(len(left_set), len(right_set))))


def _validate_plan(left_df: pd.DataFrame, right_df: pd.DataFrame, plan: dict) -> MergePlan:
    left_key = str(plan.get("left_key", "")).strip()
    right_key = str(plan.get("right_key", "")).strip()
    key_type = str(plan.get("key_type", "none")).strip().lower()
    confidence = float(plan.get("confidence", 0.0))
    reason = str(plan.get("reason", "")).strip()
    if key_type not in ALLOWED_KEY_TYPES:
        key_type = "none"

    left_quality = _column_quality(left_df, left_key)
    right_quality = _column_quality(right_df, right_key)
    overlap_ratio = _exact_overlap_ratio(left_df, right_df, left_key, right_key)
    notes: list[str] = []

    if not left_key or not right_key:
        notes.append("主键列为空。")
    if left_key and left_key not in left_df.columns:
        notes.append(f"左表不存在列 `{left_key}`。")
    if right_key and right_key not in right_df.columns:
        notes.append(f"右表不存在列 `{right_key}`。")
    if left_quality.non_null_ratio < 0.4:
        notes.append(f"左表键列非空率过低({left_quality.non_null_ratio:.2f})。")
    if right_quality.non_null_ratio < 0.4:
        notes.append(f"右表键列非空率过低({right_quality.non_null_ratio:.2f})。")
    if left_quality.unique_ratio < 0.1:
        notes.append(f"左表键列区分度过低({left_quality.unique_ratio:.2f})。")
    if right_quality.unique_ratio < 0.1:
        notes.append(f"右表键列区分度过低({right_quality.unique_ratio:.2f})。")
    if key_type == "deterministic" and overlap_ratio < 0.05:
        notes.append(f"确定性主键交集过低({overlap_ratio:.2f})。")
    if key_type == "none":
        notes.append("LLM 判定无合适主键。")
    if confidence < 0.35:
        notes.append(f"主键判定置信度过低({confidence:.2f})。")

    valid = len(notes) == 0
    return MergePlan(
        left_key=left_key,
        right_key=right_key,
        key_type=key_type,
        confidence=confidence,
        reason=reason,
        left_quality=left_quality,
        right_quality=right_quality,
        overlap_ratio=overlap_ratio,
        valid=valid,
        validation_notes=tuple(notes),
    )


def propose_merge_plan(
    *,
    left_name: str,
    right_name: str,
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_semantic: Optional[SemanticInferenceResult] = None,
    right_semantic: Optional[SemanticInferenceResult] = None,
    user_instruction: str = "",
) -> MergePlan:
    llm = get_llm(temperature=0)
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
你是金融数据合并专家。请判断两张表的 merge 主键。

输出 JSON:
{{
  "left_key": "左表主键列名",
  "right_key": "右表主键列名",
  "key_type": "entity_name|deterministic|none",
  "confidence": 0.0,
  "reason": "一句话原因"
}}

要求:
1. key_type=entity_name 仅用于公司/客户名称等别名场景。
2. key_type=deterministic 用于可稳定一一对应的编码/ID/日期组合键（这里只返回单列）。
3. 若没有可靠主键，返回 key_type=none。
4. 不要用订单号去匹配客户主数据。
""".strip(),
            ),
            (
                "human",
                """
用户意图:
{instruction}

左表: {left_name}
左表列: {left_columns}
左表语义: {left_sem}
左表样例: {left_preview}

右表: {right_name}
右表列: {right_columns}
右表语义: {right_sem}
右表样例: {right_preview}
""".strip(),
            ),
        ]
    )
    chain = prompt | llm | StrOutputParser()
    raw = chain.invoke(
        {
            "instruction": user_instruction or "",
            "left_name": left_name,
            "right_name": right_name,
            "left_columns": json.dumps([str(c) for c in left_df.columns], ensure_ascii=False),
            "right_columns": json.dumps([str(c) for c in right_df.columns], ensure_ascii=False),
            "left_sem": json.dumps(_sem_brief(left_semantic), ensure_ascii=False),
            "right_sem": json.dumps(_sem_brief(right_semantic), ensure_ascii=False),
            "left_preview": json.dumps(_preview_records(left_df), ensure_ascii=False),
            "right_preview": json.dumps(_preview_records(right_df), ensure_ascii=False),
        }
    )
    payload = _safe_json(raw)
    if not payload:
        payload = _heuristic_plan(left_df, right_df)
    return _validate_plan(left_df, right_df, payload)
