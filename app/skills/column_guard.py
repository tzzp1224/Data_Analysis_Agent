from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Sequence, Tuple

import pandas as pd

from app.services.semantic_infer import SemanticInferenceResult


@dataclass(frozen=True)
class RequiredColumnSpec:
    key: str
    display_name: str
    semantic_labels: Tuple[str, ...]
    min_confidence: float = 0.6
    name_tokens: Tuple[str, ...] = ()


def _find_column_by_tokens(columns: Iterable[str], tokens: Sequence[str]) -> Optional[str]:
    text_cols = [str(col) for col in columns]
    for token in tokens:
        token_lower = str(token).lower()
        for col in text_cols:
            if token_lower in col.lower():
                return col
    return None


def _pick_from_semantic(
    sem: SemanticInferenceResult,
    allowed_labels: Sequence[str],
    min_confidence: float,
) -> Optional[str]:
    candidates = [
        col
        for col in sem.columns
        if col.label in allowed_labels and col.confidence >= min_confidence
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.confidence, reverse=True)
    return candidates[0].name


def resolve_required_columns(
    df: pd.DataFrame,
    sem: SemanticInferenceResult,
    specs: Sequence[RequiredColumnSpec],
) -> tuple[Dict[str, str], list[RequiredColumnSpec]]:
    resolved: Dict[str, str] = {}
    missing: list[RequiredColumnSpec] = []

    for spec in specs:
        selected = _pick_from_semantic(
            sem=sem,
            allowed_labels=spec.semantic_labels,
            min_confidence=spec.min_confidence,
        )
        if not selected and spec.name_tokens:
            selected = _find_column_by_tokens(df.columns, spec.name_tokens)
        if selected:
            resolved[spec.key] = selected
        else:
            missing.append(spec)
    return resolved, missing


def build_missing_columns_message(
    *,
    skill_name: str,
    table_name: str,
    df: pd.DataFrame,
    sem: SemanticInferenceResult,
    missing_specs: Sequence[RequiredColumnSpec],
    guidance: str = "",
) -> str:
    expected = "、".join(spec.display_name for spec in missing_specs)
    columns_preview = ", ".join(str(col) for col in list(df.columns)[:16]) or "无"
    semantic_preview = ", ".join(
        f"{c.name}:{c.label}({c.confidence:.2f})" for c in sem.columns[:10]
    ) or "无"
    suffix = f"\n\n{guidance.strip()}" if guidance.strip() else ""
    return (
        f"{skill_name}已阻断：`{table_name}` 缺少关键列（{expected}），为避免误判不继续执行。\n\n"
        f"当前列: {columns_preview}\n\n"
        f"语义识别(前10列): {semantic_preview}"
        f"{suffix}"
    )

