from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Dict, Tuple

import pandas as pd

from app.services.semantic_infer import SemanticInferenceResult, infer_dataframe_semantics


SEMANTIC_CACHE_KEY = "__semantic_contract__"
SEMANTIC_META_KEY = "__semantic_contract_meta__"


@dataclass(frozen=True)
class SemanticMemory:
    instruction: str
    table_signatures: dict


def _business_table_items(dfs_context: Dict[str, pd.DataFrame]) -> list[Tuple[str, pd.DataFrame]]:
    items: list[Tuple[str, pd.DataFrame]] = []
    for name, df in (dfs_context or {}).items():
        if str(name).startswith("__"):
            continue
        if isinstance(df, pd.DataFrame):
            items.append((str(name), df))
    return items


def _table_signature(df: pd.DataFrame) -> dict:
    head = df.head(8).copy()
    try:
        sanitized = head.where(head.notna(), "")
        head_payload = sanitized.astype(str).to_json(orient="split", force_ascii=False)
    except Exception:
        head_payload = repr(head.values.tolist())
    head_digest = hashlib.md5(head_payload.encode("utf-8")).hexdigest()[:16]
    return {
        "shape": [int(df.shape[0]), int(df.shape[1])],
        "columns": [str(c) for c in df.columns],
        "dtypes": [str(t) for t in df.dtypes.tolist()],
        "head_digest": head_digest,
    }


def _context_signature(dfs_context: Dict[str, pd.DataFrame]) -> dict:
    return {name: _table_signature(df) for name, df in _business_table_items(dfs_context)}


def _meta_to_dict(meta: object) -> dict:
    if isinstance(meta, SemanticMemory):
        return asdict(meta)
    if isinstance(meta, dict):
        return dict(meta)
    return {}


def _cache_matches(meta: object, instruction: str, signature: dict) -> bool:
    meta_dict = _meta_to_dict(meta)
    return (
        meta_dict.get("instruction") == instruction
        and meta_dict.get("table_signatures") == signature
    )


def get_semantic_contract(dfs_context: Dict[str, pd.DataFrame]) -> Dict[str, SemanticInferenceResult]:
    cached = (dfs_context or {}).get(SEMANTIC_CACHE_KEY)
    if isinstance(cached, dict):
        return cached
    return {}


def invalidate_semantic_contract(dfs_context: Dict[str, pd.DataFrame]) -> None:
    if dfs_context is None:
        return
    dfs_context.pop(SEMANTIC_CACHE_KEY, None)
    dfs_context.pop(SEMANTIC_META_KEY, None)


def ensure_semantic_contract(
    dfs_context: Dict[str, pd.DataFrame],
    user_instruction: str = "",
    force_refresh: bool = False,
) -> Dict[str, SemanticInferenceResult]:
    if dfs_context is None:
        return {}

    instruction = str(user_instruction or "").strip()
    signature = _context_signature(dfs_context)

    cached = get_semantic_contract(dfs_context)
    meta = dfs_context.get(SEMANTIC_META_KEY)
    if not force_refresh and cached and _cache_matches(meta, instruction, signature):
        return cached

    contract: Dict[str, SemanticInferenceResult] = {}
    for table_name, df in _business_table_items(dfs_context):
        contract[table_name] = infer_dataframe_semantics(
            df=df,
            table_name=table_name,
            user_instruction=instruction,
        )

    dfs_context[SEMANTIC_CACHE_KEY] = contract
    dfs_context[SEMANTIC_META_KEY] = SemanticMemory(
        instruction=instruction,
        table_signatures=signature,
    )
    return contract
