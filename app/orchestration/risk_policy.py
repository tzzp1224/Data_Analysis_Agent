from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional

import pandas as pd


HOOK_APPROVE = "approve"
HOOK_SELECT_OPTION = "select_option"
HOOK_MAP_COLUMNS = "map_columns"
HOOK_SET_THRESHOLD = "set_threshold"


@dataclass
class PendingHook:
    hook_id: str
    hook_type: str
    risk_level: str
    question: str
    options: list[dict[str, Any]] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)
    deadline_hint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RiskDecision:
    pending_hook: Optional[PendingHook] = None
    risk_trace: list[dict[str, Any]] = field(default_factory=list)


def _trace(action: str, detail: str, **extra: Any) -> dict[str, Any]:
    payload = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "stage": "risk_policy",
        "action": action,
        "detail": detail,
    }
    payload.update(extra)
    return payload


def _new_hook(
    *,
    hook_type: str,
    risk_level: str,
    question: str,
    options: Optional[list[dict[str, Any]]] = None,
    evidence: Optional[dict[str, Any]] = None,
    deadline_hint: str = "",
) -> PendingHook:
    return PendingHook(
        hook_id=f"hook_{uuid.uuid4().hex[:10]}",
        hook_type=hook_type,
        risk_level=risk_level,
        question=question,
        options=list(options or []),
        evidence=dict(evidence or {}),
        deadline_hint=deadline_hint,
    )


def _business_tables(dfs_context: Dict[str, pd.DataFrame]) -> list[tuple[str, pd.DataFrame]]:
    tables: list[tuple[str, pd.DataFrame]] = []
    for name, df in (dfs_context or {}).items():
        if str(name).startswith("__"):
            continue
        if isinstance(df, pd.DataFrame):
            tables.append((str(name), df))
    return tables


def _merge_options(dfs_context: Dict[str, pd.DataFrame], limit: int = 8) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    tables = _business_tables(dfs_context)
    if len(tables) < 2:
        return [], {"reason": "not_enough_tables"}

    left_name, left_df = tables[0]
    right_name, right_df = tables[1]
    shared = [str(col) for col in left_df.columns if col in right_df.columns]
    options = [{"label": col, "value": col} for col in shared[:limit]]

    evidence = {
        "target": "merge_key_override",
        "left_table": left_name,
        "right_table": right_name,
        "shared_columns": shared[:limit],
    }
    return options, evidence


def _is_strict_instruction(instruction: str) -> bool:
    text = str(instruction or "")
    strict_tokens = ("严格", "强清洗", "删除异常", "剔除")
    return any(token in text for token in strict_tokens)


def evaluate_pre_execution_risk(
    *,
    instruction: str,
    step: dict,
    dfs_context: Dict[str, pd.DataFrame],
    step_results: Optional[list[dict[str, Any]]] = None,
) -> RiskDecision:
    decision = RiskDecision()
    worker = str(step.get("selected_worker", "")).strip()
    confidence = float(step.get("confidence", 0.5) or 0.5)
    risk_level = str(step.get("risk_level", "low") or "low")

    readiness = dfs_context.get("__data_readiness__")
    readiness_status = ""
    if isinstance(readiness, dict):
        readiness_status = str(readiness.get("status", "")).strip().lower()

    if readiness_status == "blocked" and not bool(dfs_context.get("__data_readiness_ack__")):
        hook = _new_hook(
            hook_type=HOOK_MAP_COLUMNS,
            risk_level="high",
            question="上传文件结构质量较低，建议先进行字段映射/修复后再执行。是否继续？",
            evidence={
                "target": "data_readiness",
                "status": "blocked",
                "issues": list(readiness.get("issues") or [])[:6],
                "recommendations": list(readiness.get("recommendations") or [])[:6],
            },
            deadline_hint="建议先修复映射后再继续。",
        )
        decision.pending_hook = hook
        decision.risk_trace.append(_trace("hook_triggered", "data readiness blocked", hook_type=hook.hook_type))
        return decision

    if readiness_status == "recoverable" and not bool(dfs_context.get("__data_readiness_ack__")):
        hook = _new_hook(
            hook_type=HOOK_APPROVE,
            risk_level="medium",
            question="文件结构可恢复但存在风险，建议先修复后继续。是否按当前数据继续执行？",
            evidence={
                "target": "data_readiness",
                "status": "recoverable",
                "issues": list(readiness.get("issues") or [])[:6],
            },
            deadline_hint="继续执行可能影响精度。",
        )
        decision.pending_hook = hook
        decision.risk_trace.append(_trace("hook_triggered", "data readiness recoverable", hook_type=hook.hook_type))
        return decision

    if worker == "l2_merge" and confidence < 0.7:
        options, evidence = _merge_options(dfs_context)
        hook_type = HOOK_SELECT_OPTION if options else HOOK_APPROVE
        question = "主键置信度偏低，请选择用于合并的主键列。" if options else "主键置信度偏低，是否继续自动合并？"
        hook = _new_hook(
            hook_type=hook_type,
            risk_level="high",
            question=question,
            options=options,
            evidence=evidence,
            deadline_hint="建议人工确认主键。",
        )
        decision.pending_hook = hook
        decision.risk_trace.append(_trace("hook_triggered", "merge key low confidence", hook_type=hook.hook_type))
        return decision

    if worker == "expert_excel" and confidence < 0.55:
        hook = _new_hook(
            hook_type=HOOK_APPROVE,
            risk_level="medium",
            question="当前将进入 Expert 兜底执行路径，是否继续？",
            evidence={"target": "expert_route", "confidence": round(confidence, 3)},
            deadline_hint="建议先确认目标输出。",
        )
        decision.pending_hook = hook
        decision.risk_trace.append(_trace("hook_triggered", "expert route low confidence", hook_type=hook.hook_type))
        return decision

    if risk_level == "high" and _is_strict_instruction(instruction):
        hook = _new_hook(
            hook_type=HOOK_APPROVE,
            risk_level="high",
            question="当前步骤为高风险且包含严格清洗指令，是否继续执行？",
            evidence={"target": "high_risk_write", "worker": worker, "risk_level": risk_level},
            deadline_hint="继续执行可能造成不可逆数据变更。",
        )
        decision.pending_hook = hook
        decision.risk_trace.append(_trace("hook_triggered", "high risk strict operation", hook_type=hook.hook_type))
        return decision

    recent = list(step_results or [])
    if len(recent) >= 2 and all(not bool(item.get("handled")) for item in recent[-2:]):
        hook = _new_hook(
            hook_type=HOOK_SET_THRESHOLD,
            risk_level="medium",
            question="最近连续两步失败，是否调整容差/阈值后重试？",
            evidence={"target": "retry_strategy", "recent_failures": 2},
            deadline_hint="可提供新的阈值参数。",
        )
        decision.pending_hook = hook
        decision.risk_trace.append(_trace("hook_triggered", "repeated failures", hook_type=hook.hook_type))
        return decision

    decision.risk_trace.append(_trace("risk_passed", "no blocking hook triggered", worker=worker))
    return decision


def normalize_human_payload(payload: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return {
        "hook_id": str(payload.get("hook_id", "")).strip(),
        "decision_type": str(payload.get("decision_type", "")).strip().lower(),
        "decision_value": payload.get("decision_value"),
        "comment": str(payload.get("comment", "")).strip(),
    }


def build_pending_hook_for_failure(reason: str, error_type: str = "") -> dict[str, Any]:
    hook = _new_hook(
        hook_type=HOOK_APPROVE,
        risk_level="high",
        question="步骤执行失败，是否继续（approve）/终止（reject）/按新指令重规划（revise）？",
        evidence={"target": "failure_recovery", "error_type": error_type, "reason": reason},
        deadline_hint="建议根据错误原因选择。",
    )
    return hook.to_dict()
