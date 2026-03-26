from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict

import pandas as pd


@dataclass
class DataReadinessReport:
    status: str
    score: float
    issues: list[str]
    recommendations: list[str]
    evidence: dict

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["score"] = round(float(self.score), 3)
        return payload


def _business_tables(dfs_context: Dict[str, pd.DataFrame]) -> list[tuple[str, pd.DataFrame]]:
    tables: list[tuple[str, pd.DataFrame]] = []
    for name, df in (dfs_context or {}).items():
        if str(name).startswith("__"):
            continue
        if isinstance(df, pd.DataFrame):
            tables.append((str(name), df))
    return tables


def _table_score(df: pd.DataFrame) -> tuple[float, dict, list[str]]:
    if df.empty:
        return 0.15, {"rows": 0, "cols": int(df.shape[1])}, ["表为空"]

    rows = int(df.shape[0])
    cols = int(df.shape[1])
    unnamed_ratio = 0.0
    if cols > 0:
        unnamed = [c for c in df.columns if str(c).lower().startswith("unnamed")]
        unnamed_ratio = float(len(unnamed) / cols)

    null_ratio = float(df.isna().mean().mean()) if rows > 0 and cols > 0 else 1.0
    object_ratio = float((df.dtypes == "object").mean()) if cols > 0 else 1.0

    score = 1.0
    score -= min(unnamed_ratio * 0.5, 0.5)
    score -= min(max(null_ratio - 0.25, 0.0) * 0.6, 0.4)
    if rows < 3:
        score -= 0.25
    if cols < 2:
        score -= 0.2

    issues: list[str] = []
    if unnamed_ratio > 0.35:
        issues.append("表头质量较差（Unnamed列较多）")
    if null_ratio > 0.65:
        issues.append("空值占比较高")
    if rows < 3:
        issues.append("有效数据行过少")
    if cols < 2:
        issues.append("有效字段过少")
    if object_ratio > 0.95:
        issues.append("字段类型几乎全为文本，结构化程度较低")

    evidence = {
        "rows": rows,
        "cols": cols,
        "unnamed_ratio": round(unnamed_ratio, 3),
        "null_ratio": round(null_ratio, 3),
        "object_ratio": round(object_ratio, 3),
        "score": round(max(min(score, 1.0), 0.0), 3),
    }
    return max(min(score, 1.0), 0.0), evidence, issues


def assess_data_readiness(dfs_context: Dict[str, pd.DataFrame]) -> DataReadinessReport:
    tables = _business_tables(dfs_context)
    if not tables:
        return DataReadinessReport(
            status="blocked",
            score=0.0,
            issues=["未检测到可用业务表"],
            recommendations=["请上传至少一个可解析的 Excel/CSV 数据表"],
            evidence={"table_count": 0},
        )

    table_scores: dict[str, dict] = {}
    all_issues: list[str] = []
    raw_scores: list[float] = []

    for name, df in tables:
        score, evidence, issues = _table_score(df)
        raw_scores.append(score)
        table_scores[name] = evidence
        for issue in issues:
            all_issues.append(f"{name}: {issue}")

    score = sum(raw_scores) / max(1, len(raw_scores))
    if score >= 0.7:
        status = "ready"
    elif score >= 0.4:
        status = "recoverable"
    else:
        status = "blocked"

    recommendations: list[str] = []
    if status == "ready":
        recommendations.append("数据结构基本可用，可进入主流程")
    elif status == "recoverable":
        recommendations.append("建议先执行字段标准化与表头修复，再进入分析")
        recommendations.append("如关键列缺失，请人工映射主键/金额/日期列")
    else:
        recommendations.append("建议先清理模板说明行、合并单元格和空白列")
        recommendations.append("必要时请提供字段映射（主键/金额/日期）后重试")

    return DataReadinessReport(
        status=status,
        score=score,
        issues=all_issues[:20],
        recommendations=recommendations,
        evidence={
            "table_count": len(tables),
            "table_scores": table_scores,
        },
    )
