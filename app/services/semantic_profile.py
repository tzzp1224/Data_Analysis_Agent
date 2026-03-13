from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List
import re

import pandas as pd


DATE_RE = re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{4}年\d{1,2}月")
NUMERIC_RE = re.compile(r"-?\d+(?:\.\d+)?")
CURRENCY_RE = re.compile(r"[¥$￥]|usd|cny|rmb", re.I)


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    non_null_ratio: float
    unique_ratio: float
    numeric_parse_ratio: float
    date_parse_ratio: float
    integer_like_ratio: float
    negative_ratio: float
    currency_symbol_ratio: float
    sample_values: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "dtype": self.dtype,
            "non_null_ratio": self.non_null_ratio,
            "unique_ratio": self.unique_ratio,
            "numeric_parse_ratio": self.numeric_parse_ratio,
            "date_parse_ratio": self.date_parse_ratio,
            "integer_like_ratio": self.integer_like_ratio,
            "negative_ratio": self.negative_ratio,
            "currency_symbol_ratio": self.currency_symbol_ratio,
            "sample_values": self.sample_values,
        }


@dataclass
class RowProfile:
    row_index: int
    non_null_ratio: float
    numeric_like_ratio: float
    first_text_cell: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "row_index": self.row_index,
            "non_null_ratio": self.non_null_ratio,
            "numeric_like_ratio": self.numeric_like_ratio,
            "first_text_cell": self.first_text_cell,
        }


def _safe_to_str_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _numeric_parse_ratio(series: pd.Series) -> float:
    raw = _safe_to_str_series(series)
    if len(raw) == 0:
        return 0.0
    cleaned = raw.str.replace(",", "", regex=False).str.replace("¥", "", regex=False).str.replace("$", "", regex=False)
    numeric = pd.to_numeric(cleaned, errors="coerce")
    return float(numeric.notna().mean())


def _date_parse_ratio(series: pd.Series) -> float:
    raw = _safe_to_str_series(series)
    if len(raw) == 0:
        return 0.0
    parsed = pd.to_datetime(raw, errors="coerce")
    return float(parsed.notna().mean())


def _integer_like_ratio(series: pd.Series) -> float:
    raw = _safe_to_str_series(series)
    cleaned = raw.str.replace(",", "", regex=False).str.replace("¥", "", regex=False).str.replace("$", "", regex=False)
    numeric = pd.to_numeric(cleaned, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return 0.0
    int_like = (valid.round(0) == valid).mean()
    return float(int_like)


def _negative_ratio(series: pd.Series) -> float:
    raw = _safe_to_str_series(series)
    cleaned = raw.str.replace(",", "", regex=False).str.replace("¥", "", regex=False).str.replace("$", "", regex=False)
    numeric = pd.to_numeric(cleaned, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return 0.0
    return float((valid < 0).mean())


def _currency_symbol_ratio(series: pd.Series) -> float:
    raw = _safe_to_str_series(series)
    if len(raw) == 0:
        return 0.0
    has_currency = raw.str.contains(CURRENCY_RE, na=False)
    return float(has_currency.mean())


def build_column_profiles(df: pd.DataFrame, sample_size: int = 8) -> List[ColumnProfile]:
    profiles: List[ColumnProfile] = []
    row_count = max(len(df), 1)
    for col in df.columns:
        series = df[col]
        text_series = _safe_to_str_series(series)
        sample_values = [v for v in text_series.head(sample_size).tolist() if v][:sample_size]

        profile = ColumnProfile(
            name=str(col),
            dtype=str(series.dtype),
            non_null_ratio=float(series.notna().mean()) if row_count else 0.0,
            unique_ratio=float(series.nunique(dropna=True) / row_count),
            numeric_parse_ratio=_numeric_parse_ratio(series),
            date_parse_ratio=_date_parse_ratio(series),
            integer_like_ratio=_integer_like_ratio(series),
            negative_ratio=_negative_ratio(series),
            currency_symbol_ratio=_currency_symbol_ratio(series),
            sample_values=sample_values,
        )
        profiles.append(profile)
    return profiles


def build_row_profiles(df: pd.DataFrame, max_rows: int = 30) -> List[RowProfile]:
    profiles: List[RowProfile] = []
    if df.empty:
        return profiles
    subset = df.head(max_rows)
    for row_index, row in subset.iterrows():
        values = ["" if pd.isna(v) else str(v).strip() for v in row.tolist()]
        non_empty = [v for v in values if v]
        non_null_ratio = float(len(non_empty) / max(len(values), 1))
        numeric_like = sum(bool(NUMERIC_RE.search(v)) for v in non_empty)
        numeric_like_ratio = float(numeric_like / max(len(non_empty), 1)) if non_empty else 0.0
        first_text_cell = ""
        for v in non_empty:
            if not NUMERIC_RE.fullmatch(v):
                first_text_cell = v[:80]
                break

        profiles.append(
            RowProfile(
                row_index=int(row_index),
                non_null_ratio=non_null_ratio,
                numeric_like_ratio=numeric_like_ratio,
                first_text_cell=first_text_cell,
            )
        )
    return profiles


def build_dataframe_profile(df: pd.DataFrame, table_name: str, row_profile_limit: int = 30) -> Dict[str, Any]:
    columns = [profile.to_dict() for profile in build_column_profiles(df)]
    rows = [profile.to_dict() for profile in build_row_profiles(df, max_rows=row_profile_limit)]
    return {
        "table_name": table_name,
        "shape": [int(df.shape[0]), int(df.shape[1])],
        "columns": columns,
        "rows": rows,
    }

