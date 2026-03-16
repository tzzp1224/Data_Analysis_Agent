import pandas as pd
import os
import re
import json
import csv
from typing import Iterable, List, Tuple
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from app.services.llm_factory import get_llm
from pydantic import BaseModel, Field


EXCEL_EXTENSIONS = {".xlsx", ".xls", ".xlsm", ".xlsb"}
CSV_EXTENSIONS = {".csv"}
HEADER_HINT_TOKENS = (
    "日期",
    "时间",
    "金额",
    "数量",
    "单价",
    "总额",
    "总价",
    "客户",
    "名称",
    "编码",
    "编号",
    "流水",
    "订单",
    "区域",
    "部门",
    "月份",
    "date",
    "time",
    "amount",
    "price",
    "qty",
    "quantity",
    "total",
    "id",
    "code",
    "name",
)


# 定义加载配置对象
class FileLoadConfig(BaseModel):
    file_path: str
    sheet_name: str
    header_row: int
    file_type: str = Field(default="excel", description="excel 或 csv")
    delimiter: str = Field(default=",", description="CSV 分隔符")
    reason: str = Field(description="AI 做出此判断的理由")

def clean_gemini_output(raw_content: str) -> str:
    """清洗 Gemini 输出"""
    content = str(raw_content).strip()
    if "text:" in content:
        pattern = r"text:\s*(.*?)(?:,\s*extras|\})"
        match = re.search(pattern, content, re.DOTALL)
        if match: return match.group(1).strip().strip("'").strip('"')
    
    content = content.replace("```json", "").replace("```", "").strip()
    return content

def detect_file_type(file_path: str) -> str:
    ext = os.path.splitext(file_path)[1].lower()
    if ext in EXCEL_EXTENSIONS:
        return "excel"
    if ext in CSV_EXTENSIONS:
        return "csv"
    raise ValueError(f"不支持的文件类型: {ext}")


def detect_csv_delimiter(file_path: str) -> str:
    try:
        with open(file_path, "r", encoding="utf-8-sig", newline="") as fp:
            sample = fp.read(4096)
        if not sample.strip():
            return ","
        sniffed = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return sniffed.delimiter
    except Exception:
        return ","


def _normalize_cell(value: object) -> str:
    return str(value or "").replace("\ufeff", "").strip()


def _is_numeric_like(text: str) -> bool:
    if not text:
        return False
    cleaned = (
        text.replace(",", "")
        .replace("¥", "")
        .replace("￥", "")
        .replace("$", "")
        .strip()
    )
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", cleaned))


def _header_hint_hits(cells: Iterable[str]) -> int:
    hits = 0
    for raw in cells:
        cell = str(raw).lower()
        if any(token in cell for token in HEADER_HINT_TOKENS):
            hits += 1
    return hits


def _numeric_ratio(cells: Iterable[str]) -> float:
    values = [str(c).strip() for c in cells if str(c).strip()]
    if not values:
        return 0.0
    return float(sum(_is_numeric_like(v) for v in values) / len(values))


def _row_fill_ratio(cells: Iterable[str]) -> float:
    values = [str(c) for c in cells]
    if not values:
        return 0.0
    return float(sum(bool(v.strip()) for v in values) / len(values))


def _score_header_candidate(rows: List[List[str]], idx: int) -> float:
    if idx >= len(rows):
        return -1.0
    row = rows[idx]
    non_empty = [cell for cell in row if str(cell).strip()]
    if not non_empty:
        return -1.0

    unique_ratio = float(len(set(non_empty)) / max(1, len(non_empty)))
    token_score = min(_header_hint_hits(non_empty) / 3.0, 1.0)
    numeric_in_header = _numeric_ratio(non_empty)
    long_text_penalty = 0.2 if any(len(v) > 40 for v in non_empty) else 0.0

    next_rows = rows[idx + 1 : idx + 4]
    if next_rows:
        next_numeric = sum(_numeric_ratio(r) for r in next_rows) / len(next_rows)
        next_fill = sum(_row_fill_ratio(r) for r in next_rows) / len(next_rows)
    else:
        next_numeric = 0.0
        next_fill = 0.0

    score = (
        0.30 * (1.0 - numeric_in_header)
        + 0.20 * unique_ratio
        + 0.30 * token_score
        + 0.20 * next_numeric
    )
    if next_fill < 0.25:
        score -= 0.15
    score -= long_text_penalty
    return score


def _detect_header_row_by_heuristic(rows: List[List[str]], max_scan_rows: int = 10) -> Tuple[int, str]:
    if not rows:
        return 0, "预览为空，回退到首行表头。"

    limit = min(max_scan_rows, len(rows))
    scored = [(idx, _score_header_candidate(rows, idx)) for idx in range(limit)]
    best_idx, best_score = max(scored, key=lambda x: x[1])
    if best_score < 0.45:
        return 0, f"启发式置信度不足(score={best_score:.2f})，回退首行。"
    return best_idx, f"启发式判断第 {best_idx} 行最像表头(score={best_score:.2f})。"


def _preview_df_to_rows(preview_df: pd.DataFrame) -> List[List[str]]:
    rows: List[List[str]] = []
    if preview_df.empty:
        return rows
    for _, row in preview_df.fillna("").iterrows():
        rows.append([_normalize_cell(v) for v in row.tolist()])
    return rows


def _load_csv_preview_rows(file_path: str, delimiter: str, max_rows: int = 30) -> Tuple[List[List[str]], str]:
    decode_errors = []
    for encoding in ("utf-8-sig", "utf-8", "gbk"):
        try:
            with open(file_path, "r", encoding=encoding, newline="") as fp:
                reader = csv.reader(fp, delimiter=delimiter)
                rows: List[List[str]] = []
                for i, row in enumerate(reader):
                    if i >= max_rows:
                        break
                    rows.append([_normalize_cell(cell) for cell in row])
            if not rows:
                return [], encoding
            max_len = max(len(r) for r in rows)
            padded = [r + [""] * (max_len - len(r)) for r in rows]
            return padded, encoding
        except UnicodeDecodeError as exc:
            decode_errors.append(f"{encoding}: {exc}")
    raise ValueError("CSV 预览解码失败: " + " | ".join(decode_errors))


def _infer_header_row_with_llm(preview_csv: str, source: str) -> Tuple[int, str]:
    llm = get_llm(temperature=0)
    header_prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
你是严谨的数据工程师。任务是识别真实表头所在行（0-indexed）。

判断原则：
1. 表头一般是字段名（短文本），而不是报表标题/说明段落。
2. 表头下一行通常开始出现业务数据（数字、日期、ID）。
3. 当不确定时返回 0（保守）。

只返回 JSON: {"row": 0, "reason": "..."}
""".strip(),
            ),
            (
                "human",
                """
数据来源: {source}
预览数据:
{csv_preview}

请返回 JSON: {"row": 0, "reason": "..."}
""".strip(),
            ),
        ]
    )
    response = (header_prompt | llm | StrOutputParser()).invoke(
        {"csv_preview": preview_csv, "source": source}
    )
    clean_resp = clean_gemini_output(response)
    json_match = re.search(r"\{.*\}", clean_resp, re.DOTALL)
    if not json_match:
        raise ValueError("LLM header response is not JSON")
    payload = json.loads(json_match.group())
    header_row = int(payload.get("row", 0))
    reason = str(payload.get("reason", "LLM 自动识别"))
    return max(0, header_row), reason


def _propose_csv_ingestion_config(file_path: str) -> FileLoadConfig:
    delimiter = detect_csv_delimiter(file_path)
    rows, encoding = _load_csv_preview_rows(file_path, delimiter=delimiter, max_rows=30)
    if rows:
        preview_csv = pd.DataFrame(rows).to_csv(index=True, header=False)
    else:
        preview_csv = ""

    header_row = 0
    reason = f"CSV 预览为空，回退首行，分隔符='{delimiter}'。"
    llm_failed = False
    if preview_csv.strip():
        try:
            header_row, llm_reason = _infer_header_row_with_llm(preview_csv, source="csv")
            if rows and header_row >= len(rows):
                raise ValueError(f"header_row out of preview range: {header_row}")
            reason = f"LLM识别表头: 第 {header_row} 行。{llm_reason} (encoding={encoding}, delimiter='{delimiter}')"
        except Exception:
            llm_failed = True

    if llm_failed:
        heur_row, heur_reason = _detect_header_row_by_heuristic(rows)
        header_row = heur_row
        reason = f"LLM不可用，{heur_reason} (encoding={encoding}, delimiter='{delimiter}')"

    return FileLoadConfig(
        file_path=file_path,
        sheet_name="__csv__",
        header_row=header_row,
        file_type="csv",
        delimiter=delimiter,
        reason=reason,
    )


def _propose_excel_ingestion_config(file_path: str) -> FileLoadConfig:
    """
    👁️ AI 观察 Excel，提出加载建议
    """

    # 1. 扫描 Sheet
    xls_file = pd.ExcelFile(file_path)
    sheet_names = xls_file.sheet_names
    
    # 2. 选择 Sheet (LLM)
    llm = get_llm(temperature=0)
    
    sheet_prompt = ChatPromptTemplate.from_messages([
        ("system", "从以下 Excel Sheet 列表中，找出最可能包含主数据的那个。排除 '封面', '说明' 等。只返回 Sheet 名称。"),
        ("human", "Sheets: {sheets}")
    ])
    
    target_sheet = (sheet_prompt | llm | StrOutputParser()).invoke({"sheets": str(sheet_names)})
    target_sheet = clean_gemini_output(target_sheet)
    
    if target_sheet not in sheet_names: 
        found = False
        for s in sheet_names:
            if target_sheet in s:
                target_sheet = s
                found = True
                break
        if not found:
            target_sheet = sheet_names[0]

    # 3. 探测 Header (读取前25行)
    df_preview = pd.read_excel(file_path, sheet_name=target_sheet, header=None, nrows=25)
    preview_rows = _preview_df_to_rows(df_preview)
    csv_preview = pd.DataFrame(preview_rows).to_csv(index=True, header=False) if preview_rows else ""
    header_row = 0
    reason = "预览为空，默认首行。"
    try:
        if csv_preview.strip():
            header_row, llm_reason = _infer_header_row_with_llm(csv_preview, source="excel")
            if preview_rows and header_row >= len(preview_rows):
                raise ValueError(f"header_row out of preview range: {header_row}")
            reason = f"LLM识别表头: 第 {header_row} 行。{llm_reason}"
        else:
            heur_row, heur_reason = _detect_header_row_by_heuristic(preview_rows)
            header_row, reason = heur_row, heur_reason
    except Exception:
        heur_row, heur_reason = _detect_header_row_by_heuristic(preview_rows)
        header_row, reason = heur_row, f"LLM不可用，{heur_reason}"

    return FileLoadConfig(
        file_path=file_path,
        sheet_name=target_sheet,
        header_row=header_row,
        file_type="excel",
        delimiter=",",
        reason=reason,
    )


def propose_ingestion_config(file_path: str) -> FileLoadConfig:
    """
    👁️ AI/规则观察文件，提出加载建议
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件未找到: {file_path}")

    file_type = detect_file_type(file_path)
    if file_type == "excel":
        return _propose_excel_ingestion_config(file_path)
    return _propose_csv_ingestion_config(file_path)


def read_csv_with_fallback(file_path: str, header_row: int, delimiter: str) -> pd.DataFrame:
    decode_errors = []
    for encoding in ("utf-8-sig", "utf-8", "gbk"):
        try:
            return pd.read_csv(
                file_path,
                header=header_row,
                encoding=encoding,
                sep=delimiter,
                low_memory=False,
            )
        except UnicodeDecodeError as exc:
            decode_errors.append(f"{encoding}: {exc}")
        except Exception as exc:
            raise ValueError(f"CSV 读取失败 ({encoding}): {exc}") from exc
    raise ValueError("CSV 解码失败: " + " | ".join(decode_errors))

def apply_ingestion(config: FileLoadConfig) -> pd.DataFrame:
    """
    🚀 执行加载
    """
    if config.file_type == "csv":
        print(
            f"   📂 [Loader] 加载参数: Type='csv', Header={config.header_row}, Delimiter='{config.delimiter}'"
        )
        df = read_csv_with_fallback(config.file_path, config.header_row, config.delimiter)
    else:
        print(f"   📂 [Loader] 加载参数: Sheet='{config.sheet_name}', Header={config.header_row}")
        df = pd.read_excel(
            config.file_path,
            sheet_name=config.sheet_name,
            header=config.header_row,
        )
    df.dropna(how='all', axis=1, inplace=True)
    df.dropna(how='all', axis=0, inplace=True)
    return df

# ==========================================
# ✅ 补回 load_file 函数 (适配 Web API)
# ==========================================
def load_file(file_path: str, display_name: str = "") -> pd.DataFrame:
    """
    [自动模式] 组合 propose 和 apply，直接加载文件。
    专门供 Server API 使用，默认采纳 AI 建议。
    """
    shown_name = display_name or os.path.basename(file_path)
    print(f"🔄 [Auto-Ingest] 正在自动分析并加载: {shown_name}")
    config = propose_ingestion_config(file_path)
    return apply_ingestion(config)
