import pandas as pd
import os
import re
import json
import csv
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from app.services.llm_factory import get_llm
from pydantic import BaseModel, Field


EXCEL_EXTENSIONS = {".xlsx", ".xls", ".xlsm", ".xlsb"}
CSV_EXTENSIONS = {".csv"}


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

    # 3. 探测 Header (读取前20行)
    df_preview = pd.read_excel(file_path, sheet_name=target_sheet, header=None, nrows=20)
    csv_preview = df_preview.to_csv(index=True)
    
    header_prompt = ChatPromptTemplate.from_messages([
        ("system", """你是一个严谨的数据工程师。任务是找出 Excel 的 Header 行号。
        
        【判断规则】
        1. **默认策略**：除非第 0 行显然是“表标题”（如“2024年财务报表”这种合并单元格），或者第 0 行是空行，否则选 0。
        2. **特征识别**：真正的 Header 行通常包含："日期", "金额", "Name", "ID", "Code" 等字段名。
        3. **保守原则**：如果你犹豫不决，请返回 0。不要随意跳过行。
        
        只返回 JSON: {{ "row": 0, "reason": "..." }}
        """),
        ("human", """
        数据预览:
        {csv_preview}
        
        任务：
        1. 返回真正的 Header 行号 (0-indexed)。
        2. 给出一句话理由。
        
        只返回 JSON 格式: {{ "row": 0, "reason": "..." }}
        """)
    ])
    
    try:
        response = (header_prompt | llm | StrOutputParser()).invoke({"csv_preview": csv_preview})
        clean_resp = clean_gemini_output(response)
        json_match = re.search(r"\{.*\}", clean_resp, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            header_row = int(data.get("row", 0))
            reason = data.get("reason", "AI 自动识别")
        else:
            header_row = 0
            reason = "JSON 解析失败，默认首行"
    except Exception as e:
        print(f"Ingestion Error: {e}")
        header_row = 0
        reason = f"智能识别出错，默认首行"

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

    delimiter = detect_csv_delimiter(file_path)
    return FileLoadConfig(
        file_path=file_path,
        sheet_name="__csv__",
        header_row=0,
        file_type="csv",
        delimiter=delimiter,
        reason=f"CSV 文件采用规则模式，默认首行表头，分隔符='{delimiter}'",
    )


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
