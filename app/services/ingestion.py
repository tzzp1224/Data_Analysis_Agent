# 智能摄入层。包含清洗逻辑、决策逻辑和文件加载逻辑。
import pandas as pd
import os
import re
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from app.services.llm_factory import get_llm

def clean_gemini_output(raw_content: str) -> str:
    """
    🧹 强力清洗函数：处理 Gemini SDK 偶尔返回的对象字符串表示问题。
    针对: '{type: text, text: 目标内容, extras: {...}}' 这种格式
    """
    raw_content = str(raw_content).strip()
    
    # 1. 尝试直接匹配 'text: 内容' 这种模式 (针对 Gemini 内部对象泄漏)
    # 匹配 text: 后面直到 , extras 或者 } 结束的内容
    pattern = r"text:\s*(.*?)(?:,\s*extras|\})"
    match = re.search(pattern, raw_content, re.DOTALL)
    
    if match:
        cleaned = match.group(1).strip()
        # 再次去引号（防止提取出 'Sheet1'）
        return cleaned.replace("'", "").replace('"', "")
    
    # 2. 如果不是那种奇怪的格式，只是普通字符串，直接返回
    return raw_content.replace("'", "").replace('"', "")

def select_target_sheet(sheet_names: list[str]) -> str:
    """
    当 Excel 有多个 Sheet 时，使用 LLM 判断读取哪一个。
    """
    if len(sheet_names) == 1:
        return sheet_names[0]

    llm = get_llm(temperature=0)
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", "你是一个数据分析助手。你的任务是从 Excel 的多个工作表名称中，找出最可能包含'主要数据源'的那一个。"),
        ("human", """
        以下是一个 Excel 文件的所有 Sheet 名称列表：
        {sheet_names}
        
        请分析：
        1. 排除看起来像 "封面", "说明", "Cover", "Notes", "Sheet1"(如果有更具体的名称) 的 Sheet。
        2. 优先选择包含 "Data", "明细", "报表", "Source", "202x" 等具体业务含义的 Sheet。
        3. **你必须且只能返回一个 Sheet 名称**，不要加引号，不要加解释。
        
        返回结果：
        """)
    ])
    
    # ✅ 核心改动：加入 StrOutputParser，自动处理 AIMessage 转 String
    chain = prompt | llm | StrOutputParser()
    
    try:
        names_str = str(sheet_names)
        # invoke 后拿到的直接是 string (但可能是脏 string)
        raw_response = chain.invoke({"sheet_names": names_str})
        
        # ✅ 调用强力清洗
        target_sheet = clean_gemini_output(raw_response)
        
        print(f"🤖 (Raw: {raw_response[:20]}...) -> Cleaned: [{target_sheet}]")
        
        if target_sheet in sheet_names:
            print(f"✅ Gemini 锁定目标 Sheet: [{target_sheet}]")
            return target_sheet
        else:
            # 容错：如果 AI 返回的名字有一点偏差（比如多了空格），尝试模糊匹配
            for name in sheet_names:
                if target_sheet in name or name in target_sheet:
                    print(f"⚠️ 模糊匹配成功: '{target_sheet}' -> '{name}'")
                    return name
            
            print(f"⚠️ Gemini 返回的 '{target_sheet}' 不在列表中，回退到第一个")
            return sheet_names[0]
            
    except Exception as e:
        print(f"⚠️ Sheet 选择流程出错 ({e})，回退到第一个")
        return sheet_names[0]

def detect_header_row(df_preview: pd.DataFrame) -> int:
    """
    使用 Gemini 分析 DataFrame 的前几行，判断哪一行是真正的 Header。
    """
    llm = get_llm(temperature=0)
    
    csv_string = df_preview.to_csv(index=True)
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", "你是一个数据清洗专家。你的任务是找出 Excel 数据中真正的'列名行'(Header Row)所在的索引号。"),
        ("human", """
        以下是 Excel 文件的前 20 行数据预览（包含索引）：
        {csv_content}
        
        请分析并找出哪一行包含了数据的列名（例如：'日期', '销售额', '产品名称' 等字段描述）。
        
        **重要规则：**
        1. 如果第 0 行就是列名，返回 0。
        2. 如果第 3 行才是列名，返回 3。
        3. **你必须且只能返回一个纯数字**。不要包含任何文字。
        
        返回结果：
        """)
    ])
    
    # ✅ 核心改动：加入 StrOutputParser
    chain = prompt | llm | StrOutputParser()
    
    try:
        raw_response = chain.invoke({"csv_content": csv_string})
        
        # ✅ 调用强力清洗
        content = clean_gemini_output(raw_response)
        
        # 使用正则提取数字
        match = re.search(r'\d+', content)
        if match:
            header_index = int(match.group())
            print(f"🤖 Gemini 识别到 Header 在第 {header_index} 行")
            return header_index
        else:
            print(f"⚠️ 无法从 '{content}' 中提取行号，默认为 0")
            return 0
            
    except Exception as e:
        print(f"⚠️ Header 识别出错 ({e})，默认为 0")
        return 0

def load_file(file_path: str) -> pd.DataFrame:
    """
    智能加载文件，支持多 Sheet 选择和 Header 自动探测。
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件未找到: {file_path}")

    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    
    elif file_path.endswith(('.xls', '.xlsx')):
        print(f"📂 正在扫描 Excel 结构: {os.path.basename(file_path)}")
        
        xls_file = pd.ExcelFile(file_path)
        sheet_names = xls_file.sheet_names
        print(f"📑 发现 Sheet 列表: {sheet_names}")
        
        # 1. 智能选择 Target Sheet
        target_sheet_name = select_target_sheet(sheet_names)
        
        # 2. 在选定的 Sheet 中预读取前 20 行
        df_preview = pd.read_excel(file_path, sheet_name=target_sheet_name, header=None, nrows=20)
        
        # 3. 智能探测 Header 位置
        header_row = detect_header_row(df_preview)
        
        # 4. 读取最终数据
        df = pd.read_excel(file_path, sheet_name=target_sheet_name, header=header_row)
        
        # 5. 后处理
        df.dropna(how='all', axis=1, inplace=True)
        df.dropna(how='all', axis=0, inplace=True)
        
        # 记录元数据
        df.attrs['source_sheet'] = target_sheet_name
        
        return df
    
    else:
        raise ValueError("不支持的文件格式")