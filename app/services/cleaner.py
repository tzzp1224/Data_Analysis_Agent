import pandas as pd
import numpy as np  # ✅ 1. 引入 numpy
import sys
import io
import re
import ast  # ✅ 2. 引入 ast 用于安全解析字符串结构
from typing import TypedDict, Annotated, List, Union
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import StateGraph, END
from app.services.llm_factory import get_llm
import operator

# ==========================================
# 0. 工具函数：清洗 Gemini 的输出
# ==========================================
def clean_code_string(raw_content: Union[str, list]) -> str:
    """
    🧹 清洗 LLM 返回的代码字符串。
    升级版：支持解析 Python 结构的字符串表示（List/Dict）。
    """
    content = raw_content
    
    # 情况 A: 如果直接就是列表（LangChain 某些版本适配行为）
    if isinstance(content, list):
        # 尝试从中提取 text 字段
        for item in content:
            if isinstance(item, dict) and 'text' in item:
                content = item['text']
                break
            # 或者是 Part 对象，尝试转字符串
            if hasattr(item, 'text'):
                content = item.text
                break
    
    # 强制转字符串进行后续处理
    content_str = str(content).strip()
    
    # 情况 B: 看起来像是 Python 的列表/字典字符串表示 "[{'type': 'text'...}]"
    if (content_str.startswith("[") and content_str.endswith("]")) or \
       (content_str.startswith("{") and "text" in content_str):
        try:
            # 使用 ast.literal_eval 安全地将字符串还原为 Python 对象
            parsed = ast.literal_eval(content_str)
            if isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], dict):
                content_str = parsed[0].get('text', content_str)
            elif isinstance(parsed, dict):
                content_str = parsed.get('text', content_str)
        except:
            # 解析失败就降级到正则处理
            pass

    # 情况 C: 正则兜底清洗 (针对漏网之鱼)
    if "text':" in content_str or 'text":' in content_str:
        # 匹配 'text': '...' 或 "text": "..."
        pattern = r"['\"]text['\"]\s*:\s*['\"](.*?)['\"](?:,\s*['\"]extras|\})"
        match = re.search(pattern, content_str, re.DOTALL)
        if match:
            content_str = match.group(1)

    # 3. 去除 Markdown 代码块标记 (这是最常见的)
    content_str = content_str.replace("```python", "").replace("```", "").strip()
    
    return content_str

# ==========================================
# 1. 定义 State (状态)
# ==========================================
class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]

# ==========================================
# 2. 辅助工具：安全的 Python 代码执行器
# ==========================================
def execute_pandas_code(df: pd.DataFrame, code: str) -> dict:
    """
    在沙箱中执行 Pandas 代码。
    """
    # ✅ 修复点：直接使用 numpy，不再依赖 pd.np
    local_vars = {"df": df.copy(), "pd": pd, "np": np}
    
    old_stdout = sys.stdout
    redirected_output = io.StringIO()
    sys.stdout = redirected_output
    
    success = False
    error_msg = ""
    
    try:
        # 清洗代码
        clean_code = clean_code_string(code)
        
        # 简单检查代码非空
        if not clean_code:
            raise ValueError("生成的代码为空，无法执行")

        # 执行
        exec(clean_code, {}, local_vars)
        success = True
        
    except Exception as e:
        error_msg = str(e)
    finally:
        sys.stdout = old_stdout
        
    output_log = redirected_output.getvalue()
    
    if success:
        return {
            "success": True,
            "new_df": local_vars["df"], 
            "log": output_log if output_log else "执行成功 (无print输出)"
        }
    else:
        return {
            "success": False,
            "new_df": df, 
            "log": f"❌ 执行报错: {error_msg}"
        }

# ==========================================
# 3. 定义 Nodes (节点)
# ==========================================

def analyst_node(state: AgentState, df_context: dict):
    """
    [思考节点] 分析数据并生成清洗代码。
    """
    df = df_context['df']
    messages = state['messages']
    
    buffer = io.StringIO()
    df.info(buf=buffer)
    info_str = buffer.getvalue()
    head_str = df.head().to_string()
    
    last_message = messages[-1] if messages else None
    error_context = "无"
    
    if isinstance(last_message, HumanMessage) and "❌" in str(last_message.content):
        error_context = last_message.content

    llm = get_llm(temperature=0)
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", """你是一个 Pandas 数据清洗专家。
你的任务是编写 Python 代码来清洗给定的 DataFrame (变量名为 `df`)。

**编写代码的严格规则：**
1. 必须且只能修改变量 `df`。例如 `df = df.dropna()` 或 `df['col'] = df['col'].fillna(0)`。
2. 不要重新读取文件 (pd.read_csv)，直接使用已有的 `df` 变量。
3. 如果涉及到字符串操作，请注意处理 NaN 的情况。
4. **只返回纯 Python 代码**，不要包含 Markdown 标记。
5. 如果你认为数据已经清洗完毕（没有缺失值），请执行: `print("CLEANING_DONE")`
"""),
        ("human", """
当前数据信息 (df.info()):
{info_str}

前 5 行预览:
{head_str}

上一轮执行反馈/错误信息:
{error_context}

请编写 Python 代码进行清洗（如填充缺失值、转换类型等）：
""")
    ])
    
    chain = prompt | llm
    
    response = chain.invoke({
        "info_str": info_str, 
        "head_str": head_str, 
        "error_context": error_context
    })
    
    return {"messages": [response]}

def execution_node(state: AgentState, df_context: dict):
    """
    [行动节点] 执行代码并反馈结果。
    """
    messages = state['messages']
    last_ai_message = messages[-1]
    
    # ✅ 清洗代码
    code = clean_code_string(last_ai_message.content)
    
    current_df = df_context['df']
    
    print(f"\n⚡ [Executor] 正在执行代码:\n{code}")
    
    result = execute_pandas_code(current_df, code)
    
    if result['success']:
        df_context['df'] = result['new_df']
        
        if "CLEANING_DONE" in result['log'] or "CLEANING_DONE" in code:
            return {"messages": [HumanMessage(content="CLEANING_DONE")]}
            
        return {"messages": [HumanMessage(content=f"✅ 执行成功。输出日志: {result['log']}")]}
    else:
        return {"messages": [HumanMessage(content=result['log'])]}

# ==========================================
# 4. 构建 Graph (图)
# ==========================================

def should_continue(state: AgentState):
    """
    [决策边]
    """
    messages = state['messages']
    last_message = messages[-1]
    
    if isinstance(last_message, HumanMessage):
        if "CLEANING_DONE" in last_message.content:
            return "end"
        return "analyze"
    
    if isinstance(last_message, AIMessage):
        return "execute"
    
    return "end"

def create_cleaning_graph():
    pass