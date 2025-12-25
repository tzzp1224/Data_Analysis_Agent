import pandas as pd
import numpy as np
import sys
import io
import re
import ast
import traceback
import json
from typing import TypedDict, Annotated, List, Literal, Optional, Union, Dict
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langgraph.graph import StateGraph, END
from app.services.llm_factory import get_llm
import operator

# ==========================================
# 0. 基础工具
# ==========================================
def clean_code_string(raw_content: Union[str, list, dict]) -> str:
    """清洗代码，适配 Gemini 的各种返回格式"""
    content = raw_content
    # 处理 Gemini 可能返回的 list[part]
    if isinstance(content, list):
        text_parts = []
        for part in content:
            if isinstance(part, dict) and 'text' in part:
                text_parts.append(part['text'])
            elif hasattr(part, 'text'):
                text_parts.append(part.text)
            elif isinstance(part, str):
                text_parts.append(part)
        content = "\n".join(text_parts)
    
    # 尝试解析 repr 字符串
    content_str = str(content).strip()
    if (content_str.startswith("[") and content_str.endswith("]")) or \
       (content_str.startswith("{") and "text" in content_str):
        try:
            parsed = ast.literal_eval(content_str)
            if isinstance(parsed, list) and len(parsed) > 0:
                return clean_code_string(parsed)
            elif isinstance(parsed, dict):
                return clean_code_string(parsed.get('text', ''))
        except:
            pass

    # 正则兜底
    if "text:" in content_str:
        pattern = r"text:\s*(.*?)(?:,\s*extras|\})"
        match = re.search(pattern, content_str, re.DOTALL)
        if match:
            content_str = match.group(1).strip().strip("'").strip('"')

    content_str = content_str.replace("```python", "").replace("```json", "").replace("```", "").strip()
    return content_str

# ==========================================
# 1. 定义 State
# ==========================================
class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]
    user_instruction: str
    router_decision: str
    error_count: int
    chart_jsons: Annotated[List[str], operator.add]
    reply: str

# ==========================================
# 2. 多文件代码执行器
# ==========================================
def execute_code(dfs: Dict[str, pd.DataFrame], code: str) -> dict:
    import plotly.graph_objects as go
    import plotly.express as px
    
    local_vars = {"dfs": dfs, "pd": pd, "np": np, "px": px, "go": go}
    
    # 为了兼容旧习惯，如果只有一个文件，也注入 df
    if len(dfs) > 0:
        first_key = list(dfs.keys())[0]
        local_vars['df'] = dfs[first_key]

    old_stdout = sys.stdout
    redirected_output = io.StringIO()
    sys.stdout = redirected_output
    
    captured_figs = []
    
    try:
        clean_code = clean_code_string(code)
        # 简单检查防止空代码执行
        if not clean_code: 
            return {"success": True, "dfs": dfs, "chart_jsons": [], "log": "无代码需要执行"}

        exec(clean_code, {}, local_vars)
        
        # 捕获图表
        for var_name, var_val in local_vars.items():
            if var_name.startswith("fig"): # 约定图表变量名以 fig 开头
                if hasattr(var_val, "to_json"):
                    print(f"📊 [System] 捕获图表对象: {var_name}")
                    captured_figs.append(var_val.to_json())
        
        return {
            "success": True,
            "dfs": local_vars["dfs"],
            "chart_jsons": captured_figs,
            "log": redirected_output.getvalue()
        }
    except Exception:
        error_trace = traceback.format_exc()
        return {
            "success": False,
            "dfs": dfs,
            "chart_jsons": [],
            "log": f"❌ Runtime Error:\n{error_trace}"
        }
    finally:
        sys.stdout = old_stdout

# ==========================================
# 3. Nodes (节点)
# ==========================================

def supervisor_node(state: AgentState, dfs_context: dict):
    """大脑节点"""
    instruction = state.get("user_instruction", "")
    messages = state.get("messages", [])
    
    # 检查任务是否完成
    if messages:
        last_msg = messages[-1]
        if isinstance(last_msg, HumanMessage) and "WORKER_DONE" in str(last_msg.content):
            return {"router_decision": "end"}

    # 默认 EDA
    if not instruction and len(messages) == 0:
        return {"router_decision": "auto_eda"}
        
    llm = get_llm(temperature=0)
    
    # 构建文件列表字符串
    file_list_str = ", ".join(dfs_context.keys())
    
    # ✅ 修复点 1: 移除 f-string，改用 Prompt Template 变量传递
    # ✅ 修复点 2: JSON 的大括号必须用 {{ }} 转义
    system_prompt = """你是一个高级数据分析系统的指挥官。
    当前已加载的文件: [{file_list}]
    
    你需要分析用户的自然语言指令，决定下一步操作：
    
    1. 'python_worker': 当用户想要对数据进行操作时（如：合并表格、画图、清洗、统计分析）。
    2. 'general_chat': 当用户的指令与数据分析完全无关（如：“讲个笑话”），或者无法实现时。
       在此模式下，拒绝并解释原因。
    3. 'end': 任务结束。
    
    只返回 JSON 格式： {{ "decision": "...", "reason": "..." }}
    """
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "用户指令: {instruction}\n近期历史: {history}")
    ])
    
    history_summary = messages[-2:] if len(messages) > 2 else messages
    
    # ✅ 修复点 3: 在 invoke 中传入 file_list
    chain = prompt | llm | StrOutputParser()
    response = chain.invoke({
        "instruction": str(instruction), 
        "history": str(history_summary),
        "file_list": file_list_str
    })
    
    try:
        import json
        clean_resp = clean_code_string(response)
        # 尝试提取 json 部分 (防止 LLM 废话)
        json_match = re.search(r"\{.*\}", clean_resp, re.DOTALL)
        if json_match:
            clean_resp = json_match.group()
            
        res_json = json.loads(clean_resp)
        decision = res_json.get("decision", "general_chat")
        
        if decision == "python_worker": return {"router_decision": "python_worker"}
        if decision == "general_chat": 
            return {"router_decision": "general_chat", "reply": res_json.get("reason", "无法处理该请求")}
            
        return {"router_decision": "end"}
        
    except Exception as e:
        print(f"Supervisor JSON Parse Error: {e}, Raw: {response}")
        return {"router_decision": "general_chat", "reply": "指令解析失败，请重试。"}

def general_chat_node(state: AgentState):
    reply = state.get("reply", "我只能处理数据分析相关的请求。")
    return {"messages": [AIMessage(content=reply)]}

def python_worker_node(state: AgentState, dfs_context: dict, mode: str = "custom"):
    dfs = dfs_context
    messages = state['messages']
    instruction = state.get('user_instruction', '')
    
    # 构建 Schema
    schema_info = ""
    for name, df in dfs.items():
        buffer = io.StringIO()
        df.info(buf=buffer)
        schema_info += f"\n--- File: {name} ---\n{buffer.getvalue()}\nHead:\n{df.head().to_string()}\n"
    
    last_message = messages[-1] if messages else None
    error_context = ""
    if isinstance(last_message, HumanMessage) and "❌ Runtime Error" in str(last_message.content):
        error_context = f"⚠️ 上一次代码报错:\n{last_message.content}"
    
    llm = get_llm(temperature=0)
    
    if mode == "auto_eda":
        system_instructions = """
        你是一个自动化 EDA 专家。
        用户上传了文件但未给出指令。请编写代码对数据进行基础概览。
        
        要求：
        1. 使用 `dfs['filename']` 读取数据。**不要使用 pd.read_excel**。
        2. 使用 Plotly (px) 绘制 **至少两张** 图表，赋值给 `fig1`, `fig2`。
        3. 打印 "WORKER_DONE" 结束。
        """
        instruction = "请进行自动 EDA 分析，生成多维度图表。"
    else:
        system_instructions = """
        你是一个 Python 数据分析专家。
        你可以通过字典 `dfs` 访问所有数据，例如 `dfs['sales.xlsx']`。
        **不要使用 pd.read_excel / pd.read_csv 读取文件，因为数据已经在内存的 `dfs` 变量中了。**
        
        要求：
        1. 根据用户指令编写 Pandas/Plotly 代码。
        2. 如果需要合并表格，请使用 `pd.merge`。
        3. 画图请使用 `plotly.express` (px) 并将对象赋值给 `fig` (或 fig1, fig2)。
        4. **不要**使用 `plt.show()` 或 `fig.show()`。
        5. 任务完成后打印 "WORKER_DONE"。
        """

    # ✅ 修复点 4: 把 schema_info 作为变量传递，而不是 f-string 注入
    # 这样可以防止 schema_info 里的 {} 干扰 Prompt Template
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_instructions + "\n只返回纯 Python 代码。"),
        ("human", """
        可用数据上下文:
        {schema}
        
        用户指令: {instruction}
        错误上下文: {error_context}
        """)
    ])
    
    response = (prompt | llm).invoke({
        "schema": schema_info,
        "instruction": instruction,
        "error_context": error_context
    })
    return {"messages": [response]}

def executor_node(state: AgentState, dfs_context: dict):
    """执行节点"""
    messages = state['messages']
    last_ai_msg = messages[-1]
    code = last_ai_msg.content
    
    print(f"\n⚡ 执行代码:\n{clean_code_string(code)[:100]}...")
    
    result = execute_code(dfs_context, code)
    
    updates = {}
    if result['success']:
        updates["error_count"] = 0
        if result['chart_jsons']:
            updates["chart_jsons"] = result['chart_jsons']
            
        log = result['log']
        if "WORKER_DONE" in log or "WORKER_DONE" in code:
             updates["messages"] = [HumanMessage(content=f"✅ 执行成功:\n{log}\n(Signal: WORKER_DONE)")]
        else:
             updates["messages"] = [HumanMessage(content=f"✅ 执行成功:\n{log}")]
    else:
        updates["messages"] = [HumanMessage(content=result['log'])]
        updates["error_count"] = state.get("error_count", 0) + 1
        
    return updates

# ==========================================
# 4. 构建 Graph
# ==========================================
def router_logic(state: AgentState):
    decision = state.get("router_decision")
    error_count = state.get("error_count", 0)
    messages = state.get("messages", [])
    
    if messages and error_count > 0:
        if error_count > 3: return END
        return 'python_worker'

    if decision == 'python_worker': return 'python_worker'
    if decision == 'auto_eda': return 'auto_eda'
    if decision == 'general_chat': return 'general_chat'
    
    return END

def executor_router(state: AgentState):
    messages = state.get("messages", [])
    if not messages: return "supervisor"
        
    last_msg = messages[-1]
    content = str(last_msg.content)
    
    if "❌ Runtime Error" in content:
        return "retry"
        
    if "WORKER_DONE" in content:
        return "end"
        
    return "continue"

def create_workflow(dfs_context: dict):
    from functools import partial
    
    workflow = StateGraph(AgentState)
    
    workflow.add_node("supervisor", partial(supervisor_node, dfs_context=dfs_context))
    workflow.add_node("general_chat", general_chat_node)
    
    workflow.add_node("python_worker", partial(python_worker_node, dfs_context=dfs_context, mode='custom'))
    workflow.add_node("auto_eda", partial(python_worker_node, dfs_context=dfs_context, mode='auto_eda'))
    
    workflow.add_node("executor", partial(executor_node, dfs_context=dfs_context))
    
    workflow.set_entry_point("supervisor")
    
    workflow.add_conditional_edges(
        "supervisor",
        router_logic,
        {
            "python_worker": "python_worker",
            "auto_eda": "auto_eda",
            "general_chat": "general_chat",
            END: END
        }
    )
    
    workflow.add_edge("auto_eda", "executor")
    workflow.add_edge("python_worker", "executor")
    
    workflow.add_conditional_edges(
        "executor",
        executor_router,
        {
            "retry": "python_worker", 
            "end": END,
            "continue": "python_worker",
            "supervisor": "supervisor"
        }
    )
    
    workflow.add_edge("general_chat", END)
    
    return workflow.compile()