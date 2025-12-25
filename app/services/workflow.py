import pandas as pd
import numpy as np
import sys
import io
import re
import ast
import traceback
import json
from typing import TypedDict, Annotated, List, Literal, Optional, Union, Dict, Any
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
    """清洗代码"""
    content = raw_content
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
    
    content_str = str(content).strip()
    # 处理 repr 字符串
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
    # ✅ 新增：用于传递生成的 Excel 数据对象 (不直接存 DF，而是存标记，实际数据在 context 中流转)
    # 这里我们简化：数据通过 return 字典传回，在 main 中处理
    reply: str

# ==========================================
# 2. 代码执行器 (支持 result_df 捕获)
# ==========================================
def execute_code(dfs: Dict[str, pd.DataFrame], code: str) -> dict:
    import plotly.graph_objects as go
    import plotly.express as px
    
    local_vars = {"dfs": dfs, "pd": pd, "np": np, "px": px, "go": go}
    if len(dfs) > 0:
        local_vars['df'] = dfs[list(dfs.keys())[0]]

    old_stdout = sys.stdout
    redirected_output = io.StringIO()
    sys.stdout = redirected_output
    
    captured_figs = []
    generated_df = None # 用于存储 result_df
    
    try:
        clean_code = clean_code_string(code)
        if not clean_code: 
            return {"success": True, "dfs": dfs, "chart_jsons": [], "log": "无代码", "result_df": None}

        exec(clean_code, {}, local_vars)
        
        # 1. 捕获图表
        for var_name, var_val in local_vars.items():
            if var_name.startswith("fig") and hasattr(var_val, "to_json"):
                captured_figs.append(var_val.to_json())
        
        # 2. ✅ 核心升级：捕获 result_df
        # 如果 LLM 生成了 result_df，说明它想输出文件
        if "result_df" in local_vars:
            obj = local_vars["result_df"]
            if isinstance(obj, pd.DataFrame):
                print("💾 [System] 捕获到结果数据: result_df")
                generated_df = obj
        
        return {
            "success": True,
            "dfs": local_vars["dfs"],
            "chart_jsons": captured_figs,
            "result_df": generated_df, # 返回这个对象
            "log": redirected_output.getvalue()
        }
    except Exception:
        error_trace = traceback.format_exc()
        return {
            "success": False,
            "dfs": dfs,
            "chart_jsons": [],
            "result_df": None,
            "log": f"❌ Runtime Error:\n{error_trace}"
        }
    finally:
        sys.stdout = old_stdout

# ==========================================
# 3. Nodes
# ==========================================

def supervisor_node(state: AgentState, dfs_context: dict):
    instruction = state.get("user_instruction", "")
    messages = state.get("messages", [])
    
    if messages:
        last_msg = messages[-1]
        if isinstance(last_msg, HumanMessage) and "WORKER_DONE" in str(last_msg.content):
            return {"router_decision": "end"}

    if not instruction and len(messages) == 0:
        return {"router_decision": "auto_eda"}
        
    llm = get_llm(temperature=0)
    file_list_str = ", ".join(dfs_context.keys())
    
    system_prompt = """你是一个数据操作系统的指挥官。
    当前文件: [{file_list}]
    
    根据指令决定：
    1. 'python_worker': 需要操作数据（合并、筛选、计算、画图、输出新表格）。
    2. 'general_chat': 无关指令。
    3. 'end': 结束。
    
    返回 JSON: {{ "decision": "...", "reason": "..." }}
    """
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "指令: {instruction}\n历史: {history}")
    ])
    
    chain = prompt | llm | StrOutputParser()
    response = chain.invoke({
        "instruction": str(instruction), 
        "history": str(messages[-2:]),
        "file_list": file_list_str
    })
    
    try:
        import json
        clean_resp = clean_code_string(response)
        json_match = re.search(r"\{.*\}", clean_resp, re.DOTALL)
        if json_match: clean_resp = json_match.group()
        res_json = json.loads(clean_resp)
        decision = res_json.get("decision", "general_chat")
        
        if decision == "python_worker": return {"router_decision": "python_worker"}
        if decision == "general_chat": 
            return {"router_decision": "general_chat", "reply": res_json.get("reason", "无法处理")}
        return {"router_decision": "end"}
    except:
        return {"router_decision": "general_chat", "reply": "指令解析失败。"}

def general_chat_node(state: AgentState):
    return {"messages": [AIMessage(content=state.get("reply", "无法处理。"))]}

def python_worker_node(state: AgentState, dfs_context: dict, mode: str = "custom"):
    dfs = dfs_context
    messages = state['messages']
    instruction = state.get('user_instruction', '')
    
    schema_info = ""
    for name, df in dfs.items():
        buffer = io.StringIO()
        df.info(buf=buffer)
        schema_info += f"\nFile: {name}\n{buffer.getvalue()}\nHead:\n{df.head().to_string()}\n"
    
    last_message = messages[-1] if messages else None
    error_context = ""
    if isinstance(last_message, HumanMessage) and "❌ Runtime Error" in str(last_message.content):
        error_context = f"⚠️ 上一次报错:\n{last_message.content}"
    
    llm = get_llm(temperature=0)
    
    if mode == "auto_eda":
        system_instructions = """
        用户未输入指令。请进行 Auto EDA。
        要求：
        1. 使用 plotly (px) 画两张图，赋值给 fig1, fig2。
        2. 打印 "WORKER_DONE"。
        """
        instruction = "Auto EDA"
    else:
        # ✅ 核心 Prompt 修改：强调数据处理和 result_df
        system_instructions = """
        你是一个 Python 数据处理专家。
        可以通过 `dfs['filename']` 访问数据。
        
        【核心规则】
        1. **数据操作（合并/筛选/计算）：** 如果你生成了一个新的 DataFrame 作为最终结果（例如：合并后的表、筛选出的子表），
           **必须**将其赋值给变量 `result_df`。
           例如：`result_df = pd.merge(...)` 或 `result_df = df[df['id']=='P001']`。
           
        2. **画图：** 使用 plotly.express，赋值给 `fig`。
        
        3. **禁止：** - 不要使用 `to_excel` 或 `to_csv` 保存文件（由系统接管）。
           - 不要使用 `read_excel` (直接从 dfs 读取)。
           
        4. **结束：** 打印 "WORKER_DONE"。
        """

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_instructions + "\n只返回 Python 代码。"),
        ("human", "数据上下文:\n{schema}\n\n指令: {instruction}\n错误: {error_context}")
    ])
    
    response = (prompt | llm).invoke({
        "schema": schema_info,
        "instruction": instruction,
        "error_context": error_context
    })
    return {"messages": [response]}

def executor_node(state: AgentState, dfs_context: dict):
    messages = state['messages']
    code = messages[-1].content
    print(f"\n⚡ 执行代码:\n{clean_code_string(code)[:80]}...")
    
    result = execute_code(dfs_context, code)
    
    updates = {}
    if result['success']:
        updates["error_count"] = 0
        if result['chart_jsons']:
            updates["chart_jsons"] = result['chart_jsons']
        
        # ✅ 处理结果数据
        if result['result_df'] is not None:
            # 我们将结果 DF 暂存入 context 的一个特殊 key，或者通过 updates 返回
            # 为了简单，我们在 main.py 里通过监听 updates 拿不到对象（State不能存DF）
            # 所以我们把 result_df 放入 dfs_context 的一个特殊槽位，供 Main 读取
            dfs_context['__last_result_df__'] = result['result_df']
            
            # 并在消息里标记，通知前端
            log = result['log'] + "\n[System] 已生成结果表格 (result_df)，准备导出。"
        else:
            log = result['log']
            
        if "WORKER_DONE" in log or "WORKER_DONE" in code:
             updates["messages"] = [HumanMessage(content=f"✅ 成功:\n{log}\n(Signal: WORKER_DONE)")]
        else:
             updates["messages"] = [HumanMessage(content=f"✅ 成功:\n{log}")]
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
    last_content = str(messages[-1].content)
    if "❌ Runtime Error" in last_content: return "retry"
    if "WORKER_DONE" in last_content: return "end"
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
    workflow.add_conditional_edges("supervisor", router_logic, {"python_worker": "python_worker", "auto_eda": "auto_eda", "general_chat": "general_chat", END: END})
    workflow.add_edge("auto_eda", "executor")
    workflow.add_edge("python_worker", "executor")
    workflow.add_conditional_edges("executor", executor_router, {"retry": "python_worker", "end": END, "continue": "python_worker", "supervisor": "supervisor"})
    workflow.add_edge("general_chat", END)
    return workflow.compile()