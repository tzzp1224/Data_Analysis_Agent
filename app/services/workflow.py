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
from app.utils.tools import AuditLogger, smart_merge

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
    
    # ✅ 1. 初始化审计记录器
    from app.utils.tools import AuditLogger, smart_merge # 确保导入
    audit = AuditLogger()
    
    # ✅ 2. 注入工具到局部变量
    # 修复点：包装器不再传递 threshold 参数，以匹配 tools.py 的新定义
    def smart_merge_wrapper(left, right, left_on, right_on, threshold=None):
        # 注意：为了兼容 Agent 可能会瞎传 threshold 参数的习惯，
        # 我们在 wrapper 定义里保留 threshold=None，但在调用真实函数时 **丢弃它**。
        return smart_merge(left, right, left_on, right_on, logger=audit)

    local_vars = {
        "dfs": dfs, 
        "pd": pd, 
        "np": np, 
        "px": px, 
        "go": go,
        "audit": audit,
        "smart_merge": smart_merge_wrapper # 使用修复后的包装器
    }
    
    if len(dfs) > 0:
        local_vars['df'] = dfs[list(dfs.keys())[0]]

    old_stdout = sys.stdout
    redirected_output = io.StringIO()
    sys.stdout = redirected_output
    
    captured_figs = []
    generated_df = None
    
    try:
        clean_code = clean_code_string(code)
        if not clean_code: 
            return {"success": True, "dfs": dfs, "chart_jsons": [], "log": "无代码", "result_df": None, "audit_logger": audit}

        exec(clean_code, {}, local_vars)
        
        # 1. 捕获图表
        for var_name, var_val in local_vars.items():
            if var_name.startswith("fig") and hasattr(var_val, "to_json"):
                captured_figs.append(var_val.to_json())
        
        # 2. 捕获 result_df
        if "result_df" in local_vars:
            obj = local_vars["result_df"]
            if isinstance(obj, pd.DataFrame):
                print("💾 [System] 捕获到结果数据: result_df")
                generated_df = obj
        
        return {
            "success": True,
            "dfs": local_vars["dfs"],
            "chart_jsons": captured_figs,
            "result_df": generated_df,
            "audit_logger": audit, 
            "log": redirected_output.getvalue()
        }
    except Exception:
        error_trace = traceback.format_exc()
        return {
            "success": False,
            "dfs": dfs,
            "chart_jsons": [],
            "result_df": None,
            "audit_logger": audit, # 即使失败也返回 log
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
    """
    全能型 Python 代码生成节点。
    
    Args:
        state: LangGraph 状态
        dfs_context: 包含所有 DataFrame 的字典 {'filename': df}
        mode: 'custom' (响应用户指令) | 'auto_eda' (自动探索)
    """
    dfs = dfs_context
    messages = state['messages']
    instruction = state.get('user_instruction', '')
    
    # ---------------------------------------------------------
    # 1. 构建数据全景 (Schema Context)
    # ---------------------------------------------------------
    # 我们只给 LLM 看列名、类型和前5行，绝不传输全量数据，节省 Token
    schema_info = ""
    for name, df in dfs.items():
        buffer = io.StringIO()
        df.info(buf=buffer)
        info_str = buffer.getvalue()
        head_str = df.head().to_string()
        schema_info += f"\n=== File: {name} ===\n[Info]:\n{info_str}\n[Head (First 5 rows)]:\n{head_str}\n"
    
    # ---------------------------------------------------------
    # 2. 获取错误上下文 (Self-Healing)
    # ---------------------------------------------------------
    # 检查上一条消息是否是 Executor 返回的报错
    last_message = messages[-1] if messages else None
    error_context = "无"
    if isinstance(last_message, HumanMessage) and "❌ Runtime Error" in str(last_message.content):
        error_context = f"⚠️ 上一次代码执行报错，请根据以下 Traceback 修正代码:\n{last_message.content}"
    
    # ---------------------------------------------------------
    # 3. 定义核心 System Prompt (植入四大层级能力)
    # ---------------------------------------------------------
    llm = get_llm(temperature=0)
    
    system_instructions = """
    你是一个全能型 Python 数据分析专家。你拥有对 `dfs` 字典的完全访问权限，其中包含了用户上传的所有数据表。
    
    【强大的内置工具 (Built-in Tools)】
    你拥有以下特殊对象和函数，**请务必使用它们**来增强代码的健壮性和可信度：

    1. **`audit` (审计记录器)**:
       - 每当你执行数据清洗（删除行、填充空值）或关键计算时，**必须**记录日志。
       - 用法1 (普通操作): `audit.info("清洗步骤", "删除了空值行", affected_rows=5)`
       - 用法2 (剔除数据): `audit.log_exclusion("异常剔除", "销售额为负数的行", excluded_df)`
       - **原则：不要只默默做事，要留痕！**

    2. **`smart_merge` (智能模糊关联)**:
       - 当你需要合并两张表，但怀疑 Key 列（如公司名、人名）可能存在拼写不一致时（如 '腾讯' vs '腾讯科技'），**不要用 `pd.merge`**。
       - **请使用**: `result_df = smart_merge(df1, df2, left_on='name', right_on='comp_name')`
       - 它会自动处理模糊匹配并记录日志。

    【能力层级更新】
    🔍 **L1: 数据清洗**
       - 遇到异常值，先筛选出来：`bad_rows = df[df['age'] < 0]`
       - 记录审计：`audit.log_exclusion("年龄清洗", "剔除负数年龄", bad_rows)`
       - 然后剔除：`df = df[df['age'] >= 0]`
       - **重要技巧**：在筛选子集后如果需要修改数据，请务必使用 `.copy()`，例如 `df_clean = df[df['val']>0].copy()`，以避免 SettingWithCopyWarning。
       
    🔗 **L2: 多表关联与整合 (Integration)**
       - **智能工具**：遇到 Key 列不一致（如中英文、别名、简称），**必须使用 `smart_merge`**。
       - **能力增强**：该工具已集成 **语义向量模型 (Sentence-BERT)**，可以识别 'Tencent' <-> '腾讯', '今日头条' <-> '字节跳动' 等复杂关系，无需人工干预。
       - **代码示例**: `result_df = smart_merge(sales_df, client_df, left_on='客户名称', right_on='标准公司名')`
       
    📊 **L3: 统计与透视 (Analysis)**
       - **聚合**：使用 `groupby`, `pivot_table` 进行多维度汇总。
       - **计算**：计算占比、增长率、统计分布。
       
    📈 **L4: 可视化与交付 (Delivery)**
       - **文件交付 (严格限制)**：
         - 只有当用户**明确要求**“导出”、“保存”、“下载”、“生成新表”或“输出文件”时，才将结果 DataFrame 赋值给变量 `result_df`。
         - 如果用户只是问“是什么”、“分析一下”、“统计一下”，**不要**赋值给 `result_df`，直接 `print` 打印结果即可。
       - **可视化**：使用 `plotly.express` (px) 绘制交互式图表，并将图表对象赋值给 `fig` (或 fig1, fig2)。
    
    【输出规范 - 非常重要】
    你的代码输出必须包含以下三部分（通过 `print` 输出）：
    1. **# PLAN**: 简单注释，说明你打算做什么。
    2. **# CODE**: 执行的具体代码。
    3. **# INSIGHTS**: **(核心要求)** 代码执行完后，必须使用 `print` 输出一段**自然语言的分析结论**。
       - 如果是画图，请解释图表展示了什么趋势（例如：“从图表可见，P001销量在5月达到顶峰...”）。
       - 如果是数据处理，请汇报处理结果（例如：“已成功合并两张表，共生成 500 行数据...”）。
       - 不要只给冷冰冰的数字或图表，要给“洞察”。

    【代码编写规范】
    1. **数据访问**：直接使用 `dfs['filename']` 读取数据。**严禁**使用 `pd.read_excel` 或 `pd.read_csv`。
    2. **可解释性**：在编写代码前，必须先写一段 Python 注释 (`# PLAN: ...`)，用自然语言解释你的解题思路。
    3. **结束信号**：任务完成后，必须打印 `print("WORKER_DONE")`。
    4. **禁止**：禁止使用 `to_excel` 保存文件（系统会自动接管 `result_df` 进行保存）。禁止使用 `plt.show()`。
    """
    
    # ---------------------------------------------------------
    # 4. 根据模式调整指令
    # ---------------------------------------------------------
    if mode == "auto_eda":
        # 覆盖用户指令，强制执行 EDA
        specific_task = """
        【当前任务：自动 EDA】
        用户未输入指令。请对数据进行基础概览：
        1. 打印每个表的基本形状和缺失值统计。
        2. 挑选最有分析价值的数值列或分类列，使用 Plotly 绘制 **至少两张** 图表 (赋值给 fig1, fig2)。
        3. 打印 "WORKER_DONE"。
        """
        instruction_to_send = "请进行自动 EDA 分析。"
    else:
        # 正常响应用户指令
        specific_task = f"""
        【当前任务】
        用户指令: {instruction}
        请根据指令逻辑，编写相应的 Pandas/Plotly 代码。
        如果涉及文件输出，记得赋值给 `result_df`。
        """
        instruction_to_send = instruction

    # ---------------------------------------------------------
    # 5. 组装 Prompt 并调用
    # ---------------------------------------------------------
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_instructions + "\n" + specific_task + "\n请只返回纯 Python 代码，不要包含 Markdown 标记 (```python)。"),
        ("human", """
        【数据全景 (Schema)】
        {schema}
        
        【用户指令】
        {instruction}
        
        【错误反馈 (Self-Correction)】
        {error_context}
        """)
    ])
    
    # 调用 LLM
    response = (prompt | llm).invoke({
        "schema": schema_info,
        "instruction": instruction_to_send,
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

            if result.get('audit_logger'):
                dfs_context['__last_audit__'] = result['audit_logger']
                
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