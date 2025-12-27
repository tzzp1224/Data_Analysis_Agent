import pandas as pd
import numpy as np
import sys
import io
import re
import ast
import traceback
import json
import warnings # 
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
    import traceback
    import io
    
    # ✅ 1. 导入所有工具 (确保 tools.py 里有 smart_reconcile)
    from app.utils.tools import AuditLogger, smart_merge, smart_reconcile
    
    audit = AuditLogger()
    
    # ✅ 2. 定义包装器 (Wrappers)
    # Smart Merge 包装器
    def smart_merge_wrapper(left, right, left_on, right_on, threshold=None):
        return smart_merge(left, right, left_on, right_on, logger=audit)

    # ✅ Smart Reconcile 包装器 (关键！)
    def smart_reconcile_wrapper(df_sys, df_bank, sys_key, bank_key, sys_amount, bank_amount, tolerance=0.01):
        return smart_reconcile(df_sys, df_bank, sys_key, bank_key, sys_amount, bank_amount, tolerance, logger=audit)

    # ✅ 新增：定义还原函数
    def reload_data_wrapper(filename: str):
        backup_key = f"__backup_{filename}"
        if backup_key in dfs:
            print(f"🔄 [System] 正在还原数据: {filename}")
            # 从备份中恢复，并确保是深拷贝
            dfs[filename] = dfs[backup_key].copy(deep=True)
            return True
        else:
            print(f"❌ [System] 未找到备份数据: {filename}")
            return False
        
    # ✅ 3. 注入到局部变量
    local_vars = {
        "dfs": dfs, 
        "pd": pd, 
        "np": np, 
        "px": px, 
        "go": go,
        "audit": audit,
        "smart_merge": smart_merge_wrapper,       # L2 工具
        "smart_reconcile": smart_reconcile_wrapper, # L3 工具 (必须注入！)
        "reload_data": reload_data_wrapper
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

        # ✅ 修复点：强制注入屏蔽警告的代码，防止 SettingWithCopyWarning 污染控制台
        # 这可以防止 Agent 被无害的警告迷惑，导致死循环
        safe_code = "import warnings\nwarnings.filterwarnings('ignore')\n" + clean_code
        
        # 执行代码
        exec(safe_code, {}, local_vars)
        
        # 捕获结果
        for var_name, var_val in local_vars.items():
            if var_name.startswith("fig") and hasattr(var_val, "to_json"):
                captured_figs.append(var_val.to_json())
        
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
            "audit_logger": audit,
            "log": f"❌ Runtime Error:\n{error_trace}" # 将报错甩回给 Agent
        }
    finally:
        sys.stdout = old_stdout

# ==========================================
# 3. Nodes
# ==========================================

def supervisor_node(state: AgentState, dfs_context: dict):
    instruction = state.get("user_instruction", "")
    messages = state.get("messages", [])
    
    # ✅ 检查是否已完成
    if messages:
        last_msg = messages[-1]
        if isinstance(last_msg, HumanMessage):
             content = str(last_msg.content)
             # 只要检测到 WORKER_DONE 或 明确的分析结论，就结束
             if "WORKER_DONE" in content:
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

    # 简单的容错机制，防止 supervisor 死循环
    if len(messages) > 10:
        return {"router_decision": "end"}
    
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
        if name.startswith("__"): continue
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

    3. **重置数据**：如果用户想“重新清洗”或“还原”某张表，请执行 `reload_data('文件名')`。
      示例：`reload_data('sales_data.xlsx')`

    【核心要求】
    1. **必须导入库**：`import pandas as pd, numpy as np, re`。
    2. **类型安全 (Crucial)**：处理字符串列（如银行卡号、身份证、电话）时，**必须先转为 string**，防止数字类型报错。
       - ❌ 错误：`df['Card'].apply(lambda x: x[:4])` (如果 x 是 int 会报错)
       - ✅ 正确：`df['Card'] = df['Card'].astype(str)` 然后再处理。
       - ✅ 处理 NaN：`df['Card'] = df['Card'].fillna('')`
    3. **任务完成标志**：代码最后一行**必须**打印 `print("WORKER_DONE")`，否则系统会认为失败并重试。
    4. **禁止 Markdown**：只返回纯代码。
    5. **数据操作安全**：当对筛选后的 DataFrame 进行修改时，**必须使用 .copy()**，防止 `SettingWithCopyWarning`。
    
    【全局原则】
    1. **回写字典**：修改后的 DataFrame 必须赋值回 `dfs[name] = df`。
    2. **遍历处理**：遇到“清洗”、“检查”、“了解”指令，必须遍历所有表。
    3. **业务清洗观**：
       - 对于**明显错误**（如价格为负、数量无限大）：执行**剔除 (Drop)** 并记录。
       - 对于**逻辑冲突**（如 P*Q != Total）：**不要盲目修改数值**（因为不知道是单价错还是数量错），而是**保留原样或剔除**，并在审计日志中**详细记录**出问题的 ID 和具体数值，供人工核查。

    【能力层级更新】
    在编写代码前，严格判断用户意图属于哪一层级：
    🔍 **L1: 通用数据体检 (General Hygiene)**
       - **触发**：用户问“数据体检”、“清洗数据”、“检查异常”。
       - **策略**：
         1. **去重与空值**：这是所有表都需要的。
         2. **数值清洗**：尝试将所有“看起来像数字”的列转为数字（去除 ¥, 等符号）。
         3. **异常值检测**：
            - **负数检测**：对于名为“金额/数量/Price/Qty”的列，检测负数。
            - **极端值检测**：检测数值是否异常巨大（如 > 10万 或 > 平均值+3倍标准差）。
         4. **(可选) 逻辑检查**：**只有**当同时检测到 `单价`、`数量`、`总金额` 列时，才执行逻辑校验。
       
       - **标准代码模板 (请严格参考)**：
         ```python
         import numpy as np
         import pandas as pd
         import re
         
         for name, df in dfs.items():
             print(f"\\n### 正在分析表: {{name}}") 
             initial_count = len(df)
             
             # --- 1. 基础清洗 (去重) ---
             if df.duplicated().any():
                 dupe_count = df.duplicated().sum()
                 print(f"- 🗑️ 剔除 {{dupe_count}} 条完全重复行")
                 audit.log_exclusion(f"重复剔除-{{name}}", "完全重复行", df[df.duplicated()])
                 df = df.drop_duplicates()

             # --- 2. 智能数值转换 (针对所有列) ---
             # 自动识别可能包含数字的 Object 列
             for col in df.columns:
                 if df[col].dtype == 'object':
                     # 如果包含数字且不包含过多字母(排除ID)，尝试清洗
                     sample = str(df[col].dropna().iloc[0]) if not df[col].dropna().empty else ""
                     if re.search(r'\d', sample) and not re.search(r'[a-zA-Z]{{3,}}', sample):
                         try:
                             # 尝试去除非数字字符转换
                             cleaned = df[col].astype(str).str.replace(r'[¥,]', '', regex=True)
                             # 只有当转换成功率高时才应用，避免误伤 ID 列
                             converted = pd.to_numeric(cleaned, errors='coerce')
                             if converted.notna().sum() > 0:
                                 df[col] = converted
                         except:
                             pass

             # --- 3. 通用异常值检测 ---
             # 仅针对数值列
             num_cols = df.select_dtypes(include=[np.number]).columns
             for col in num_cols:
                 # A. 负数检测 (仅针对具备物理意义的列名)
                 if re.search(r'(金额|价|量|Amount|Price|Qty|Count)', col, re.I):
                     mask_neg = df[col] < 0
                     if mask_neg.any():
                         print(f"- ⚠️ {{col}}: 发现 {{mask_neg.sum()}} 个负数 (已记录并剔除)")
                         audit.log_exclusion(f"负数异常-{{name}}", f"{{col}} 为负数", df[mask_neg])
                         df = df[~mask_neg]
                 
                 # B. 极端值检测 (简单阈值法，比如 > 100000，或者根据分位数)
                 # 这里使用绝对阈值示例，防止统计学误伤小样本
                 # 仅检测“数量”或“金额”相关
                 if re.search(r'(Qty|Count|数量)', col, re.I):
                     mask_huge = df[col] > 100000
                     if mask_huge.any():
                         print(f"- ⚠️ {{col}}: 发现 {{mask_huge.sum()}} 个极端大值 (已记录并剔除)")
                         audit.log_exclusion(f"极端值-{{name}}", f"{{col}} 过大", df[mask_huge])
                         df = df[~mask_huge]

             # --- 4. 逻辑一致性 (防御性执行) ---
             # 只有列名完全匹配时才执行，避免误伤
             p_col = next((c for c in df.columns if re.search(r'(单价|Price)', c, re.I)), None)
             q_col = next((c for c in df.columns if re.search(r'(数量|Qty)', c, re.I)), None)
             t_col = next((c for c in df.columns if re.search(r'(总金额|Total|Amount)', c, re.I)), None)
             
             if p_col and q_col and t_col:
                 try:
                     expected = df[p_col] * df[q_col]
                     mask_logic = abs(expected - df[t_col]) > 1.0 # 容差 1.0
                     if mask_logic.any():
                         print(f"- ⚠️ 发现 {{mask_logic.sum()}} 条金额逻辑不符 (已记录)")
                         # 这里我们只记录 Audit，不一定强制剔除，由用户决定，或者剔除
                         audit.log_exclusion(f"逻辑校验失败-{{name}}", "计算逻辑不符", df[mask_logic])
                         df = df[~mask_logic]
                 except:
                     pass # 如果列之间无法计算，跳过

             # --- 5. 保存 ---
             dfs[name] = df
             print(f"- 处理后: {{len(df)}} 行")
         
         result_df = list(dfs.values())[0]
         print("WORKER_DONE")
         ```
       
    🔗 **L2: 多表关联与整合 (Integration)**
       - **触发**：用户明确说了“合并”、“连接”、“关联表A和表B”。
       - **工具**：只有此时才允许使用 `pd.merge` (标准Key) 或 `smart_merge` (模糊Key)。
       - **智能工具**：遇到 Key 列不一致（如中英文、别名、简称），**必须使用 `smart_merge`**。
       - **能力增强**：该工具已集成 **语义向量模型 (Sentence-BERT)**，可以识别 'Tencent' <-> '腾讯', '今日头条' <-> '字节跳动' 等复杂关系，无需人工干预。
       - **代码示例**: `result_df = smart_merge(sales_df, client_df, left_on='客户名称', right_on='标准公司名')`

    💰 **L3: 财务对账 (Financial Reconciliation)**
       - **触发**：用户明确说了“对账”、“核对流水”、“找两表差异”。
       - **工具**：只有此时才允许使用 `smart_reconcile`。
       - **核心工具**：使用 `smart_reconcile(df1, df2, key1, key2, amt1, amt2, tolerance=0.05)`。
       - **多对一问题 (Many-to-One)**：
         - 如果用户提到“多笔订单合并支付”或“系统多条对应银行一条”，**必须先聚合数据**！
         - 示例：`df_sys_grouped = df_sys.groupby('外部流水号')['应收金额'].sum().reset_index()`
         - 然后再拿聚合后的 `df_sys_grouped` 去和银行表 `smart_reconcile`。
         **重置索引 (非常重要)**：`df_agg = df_agg.reset_index()`。
         - ❌ 错误：直接把 GroupBy 后的 Series 传给工具。
         - ✅ 正确：必须传 DataFrame，且 Key 必须是列名。
         在进行 groupby 聚合后，必须 立即调用 .reset_index()，并打印 df.columns 确认列名存在，然后再传入 smart_reconcile 工具。
       - **容差 (Tolerance)**：默认容差为 0.01。如果用户说“忽略 5 元以内差异”，请设置 `tolerance=5`。
       
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
    5. **“模糊查询”规范**：“当用户查询某个实体（如 'Tencent'）但数据表中可能存储为中文或别名时，不要直接用 ==。请使用 df['列'].str.contains('腾讯|Tencent', case=False)，
        或者先调用 vector_match('Tencent', df['列'].unique()) (如果你想做得更高级)。”
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
        
        if result['result_df'] is not None:
            dfs_context['__last_result_df__'] = result['result_df']
            if result.get('audit_logger'):
                dfs_context['__last_audit__'] = result['audit_logger']
        
        # 即使没有显式 print WORKER_DONE，如果没报错，我们也尝试追加标志
        log = result['log']
        if "WORKER_DONE" in log or "WORKER_DONE" in code:
             updates["messages"] = [HumanMessage(content=f"✅ 成功:\n{log}\n(Signal: WORKER_DONE)")]
        else:
             # 如果没有 done，但也没错，可能是忘了打印。
             updates["messages"] = [HumanMessage(content=f"✅ 成功 (未检测到结束信号):\n{log}")]
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
    # ✅ 修复点：如果既没报错，又没DONE，不要死循环回 Worker。
    # 而是回到 Supervisor，让 LLM 决定是继续还是结束（通常 LLM 看到 log 会觉得完成了）
    return "supervisor"

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
    # 路由逻辑修正
    workflow.add_conditional_edges("executor", executor_router, {
        "retry": "python_worker", 
        "end": END, 
        "supervisor": "supervisor" # 避免死循环
    })
    workflow.add_edge("general_chat", END)
    return workflow.compile()