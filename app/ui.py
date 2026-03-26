import streamlit as st
import requests
import pandas as pd
import plotly.io as pio
import uuid
import time
import json

# ==========================================
# 🎨 1. 页面配置与 CSS 美化
# ==========================================
st.set_page_config(
    page_title="Agentic Finance | 智能财务对账系统",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 后端 API 地址
API_URL = "http://localhost:8000"

# 初始化 Session State
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    st.session_state.messages = []
if "files_uploaded" not in st.session_state:
    st.session_state.files_uploaded = False
if "pending_plan_id" not in st.session_state:
    st.session_state.pending_plan_id = None

# 注入自定义 CSS (增强版)
st.markdown("""
<style>
    /* 全局字体优化 */
    .stMarkdown p {
        font-size: 16px !important;
        line-height: 1.6 !important; /* 增加行间距，解决拥挤 */
        margin-bottom: 1.2em !important; /* 增加段落间距 */
    }
    
    /* 修复无序列表的间距 */
    .stMarkdown ul {
        margin-bottom: 1em !important;
    }
    .stMarkdown li {
        margin-bottom: 0.5em !important; /* 列表项之间强制换行 */
    }

    /* 按钮样式 */
    .stButton>button {
        width: 100%;
        border-radius: 8px;
        height: 3.5em;
        font-weight: 600;
        border: 1px solid #e0e0e0;
    }
    
    /* 审计日志高亮框 (优化版) */
    .audit-box {
        padding: 1.2rem;
        background-color: #f0fdf4;
        border: 1px solid #bbf7d0;
        border-radius: 8px;
        color: #166534;
        margin-bottom: 1.5rem;
        font-weight: 500;
        line-height: 1.8; /* 审计日志内部更宽松 */
        white-space: pre-wrap; /* 保留换行符 */
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 🧩 2. 核心渲染逻辑 (Parser & Renderer)
# ==========================================
def render_message(msg):
    """
    解析并渲染一条消息。
    """
    role = msg["role"]
    content = msg.get("content", "")
    
    with st.chat_message(role):
        if role == "user":
            st.markdown(content)
            return

        # --- AI 消息 ---
        
        # 1. 思考过程
        if "### 🧩 执行过程" in content:
            parts = content.split("### 💡 分析结论")
            process_part = parts[0].replace("### 🧩 执行过程", "").strip()
            result_part = "### 💡 分析结论\n\n" + parts[1].strip() if len(parts) > 1 else ""
            
            with st.expander("👁️ 查看 AI 思考与自愈过程"):
                # 再次确保 process_part 里的换行被渲染
                st.markdown(process_part)
        else:
            result_part = content

        # 2. 分析结论 (含审计日志处理)
        if result_part:
            lines = result_part.split('\n')
            final_lines = []
            audit_html = ""
            
            for line in lines:
                if "🛡️ 审计追踪" in line:
                    # 将审计日志单独提取，并强制在内部换行
                    clean_audit = line.replace("🛡️", "").strip()
                    # 如果有逗号，替换为换行符显示，增加可读性
                    clean_audit = clean_audit.replace(", ", "<br>• ")
                    audit_html = f'<div class="audit-box">🛡️ <b>审计追踪报告</b><br>• {clean_audit}</div>'
                else:
                    final_lines.append(line)
            
            if audit_html:
                st.markdown(audit_html, unsafe_allow_html=True)
            
            # 使用 join('\n\n') 再次确保 Markdown 段落生效
            st.markdown("\n\n".join(final_lines))

        # 3. 渲染图表
        if "charts" in msg and msg["charts"]:
            for c_json in msg["charts"]:
                try:
                    fig = pio.from_json(c_json)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    st.error("图表渲染失败")

        # 4. 渲染下载按钮
        if "download" in msg and msg["download"]:
            full_url = f"{API_URL}{msg['download']}"
            st.link_button("⬇️ 下载 Excel 分析报告 (含审计日志)", full_url, type="primary")

# ==========================================
# 📡 3. 后端通信逻辑
# ==========================================
def send_to_agent(prompt_text, is_system_trigger=False):
    """发送请求到后端，处理响应，更新状态，并强制刷新"""
    command = str(prompt_text or "").strip()
    lowered = command.lower()
    human_action = None
    message_text = prompt_text
    if lowered in {"/approve", "approve", "同意"}:
        human_action = "approve"
    elif lowered in {"/reject", "reject", "拒绝"}:
        human_action = "reject"
    elif lowered.startswith("/revise "):
        human_action = "revise"
        message_text = command.split(" ", 1)[1].strip() or prompt_text
    
    # 如果是用户手动输入，先展示用户消息（占位，防止刷新前看不见）
    if not is_system_trigger:
        st.session_state.messages.append({"role": "user", "content": prompt_text})
        with st.chat_message("user"):
            st.markdown(prompt_text)
    
    # 展示 AI 加载状态
    with st.chat_message("assistant"):
        with st.spinner("🤖 Agent 正在思考、编写代码并执行..."):
            try:
                payload = {
                    "session_id": st.session_state.session_id,
                    "message": message_text
                }
                if human_action:
                    payload["human_action"] = human_action
                    if st.session_state.pending_plan_id:
                        payload["resume_plan_id"] = st.session_state.pending_plan_id
                res = requests.post(f"{API_URL}/chat", json=payload)
                
                if res.status_code == 200:
                    data = res.json()
                    artifacts = data.get("artifacts", {}) or {}
                    
                    # 构造新的消息对象
                    new_msg = {
                        "role": "assistant",
                        "content": data.get("message", ""),
                        "charts": artifacts.get("chart_jsons", []),
                        "download": artifacts.get("download_url")
                    }
                    if data.get("status") and data.get("status") != "done":
                        action_tip = data.get("next_action") or ""
                        if action_tip:
                            new_msg["content"] = f"{new_msg['content']}\n\n---\n\n{action_tip}"
                    execution = data.get("execution", {}) or {}
                    if data.get("status") == "awaiting_human":
                        st.session_state.pending_plan_id = execution.get("plan_id")
                    else:
                        st.session_state.pending_plan_id = None
                    
                    # 存入历史
                    st.session_state.messages.append(new_msg)
                    
                    # ⚡ 强制刷新页面
                    # 这是为了让 render_message 函数统一负责渲染历史记录，
                    # 避免"实时渲染"和"历史回显"代码重复导致的格式不一致。
                    time.sleep(0.1) 
                    st.rerun()
                    
                else:
                    st.error(f"Server Error {res.status_code}: {res.text}")
            except Exception as e:
                st.error(f"Connection Failed: {str(e)}")

# ==========================================
# 📂 4. 侧边栏：文件管理
# ==========================================
with st.sidebar:
    st.title("📂 数据工作台")
    st.info("支持多文件上传，系统将自动识别表头。")
    
    uploaded_files = st.file_uploader(
        "上传数据表 (Excel/CSV)", 
        accept_multiple_files=True,
        type=['xlsx', 'csv', 'xls']
    )
    
    if st.button("🚀 加载数据", type="primary"):
        if uploaded_files:
            with st.spinner("正在智能识别 Schema (AI Ingestion)..."):
                files_data = [('files', (f.name, f, f.type)) for f in uploaded_files]
                data = {'session_id': st.session_state.session_id}
                try:
                    res = requests.post(f"{API_URL}/upload", data=data, files=files_data)
                    if res.status_code == 200:
                        details = res.json().get('details', [])
                        st.session_state.files_uploaded = True
                        st.success(f"已加载 {len(details)} 个文件")
                        with st.expander("查看文件详情"):
                            for d in details:
                                st.write(f"- {d}")
                    else:
                        st.error("上传失败，请检查后端日志")
                except Exception as e:
                    st.error(f"连接失败: {e}")
        else:
            st.warning("请先选择文件")
            
    st.markdown("---")
    st.markdown("**核心能力:**")
    st.markdown("- 🧹 **L1 智能清洗** (Audit Logging)")
    st.markdown("- 🔗 **L2 向量对齐** (Vector Match)")
    st.markdown("- 💰 **L3 财务对账** (Reconciliation)")
    st.caption("v2.5 Enterprise Edition")

# ==========================================
# 🖥️ 5. 主界面布局
# ==========================================
st.title("🤖 智能财务对账助手")
st.markdown("##### Enterprise Agentic Data Analyst")
st.divider()

# --- A. 历史消息渲染区域 ---
# 每次刷新时，重新渲染所有历史消息
# 这保证了页面布局的一致性
for msg in st.session_state.messages:
    render_message(msg)

# 占位符变量
trigger_prompt = None
is_btn_trigger = False

# --- B. 快捷操作区 (Magic Buttons) ---
# 只有当文件上传后才显示
if st.session_state.files_uploaded:
    st.markdown("### 🛠️ 快捷指令")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("⚡ 一键智能对账"):
            trigger_prompt = """
            【任务：一键对账】
            请识别“系统日记账”和“银行流水”。
            1. 多对一处理：若系统表有同名外部流水号，先汇总金额。
            2. 智能对账：执行 smart_reconcile，允许 5 元容差。
            3. 交付：导出包含差异明细和审计日志的 Excel。
            """
            is_btn_trigger = False # 设为False，让指令显示在聊天框，告诉用户发生了什么

    with col2:
        if st.button("🧹 数据清洗与检查"):
            trigger_prompt = """
            【任务：数据体检】
            1. 扫描所有表格，查找空值、重复行和格式错误。
            2. 执行清洗操作，并使用 AuditLogger 记录剔除的数据。
            3. 导出清洗后的数据表。
            """
            is_btn_trigger = False

    with col3:
        if st.button("📊 销售趋势分析"):
            trigger_prompt = """
            【任务：可视化分析】
            请分析销售数据（或对账差异数据）。
            1. 按日期或类别统计金额。
            2. 使用 Plotly 绘制交互式图表（折线图或柱状图）。
            3. 在图表下方给出简要的文字趋势分析。
            """
            is_btn_trigger = False

    with col4:
        if st.button("🗑️ 清空历史"):
            st.session_state.messages = []
            st.rerun()

# --- C. 底部输入框 ---
if user_input := st.chat_input("输入指令，例如：‘查询 Tencent 的订单金额’..."):
    trigger_prompt = user_input
    is_btn_trigger = False

# --- D. 触发执行逻辑 ---
# 将逻辑放在布局代码之后，确保执行时页面已经渲染完毕
if trigger_prompt:
    send_to_agent(trigger_prompt, is_system_trigger=is_btn_trigger)
