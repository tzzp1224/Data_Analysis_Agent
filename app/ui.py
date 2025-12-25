import streamlit as st
import requests
import pandas as pd
import json
import plotly.io as pio
import uuid

# 后端地址
API_URL = "http://localhost:8000"

st.set_page_config(page_title="Agentic Data Analyst", layout="wide")

# ==========================================
# Session State 初始化
# ==========================================
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

if "messages" not in st.session_state:
    st.session_state.messages = []

# ==========================================
# Sidebar: 文件上传
# ==========================================
with st.sidebar:
    st.title("📂 数据上传")
    uploaded_files = st.file_uploader("上传 Excel/CSV 文件", accept_multiple_files=True)
    
    if st.button("开始分析"):
        if uploaded_files:
            with st.spinner("正在智能摄取数据 (Ingestion)..."):
                # 构造 multipart/form-data 请求
                files_data = [('files', (f.name, f, f.type)) for f in uploaded_files]
                data = {'session_id': st.session_state.session_id}
                
                try:
                    res = requests.post(f"{API_URL}/upload", data=data, files=files_data)
                    if res.status_code == 200:
                        st.success(f"成功加载 {len(uploaded_files)} 个文件！")
                        st.json(res.json())
                    else:
                        st.error("上传失败，请检查后端日志。")
                except Exception as e:
                    st.error(f"连接失败: {e}")
        else:
            st.warning("请先选择文件。")

# ==========================================
# Main: 聊天界面
# ==========================================
st.title("🤖 AI 数据分析师")
st.caption("支持：数据清洗 · 多表关联 · 统计分析 · 可视化 · 结果导出")

# 1. 展示历史消息
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        # 如果历史消息里有图表，这里比较难复现， MVP 版本暂只存文本历史

# 2. 处理用户输入
if prompt := st.chat_input("请输入指令，例如：'分析销售趋势' 或 '合并表格并导出'"):
    # 显示用户消息
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 调用后端
    with st.chat_message("assistant"):
        with st.spinner("思考中..."):
            try:
                payload = {
                    "session_id": st.session_state.session_id,
                    "message": prompt
                }
                res = requests.post(f"{API_URL}/chat", json=payload)
                
                if res.status_code == 200:
                    data = res.json()
                    
                    # 1. 展示文本回复 (Insights)
                    response_text = data.get("response_text", "")
                    if response_text:
                        st.markdown(response_text)
                    
                    # 2. 展示图表
                    chart_jsons = data.get("chart_jsons", [])
                    for c_json in chart_jsons:
                        fig = pio.from_json(c_json)
                        st.plotly_chart(fig, use_container_width=True)
                        
                    # 3. 展示下载链接
                    download_url = data.get("download_url")
                    if download_url:
                        full_url = f"{API_URL}{download_url}"
                        st.success("✅ 文件已生成")
                        st.link_button("⬇️ 点击下载 Excel 结果", full_url)
                        
                    # 更新历史 (只存文本，简化)
                    st.session_state.messages.append({"role": "assistant", "content": response_text})
                    
                else:
                    st.error(f"Error {res.status_code}: {res.text}")
            except Exception as e:
                st.error(f"Request failed: {e}")