import sys
import os
import uuid
import shutil
import pandas as pd
import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional

# 引入我们的核心逻辑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services.ingestion import load_file
from app.services.workflow import create_workflow

app = FastAPI(title="Agentic Data Analyst API")

# ==========================================
# Session Management (内存存储，重启后丢失)
# ==========================================
class SessionData:
    def __init__(self):
        self.dfs_context = {}  # 存放 DataFrames
        self.messages = []     # 存放 LangChain 消息历史
        self.workflow_app = None # 编译好的 Graph

sessions: dict[str, SessionData] = {}

# 临时文件存储目录
UPLOAD_DIR = "temp_uploads"
OUTPUT_DIR = "temp_outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==========================================
# Models
# ==========================================
class ChatRequest(BaseModel):
    session_id: str
    message: str

class ChatResponse(BaseModel):
    response_text: str
    chart_jsons: List[str] = []
    download_url: Optional[str] = None

# ==========================================
# Endpoints
# ==========================================

@app.post("/upload")
async def upload_files(session_id: str = Form(...), files: List[UploadFile] = File(...)):
    """
    上传文件并进行 Ingestion
    """
    if session_id not in sessions:
        sessions[session_id] = SessionData()
    
    session = sessions[session_id]
    loaded_files = []

    for file in files:
        # 1. 保存文件到本地临时目录
        file_location = os.path.join(UPLOAD_DIR, f"{session_id}_{file.filename}")
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # 2. 调用 Ingestion (简化版：自动接受 AI 建议，不搞交互式确认了，为了流畅)
        try:
            # 这里直接复用之前的 load_file，或者使用 ingestion.propose_config 但自动 apply
            df = load_file(file_location) 
            session.dfs_context[file.filename] = df
            loaded_files.append(file.filename)
        except Exception as e:
            return {"error": f"Failed to load {file.filename}: {str(e)}"}

    # 3. 重新初始化 Workflow (因为 dfs 变了)
    session.workflow_app = create_workflow(session.dfs_context)
    
    return {"message": "Files loaded successfully", "loaded_files": loaded_files}

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    与 Agent 对话
    """
    session_id = request.session_id
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session = sessions[session_id]
    if not session.workflow_app:
        # 如果没有上传文件，初始化一个空的 context
        session.workflow_app = create_workflow({})

    # 构建初始状态
    # 注意：为了让 Graph 记住历史，我们需要把 session.messages 传进去
    # 但 LangGraph 的 state 是不可变的，所以我们需要把新的一轮 append 进去
    state = {
        "messages": session.messages, # 传入历史
        "user_instruction": request.message,
        "error_count": 0,
        "chart_jsons": [],
        "reply": ""
    }
    
    response_text = ""
    chart_jsons = []
    download_link = None
    
    # 清理掉旧的 result_df
    if '__last_result_df__' in session.dfs_context:
        del session.dfs_context['__last_result_df__']

    try:
        # 运行 Graph
        # 我们只关心 executor 的输出
        final_state = None
        
        for event in session.workflow_app.stream(state, config={"recursion_limit": 25}):
            for key, val in event.items():
                if key == "executor":
                    # 收集文本回复
                    if "messages" in val:
                        msg_content = val["messages"][-1].content
                        # 简单的清洗，去掉系统信号
                        clean_msg = msg_content.replace("(Signal: WORKER_DONE)", "").replace("✅ 执行成功:", "").strip()
                        response_text += clean_msg + "\n\n"
                    
                    # 收集图表
                    if "chart_jsons" in val:
                        chart_jsons.extend(val["chart_jsons"])
                
                if key == "general_chat":
                     if "messages" in val:
                        response_text += val["messages"][0].content

                # 更新 Memory (简单粗暴法：保存最后的状态中的 messages)
                if "messages" in val:
                    # 注意：这里逻辑稍微复杂，LangGraph 的 stream 返回的是 update。
                    # 为了简化，我们在真实项目中通常使用 Checkpointer。
                    # 这里 MVP 我们暂时不手动维护 session.messages，依赖 Graph 内部传递，
                    # 但 HTTP 请求是无状态的... 
                    # 💡 修正：为了 MVP 跑通，我们简化处理：假设每次对话都是独立的 Context，
                    # 或者我们可以简单地把这次交互产生的 HumanMessage 和 AIMessage 存入 session.messages
                    pass

        # 检查是否有文件生成
        if '__last_result_df__' in session.dfs_context:
            result_df = session.dfs_context.pop('__last_result_df__')
            filename = f"result_{uuid.uuid4().hex[:8]}.xlsx"
            file_path = os.path.join(OUTPUT_DIR, filename)
            result_df.to_excel(file_path, index=False)
            download_link = f"/download/{filename}"
            response_text += f"\n\n💾 结果文件已生成，请点击下方链接下载。"

    except Exception as e:
        response_text = f"系统错误: {str(e)}"

    return ChatResponse(
        response_text=response_text,
        chart_jsons=chart_jsons,
        download_url=download_link
    )

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=filename)
    raise HTTPException(status_code=404, detail="File not found")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)