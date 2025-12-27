import sys
import os
import uuid
import shutil
import pandas as pd
import uvicorn
import io
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional, Dict

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.ingestion import load_file
from app.services.workflow import create_workflow
from app.utils.tools import AuditLogger

app = FastAPI(title="Agentic Data Analyst API")

# ==========================================
# 📂 路径配置
# ==========================================
UPLOAD_DIR = "temp_uploads"
OUTPUT_DIR = "temp_outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==========================================
# 🧠 Session 管理
# ==========================================
class SessionData:
    def __init__(self):
        self.dfs_context = {}  # 存放 DataFrames
        self.workflow_app = None # 编译好的 Graph

sessions: dict[str, SessionData] = {}

# ==========================================
# 📦 数据模型
# ==========================================
class ChatRequest(BaseModel):
    session_id: str
    message: str

class ChatResponse(BaseModel):
    response_text: str
    chart_jsons: List[str] = []
    download_url: Optional[str] = None
    audit_summary: Optional[str] = None

# ==========================================
# 🛠️ 核心工具：纯净版导出 (User Request Fix)
# ==========================================
def save_full_context_excel(result_df: Optional[pd.DataFrame], 
                          dfs_context: Dict[str, pd.DataFrame], 
                          audit: AuditLogger, 
                          output_path: str):
    """
    将 【所有当前数据表】 + 【审计日志】 保存到一个 Excel。
    修改点：不再强制生成“分析结果”Sheet，而是直接保存 dfs_context 中的文件，
    确保文件名和 Sheet 名一一对应，且内容为清洗后的版本。
    """
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        saved_sheets = set()

        # 1. 优先写入上下文中的所有数据表 (Cleaned Files)
        # 因为 Worker 已经执行了 dfs[name] = df，所以这里就是清洗后的数据
        if dfs_context:
            for name, df in dfs_context.items():
                if name.startswith("__"): continue # 跳过系统变量
                
                # Sheet 名处理 (Excel 限制 31 字符)
                # 去掉 .xlsx 后缀，直接用文件名，简洁明了
                safe_name = os.path.splitext(name)[0][:30]
                
                # 避免重名
                counter = 1
                original_name = safe_name
                while safe_name in saved_sheets:
                    safe_name = f"{original_name}_{counter}"
                    counter += 1
                
                df.to_excel(writer, sheet_name=safe_name, index=False)
                saved_sheets.add(safe_name)
        
        # 2. (可选) 只有当 result_df 是全新的聚合结果(不在dfs_context里)时，才保存
        # 但为了满足“不需要分析结果Sheet”的要求，这里直接注释掉，除非你做聚合分析
        # if result_df is not None: ...
        
        # 3. 写入审计日志 (Audit)
        if audit:
            log_df = audit.get_log_df()
            if not log_df.empty:
                log_df.to_excel(writer, sheet_name='处理日志(Audit)', index=False)
            
            # 4. 写入被剔除的数据 (Exclusions)
            for name, ex_df in audit.excluded_data.items():
                # 简化的 Sheet 名
                clean_name = os.path.splitext(name)[0][:10]
                sheet_name = f"剔除_{clean_name}"[:30]
                ex_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
# ==========================================
# 🚀 API 接口
# ==========================================

@app.post("/upload")
async def upload_files(session_id: str = Form(...), files: List[UploadFile] = File(...)):
    if session_id not in sessions:
        sessions[session_id] = SessionData()
    
    session = sessions[session_id]
    loaded_info = []

    for file in files:
        file_path = os.path.join(UPLOAD_DIR, f"{session_id}_{file.filename}")
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        try:
            df = load_file(file_path)
            session.dfs_context[file.filename] = df
            # ✅ 新增：创建隐形备份 (Deep Copy)
            session.dfs_context[f"__backup_{file.filename}"] = df.copy(deep=True)
            loaded_info.append(f"{file.filename} (Rows: {len(df)})")
        except Exception as e:
            return {"error": f"Failed to load {file.filename}: {str(e)}"}

    session.workflow_app = create_workflow(session.dfs_context)
    return {"message": "Upload success", "details": loaded_info}

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    session_id = request.session_id
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session expired")
    
    session = sessions[session_id]
    if not session.workflow_app:
        session.workflow_app = create_workflow({})

    state = {
        "messages": [], 
        "user_instruction": request.message,
        "error_count": 0,
        "chart_jsons": [],
        "reply": ""
    }
    
    # 清理旧状态 (保留 context 中的数据表，清除上一次的临时结果)
    if '__last_result_df__' in session.dfs_context: del session.dfs_context['__last_result_df__']
    if '__last_audit__' in session.dfs_context: del session.dfs_context['__last_audit__']

    # 初始化返回变量
    chart_jsons = []
    download_link = None
    audit_summary = None
    steps_log = []
    final_answer = ""
    error_msg = None

    try:
        # 运行 Workflow
        for event in session.workflow_app.stream(state, config={"recursion_limit": 30}):
            for key, val in event.items():
                if key == "executor":
                    if "messages" in val:
                        raw_msg = val["messages"][-1].content
                        
                        # 1. 提取 PLAN (思考过程)
                        if "# PLAN:" in raw_msg:
                            try:
                                plan_part = raw_msg.split("# PLAN:")[1].split("# CODE")[0].strip()
                                # 移除 # 号，防止字体过大
                                plan_clean = "\n".join([line.strip("# ").strip() for line in plan_part.splitlines()])
                                steps_log.append(f"🧠 **思考**: {plan_clean}")
                            except:
                                pass
                        
                        # 2. 提取 Insights (分析结论)
                        # 识别包含结论的文本，并清洗
                        if "📊 分析结论" in raw_msg or "✅" in raw_msg or "清洗完成" in raw_msg:
                            clean = raw_msg.replace("(Signal: WORKER_DONE)", "").strip()
                            if clean not in final_answer:
                                final_answer += clean + "\n\n"

                        # 3. 拦截报错
                        if "❌ Runtime Error" in raw_msg:
                            steps_log.append("🔧 **自愈**: 检测到代码错误，正在自动修正...")

                    if "chart_jsons" in val:
                        chart_jsons.extend(val["chart_jsons"])
                
                elif key == "general_chat":
                    if "messages" in val:
                        final_answer += val["messages"][0].content

        # ==========================================
        # 💾 文件导出逻辑 (核心修改)
        # ==========================================
        # 即使没有 __last_result_df__，只要有数据表和审计日志，也可以导出
        # 但通常 Workflow 结束时至少会生成审计对象
        
        result_df = session.dfs_context.pop('__last_result_df__', None)
        audit_logger = session.dfs_context.pop('__last_audit__', None)
        
        # 只要有数据或者有结果，就生成 Excel
        if result_df is not None or len(session.dfs_context) > 0:
            filename = f"Analysis_Report_{uuid.uuid4().hex[:6]}.xlsx"
            file_path = os.path.join(OUTPUT_DIR, filename)
            
            # ✅ 调用新的全量保存函数
            # 传入 session.dfs_context 以保存所有被清洗过的表
            save_full_context_excel(result_df, session.dfs_context, audit_logger, file_path)
            
            download_link = f"/download/{filename}"
            
            if audit_logger:
                op_count = len([l for l in audit_logger.logs if l['Type']=='Operation'])
                ex_count = len([l for l in audit_logger.logs if l['Type']=='Exclusion'])
                audit_summary = f"🛡️ 审计追踪: 执行 {op_count} 步操作, 剔除 {ex_count} 次异常数据。"

    except Exception as e:
        error_msg = f"系统异常: {str(e)}"
        print(f"Server Error: {str(e)}")

    # ==========================================
    # 🎨 响应文本格式化 (解决字体过大问题)
    # ==========================================
    formatted_response = ""
    
    if steps_log:
        formatted_response += "### 🧩 执行过程\n\n"
        for step in steps_log:
            # 再次确保清洗掉 Markdown 标题符
            clean_step = step.replace("#", "").strip()
            formatted_response += f"- {clean_step}\n\n"
        formatted_response += "---\n\n"

    if final_answer:
        formatted_response += "### 💡 分析结论\n\n"
        # 降级标题，防止字体爆炸
        lines = final_answer.split('\n')
        clean_lines = []
        for line in lines:
            if line.strip().startswith("#"):
                clean_lines.append(f"**{line.strip('# ')}**")
            else:
                clean_lines.append(line)
        formatted_response += "\n\n".join(clean_lines)
    
    if error_msg:
        formatted_response += f"\n\n🚨 **错误提示**: {error_msg}"
        if not final_answer: formatted_response = error_msg

    return ChatResponse(
        response_text=formatted_response,
        chart_jsons=chart_jsons,
        download_url=download_link,
        audit_summary=audit_summary
    )

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=filename)
    raise HTTPException(status_code=404, detail="File not found")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)