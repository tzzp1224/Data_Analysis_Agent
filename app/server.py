import sys
import os
import uuid
import re
import time
import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.ingestion import load_file
from app.services.exporter import save_full_context_excel
from app.services.workflow import create_workflow
from app.core.config import settings
from app.skills.router import route_skill
from app.skills.engine import execute_skill

app = FastAPI(title="Agentic Data Analyst API")

# ==========================================
# 📂 路径配置
# ==========================================
UPLOAD_DIR = "temp_uploads"
OUTPUT_DIR = "temp_outputs"
MAX_UPLOAD_SIZE_BYTES = 20 * 1024 * 1024
SESSION_TTL_SECONDS = 4 * 60 * 60
ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".csv"}
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==========================================
# 🧠 Session 管理
# ==========================================
class SessionData:
    def __init__(self):
        self.dfs_context = {}  # 存放 DataFrames
        self.backups = {}  # 独立备份区，避免污染业务表上下文
        self.workflow_app = None # 编译好的 Graph
        self.download_tokens: dict[str, str] = {}
        self.uploaded_files: set[str] = set()
        self.generated_files: set[str] = set()
        self.last_active_ts = time.time()

    def touch(self):
        self.last_active_ts = time.time()

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


def get_exportable_context(dfs_context: dict) -> dict:
    return {name: df for name, df in dfs_context.items() if not name.startswith("__")}


def extract_runtime_error_summary(raw_msg: str) -> str:
    if "❌ Runtime Error" not in raw_msg:
        return ""
    lines = [line.strip() for line in str(raw_msg).splitlines() if line.strip()]
    # Prefer the last meaningful exception line over traceback header.
    for line in reversed(lines):
        if line.startswith("❌ Runtime Error"):
            continue
        if line.startswith("Traceback"):
            continue
        if line.startswith("File "):
            continue
        return line[:300]
    return "Unknown runtime error"


def sanitize_filename(filename: str) -> str:
    base_name = os.path.basename(filename or "")
    sanitized = re.sub(r"[^A-Za-z0-9._-]", "_", base_name)
    if not sanitized:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return sanitized


def validate_upload_filename(filename: str) -> None:
    extension = os.path.splitext(filename)[1].lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {extension}. Allowed: {sorted(ALLOWED_EXTENSIONS)}",
        )


def save_upload_stream(upload_file: UploadFile, target_path: str) -> None:
    total_bytes = 0
    try:
        with open(target_path, "wb") as buffer:
            while True:
                chunk = upload_file.file.read(1024 * 1024)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > MAX_UPLOAD_SIZE_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Max allowed is {MAX_UPLOAD_SIZE_BYTES // (1024 * 1024)}MB",
                    )
                buffer.write(chunk)
    except HTTPException:
        if os.path.exists(target_path):
            os.remove(target_path)
        raise


def remove_file_quietly(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def cleanup_session_assets(session: SessionData) -> None:
    for upload_name in session.uploaded_files:
        remove_file_quietly(os.path.join(UPLOAD_DIR, upload_name))
    for output_name in session.generated_files:
        remove_file_quietly(os.path.join(OUTPUT_DIR, output_name))
    session.download_tokens.clear()
    session.backups.clear()


def cleanup_expired_sessions() -> None:
    now = time.time()
    expired_ids = [
        session_id
        for session_id, session in sessions.items()
        if now - session.last_active_ts > SESSION_TTL_SECONDS
    ]
    for session_id in expired_ids:
        cleanup_session_assets(sessions[session_id])
        del sessions[session_id]


def build_export_response(session: SessionData, session_id: str, result_df, audit_logger):
    download_link = None
    audit_summary = None
    exportable_context = get_exportable_context(session.dfs_context)
    has_audit_records = bool(audit_logger and not audit_logger.get_log_df().empty)

    if result_df is not None or exportable_context or has_audit_records:
        filename = f"Analysis_Report_{uuid.uuid4().hex[:6]}.xlsx"
        file_path = os.path.join(OUTPUT_DIR, filename)
        save_full_context_excel(result_df, exportable_context, audit_logger, file_path)

        download_token = uuid.uuid4().hex
        session.download_tokens[download_token] = filename
        session.generated_files.add(filename)
        download_link = f"/download/{filename}?session_id={session_id}&token={download_token}"

        if has_audit_records:
            op_count = len([l for l in audit_logger.logs if l["Type"] == "Operation"])
            ex_count = len([l for l in audit_logger.logs if l["Type"] == "Exclusion"])
            audit_summary = f"🛡️ 审计追踪: 执行 {op_count} 步操作, 剔除 {ex_count} 次异常数据。"

    return download_link, audit_summary

# ==========================================
# 🚀 API 接口
# ==========================================

@app.post("/upload")
async def upload_files(session_id: str = Form(...), files: List[UploadFile] = File(...)):
    cleanup_expired_sessions()
    if session_id not in sessions:
        sessions[session_id] = SessionData()
    
    session = sessions[session_id]
    session.touch()
    loaded_info = []

    for file in files:
        original_filename = os.path.basename(file.filename or "")
        if not original_filename:
            raise HTTPException(status_code=400, detail="Invalid filename")

        safe_filename = sanitize_filename(original_filename)
        validate_upload_filename(safe_filename)
        storage_name = f"{session_id}_{safe_filename}"
        file_path = os.path.join(UPLOAD_DIR, storage_name)
        save_upload_stream(file, file_path)
        
        try:
            df = load_file(file_path, display_name=original_filename)
            session.dfs_context[original_filename] = df
            session.backups[original_filename] = df.copy(deep=True)
            session.uploaded_files.add(storage_name)
            loaded_info.append(f"{original_filename} (Rows: {len(df)})")
        except HTTPException:
            remove_file_quietly(file_path)
            raise
        except Exception as e:
            remove_file_quietly(file_path)
            raise HTTPException(status_code=400, detail=f"Failed to load {safe_filename}: {str(e)}")

    session.workflow_app = create_workflow(session.dfs_context, session.backups)
    return {"message": "Upload success", "details": loaded_info}


@app.get("/health")
async def health():
    cleanup_expired_sessions()
    return {
        "status": "ok",
        "llm_ready": bool(settings.GOOGLE_API_KEY),
        "model": settings.GOOGLE_MODEL_NAME,
        "active_sessions": len(sessions),
    }

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    cleanup_expired_sessions()
    session_id = request.session_id
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session expired")
    
    session = sessions[session_id]
    session.touch()
    if not session.workflow_app:
        session.workflow_app = create_workflow({}, {})

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
    for key in list(session.dfs_context.keys()):
        if str(key).startswith("__backup_"):
            del session.dfs_context[key]

    skill_name = route_skill(request.message, session.dfs_context)
    if skill_name:
        skill_result = execute_skill(skill_name, session.dfs_context, request.message)
        if skill_result and skill_result.handled:
            response_text = (
                f"❌ Runtime Error: {skill_result.error}"
                if skill_result.error
                else skill_result.response_text
            )
            download_link, audit_summary = build_export_response(
                session,
                session_id=session_id,
                result_df=skill_result.result_df,
                audit_logger=skill_result.audit,
            )
            return ChatResponse(
                response_text=response_text,
                chart_jsons=skill_result.chart_jsons,
                download_url=download_link,
                audit_summary=audit_summary,
            )

    # 初始化返回变量
    chart_jsons = []
    download_link = None
    audit_summary = None
    steps_log = []
    final_answer = ""
    error_msg = None
    last_runtime_error_summary = ""
    saw_executor_success = False

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
                        if "✅ 成功" in raw_msg:
                            saw_executor_success = True

                        # 3. 拦截报错
                        if "❌ Runtime Error" in raw_msg:
                            steps_log.append("🔧 **自愈**: 检测到代码错误，正在自动修正...")
                            summary = extract_runtime_error_summary(raw_msg)
                            if summary:
                                last_runtime_error_summary = summary

                    if "chart_jsons" in val:
                        chart_jsons.extend(val["chart_jsons"])
                
                elif key == "general_chat":
                    if "messages" in val:
                        final_answer += val["messages"][0].content

        result_df = session.dfs_context.pop('__last_result_df__', None)
        audit_logger = session.dfs_context.pop('__last_audit__', None)
        download_link, audit_summary = build_export_response(session, session_id, result_df, audit_logger)

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
    elif last_runtime_error_summary and not saw_executor_success:
        formatted_response += f"\n\n❌ Runtime Error: {last_runtime_error_summary}"
        if not final_answer and not steps_log:
            formatted_response = f"❌ Runtime Error: {last_runtime_error_summary}"

    return ChatResponse(
        response_text=formatted_response,
        chart_jsons=chart_jsons,
        download_url=download_link,
        audit_summary=audit_summary
    )

@app.get("/download/{filename}")
async def download_file(filename: str, session_id: str, token: str):
    cleanup_expired_sessions()
    safe_filename = os.path.basename(filename)
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session expired")
    session.touch()

    issued_filename = session.download_tokens.get(token)
    if issued_filename != safe_filename:
        raise HTTPException(status_code=403, detail="Invalid download token")

    file_path = os.path.join(OUTPUT_DIR, safe_filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=safe_filename)
    raise HTTPException(status_code=404, detail="File not found")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
