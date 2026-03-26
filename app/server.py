import sys
import os
import uuid
import re
import time
import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Any, List, Optional

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.ingestion import load_file
from app.services.exporter import save_full_context_excel
from app.services.semantic_contract import ensure_semantic_contract, invalidate_semantic_contract
from app.orchestration import (
    create_agent_first_workflow,
    run_agent_first_workflow,
    merge_audit_envelope,
    get_official_supervisor_backend,
)
from app.orchestration.memory import ExecutionMemory
from app.core.config import settings

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
        self.agent_graph_app = None
        self.pending_execution_state: ExecutionMemory | None = None
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
    human_action: Optional[str] = None
    resume_plan_id: Optional[str] = None


class ExecutionPayload(BaseModel):
    plan_id: str = ""
    current_step_idx: int = 0
    plan_steps: List[dict] = Field(default_factory=list)
    step_results: List[dict] = Field(default_factory=list)
    route_trace: List[dict] = Field(default_factory=list)
    execution_status: str = "running"


class ArtifactPayload(BaseModel):
    chart_jsons: List[str] = Field(default_factory=list)
    download_url: Optional[str] = None
    audit_summary: Optional[str] = None


class ChatResponse(BaseModel):
    status: str = "done"
    message: str
    next_action: Optional[str] = None
    execution: ExecutionPayload = Field(default_factory=ExecutionPayload)
    artifacts: ArtifactPayload = Field(default_factory=ArtifactPayload)


def get_exportable_context(dfs_context: dict) -> dict:
    return {name: df for name, df in dfs_context.items() if not name.startswith("__")}


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

    invalidate_semantic_contract(session.dfs_context)
    try:
        session.agent_graph_app = create_agent_first_workflow(session.dfs_context, session.backups)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    session.pending_execution_state = None
    return {"message": "Upload success", "details": loaded_info}


@app.get("/health")
async def health():
    cleanup_expired_sessions()
    backend = get_official_supervisor_backend()
    return {
        "status": "ok",
        "llm_ready": bool(settings.GOOGLE_API_KEY),
        "model": settings.GOOGLE_MODEL_NAME,
        "active_sessions": len(sessions),
        "agent_first_enabled": bool(settings.AGENT_FIRST_ENABLED),
        "supervisor_backend": backend or "unavailable",
        "official_supervisor_ready": bool(backend),
    }

def reset_temporary_state(session: SessionData, *, keep_last_audit: bool = False) -> None:
    if "__last_result_df__" in session.dfs_context:
        del session.dfs_context["__last_result_df__"]
    if (not keep_last_audit) and "__last_audit__" in session.dfs_context:
        del session.dfs_context["__last_audit__"]
    for key in list(session.dfs_context.keys()):
        if str(key).startswith("__backup_"):
            del session.dfs_context[key]


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    cleanup_expired_sessions()
    session_id = request.session_id
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session expired")

    session = sessions[session_id]
    session.touch()
    action = str(request.human_action or "").strip().lower()
    keep_last_audit = bool(session.pending_execution_state and action in {"", "approve", "revise"})
    reset_temporary_state(session, keep_last_audit=keep_last_audit)
    if not settings.AGENT_FIRST_ENABLED:
        raise HTTPException(status_code=503, detail="Agent-first path is disabled by configuration.")

    if not session.agent_graph_app:
        try:
            session.agent_graph_app = create_agent_first_workflow(session.dfs_context, session.backups)
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    ensure_semantic_contract(session.dfs_context, user_instruction=request.message)
    final_state = run_agent_first_workflow(
        session.agent_graph_app,
        user_instruction=request.message,
        human_action=request.human_action or "",
        pending_state=session.pending_execution_state.to_pending_state()
        if session.pending_execution_state
        else None,
        resume_plan_id=request.resume_plan_id or "",
    )
    status = str(final_state.get("status", "done"))
    route_trace = list(final_state.get("route_trace", []))
    audit_envelope = list(final_state.get("audit_envelope", []))
    next_action = str(final_state.get("next_action", "")).strip() or None

    plan_id = str(final_state.get("plan_id", "")).strip()
    plan_steps = list(final_state.get("plan_steps", []))
    current_step_idx = int(final_state.get("current_step_idx", 0) or 0)
    step_results = list(final_state.get("step_results", []))
    execution_status = str(final_state.get("execution_status", status)).strip() or status
    pending_hitl = dict(final_state.get("pending_hitl") or {})

    if status == "awaiting_human":
        session.pending_execution_state = ExecutionMemory(
            plan_id=plan_id,
            plan_steps=plan_steps,
            current_step_idx=current_step_idx,
            step_results=step_results,
            pending_hitl=pending_hitl,
        )
    else:
        session.pending_execution_state = None

    merged_audit = merge_audit_envelope(session.dfs_context.get("__last_audit__"), audit_envelope)
    if merged_audit is not None:
        session.dfs_context["__last_audit__"] = merged_audit

    message = str(final_state.get("reply", "")).strip()
    if not message:
        message = "执行完成。" if status == "done" else "任务处理中。"

    chart_jsons = list(final_state.get("chart_jsons", []))
    download_link = None
    audit_summary = None
    if status == "done":
        result_df = session.dfs_context.pop("__last_result_df__", None)
        audit_logger = session.dfs_context.pop("__last_audit__", None)
        download_link, audit_summary = build_export_response(session, session_id, result_df, audit_logger)

    return ChatResponse(
        status=status,
        message=message,
        next_action=next_action,
        execution=ExecutionPayload(
            plan_id=plan_id,
            current_step_idx=current_step_idx,
            plan_steps=plan_steps,
            step_results=step_results,
            route_trace=route_trace,
            execution_status=execution_status,
        ),
        artifacts=ArtifactPayload(
            chart_jsons=chart_jsons,
            download_url=download_link,
            audit_summary=audit_summary,
        ),
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
