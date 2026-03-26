from __future__ import annotations

import time
from typing import Any

import streamlit as st

from app.ui_api_client import chat_stream, chat_sync, upload_files
from app.ui_renderer import format_timeline, render_data_readiness, render_hook_panel, render_message
from app.ui_state import append_message, ensure_state, set_data_readiness, set_execution_state


API_URL = "http://localhost:8000"


st.set_page_config(
    page_title="Agentic Finance | 智能财务对账系统",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

ensure_state()

st.markdown(
    """
<style>
.stMarkdown p {
    font-size: 16px !important;
    line-height: 1.55 !important;
}
.timeline-box {
    border: 1px solid #dbeafe;
    background: #f8fbff;
    border-radius: 10px;
    padding: 8px 12px;
}
</style>
""",
    unsafe_allow_html=True,
)


def _parse_legacy_command(command: str) -> tuple[str, str]:
    text = str(command or "").strip()
    lowered = text.lower()
    if lowered in {"/approve", "approve", "同意"}:
        return "approve", text
    if lowered in {"/reject", "reject", "拒绝"}:
        return "reject", text
    if lowered.startswith("/revise "):
        revised = text.split(" ", 1)[1].strip()
        return "revise", revised or text
    return "", text


def _build_chat_payload(
    *,
    message_text: str,
    human_action: str = "",
    human_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "session_id": st.session_state.session_id,
        "message": message_text,
    }
    if human_action:
        payload["human_action"] = human_action
    if human_payload:
        payload["human_payload"] = dict(human_payload)

    if st.session_state.pending_plan_id and (human_action or human_payload):
        payload["resume_plan_id"] = st.session_state.pending_plan_id

    return payload


def _consume_chat_response(response_payload: dict[str, Any], *, timeline_override: list[dict[str, Any]] | None = None) -> None:
    status = str(response_payload.get("status", "done"))
    message = str(response_payload.get("message", "") or "")
    next_action = str(response_payload.get("next_action", "") or "").strip()
    execution = dict(response_payload.get("execution") or {})
    artifacts = dict(response_payload.get("artifacts") or {})

    if status != "done" and next_action:
        message = f"{message}\n\n---\n\n{next_action}"

    events = list(execution.get("events") or [])
    if (not events) and timeline_override:
        events = list(timeline_override)

    append_message(
        role="assistant",
        content=message,
        charts=list(artifacts.get("chart_jsons") or []),
        download=str(artifacts.get("download_url") or "") or None,
        events=events,
    )
    set_execution_state(status, execution)


def _send_chat(
    *,
    message_text: str,
    human_action: str = "",
    human_payload: dict[str, Any] | None = None,
    show_user_message: bool = True,
) -> None:
    if show_user_message:
        append_message(role="user", content=message_text)
        with st.chat_message("user"):
            st.markdown(message_text)

    payload = _build_chat_payload(
        message_text=message_text,
        human_action=human_action,
        human_payload=human_payload,
    )

    with st.chat_message("assistant"):
        timeline_placeholder = st.empty()
        with st.spinner("🤖 Agent 正在执行并生成审计事件..."):
            response_payload: dict[str, Any] | None = None
            streamed_events: list[dict[str, Any]] = []
            try:
                for event_name, event_data in chat_stream(API_URL, payload):
                    if event_name == "heartbeat":
                        timeline_placeholder.caption("SSE connected...")
                        continue
                    if event_name == "final":
                        response_payload = dict((event_data or {}).get("response") or {})
                        continue

                    if event_name in {
                        "plan_created",
                        "route_selected",
                        "worker_started",
                        "worker_finished",
                        "hook_triggered",
                        "review_verdict",
                        "artifact_emitted",
                        "trace_event",
                    }:
                        streamed_events.append(dict(event_data or {}))
                        timeline_placeholder.markdown(
                            "<div class='timeline-box'>" + format_timeline(streamed_events) + "</div>",
                            unsafe_allow_html=True,
                        )

                if not response_payload:
                    raise RuntimeError("SSE final payload missing")
            except Exception:
                timeline_placeholder.caption("SSE fallback to /chat")
                response_payload = chat_sync(API_URL, payload)

            _consume_chat_response(response_payload, timeline_override=streamed_events)

    time.sleep(0.05)
    st.rerun()


with st.sidebar:
    st.title("📂 数据工作台")
    st.info("支持多文件上传，系统将自动识别表结构与数据就绪度。")

    uploaded_files = st.file_uploader(
        "上传数据表 (Excel/CSV)",
        accept_multiple_files=True,
        type=["xlsx", "csv", "xls"],
    )

    if st.button("🚀 加载数据", type="primary"):
        if uploaded_files:
            try:
                with st.spinner("正在加载并评估数据就绪度..."):
                    payload = upload_files(API_URL, st.session_state.session_id, uploaded_files)
                details = list(payload.get("details") or [])
                st.session_state.files_uploaded = True
                st.success(f"已加载 {len(details)} 个文件")
                with st.expander("查看文件详情"):
                    for item in details:
                        st.write(f"- {item}")

                readiness = dict(payload.get("data_readiness") or {})
                set_data_readiness(readiness)
            except Exception as exc:
                st.error(f"上传失败: {exc}")
        else:
            st.warning("请先选择文件")

    st.markdown("---")
    render_data_readiness(dict(st.session_state.data_readiness or {}))

    st.markdown("---")
    st.markdown("**核心能力:**")
    st.markdown("- 🧹 数据清理")
    st.markdown("- 🔗 主数据合并")
    st.markdown("- 📊 数据可视化")
    st.caption("v2.8-stream")


st.title("🤖 智能财务对账助手")
st.markdown("##### Enterprise Agentic Data Analyst")
st.divider()


for message in st.session_state.messages:
    render_message(message, API_URL)


pending_hook_payload = dict(st.session_state.pending_hook or {})
if pending_hook_payload:
    decision_payload = render_hook_panel(pending_hook_payload)
    if decision_payload:
        decision_text = str(decision_payload.get("decision_type", "approve"))
        decision_value = decision_payload.get("decision_value")
        append_message(
            role="user",
            content=f"🧑‍⚖️ HITL 决策: {decision_text} / {decision_value}",
        )
        override_message = str(decision_value).strip() if decision_text == "revise" else "继续执行"
        _send_chat(
            message_text=override_message,
            human_payload=decision_payload,
            show_user_message=False,
        )


trigger_prompt: str | None = None
parsed_human_action = ""

if st.session_state.files_uploaded:
    st.markdown("### 🛠️ 快捷指令")
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🧹 数据清理与检查"):
            trigger_prompt = "请先做数据体检与清洗，再总结关键问题。"
    with col2:
        if st.button("📊 数据可视化分析"):
            trigger_prompt = "请分析趋势并生成可视化图表与结论。"
    with col3:
        if st.button("🗑️ 清空历史"):
            st.session_state.messages = []
            st.session_state.timeline_events = []
            st.rerun()


if user_input := st.chat_input("输入指令，例如：‘先清洗后合并，再做对账’..."):
    parsed_human_action, normalized_text = _parse_legacy_command(user_input)
    trigger_prompt = normalized_text

if trigger_prompt:
    _send_chat(
        message_text=trigger_prompt,
        human_action=parsed_human_action,
        show_user_message=True,
    )
