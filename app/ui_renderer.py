from __future__ import annotations

import json
from typing import Any

import plotly.io as pio
import streamlit as st


def format_timeline(events: list[dict[str, Any]]) -> str:
    if not events:
        return "- (暂无执行事件)"
    lines: list[str] = []
    for item in events[-20:]:
        event_type = str(item.get("event_type", "trace_event")).strip()
        detail = str(item.get("detail", "")).strip()
        worker = str(item.get("worker", "")).strip()
        ts = str(item.get("timestamp", "")).strip()
        worker_suffix = f" · `{worker}`" if worker else ""
        detail_suffix = f" · {detail}" if detail else ""
        time_prefix = f"[{ts}] " if ts else ""
        lines.append(f"- {time_prefix}`{event_type}`{worker_suffix}{detail_suffix}")
    return "\n".join(lines)


def render_message(msg: dict[str, Any], api_url: str) -> None:
    role = str(msg.get("role", "assistant"))
    content = str(msg.get("content", "") or "")

    with st.chat_message(role):
        st.markdown(content)

        if role == "assistant":
            charts = list(msg.get("charts") or [])
            for chart_json in charts:
                try:
                    fig = pio.from_json(chart_json)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    st.error("图表渲染失败")

            download = str(msg.get("download", "") or "").strip()
            if download:
                st.link_button(
                    "⬇️ 下载 Excel 分析报告 (含审计日志)",
                    f"{api_url}{download}",
                    type="primary",
                )

            events = list(msg.get("events") or [])
            if events:
                with st.expander("🧭 查看执行时间线"):
                    st.markdown(format_timeline(events))


def render_data_readiness(readiness: dict[str, Any]) -> None:
    if not readiness:
        return

    status = str(readiness.get("status", "")).strip().lower()
    score = readiness.get("score")
    status_label = {
        "ready": "READY",
        "recoverable": "RECOVERABLE",
        "blocked": "BLOCKED",
    }.get(status, status.upper() or "UNKNOWN")
    tone = {
        "ready": "🟢",
        "recoverable": "🟠",
        "blocked": "🔴",
    }.get(status, "⚪")
    st.markdown(f"### {tone} Data Readiness")
    if score is not None:
        st.caption(f"status={status_label} · score={score}")
    else:
        st.caption(f"status={status_label}")

    issues = list(readiness.get("issues") or [])
    recs = list(readiness.get("recommendations") or [])
    if issues:
        with st.expander("查看问题"):
            for issue in issues[:8]:
                st.write(f"- {issue}")
    if recs:
        with st.expander("查看建议"):
            for rec in recs[:8]:
                st.write(f"- {rec}")


def render_hook_panel(pending_hook: dict[str, Any]) -> dict[str, Any] | None:
    hook = dict(pending_hook or {})
    hook_type = str(hook.get("hook_type", "")).strip().lower()
    hook_id = str(hook.get("hook_id", "")).strip()
    if not hook_type:
        return None

    st.markdown(
        """
<style>
.hitl-card {
    border: 1px solid #f59e0b;
    border-radius: 12px;
    padding: 12px;
    background: #fff8eb;
}
</style>
""",
        unsafe_allow_html=True,
    )
    st.markdown('<div class="hitl-card">', unsafe_allow_html=True)
    st.markdown("### 🧑‍⚖️ Human In The Loop")
    st.markdown(str(hook.get("question", "需要人工确认。")))

    options = list(hook.get("options") or [])
    evidence = dict(hook.get("evidence") or {})
    deadline_hint = str(hook.get("deadline_hint", "")).strip()
    if deadline_hint:
        st.caption(deadline_hint)
    if evidence:
        with st.expander("查看证据"):
            st.json(evidence)

    form_key = f"hitl_{hook_id or 'default'}"
    with st.form(form_key):
        decision_type = hook_type
        decision_value: Any = None

        if hook_type == "approve":
            decision_type = st.selectbox(
                "决策类型",
                options=["approve", "reject", "revise"],
                format_func=lambda x: {"approve": "批准继续", "reject": "拒绝终止", "revise": "修改后重规划"}[x],
            )
            if decision_type == "revise":
                decision_value = st.text_input("请输入新的执行指令")
        elif hook_type == "select_option":
            labels = []
            value_by_label: dict[str, Any] = {}
            for item in options:
                label = str(item.get("label", "")).strip() or str(item.get("value", "")).strip()
                if not label:
                    continue
                labels.append(label)
                value_by_label[label] = item.get("value", label)
            if labels:
                picked = st.selectbox("请选择执行选项", options=labels)
                decision_value = value_by_label.get(picked, picked)
            else:
                decision_type = "approve"
        elif hook_type == "map_columns":
            default_mapping = json.dumps(evidence.get("mapping_template", {}), ensure_ascii=False, indent=2)
            raw_mapping = st.text_area("请输入字段映射(JSON)", value=default_mapping if default_mapping != "{}" else "")
            decision_value = raw_mapping.strip()
        elif hook_type == "set_threshold":
            suggested = float(evidence.get("suggested_threshold", 0.05) or 0.05)
            decision_value = st.number_input("设置阈值", value=suggested, step=0.01, format="%.4f")

        comment = st.text_input("备注（可选）")
        submitted = st.form_submit_button("提交决策", type="primary")

    st.markdown("</div>", unsafe_allow_html=True)

    if not submitted:
        return None

    if decision_type == "revise":
        text = str(decision_value or "").strip()
        if not text:
            st.warning("`revise` 需要提供新的指令。")
            return None
        decision_value = text

    if decision_type == "map_columns":
        raw = str(decision_value or "").strip()
        if raw:
            try:
                decision_value = json.loads(raw)
            except Exception:
                st.warning("字段映射 JSON 解析失败，请检查格式。")
                return None
        else:
            decision_value = {}

    return {
        "hook_id": hook_id,
        "decision_type": decision_type,
        "decision_value": decision_value,
        "comment": str(comment or "").strip(),
    }
