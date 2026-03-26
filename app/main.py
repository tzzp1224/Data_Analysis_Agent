import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.orchestration import (  # noqa: E402
    create_agent_first_workflow,
    merge_audit_envelope,
    run_agent_first_workflow,
)
from app.orchestration.memory import ExecutionMemory  # noqa: E402
from app.services.exporter import save_result_with_audit  # noqa: E402
from app.services.ingestion import apply_ingestion, propose_ingestion_config  # noqa: E402
from app.services.semantic_contract import ensure_semantic_contract  # noqa: E402
from app.utils.finance_generator import create_reconciliation_data  # noqa: E402
from app.utils.generator import create_complex_test_data  # noqa: E402


def interactive_file_loader(file_paths: list[str]):
    """模拟前端的交互式文件导入过程。"""

    dfs_context = {}
    print("\n" + "=" * 50)
    print("📂 交互式数据摄取 (Interactive Ingestion)")
    print("=" * 50)

    for fp in file_paths:
        filename = os.path.basename(fp)
        print(f"\n🔍 正在分析文件结构: {filename} ...")

        try:
            config = propose_ingestion_config(fp)
            print(f"   🤖 AI 建议: Sheet='{config.sheet_name}', Header在第 {config.header_row} 行")
            print(f"      理由: {config.reason}")

            user_input = input(f"   👉 是否采用此配置加载 {filename}? (y/n) [y]: ").strip().lower()
            if user_input == "n":
                print("   (跳过加载)")
                continue

            df = apply_ingestion(config)
            dfs_context[filename] = df
            print(f"   ✅ 加载成功! Shape: {df.shape}")
        except Exception as e:
            print(f"   ❌ 加载失败: {e}")

    return dfs_context


def _parse_cli_action(command: str) -> tuple[str, str]:
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


def _reset_temporary_state(dfs_context: dict, *, keep_last_audit: bool = False) -> None:
    if "__last_result_df__" in dfs_context:
        del dfs_context["__last_result_df__"]
    if (not keep_last_audit) and "__last_audit__" in dfs_context:
        del dfs_context["__last_audit__"]
    for key in list(dfs_context.keys()):
        if str(key).startswith("__backup_"):
            del dfs_context[key]


def main():
    print("=" * 50)
    print("🤖 AI Agentic Data Analyst (CLI Mode / Supervisor v2)")
    print("=" * 50)

    file_paths_1 = create_complex_test_data()
    file_paths_2 = create_reconciliation_data()
    all_files = file_paths_1 + file_paths_2

    dfs_context = interactive_file_loader(all_files)
    if not dfs_context:
        print("没有数据被加载，程序退出。")
        return

    backups_context = {name: df.copy(deep=True) for name, df in dfs_context.items()}
    graph_app = create_agent_first_workflow(dfs_context, backups_context)
    pending_execution_state: ExecutionMemory | None = None

    print("\n" + "=" * 50)
    print("🤖 系统已就绪（Supervisor v2）。")
    print("支持：清洗 / 合并 / 对账 / 可视化 / HITL 续跑")
    print("HITL 指令：/approve, /reject, /revise <新指令>")
    print("=" * 50)

    while True:
        try:
            command = input("\n💬 请输入指令 (exit退出): ").strip()
        except EOFError:
            break

        if command.lower() == "exit":
            break
        if not command:
            continue

        human_action, message_text = _parse_cli_action(command)
        keep_last_audit = bool(pending_execution_state and human_action in {"", "approve", "revise"})
        _reset_temporary_state(dfs_context, keep_last_audit=keep_last_audit)

        if not message_text:
            print("⚠️ revise 需要提供新指令，例如: /revise 先清洗再对账")
            continue

        ensure_semantic_contract(dfs_context, user_instruction=message_text)

        pending_payload = (
            pending_execution_state.to_pending_state() if pending_execution_state else None
        )
        resume_plan_id = pending_execution_state.plan_id if pending_execution_state else ""

        print("⚙️ Supervisor 正在执行...")
        try:
            final_state = run_agent_first_workflow(
                graph_app,
                user_instruction=message_text,
                human_action=human_action,
                pending_state=pending_payload,
                resume_plan_id=resume_plan_id,
            )
        except Exception as e:
            print(f"❌ 系统错误: {e}")
            continue

        status = str(final_state.get("status", "done"))
        execution_status = str(final_state.get("execution_status", status))
        reply = str(final_state.get("reply", "")).strip() or "流程已结束。"
        print(f"\n[{execution_status}] {reply}")

        plan_id = str(final_state.get("plan_id", "")).strip()
        plan_steps = list(final_state.get("plan_steps", []))
        current_step_idx = int(final_state.get("current_step_idx", 0) or 0)
        step_results = list(final_state.get("step_results", []))

        if status == "awaiting_human":
            pending_execution_state = ExecutionMemory(
                plan_id=plan_id,
                plan_steps=plan_steps,
                current_step_idx=current_step_idx,
                step_results=step_results,
                pending_hitl=dict(final_state.get("pending_hitl") or {}),
            )
            next_action = str(final_state.get("next_action", "")).strip()
            if next_action:
                print(f"👉 下一步: {next_action}")
        else:
            pending_execution_state = None

        merged_audit = merge_audit_envelope(
            dfs_context.get("__last_audit__"),
            list(final_state.get("audit_envelope", [])),
        )
        if merged_audit is not None:
            dfs_context["__last_audit__"] = merged_audit

        chart_jsons = list(final_state.get("chart_jsons", []))
        if chart_jsons:
            import plotly.io as pio

            os.makedirs("data", exist_ok=True)
            pio.from_json(chart_jsons[0]).write_html("data/chart.html")
            print(f"🎨 已生成 {len(chart_jsons)} 张图表，保存至 data/chart.html")

        if status == "done":
            result_df = dfs_context.pop("__last_result_df__", None)
            audit_logger = dfs_context.pop("__last_audit__", None)
            if result_df is not None or audit_logger is not None:
                output_path = "data/output_result.xlsx"
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                save_result_with_audit(result_df, audit_logger, output_path)
                print(f"💾 [交付] 结果文件已保存至: {output_path}")


if __name__ == "__main__":
    main()
