import sys
import os
import pandas as pd
from functools import partial

# 路径 hack
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.generator import create_complex_test_excel
from app.services.ingestion import load_file
from app.services.cleaner import (
    create_cleaning_graph, 
    analyst_node, 
    execution_node, 
    should_continue, 
    AgentState, 
    END
)
from langgraph.graph import StateGraph

def main():
    print("="*60)
    print("🤖 Agentic Data Analyst - Bootstrapping")
    print("="*60)

    # ---------------------------------------------------------
    # Step 0: 生成环境
    # ---------------------------------------------------------
    file_path = create_complex_test_excel()
    if not file_path:
        return

    # ---------------------------------------------------------
    # Step 1: 智能摄入
    # ---------------------------------------------------------
    print("\n🔍 [Phase 1] 智能加载与感知 (Ingestion Agent)...")
    try:
        df = load_file(file_path)
        print(f"\n✅ 加载完成。数据形状: {df.shape}")
        
    except Exception as e:
        print(f"❌ 致命错误 (加载阶段): {e}")
        return

    # ---------------------------------------------------------
    # Step 2: 智能清洗
    # ---------------------------------------------------------
    print("\n🧹 [Phase 2] 启动清洗智能体 (Cleaning Agent)...")
    
    df_context = {"df": df}
    
    workflow = StateGraph(AgentState)
    workflow.add_node("analyst", partial(analyst_node, df_context=df_context))
    workflow.add_node("executor", partial(execution_node, df_context=df_context))
    workflow.set_entry_point("analyst")
    workflow.add_edge("analyst", "executor")
    workflow.add_conditional_edges(
        "executor",
        should_continue,
        {
            "analyze": "analyst",
            "end": END
        }
    )
    
    app = workflow.compile()
    initial_state = {"messages": []}
    print("⚡ Agent 正在思考与执行代码...\n")
    
    try:
        for event in app.stream(initial_state, config={"recursion_limit": 15}):
            for node_name, state_update in event.items():
                print(f"   ---> 节点完成: [{node_name}]")
                if node_name == "executor" and "messages" in state_update:
                    last_msg = state_update["messages"][-1]
                    # 打印部分日志以便观察
                    print(f"       📝 执行反馈: {str(last_msg.content)[:100]}...")

    except Exception as e:
        print(f"❌ Agent 运行出错: {e}")
    
    # ---------------------------------------------------------
    # Step 3: 最终成果展示与保存 (Final Result & Save)
    # ---------------------------------------------------------
    print("\n" + "="*60)
    print("🎉 任务完成！结果验证与保存:")
    print("="*60)
    
    final_df = df_context['df']
    
    # 1. 验证：打印 Info
    print("📊 最终数据结构:")
    print(final_df.info())
    
    # 2. 验证：检查缺失值
    missing_count = final_df.isnull().sum().sum()
    if missing_count == 0:
        print("\n✨ 验证通过：所有缺失值已被修复 (NaN count = 0)。")
    else:
        print(f"\n⚠️ 警告：仍有 {missing_count} 个缺失值未处理。")
        print(final_df.isnull().sum())

    # 3. 行动：保存文件 (Persistence)
    output_filename = "cleaned_result.xlsx"
    output_path = os.path.join("data", output_filename)
    
    print(f"\n💾 正在保存文件至: {output_path} ...")
    try:
        # 将清洗后的数据保存为 Excel
        final_df.to_excel(output_path, index=False)
        print(f"✅ 文件保存成功！你可以打开 'data/{output_filename}' 查看最终结果。")
    except Exception as e:
        print(f"❌ 文件保存失败: {e}")

if __name__ == "__main__":
    main()