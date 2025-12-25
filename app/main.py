import sys
import os
import pandas as pd
import plotly.io as pio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 引入新的 Ingestion 组件
from app.services.ingestion import propose_ingestion_config, apply_ingestion
from app.utils.generator import create_multi_file_test_data
from app.services.workflow import create_workflow

def interactive_file_loader(file_paths: list):
    """
    模拟前端的 '交互式文件导入' 过程
    """
    dfs_context = {}
    print("\n" + "="*50)
    print("📂 交互式数据摄取 (Interactive Ingestion)")
    print("="*50)
    
    for fp in file_paths:
        filename = os.path.basename(fp)
        print(f"\n🔍 正在分析文件结构: {filename} ...")
        
        # 1. AI 提案
        config = propose_ingestion_config(fp)
        
        # 2. 用户确认 (模拟前端弹窗)
        print(f"   🤖 AI 建议: Sheet='{config.sheet_name}', Header在第 {config.header_row} 行")
        print(f"      理由: {config.reason}")
        
        # 在这里，用户可以输入 'n' 来拒绝，然后手动输入参数（后端逻辑暂略，先模拟同意）
        user_input = input(f"   👉 是否采用此配置加载 {filename}? (y/n/edit) [y]: ").strip().lower()
        
        if user_input == 'n':
            print("   (此处应弹出前端表单让用户手动选 Sheet，暂跳过)")
            continue # 或者 break
        
        # 3. 执行加载
        try:
            df = apply_ingestion(config)
            dfs_context[filename] = df
            print(f"   ✅ 加载成功! Shape: {df.shape}")
        except Exception as e:
            print(f"   ❌ 加载失败: {e}")
            
    return dfs_context

def main():
    # 1. 生成数据
    file_paths = create_multi_file_test_data()
    
    # 2. 交互式加载 (解决痛点：黑盒)
    dfs_context = interactive_file_loader(file_paths)
    
    if not dfs_context:
        print("没有数据被加载，程序退出。")
        return

    # 3. 进入指令循环 (模拟聊天框)
    app = create_workflow(dfs_context)
    
    print("\n" + "="*50)
    print("🤖 AI 数据分析师已就绪 (输入 'exit' 退出)")
    print("支持能力：清洗 / 合并 / 分析 / 画图 / 导出")
    print("="*50)
    
    # 保存历史 context
    state = {
        "messages": [], 
        "user_instruction": "", 
        "error_count": 0,
        "chart_jsons": []
    }

    while True:
        instruction = input("\n💬 请输入指令: ").strip()
        if instruction.lower() == 'exit':
            break
        
        # 更新指令
        state["user_instruction"] = instruction
        # 每次新指令重置 error count，但保留 messages 历史（为了多轮对话）
        state["error_count"] = 0 
        
        print(f"⚙️ 正在思考...")
        try:
            for event in app.stream(state, config={"recursion_limit": 25}):
                for key, val in event.items():
                    # 实时反馈
                    if key == "supervisor":
                        print(f"   🧠 决策: {val.get('router_decision')}")
                    
                    if key == "executor":
                        # 打印执行日志
                        if "messages" in val:
                            log = val['messages'][-1].content
                            # 简单的日志清洗，只显示关键信息
                            if "✅" in log:
                                print(f"   ✅ 执行成功")
                            elif "❌" in log:
                                print(f"   ❌ 执行报错 (正在自愈...)")
                        
                        # 处理文件导出
                        if '__last_result_df__' in dfs_context:
                            res_df = dfs_context.pop('__last_result_df__')
                            output_path = "data/output_result.xlsx"
                            res_df.to_excel(output_path, index=False)
                            print(f"   💾 [交付] 结果文件已保存: {output_path}")

                        # 处理图表
                        if "chart_jsons" in val and val["chart_jsons"]:
                            print(f"   🎨 [交付] 生成了 {len(val['chart_jsons'])} 张图表 (data/chart.html)")
                            pio.from_json(val['chart_jsons'][0]).write_html("data/chart.html")
            
            # 更新 state 为最新状态，以便下一轮对话有记忆
            # 注意：LangGraph 的 stream 会自动处理 state 更新，
            # 但如果你想在外部维护 state，需要从 event 的最后一个状态获取
            # 这里简化处理，假设 graph 内部维护了 memory (实际上我们需要 checkpointer 才能真正实现多轮记忆，这里暂用简单模式)
            
        except Exception as e:
            print(f"❌ 系统错误: {e}")

if __name__ == "__main__":
    main()