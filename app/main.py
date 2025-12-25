import sys
import os
import pandas as pd
import plotly.io as pio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.generator import create_multi_file_test_data
from app.services.ingestion import load_file
from app.services.workflow import create_workflow

def main():
    print("="*50)
    print("🤖 Multi-File Agentic Analyst")
    print("="*50)

    # 1. 生成并加载多个文件
    file_paths = create_multi_file_test_data()
    
    # 构建 Data Context: {'sales.xlsx': df1, 'products.xlsx': df2}
    dfs_context = {}
    print("\n🔍 Loading Files:")
    for fp in file_paths:
        filename = os.path.basename(fp)
        try:
            df = load_file(fp)
            dfs_context[filename] = df
            print(f"  ✅ Loaded: {filename} {df.shape}")
        except Exception as e:
            print(f"  ❌ Failed: {filename} - {e}")

    # ---------------------------------------------------------
    # 🧪 场景 1: 默认 Auto EDA (多图展示)
    # ---------------------------------------------------------
    print("\n" + "-"*50)
    print("🧪 场景 1: 用户无指令 -> 触发 Auto EDA (多图)")
    print("-" * 50)
    
    app = create_workflow(dfs_context)
    state_1 = {"messages": [], "user_instruction": "", "error_count": 0, "chart_jsons": []}
    
    try:
        for event in app.stream(state_1, config={"recursion_limit": 25}):
            for key, val in event.items():
                print(f"--> Node: {key}")
                if "router_decision" in val:
                    print(f"    🧠 决策: {val['router_decision']}")
                
                if key == "executor" and "chart_jsons" in val:
                    charts = val['chart_jsons']
                    print(f"    🎨 生成了 {len(charts)} 张图表")
                    # 保存所有图表
                    for idx, c_json in enumerate(charts):
                        pio.from_json(c_json).write_html(f"data/eda_chart_{idx+1}.html")
                    print("    ✨ 图表已保存至 data/eda_chart_*.html")

    except Exception as e:
        print(f"Error: {e}")

    # ---------------------------------------------------------
    # 🧪 场景 2: 多文件关联操作
    # ---------------------------------------------------------
    print("\n" + "-"*50)
    print("🧪 场景 2: 多文件操作 (Merge)")
    print("指令: '把销售表和产品表合并，然后画一个各类别销量的柱状图'")
    print("-" * 50)
    
    state_2 = {
        "messages": [], 
        "user_instruction": "请把 sales.xlsx 和 products.xlsx 根据产品ID合并，统计各类别的总销量，并画柱状图。", 
        "error_count": 0,
        "chart_jsons": []
    }
    
    try:
        for event in app.stream(state_2, config={"recursion_limit": 25}):
            for key, val in event.items():
                print(f"--> Node: {key}")
                if key == "executor" and "chart_jsons" in val:
                     if val['chart_jsons']:
                        pio.from_json(val['chart_jsons'][0]).write_html("data/merge_chart.html")
                        print("    ✨ 合并分析图表已保存: data/merge_chart.html")
    except Exception as e:
        print(f"Error: {e}")

    # ---------------------------------------------------------
    # 🧪 场景 3: 无关指令 (Rejection)
    # ---------------------------------------------------------
    print("\n" + "-"*50)
    print("🧪 场景 3: 无关指令 (Reject)")
    print("指令: '给我讲个笑话'")
    print("-" * 50)
    
    state_3 = {"messages": [], "user_instruction": "给我讲个笑话", "error_count": 0}
    
    try:
        for event in app.stream(state_3, config={"recursion_limit": 10}):
            for key, val in event.items():
                print(f"--> Node: {key}")
                if key == "general_chat":
                    print(f"    🤖 回复: {val['messages'][0].content}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()