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
    print("🤖 Agentic ETL Analyst (Excel Output Mode)")
    print("="*50)

    # 1. 生成并加载
    file_paths = create_multi_file_test_data()
    dfs_context = {}
    for fp in file_paths:
        dfs_context[os.path.basename(fp)] = load_file(fp)

    # ---------------------------------------------------------
    # 🧪 场景: 筛选 + 输出文件
    # ---------------------------------------------------------
    instruction = "请筛选出所有编号为a001的销量，输出为新表格"
    
    print("\n" + "-"*50)
    print(f"🧪 指令: {instruction}")
    print("-" * 50)
    
    app = create_workflow(dfs_context)
    state = {
        "messages": [], 
        "user_instruction": instruction, 
        "error_count": 0,
        "chart_jsons": []
    }
    
    # 清理掉可能存在的旧结果
    if '__last_result_df__' in dfs_context:
        del dfs_context['__last_result_df__']
    
    try:
        for event in app.stream(state, config={"recursion_limit": 25}):
            for key, val in event.items():
                print(f"--> Node: {key}")
                
                if key == "executor":
                    # 1. 打印文本日志
                    if "messages" in val:
                        print(f"    📝 Log: {val['messages'][-1].content[:100]}...")
                    
                    # 2. 检查是否有文件输出信号
                    # 我们检查 dfs_context 中是否有被写入 __last_result_df__
                    if '__last_result_df__' in dfs_context:
                        result_df = dfs_context.pop('__last_result_df__') # 取出并删除，防止重复
                        
                        output_path = "data/output_result.xlsx"
                        print(f"    💾 [System] 检测到结果表格，正在保存至 {output_path}...")
                        result_df.to_excel(output_path, index=False)
                        print(f"    ✅ 文件保存成功! Rows: {len(result_df)}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()