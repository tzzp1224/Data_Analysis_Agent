import sys
import os
import plotly.io as pio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ✅ 修正导入：使用新的 generator 函数名
from app.utils.generator import create_complex_test_data
from app.utils.finance_generator import create_reconciliation_data
# 引入 Ingestion 组件
from app.services.ingestion import propose_ingestion_config, apply_ingestion
from app.services.exporter import save_result_with_audit
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
        try:
            config = propose_ingestion_config(fp)
            
            # 2. 用户确认 (模拟前端弹窗)
            print(f"   🤖 AI 建议: Sheet='{config.sheet_name}', Header在第 {config.header_row} 行")
            print(f"      理由: {config.reason}")
            
            # 模拟用户点击确认 (y)
            user_input = input(f"   👉 是否采用此配置加载 {filename}? (y/n) [y]: ").strip().lower()
            if user_input == 'n':
                print("   (跳过加载)")
                continue
            
            # 3. 执行加载
            df = apply_ingestion(config)
            dfs_context[filename] = df
            print(f"   ✅ 加载成功! Shape: {df.shape}")
            
        except Exception as e:
            print(f"   ❌ 加载失败: {e}")
            
    return dfs_context

def main():
    print("="*50)
    print("🤖 AI Agentic Data Analyst (CLI Mode)")
    print("="*50)

    # 1. ✅ 调用新的生成器函数
    file_paths_1 = create_complex_test_data()
    file_paths_2 = create_reconciliation_data()   # ✅ 新增：财务对账数据
    # 合并文件列表供加载
    all_files = file_paths_1 + file_paths_2
    
    # 2. 交互式加载 (传入所有文件)
    dfs_context = interactive_file_loader(all_files)

    
    if not dfs_context:
        print("没有数据被加载，程序退出。")
        return

    # 3. 初始化 Workflow
    backups_context = {name: df.copy(deep=True) for name, df in dfs_context.items()}
    app = create_workflow(dfs_context, backups_context)
    
    # 清理可能存在的旧状态
    if '__last_result_df__' in dfs_context: del dfs_context['__last_result_df__']
    if '__last_audit__' in dfs_context: del dfs_context['__last_audit__']

    print("\n" + "="*50)
    print("🤖 系统已就绪。支持：清洗 / 模糊匹配 / 审计 / 导出")
    print("提示：试试输入 '请清洗数据并导出' 或 '合并表格并导出'")
    print("="*50)
    
    # 保存历史 context
    state = {
        "messages": [], 
        "user_instruction": "", 
        "error_count": 0,
        "chart_jsons": [],
        "reply": ""
    }

    while True:
        try:
            instruction = input("\n💬 请输入指令 (exit退出): ").strip()
        except EOFError:
            break
            
        if instruction.lower() == 'exit':
            break
        if not instruction:
            continue
        
        # 更新指令
        state["user_instruction"] = instruction
        state["error_count"] = 0 
        
        print(f"⚙️ 正在思考...")
        try:
            for event in app.stream(state, config={"recursion_limit": 25}):
                for key, val in event.items():
                    if key == "executor":
                        # 打印执行日志
                        if "messages" in val:
                            log = val['messages'][-1].content
                            # 提取关键信息打印
                            if "✅" in log:
                                print(f"   ✅ 执行成功")
                            elif "❌" in log:
                                print(f"   ❌ 执行报错 (正在自愈...)")
                            # 如果有 Print 输出的 Insights，也可以在这里看到
                            
                        # 处理 Excel 导出
                        if '__last_result_df__' in dfs_context:
                            res_df = dfs_context.pop('__last_result_df__')
                            # 获取 Audit 对象 (如果有)
                            audit_logger = dfs_context.pop('__last_audit__', None)
                            
                            output_path = "data/output_result.xlsx"
                            os.makedirs(os.path.dirname(output_path), exist_ok=True)
                            
                            save_result_with_audit(res_df, audit_logger, output_path)
                            print(f"   💾 [交付] 结果文件已保存至: {output_path}")
                            if audit_logger:
                                print(f"      (包含审计日志 Sheet)")

                        # 处理图表
                        if "chart_jsons" in val and val["chart_jsons"]:
                            print(f"   🎨 [交付] 生成了 {len(val['chart_jsons'])} 张图表 (data/chart.html)")
                            pio.from_json(val['chart_jsons'][0]).write_html("data/chart.html")
                            
        except Exception as e:
            print(f"❌ 系统错误: {e}")

if __name__ == "__main__":
    main()
