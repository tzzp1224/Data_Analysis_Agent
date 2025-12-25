import sys
import os
import pandas as pd

# 路径 hack
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.ingestion import load_file

def create_test_excel():
    """创建一个用于测试的多 Sheet 复杂 Excel"""
    file_path = "data/complex_test.xlsx"
    
    # Sheet 1: 封面 (干扰项)
    df_cover = pd.DataFrame({'说明': ['这是一份绝密文件', '请翻到下一页查看数据']})
    
    # Sheet 2: 真实数据 (带有乱七八糟的表头)
    # 模拟：前两行是废话，第三行是表头
    data = {
        '无意义A': ['公司报表', '单位：万元', '日期', '2023-01-01', '2023-01-02'],
        '无意义B': [None, None, '销售额', 1000, 2000],
        '无意义C': [None, None, '备注', '正常', '促销']
    }
    df_data = pd.DataFrame(data)
    
    # Sheet 3: 打印设置 (干扰项)
    df_print = pd.DataFrame({'设置': ['A4', '横向']})

    # 写入 Excel
    with pd.ExcelWriter(file_path) as writer:
        df_cover.to_excel(writer, sheet_name='封面', index=False)
        df_data.to_excel(writer, sheet_name='2024年销售明细', index=False, header=False)
        df_print.to_excel(writer, sheet_name='打印参数', index=False)
        
    print(f"🔨 测试文件已生成: {file_path}")
    return file_path

def main():
    # 1. 自动生成测试数据
    file_path = create_test_excel()
    
    print("-" * 50)
    print("🚀 开始智能加载...")
    
    try:
        df = load_file(file_path)
        
        print("\n✅ 加载成功！")
        print(f"📌 数据来源 Sheet: {df.attrs.get('source_sheet', 'Unknown')}")
        print("📊 数据预览 (Top 5):")
        print(df.head())
        print("-" * 30)
        print("📋 最终列名:", df.columns.tolist())
        
    except Exception as e:
        print(f"❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()