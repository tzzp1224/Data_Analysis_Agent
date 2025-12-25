import pandas as pd
import numpy as np
import os

def create_multi_file_test_data(data_dir: str = "data"):
    os.makedirs(data_dir, exist_ok=True)
    
    # === 文件 1: 销售明细 (Sales) ===
    # 包含：日期, 产品ID, 销量
    df_sales = pd.DataFrame({
        '日期': pd.date_range(start='2024-01-01', periods=20),
        '产品ID': np.random.choice(['P001', 'P002', 'P003'], 20),
        '销量': np.random.randint(10, 100, 20)
    })
    path_sales = os.path.join(data_dir, "sales.xlsx")
    df_sales.to_excel(path_sales, index=False)
    
    # === 文件 2: 产品信息 (Products) ===
    # 包含：产品ID, 产品名称, 类别
    df_products = pd.DataFrame({
        '产品ID': ['P001', 'P002', 'P003'],
        '产品名称': ['高性能显卡', '机械键盘', '电竞鼠标'],
        '类别': ['硬件', '外设', '外设']
    })
    path_products = os.path.join(data_dir, "products.xlsx")
    df_products.to_excel(path_products, index=False)
    
    print(f"🔨 [Generator] 已生成多文件测试数据:\n  - {path_sales}\n  - {path_products}")
    return [path_sales, path_products]