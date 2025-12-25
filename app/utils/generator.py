import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

def create_complex_test_data(data_dir: str = "data"):
    """
    生成用于测试 '可信审计' 和 '模糊匹配' 的高难度测试数据。
    """
    os.makedirs(data_dir, exist_ok=True)
    
    # ==========================================
    # 1. 生成标准客户表 (Standard Clients)
    # ==========================================
    # 这是我们的“字典”或“主数据”
    clients_data = {
        '客户ID': ['C001', 'C002', 'C003', 'C004', 'C005'],
        '标准公司名': [
            '腾讯科技有限公司',      # Tencent
            '阿里巴巴集团控股',      # Alibaba
            '字节跳动有限公司',      # ByteDance
            '京东世纪贸易有限公司',  # JD
            '美团点评集团'          # Meituan
        ],
        '行业': ['互联网', '电商', '社交/短视频', '电商物流', '本地生活'],
        '客户等级': ['KA', 'KA', 'KA', 'A', 'A']
    }
    df_clients = pd.DataFrame(clients_data)
    path_clients = os.path.join(data_dir, "standard_clients.xlsx")
    df_clients.to_excel(path_clients, index=False)
    
    # ==========================================
    # 2. 生成脏销售数据 (Dirty Sales Data)
    # ==========================================
    # 这里包含了所有需要 Agent 清洗和模糊匹配的“坑”
    
    # 模糊匹配映射 (标准名 -> 各种乱七八糟的写法)
    fuzzy_map = {
        '腾讯科技有限公司': ['腾讯', '腾讯科技', 'Tencent', '腾讯深圳'],
        '阿里巴巴集团控股': ['阿里巴巴', '阿里', 'AliBaba Group', '淘宝网络'],
        '字节跳动有限公司': ['字节', '字节跳动', 'ByteDance', '今日头条'],
        '京东世纪贸易有限公司': ['京东', 'JD.com', '京东商城'],
        '美团点评集团': ['美团', '美团网', 'Meituan']
    }
    
    rows = []
    start_date = datetime(2024, 1, 1)
    
    # 生成 50 条基础数据
    for i in range(50):
        # 随机选一个标准客户，然后取其“脏名字”
        std_name = random.choice(list(fuzzy_map.keys()))
        dirty_name = random.choice(fuzzy_map[std_name])
        
        row = {
            '订单号': f"ORD-{20240000 + i}",
            '日期': (start_date + timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            '客户名称': dirty_name, # 这里是需要 Fuzzy Merge 的列
            '产品': random.choice(['云服务器', '企业邮箱', 'SaaS订阅', '广告推广']),
            '单价': round(random.uniform(100, 5000), 2),
            '数量': random.randint(1, 10),
            '状态': '已完成'
        }
        # 计算总价 (稍后会故意制造错误)
        row['总金额'] = row['单价'] * row['数量']
        rows.append(row)

    df_sales = pd.DataFrame(rows)

    # ------------------------------------------
    # 😈 开始埋雷 (制造脏数据)
    # ------------------------------------------
    
    # 1. 制造重复行 (Duplicates)
    # 复制第 0 行和第 5 行并追加到末尾
    df_sales = pd.concat([df_sales, df_sales.iloc[[0, 5]]], ignore_index=True)
    
    # 2. 制造空值 (Nulls)
    # 将第 10, 15 行的“总金额”设为空
    df_sales.loc[10, '总金额'] = np.nan
    df_sales.loc[15, '客户名称'] = None 
    
    # 3. 制造业务异常值 (Outliers) -> 需要 Audit 剔除
    # 将第 20 行的“单价”设为负数 (退款逻辑? 但这里假设是错误)
    df_sales.loc[20, '单价'] = -100.00
    df_sales.loc[20, '总金额'] = -500.00
    
    # 将第 25 行的“数量”设为异常大
    df_sales.loc[25, '数量'] = 100000 
    
    # 4. 制造格式错误 (Type Issues) -> 需要清洗
    # 将第 30 行的“总金额”变成字符串 "1,000.00"
    df_sales.loc[30, '总金额'] = "1,000.00"
    
    path_sales = os.path.join(data_dir, "dirty_sales_data.xlsx")
    df_sales.to_excel(path_sales, index=False)

    print(f"🔨 [Generator] 已生成高难度测试数据:")
    print(f"  - {path_sales} (含脏数据、空值、异常值、重复行)")
    print(f"  - {path_clients} (标准客户名)")
    
    return [path_sales, path_clients]