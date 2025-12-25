# 专门生成脏数据excel文件，用于测试agent三大能力

# Sheet 选择能力（排除封面和干扰项）。
# 表头定位能力（跳过顶部的“机密”、“制表人”等元数据）。
# 数据清洗能力（处理 NaN 缺失值）。
import pandas as pd
import numpy as np
import os

def create_complex_test_excel(file_path: str = "data/complex_test.xlsx"):
    """
    生成一个用于测试的"脏" Excel 文件。
    包含：多个 Sheet、非标准表头、缺失值 (NaN)。
    """
    # 确保目录存在
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # === Sheet 1: 封面 (干扰项) ===
    df_cover = pd.DataFrame({
        '文档说明': ['这是一份内部机密文件', '请勿外传', '数据在下一个 Sheet', 'V1.0 版本']
    })
    
    # === Sheet 2: 真实数据 (核心测试项) ===
    # 构造一个 20 行的数据，模拟真实的销售记录
    data_rows = []
    
    # 插入 3 行"脏"元数据 (Metadata)
    data_rows.append(["公司：ABC 科技集团", "", "", "", ""]) # Row 0
    data_rows.append(["报表类型：季度销售", "密级：高", "", "", ""]) # Row 1
    data_rows.append(["制表人：John Doe", "日期：2024-05-20", "", "", ""]) # Row 2
    
    # 插入真实的表头 (Header) - 在第 4 行 (Index 3)
    cols = ["日期", "产品名称", "地区", "销售额", "利润率"]
    data_rows.append(cols) # Row 3
    
    # 插入模拟数据
    products = ["AI 芯片", "服务器", "云服务", "智能终端"]
    regions = ["华东", "华南", "华北", "海外"]
    
    for i in range(15):
        row = [
            f"2024-05-{i+1:02d}",
            np.random.choice(products),
            np.random.choice(regions),
            np.random.randint(1000, 50000),
            round(np.random.uniform(0.1, 0.4), 2)
        ]
        data_rows.append(row)
        
    # 转化 list 为 DataFrame
    df_main = pd.DataFrame(data_rows)
    
    # 🔥 注入脏数据 (缺失值) 🔥
    # Pandas 在这里还没有把第一行当 header，所以我们要按索引操作
    # 注入一些 NaN 到 "销售额" (索引 3) 和 "地区" (索引 2)
    # 注意：真实数据从第 5 行 (Index 4) 开始
    
    # 制造缺失值：第 6 行的 销售额 设为 NaN
    df_main.iloc[6, 3] = np.nan 
    # 制造缺失值：第 10 行的 地区 设为 NaN
    df_main.iloc[10, 2] = np.nan
    # 制造缺失值：第 12 行的 利润率 设为 NaN
    df_main.iloc[12, 4] = np.nan
    
    # === Sheet 3: 格式说明 (干扰项) ===
    df_notes = pd.DataFrame({
        '字段': ['销售额', '利润'],
        '单位': ['万元', '%']
    })

    # 写入 Excel
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df_cover.to_excel(writer, sheet_name='封面_Cover', index=False)
            # 注意：这里 header=False，index=False，因为我们已经把 header 也就是那一行写在 data_rows 里了
            df_main.to_excel(writer, sheet_name='2024_Q2_销售明细', index=False, header=False)
            df_notes.to_excel(writer, sheet_name='数据字典', index=False)
            
        print(f"🔨 [Generator] 测试文件已生成: {file_path}")
        print(f"   - Sheet 1: 封面 (干扰)")
        print(f"   - Sheet 2: 2024_Q2_销售明细 (真实数据，Header在第3行，含NaN)")
        print(f"   - Sheet 3: 数据字典 (干扰)")
        return file_path
    except Exception as e:
        print(f"❌ 生成测试文件失败: {e}")
        return None