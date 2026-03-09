#!/usr/bin/env python3
"""
检查 Baostock 返回的股票列表数据格式
"""

import pandas as pd
import baostock as bs
from datetime import datetime

# 登录 Baostock
print("登录 Baostock...")
lg = bs.login()
print(f"登录状态: {lg.error_code} {lg.error_msg}")

# 获取股票列表
print("\n获取股票列表...")
today = datetime.now().strftime('%Y-%m-%d')
rs = bs.query_all_stock(day=today)

# 检查返回结果
if rs and rs.error_code == '0':
    print(f"\n返回字段: {rs.fields}")
    
    # 读取前几条数据
    data_list = []
    count = 0
    while rs.next() and count < 10:
        data = rs.get_row_data()
        data_list.append(data)
        count += 1
    
    # 创建 DataFrame
    df = pd.DataFrame(data_list, columns=rs.fields)
    print(f"\n前 10 条数据:")
    print(df)
    
    # 检查是否包含股票名称字段
    if 'stockName' in df.columns:
        print(f"\nstockName 列前 10 个值:")
        print(df['stockName'].head(10).tolist())
    elif 'name' in df.columns:
        print(f"\nname 列前 10 个值:")
        print(df['name'].head(10).tolist())
    else:
        print("\n未找到股票名称字段")
else:
    print(f"获取股票列表失败: {rs.error_code} {rs.error_msg}")

# 登出
bs.logout()
print("\n已登出 Baostock")
