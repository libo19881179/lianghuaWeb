#!/usr/bin/env python3
"""
测试股票名称提取功能
"""

import pandas as pd
import os
from unified_data_manager import UnifiedDataManager

# 初始化数据管理器
dm = UnifiedDataManager()

# 获取所有缓存的股票代码
stocks = dm.get_all_stocks()
print(f"缓存的股票数量: {len(stocks)}")

# 测试前几个股票的名称提取
print("\n测试股票名称提取:")
test_stocks = stocks[:5]  # 测试前5个股票

for code in test_stocks:
    # 获取股票数据
    df = dm.get_stock_data(code)
    if df is not None and not df.empty:
        # 尝试提取股票名称
        name = ''
        if 'name' in df.columns and not df['name'].empty:
            name_values = df['name'].dropna().unique()
            if len(name_values) > 0:
                name = name_values[0]
        print(f"{code}: {name}")
    else:
        print(f"{code}: 无数据")

print("\n测试完成！")
