#!/usr/bin/env python3
"""
测试增量更新场景下的 adjustflag 列类型处理
"""

import sys
import pandas as pd
from data_sources import DataSourceManager
from unified_data_manager import UnifiedDataManager

# 测试股票代码
TEST_STOCK = "sh.600004"  # 白云机场
TEST_START_DATE1 = "2026-03-01"
TEST_END_DATE1 = "2026-03-05"
TEST_START_DATE2 = "2026-03-06"
TEST_END_DATE2 = "2026-03-09"

print("=== 测试增量更新场景 ===")

# 初始化数据源
ds = DataSourceManager()
print(f"1. 数据源初始化 {'成功' if ds.bs_login_status else '失败'}")

if not ds.bs_login_status:
    print("数据源登录失败，无法继续测试")
    sys.exit(1)

# 初始化数据管理器
dm = UnifiedDataManager()
print("2. 数据管理器初始化成功")

# 第一次获取数据（模拟历史数据）
print(f"3. 第一次获取股票 {TEST_STOCK} 数据...")
df1 = ds.get_stock_data(TEST_STOCK, TEST_START_DATE1, TEST_END_DATE1)

if df1 is not None and not df1.empty:
    print(f"   ✓ 数据获取成功，共 {len(df1)} 条记录")
    print(f"   ✓ adjustflag 数据类型: {df1['adjustflag'].dtype}")
    
    # 保存第一次数据
    print("4. 保存第一次数据...")
    try:
        dm.save_stock_data(TEST_STOCK, df1, stock_name="白云机场")
        print("   ✓ 数据存储成功")
    except Exception as e:
        print(f"   ✗ 数据存储失败: {e}")
        sys.exit(1)
else:
    print("   ✗ 数据获取失败")
    sys.exit(1)

# 第二次获取数据（模拟增量更新）
print(f"5. 第二次获取股票 {TEST_STOCK} 数据...")
df2 = ds.get_stock_data(TEST_STOCK, TEST_START_DATE2, TEST_END_DATE2)

if df2 is not None and not df2.empty:
    print(f"   ✓ 数据获取成功，共 {len(df2)} 条记录")
    print(f"   ✓ adjustflag 数据类型: {df2['adjustflag'].dtype}")
    
    # 模拟增量更新：合并数据
    print("6. 模拟增量更新 - 合并数据...")
    existing_df = dm.get_stock_data(TEST_STOCK)
    if existing_df is not None:
        print(f"   ✓ 加载现有数据，共 {len(existing_df)} 条记录")
        print(f"   ✓ 现有数据 adjustflag 类型: {existing_df['adjustflag'].dtype}")
        
        # 合并数据
        combined_df = pd.concat([existing_df, df2], ignore_index=True).drop_duplicates(subset=['date'], keep='last')
        print(f"   ✓ 合并后数据，共 {len(combined_df)} 条记录")
        print(f"   ✓ 合并后 adjustflag 类型: {combined_df['adjustflag'].dtype}")
        
        # 保存合并后的数据
        print("7. 保存合并后的数据...")
        try:
            dm.save_stock_data(TEST_STOCK, combined_df, stock_name="白云机场")
            print("   ✓ 数据存储成功")
        except Exception as e:
            print(f"   ✗ 数据存储失败: {e}")
            sys.exit(1)
    else:
        print("   ✗ 无法加载现有数据")
        sys.exit(1)
else:
    print("   ✗ 数据获取失败")
    sys.exit(1)

# 测试加载最终数据
print("8. 测试加载最终数据...")
final_df = dm.get_stock_data(TEST_STOCK, TEST_START_DATE1, TEST_END_DATE2)
if final_df is not None and not final_df.empty:
    print(f"   ✓ 数据加载成功，共 {len(final_df)} 条记录")
    print(f"   ✓ 最终数据 adjustflag 类型: {final_df['adjustflag'].dtype}")
    print(f"   ✓ 数据范围: {final_df['date'].min()} ~ {final_df['date'].max()}")
else:
    print("   ✗ 数据加载失败")

# 退出登录
try:
    ds.logout()
except Exception as e:
    print(f"   ⚠ 登出失败：{e}")
print("9. 测试完成，已退出登录")
