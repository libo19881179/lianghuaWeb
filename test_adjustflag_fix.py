#!/usr/bin/env python3
"""
测试 adjustflag 列类型转换修复
"""

import sys
from data_sources import DataSourceManager
from unified_data_manager import UnifiedDataManager

# 测试股票代码
TEST_STOCK = "sh.600000"  # 浦发银行
TEST_START_DATE = "2026-03-07"
TEST_END_DATE = "2026-03-09"

print("=== 测试 adjustflag 列类型转换修复 ===")

# 初始化数据源
ds = DataSourceManager()
print(f"1. 数据源初始化 {'成功' if ds.bs_login_status else '失败'}")

if not ds.bs_login_status:
    print("数据源登录失败，无法继续测试")
    sys.exit(1)

# 初始化数据管理器
dm = UnifiedDataManager()
print("2. 数据管理器初始化成功")

# 测试获取数据
print(f"3. 获取股票 {TEST_STOCK} 数据...")
df = ds.get_stock_data(TEST_STOCK, TEST_START_DATE, TEST_END_DATE)

if df is not None and not df.empty:
    print(f"   ✓ 数据获取成功，共 {len(df)} 条记录")
    print(f"   ✓ 列名: {list(df.columns)}")
    
    # 检查 adjustflag 列
    if 'adjustflag' in df.columns:
        print(f"   ✓ adjustflag 列存在")
        print(f"   ✓ adjustflag 数据类型: {df['adjustflag'].dtype}")
        print(f"   ✓ adjustflag 示例值: {df['adjustflag'].head()}")
    else:
        print("   ✗ adjustflag 列不存在")
    
    # 测试存储数据
    print("4. 测试存储数据...")
    try:
        dm.save_stock_data(TEST_STOCK, df, stock_name="浦发银行")
        print("   ✓ 数据存储成功")
        
        # 测试加载数据
        print("5. 测试加载数据...")
        loaded_df = dm.get_stock_data(TEST_STOCK, TEST_START_DATE, TEST_END_DATE)
        if loaded_df is not None and not loaded_df.empty:
            print(f"   ✓ 数据加载成功，共 {len(loaded_df)} 条记录")
            print(f"   ✓ 加载后 adjustflag 列数据类型: {loaded_df['adjustflag'].dtype}")
        else:
            print("   ✗ 数据加载失败")
            
    except Exception as e:
        print(f"   ✗ 数据存储失败: {e}")
else:
    print("   ✗ 数据获取失败")

# 退出登录
try:
    ds.logout()
except Exception as e:
    print(f"   ⚠ 登出失败：{e}")
print("6. 测试完成，已退出登录")
