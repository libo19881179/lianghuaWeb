#!/usr/bin/env python3
"""
测试修复后的代码功能
"""

import sys
sys.path.append('.')

from unified_data_manager import UnifiedDataManager
from data_sources import DataSourceManager

def test_fix():
    """测试修复后的代码"""
    print("\n========== 测试修复后的代码 ==========")
    
    # 初始化数据源管理器
    print("1. 初始化数据源管理器...")
    ds = DataSourceManager()
    
    # 测试获取A股列表（带重试机制）
    print("\n2. 测试获取A股列表（带重试机制）...")
    all_stocks_df = ds.get_all_a_stock_codes()
    print(f"   获取到 {len(all_stocks_df)} 只A股股票")
    
    if not all_stocks_df.empty:
        print("   前5只股票:")
        print(all_stocks_df.head())
    else:
        print("   未获取到股票数据，可能是网络问题")
    
    # 测试数据管理器
    print("\n3. 测试统一数据管理器...")
    manager = UnifiedDataManager()
    
    # 测试智能刷新（如果有股票数据）
    if not all_stocks_df.empty:
        print("\n4. 测试智能刷新（仅测试前5只股票）...")
        test_codes = all_stocks_df['code'].head(5).tolist()
        
        for i, stock_code in enumerate(test_codes):
            print(f"   [{i+1}/5] 测试股票：{stock_code}")
            try:
                # 检查缓存状态
                min_date, max_date = manager.get_stock_date_range(stock_code)
                print(f"     缓存日期范围: {min_date} ~ {max_date}")
                
                # 测试获取缺失数据范围
                need_start, need_end = manager.get_missing_date_range(
                    stock_code, '2024-01-01', '2024-01-31'
                )
                print(f"     需要补充的日期范围: {need_start} ~ {need_end}")
            except Exception as e:
                print(f"     测试失败: {e}")
    
    print("\n========== 测试完成 ==========")

if __name__ == "__main__":
    test_fix()
