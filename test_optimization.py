#!/usr/bin/env python3
"""
测试优化后的代码功能
"""

import sys
sys.path.append('.')

from unified_data_manager import UnifiedDataManager
from data_sources import DataSourceManager

def test_optimization():
    """测试优化后的代码"""
    print("\n========== 测试优化后的代码 ==========")
    
    # 初始化数据源管理器
    print("1. 初始化数据源管理器...")
    ds = DataSourceManager()
    
    # 测试获取A股列表
    print("\n2. 测试获取A股列表...")
    all_stocks_df = ds.get_all_a_stock_codes()
    print(f"   获取到 {len(all_stocks_df)} 只A股股票")
    
    if not all_stocks_df.empty:
        print("   前5只股票:")
        print(all_stocks_df.head())
        
        # 检查数据格式
        print("\n3. 检查数据格式...")
        if 'code' in all_stocks_df.columns:
            print(f"   代码列存在: {all_stocks_df['code'].dtype}")
            # 检查代码格式
            sample_codes = all_stocks_df['code'].head(5).tolist()
            print(f"   代码示例: {sample_codes}")
            # 检查是否为标准格式
            for code in sample_codes:
                if isinstance(code, str):
                    if code.startswith('sh.') or code.startswith('sz.'):
                        print(f"   ✓ {code} 格式正确")
                    else:
                        print(f"   ✗ {code} 格式错误")
        
        if 'name' in all_stocks_df.columns:
            print(f"   名称列存在: {all_stocks_df['name'].dtype}")
            sample_names = all_stocks_df['name'].head(5).tolist()
            print(f"   名称示例: {sample_names}")
    
    # 测试数据管理器
    print("\n4. 测试统一数据管理器...")
    manager = UnifiedDataManager()
    
    # 测试获取股票日期范围
    if not all_stocks_df.empty:
        test_code = all_stocks_df['code'].iloc[0] if 'code' in all_stocks_df.columns else None
        if test_code:
            print(f"   测试股票: {test_code}")
            min_date, max_date = manager.get_stock_date_range(test_code)
            print(f"   缓存日期范围: {min_date} ~ {max_date}")
    
    print("\n========== 测试完成 ==========")

if __name__ == "__main__":
    test_optimization()
