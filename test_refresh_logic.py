#!/usr/bin/env python3
"""
测试智能刷新逻辑
"""

import sys
import os
from datetime import datetime

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_sources import DataSourceManager
from unified_data_manager import UnifiedDataManager

def test_trading_day_check():
    """测试交易日检查"""
    print("\n=== 测试交易日检查 ===")
    
    with DataSourceManager() as ds:
        # 测试今天
        today = datetime.now().strftime('%Y-%m-%d')
        is_today_trading = ds.is_trading_day(today)
        print(f"今天 {today} {'是' if is_today_trading else '不是'} 交易日")
        
        # 测试周末
        # 假设今天是工作日，测试周六
        from datetime import timedelta
        saturday = (datetime.now() + timedelta(days=(5 - datetime.now().weekday() + 7) % 7)).strftime('%Y-%m-%d')
        is_saturday_trading = ds.is_trading_day(saturday)
        print(f"周六 {saturday} {'是' if is_saturday_trading else '不是'} 交易日")
        
        # 测试周日
        sunday = (datetime.now() + timedelta(days=(6 - datetime.now().weekday() + 7) % 7)).strftime('%Y-%m-%d')
        is_sunday_trading = ds.is_trading_day(sunday)
        print(f"周日 {sunday} {'是' if is_sunday_trading else '不是'} 交易日")

def test_refresh_logic():
    """测试智能刷新逻辑"""
    print("\n=== 测试智能刷新逻辑 ===")
    
    # 初始化数据管理器
    dm = UnifiedDataManager()
    
    # 初始化数据源
    ds = DataSourceManager()
    
    try:
        # 执行智能刷新
        result = dm.refresh_all_stocks(ds, start_date='2024-01-01')
        
        print("\n刷新结果:")
        print(f"成功: {result.get('success', False)}")
        if 'total_stocks' in result:
            print(f"总计: {result['total_stocks']}只")
            print(f"缓存: {result['cached_stocks']}")
            print(f"新增: {result['new_stocks']}")
            print(f"更新: {result['updated_stocks']}")
            print(f"失败: {result['failed_stocks']}")
        elif 'error' in result:
            print(f"错误: {result['error']}")
    finally:
        # 登出 Baostock
        ds.logout()

if __name__ == "__main__":
    # 测试交易日检查
    test_trading_day_check()
    
    # 测试智能刷新逻辑
    test_refresh_logic()
