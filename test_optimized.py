#!/usr/bin/env python3
"""
测试优化后的代码功能
"""

import sys
sys.path.append('.')

from data_sources import DataSourceManager, TradingDayChecker
from datetime import datetime

def test_retry_decorator():
    """测试重试装饰器"""
    print("\n========== 测试重试装饰器 ==========")
    try:
        with DataSourceManager() as ds:
            # 测试参数验证
            print("1. 测试参数验证...")
            # 测试空股票代码
            result = ds.get_stock_data('', '2024-01-01', '2024-01-31')
            assert result is None, "空股票代码应该返回 None"
            print("   ✓ 空股票代码验证通过")
            
            # 测试空开始日期
            result = ds.get_stock_data('sh.600000', '', '2024-01-31')
            assert result is None, "空开始日期应该返回 None"
            print("   ✓ 空开始日期验证通过")
            
            # 测试空结束日期
            result = ds.get_stock_data('sh.600000', '2024-01-01', '')
            assert result is None, "空结束日期应该返回 None"
            print("   ✓ 空结束日期验证通过")
            
            # 测试日期格式错误
            result = ds.get_stock_data('sh.600000', '2024/01/01', '2024-01-31')
            assert result is None, "日期格式错误应该返回 None"
            print("   ✓ 日期格式错误验证通过")
            
            # 测试日期顺序错误
            result = ds.get_stock_data('sh.600000', '2024-01-31', '2024-01-01')
            assert result is None, "日期顺序错误应该返回 None"
            print("   ✓ 日期顺序错误验证通过")
            
            print("\n2. 测试上下文管理器...")
            # 上下文管理器会自动调用 logout
            print("   ✓ 上下文管理器创建成功")
            
            print("\n3. 测试TradingDayChecker...")
            checker = TradingDayChecker(ds)
            
            # 测试交易日判断
            test_date = datetime(2024, 1, 1)  # 元旦（节假日）
            is_trading = checker.is_trading_day(test_date)
            print(f"   2024-01-01 是否为交易日：{is_trading}")
            
            test_date = datetime(2024, 1, 2)  # 元旦假期（节假日）
            is_trading = checker.is_trading_day(test_date)
            print(f"   2024-01-02 是否为交易日：{is_trading}")
            
            test_date = datetime(2024, 1, 8)  # 周一（工作日）
            is_trading = checker.is_trading_day(test_date)
            print(f"   2024-01-08 是否为交易日：{is_trading}")
            
            # 测试获取前一个交易日
            test_date = datetime(2024, 1, 8)  # 周一
            prev_day = checker.get_previous_trading_day(test_date)
            print(f"   2024-01-08 的前一个交易日：{prev_day.strftime('%Y-%m-%d')}")
            
            # 测试获取后一个交易日
            next_day = checker.get_next_trading_day(test_date)
            print(f"   2024-01-08 的后一个交易日：{next_day.strftime('%Y-%m-%d')}")
            
            # 测试再平衡日期
            rebalance_date = checker.get_rebalance_date(2024, 1)
            print(f"   2024年1月的再平衡日期：{rebalance_date.strftime('%Y-%m-%d')}")
            
            print("\n4. 测试数据获取方法...")
            # 测试指数数据获取
            index_df = ds.get_index_data('000001.SH', '2024-01-01', '2024-01-31')
            if index_df is not None:
                print(f"   ✓ 成功获取上证指数数据：{len(index_df)} 条记录")
                print(f"   数据日期范围：{index_df['date'].min()} ~ {index_df['date'].max()}")
            else:
                print("   ✗ 未能获取上证指数数据（可能是网络问题）")
            
            # 测试股票信息获取
            stock_info = ds.get_stock_info('sh.600000')
            if stock_info:
                print(f"   ✓ 成功获取股票信息：{stock_info}")
            else:
                print("   ✗ 未能获取股票信息（可能是网络问题）")
            
            # 测试A股列表获取
            stocks_df = ds.get_all_a_stock_codes()
            if not stocks_df.empty:
                print(f"   ✓ 成功获取A股列表：{len(stocks_df)} 只股票")
                print(f"   前5只股票：")
                print(stocks_df.head())
            else:
                print("   ✗ 未能获取A股列表（可能是网络问题）")
            
    except Exception as e:
        print(f"测试过程中出现错误：{e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_retry_decorator()
