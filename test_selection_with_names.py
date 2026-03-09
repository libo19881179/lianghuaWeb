#!/usr/bin/env python3
"""
测试选股结果中的股票名称显示
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from data_sources import DataSourceManager
from unified_data_manager import UnifiedDataManager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 测试选股结果中的股票名称
print("=== 测试选股结果中的股票名称 ===")

# 初始化数据管理器
dm = UnifiedDataManager()

# 获取所有缓存的股票代码
stocks = dm.get_all_stocks()
print(f"\n缓存的股票数量: {len(stocks)}")

# 选择前几个股票进行测试
test_stocks = stocks[:10]  # 测试前10个股票

# 准备测试数据
data_dict = {}
for code in test_stocks:
    df = dm.get_stock_data(code)
    if df is not None and not df.empty:
        data_dict[code] = df

print(f"\n加载测试数据: {len(data_dict)} 只股票")

# 从 Baostock 获取股票名称
stock_name_map = {}
ds = None
try:
    ds = DataSourceManager()
    all_stocks_df = ds.get_all_a_stock_codes()
    
    if all_stocks_df is not None:
        for _, row in all_stocks_df.iterrows():
            if 'code' in row and 'name' in row:
                stock_name_map[row['code']] = row['name']
except Exception as e:
    logger.warning(f"获取股票名称失败：{e}")
finally:
    # 确保无论是否发生异常，都会执行 logout 操作
    if ds is not None:
        try:
            ds.logout()
        except Exception as e:
            logger.warning(f"登出 Baostock 失败：{e}")

# 构建股票列表
stock_list = []
for code in data_dict.keys():
    name = stock_name_map.get(code, '')
    stock_list.append({'code': code, 'name': name})

print("\n股票列表（包含名称）:")
for stock in stock_list:
    print(f"  {stock['code']}: {stock['name']}")

# 测试策略选股
from strategy_selector import StrategyCombiner

combiner = StrategyCombiner()

# 简单测试：使用默认策略
selected_stocks = combiner.combine_strategies(
    stock_list,
    data_dict,
    selected_strategies=['value_multifactor'],
    weights={'value_multifactor': 1.0}
)

print(f"\n选股结果: {len(selected_stocks)} 只股票")
print("\n选股结果详情:")
for stock in selected_stocks:
    print(f"  {stock['code']}: {stock['name']} - 权重: {stock['weight']:.2f}")

print("\n测试完成！")
