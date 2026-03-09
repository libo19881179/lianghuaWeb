#!/usr/bin/env python3
"""
测试修复后的错误处理逻辑
"""

import logging
from data_sources import DataSourceManager
from unified_data_manager import UnifiedDataManager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("=== 测试错误处理逻辑 ===")

# 测试数据获取
data_source = DataSourceManager()

# 测试一只可能停牌的股票
test_stock = "sh.600538"  # 国发股份
print(f"\n测试股票: {test_stock}")

# 尝试获取数据
df = data_source.get_stock_data(test_stock, "2026-03-07", "2026-03-09")
print(f"数据获取结果: {'成功' if df is not None and not df.empty else '无数据'}")

# 测试数据管理器的处理
print("\n测试数据管理器处理...")
dm = UnifiedDataManager()

# 模拟增量更新场景
print(f"模拟增量更新 {test_stock}...")
try:
    # 检查日期范围
    cached_min, cached_max = dm.get_stock_date_range(test_stock)
    print(f"缓存日期范围: {cached_min} ~ {cached_max}")
    
    # 尝试增量更新
    if cached_min is not None:
        need_start, need_end = dm.get_missing_date_range(test_stock, "2026-03-01", "2026-03-09")
        if need_start and need_end:
            print(f"需要更新的日期范围: {need_start} ~ {need_end}")
            df_new = data_source.get_stock_data(test_stock, need_start, need_end)
            print(f"增量数据获取结果: {'成功' if df_new is not None and not df_new.empty else '无数据'}")
        else:
            print("数据已完整，无需更新")
except Exception as e:
    print(f"测试过程中发生错误: {e}")

# 登出
data_source.logout()
print("\n测试完成！")
