"""
修复已有的数据文件，将缺失值填充为合理的默认值
"""

import pandas as pd
import numpy as np
import os
from data_storage import EnhancedDataStorage


def fix_existing_data():
    """修复已有的数据文件"""
    # 创建数据存储实例
    storage = EnhancedDataStorage()
    
    # 获取所有已缓存的股票代码
    stock_codes = storage.get_all_stocks()
    
    print(f"发现 {len(stock_codes)} 只股票的数据文件")
    
    for stock_code in stock_codes:
        print(f"\n修复 {stock_code} 的数据...")
        
        # 加载数据
        df = storage.load_daily_data(stock_code)
        
        if df is None or df.empty:
            print(f"  跳过：数据不存在或为空")
            continue
        
        # 填充缺失值
        # 1. 基础信息
        if 'name' in df.columns:
            df['name'] = df['name'].fillna('')
        
        # 2. 行情数据
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0.0)
        
        # 3. 成交量数据
        volume_cols = ['volume', 'amount']
        for col in volume_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0.0)
        
        # 4. 衍生字段
        if 'pre_close' in df.columns:
            df['pre_close'] = df['pre_close'].fillna(df['close'].shift(1))
        
        if 'change' in df.columns:
            df['change'] = df['change'].fillna(df['close'] - df['pre_close'])
        
        if 'pct_chg' in df.columns:
            df['pct_chg'] = df['pct_chg'].fillna((df['change'] / df['pre_close'] * 100).round(2))
        
        # 5. 其他字段
        float_cols = ['turnover', 'adj_factor', 'open_adj', 'high_adj', 'low_adj', 'close_adj',
                     'pe', 'pe_ttm', 'pb', 'ps', 'pcf', 'total_shares', 'float_shares',
                     'total_mv', 'circ_mv', 'high_limit', 'low_limit', 'volume_ratio', 'turnover_rate']
        
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0.0)
        
        # 6. 整数字段
        int_cols = ['limit_status', 'trade_status']
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        
        # 7. 确保复权价格有值
        if 'adj_factor' in df.columns:
            for price_col in ['open', 'high', 'low', 'close']:
                adj_col = f"{price_col}_adj"
                if adj_col in df.columns:
                    df[adj_col] = df[adj_col].fillna(df[price_col] * df['adj_factor'])
        
        # 8. 确保量比有值
        if 'volume_ratio' in df.columns:
            df['volume_ratio'] = df['volume_ratio'].fillna(1.0)
        
        # 保存修复后的数据
        storage.save_daily_data(stock_code, df)
        
        print(f"  ✓ 修复完成")
    
    print(f"\n所有数据文件修复完成！")


if __name__ == "__main__":
    fix_existing_data()
