#!/usr/bin/env python3
"""
检查 Parquet 文件结构，确认是否包含股票名称信息
"""

import pandas as pd
import os

# 检查第一个 Parquet 文件
daily_dir = "data/daily"
parquet_files = [f for f in os.listdir(daily_dir) if f.endswith('.parquet')]

if parquet_files:
    test_file = os.path.join(daily_dir, parquet_files[0])
    print(f"检查文件: {test_file}")
    
    # 读取文件
    df = pd.read_parquet(test_file)
    print(f"\n文件形状: {df.shape}")
    print(f"\n列名: {list(df.columns)}")
    print(f"\n前几行数据:")
    print(df.head())
    
    # 检查是否包含 name 列
    if 'name' in df.columns:
        print(f"\n✓ 包含 name 列")
        print(f"name 列前几个值: {df['name'].head().tolist()}")
    else:
        print(f"\n✗ 不包含 name 列")
else:
    print("没有找到 Parquet 文件")
