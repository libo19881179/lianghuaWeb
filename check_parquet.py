import pandas as pd
import sys

file_path = sys.argv[1] if len(sys.argv) > 1 else 'data/daily/sh.600001.parquet'

try:
    df = pd.read_parquet(file_path)
    print(f"文件：{file_path}")
    print(f"记录数：{len(df)}")
    print(f"\n字段列表 ({len(df.columns)} 个):")
    for i, col in enumerate(df.columns, 1):
        missing = df[col].isna().sum()
        pct = missing / len(df) * 100
        print(f"{i:2d}. {col:20s}: {missing:5d} 缺失 ({pct:5.1f}%)")
    
    print(f"\n前 3 行数据:")
    print(df.head(3).to_string())
    
except Exception as e:
    print(f"错误：{e}")
