#!/usr/bin/env python3
"""
测试时间戳文件创建机制
"""

import os
import sys
from app import DataTimestampManager

# 测试 DataTimestampManager
print("测试 DataTimestampManager...")

# 创建实例
manager = DataTimestampManager()

# 检查目录是否创建
print(f"Data目录是否存在: {os.path.exists('data')}")
print(f"时间戳文件是否存在: {os.path.exists('data/last_update_timestamp.json')}")

# 测试方法
print(f"\n当前最后更新日期: {manager.get_last_update_date()}")
print(f"是否需要更新: {manager.needs_update('2026-03-09')}")

# 设置更新日期
manager.set_last_update_date('2026-03-09')
print(f"\n设置后最后更新日期: {manager.get_last_update_date()}")
print(f"时间戳文件是否存在: {os.path.exists('data/last_update_timestamp.json')}")

# 读取文件内容
if os.path.exists('data/last_update_timestamp.json'):
    with open('data/last_update_timestamp.json', 'r', encoding='utf-8') as f:
        content = f.read()
    print(f"\n时间戳文件内容:\n{content}")

print("\n测试完成！")
