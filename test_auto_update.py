#!/usr/bin/env python3
"""
测试自动更新服务修复
"""

from auto_update_service import update_all_stocks

if __name__ == "__main__":
    print("=== 测试自动更新服务 ===")
    update_all_stocks()
    print("=== 测试完成 ===")
