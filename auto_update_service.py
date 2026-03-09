#!/usr/bin/env python3
"""
自动更新服务
每天02:00自动执行数据更新
"""

import os
import sys
import time
import logging
import schedule
from datetime import datetime
from unified_data_manager import UnifiedDataManager
from data_sources import DataSourceManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('auto_update.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('auto_update_service')

def update_all_stocks():
    """执行全市场股票数据更新"""
    logger.info("=== 开始自动更新股票数据 ===")
    
    try:
        # 初始化数据管理器
        dm = UnifiedDataManager()
        
        # 执行全市场更新
        result = dm.refresh_all_stocks()
        
        logger.info(f"更新完成：")
        logger.info(f"总股票数：{result['total_stocks']}")
        logger.info(f"缓存完整：{result['cached_stocks']}")
        logger.info(f"新股票：{result['new_stocks']}")
        logger.info(f"已更新：{result['updated_stocks']}")
        logger.info(f"失败：{result['failed_stocks']}")
        
    except Exception as e:
        logger.error(f"更新过程中发生错误：{e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("=== 自动更新完成 ===")

def setup_schedule():
    """设置定时任务"""
    # 每天凌晨2:00执行更新
    schedule.every().day.at("02:00").do(update_all_stocks)
    logger.info("定时任务已设置：每天02:00执行数据更新")

def run_service():
    """运行服务"""
    logger.info("自动更新服务启动")
    
    # 设置定时任务
    setup_schedule()
    
    # 立即执行一次更新（可选）
    # update_all_stocks()
    
    # 主循环
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            logger.info("服务被手动停止")
            break
        except Exception as e:
            logger.error(f"服务运行错误：{e}")
            time.sleep(60)  # 发生错误后暂停一分钟

if __name__ == "__main__":
    run_service()
