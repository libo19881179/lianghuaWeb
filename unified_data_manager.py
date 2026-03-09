"""
统一数据管理器
兼容新旧两种存储方式，优先使用新的 Parquet 存储结构
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from data_storage import EnhancedDataStorage
from data_manager import DataManager


class UnifiedDataManager:
    """
    统一数据管理器
    - 优先使用新的 EnhancedDataStorage（Parquet 格式）
    - 向后兼容旧的 DataManager（CSV 格式）
    - 支持平滑迁移
    """
    
    def __init__(self, use_new_storage: bool = True):
        """
        初始化数据管理器
        
        Args:
            use_new_storage: 是否使用新存储（默认 True）
        """
        self.use_new_storage = use_new_storage
        
        if use_new_storage:
            # 创建新的存储器
            self.new_storage = EnhancedDataStorage()
            print("✓ 使用新版数据存储（Parquet 格式）")
        else:
            # 使用旧的 CSV 缓存
            self.old_manager = DataManager()
            print("✓ 使用旧版数据存储（CSV 格式）")
    
    def get_stock_data(
        self,
        stock_code: str,
        start_date: str = None,
        end_date: str = None,
        columns: List[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取股票数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            columns: 需要读取的列（仅新存储支持）
            
        Returns:
            DataFrame 或 None
        """
        if self.use_new_storage:
            return self.new_storage.load_daily_data(
                stock_code, 
                start_date, 
                end_date,
                columns
            )
        else:
            return self.old_manager.get_cached_data(
                stock_code,
                start_date,
                end_date
            )
    
    def save_stock_data(
        self,
        stock_code: str,
        df: pd.DataFrame,
        data_source: str = "baostock",
        adjust_type: str = "qfq",
        stock_name: str = None
    ):
        """
        保存股票数据
        
        Args:
            stock_code: 股票代码
            df: 数据 DataFrame
            data_source: 数据源
            adjust_type: 复权类型
            stock_name: 股票名称（可选）
        """
        if self.use_new_storage:
            self.new_storage.save_daily_data(
                stock_code,
                df,
                data_source,
                adjust_type,
                stock_name
            )
        else:
            self.old_manager.update_cache(stock_code, df, None, None)
    
    def get_stock_date_range(self, stock_code: str) -> Tuple[Optional[str], Optional[str]]:
        """获取股票在缓存中的日期范围"""
        if self.use_new_storage:
            meta = self.new_storage.get_stock_info(stock_code)
            if meta and 'daily' in meta:
                min_date = meta['daily'].get('min_date')
                max_date = meta['daily'].get('max_date')
                
                # 格式化为纯日期（去除时间部分）
                if min_date:
                    min_date = str(min_date).split(' ')[0]
                if max_date:
                    max_date = str(max_date).split(' ')[0]
                
                return (min_date, max_date)
            return None, None
        else:
            return self.old_manager.get_stock_date_range(stock_code)
    
    def get_index_date_range(self, index_code: str) -> Tuple[Optional[str], Optional[str]]:
        """获取指数在缓存中的日期范围"""
        if self.use_new_storage:
            meta = self.new_storage.get_stock_info(index_code)
            if meta and 'index' in meta:
                min_date = meta['index'].get('min_date')
                max_date = meta['index'].get('max_date')
                
                # 格式化为纯日期（去除时间部分）
                if min_date:
                    min_date = str(min_date).split(' ')[0]
                if max_date:
                    max_date = str(max_date).split(' ')[0]
                
                return (min_date, max_date)
            return None, None
        else:
            # 旧存储不支持指数数据
            return None, None
    
    def get_missing_date_range(
        self,
        stock_code: str,
        start_date: str,
        end_date: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        获取需要补充的数据范围
        
        逻辑：
        - 如果本地没有缓存，返回 (start_date, end_date)
        - 如果本地有缓存，只检查是否需要更新最新数据（cached_max 之后）
        - 不检查 cached_min 之前，因为那部分数据已经有了
        
        Args:
            stock_code: 股票代码
            start_date: 期望的开始日期
            end_date: 期望的结束日期
            
        Returns:
            (need_start, need_end) 需要补充的数据范围，(None, None) 表示不需要补充
        """
        cached_min, cached_max = self.get_stock_date_range(stock_code)
        
        if cached_min is None or cached_max is None:
            # 没有缓存，需要获取全部数据
            return start_date, end_date
        
        # 本地已有缓存，只需要检查是否需要更新最新数据
        need_end = None
        
        if end_date > cached_max:
            # 需要获取 cached_max 之后的数据
            # 从 cached_max 的下一个交易日开始（简单处理为 +1 天）
            from datetime import timedelta
            next_date = (pd.to_datetime(cached_max) + timedelta(days=1)).strftime('%Y-%m-%d')
            need_end = end_date
            return next_date, need_end
        
        # 缓存完整，不需要补充
        return None, None
    
    def get_index_data(
        self,
        index_code: str,
        start_date: str = None,
        end_date: str = None
    ) -> Optional[pd.DataFrame]:
        """
        获取指数数据（仅新存储支持）
        
        Args:
            index_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame 或 None
        """
        if not self.use_new_storage:
            print("警告：旧版存储不支持指数数据")
            return None
        
        return self.new_storage.load_index_data(index_code, start_date, end_date)
    
    def save_index_data(
        self,
        index_code: str,
        df: pd.DataFrame,
        data_source: str = "akshare"
    ):
        """
        保存指数数据（仅新存储支持）
        
        Args:
            index_code: 指数代码
            df: 指数数据
            data_source: 数据源
        """
        if not self.use_new_storage:
            print("警告：旧版存储不支持指数数据")
            return
        
        self.new_storage.save_index_data(index_code, df, data_source)
    
    def get_fundamentals_data(
        self,
        data_type: str
    ) -> Optional[pd.DataFrame]:
        """
        获取基本面数据（仅新存储支持）
        
        Args:
            data_type: 数据类型（valuation_metrics/financial_reports/industry_classification）
            
        Returns:
            DataFrame 或 None
        """
        if not self.use_new_storage:
            print("警告：旧版存储不支持基本面数据")
            return None
        
        return self.new_storage.load_fundamentals_data(data_type)
    
    def save_fundamentals_data(
        self,
        data_type: str,
        df: pd.DataFrame,
        data_source: str = "baostock"
    ):
        """
        保存基本面数据（仅新存储支持）
        
        Args:
            data_type: 数据类型
            df: 基本面数据
            data_source: 数据源
        """
        if not self.use_new_storage:
            print("警告：旧版存储不支持基本面数据")
            return
        
        self.new_storage.save_fundamentals_data(data_type, df, data_source)
    
    def check_data_quality(self, stock_code: str) -> Dict:
        """检查数据质量（仅新存储支持）"""
        if not self.use_new_storage:
            return {'status': 'unknown', 'message': '旧版存储不支持质量检查'}
        
        return self.new_storage.check_data_quality(stock_code)
    
    def get_all_stocks(self) -> List[str]:
        """获取所有已缓存的股票代码"""
        if self.use_new_storage:
            return self.new_storage.get_all_stocks()
        else:
            # 旧版存储需要遍历元数据
            return list(self.old_manager.cache_meta.keys())
    
    def get_cache_info(self) -> Dict:
        """获取缓存信息"""
        if self.use_new_storage:
            # 统计新存储的信息
            stocks = self.get_all_stocks()
            total_rows = 0
            
            for stock_code in stocks:
                meta = self.new_storage.get_stock_info(stock_code)
                if meta and 'daily' in meta:
                    total_rows += meta['daily'].get('rows', 0)
            
            return {
                'total_rows': total_rows,
                'total_stocks': len(stocks),
                'cache_file_size': 0,  # Parquet 文件分散存储
                'cache_file': 'data/daily/*.parquet',
                'storage_type': 'parquet'
            }
        else:
            return self.old_manager.get_cache_info()
    
    def migrate_from_old_storage(self):
        """从旧版存储迁移到新版"""
        if os.path.exists('stock_data.csv'):
            print("\n检测到旧版 CSV 缓存，是否迁移到新版存储？")
            print("提示：新版存储使用 Parquet 格式，性能更好，字段更全")
            
            choice = input("是否迁移？(y/n): ").strip().lower()
            if choice == 'y':
                from migrate_data import migrate_from_csv
                migrate_from_csv()
    
    def refresh_all_stocks(self, data_source, start_date: str = '1990-01-01') -> Dict:
        """
        智能刷新全部 A 股数据（自动筛选真正的 A 股，排除指数）
        
        Args:
            data_source: 数据源管理器
            start_date: 开始日期
            
        Returns:
            刷新结果统计
        """
        print("\n========== 开始智能刷新全部 A 股数据 ==========")
        
        # 检查今天是否为交易日
        today = datetime.now().strftime('%Y-%m-%d')
        is_today_trading_day = data_source.is_trading_day(today)
        print(f"[1/5] 检查交易日状态：{today} {'是' if is_today_trading_day else '不是'}交易日")
        
        # 确定结束日期：如果今天是交易日则使用今天，否则使用最近的交易日
        end_date = today
        if not is_today_trading_day:
            # 查找最近的交易日
            from datetime import timedelta
            for i in range(1, 10):  # 最多向前查找10天
                check_date = (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d')
                if data_source.is_trading_day(check_date):
                    end_date = check_date
                    print(f"[提示] 使用最近的交易日 {end_date} 作为结束日期")
                    break
        
        # 获取全部 A 股列表（已自动筛选，排除指数）
        print("[2/5] 获取全部 A 股列表（自动排除指数、基金、债券）...")
        all_stocks_df = data_source.get_all_a_stock_codes(end_date)
        
        if all_stocks_df.empty:
            print("[错误] 获取 A 股列表失败")
            return {'success': False, 'error': '获取 A 股列表失败'}
        
        # 提取股票代码和名称（直接使用标准化后的数据）
        stock_codes = []
        stock_names = {}  # 存储股票代码对应的名称
        
        for _, row in all_stocks_df.iterrows():
            if 'code' in row and pd.notna(row['code']):
                code = row['code']
                name = row.get('name', '') if 'name' in row and pd.notna(row['name']) else ''
                stock_codes.append(code)
                stock_names[code] = name
        
        print(f"[3/5] 获取到 {len(stock_codes)} 只 A 股股票")
        
        # 统计信息
        cached_count = 0
        new_count = 0
        updated_count = 0
        failed_count = 0
        
        # 逐个刷新
        print("[4/5] 开始刷新 A 股数据...")
        
        for i, stock_code in enumerate(stock_codes):
            try:
                # 检查缓存
                cached_min, cached_max = self.get_stock_date_range(stock_code)
                
                # 如果缓存中没有，检查是否有格式不同但实际相同的股票代码
                if cached_min is None:
                    # 尝试标准化股票代码格式
                    def standardize_code(code):
                        if isinstance(code, str) and '.' in code:
                            num = code.split('.')[-1]
                            if num.startswith('6'):
                                return f"sh.{num}"
                            else:
                                return f"sz.{num}"
                        return code
                    
                    # 检查是否存在格式不同的同一只股票
                    standardized_code = standardize_code(stock_code)
                    if standardized_code != stock_code:
                        alt_min, alt_max = self.get_stock_date_range(standardized_code)
                        if alt_min is not None:
                            # 存在格式不同的缓存，使用现有数据
                            print(f"[{i+1}/{len(stock_codes)}] {stock_code} - 使用现有缓存数据（格式标准化）")
                            cached_count += 1
                            continue
                
                if cached_min is None:
                    # 缓存中没有，需要获取全部数据
                    print(f"[{i+1}/{len(stock_codes)}] {stock_code} - 从网络获取全部数据")
                    df = data_source.get_stock_data(stock_code, start_date, end_date)
                    if df is not None and not df.empty:
                        stock_name = stock_names.get(stock_code, '')
                        self.save_stock_data(stock_code, df, stock_name=stock_name)
                        new_count += 1
                    elif df is None:
                        # 空数据（可能是停牌或无数据），不视为失败
                        print(f"[{i+1}/{len(stock_codes)}] {stock_code} - 无数据（可能是停牌）")
                    else:
                        failed_count += 1
                else:
                    # 检查是否需要增量更新
                    need_start, need_end = self.get_missing_date_range(
                        stock_code,
                        start_date,
                        end_date
                    )
                    
                    if need_start is None and need_end is None:
                        # 缓存完整
                        cached_count += 1
                    else:
                        # 只有在数据确实不完整时才进行更新
                        # 非交易日时，end_date 已经是最近的交易日，所以如果有缺失数据，说明确实需要更新
                        print(f"[{i+1}/{len(stock_codes)}] {stock_code} - 增量更新：{need_start} ~ {need_end}")
                        df_new = data_source.get_stock_data(stock_code, need_start, need_end)
                        if df_new is not None and not df_new.empty:
                            # 合并到缓存
                            existing_df = self.get_stock_data(stock_code)
                            if existing_df is not None:
                                combined_df = pd.concat([existing_df, df_new], ignore_index=True).drop_duplicates(subset=['date'], keep='last')
                                stock_name = stock_names.get(stock_code, '')
                                self.save_stock_data(stock_code, combined_df, stock_name=stock_name)
                                updated_count += 1
                            else:
                                stock_name = stock_names.get(stock_code, '')
                                self.save_stock_data(stock_code, df_new, stock_name=stock_name)
                                updated_count += 1
                        elif df_new is None:
                            # 空数据（可能是停牌或无数据），不视为失败
                            print(f"[{i+1}/{len(stock_codes)}] {stock_code} - 无数据（可能是停牌）")
                        else:
                            failed_count += 1
                
            except Exception as e:
                print(f"[错误] {stock_code} 刷新失败：{e}")
                failed_count += 1
        
        # 下载沪深 300 指数
        print(f"\n[5/5] 下载沪深 300 指数（000300.SH）...")
        try:
            # 只有在指数数据不完整时才下载
            index_min, index_max = self.get_index_date_range('000300.SH')
            need_index_update = False
            
            if index_min is None:
                # 指数数据不存在，需要下载
                need_index_update = True
            else:
                # 检查指数数据是否需要更新
                if end_date > index_max:
                    # 需要获取 index_max 之后的数据
                    need_index_update = True
            
            if need_index_update:
                index_df = data_source.get_index_data('000300.SH', start_date, end_date)
                if index_df is not None and not index_df.empty:
                    self.save_index_data('000300.SH', index_df)
                    print("✓ 沪深 300 指数下载完成")
                else:
                    print("✗ 沪深 300 指数下载失败")
            else:
                print("✓ 沪深 300 指数数据完整，跳过更新")
        except Exception as e:
            print(f"[错误] 下载沪深 300 指数失败：{e}")
        
        # 统计结果
        print("\n[5/5] 刷新完成")
        result = {
            'success': True,
            'total_stocks': len(stock_codes),
            'cached_stocks': cached_count,
            'new_stocks': new_count,
            'updated_stocks': updated_count,
            'failed_stocks': failed_count
        }
        
        print(f"\n========== 刷新结果 ==========")
        print(f"A 股总计：{len(stock_codes)} 只")
        print(f"缓存完整：{cached_count} 只")
        print(f"新增下载：{new_count} 只")
        print(f"增量更新：{updated_count} 只")
        print(f"下载失败：{failed_count} 只")
        print(f"缓存完整率：{cached_count/len(stock_codes)*100:.1f}%")
        print(f"沪深 300 指数：已下载")
        print("=" * 40)
        
        return result
    
    def clear_cache(self):
        """清空缓存"""
        if self.use_new_storage:
            self.new_storage.clear_all_data()
        else:
            self.old_manager.clear_cache()
