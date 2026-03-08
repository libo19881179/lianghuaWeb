"""
数据管理模块
实现本地缓存、数据过期判断、智能更新
"""

import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import hashlib


class DataManager:
    """数据管理器，负责缓存和数据更新"""
    
    def __init__(self, cache_file: str = "stock_data.csv", cache_meta_file: str = "cache_meta.json"):
        self.cache_file = cache_file
        self.cache_meta_file = cache_meta_file
        self.cache_data = None
        self.cache_meta = {}
        self._load_cache()
    
    def _load_cache(self):
        """加载缓存数据"""
        if os.path.exists(self.cache_file):
            try:
                self.cache_data = pd.read_csv(self.cache_file, parse_dates=['date'])
            except Exception as e:
                print(f"加载缓存失败：{e}")
                self.cache_data = pd.DataFrame()
        else:
            self.cache_data = pd.DataFrame()
        
        # 加载元数据
        if os.path.exists(self.cache_meta_file):
            try:
                with open(self.cache_meta_file, 'r', encoding='utf-8') as f:
                    self.cache_meta = json.load(f)
            except:
                self.cache_meta = {}
    
    def _save_cache(self):
        """保存缓存数据"""
        if not self.cache_data.empty:
            self.cache_data.to_csv(self.cache_file, index=False, encoding='utf-8-sig')
        
        # 保存元数据
        with open(self.cache_meta_file, 'w', encoding='utf-8') as f:
            json.dump(self.cache_meta, f, ensure_ascii=False, indent=2)
    
    def _generate_cache_key(self, stock_code: str, start_date: str = None, end_date: str = None) -> str:
        """
        生成缓存键
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期（可选，为了向后兼容）
            end_date: 结束日期（可选，为了向后兼容）
            
        Returns:
            缓存键（只基于股票代码）
        """
        # 只使用股票代码作为缓存键，不按日期范围区分
        # 这样可以避免重复缓存同一股票的不同日期范围数据
        return hashlib.md5(stock_code.encode()).hexdigest()
    
    def get_stock_date_range(self, stock_code: str) -> Tuple[Optional[str], Optional[str]]:
        """
        获取某股票在缓存中的日期范围
        
        Args:
            stock_code: 股票代码
            
        Returns:
            (min_date, max_date) 字符串格式，如果没有数据则返回 (None, None)
        """
        if self.cache_data is None or self.cache_data.empty:
            return None, None
        
        stock_data = self.cache_data[self.cache_data['code'] == stock_code]
        if stock_data.empty:
            return None, None
        
        # 获取最小和最大日期
        min_date_val = stock_data['date'].min()
        max_date_val = stock_data['date'].max()
        
        # 转换为字符串格式
        if hasattr(min_date_val, 'strftime'):
            min_date = min_date_val.strftime('%Y-%m-%d')
        else:
            min_date = str(min_date_val)
            
        if hasattr(max_date_val, 'strftime'):
            max_date = max_date_val.strftime('%Y-%m-%d')
        else:
            max_date = str(max_date_val)
        
        return min_date, max_date
    
    def _is_cache_valid(
        self, 
        stock_code: str, 
        start_date: str = None, 
        end_date: str = None,
        max_age_days: int = 7
    ) -> bool:
        """
        判断缓存是否有效
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            max_age_days: 最大缓存天数（默认 7 天）
            
        Returns:
            缓存是否有效
        """
        cache_key = self._generate_cache_key(stock_code)
        
        if cache_key not in self.cache_meta:
            return False
        
        meta = self.cache_meta[cache_key]
        cache_time = datetime.fromisoformat(meta['cache_time'])
        
        # 检查缓存是否过期
        if datetime.now() - cache_time > timedelta(days=max_age_days):
            return False
        
        # 检查数据是否存在
        if self.cache_data is None or self.cache_data.empty:
            return False
        
        # 检查是否有该股票的数据
        stock_data = self.cache_data[self.cache_data['code'] == stock_code]
        if stock_data.empty:
            return False
        
        # 如果指定了日期范围，检查缓存是否覆盖所需范围
        if start_date and end_date:
            min_date, max_date = self.get_stock_date_range(stock_code)
            if min_date and max_date:
                # 检查是否需要补充数据
                if start_date < min_date or end_date > max_date:
                    return False
        
        return True
    
    def get_cached_data(
        self, 
        stock_code: str, 
        start_date: str = None, 
        end_date: str = None
    ) -> Optional[pd.DataFrame]:
        """
        获取缓存数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            缓存的 DataFrame 或 None
        """
        if self.cache_data is None or self.cache_data.empty:
            return None
        
        cache_key = self._generate_cache_key(stock_code)
        
        if cache_key not in self.cache_meta:
            return None
        
        # 筛选股票数据
        stock_data = self.cache_data[self.cache_data['code'] == stock_code].copy()
        
        if stock_data.empty:
            return None
        
        # 如果有日期范围，进行筛选（用于回测时的数据截取）
        if start_date and end_date:
            # 确保日期列是字符串类型以便比较
            if pd.api.types.is_datetime64_any_dtype(stock_data['date']):
                # 如果 date 列是 datetime 类型，转换为字符串
                stock_data = stock_data.copy()
                stock_data['date'] = stock_data['date'].dt.strftime('%Y-%m-%d')
            
            stock_data = stock_data[
                (stock_data['date'] >= start_date) & 
                (stock_data['date'] <= end_date)
            ]
        
        return stock_data if not stock_data.empty else None
    
    def get_missing_date_range(
        self, 
        stock_code: str, 
        start_date: str, 
        end_date: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        获取某股票在指定范围内缺失的日期范围
        
        Args:
            stock_code: 股票代码
            start_date: 需要的开始日期
            end_date: 需要的结束日期
            
        Returns:
            (need_start_date, need_end_date) 需要补充的数据范围，如果不需要补充则返回 (None, None)
        """
        cached_min, cached_max = self.get_stock_date_range(stock_code)
        
        # 如果缓存中完全没有该股票数据
        if cached_min is None or cached_max is None:
            return start_date, end_date
        
        # 检查是否需要补充数据
        need_start = None
        need_end = None
        
        # 需要补充开始日期之前的数据
        if start_date < cached_min:
            need_start = start_date
        
        # 需要补充结束日期之后的数据
        if end_date > cached_max:
            need_end = end_date
        
        if need_start or need_end:
            # 返回需要补充的范围（只返回缺失的部分）
            # 如果需要补充开始日期之前的数据
            if need_start:
                # 返回 need_start 到 cached_min 之间的数据
                return need_start, cached_min
            # 如果需要补充结束日期之后的数据
            if need_end:
                # 返回 cached_max 到 need_end 之间的数据
                return cached_max, need_end
            # 如果两头都需要补充（理论上不会发生）
            return need_start, need_end
        
        return None, None
    
    def update_cache(
        self, 
        stock_code: str, 
        df: pd.DataFrame,
        start_date: str = None, 
        end_date: str = None
    ):
        """
        增量更新缓存数据
        
        Args:
            stock_code: 股票代码
            df: 股票数据 DataFrame（新获取的数据）
            start_date: 开始日期
            end_date: 结束日期
        """
        # 添加股票代码列
        if 'code' not in df.columns:
            df['code'] = stock_code
        
        # 合并数据
        if self.cache_data is None or self.cache_data.empty:
            self.cache_data = df.copy()
        else:
            # 检查是否已有该股票的数据
            existing_stock_data = self.cache_data[self.cache_data['code'] == stock_code]
            
            if existing_stock_data.empty:
                # 没有该股票的数据，直接添加
                self.cache_data = pd.concat([self.cache_data, df], ignore_index=True)
            else:
                # 已有该股票的数据，增量合并（去重）
                new_df = df.copy()
                min_date = new_df['date'].min()
                max_date = new_df['date'].max()
                
                # 保留不重叠的旧数据
                non_overlapping = self.cache_data[
                    (self.cache_data['code'] != stock_code) | 
                    (self.cache_data['date'] < min_date) | 
                    (self.cache_data['date'] > max_date)
                ]
                
                # 合并
                self.cache_data = pd.concat([non_overlapping, new_df], ignore_index=True)
        
        # 更新元数据（基于整个股票的数据范围）
        cache_key = self._generate_cache_key(stock_code)
        all_stock_data = self.cache_data[self.cache_data['code'] == stock_code]
        self.cache_meta[cache_key] = {
            'stock_code': stock_code,
            'cache_time': datetime.now().isoformat(),
            'rows': len(all_stock_data),
            'min_date': str(all_stock_data['date'].min()) if 'date' in all_stock_data.columns and not all_stock_data.empty else None,
            'max_date': str(all_stock_data['date'].max()) if 'date' in all_stock_data.columns and not all_stock_data.empty else None
        }
        
        # 保存缓存
        self._save_cache()
    
    def clear_cache(self):
        """清空缓存"""
        self.cache_data = pd.DataFrame()
        self.cache_meta = {}
        
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
        if os.path.exists(self.cache_meta_file):
            os.remove(self.cache_meta_file)
    
    def get_cache_info(self) -> Dict:
        """获取缓存信息"""
        return {
            'total_rows': len(self.cache_data) if self.cache_data is not None else 0,
            'total_stocks': self.cache_data['code'].nunique() if self.cache_data is not None and not self.cache_data.empty else 0,
            'cache_entries': len(self.cache_meta),
            'cache_file_size': os.path.getsize(self.cache_file) if os.path.exists(self.cache_file) else 0,
            'cache_file': self.cache_file
        }
    
    def refresh_all_stocks(self, data_source) -> Dict:
        """
        智能刷新全部 A 股数据
        
        Args:
            data_source: DataSourceManager 实例
            
        Returns:
            刷新结果统计信息
        """
        print("\n========== 开始智能刷新全部 A 股数据 ==========")
        
        # 获取全部 A 股列表
        print("[1/4] 获取全部 A 股列表...")
        all_stocks_df = data_source.get_all_stock_codes()
        
        if all_stocks_df.empty:
            print("[错误] 获取 A 股列表失败")
            return {'success': False, 'error': '获取 A 股列表失败'}
        
        # 提取股票代码（需要适配不同数据源的返回格式）
        stock_codes = []
        for _, row in all_stocks_df.iterrows():
            if 'code' in row:
                code = row['code']
                # 转换为标准格式 sh.600000 或 sz.000001
                if code.startswith('sh') or code.startswith('sz'):
                    stock_codes.append(code)
                elif code.isdigit() and len(code) == 6:
                    if code.startswith(('600', '601', '603', '605')):
                        stock_codes.append(f"sh.{code}")
                    elif code.startswith(('000', '001', '002', '003')):
                        stock_codes.append(f"sz.{code}")
        
        print(f"[OK] 获取到 {len(stock_codes)} 只 A 股")
        
        # 统计信息
        total_stocks = len(stock_codes)
        cached_stocks = 0
        new_stocks = 0
        updated_stocks = 0
        failed_stocks = 0
        
        # 获取当前日期范围（最近 1 年）
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        print(f"\n[2/4] 开始批量获取数据，日期范围：{start_date_str} ~ {end_date_str}")
        print(f"{'='*60}")
        
        # 逐个处理股票
        for idx, code in enumerate(stock_codes, 1):
            try:
                # 检查缓存
                cached_min, cached_max = self.get_stock_date_range(code)
                
                if cached_min is None or cached_max is None:
                    # 缓存中没有，获取全部数据
                    print(f"[{idx}/{total_stocks}] {code} - 新股票，获取全部数据")
                    df = data_source.get_stock_data(code, start_date_str, end_date_str)
                    if df is not None and not df.empty:
                        self.update_cache(code, df, start_date_str, end_date_str)
                        new_stocks += 1
                        print(f"  ✓ 获取 {len(df)} 条记录，已保存缓存")
                    else:
                        failed_stocks += 1
                        print(f"  ✗ 数据获取失败")
                else:
                    # 检查是否需要更新
                    need_start, need_end = self.get_missing_date_range(code, start_date_str, end_date_str)
                    
                    if need_start is None and need_end is None:
                        # 缓存完整
                        cached_stocks += 1
                        print(f"[{idx}/{total_stocks}] {code} - 缓存完整 ✓")
                    else:
                        # 需要增量更新
                        print(f"[{idx}/{total_stocks}] {code} - 增量更新：{need_start} ~ {need_end}")
                        df_new = data_source.get_stock_data(code, need_start, need_end)
                        if df_new is not None and not df_new.empty:
                            self.update_cache(code, df_new, start_date_str, end_date_str)
                            updated_stocks += 1
                            print(f"  ✓ 获取 {len(df_new)} 条记录，已更新缓存")
                        else:
                            failed_stocks += 1
                            print(f"  ✗ 增量数据获取失败")
                
                # 每 100 只股票显示一次进度
                if idx % 100 == 0:
                    print(f"\n{'='*60}")
                    print(f"进度：{idx}/{total_stocks} ({idx/total_stocks*100:.1f}%)")
                    print(f"已缓存：{cached_stocks} | 新股票：{new_stocks} | 已更新：{updated_stocks} | 失败：{failed_stocks}")
                    print(f"{'='*60}\n")
                    
            except Exception as e:
                failed_stocks += 1
                print(f"[{idx}/{total_stocks}] {code} - 错误：{e}")
        
        # 最终统计
        print("\n========== 刷新完成 ==========")
        print(f"总股票数：{total_stocks}")
        print(f"缓存完整：{cached_stocks} ({cached_stocks/total_stocks*100:.1f}%)")
        print(f"新股票：{new_stocks} ({new_stocks/total_stocks*100:.1f}%)")
        print(f"已更新：{updated_stocks} ({updated_stocks/total_stocks*100:.1f}%)")
        print(f"失败：{failed_stocks} ({failed_stocks/total_stocks*100:.1f}%)")
        print(f"================================\n")
        
        return {
            'success': True,
            'total_stocks': total_stocks,
            'cached_stocks': cached_stocks,
            'new_stocks': new_stocks,
            'updated_stocks': updated_stocks,
            'failed_stocks': failed_stocks
        }
    
    def force_refresh(self, stock_code: str = None):
        """
        强制刷新缓存（重新从网络获取数据）
        
        Args:
            stock_code: 股票代码，如果为 None 则刷新所有
        """
        if stock_code:
            # 移除特定股票的缓存
            if self.cache_data is not None and not self.cache_data.empty:
                self.cache_data = self.cache_data[self.cache_data['code'] != stock_code]
            
            # 移除相关元数据
            keys_to_remove = [k for k in self.cache_meta.keys() if self.cache_meta[k].get('stock_code') == stock_code]
            for key in keys_to_remove:
                del self.cache_meta[key]
            
            self._save_cache()
        else:
            # 刷新所有：删除所有缓存文件，下次使用时重新获取
            self.clear_cache()
    
    def reload_cache(self):
        """
        重新加载本地缓存文件
        只从文件重新加载数据，不清空缓存
        """
        self._load_cache()


class StockPoolManager:
    """股票池管理器"""
    
    def __init__(self, pool_file: str = "stock_pool.json"):
        self.pool_file = pool_file
        self.stocks = []
        self._load_pool()
    
    def _load_pool(self):
        """加载股票池"""
        if os.path.exists(self.pool_file):
            try:
                with open(self.pool_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.stocks = data.get('stocks', [])
            except:
                self.stocks = []
        else:
            # 默认股票池
            self.stocks = [
                {"code": "sh.600000", "name": "浦发银行", "weight": 0.2},
                {"code": "sh.600036", "name": "招商银行", "weight": 0.2},
                {"code": "sh.601318", "name": "中国平安", "weight": 0.2},
                {"code": "sh.600519", "name": "贵州茅台", "weight": 0.2},
                {"code": "sz.000001", "name": "平安银行", "weight": 0.2},
            ]
            self._save_pool()
    
    def _save_pool(self):
        """保存股票池"""
        data = {
            'stocks': self.stocks,
            'last_update': datetime.now().isoformat()
        }
        with open(self.pool_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def add_stock(self, code: str, name: str, weight: float):
        """添加股票"""
        # 检查是否已存在
        for stock in self.stocks:
            if stock['code'] == code:
                stock['weight'] = weight
                self._save_pool()
                return
        
        self.stocks.append({
            'code': code,
            'name': name,
            'weight': weight
        })
        self._save_pool()
    
    def remove_stock(self, code: str):
        """移除股票"""
        self.stocks = [s for s in self.stocks if s['code'] != code]
        self._save_pool()
    
    def update_weight(self, code: str, weight: float):
        """更新权重"""
        for stock in self.stocks:
            if stock['code'] == code:
                stock['weight'] = weight
                break
        self._save_pool()
    
    def normalize_weights(self):
        """归一化权重（总和为 1）"""
        total_weight = sum(s['weight'] for s in self.stocks)
        if total_weight > 0:
            for stock in self.stocks:
                stock['weight'] = stock['weight'] / total_weight
            self._save_pool()
    
    def get_stocks(self) -> List[Dict]:
        """获取股票列表"""
        return self.stocks
    
    def get_stock_codes(self) -> List[str]:
        """获取股票代码列表"""
        return [s['code'] for s in self.stocks]
