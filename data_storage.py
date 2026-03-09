"""
增强版数据存储模块
支持 Parquet 格式、按股票分文件存储、完整字段扩展
"""

import pandas as pd
import numpy as np
import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pyarrow as pa
import pyarrow.parquet as pq


class EnhancedDataStorage:
    """增强版数据存储器，使用 Parquet 格式和分层存储结构"""
    
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.daily_dir = self.base_dir / "daily"
        self.fundamentals_dir = self.base_dir / "fundamentals"
        self.index_dir = self.base_dir / "index"
        self.metadata_dir = self.base_dir / "metadata"
        
        # 创建目录结构
        self._create_directories()
        
        # 元数据
        self.metadata_file = self.metadata_dir / "metadata.json"
        self.metadata = self._load_metadata()
    
    def _create_directories(self):
        """创建目录结构"""
        for directory in [self.daily_dir, self.fundamentals_dir, self.index_dir, self.metadata_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _load_metadata(self) -> Dict:
        """加载元数据"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"加载元数据失败：{e}")
                return {}
        return {}
    
    def _save_metadata(self):
        """保存元数据"""
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, ensure_ascii=False, indent=2)
    
    def _generate_checksum(self, df: pd.DataFrame) -> str:
        """生成数据校验和"""
        return hashlib.md5(pd.util.hash_pandas_object(df).values).hexdigest()
    
    def get_stock_daily_path(self, stock_code: str) -> Path:
        """获取股票日线数据文件路径"""
        return self.daily_dir / f"{stock_code}.parquet"
    
    def get_index_daily_path(self, index_code: str) -> Path:
        """获取指数数据文件路径"""
        return self.index_dir / f"{index_code}.parquet"
    
    def get_fundamentals_path(self, data_type: str) -> Path:
        """获取基本面数据文件路径"""
        return self.fundamentals_dir / f"{data_type}.parquet"
    
    def save_daily_data(
        self, 
        stock_code: str, 
        df: pd.DataFrame,
        data_source: str = "baostock",
        adjust_type: str = "qfq",
        stock_name: str = None
    ):
        """
        保存股票日线数据（Parquet 格式）
        
        Args:
            stock_code: 股票代码（如 sh.600000）
            df: 数据 DataFrame
            data_source: 数据源（baostock/akshare/tushare）
            adjust_type: 复权类型（qfq/hfq/none）
            stock_name: 股票名称（可选）
        """
        if df.empty:
            print(f"警告：保存空数据到 {stock_code}")
            return
        
        # 确保有所有必需的列
        df = self._ensure_daily_columns(df, stock_code, stock_name)
        
        # 按日期排序
        df = df.sort_values('date').reset_index(drop=True)
        
        # 保存为 Parquet 文件
        file_path = self.get_stock_daily_path(stock_code)
        df.to_parquet(file_path, index=False, compression='snappy')
        
        # 更新元数据
        self._update_daily_metadata(
            stock_code, 
            df, 
            data_source, 
            adjust_type,
            file_path=str(file_path)
        )
        
        print(f"✓ 已保存 {stock_code} 日线数据：{len(df)} 条记录 -> {file_path}")
    
    def load_daily_data(
        self, 
        stock_code: str, 
        start_date: str = None, 
        end_date: str = None,
        columns: List[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        加载股票日线数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）
            columns: 需要读取的列（可选，默认读取所有列）
            
        Returns:
            DataFrame 或 None
        """
        file_path = self.get_stock_daily_path(stock_code)
        
        if not file_path.exists():
            return None
        
        try:
            # 读取 Parquet 文件
            if columns:
                # 只读取需要的列（Parquet 优势）
                df = pd.read_parquet(file_path, columns=columns)
            else:
                df = pd.read_parquet(file_path)
            
            # 日期筛选
            if start_date or end_date:
                if 'date' in df.columns:
                    # 确保日期是字符串格式以便比较
                    if pd.api.types.is_datetime64_any_dtype(df['date']):
                        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                    
                    if start_date and end_date:
                        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
                    elif start_date:
                        df = df[df['date'] >= start_date]
                    elif end_date:
                        df = df[df['date'] <= end_date]
            
            return df if not df.empty else None
            
        except Exception as e:
            print(f"加载 {stock_code} 日线数据失败：{e}")
            return None
    
    def _ensure_daily_columns(self, df: pd.DataFrame, stock_code: str, stock_name: str = None) -> pd.DataFrame:
        """
        确保日线数据包含所有必需的列
        
        Args:
            df: 原始数据
            stock_code: 股票代码
            stock_name: 股票名称（可选）
            
        Returns:
            包含完整列的 DataFrame
        """
        # 标准列定义
        standard_columns = {
            # 基础信息
            'date': 'datetime64[ns]',
            'code': 'object',
            'name': 'object',
            
            # 行情数据
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'pre_close': 'float64',
            'change': 'float64',
            'pct_chg': 'float64',
            
            # 成交量数据
            'volume': 'float64',
            'amount': 'float64',
            'turnover': 'float64',  # 换手率
            
            # 复权数据
            'adj_factor': 'float64',
            'open_adj': 'float64',
            'high_adj': 'float64',
            'low_adj': 'float64',
            'close_adj': 'float64',
            
            # 估值指标
            'pe': 'float64',
            'pe_ttm': 'float64',
            'pb': 'float64',
            'ps': 'float64',
            'pcf': 'float64',
            
            # 股本信息
            'total_shares': 'float64',
            'float_shares': 'float64',
            'total_mv': 'float64',  # 总市值
            'circ_mv': 'float64',  # 流通市值
            
            # 交易状态
            'limit_status': 'int8',  # 0-正常，1-涨停，-1-跌停
            'trade_status': 'int8',  # 0-交易，1-停牌
            
            # 复权标志
            'adjustflag': 'int8',  # 复权标志：1-前复权，2-后复权，3-不复权
            
            # 其他
            'high_limit': 'float64',  # 涨停价
            'low_limit': 'float64',  # 跌停价
            'volume_ratio': 'float64',  # 量比
            'turnover_rate': 'float64',  # 换手率（百分比）
        }
        
        # 添加缺失的列并设置默认值，同时确保现有列的类型正确
        for col, dtype in standard_columns.items():
            if col not in df.columns:
                if col == 'date':
                    df[col] = pd.NaT
                elif col == 'code':
                    df[col] = stock_code
                elif col == 'name':
                    df[col] = stock_name or ''
                elif dtype.startswith('float'):
                    df[col] = 0.0
                elif dtype.startswith('int'):
                    df[col] = 0
            else:
                # 确保现有列的类型正确
                if col == 'date':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif dtype.startswith('float'):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                elif dtype.startswith('int'):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        # 确保股票代码和名称有值
        if 'code' in df.columns:
            df['code'] = stock_code
        
        if 'name' in df.columns:
            df['name'] = stock_name or ''
        
        # 计算衍生字段（如果原始数据有基础字段）
        df = self._calculate_derived_fields(df)
        
        return df
    
    def _calculate_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算衍生字段"""
        # 计算涨跌幅
        if 'close' in df.columns:
            # 计算 pre_close（前收盘价）
            if 'pre_close' not in df.columns or df['pre_close'].isna().all():
                df['pre_close'] = df['close'].shift(1)
            
            # 计算 change（涨跌额）
            if 'change' not in df.columns or df['change'].isna().all():
                df['change'] = df['close'] - df['pre_close']
            
            # 计算 pct_chg（涨跌幅）
            if 'pct_chg' not in df.columns or df['pct_chg'].isna().all():
                df['pct_chg'] = (df['change'] / df['pre_close'] * 100).round(2)
        
        # 计算换手率
        if 'volume' in df.columns:
            if 'turnover' not in df.columns or df['turnover'].isna().all():
                # 即使没有流通股本，也设置默认值
                df['turnover'] = 0.0
            
            if 'turnover_rate' not in df.columns or df['turnover_rate'].isna().all():
                df['turnover_rate'] = df['turnover']
        
        # 计算复权价格
        if 'adj_factor' not in df.columns or df['adj_factor'].isna().all():
            # 简单复权因子（1.0 表示未复权）
            df['adj_factor'] = 1.0
        
        # 计算复权价格
        for price_col in ['open', 'high', 'low', 'close']:
            adj_col = f"{price_col}_adj"
            if adj_col not in df.columns or df[adj_col].isna().all():
                df[adj_col] = df[price_col] * df['adj_factor']
        
        # 计算涨跌停价（A 股涨跌幅限制 10%，ST 为 5%）
        if 'pre_close' in df.columns and not df['pre_close'].isna().all():
            if 'high_limit' not in df.columns or df['high_limit'].isna().all():
                df['high_limit'] = (df['pre_close'] * 1.1).round(2)
                df['low_limit'] = (df['pre_close'] * 0.9).round(2)
            
            # 判断涨跌停状态
            if 'limit_status' not in df.columns or df['limit_status'].isna().all():
                df['limit_status'] = 0
                df.loc[df['close'] >= df['high_limit'], 'limit_status'] = 1
                df.loc[df['close'] <= df['low_limit'], 'limit_status'] = -1
        
        # 计算量比（Volume Ratio）
        if 'volume' in df.columns:
            if 'volume_ratio' not in df.columns or df['volume_ratio'].isna().all():
                # 量比 = 今日成交量 / 过去5日平均成交量
                df['volume_ratio'] = (df['volume'] / df['volume'].rolling(window=5, min_periods=1).mean()).round(2)
        
        # 填充估值指标和股本信息的默认值
        valuation_cols = ['pe', 'pe_ttm', 'pb', 'ps', 'pcf']
        for col in valuation_cols:
            if col not in df.columns or df[col].isna().all():
                df[col] = 0.0
        
        equity_cols = ['total_shares', 'float_shares', 'total_mv', 'circ_mv']
        for col in equity_cols:
            if col not in df.columns or df[col].isna().all():
                df[col] = 0.0
        
        # 填充交易状态的默认值
        if 'trade_status' not in df.columns or df['trade_status'].isna().all():
            df['trade_status'] = 0
        
        return df
    
    def _update_daily_metadata(
        self, 
        stock_code: str, 
        df: pd.DataFrame,
        data_source: str,
        adjust_type: str,
        file_path: str
    ):
        """更新日线数据元数据"""
        if stock_code not in self.metadata:
            self.metadata[stock_code] = {}
        
        self.metadata[stock_code]['daily'] = {
            'source': data_source,
            'freq': 'daily',
            'adjust_type': adjust_type,
            'min_date': str(df['date'].min()) if 'date' in df.columns else None,
            'max_date': str(df['date'].max()) if 'date' in df.columns else None,
            'rows': len(df),
            'last_update': datetime.now().isoformat(),
            'checksum': self._generate_checksum(df),
            'file_path': file_path,
            'quality': {
                'missing_dates': [],
                'anomalies': []
            }
        }
        
        self._save_metadata()
    
    def save_index_data(
        self,
        index_code: str,
        df: pd.DataFrame,
        data_source: str = "akshare"
    ):
        """
        保存指数数据
        
        Args:
            index_code: 指数代码（如 000001.SH）
            df: 指数数据
            data_source: 数据源
        """
        if df.empty:
            print(f"警告：保存空指数数据到 {index_code}")
            return
        
        # 确保有必需的列
        required_columns = ['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount']
        for col in required_columns:
            if col not in df.columns:
                if col == 'code':
                    df[col] = index_code
                elif col == 'date':
                    df[col] = pd.to_datetime(df['date'])
                else:
                    df[col] = np.nan
        
        # 排序
        df = df.sort_values('date').reset_index(drop=True)
        
        # 保存为 Parquet
        file_path = self.get_index_daily_path(index_code)
        df.to_parquet(file_path, index=False, compression='snappy')
        
        # 更新元数据
        if index_code not in self.metadata:
            self.metadata[index_code] = {}
        
        self.metadata[index_code]['index'] = {
            'source': data_source,
            'freq': 'daily',
            'min_date': str(df['date'].min()),
            'max_date': str(df['date'].max()),
            'rows': len(df),
            'last_update': datetime.now().isoformat(),
            'checksum': self._generate_checksum(df),
            'file_path': str(file_path)
        }
        
        self._save_metadata()
        print(f"✓ 已保存 {index_code} 指数数据：{len(df)} 条记录 -> {file_path}")
    
    def load_index_data(
        self,
        index_code: str,
        start_date: str = None,
        end_date: str = None
    ) -> Optional[pd.DataFrame]:
        """加载指数数据"""
        file_path = self.get_index_daily_path(index_code)
        
        if not file_path.exists():
            return None
        
        try:
            df = pd.read_parquet(file_path)
            
            # 日期筛选
            if start_date or end_date:
                if 'date' in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df['date']):
                        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                    
                    if start_date and end_date:
                        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
                    elif start_date:
                        df = df[df['date'] >= start_date]
                    elif end_date:
                        df = df[df['date'] <= end_date]
            
            return df if not df.empty else None
            
        except Exception as e:
            print(f"加载 {index_code} 指数数据失败：{e}")
            return None
    
    def save_fundamentals_data(
        self,
        data_type: str,
        df: pd.DataFrame,
        data_source: str = "tushare"
    ):
        """
        保存基本面数据
        
        Args:
            data_type: 数据类型（financial_reports/valuation_metrics/industry）
            df: 基本面数据
            data_source: 数据源
        """
        if df.empty:
            print(f"警告：保存空基本面数据到 {data_type}")
            return
        
        file_path = self.get_fundamentals_path(data_type)
        df.to_parquet(file_path, index=False, compression='snappy')
        
        # 更新元数据
        if 'fundamentals' not in self.metadata:
            self.metadata['fundamentals'] = {}
        
        self.metadata['fundamentals'][data_type] = {
            'source': data_source,
            'rows': len(df),
            'last_update': datetime.now().isoformat(),
            'checksum': self._generate_checksum(df),
            'file_path': str(file_path)
        }
        
        self._save_metadata()
        print(f"✓ 已保存 {data_type} 基本面数据：{len(df)} 条记录 -> {file_path}")
    
    def load_fundamentals_data(
        self,
        data_type: str
    ) -> Optional[pd.DataFrame]:
        """加载基本面数据"""
        file_path = self.get_fundamentals_path(data_type)
        
        if not file_path.exists():
            return None
        
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            print(f"加载 {data_type} 基本面数据失败：{e}")
            return None
    
    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        """获取股票元数据信息"""
        return self.metadata.get(stock_code)
    
    def get_all_stocks(self) -> List[str]:
        """获取所有已缓存的股票代码"""
        return [code for code in self.metadata.keys() if 'daily' in self.metadata[code]]
    
    def check_data_quality(self, stock_code: str) -> Dict:
        """
        检查数据质量
        
        Returns:
            质量检查报告
        """
        df = self.load_daily_data(stock_code)
        
        if df is None or df.empty:
            return {'status': 'error', 'message': '数据不存在'}
        
        report = {
            'stock_code': stock_code,
            'total_rows': len(df),
            'date_range': f"{df['date'].min()} ~ {df['date'].max()}",
            'missing_values': {},
            'anomalies': []
        }
        
        # 检查缺失值
        for col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                report['missing_values'][col] = {
                    'count': int(missing_count),
                    'percentage': round(missing_count / len(df) * 100, 2)
                }
        
        # 检查异常值
        if 'close' in df.columns:
            # 检查价格是否为 0 或负数
            zero_prices = (df['close'] <= 0).sum()
            if zero_prices > 0:
                report['anomalies'].append(f"发现 {zero_prices} 条价格异常记录")
            
            # 检查涨跌幅是否超过 20%（异常波动）
            if 'pct_chg' in df.columns:
                extreme_changes = (df['pct_chg'].abs() > 20).sum()
                if extreme_changes > 0:
                    report['anomalies'].append(f"发现 {extreme_changes} 条涨跌幅异常记录")
        
        report['status'] = 'warning' if report['anomalies'] else 'ok'
        return report
    
    def migrate_from_csv(self, old_csv_file: str = "stock_data.csv"):
        """
        从旧版 CSV 格式迁移数据到新结构
        
        Args:
            old_csv_file: 旧 CSV 文件路径
        """
        if not os.path.exists(old_csv_file):
            print(f"错误：CSV 文件不存在：{old_csv_file}")
            return False
        
        print(f"\n开始从 CSV 迁移数据：{old_csv_file}")
        
        # 读取 CSV
        df = pd.read_csv(old_csv_file, parse_dates=['date'])
        print(f"✓ 读取 CSV 文件：{len(df)} 条记录")
        
        # 按股票代码分组
        stock_codes = df['code'].unique()
        print(f"✓ 发现 {len(stock_codes)} 只股票")
        
        # 逐只股票保存
        for stock_code in stock_codes:
            stock_df = df[df['code'] == stock_code].copy()
            self.save_daily_data(stock_code, stock_df)
        
        print(f"\n✓ 迁移完成！")
        return True
    
    def clear_all_data(self):
        """清空所有数据"""
        import shutil
        
        for directory in [self.daily_dir, self.fundamentals_dir, self.index_dir]:
            if directory.exists():
                shutil.rmtree(directory)
                directory.mkdir(parents=True, exist_ok=True)
        
        self.metadata = {}
        self._save_metadata()
        print("✓ 已清空所有数据")
