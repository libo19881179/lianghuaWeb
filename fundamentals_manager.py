"""
基本面数据管理模块
支持财务指标、估值数据、行业分类等基本面数据的获取和缓存
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from data_storage import EnhancedDataStorage


class FundamentalsManager:
    """基本面数据管理器"""
    
    def __init__(self, data_storage: EnhancedDataStorage = None):
        self.data_storage = data_storage or EnhancedDataStorage()
    
    def fetch_valuation_metrics(
        self,
        stock_codes: List[str],
        data_source,
        trade_date: str = None
    ) -> pd.DataFrame:
        """
        获取估值指标数据
        
        Args:
            stock_codes: 股票代码列表
            data_source: DataSourceManager 实例
            trade_date: 交易日期（默认最新）
            
        Returns:
            估值指标 DataFrame
        """
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y-%m-%d')
        
        all_metrics = []
        
        for stock_code in stock_codes:
            try:
                # 从数据源获取估值数据
                metrics = self._fetch_single_stock_valuation(
                    stock_code, trade_date, data_source
                )
                
                if metrics is not None:
                    all_metrics.append(metrics)
                    
            except Exception as e:
                print(f"获取 {stock_code} 估值数据失败：{e}")
        
        if all_metrics:
            df = pd.DataFrame(all_metrics)
            
            # 保存到缓存
            self.data_storage.save_fundamentals_data(
                'valuation_metrics',
                df,
                data_source='tushare'
            )
            
            return df
        
        return pd.DataFrame()
    
    def _fetch_single_stock_valuation(
        self,
        stock_code: str,
        trade_date: str,
        data_source
    ) -> Optional[Dict]:
        """获取单只股票的估值指标"""
        try:
            # 尝试从 Tushare 获取
            if hasattr(data_source, 'pro') and data_source.pro is not None:
                ts_code = self._convert_to_ts_code(stock_code)
                
                # 获取每日估值指标
                df = data_source.pro.daily_basic(
                    ts_code=ts_code,
                    trade_date=trade_date.replace('-', '')
                )
                
                if df is not None and not df.empty:
                    return {
                        'date': trade_date,
                        'code': stock_code,
                        'pe': df['pe'].iloc[0] if 'pe' in df.columns else np.nan,
                        'pe_ttm': df['pe_ttm'].iloc[0] if 'pe_ttm' in df.columns else np.nan,
                        'pb': df['pb'].iloc[0] if 'pb' in df.columns else np.nan,
                        'ps': df['ps'].iloc[0] if 'ps' in df.columns else np.nan,
                        'total_shares': df['total_share'].iloc[0] if 'total_share' in df.columns else np.nan,
                        'float_shares': df['float_share'].iloc[0] if 'float_share' in df.columns else np.nan,
                        'total_mv': df['total_mv'].iloc[0] if 'total_mv' in df.columns else np.nan,
                        'circ_mv': df['circ_mv'].iloc[0] if 'circ_mv' in df.columns else np.nan,
                        'turnover_rate': df['turnover_rate'].iloc[0] if 'turnover_rate' in df.columns else np.nan,
                        'volume_ratio': df['volume_ratio'].iloc[0] if 'volume_ratio' in df.columns else np.nan,
                    }
            
            # 如果 Tushare 失败，尝试从行情数据估算
            daily_df = self.data_storage.load_daily_data(stock_code)
            if daily_df is not None and not daily_df.empty:
                # 筛选最近日期
                latest_date = daily_df['date'].max()
                latest_row = daily_df[daily_df['date'] == latest_date].iloc[0]
                
                return {
                    'date': str(latest_date),
                    'code': stock_code,
                    'pe': np.nan,  # 需要财务报表数据才能计算
                    'pe_ttm': np.nan,
                    'pb': np.nan,
                    'ps': np.nan,
                    'total_shares': np.nan,
                    'float_shares': np.nan,
                    'total_mv': latest_row.get('total_mv', np.nan),
                    'circ_mv': latest_row.get('circ_mv', np.nan),
                    'turnover_rate': latest_row.get('turnover_rate', np.nan),
                    'volume_ratio': latest_row.get('volume_ratio', np.nan),
                }
                
        except Exception as e:
            print(f"获取 {stock_code} 估值指标失败：{e}")
        
        return None
    
    def fetch_financial_reports(
        self,
        stock_codes: List[str],
        data_source
    ) -> pd.DataFrame:
        """
        获取财务指标数据
        
        Args:
            stock_codes: 股票代码列表
            data_source: DataSourceManager 实例
            
        Returns:
            财务指标 DataFrame
        """
        all_reports = []
        
        for stock_code in stock_codes:
            try:
                report = self._fetch_single_stock_financials(
                    stock_code, data_source
                )
                
                if report is not None:
                    all_reports.append(report)
                    
            except Exception as e:
                print(f"获取 {stock_code} 财务数据失败：{e}")
        
        if all_reports:
            df = pd.DataFrame(all_reports)
            
            # 保存到缓存
            self.data_storage.save_fundamentals_data(
                'financial_reports',
                df,
                data_source='tushare'
            )
            
            return df
        
        return pd.DataFrame()
    
    def _fetch_single_stock_financials(
        self,
        stock_code: str,
        data_source
    ) -> Optional[Dict]:
        """获取单只股票的财务指标"""
        try:
            if hasattr(data_source, 'pro') and data_source.pro is not None:
                ts_code = self._convert_to_ts_code(stock_code)
                
                # 获取最新财务指标
                df = data_source.pro.fina_indicator(ts_code=ts_code)
                
                if df is not None and not df.empty:
                    latest = df.iloc[0]
                    
                    return {
                        'code': stock_code,
                        'report_date': latest.get('ann_date', ''),
                        'roe': latest.get('roe', np.nan),
                        'roa': latest.get('roa', np.nan),
                        'gross_margin': latest.get('gross_margin', np.nan),
                        'net_margin': latest.get('net_profit_margin', np.nan),
                        'debt_ratio': latest.get('debt_ratio', np.nan),
                        'revenue': latest.get('total_revenue', np.nan),
                        'net_profit': latest.get('net_profit', np.nan),
                        'operating_profit': latest.get('operating_profit', np.nan),
                        'eps': latest.get('basic_eps', np.nan),
                    }
                    
        except Exception as e:
            print(f"获取 {stock_code} 财务指标失败：{e}")
        
        return None
    
    def fetch_industry_classification(
        self,
        stock_codes: List[str],
        data_source
    ) -> pd.DataFrame:
        """
        获取行业分类数据
        
        Args:
            stock_codes: 股票代码列表
            data_source: DataSourceManager 实例
            
        Returns:
            行业分类 DataFrame
        """
        all_industries = []
        
        for stock_code in stock_codes:
            try:
                industry = self._fetch_single_stock_industry(
                    stock_code, data_source
                )
                
                if industry is not None:
                    all_industries.append(industry)
                    
            except Exception as e:
                print(f"获取 {stock_code} 行业分类失败：{e}")
        
        if all_industries:
            df = pd.DataFrame(all_industries)
            
            # 保存到缓存
            self.data_storage.save_fundamentals_data(
                'industry_classification',
                df,
                data_source='tushare'
            )
            
            return df
        
        return pd.DataFrame()
    
    def _fetch_single_stock_industry(
        self,
        stock_code: str,
        data_source
    ) -> Optional[Dict]:
        """获取单只股票的行业分类"""
        try:
            if hasattr(data_source, 'pro') and data_source.pro is not None:
                ts_code = self._convert_to_ts_code(stock_code)
                
                # 获取股票基本信息
                df = data_source.pro.stock_basic(ts_code=ts_code)
                
                if df is not None and not df.empty:
                    return {
                        'code': stock_code,
                        'name': df.get('name', [''])[0],
                        'industry_sw': df.get('industry', [''])[0],  # 申万行业
                        'area': df.get('area', [''])[0],  # 地区
                        'market': df.get('market', [''])[0],  # 所属板块
                        'list_date': df.get('list_date', [''])[0],  # 上市日期
                    }
                    
        except Exception as e:
            print(f"获取 {stock_code} 行业分类失败：{e}")
        
        return None
    
    def _convert_to_ts_code(self, stock_code: str) -> str:
        """
        将股票代码转换为 Tushare 格式
        
        Args:
            stock_code: 原始代码（如 sh.600000）
            
        Returns:
            Tushare 格式代码（如 600000.SH）
        """
        code = stock_code.split('.')[-1]
        exchange = 'SH' if stock_code.startswith('sh') else 'SZ'
        return f"{code}.{exchange}"
    
    def get_cached_valuation(self, stock_code: str) -> Optional[pd.DataFrame]:
        """获取缓存的估值数据"""
        df = self.data_storage.load_fundamentals_data('valuation_metrics')
        if df is not None and not df.empty:
            return df[df['code'] == stock_code]
        return None
    
    def get_cached_financials(self, stock_code: str) -> Optional[pd.DataFrame]:
        """获取缓存的财务数据"""
        df = self.data_storage.load_fundamentals_data('financial_reports')
        if df is not None and not df.empty:
            return df[df['code'] == stock_code]
        return None
    
    def get_cached_industry(self, stock_code: str) -> Optional[pd.DataFrame]:
        """获取缓存的行业分类"""
        df = self.data_storage.load_fundamentals_data('industry_classification')
        if df is not None and not df.empty:
            return df[df['code'] == stock_code]
        return None
    
    def update_all_fundamentals(
        self,
        stock_codes: List[str],
        data_source
    ) -> Dict:
        """
        更新所有股票的基本面数据
        
        Args:
            stock_codes: 股票代码列表
            data_source: DataSourceManager 实例
            
        Returns:
            更新结果统计
        """
        print("\n========== 开始更新基本面数据 ==========")
        
        result = {
            'success': True,
            'valuation_count': 0,
            'financial_count': 0,
            'industry_count': 0,
            'errors': []
        }
        
        try:
            # 更新估值数据
            print("[1/3] 更新估值指标...")
            valuation_df = self.fetch_valuation_metrics(stock_codes, data_source)
            if not valuation_df.empty:
                result['valuation_count'] = len(valuation_df)
                print(f"✓ 更新估值数据：{len(valuation_df)} 只股票")
            
            # 更新财务数据
            print("[2/3] 更新财务指标...")
            financial_df = self.fetch_financial_reports(stock_codes, data_source)
            if not financial_df.empty:
                result['financial_count'] = len(financial_df)
                print(f"✓ 更新财务数据：{len(financial_df)} 只股票")
            
            # 更新行业分类
            print("[3/3] 更新行业分类...")
            industry_df = self.fetch_industry_classification(stock_codes, data_source)
            if not industry_df.empty:
                result['industry_count'] = len(industry_df)
                print(f"✓ 更新行业分类：{len(industry_df)} 只股票")
            
            print("\n========== 基本面数据更新完成 ==========")
            
        except Exception as e:
            result['success'] = False
            result['errors'].append(str(e))
            print(f"更新基本面数据失败：{e}")
        
        return result
