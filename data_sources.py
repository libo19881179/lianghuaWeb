"""
数据源适配模块
仅使用 Baostock（免费官方）获取数据
"""

import baostock as bs
import pandas as pd
import random
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Callable, Any
import functools

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def retry_on_network_error(max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 15.0):
    """
    网络请求重试装饰器
    
    Args:
        max_retries: 最大重试次数
        base_delay: 基础延迟时间（秒）
        max_delay: 最大延迟时间（秒）
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    # 每次请求前增加延迟
                    if attempt > 0:
                        # 重试时增加更长的延迟
                        delay = min(base_delay + attempt * 2.0, max_delay)
                        logger.info(f"    等待 {delay:.1f} 秒后重试...")
                        time.sleep(delay)
                    else:
                        # 首次尝试，随机延迟
                        time.sleep(random.uniform(0.5, 1.5))
                    
                    return func(*args, **kwargs)
                except Exception as e:
                    error_msg = str(e)
                    # 如果是连接中断错误，增加更长的等待并重新登录
                    if 'Connection aborted' in error_msg or 'RemoteDisconnected' in error_msg or 'WinError 10038' in error_msg:
                        wait_time = min(10.0 + attempt * 5.0, 30.0)
                        logger.warning(f"    连接中断，等待 {wait_time:.1f} 秒...")
                        time.sleep(wait_time)
                        
                        # 检查是否是实例方法
                        if args and hasattr(args[0], '_init_baostock'):
                            logger.info("    重新登录 Baostock...")
                            args[0]._init_baostock()
                            time.sleep(2.0)  # 登录后等待
                    
                    logger.error(f"{func.__name__} 第{attempt+1}次尝试失败：{e}")
                    if attempt == max_retries - 1:
                        raise
                    continue
        return wrapper
    return decorator


class DataSourceManager:
    """数据源管理器，仅使用 Baostock"""
    
    def __init__(self):
        self.current_source = "baostock"
        self.bs_login_status = False
        
        # 初始化 Baostock
        self._init_baostock()
    
    def __enter__(self):
        """进入上下文管理器"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器，确保资源释放"""
        self.logout()
    
    def _init_baostock(self):
        """初始化 Baostock"""
        try:
            lg = bs.login()
            self.bs_login_status = (lg.error_code == '0')
            if self.bs_login_status:
                logger.info("✓ Baostock 登录成功")
            else:
                logger.error(f"✗ Baostock 登录失败：error_code={lg.error_code}, error_msg={lg.error_msg}")
        except Exception as e:
            logger.error(f"✗ Baostock 初始化失败：{e}")
            self.bs_login_status = False
    
    def _random_delay(self, min_sec: float = 1.0, max_sec: float = 3.0):
        """随机延迟，规避接口风控（批量下载时增加延迟）"""
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)
    
    @retry_on_network_error(max_retries=5)
    def get_stock_data(
        self, 
        stock_code: str, 
        start_date: str, 
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取股票数据，仅使用 Baostock
        
        Args:
            stock_code: 股票代码（如 sh.600000）
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）
            
        Returns:
            DataFrame 包含股票数据
        """
        # 参数验证
        if not stock_code:
            logger.error("错误：股票代码不能为空")
            return None
        
        if not start_date:
            logger.error("错误：开始日期不能为空")
            return None
        
        if not end_date:
            logger.error("错误：结束日期不能为空")
            return None
        
        # 验证日期格式
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            logger.error("错误：日期格式错误，应为 YYYY-MM-DD")
            return None
        
        # 验证日期顺序
        if start_date > end_date:
            logger.error("错误：开始日期不能晚于结束日期")
            return None
        
        if self.bs_login_status:
            df = self._get_from_baostock(stock_code, start_date, end_date)
            if df is not None and not df.empty:
                return df
        
        logger.error("Baostock 获取数据失败")
        return None
    
    def _get_baostock_data(
        self, 
        code: str, 
        start_date: str, 
        end_date: str, 
        fields: str, 
        frequency: str = "d",
        adjustflag: str = "3"
    ) -> Optional[pd.DataFrame]:
        """从 Baostock 获取数据的通用方法"""
        try:
            rs = bs.query_history_k_data_plus(
                code,
                fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag
            )
            
            data_list = []
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return None
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            df['date'] = pd.to_datetime(df['date'])
            
            # 自动转换数值列
            for col in df.columns:
                if col not in ['date', 'code']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.sort_values('date').reset_index(drop=True)
            return df
            
        except Exception as e:
            logger.error(f"Baostock 获取数据失败：{e}")
            return None
    
    def _get_from_baostock(
        self, 
        stock_code: str, 
        start_date: str, 
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """从 Baostock 获取股票数据（分两次查询以保证稳定性）"""
        try:
            # 转换股票代码格式
            code = stock_code.lower()
            
            # 第一次查询：获取基本字段（这些字段最稳定）
            basic_fields = "date,code,open,high,low,close,volume,amount,adjustflag"
            df = self._get_baostock_data(code, start_date, end_date, basic_fields)
            
            if df is None:
                logger.info(f"  Baostock 返回空数据（可能是停牌或无数据）")
                return None
            
            # 第二次查询：获取估值和股本数据（这些字段可能为空）
            try:
                extra_fields = "date,code,pe,pb,ps,pcf,total_shares,float_shares,total_mv,circ_mv"
                extra_df = self._get_baostock_data(code, start_date, end_date, extra_fields)
                
                # 合并数据
                if extra_df is not None:
                    df = pd.merge(df, extra_df, on=['date', 'code'], how='left')
                    
            except Exception as e:
                logger.warning(f"  获取扩展字段失败：{e}，将只使用基本字段")
            
            return df
            
        except Exception as e:
            logger.error(f"Baostock 获取数据失败：{e}")
            return None
    
    @retry_on_network_error(max_retries=5)
    def get_all_a_stock_codes(self) -> pd.DataFrame:
        """获取所有 A 股代码列表（排除指数、基金、债券等）"""
        # 使用 Baostock
        if self.bs_login_status:
            logger.info("  从 Baostock 获取 A 股列表...")
            
            # 尝试获取最近的有数据的日期
            current_date = datetime.now().strftime('%Y-%m-%d')
            max_attempts = 30  # 最多尝试30天
            data_list = []
            rs = None
            
            for attempt in range(max_attempts):
                try:
                    rs = bs.query_all_stock(day=current_date)
                    if rs and rs.error_code == '0':
                        # 检查是否有数据
                        temp_data = []
                        while rs.next():
                            temp_data.append(rs.get_row_data())
                        if temp_data:
                            data_list = temp_data
                            logger.info(f"  使用日期：{current_date}")
                            break
                except Exception as e:
                    logger.error(f"  日期 {current_date} 获取失败：{e}")
                
                # 如果当前日期没有数据，尝试前一天
                current_date = (datetime.strptime(current_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"  尝试前一天日期：{current_date}")
            
            try:
                # 如果没有获取到数据，再次尝试
                if not data_list:
                    rs = bs.query_all_stock(day=current_date)
                    if rs and rs.error_code == '0':
                        while rs.next():
                            data_list.append(rs.get_row_data())
                
                if data_list:
                    df = pd.DataFrame(data_list, columns=rs.fields)
                    logger.info(f"  ✓ Baostock 获取成功：{len(df)} 只股票")
                    # 筛选 A 股股票：代码以 60/68/00/30 开头，且状态为 1（正常交易）
                    if 'code' in df.columns:
                        # 确保 code 列是字符串类型
                        df['code'] = df['code'].astype(str)
                        
                        # 提取代码数字部分进行筛选
                        def extract_code_number(code):
                            if isinstance(code, str):
                                # 去除市场前缀（如 sh. 或 sz.）
                                if '.' in code:
                                    return code.split('.')[-1]
                            return code
                        
                        # 应用函数提取代码数字部分
                        df['code_number'] = df['code'].apply(extract_code_number)
                        
                        # 检查是否有 tradeStatus 列
                        if 'tradeStatus' in df.columns:
                            # 先筛选代码：60、68、00、30 开头
                            code_filtered = df[(df['code_number'].str.startswith('60', na=False)) | 
                                           (df['code_number'].str.startswith('68', na=False)) | 
                                           (df['code_number'].str.startswith('00', na=False)) | 
                                           (df['code_number'].str.startswith('30', na=False))]
                            
                            # 再筛选交易状态
                            df = code_filtered[code_filtered['tradeStatus'] == '1']  # 只保留正常交易的股票
                            logger.info(f"  ✓ 筛选后 A 股数量：{len(df)} 只")
                        else:
                            # 如果没有 tradeStatus 列，只根据代码筛选
                            df = df[(df['code_number'].str.startswith('60', na=False)) | 
                                   (df['code_number'].str.startswith('68', na=False)) | 
                                   (df['code_number'].str.startswith('00', na=False)) | 
                                   (df['code_number'].str.startswith('30', na=False))]
                            logger.info(f"  没有 tradeStatus 列，仅按代码筛选：{len(df)} 只")
                        
                        # 标准化股票代码格式和列名
                        # 复制数据以避免修改原始数据
                        standardized_df = df.copy()
                        
                        # 确保 code 列是字符串类型
                        standardized_df['code'] = standardized_df['code'].astype(str)
                        
                        # 标准化股票代码格式
                        def format_code(code):
                            if isinstance(code, str):
                                if not code.startswith('sh.') and not code.startswith('sz.'):
                                    if code.startswith('6'):
                                        return f"sh.{code}"
                                    else:
                                        return f"sz.{code}"
                            return code
                        
                        # 使用向量化操作
                        standardized_df['code'] = standardized_df['code'].apply(format_code)
                        
                        # 提取股票名称
                        if 'stockName' in standardized_df.columns:
                            standardized_df['name'] = standardized_df['stockName']
                        elif 'name' in standardized_df.columns:
                            standardized_df['name'] = standardized_df['name']
                        else:
                            standardized_df['name'] = ''
                        
                        # 只保留需要的列
                        standardized_df = standardized_df[['code', 'name']]
                        
                        # 返回标准化的 DataFrame
                        return standardized_df
                    else:
                        logger.error("  ✗ 数据格式错误：缺少 code 列")
                else:
                    logger.error("  ✗ Baostock 返回空数据")
            except Exception as e:
                logger.error(f"  获取 A 股列表失败：{e}")
                # 尝试重新登录
                logger.info("  尝试重新登录 Baostock...")
                self._init_baostock()
                time.sleep(2)
                # 再次尝试获取
                try:
                    rs = bs.query_all_stock(day=current_date)
                    data_list = []
                    while rs and (rs.error_code == '0') and rs.next():
                        data_list.append(rs.get_row_data())
                    
                    if data_list:
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        logger.info(f"  ✓ Baostock 获取成功：{len(df)} 只股票")
                        # 筛选和标准化处理
                        if 'code' in df.columns:
                            df['code'] = df['code'].astype(str)
                            if 'tradeStatus' in df.columns:
                                df = df[(df['code'].str.startswith('60', na=False)) | 
                                       (df['code'].str.startswith('68', na=False)) | 
                                       (df['code'].str.startswith('00', na=False)) | 
                                       (df['code'].str.startswith('30', na=False))]
                                df = df[df['tradeStatus'] == '1']
                                logger.info(f"  ✓ 筛选后 A 股数量：{len(df)} 只")
                            
                            standardized_df = df.copy()
                            standardized_df['code'] = standardized_df['code'].astype(str)
                            
                            def format_code(code):
                                if isinstance(code, str):
                                    if not code.startswith('sh.') and not code.startswith('sz.'):
                                        if code.startswith('6'):
                                            return f"sh.{code}"
                                        else:
                                            return f"sz.{code}"
                                return code
                            
                            standardized_df['code'] = standardized_df['code'].apply(format_code)
                            
                            if 'stockName' in standardized_df.columns:
                                standardized_df['name'] = standardized_df['stockName']
                            elif 'name' in standardized_df.columns:
                                standardized_df['name'] = standardized_df['name']
                            else:
                                standardized_df['name'] = ''
                            
                            standardized_df = standardized_df[['code', 'name']]
                            return standardized_df
                except Exception as e2:
                    logger.error(f"  再次尝试失败：{e2}")
        
        logger.error("  ✗ 获取 A 股列表失败")
        return pd.DataFrame()
    
    def get_all_stock_codes(self) -> pd.DataFrame:
        """获取所有 A 股代码列表（已废弃，请使用 get_all_a_stock_codes）"""
        return self.get_all_a_stock_codes()
    
    @retry_on_network_error(max_retries=3)
    def get_index_data(
        self,
        index_code: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取指数数据，仅使用 Baostock
        
        Args:
            index_code: 指数代码（如 000001.SH 上证指数）
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）
            
        Returns:
            DataFrame 包含指数数据
        """
        if self.bs_login_status:
            df = self._get_index_from_baostock(index_code, start_date, end_date)
            if df is not None and not df.empty:
                return df
        
        logger.error(f"Baostock 获取指数 {index_code} 失败")
        return None
    
    def _get_index_from_baostock(
        self,
        index_code: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """从 Baostock 获取指数数据"""
        try:
            # 转换指数代码格式
            code = index_code.lower()
            
            fields = "date,code,open,high,low,close,volume,amount"
            df = self._get_baostock_data(code, start_date, end_date, fields)
            
            return df
            
        except Exception as e:
            logger.error(f"Baostock 获取指数数据失败：{e}")
            return None
    
    def logout(self):
        """登出 Baostock"""
        if self.bs_login_status:
            bs.logout()
    
    @retry_on_network_error(max_retries=3)
    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        """
        获取股票基本信息（代码、名称），仅使用 Baostock
        
        Args:
            stock_code: 股票代码（如 sh.600000 或 600000）
            
        Returns:
            包含股票信息的字典，或 None
        """
        # 格式化股票代码
        formatted_code = self._format_stock_code(stock_code)
        
        # 仅使用 Baostock
        if self.bs_login_status:
            info = self._get_stock_info_from_baostock(formatted_code)
            if info:
                return info
        
        return None
    
    def _get_stock_info_from_baostock(self, stock_code: str) -> Optional[Dict]:
        """从 Baostock 获取股票信息"""
        try:
            # 获取基本资料
            code = stock_code.lower()
            
            rs = bs.query_stock_basic(code)
            
            data_list = []
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())
            
            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)
                
                if not df.empty:
                    return {
                        'code': stock_code,
                        'name': df['stockName'].iloc[0] if 'stockName' in df.columns else '',
                        'ipoDate': df.get('ipoDate', [''])[0],
                    }
        except Exception as e:
            pass
        
        return None
    
    def _format_stock_code(self, code: str) -> str:
        """
        格式化股票代码（内部使用）
        
        Args:
            code: 原始代码
            
        Returns:
            格式化后的代码（如 sh.600000）
        """
        code = code.strip()
        
        # 如果已经有市场前缀
        if '.' in code:
            return code
        
        # 纯数字，根据规则判断市场
        if code.startswith('6'):
            return f"sh.{code}"
        elif code.startswith('0') or code.startswith('3'):
            return f"sz.{code}"
        else:
            return f"sh.{code}"  # 默认沪市


class TradingDayChecker:
    """交易日判断工具类"""
    
    def __init__(self, data_source: DataSourceManager):
        self.data_source = data_source
        self._trading_days_cache = {}
        self._holidays = set()  # 存储已知的节假日
        
        # 初始化一些常见的节假日（可以根据实际情况扩展）
        self._init_holidays()
    
    def _init_holidays(self):
        """初始化节假日列表"""
        # 这里可以添加已知的节假日
        # 格式：{year: {month: [day1, day2, ...]}}
        holidays_data = {
            2024: {
                1: [1, 2, 3, 4, 5, 6, 7],  # 元旦
                2: [10, 11, 12],  # 春节
                4: [4, 5, 6],  # 清明节
                5: [1, 2, 3, 4, 5],  # 劳动节
                6: [10, 11, 12],  # 端午节
                9: [15, 16, 17],  # 中秋节
                10: [1, 2, 3, 4, 5, 6, 7]  # 国庆节
            }
            # 可以添加其他年份的节假日
        }
        
        # 转换为datetime对象集合
        for year, months in holidays_data.items():
            for month, days in months.items():
                for day in days:
                    self._holidays.add(datetime(year, month, day))
    
    def _get_trading_days(self, year: int = None) -> list:
        """获取指定年份的交易日列表"""
        if year is None:
            year = datetime.now().year
        
        # 检查缓存
        if year in self._trading_days_cache:
            return self._trading_days_cache[year]
        
        # 生成工作日列表
        trading_days = []
        start = datetime(year, 1, 1)
        end = datetime(year, 12, 31)
        current = start
        
        while current <= end:
            # 排除周末和已知节假日
            if current.weekday() < 5 and current not in self._holidays:
                trading_days.append(current)
            current += timedelta(days=1)
        
        self._trading_days_cache[year] = trading_days
        return trading_days
    
    def is_trading_day(self, date: datetime) -> bool:
        """判断是否为交易日"""
        # 首先检查是否为已知节假日
        if date in self._holidays:
            return False
        
        # 检查是否为周末
        if date.weekday() >= 5:
            return False
        
        # 进一步验证（可以通过API获取实际交易日）
        trading_days = self._get_trading_days(date.year)
        return date in trading_days
    
    def get_previous_trading_day(self, date: datetime, days_back: int = 1) -> datetime:
        """获取指定日期前的第 N 个交易日"""
        if days_back <= 0:
            return date
        
        # 从指定日期向前查找
        current = date
        count = 0
        
        while count < days_back:
            current -= timedelta(days=1)
            if self.is_trading_day(current):
                count += 1
        
        return current
    
    def get_next_trading_day(self, date: datetime, days_forward: int = 1) -> datetime:
        """获取指定日期后的第 N 个交易日"""
        if days_forward <= 0:
            return date
        
        # 从指定日期向后查找
        current = date
        count = 0
        
        while count < days_forward:
            current += timedelta(days=1)
            if self.is_trading_day(current):
                count += 1
        
        return current
    
    def get_rebalance_date(self, year: int, month: int) -> datetime:
        """
        获取指定年月的再平衡日期（每月 25 日后的第一个交易日）
        
        Args:
            year: 年份
            month: 月份
            
        Returns:
            再平衡日期
        """
        # 构造 25 日日期
        try:
            target_date = datetime(year, month, 25)
        except ValueError:
            # 处理 2 月等特殊情况
            target_date = datetime(year, month, 28)
        
        # 找到 25 日后的第一个交易日
        current = target_date
        while True:
            if self.is_trading_day(current):
                return current
            current += timedelta(days=1)
        
        return target_date
