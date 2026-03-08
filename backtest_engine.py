"""
回测核心引擎
实现股票组合定期再平衡、收益指标计算
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from data_sources import DataSourceManager, TradingDayChecker


class BacktestEngine:
    """回测引擎"""
    
    def __init__(
        self,
        data_source: DataSourceManager,
        trading_day_checker: TradingDayChecker,
        data_manager = None
    ):
        self.data_source = data_source
        self.trading_day_checker = trading_day_checker
        self.data_manager = data_manager  # 添加数据管理器用于缓存
        self.results = None
        self.portfolio_values = None
        self.rebalance_dates = []
    
    def run_backtest(
        self,
        stocks: List[Dict],
        start_date: str,
        end_date: str,
        initial_capital: float = 1000000.0,
        rebalance_frequency: str = "monthly",
        commission_rate: float = 0.0003,
        slippage: float = 0.001
    ) -> Dict:
        """
        运行回测
        
        Args:
            stocks: 股票列表 [{"code": "sh.600000", "weight": 0.2}, ...]
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            initial_capital: 初始资金
            rebalance_frequency: 再平衡频率 ("monthly", "quarterly")
            commission_rate: 佣金费率
            slippage: 滑点
            
        Returns:
            回测结果字典
        """
        print(f"开始回测：{start_date} 至 {end_date}")
        print(f"股票数量：{len(stocks)}")
        print(f"初始资金：{initial_capital:,.2f}")
        
        # 获取所有股票数据（智能增量更新）
        stock_data_dict = {}
        for stock in stocks:
            code = stock['code']
            
            # 检查缓存并智能增量更新
            if self.data_manager is not None:
                try:
                    # 获取缓存中该股票的日期范围
                    cached_min, cached_max = self.data_manager.get_stock_date_range(code)
                    print(f"[DEBUG] {code} 缓存日期范围：{cached_min} ~ {cached_max}")
                    
                    if cached_min is None or cached_max is None:
                        # 缓存中没有该股票，需要获取全部数据
                        print(f"[INFO] {code} 缓存中不存在，从网络获取全部数据")
                        df = self.data_source.get_stock_data(code, start_date, end_date)
                        if df is not None and not df.empty:
                            stock_data_dict[code] = df
                            # 更新缓存
                            self.data_manager.update_cache(code, df, start_date, end_date)
                            print(f"[OK] {code} 从网络获取，{len(df)} 条记录，已保存缓存")
                        else:
                            print(f"[ERROR] {code} 数据获取失败")
                    else:
                        # 检查是否需要增量更新
                        need_start, need_end = self.data_manager.get_missing_date_range(code, start_date, end_date)
                        print(f"[DEBUG] {code} 需要更新范围：{need_start} ~ {need_end}")
                        
                        if need_start is None and need_end is None:
                            # 缓存完整，直接使用缓存
                            print(f"[INFO] {code} 缓存完整，从缓存读取")
                            df = self.data_manager.get_cached_data(code, start_date, end_date)
                            if df is not None and not df.empty:
                                stock_data_dict[code] = df
                                print(f"[OK] {code} 缓存完整，使用缓存，{len(df)} 条记录")
                            else:
                                print(f"[ERROR] {code} 缓存数据为空")
                        else:
                            # 需要增量更新
                            print(f"[INFO] {code} 需要增量更新：{need_start} ~ {need_end}")
                            df_new = self.data_source.get_stock_data(code, need_start, need_end)
                            if df_new is not None and not df_new.empty:
                                # 更新缓存
                                self.data_manager.update_cache(code, df_new, start_date, end_date)
                                print(f"[OK] {code} 增量获取 {len(df_new)} 条记录，已更新缓存")
                                
                                # 从更新后的缓存中获取完整数据
                                df = self.data_manager.get_cached_data(code, start_date, end_date)
                                if df is not None and not df.empty:
                                    stock_data_dict[code] = df
                                    print(f"[OK] {code} 从更新后的缓存读取，{len(df)} 条记录")
                                else:
                                    print(f"[ERROR] {code} 更新缓存后读取失败")
                            else:
                                print(f"[ERROR] {code} 增量数据获取失败")
                except Exception as e:
                    print(f"[ERROR] {code} 处理缓存时出错：{e}")
                    import traceback
                    print(traceback.format_exc())
                    # 回退到直接从数据源获取
                    df = self.data_source.get_stock_data(code, start_date, end_date)
                    if df is not None and not df.empty:
                        stock_data_dict[code] = df
                        print(f"[OK] {code} 从网络获取（回退模式），{len(df)} 条记录")
                    else:
                        print(f"[ERROR] {code} 数据获取失败")
            else:
                # 没有 data_manager，直接从数据源获取
                df = self.data_source.get_stock_data(code, start_date, end_date)
                if df is not None and not df.empty:
                    stock_data_dict[code] = df
                    print(f"[OK] {code} 从网络获取，{len(df)} 条记录")
                else:
                    print(f"[ERROR] {code} 数据获取失败")
        
        if not stock_data_dict:
            print("错误：没有获取到任何股票数据")
            return None
        
        # 生成交易日历
        all_dates = set()
        for df in stock_data_dict.values():
            # 确保日期是字符串格式
            date_list = df['date'].tolist()
            for d in date_list:
                if hasattr(d, 'strftime'):
                    all_dates.add(d.strftime('%Y-%m-%d'))
                else:
                    all_dates.add(str(d))
        
        trading_calendar = sorted(list(all_dates))
        
        if not trading_calendar:
            print("错误：交易日历为空")
            return None
        
        # 初始化回测变量
        portfolio = {stock['code']: 0 for stock in stocks}  # 持仓数量
        cash = initial_capital
        portfolio_values = []
        daily_returns = []
        self.rebalance_dates = []
        
        # 计算再平衡日期（转换为字符串格式）
        rebalance_dates_dt = self._calculate_rebalance_dates(
            start_date, end_date, rebalance_frequency
        )
        # 将 datetime 转换为字符串
        rebalance_dates = []
        for d in rebalance_dates_dt:
            if hasattr(d, 'strftime'):
                rebalance_dates.append(d.strftime('%Y-%m-%d'))
            else:
                rebalance_dates.append(str(d))
        self.rebalance_dates = rebalance_dates_dt  # 保存原始的 datetime 对象用于显示
        
        # 逐日回测
        for current_date in trading_calendar:
            date_str = current_date  # 已经是字符串格式
            
            # 检查是否需要再平衡
            if date_str in rebalance_dates:
                print(f"\n[{date_str}] 执行再平衡")
                portfolio, cash = self._rebalance_portfolio(
                    stocks, stock_data_dict, date_str, 
                    portfolio, cash, commission_rate, slippage
                )
            
            # 计算当前组合价值
            total_value = cash
            for stock in stocks:
                code = stock['code']
                if code in stock_data_dict:
                    df = stock_data_dict[code]
                    day_data = df[df['date'] == date_str]
                    if not day_data.empty:
                        close_price = day_data['close'].values[0]
                        total_value += portfolio[code] * close_price
            
            portfolio_values.append({
                'date': current_date,
                'total_value': total_value,
                'cash': cash,
                'stock_value': total_value - cash
            })
            
            # 计算日收益率
            if len(portfolio_values) > 1:
                prev_value = portfolio_values[-2]['total_value']
                curr_return = (total_value - prev_value) / prev_value
                daily_returns.append(curr_return)
        
        # 转换为 DataFrame
        self.portfolio_values = pd.DataFrame(portfolio_values)
        
        # 计算收益指标
        metrics = self._calculate_metrics(
            initial_capital, daily_returns, self.portfolio_values
        )
        
        self.results = {
            'portfolio_values': self.portfolio_values,
            'metrics': metrics,
            'rebalance_dates': rebalance_dates,
            'stocks': stocks
        }
        
        # 保存到缓存（如果提供了 data_manager）
        if self.data_manager is not None:
            print("\n正在保存数据到缓存...")
            for code, df in stock_data_dict.items():
                try:
                    self.data_manager.update_cache(code, df, start_date, end_date)
                    print(f"✓ {code} 数据已缓存")
                except Exception as e:
                    print(f"✗ {code} 缓存失败：{e}")
            print("缓存保存完成")
        
        print(f"\n回测完成！")
        print(f"最终资产：{self.portfolio_values['total_value'].iloc[-1]:,.2f}")
        print(f"累计收益：{metrics['total_return']*100:.2f}%")
        
        return self.results
    
    def _calculate_rebalance_dates(
        self, 
        start_date: str, 
        end_date: str, 
        frequency: str
    ) -> List[datetime]:
        """计算再平衡日期"""
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        rebalance_dates = []
        
        current = start
        while current <= end:
            if frequency == "monthly":
                # 每月 25 日后的第一个交易日
                rebalance_date = self.trading_day_checker.get_rebalance_date(
                    current.year, current.month
                )
                if start <= rebalance_date <= end:
                    rebalance_dates.append(rebalance_date)
                # 移动到下个月
                if current.month == 12:
                    current = datetime(current.year + 1, 1, 1)
                else:
                    current = datetime(current.year, current.month + 1, 1)
            
            elif frequency == "quarterly":
                # 每季度最后一个月 25 日后的第一个交易日
                quarter_months = [3, 6, 9, 12]
                for month in quarter_months:
                    rebalance_date = self.trading_day_checker.get_rebalance_date(
                        current.year, month
                    )
                    if start <= rebalance_date <= end:
                        rebalance_dates.append(rebalance_date)
                current = datetime(current.year + 1, 1, 1)
            
            elif frequency == "semi-annually":
                # 每半年：6 月和 12 月 25 日后的第一个交易日
                half_year_months = [6, 12]
                for month in half_year_months:
                    rebalance_date = self.trading_day_checker.get_rebalance_date(
                        current.year, month
                    )
                    if start <= rebalance_date <= end:
                        rebalance_dates.append(rebalance_date)
                # 移动到下一年
                current = datetime(current.year + 1, 1, 1)
            
            elif frequency == "annually":
                # 每年：12 月 25 日后的第一个交易日
                rebalance_date = self.trading_day_checker.get_rebalance_date(
                    current.year, 12
                )
                if start <= rebalance_date <= end:
                    rebalance_dates.append(rebalance_date)
                # 移动到下一年
                current = datetime(current.year + 1, 1, 1)
        
        return sorted(list(set(rebalance_dates)))
    
    def _rebalance_portfolio(
        self,
        stocks: List[Dict],
        stock_data_dict: Dict,
        rebalance_date,  # 可以是 datetime 或字符串
        current_portfolio: Dict,
        current_cash: float,
        commission_rate: float,
        slippage: float
    ) -> Tuple[Dict, float]:
        """
        执行再平衡
        
        Returns:
            (新持仓，新现金)
        """
        # 将日期转换为字符串格式进行比较
        if hasattr(rebalance_date, 'strftime'):
            date_str = rebalance_date.strftime('%Y-%m-%d')
        else:
            date_str = str(rebalance_date)
        
        # 计算当前总资产
        total_value = current_cash
        for stock in stocks:
            code = stock['code']
            if code in stock_data_dict:
                df = stock_data_dict[code]
                day_data = df[df['date'] == date_str]
                if not day_data.empty:
                    close_price = day_data['close'].values[0]
                    total_value += current_portfolio[code] * close_price
        
        # 计算目标持仓
        new_portfolio = {stock['code']: 0 for stock in stocks}
        transaction_costs = 0
        
        for stock in stocks:
            code = stock['code']
            target_value = total_value * stock['weight']
            
            if code in stock_data_dict:
                df = stock_data_dict[code]
                day_data = df[df['date'] == date_str]
                if not day_data.empty:
                    close_price = day_data['close'].values[0]
                    # 考虑滑点
                    adjusted_price = close_price * (1 + slippage)
                    # 计算目标股数（向下取整，避免现金不足）
                    target_shares = int(target_value / adjusted_price / 100) * 100
                    new_portfolio[code] = target_shares
                    
                    # 计算交易费用
                    if code in current_portfolio:
                        diff_shares = abs(target_shares - current_portfolio[code])
                        transaction_costs += diff_shares * adjusted_price * commission_rate
        
        # 更新现金（使用包含滑点的价格计算实际花费）
        stock_purchase_cost = 0
        for stock in stocks:
            code = stock['code']
            if code in stock_data_dict and new_portfolio[code] > 0:
                df = stock_data_dict[code]
                day_data = df[df['date'] == rebalance_date]
                if not day_data.empty:
                    close_price = day_data['close'].values[0]
                    # 买入时考虑滑点（实际支付更多）
                    adjusted_price = close_price * (1 + slippage)
                    stock_purchase_cost += new_portfolio[code] * adjusted_price
        
        new_cash = total_value - stock_purchase_cost - transaction_costs
        
        return new_portfolio, max(new_cash, 0)  # 确保现金不为负
    
    def _calculate_metrics(
        self,
        initial_capital: float,
        daily_returns: List[float],
        portfolio_values: pd.DataFrame
    ) -> Dict:
        """计算收益指标"""
        if portfolio_values.empty or len(daily_returns) == 0:
            return {}
        
        final_value = portfolio_values['total_value'].iloc[-1]
        
        # 累计收益率
        total_return = (final_value - initial_capital) / initial_capital
        
        # 年化收益率
        days = (portfolio_values['date'].iloc[-1] - portfolio_values['date'].iloc[0]).days
        years = days / 365.25
        annual_return = (final_value / initial_capital) ** (1 / years) - 1 if years > 0 else 0
        
        # 波动率（年化）
        daily_returns_series = pd.Series(daily_returns)
        volatility = daily_returns_series.std() * np.sqrt(252)
        
        # 最大回撤
        cum_max = portfolio_values['total_value'].cummax()
        drawdown = (portfolio_values['total_value'] - cum_max) / cum_max
        max_drawdown = drawdown.min()
        
        # 夏普比率（假设无风险利率为 3%）
        risk_free_rate = 0.03
        sharpe_ratio = (annual_return - risk_free_rate) / volatility if volatility > 0 else 0
        
        # 胜率
        winning_days = (daily_returns_series > 0).sum()
        total_days = len(daily_returns_series)
        win_rate = winning_days / total_days if total_days > 0 else 0
        
        # 卡玛比率
        calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        return {
            'initial_capital': initial_capital,
            'final_value': final_value,
            'total_return': total_return,
            'total_return_pct': f"{total_return*100:.2f}%",
            'annual_return': annual_return,
            'annual_return_pct': f"{annual_return*100:.2f}%",
            'volatility': volatility,
            'volatility_pct': f"{volatility*100:.2f}%",
            'max_drawdown': max_drawdown,
            'max_drawdown_pct': f"{max_drawdown*100:.2f}%",
            'sharpe_ratio': sharpe_ratio,
            'calmar_ratio': calmar_ratio,
            'win_rate': win_rate,
            'win_rate_pct': f"{win_rate*100:.2f}%",
            'trading_days': total_days,
            'years': years
        }
    
    def get_portfolio_composition(self, date: datetime = None) -> pd.DataFrame:
        """获取组合持仓"""
        if self.results is None:
            return pd.DataFrame()
        
        # 默认使用最后一个交易日
        if date is None:
            date = self.portfolio_values['date'].iloc[-1]
        
        # 这里需要扩展存储持仓数据
        # 目前简化处理
        return pd.DataFrame(self.results['stocks'])
    
    def export_results(self, filename: str = "backtest_results.csv"):
        """导出回测结果"""
        if self.portfolio_values is not None:
            self.portfolio_values.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"结果已导出至：{filename}")
