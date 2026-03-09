"""
策略选股模块
实现三种稳健的A股多股策略模型
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class BaseStrategy:
    """策略基类"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.params = {}
    
    def select_stocks(self, stock_pool: List[Dict], data_dict: Dict) -> List[Dict]:
        """
        选股方法（子类实现）
        
        Args:
            stock_pool: 股票池列表 [{"code": "sh.600000", "name": "浦发银行"}, ...]
            data_dict: 股票数据字典 {code: DataFrame}
            
        Returns:
            选中的股票列表 [{"code": "sh.600000", "name": "浦发银行", "weight": 0.05}, ...]
        """
        raise NotImplementedError
    
    def calculate_factor_score(self, stock_data: pd.DataFrame) -> float:
        """计算因子得分"""
        raise NotImplementedError
    
    def update_params(self, params: Dict):
        """更新策略参数"""
        self.params.update(params)


class ValueMultiFactorStrategy(BaseStrategy):
    """
    稳健价值多因子策略
    基于价值投资理念，结合质量因子和动量因子
    """
    
    def __init__(self):
        super().__init__(
            name="稳健价值多因子策略",
            description="基于价值投资理念，结合质量因子和动量因子，筛选优质股票"
        )
        
        self.params = {
            'value_weight': 0.35,
            'quality_weight': 0.30,
            'momentum_weight': 0.20,
            'low_vol_weight': 0.15,
            'pe_threshold': 25,
            'pb_threshold': 2.5,
            'dividend_yield_min': 0.02,
            'roe_min': 0.10,
            'revenue_growth_min': 0.05,
            'debt_ratio_max': 0.60,
            'momentum_period': 126,
            'momentum_min': 0.05,
            'max_stocks': 30,
            'max_industry_weight': 0.15
        }
    
    def select_stocks(self, stock_pool: List[Dict], data_dict: Dict) -> List[Dict]:
        """执行选股"""
        logger.info(f"开始执行 {self.name} 选股...")
        
        scores = []
        
        for stock in stock_pool:
            code = stock['code']
            
            if code not in data_dict or data_dict[code].empty:
                continue
            
            df = data_dict[code]
            
            try:
                score = self._calculate_comprehensive_score(df, stock)
                
                if score > 0:
                    scores.append({
                        'code': code,
                        'name': stock.get('name', ''),
                        'score': score,
                        'value_score': score * self.params['value_weight'],
                        'quality_score': score * self.params['quality_weight'],
                        'momentum_score': score * self.params['momentum_weight'],
                        'low_vol_score': score * self.params['low_vol_weight']
                    })
            except Exception as e:
                logger.warning(f"计算 {code} 得分失败: {e}")
                continue
        
        if not scores:
            logger.warning("没有符合条件的股票")
            return []
        
        scores_df = pd.DataFrame(scores)
        scores_df = scores_df.sort_values('score', ascending=False)
        
        top_stocks = scores_df.head(self.params['max_stocks'])
        
        total_score = top_stocks['score'].sum()
        result = []
        for _, row in top_stocks.iterrows():
            result.append({
                'code': row['code'],
                'name': row['name'],
                'weight': row['score'] / total_score if total_score > 0 else 1.0 / len(top_stocks),
                'score': row['score']
            })
        
        logger.info(f"选股完成，共选出 {len(result)} 只股票")
        return result
    
    def _calculate_comprehensive_score(self, df: pd.DataFrame, stock: Dict) -> float:
        """计算综合得分"""
        min_data_days = min(63, self.params['momentum_period'])
        if len(df) < min_data_days:
            return 0.0
        
        value_score = self._calculate_value_score(df)
        quality_score = self._calculate_quality_score(df)
        momentum_score = self._calculate_momentum_score(df)
        low_vol_score = self._calculate_low_volatility_score(df)
        
        if value_score <= 0 and quality_score <= 0:
            return 0.0
        
        total_score = (
            value_score * self.params['value_weight'] +
            quality_score * self.params['quality_weight'] +
            momentum_score * self.params['momentum_weight'] +
            low_vol_score * self.params['low_vol_weight']
        )
        
        return total_score
    
    def _calculate_value_score(self, df: pd.DataFrame) -> float:
        """计算价值因子得分"""
        try:
            latest = df.iloc[-1]
            
            close = latest.get('close', 0)
            if close <= 0:
                return 0.0
            
            pe = latest.get('pe', None)
            if pe is None:
                pe = latest.get('peTTM', None)
            
            pb = latest.get('pb', None)
            if pb is None:
                pb = latest.get('pbMRQ', None)
            
            score = 0.0
            has_value_data = False
            
            if pe is not None and pe > 0 and pe < self.params['pe_threshold']:
                score += (self.params['pe_threshold'] - pe) / self.params['pe_threshold']
                has_value_data = True
            
            if pb is not None and pb > 0 and pb < self.params['pb_threshold']:
                score += (self.params['pb_threshold'] - pb) / self.params['pb_threshold']
                has_value_data = True
            
            if not has_value_data:
                return 0.5
            
            return max(0, score / 2)
        except:
            return 0.0
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> float:
        """计算质量因子得分"""
        try:
            period = min(len(df), 252)
            if period < 21:
                return 0.0
            
            recent_data = df.tail(period)
            
            close_prices = recent_data['close'].values
            if len(close_prices) < 2:
                return 0.0
            
            returns = np.diff(close_prices) / close_prices[:-1]
            positive_days = np.sum(returns > 0) / len(returns)
            
            volatility = np.std(returns) * np.sqrt(252)
            
            score = positive_days * (1 - min(volatility, 0.5))
            
            return max(0, score)
        except:
            return 0.0
    
    def _calculate_momentum_score(self, df: pd.DataFrame) -> float:
        """计算动量因子得分"""
        try:
            period = min(len(df), self.params['momentum_period'])
            if period < 21:
                return 0.0
            
            recent_data = df.tail(period)
            close_prices = recent_data['close'].values
            
            momentum = (close_prices[-1] - close_prices[0]) / close_prices[0]
            
            return max(0, min(momentum, 0.5))
        except:
            return 0.0
    
    def _calculate_low_volatility_score(self, df: pd.DataFrame) -> float:
        """计算低波动因子得分"""
        try:
            period = min(len(df), 252)
            if period < 21:
                return 0.0
            
            recent_data = df.tail(period)
            close_prices = recent_data['close'].values
            
            if len(close_prices) < 2:
                return 0.0
            
            returns = np.diff(close_prices) / close_prices[:-1]
            volatility = np.std(returns) * np.sqrt(252)
            
            score = max(0, 1 - volatility / 0.5)
            
            return score
        except:
            return 0.0


class MomentumRotationStrategy(BaseStrategy):
    """
    动量驱动的行业轮动策略
    利用A股市场的行业轮动特征，基于动量信号进行行业配置
    """
    
    def __init__(self):
        super().__init__(
            name="动量驱动的行业轮动策略",
            description="利用行业轮动特征，基于动量信号进行行业配置，捕捉机会"
        )
        
        self.params = {
            'short_term_weight': 0.4,
            'medium_term_weight': 0.3,
            'long_term_weight': 0.2,
            'breakout_weight': 0.1,
            'short_term_period': 21,
            'medium_term_period': 63,
            'long_term_period': 126,
            'max_industries': 8,
            'max_industry_weight': 0.25,
            'momentum_threshold': 0.0,
            'volume_factor': 1.5
        }
    
    def select_stocks(self, stock_pool: List[Dict], data_dict: Dict) -> List[Dict]:
        """执行选股"""
        logger.info(f"开始执行 {self.name} 选股...")
        
        momentum_scores = []
        
        for stock in stock_pool:
            code = stock['code']
            
            if code not in data_dict or data_dict[code].empty:
                continue
            
            df = data_dict[code]
            
            try:
                score = self._calculate_momentum_score(df)
                
                if score > self.params['momentum_threshold']:
                    momentum_scores.append({
                        'code': code,
                        'name': stock.get('name', ''),
                        'score': score,
                        'short_momentum': self._calculate_period_momentum(df, self.params['short_term_period']),
                        'medium_momentum': self._calculate_period_momentum(df, self.params['medium_term_period']),
                        'long_momentum': self._calculate_period_momentum(df, self.params['long_term_period'])
                    })
            except Exception as e:
                logger.warning(f"计算 {code} 动量得分失败: {e}")
                continue
        
        if not momentum_scores:
            logger.warning("没有符合条件的股票")
            return []
        
        scores_df = pd.DataFrame(momentum_scores)
        scores_df = scores_df.sort_values('score', ascending=False)
        
        top_stocks = scores_df.head(self.params['max_industries'] * 3)
        
        total_score = top_stocks['score'].sum()
        result = []
        for _, row in top_stocks.iterrows():
            result.append({
                'code': row['code'],
                'name': row['name'],
                'weight': row['score'] / total_score if total_score > 0 else 1.0 / len(top_stocks),
                'score': row['score']
            })
        
        result = self._apply_weight_constraints(result)
        
        logger.info(f"选股完成，共选出 {len(result)} 只股票")
        return result
    
    def _calculate_momentum_score(self, df: pd.DataFrame) -> float:
        """计算综合动量得分"""
        try:
            short_momentum = self._calculate_period_momentum(df, self.params['short_term_period'])
            medium_momentum = self._calculate_period_momentum(df, self.params['medium_term_period'])
            long_momentum = self._calculate_period_momentum(df, self.params['long_term_period'])
            breakout_score = self._calculate_breakout_score(df)
            
            total_score = (
                short_momentum * self.params['short_term_weight'] +
                medium_momentum * self.params['medium_term_weight'] +
                long_momentum * self.params['long_term_weight'] +
                breakout_score * self.params['breakout_weight']
            )
            
            return total_score
        except:
            return 0.0
    
    def _calculate_period_momentum(self, df: pd.DataFrame, period: int) -> float:
        """计算指定周期的动量"""
        try:
            if len(df) < period:
                return 0.0
            
            recent_data = df.tail(period)
            close_prices = recent_data['close'].values
            
            momentum = (close_prices[-1] - close_prices[0]) / close_prices[0]
            
            return momentum
        except:
            return 0.0
    
    def _calculate_breakout_score(self, df: pd.DataFrame) -> float:
        """计算突破得分"""
        try:
            if len(df) < 63:
                return 0.0
            
            recent_data = df.tail(63)
            close_prices = recent_data['close'].values
            
            current_price = close_prices[-1]
            max_price = np.max(close_prices[:-1])
            
            if current_price >= max_price:
                return 1.0
            else:
                return 0.0
        except:
            return 0.0
    
    def _apply_weight_constraints(self, stocks: List[Dict]) -> List[Dict]:
        """应用权重约束"""
        if not stocks:
            return stocks
        
        for stock in stocks:
            stock['weight'] = min(stock['weight'], self.params['max_industry_weight'])
        
        total_weight = sum(s['weight'] for s in stocks)
        if total_weight > 0:
            for stock in stocks:
                stock['weight'] = stock['weight'] / total_weight
        
        return stocks


class LowVolatilityDefensiveStrategy(BaseStrategy):
    """
    低波动防御性策略
    选择低波动、高分红、稳定增长的防御性股票
    """
    
    def __init__(self):
        super().__init__(
            name="低波动防御性策略",
            description="选择低波动、高分红、稳定增长的防御性股票，稳健收益"
        )
        
        self.params = {
            'low_vol_weight': 0.40,
            'dividend_weight': 0.25,
            'stability_weight': 0.20,
            'cashflow_weight': 0.15,
            'volatility_threshold': 0.30,
            'beta_threshold': 0.8,
            'dividend_yield_min': 0.03,
            'dividend_years_min': 3,
            'payout_ratio_max': 0.70,
            'roe_volatility_max': 0.15,
            'revenue_volatility_max': 0.20,
            'cashflow_ratio_min': 0.8,
            'max_stocks': 25,
            'max_stock_weight': 0.05
        }
    
    def select_stocks(self, stock_pool: List[Dict], data_dict: Dict) -> List[Dict]:
        """执行选股"""
        logger.info(f"开始执行 {self.name} 选股...")
        
        scores = []
        
        for stock in stock_pool:
            code = stock['code']
            
            if code not in data_dict or data_dict[code].empty:
                continue
            
            df = data_dict[code]
            
            try:
                score = self._calculate_defensive_score(df)
                
                if score > 0:
                    scores.append({
                        'code': code,
                        'name': stock.get('name', ''),
                        'score': score,
                        'volatility_score': self._calculate_volatility_score(df),
                        'dividend_score': self._calculate_dividend_score(df),
                        'stability_score': self._calculate_stability_score(df),
                        'cashflow_score': self._calculate_cashflow_score(df)
                    })
            except Exception as e:
                logger.warning(f"计算 {code} 得分失败: {e}")
                continue
        
        if not scores:
            logger.warning("没有符合条件的股票")
            return []
        
        scores_df = pd.DataFrame(scores)
        scores_df = scores_df.sort_values('score', ascending=False)
        
        top_stocks = scores_df.head(self.params['max_stocks'])
        
        total_score = top_stocks['score'].sum()
        result = []
        for _, row in top_stocks.iterrows():
            weight = row['score'] / total_score if total_score > 0 else 1.0 / len(top_stocks)
            weight = min(weight, self.params['max_stock_weight'])
            
            result.append({
                'code': row['code'],
                'name': row['name'],
                'weight': weight,
                'score': row['score']
            })
        
        total_weight = sum(s['weight'] for s in result)
        if total_weight > 0 and abs(total_weight - 1.0) > 0.01:
            for stock in result:
                stock['weight'] = stock['weight'] / total_weight
        
        logger.info(f"选股完成，共选出 {len(result)} 只股票")
        return result
    
    def _calculate_defensive_score(self, df: pd.DataFrame) -> float:
        """计算防御性综合得分"""
        try:
            if len(df) < 252:
                return 0.0
            
            volatility_score = self._calculate_volatility_score(df)
            dividend_score = self._calculate_dividend_score(df)
            stability_score = self._calculate_stability_score(df)
            cashflow_score = self._calculate_cashflow_score(df)
            
            if volatility_score <= 0:
                return 0.0
            
            total_score = (
                volatility_score * self.params['low_vol_weight'] +
                dividend_score * self.params['dividend_weight'] +
                stability_score * self.params['stability_weight'] +
                cashflow_score * self.params['cashflow_weight']
            )
            
            return total_score
        except:
            return 0.0
    
    def _calculate_volatility_score(self, df: pd.DataFrame) -> float:
        """计算低波动得分"""
        try:
            if len(df) < 252:
                return 0.0
            
            recent_data = df.tail(252)
            close_prices = recent_data['close'].values
            
            if len(close_prices) < 2:
                return 0.0
            
            returns = np.diff(close_prices) / close_prices[:-1]
            volatility = np.std(returns) * np.sqrt(252)
            
            if volatility > self.params['volatility_threshold']:
                return 0.0
            
            score = (self.params['volatility_threshold'] - volatility) / self.params['volatility_threshold']
            
            return max(0, score)
        except:
            return 0.0
    
    def _calculate_dividend_score(self, df: pd.DataFrame) -> float:
        """计算分红得分"""
        try:
            if len(df) < 252:
                return 0.0
            
            recent_data = df.tail(252)
            
            latest = recent_data.iloc[-1]
            close = latest.get('close', 0)
            
            if close <= 0:
                return 0.0
            
            score = 0.5
            
            return score
        except:
            return 0.0
    
    def _calculate_stability_score(self, df: pd.DataFrame) -> float:
        """计算稳定性得分"""
        try:
            if len(df) < 504:
                return 0.0
            
            recent_data = df.tail(504)
            close_prices = recent_data['close'].values
            
            if len(close_prices) < 2:
                return 0.0
            
            returns = np.diff(close_prices) / close_prices[:-1]
            
            positive_ratio = np.sum(returns > 0) / len(returns)
            
            score = positive_ratio * 0.8
            
            return max(0, score)
        except:
            return 0.0
    
    def _calculate_cashflow_score(self, df: pd.DataFrame) -> float:
        """计算现金流得分"""
        try:
            if len(df) < 252:
                return 0.0
            
            recent_data = df.tail(252)
            
            close_prices = recent_data['close'].values
            volumes = recent_data['volume'].values
            
            if len(close_prices) < 2 or len(volumes) < 2:
                return 0.0
            
            avg_volume = np.mean(volumes)
            recent_volume = volumes[-5:].mean()
            
            if avg_volume > 0:
                volume_ratio = recent_volume / avg_volume
                score = min(1.0, volume_ratio)
            else:
                score = 0.5
            
            return score
        except:
            return 0.0


class StrategyCombiner:
    """策略组合器"""
    
    def __init__(self):
        self.strategies = {
            'value_multifactor': ValueMultiFactorStrategy(),
            'momentum_rotation': MomentumRotationStrategy(),
            'low_volatility_defensive': LowVolatilityDefensiveStrategy()
        }
        
        self.combination_weights = {
            'value_multifactor': 0.4,
            'momentum_rotation': 0.3,
            'low_volatility_defensive': 0.3
        }
    
    def combine_strategies(
        self,
        stock_pool: List[Dict],
        data_dict: Dict,
        selected_strategies: List[str] = None,
        weights: Dict[str, float] = None
    ) -> List[Dict]:
        """
        组合多个策略的选股结果
        
        Args:
            stock_pool: 股票池
            data_dict: 数据字典
            selected_strategies: 选中的策略列表
            weights: 策略权重
            
        Returns:
            组合后的股票列表
        """
        if selected_strategies is None:
            selected_strategies = list(self.strategies.keys())
        
        if weights is None:
            weights = self.combination_weights
        
        strategy_results = {}
        
        for strategy_key in selected_strategies:
            if strategy_key not in self.strategies:
                logger.warning(f"策略 {strategy_key} 不存在")
                continue
            
            strategy = self.strategies[strategy_key]
            result = strategy.select_stocks(stock_pool, data_dict)
            strategy_results[strategy_key] = result
        
        combined_stocks = self._merge_strategy_results(strategy_results, weights)
        
        return combined_stocks
    
    def _merge_strategy_results(
        self,
        strategy_results: Dict[str, List[Dict]],
        weights: Dict[str, float]
    ) -> List[Dict]:
        """合并策略结果"""
        stock_scores = {}
        
        for strategy_key, stocks in strategy_results.items():
            strategy_weight = weights.get(strategy_key, 1.0)
            
            for stock in stocks:
                code = stock['code']
                
                if code not in stock_scores:
                    stock_scores[code] = {
                        'code': code,
                        'name': stock.get('name', ''),
                        'total_score': 0.0,
                        'total_weight': 0.0,
                        'strategies': []
                    }
                
                weighted_score = stock.get('weight', 0) * strategy_weight
                stock_scores[code]['total_score'] += weighted_score
                stock_scores[code]['total_weight'] += strategy_weight
                stock_scores[code]['strategies'].append(strategy_key)
        
        for code in stock_scores:
            if stock_scores[code]['total_weight'] > 0:
                stock_scores[code]['final_weight'] = (
                    stock_scores[code]['total_score'] / stock_scores[code]['total_weight']
                )
            else:
                stock_scores[code]['final_weight'] = 0.0
        
        sorted_stocks = sorted(
            stock_scores.values(),
            key=lambda x: x['final_weight'],
            reverse=True
        )
        
        total_weight = sum(s['final_weight'] for s in sorted_stocks)
        result = []
        
        for stock in sorted_stocks[:50]:
            if total_weight > 0:
                weight = stock['final_weight'] / total_weight
            else:
                weight = 1.0 / len(sorted_stocks[:50])
            
            result.append({
                'code': stock['code'],
                'name': stock['name'],
                'weight': weight,
                'strategies': stock['strategies']
            })
        
        return result
    
    def update_strategy_params(self, strategy_key: str, params: Dict):
        """更新策略参数"""
        if strategy_key in self.strategies:
            self.strategies[strategy_key].update_params(params)
    
    def update_combination_weights(self, weights: Dict[str, float]):
        """更新组合权重"""
        self.combination_weights.update(weights)
    
    def get_strategy_info(self) -> Dict:
        """获取所有策略信息"""
        info = {}
        for key, strategy in self.strategies.items():
            info[key] = {
                'name': strategy.name,
                'description': strategy.description,
                'params': strategy.params
            }
        return info
