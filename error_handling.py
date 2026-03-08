"""
容错与稳定性增强模块
提供重试机制、异常处理、兜底逻辑
"""

import time
import random
from functools import wraps
from typing import Callable, Any, Optional
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    重试装饰器
    
    Args:
        max_attempts: 最大重试次数
        delay: 初始延迟（秒）
        backoff: 延迟倍增系数
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    # 随机延迟（±20%）
                    jitter = current_delay * (0.8 + random.random() * 0.4)
                    if attempt > 0:
                        logger.warning(f"第{attempt}次重试，等待{jitter:.2f}秒")
                        time.sleep(jitter)
                    
                    return func(*args, **kwargs)
                
                except Exception as e:
                    last_exception = e
                    logger.error(f"尝试{attempt + 1}/{max_attempts}失败：{e}")
                    current_delay *= backoff
            
            logger.error(f"所有{max_attempts}次尝试均失败")
            raise last_exception
        
        return wrapper
    return decorator


def fallback(default_value: Any = None):
    """
    兜底装饰器 - 当函数失败时返回默认值
    
    Args:
        default_value: 默认返回值
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"函数{func.__name__}执行失败，使用默认值：{e}")
                return default_value
        
        return wrapper
    return decorator


def safe_execute(func: Callable, default_value: Any = None, log_error: bool = True) -> Callable:
    """
    安全执行装饰器
    
    Args:
        func: 被装饰的函数
        default_value: 失败时的默认返回值
        log_error: 是否记录错误日志
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if log_error:
                logger.error(f"执行{func.__name__}时出错：{e}")
            return default_value
    
    return wrapper


class CircuitBreaker:
    """
    熔断器 - 防止连续失败
    """
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """执行函数，带有熔断保护"""
        if self.state == "OPEN":
            # 检查是否应该尝试恢复
            if self.last_failure_time and \
               time.time() - self.last_failure_time > self.recovery_timeout:
                logger.info("熔断器进入半开状态")
                self.state = "HALF_OPEN"
            else:
                raise Exception("熔断器开启，拒绝执行")
        
        try:
            result = func(*args, **kwargs)
            
            # 成功执行
            if self.state == "HALF_OPEN":
                logger.info("熔断器关闭，恢复正常")
            self.failure_count = 0
            self.state = "CLOSED"
            
            return result
        
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                logger.error(f"熔断器开启，失败次数：{self.failure_count}")
                self.state = "OPEN"
            
            raise e
    
    def reset(self):
        """重置熔断器"""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"


class RateLimiter:
    """
    速率限制器 - 防止请求过快
    """
    
    def __init__(self, max_calls: int, period: float = 1.0):
        """
        Args:
            max_calls: 指定时间段内允许的最大调用次数
            period: 时间段（秒）
        """
        self.max_calls = max_calls
        self.period = period
        self.calls = []
    
    def wait_if_needed(self):
        """如果需要则等待"""
        now = time.time()
        
        # 移除过期的调用记录
        self.calls = [t for t in self.calls if now - t < self.period]
        
        # 如果达到限制，等待
        if len(self.calls) >= self.max_calls:
            wait_time = self.period - (now - self.calls[0])
            if wait_time > 0:
                logger.info(f"速率限制，等待{wait_time:.2f}秒")
                time.sleep(wait_time)
                # 重新清理
                now = time.time()
                self.calls = [t for t in self.calls if now - t < self.period]
        
        # 记录当前调用
        self.calls.append(time.time())
    
    def __call__(self, func: Callable) -> Callable:
        """作为装饰器使用"""
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            self.wait_if_needed()
            return func(*args, **kwargs)
        return wrapper


# 全局熔断器和速率限制器实例
data_source_circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=120)
rate_limiter = RateLimiter(max_calls=10, period=1.0)  # 每秒最多 10 次请求


def get_data_with_protection(func: Callable) -> Callable:
    """
    获取数据的保护包装器
    结合熔断器、速率限制和重试机制
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            # 速率限制
            rate_limiter.wait_if_needed()
            
            # 熔断器保护
            return data_source_circuit_breaker.call(func, *args, **kwargs)
        
        except Exception as e:
            logger.error(f"数据获取失败：{e}")
            return None
    
    return wrapper


def validate_date_range(start_date: str, end_date: str, max_days: int = 3650) -> bool:
    """
    验证日期范围
    
    Args:
        start_date: 开始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        max_days: 最大天数差
        
    Returns:
        是否有效
    """
    try:
        from datetime import datetime
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start > end:
            logger.error("开始日期不能大于结束日期")
            return False
        
        if (end - start).days > max_days:
            logger.error(f"日期范围过大，最大允许{max_days}天")
            return False
        
        return True
    
    except Exception as e:
        logger.error(f"日期验证失败：{e}")
        return False


def ensure_date_format(date_input: Any, format: str = '%Y-%m-%d') -> Optional[str]:
    """
    确保日期格式正确
    
    Args:
        date_input: 日期输入（字符串或 datetime）
        format: 目标格式
        
    Returns:
        格式化后的日期字符串
    """
    try:
        from datetime import datetime
        
        if isinstance(date_input, str):
            # 尝试解析并重新格式化
            for fmt in ['%Y-%m-%d', '%Y%m%d', '%Y/%m/%d']:
                try:
                    dt = datetime.strptime(date_input, fmt)
                    return dt.strftime(format)
                except:
                    continue
        elif hasattr(date_input, 'strftime'):
            # datetime 对象
            return date_input.strftime(format)
        
        logger.error(f"无法解析日期：{date_input}")
        return None
    
    except Exception as e:
        logger.error(f"日期格式化失败：{e}")
        return None
