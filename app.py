"""
A 股股票组合再平衡回测系统 - Streamlit 前端
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pathlib import Path
from data_sources import DataSourceManager, TradingDayChecker
try:
    from unified_data_manager import UnifiedDataManager
    USE_NEW_STORAGE = True
except ImportError:
    from data_manager import DataManager, StockPoolManager
    USE_NEW_STORAGE = False
from backtest_engine import BacktestEngine
from strategy_selector import StrategyCombiner
import os
import json


class DataTimestampManager:
    """数据时间戳管理器"""
    
    def __init__(self, timestamp_file: str = "data/last_update_timestamp.json"):
        self.timestamp_file = Path(timestamp_file)
        self.timestamp_file.parent.mkdir(parents=True, exist_ok=True)
        self._timestamp_data = self._load_timestamp()
    
    def _load_timestamp(self) -> dict:
        """加载时间戳数据"""
        if self.timestamp_file.exists():
            try:
                with open(self.timestamp_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}
    
    def _save_timestamp(self):
        """保存时间戳数据"""
        try:
            with open(self.timestamp_file, 'w', encoding='utf-8') as f:
                json.dump(self._timestamp_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存时间戳失败：{e}")
    
    def get_last_update_date(self) -> str:
        """获取上次数据更新日期"""
        return self._timestamp_data.get('last_update_date', '')
    
    def set_last_update_date(self, date: str):
        """设置上次数据更新日期"""
        self._timestamp_data['last_update_date'] = date
        self._timestamp_data['last_update_time'] = datetime.now().isoformat()
        self._save_timestamp()
    
    def needs_update(self, latest_trading_day: str) -> bool:
        """判断是否需要更新数据"""
        last_update = self.get_last_update_date()
        if not last_update:
            return True
        return last_update < latest_trading_day


def get_latest_trading_day_from_baostock(ds: DataSourceManager) -> str:
    """
    从 Baostock 获取最新的交易日
    
    Args:
        ds: DataSourceManager 实例
        
    Returns:
        最新交易日日期字符串（YYYY-MM-DD）
    """
    from datetime import datetime, timedelta
    
    # 从当前日期开始向前查找最近的交易日
    today = datetime.now()
    for i in range(10):  # 最多向前查找10天
        check_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        if ds.is_trading_day(check_date):
            return check_date
    
    return ''


# 页面配置（移动端优化）
st.set_page_config(
    page_title="A 股组合再平衡回测系统",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="auto"
)

# 自定义 CSS 样式（隐藏菜单）
st.markdown("""
<style>
/* 隐藏右上角 Streamlit 菜单 */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

/* 策略卡片对齐 */
div[data-testid="stVerticalBlock"] > div[data-testid="stElementContainer"] {
    min-height: fit-content;
}

/* 确保info框高度一致 */
div[data-testid="stAlert"] {
    min-height: 80px;
}

/* 确保checkbox对齐 */
div[data-testid="stCheckbox"] {
    margin-top: 10px;
}
</style>
""", unsafe_allow_html=True)


def format_stock_code(code_str: str) -> str:
    """
    格式化股票代码，自动添加 sh/sz 前缀
    
    Args:
        code_str: 股票代码（纯数字或带前缀）
        
    Returns:
        格式化后的股票代码（如 sh.600000）
    """
    # 去除空格和常见符号
    code_str = str(code_str).strip().replace('.', '').replace('-', '')
    
    # 如果已经有前缀，转换为标准格式
    if code_str.lower().startswith('sh') or code_str.lower().startswith('sz'):
        if len(code_str) >= 7:
            return f"{code_str[:2].lower()}.{code_str[2:]}"
        return f"{code_str[:2].lower()}.{code_str[2:].zfill(6)}"
    
    # 纯数字代码，根据代码规则判断市场
    if code_str.isdigit():
        code_str = code_str.zfill(6)
        # 600/601/603/605 开头是沪市，000/001/002/003 开头是深市
        if code_str.startswith(('600', '601', '603', '605')):
            return f"sh.{code_str}"
        elif code_str.startswith(('000', '001', '002', '003')):
            return f"sz.{code_str}"
        else:
            # 默认返回 sh 前缀
            return f"sh.{code_str}"
    
    return code_str


def init_session_state():
    """初始化会话状态"""
    if 'initialized' not in st.session_state:
        st.session_state.initialized = True
        
        # 数据源配置
        st.session_state.data_source_type = "baostock"
        st.session_state.tushare_token = ""
        
        # 回测配置
        st.session_state.start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        st.session_state.end_date = datetime.now().strftime('%Y-%m-%d')
        st.session_state.initial_capital = 1000000
        st.session_state.rebalance_frequency = "monthly"
        st.session_state.rebalance_frequency_cn = "每月"
        st.session_state.commission_rate = 0.00025  # 万分之 2.5（市场平均水平）
        st.session_state.slippage = 0.0005  # 万分之 5（更贴近实际）
        
        # 股票池
        st.session_state.stocks = [
            {"code": "sh.600000", "name": "浦发银行", "weight": 0.2},
            {"code": "sh.600036", "name": "招商银行", "weight": 0.2},
            {"code": "sh.601318", "name": "中国平安", "weight": 0.2},
            {"code": "sh.600519", "name": "贵州茅台", "weight": 0.2},
            {"code": "sz.000001", "name": "平安银行", "weight": 0.2},
        ]
        
        # 回测结果
        st.session_state.backtest_results = None
        if USE_NEW_STORAGE:
            st.session_state.data_manager = UnifiedDataManager(use_new_storage=True)
        else:
            st.session_state.data_manager = DataManager()
            st.session_state.stock_pool_manager = StockPoolManager()
        
        # 编辑状态
        st.session_state.edit_index = 0
        st.session_state.show_edit_dialog = False
        
        # 策略选股相关
        st.session_state.strategy_combiner = StrategyCombiner()
        st.session_state.selected_strategies = ['value_multifactor', 'momentum_rotation', 'low_volatility_defensive']
        st.session_state.strategy_weights = {
            'value_multifactor': 0.4,
            'momentum_rotation': 0.3,
            'low_volatility_defensive': 0.3
        }
        st.session_state.strategy_params = {}
        st.session_state.strategy_results = None
        
        # 数据时间戳管理器
        st.session_state.timestamp_manager = DataTimestampManager()
        st.session_state.saved_strategies = []
        st.session_state.stop_strategy_selection = False
        
        # 股票数据缓存
        st.session_state.stock_data_cache = {}
        st.session_state.cache_timestamp = None


def render_sidebar():
    """渲染侧边栏（移动端优化）"""
    with st.sidebar:
        st.markdown("## ⚙️ 配置")
        
        # 页面选择
        st.markdown("### 📑 功能选择")
        page = st.radio(
            "功能选择",
            ["策略选股回测", "手动选股回测"],
            index=0,
            label_visibility="collapsed"
        )
        st.session_state.current_page = page
        
        st.divider()
        
        # 移动端提示
        st.info("💡 提示：在手机浏览器上，侧边栏会自动折叠，点击右上角菜单打开")
        
        # 数据源配置
        st.markdown("### 📡 数据源设置")
        data_source_type = "baostock"
        st.session_state.data_source_type = data_source_type
        st.selectbox(
            "选择数据源",
            ["baostock"],
            index=0,
            help="Baostock: 免费官方数据",
            disabled=True
        )
        
        # 验证 Token 按钮
        if st.button("验证数据源连接", width='stretch'):
            try:
                # 在函数内部导入，避免 Streamlit 作用域问题
                from data_sources import DataSourceManager
                import threading
                
                # 使用线程和超时机制
                result = {'success': False, 'message': '', 'error': None}
                
                def verify_connection():
                    try:
                        ds = DataSourceManager()
                        
                        if data_source_type == "baostock":
                            if ds.bs_login_status:
                                result['success'] = True
                                result['message'] = "✓ Baostock 连接成功"
                            else:
                                result['success'] = False
                                result['message'] = "✗ Baostock 连接失败"
                        
                        ds.logout()
                    except Exception as e:
                        result['success'] = False
                        result['message'] = "验证失败"
                        result['error'] = str(e)
                
                # 启动验证线程
                with st.spinner("正在验证..."):
                    thread = threading.Thread(target=verify_connection)
                    thread.start()
                    thread.join(timeout=10)  # 10 秒超时
                    
                    if thread.is_alive():
                        # 超时
                        st.error("✗ 验证超时（10 秒）")
                        st.warning("💡 提示：请检查网络连接或更换数据源")
                    elif result['success']:
                        st.success(result['message'])
                    else:
                        error_msg = result['message']
                        if result['error']:
                            error_msg += f": {result['error'][:100]}"
                        st.error(error_msg)
                        
            except Exception as e:
                st.error(f"验证失败：{e}")
        
        st.divider()
        
        # 股票池管理
        st.markdown("### 📊 股票池管理")
        
        # 显示当前股票（使用可编辑的 DataFrame）
        st.markdown("**当前股票列表:**")
        if st.session_state.stocks:
            # 创建股票列表的 DataFrame
            stock_df = pd.DataFrame(st.session_state.stocks)
            stock_df['代码'] = stock_df['code']
            stock_df['名称'] = stock_df['name']
            stock_df['权重 (%)'] = (stock_df['weight'] * 100).round(1)
            stock_df_display = stock_df[['代码', '名称', '权重 (%)']].copy()
            
            # 显示股票列表（支持原位置编辑）
            for i, stock in enumerate(st.session_state.stocks):
                with st.container():
                    # 检查是否是当前编辑的股票
                    is_editing = (st.session_state.get('edit_index', -1) == i 
                                  and st.session_state.get('show_edit_dialog', False))
                    
                    if is_editing:
                        # 编辑模式：在原位置显示编辑框
                        st.markdown(f"**编辑股票 {i+1}:**")
                        
                        # 使用 session_state 存储编辑中的值
                        if f'editing_code_{i}' not in st.session_state:
                            st.session_state[f'editing_code_{i}'] = stock['code'].split('.')[-1]
                        if f'editing_name_{i}' not in st.session_state:
                            st.session_state[f'editing_name_{i}'] = stock['name']
                        if f'editing_weight_{i}' not in st.session_state:
                            st.session_state[f'editing_weight_{i}'] = stock['weight']
                        
                        # 两列布局
                        edit_col1, edit_col2 = st.columns([2, 2])
                        
                        with edit_col1:
                            edit_code = st.text_input(
                                "股票代码", 
                                value=st.session_state[f'editing_code_{i}'],
                                key=f"edit_code_input_{i}",
                                help="输入 6 位数字，系统自动识别市场并获取股票名称"
                            )
                        with edit_col2:
                            edit_name = st.text_input(
                                "股票名称", 
                                value=st.session_state[f'editing_name_{i}'],
                                key=f"edit_name_input_{i}"
                            )
                        
                        # 监听股票代码变化，自动获取名称
                        # 使用 session_state 追踪，避免重复请求
                        last_checked_key = f'last_checked_code_{i}'
                        if last_checked_key not in st.session_state:
                            st.session_state[last_checked_key] = ""
                        
                        if (edit_code and len(edit_code.strip()) == 6 and 
                            edit_code.strip() != st.session_state.get(last_checked_key, "")):
                            
                            # 标记已检查
                            st.session_state[last_checked_key] = edit_code.strip()
                            
                            # 只有当名称为空时才获取
                            should_fetch = not edit_name or edit_name.strip() == ""
                            
                            if should_fetch:
                                with st.spinner("正在获取股票信息..."):
                                    try:
                                        from data_sources import DataSourceManager
                                        
                                        ds = DataSourceManager()
                                        
                                        stock_info = ds.get_stock_info(edit_code)
                                        
                                        if stock_info and stock_info.get('name'):
                                            # 使用临时变量
                                            st.session_state[f'auto_filled_name_{i}'] = stock_info['name']
                                            st.session_state[f'editing_name_{i}'] = stock_info['name']
                                            st.success(f"✓ 已识别：{stock_info['name']}")
                                            st.rerun()
                                        else:
                                            st.warning("⚠ 未找到股票信息")
                                        
                                        ds.logout()
                                    except Exception as e:
                                        st.warning(f"⚠ 获取失败：{str(e)[:50]}")
                                        st.session_state[last_checked_key] = ""
                        
                        edit_weight = st.number_input(
                            "权重 (0-1)", 
                            min_value=0.0, 
                            max_value=1.0, 
                            value=st.session_state[f'editing_weight_{i}'], 
                            step=0.01,
                            key=f"edit_weight_input_{i}"
                        )
                        
                        # 保存和取消按钮
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("✓ 保存", width='stretch', key=f"save_edit_{i}"):
                                formatted_code = format_stock_code(edit_code)
                                st.session_state.stocks[i] = {
                                    "code": formatted_code,
                                    "name": edit_name,
                                    "weight": edit_weight
                                }
                                # 清理编辑状态
                                st.session_state.show_edit_dialog = False
                                st.session_state.edit_index = -1
                                # 清理临时编辑值
                                for key in list(st.session_state.keys()):
                                    if key.startswith(f'editing_'):
                                        del st.session_state[key]
                                st.success(f"已更新为 {edit_name} ({formatted_code})")
                                st.rerun()
                        with col2:
                            if st.button("✗ 取消", width='stretch', key=f"cancel_edit_{i}"):
                                st.session_state.show_edit_dialog = False
                                st.session_state.edit_index = -1
                                # 清理临时编辑值
                                for key in list(st.session_state.keys()):
                                    if key.startswith(f'editing_'):
                                        del st.session_state[key]
                                st.rerun()
                        
                        st.divider()
                    else:
                        # 显示模式：显示股票信息和编辑/删除按钮
                        cols = st.columns([3, 2, 2, 1, 1])
                        with cols[0]:
                            st.text(f"{stock['code']}")
                        with cols[1]:
                            st.text(stock['name'])
                        with cols[2]:
                            st.text(f"{stock['weight']*100:.1f}%")
                        with cols[3]:
                            if st.button("✏️", key=f"edit_{i}", help="编辑股票"):
                                st.session_state.edit_index = i
                                st.session_state.show_edit_dialog = True
                                st.rerun()
                        with cols[4]:
                            if st.button("🗑️", key=f"remove_{i}", help="删除股票"):
                                st.session_state.stocks.pop(i)
                                st.success("股票已删除")
                                st.rerun()
        
        # 添加新股票
        st.markdown("**添加股票:**")
        
        # 先检查是否有自动填充的名称（在 widget 创建之前）
        auto_filled_name = st.session_state.get('auto_filled_name', '')
        if auto_filled_name:
            # 清空自动填充标记
            st.session_state.auto_filled_name = ''
            # 设置初始名称值
            if 'new_stock_name_initial' not in st.session_state:
                st.session_state.new_stock_name_initial = auto_filled_name
        
        # 使用两列布局
        col1, col2 = st.columns([2, 2])
        
        with col1:
            new_code = st.text_input(
                "股票代码", 
                key="new_stock_code", 
                help="输入 6 位股票代码，不支持北交所",
                placeholder="例如：600000"
            )
        
        with col2:
            # 如果有初始值，使用它
            initial_name = st.session_state.get('new_stock_name_initial', '')
            new_name = st.text_input(
                "股票名称", 
                key="new_stock_name",
                placeholder="输入代码后自动获取或手动输入",
                value=initial_name if initial_name else ""
            )
            # 清空初始值（下次运行时不再使用）
            if initial_name:
                st.session_state.new_stock_name_initial = ''
        
        # 监听股票代码变化，自动获取名称
        # 使用 session_state 追踪上一次的代码值，避免重复请求
        if 'last_checked_code' not in st.session_state:
            st.session_state.last_checked_code = ""
        
        if (new_code and len(new_code.strip()) == 6 and 
            new_code.strip() != st.session_state.last_checked_code):
            
            # 标记已检查，避免重复请求
            st.session_state.last_checked_code = new_code.strip()
            
            # 只有当名称为空或名称与代码不匹配时才获取
            should_fetch = not new_name or new_name.strip() == ""
            
            if should_fetch:
                try:
                    from data_sources import DataSourceManager
                    
                    # 创建临时数据源实例
                    ds = DataSourceManager()
                    
                    # 获取股票信息
                    stock_info = ds.get_stock_info(new_code)
                    
                    if stock_info and stock_info.get('name'):
                        # 使用临时变量存储，在下次 rerun 时使用
                        st.session_state.auto_filled_name = stock_info['name']
                        st.success(f"✓ 已识别：{stock_info['name']}")
                        # 强制 rerun 以更新 UI
                        st.rerun()
                    else:
                        st.warning("⚠ 未找到股票信息，请手动输入名称")
                    
                    ds.logout()
                except Exception as e:
                    st.warning(f"⚠ 获取失败：{str(e)[:50]}，请手动输入名称")
                    # 重置 last_checked_code，允许重试
                    st.session_state.last_checked_code = ""
        
        new_weight = st.number_input("权重 (0-1)", min_value=0.0, max_value=1.0, value=0.1, step=0.05, key="new_stock_weight")
        
        if st.button("添加股票", width='stretch'):
            if new_code and new_name and new_weight > 0:
                # 格式化股票代码
                formatted_code = format_stock_code(new_code)
                st.session_state.stocks.append({
                    "code": formatted_code,
                    "name": new_name,
                    "weight": new_weight
                })
                st.success(f"已添加 {new_name} ({formatted_code})")
                # 清空输入框
                st.session_state.new_stock_code = ""
                st.session_state.new_stock_name = ""
                st.rerun()
        
        # 权重归一化按钮
        if st.button("归一化权重", width='stretch', help="将所有权重调整为总和为 1"):
            total_weight = sum(s['weight'] for s in st.session_state.stocks)
            if total_weight > 0:
                for stock in st.session_state.stocks:
                    stock['weight'] = stock['weight'] / total_weight
                st.success("权重已归一化")
                st.rerun()
        
        st.divider()
        
        # 缓存信息展示
        st.markdown("### 💾 缓存信息")
        cache_info = st.session_state.data_manager.get_cache_info()
        st.text(f"目前数据：{cache_info['total_rows']}")
        st.text(f"股票数量：{cache_info['total_stocks']}")
      
        if cache_info.get('cache_file_size', 0) > 0:
            st.text(f"缓存大小：{cache_info['cache_file_size']/1024:.1f} KB")
        
        # 股票数据缓存管理
        st.markdown("### 📦 数据缓存管理")
        
        # 显示缓存状态
        if st.session_state.stock_data_cache:
            cache_size = sum(len(df) for df in st.session_state.stock_data_cache.values())
            cache_keys = list(st.session_state.stock_data_cache.keys())
            st.info(f"📊 缓存状态：已缓存 {len(cache_keys)} 个日期范围，共 {cache_size} 条数据")
            if st.session_state.cache_timestamp:
                cache_time = datetime.fromisoformat(st.session_state.cache_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                st.text(f"缓存时间：{cache_time}")
            
            # 清除缓存按钮
            if st.button("🗑️ 清除选股数据缓存", width='stretch'):
                st.session_state.stock_data_cache = {}
                st.session_state.cache_timestamp = None
                st.success("✅ 缓存已清除")
                st.rerun()
        else:
            st.info("📊 缓存状态：无缓存数据")
            st.text("首次选股时会自动缓存数据")
      
        
        # 刷新缓存按钮
        col1, col2 = st.columns([3, 1])
        with col1:
            if st.button("🔄 智能刷新全部 A 股", width='stretch', help="智能更新全部 A 股数据，只获取缺失的股票和最新数据"):
                # 智能刷新全部 A 股
                with st.spinner("正在智能刷新全部 A 股数据，这可能需要几分钟..."):
                    # 创建数据源
                    from data_sources import DataSourceManager
                    ds = DataSourceManager()
                    
                    try:
                        # 获取 Baostock 最新交易日
                        latest_trading_day = get_latest_trading_day_from_baostock(ds)
                        if not latest_trading_day:
                            st.error("无法获取最新交易日信息")
                            ds.logout()
                            return
                        
                        # 检查是否需要更新
                        timestamp_manager = st.session_state.timestamp_manager
                        if not timestamp_manager.needs_update(latest_trading_day):
                            st.info(f"✓ 本地数据已是最新（{latest_trading_day}），无需更新")
                            ds.logout()
                            return
                        
                        # 执行智能刷新
                        result = st.session_state.data_manager.refresh_all_stocks(ds)
                        
                        if result['success']:
                            # 更新本地时间戳
                            timestamp_manager.set_last_update_date(latest_trading_day)
                            st.success(f"✓ 刷新完成！总计：{result['total_stocks']}只 | 缓存：{result['cached_stocks']} | 新增：{result['new_stocks']} | 更新：{result['updated_stocks']}")
                            st.info(f"缓存完整率：{result['cached_stocks']/result['total_stocks']*100:.1f}% - 避免了重复数据请求")
                            st.info(f"📅 数据已更新至：{latest_trading_day}")
                        else:
                            st.error(f"✗ 刷新失败：{result.get('error', '未知错误')}")
                        
                        ds.logout()
                    except Exception as e:
                        st.error(f"刷新出错：{e}")
                        import traceback
                        st.code(traceback.format_exc())
                    finally:
                        ds.logout()


def render_main_content():
    """渲染主内容区（移动端优化）"""
    # 标题
    st.title("📈 A 股组合再平衡回测系统")
    
    # 移动端友好提示
    st.info("💡 建议使用横屏模式获得更好的体验")
    
    # 回测配置
    st.markdown("### 🔧 回测参数")
    col1, col2, col3 = st.columns(3, gap="small")
    
    with col1:
        start_date = st.date_input(
            "开始日期",
            value=pd.to_datetime(st.session_state.start_date),
            help="回测开始日期"
        )
        st.session_state.start_date = start_date.strftime('%Y-%m-%d')
    
    with col2:
        end_date = st.date_input(
            "结束日期",
            value=pd.to_datetime(st.session_state.end_date),
            help="回测结束日期"
        )
        st.session_state.end_date = end_date.strftime('%Y-%m-%d')
    
    with col3:
        initial_capital = st.number_input(
            "初始资金",
            min_value=10000,
            max_value=100000000,
            value=st.session_state.initial_capital,
            step=10000,
            help="初始投入资金"
        )
        st.session_state.initial_capital = initial_capital
    
    col1, col2, col3 = st.columns(3, gap="small")
    with col1:
        rebalance_frequency = st.selectbox(
            "再平衡频率",
            ["每月", "每季度", "每半年", "每年"],
            index=["每月", "每季度", "每半年", "每年"].index(st.session_state.rebalance_frequency_cn),
            help="每月：每月 25 日后调仓\n每季度：每季度最后一个月 25 日后调仓\n每半年：6 月和 12 月 25 日后调仓\n每年：12 月 25 日后调仓"
        )
        # 将中文频率转换为英文代码
        frequency_map = {
            "每月": "monthly",
            "每季度": "quarterly",
            "每半年": "semi-annually",
            "每年": "annually"
        }
        st.session_state.rebalance_frequency = frequency_map[rebalance_frequency]
        st.session_state.rebalance_frequency_cn = rebalance_frequency
    
    with col2:
        commission_rate = st.number_input(
            "佣金费率",
            min_value=0.0,
            max_value=0.01,
            value=st.session_state.commission_rate,
            step=0.0001,
            help="交易佣金费率"
        )
        st.session_state.commission_rate = commission_rate
    
    with col3:
        slippage = st.number_input(
            "滑点",
            min_value=0.0,
            max_value=0.01,
            value=st.session_state.slippage,
            step=0.0001,
            help="价格滑点"
        )
        st.session_state.slippage = slippage
    
    # 开始回测按钮
    st.markdown("---")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"**股票数量:** {len(st.session_state.stocks)} 只")
        total_weight = sum(s['weight'] for s in st.session_state.stocks)
        st.markdown(f"**总权重:** {total_weight*100:.1f}%")
        if abs(total_weight - 1.0) > 0.01:
            st.warning(f"⚠️ 权重总和不为 100%，建议点击「归一化权重」")
    
    with col2:
        if st.button("🚀 开始回测", width='stretch', type="primary"):
            run_backtest()
    
    # 显示回测结果
    if st.session_state.backtest_results is not None:
        display_results()


def render_strategy_page():
    """渲染策略选股页面"""
    st.title("🎯 策略选股回测")
    
    st.markdown("""
    ### 📊 策略说明
    本系统提供三种稳健的A股多股策略模型，您可以根据风险偏好组合使用：
    """)
    
    # 策略选择区域
    st.markdown("### 1️⃣ 选择策略")
    
    strategy_info = st.session_state.strategy_combiner.get_strategy_info()
    
    col1, col2, col3 = st.columns(3)
    
    # 策略选择和权重调整
    # 初始化会话状态
    if 'strategy_weights' not in st.session_state:
        st.session_state.strategy_weights = {
            'value_multifactor': 0.4,
            'momentum_rotation': 0.3,
            'low_volatility_defensive': 0.3
        }
    
    # 处理策略选择
    with col1:
        st.markdown("#### 💎 稳健价值多因子策略")
        st.info(strategy_info['value_multifactor']['description'])
        use_value = st.checkbox("启用此策略", value=True, key="use_value")

    with col2:
        st.markdown("#### 📈 动量行业轮动策略")
        st.info(strategy_info['momentum_rotation']['description'])
        use_momentum = st.checkbox("启用此策略", value=True, key="use_momentum")

    with col3:
        st.markdown("#### 🛡️ 低波动防御性策略")
        st.info(strategy_info['low_volatility_defensive']['description'])
        use_lowvol = st.checkbox("启用此策略", value=True, key="use_lowvol")
    
    # 确定选中的策略
    selected_strategies = []
    if use_value:
        selected_strategies.append('value_multifactor')
    if use_momentum:
        selected_strategies.append('momentum_rotation')
    if use_lowvol:
        selected_strategies.append('low_volatility_defensive')
    
    st.session_state.selected_strategies = selected_strategies
    
    # 所有策略的列表（固定顺序）
    all_strategies = ['value_multifactor', 'momentum_rotation', 'low_volatility_defensive']
    strategy_names = {
        'value_multifactor': '稳健价值',
        'momentum_rotation': '动量轮动',
        'low_volatility_defensive': '低波动防御'
    }
    
    # 检测策略变化
    prev_selected = st.session_state.get('prev_selected_strategies', selected_strategies.copy())
    newly_enabled = [s for s in selected_strategies if s not in prev_selected]
    newly_disabled = [s for s in prev_selected if s not in selected_strategies]
    
    # 处理权重调整 - 始终显示所有滑块
    if len(selected_strategies) >= 1:
        # 获取当前权重值
        current_weights = {s: st.session_state.strategy_weights[s] for s in all_strategies}
        
        # 处理策略变化
        if newly_disabled or newly_enabled:
            if len(selected_strategies) == 1:
                # 问题1：只剩一个策略，强制权重为1并刷新
                st.session_state.strategy_weights[selected_strategies[0]] = 1.0
                st.session_state.prev_selected_strategies = selected_strategies.copy()
                st.session_state.slider_version = st.session_state.get('slider_version', 0) + 1
                st.rerun()
            elif len(selected_strategies) == 2 and newly_disabled:
                # 关闭一个策略（从3个变成2个），归一化剩余两个策略的权重
                enabled_weights = {s: current_weights[s] for s in selected_strategies}
                total = sum(enabled_weights.values())
                if total > 0:
                    for s in selected_strategies:
                        st.session_state.strategy_weights[s] = enabled_weights[s] / total
                st.session_state.prev_selected_strategies = selected_strategies.copy()
                st.session_state.slider_version = st.session_state.get('slider_version', 0) + 1
                st.rerun()
            elif len(selected_strategies) >= 2 and newly_enabled:
                # 问题2：有新加入的策略，以新策略为主进行归一
                # 给新策略分配平均权重
                new_strategy_weight = 1.0 / len(selected_strategies)
                remaining_weight = 1.0 - new_strategy_weight * len(newly_enabled)
                
                # 计算旧策略的总权重
                old_strategies = [s for s in selected_strategies if s not in newly_enabled]
                if old_strategies:
                    old_total = sum(current_weights[s] for s in old_strategies)
                    if old_total > 0:
                        # 按比例分配剩余权重给旧策略
                        for s in old_strategies:
                            current_weights[s] = (current_weights[s] / old_total) * remaining_weight
                    else:
                        # 平均分配
                        for s in old_strategies:
                            current_weights[s] = remaining_weight / len(old_strategies)
                
                # 设置新策略的权重
                for s in newly_enabled:
                    current_weights[s] = new_strategy_weight
                
                # 更新权重并刷新
                st.session_state.strategy_weights = current_weights.copy()
                st.session_state.prev_selected_strategies = selected_strategies.copy()
                st.session_state.slider_version = st.session_state.get('slider_version', 0) + 1
                st.rerun()
        
        # 更新prev_selected_strategies
        st.session_state.prev_selected_strategies = selected_strategies.copy()
        
        # 创建三个滑块（始终显示）
        weight_cols = st.columns(3)
        new_values = {}
        
        for idx, strategy_key in enumerate(all_strategies):
            with weight_cols[idx]:
                is_enabled = strategy_key in selected_strategies
                
                # 使用当前权重作为滑块值
                if is_enabled:
                    # 启用的策略：正常使用滑块
                    val = st.slider(
                        f"{strategy_names[strategy_key]}", 
                        0.0, 1.0, 
                        current_weights[strategy_key], 
                        0.05, 
                        key=f"slider_{strategy_key}_{st.session_state.get('slider_version', 0)}"
                    )
                    new_values[strategy_key] = val
                else:
                    # 禁用的策略：只显示禁用状态的滑块
                    st.slider(
                        f"{strategy_names[strategy_key]}", 
                        0.0, 1.0, 
                        current_weights[strategy_key], 
                        0.05, 
                        key=f"slider_disabled_{strategy_key}",
                        disabled=True
                    )
                    new_values[strategy_key] = current_weights[strategy_key]
        
        # 确保启用的策略权重正确设置
        if len(selected_strategies) == 1:
            # 只有一个启用策略时，权重固定为1
            st.session_state.strategy_weights[selected_strategies[0]] = 1.0
        elif len(selected_strategies) >= 2:
            # 检查是否有变化（只在启用的策略中检查）
            changed = False
            changed_key = None
            for s in selected_strategies:
                if abs(new_values[s] - current_weights[s]) > 0.001:
                    changed = True
                    changed_key = s
                    break
            
            if changed and changed_key:
                # 计算变化量
                old_val = current_weights[changed_key]
                new_val = new_values[changed_key]
                delta = new_val - old_val
                
                # 获取其他启用的策略
                others = [s for s in selected_strategies if s != changed_key]
                
                if len(others) > 0:
                    # 计算其他启用策略的总权重
                    others_total = sum(current_weights[s] for s in others)
                    
                    if others_total > 0:
                        # 按比例调整其他启用策略
                        for s in others:
                            ratio = current_weights[s] / others_total
                            current_weights[s] = max(0, current_weights[s] - delta * ratio)
                    else:
                        # 如果其他启用策略权重为0，平均分配
                        for s in others:
                            current_weights[s] = max(0, -delta / len(others))
                
                # 更新被改变的策略
                current_weights[changed_key] = new_val
                
                # 归一化（只对启用的策略）
                enabled_total = sum(current_weights[s] for s in selected_strategies)
                if enabled_total > 0:
                    for s in selected_strategies:
                        st.session_state.strategy_weights[s] = current_weights[s] / enabled_total
                
                # 增加版本号强制刷新
                st.session_state.slider_version = st.session_state.get('slider_version', 0) + 1
                st.rerun()
            else:
                # 没有变化，只更新启用策略的权重
                enabled_total = sum(new_values[s] for s in selected_strategies)
                if enabled_total > 0:
                    for s in selected_strategies:
                        st.session_state.strategy_weights[s] = new_values[s] / enabled_total
        
        # 显示归一化后的权重
        st.markdown("**归一化后权重：**")
        display_cols = st.columns(3)
        for idx, strategy_key in enumerate(all_strategies):
            with display_cols[idx]:
                is_enabled = strategy_key in selected_strategies
                weight = st.session_state.strategy_weights[strategy_key]
                color = "auto" if is_enabled else "#888"
                status = "" if is_enabled else " (未启用)"
                st.markdown(f"<span style='color: {color};'>**{strategy_names[strategy_key]}**{status}<br>{weight*100:.1f}%</span>", unsafe_allow_html=True)
        
    else:
        st.warning("请至少选择一个策略")

    # 策略参数调整（可折叠）
    with st.expander("⚙️ 高级参数设置"):
        st.markdown("#### 稳健价值多因子策略参数")
        col1, col2 = st.columns(2)
        with col1:
            pe_threshold = st.number_input("P/E阈值", 5, 50, 25, key="param_pe")
            pb_threshold = st.number_input("P/B阈值", 0.5, 5.0, 2.5, 0.1, key="param_pb")
        with col2:
            max_stocks_value = st.number_input("最大股票数", 10, 50, 30, key="param_max_stocks_value")
            momentum_min = st.number_input("最小动量", 0.0, 0.5, 0.05, 0.01, key="param_momentum_min")
        
        st.session_state.strategy_params['value_multifactor'] = {
            'pe_threshold': pe_threshold,
            'pb_threshold': pb_threshold,
            'max_stocks': max_stocks_value,
            'momentum_min': momentum_min
        }
        
        st.markdown("#### 动量行业轮动策略参数")
        col1, col2 = st.columns(2)
        with col1:
            short_period = st.number_input("短期动量周期（天）", 10, 30, 21, key="param_short")
            medium_period = st.number_input("中期动量周期（天）", 40, 90, 63, key="param_medium")
        with col2:
            long_period = st.number_input("长期动量周期（天）", 90, 180, 126, key="param_long")
            max_industries = st.number_input("最大行业数", 3, 15, 8, key="param_max_industries")
        
        st.session_state.strategy_params['momentum_rotation'] = {
            'short_term_period': short_period,
            'medium_term_period': medium_period,
            'long_term_period': long_period,
            'max_industries': max_industries
        }
        
        st.markdown("#### 低波动防御性策略参数")
        col1, col2 = st.columns(2)
        with col1:
            vol_threshold = st.number_input("波动率阈值", 0.1, 0.5, 0.3, 0.01, key="param_vol")
            max_stocks_lowvol = st.number_input("最大股票数", 10, 40, 25, key="param_max_stocks_lowvol")
        with col2:
            max_stock_weight = st.number_input("单股最大权重", 0.01, 0.1, 0.05, 0.01, key="param_max_weight")
        
        st.session_state.strategy_params['low_volatility_defensive'] = {
            'volatility_threshold': vol_threshold,
            'max_stocks': max_stocks_lowvol,
            'max_stock_weight': max_stock_weight
        }
    
    # 执行策略选股按钮和停止按钮
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("🎯 执行策略选股", width='stretch', type="primary"):
            st.session_state.stop_strategy_selection = False
            # 执行智能选股流程：先检查时间戳，再决定是否更新数据
            run_smart_strategy_selection()
    with col2:
        if st.button("🛑 停止选股", width='stretch', type="secondary"):
            st.session_state.stop_strategy_selection = True
    
    # 回测配置
    st.markdown("---")
    st.markdown("### 2️⃣ 回测配置")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        start_date = st.date_input(
            "开始日期",
            value=pd.to_datetime(st.session_state.start_date),
            help="回测开始日期"
        )
        st.session_state.start_date = start_date.strftime('%Y-%m-%d')
    
    with col2:
        end_date = st.date_input(
            "结束日期",
            value=pd.to_datetime(st.session_state.end_date),
            help="回测结束日期"
        )
        st.session_state.end_date = end_date.strftime('%Y-%m-%d')
    
    with col3:
        initial_capital = st.number_input(
            "初始资金",
            min_value=10000,
            max_value=100000000,
            value=st.session_state.initial_capital,
            step=10000,
            help="初始投入资金"
        )
        st.session_state.initial_capital = initial_capital
    
    col1, col2 = st.columns(2)
    with col1:
        rebalance_frequency = st.selectbox(
            "再平衡频率",
            ["每月", "每季度", "每半年", "每年"],
            index=["每月", "每季度", "每半年", "每年"].index(st.session_state.rebalance_frequency_cn),
            help="每月：每月25日后调仓"
        )
        frequency_map = {
            "每月": "monthly",
            "每季度": "quarterly",
            "每半年": "semi-annually",
            "每年": "annually"
        }
        st.session_state.rebalance_frequency = frequency_map[rebalance_frequency]
        st.session_state.rebalance_frequency_cn = rebalance_frequency
    
    with col2:
        commission_rate = st.number_input(
            "佣金费率",
            min_value=0.0,
            max_value=0.01,
            value=st.session_state.commission_rate,
            step=0.0001,
            help="交易佣金费率"
        )
        st.session_state.commission_rate = commission_rate
    
    # 执行回测按钮
    if st.button("🚀 执行回测", width='stretch', type="primary"):
        run_strategy_backtest()
    
    st.markdown("---")
    
    # 显示选股结果
    if st.session_state.strategy_results is not None:
        display_strategy_results()
    
    # 策略保存和加载
    st.markdown("---")
    st.markdown("### 💾 策略组合管理")
    
    col1, col2 = st.columns(2)
    
    with col1:
        strategy_name = st.text_input("策略组合名称", placeholder="输入名称保存当前配置")
        if st.button("💾 保存策略组合", width='stretch'):
            if strategy_name:
                save_strategy_combination(strategy_name)
            else:
                st.warning("请输入策略组合名称")
    
    with col2:
        saved_strategy_names = [s['name'] for s in st.session_state.saved_strategies]
        if saved_strategy_names:
            selected_saved = st.selectbox("已保存的策略组合", saved_strategy_names)
            if st.button("📂 加载策略组合", width='stretch'):
                load_strategy_combination(selected_saved)
        else:
            st.info("暂无保存的策略组合")


def run_smart_strategy_selection():
    """
    执行智能策略选股流程
    
    流程：
    1. 获取本地存储的时间戳
    2. 获取 Baostock 最新交易日
    3. 比对时间戳：
       - 若一致，直接执行选股
       - 若不一致，先执行数据更新，再执行选股
    """
    if not st.session_state.selected_strategies:
        st.error("请至少选择一个策略")
        return
    
    # 创建数据源
    ds = DataSourceManager()
    
    try:
        # 获取 Baostock 最新交易日
        latest_trading_day = get_latest_trading_day_from_baostock(ds)
        if not latest_trading_day:
            st.error("无法获取最新交易日信息")
            ds.logout()
            return
        
        # 获取时间戳管理器
        timestamp_manager = st.session_state.timestamp_manager
        last_update_date = timestamp_manager.get_last_update_date()
        
        # 显示当前数据状态
        if last_update_date:
            st.info(f"📅 本地数据日期：{last_update_date} | Baostock最新交易日：{latest_trading_day}")
        else:
            st.info(f"📅 本地无数据记录 | Baostock最新交易日：{latest_trading_day}")
        
        # 判断是否需要更新数据
        if timestamp_manager.needs_update(latest_trading_day):
            # 需要更新数据
            st.warning(f"🔄 数据需要更新（{last_update_date or '无'} -> {latest_trading_day}）")
            
            with st.spinner("正在更新数据..."):
                # 执行智能刷新
                result = st.session_state.data_manager.refresh_all_stocks(ds)
                
                if result['success']:
                    # 更新本地时间戳
                    timestamp_manager.set_last_update_date(latest_trading_day)
                    # 清除股票数据缓存，确保下次加载最新数据
                    st.session_state.stock_data_cache = {}
                    st.session_state.cache_timestamp = None
                    st.success(f"✓ 数据更新完成！总计：{result['total_stocks']}只")
                    st.info("🗑️ 缓存已清除，下次选股将加载最新数据")
                else:
                    st.error(f"✗ 数据更新失败：{result.get('error', '未知错误')}")
                    ds.logout()
                    return
        else:
            # 数据已是最新，直接选股
            st.success(f"✓ 本地数据已是最新（{latest_trading_day}），直接执行选股")
        
        # 执行选股（无论是否更新，都使用最新的本地数据）
        ds.logout()
        run_strategy_selection()
        
    except Exception as e:
        st.error(f"❌ 智能选股流程失败：{e}")
        import traceback
        st.code(traceback.format_exc())
    finally:
        ds.logout()


def run_strategy_selection():
    """执行策略选股（简化版，使用缓存机制）"""
    if not st.session_state.selected_strategies:
        st.error("请至少选择一个策略")
        return
    
    with st.spinner("正在执行策略选股，请稍候..."):
        try:
            dm = st.session_state.data_manager
            
            start_date = st.session_state.start_date
            end_date = st.session_state.end_date
            
            # 生成缓存键，包含日期范围
            cache_key = f"{start_date}_{end_date}"
            
            # 检查缓存是否有效
            if cache_key in st.session_state.stock_data_cache:
                st.info("📦 使用缓存的股票数据...")
                data_dict = st.session_state.stock_data_cache[cache_key]
                local_stocks = list(data_dict.keys())
                st.info(f"📊 缓存中已有 {len(local_stocks)} 只股票数据")
                # 创建空的 progress_bar 和 status_text 用于后续流程
                progress_bar = st.progress(100)
                status_text = st.empty()
            else:
                st.info("📂 正在扫描本地数据...")
                
                local_stocks = []
                local_data_dir = Path("data/daily") if dm.use_new_storage else Path("cache")
                
                if dm.use_new_storage and local_data_dir.exists():
                    for parquet_file in local_data_dir.glob("*.parquet"):
                        code = parquet_file.stem
                        local_stocks.append(code)
                
                if not local_stocks:
                    st.error("⚠️ 本地无数据，请先执行数据刷新")
                    return
                
                st.info(f"📊 本地已有 {len(local_stocks)} 只股票数据")
                
                st.info(f"📅 回测日期范围：{start_date} ~ {end_date}")
                
                data_dict = {}
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # 直接使用本地数据，不进行完整性检查
                for i, code in enumerate(local_stocks):
                    # 检查是否收到停止信号
                    if st.session_state.stop_strategy_selection:
                        st.warning("⏹️ 选股任务已停止")
                        return
                    
                    status_text.text(f"📂 加载 {code} 数据 ({i+1}/{len(local_stocks)})")
                    
                    df_local = dm.get_stock_data(code, start_date, end_date)
                    
                    if df_local is not None and not df_local.empty:
                        data_dict[code] = df_local
                    
                    progress_bar.progress((i + 1) / len(local_stocks))
                
                st.info(f"✅ 成功加载 {len(data_dict)} 只股票数据")
                
                # 更新缓存
                st.session_state.stock_data_cache[cache_key] = data_dict
                st.session_state.cache_timestamp = datetime.now().isoformat()
                st.info("💾 股票数据已缓存，后续选股操作将直接使用缓存")
            
            if not data_dict:
                st.error("❌ 没有可用的股票数据，请先刷新数据")
                return
            
            # 检查是否收到停止信号
            if st.session_state.stop_strategy_selection:
                st.warning("⏹️ 选股任务已停止")
                return
            
            status_text.text("🎯 正在执行策略选股...")
            
            stock_list = [{'code': code, 'name': ''} for code in data_dict.keys()]
            
            for strategy_key, params in st.session_state.strategy_params.items():
                st.session_state.strategy_combiner.update_strategy_params(strategy_key, params)
            
            st.session_state.strategy_combiner.update_combination_weights(
                st.session_state.strategy_weights
            )
            
            selected_stocks = st.session_state.strategy_combiner.combine_strategies(
                stock_list,
                data_dict,
                st.session_state.selected_strategies,
                st.session_state.strategy_weights
            )
            
            st.session_state.strategy_results = {
                'selected_stocks': selected_stocks,
                'stock_pool_size': len(local_stocks),
                'data_acquired': len(data_dict),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 重置停止标志
            st.session_state.stop_strategy_selection = False
            
            st.success(f"🎉 选股完成！从 {len(data_dict)} 只股票中选出 {len(selected_stocks)} 只")
            
        except Exception as e:
            st.error(f"❌ 策略选股失败：{e}")
            import traceback
            st.code(traceback.format_exc())


def run_strategy_backtest():
    """执行策略回测"""
    if st.session_state.strategy_results is None:
        st.error("请先执行策略选股")
        return
    
    selected_stocks = st.session_state.strategy_results['selected_stocks']
    
    if not selected_stocks:
        st.error("选股结果为空，无法回测")
        return
    
    st.session_state.stocks = selected_stocks
    
    run_backtest()


def display_strategy_results():
    """显示策略选股结果"""
    results = st.session_state.strategy_results
    stocks = results['selected_stocks']
    
    st.markdown("---")
    st.markdown("### 📊 选股结果")
    
    local_valid = results.get('local_valid_count', results['data_acquired'])
    updated_count = results.get('updated_count', 0)
    
    st.info(f"""
    **选股时间**: {results['timestamp']}  
    **股票池大小**: {results['stock_pool_size']} 只  
    **本地有效数据**: {local_valid} 只  
    **网络更新**: {updated_count} 只  
    **实际使用数据**: {results['data_acquired']} 只  
    **选中股票**: {len(stocks)} 只
    """)
    
    if stocks:
        df_display = pd.DataFrame([
            {
                '代码': s['code'],
                '名称': s.get('name', ''),
                '权重': f"{s['weight']*100:.2f}%",
                '入选策略': ', '.join(s.get('strategies', []))
            }
            for s in stocks
        ])
        
        st.dataframe(df_display, width='stretch', hide_index=True)
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📥 导出选股结果", width='stretch'):
                df_export = pd.DataFrame(stocks)
                df_export.to_csv("strategy_selection_results.csv", index=False, encoding='utf-8-sig')
                st.success("已导出 strategy_selection_results.csv")
        
        with col2:
            if st.button("📊 查看权重分布", width='stretch'):
                import plotly.express as px
                
                fig = px.pie(
                    df_display,
                    values=[s['weight'] for s in stocks],
                    names=[s['code'] for s in stocks],
                    title="股票权重分布"
                )
                st.plotly_chart(fig, width='stretch')


def save_strategy_combination(name: str):
    """保存策略组合"""
    combination = {
        'name': name,
        'selected_strategies': st.session_state.selected_strategies,
        'strategy_weights': st.session_state.strategy_weights,
        'strategy_params': st.session_state.strategy_params,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    st.session_state.saved_strategies.append(combination)
    
    try:
        with open('saved_strategies.json', 'w', encoding='utf-8') as f:
            json.dump(st.session_state.saved_strategies, f, ensure_ascii=False, indent=2)
        st.success(f"策略组合 '{name}' 已保存")
    except Exception as e:
        st.warning(f"保存到文件失败：{e}，已保存到会话")


def load_strategy_combination(name: str):
    """加载策略组合"""
    for combination in st.session_state.saved_strategies:
        if combination['name'] == name:
            st.session_state.selected_strategies = combination['selected_strategies']
            st.session_state.strategy_weights = combination['strategy_weights']
            st.session_state.strategy_params = combination['strategy_params']
            st.success(f"已加载策略组合 '{name}'")
            st.rerun()
            return
    
    st.warning(f"未找到策略组合 '{name}'")


def run_backtest():
    """执行回测"""
    with st.spinner("正在运行回测..."):
        try:
            # 初始化数据源
            ds = DataSourceManager()
            
            # 初始化交易日检查器
            tdc = TradingDayChecker(ds)
            
            # 初始化回测引擎（传入 data_manager 以启用缓存）
            engine = BacktestEngine(ds, tdc, st.session_state.data_manager)
            
            # 运行回测
            results = engine.run_backtest(
                stocks=st.session_state.stocks,
                start_date=st.session_state.start_date,
                end_date=st.session_state.end_date,
                initial_capital=st.session_state.initial_capital,
                rebalance_frequency=st.session_state.rebalance_frequency,
                commission_rate=st.session_state.commission_rate,
                slippage=st.session_state.slippage
            )
            
            if results:
                st.session_state.backtest_results = results
                st.success("✓ 回测完成！")
            else:
                st.error("✗ 回测失败")
            
            ds.logout()
            
        except Exception as e:
            st.error(f"回测出错：{e}")
            import traceback
            st.code(traceback.format_exc())


def display_results():
    """显示回测结果"""
    results = st.session_state.backtest_results
    metrics = results['metrics']
    portfolio_values = results['portfolio_values']
    
    st.markdown("---")
    st.markdown("## 📊 回测结果")
    
    # 核心指标卡片
    
    # 使用 st.metric 替代 HTML，更好的移动端支持
    cols = st.columns(4)
    metrics_list = [
        ("累计收益率", metrics['total_return_pct']),
        ("年化收益率", metrics['annual_return_pct']),
        ("最大回撤", metrics['max_drawdown_pct']),
        ("夏普比率", f"{metrics['sharpe_ratio']:.2f}")
    ]
    
    for i, (label, value) in enumerate(metrics_list):
        with cols[i]:
            st.metric(label=label, value=value)
    
    # 详细指标 - 移动端优化
    st.markdown("### 📋 详细指标")
    cols = st.columns(3)
    with cols[0]:
        st.metric("初始资金", f"¥{metrics['initial_capital']:,.2f}")
        st.metric("最终资产", f"¥{metrics['final_value']:,.2f}")
    with cols[1]:
        st.metric("波动率", metrics['volatility_pct'])
        st.metric("胜率", metrics['win_rate_pct'])
    with cols[2]:
        st.metric("卡玛比率", f"{metrics['calmar_ratio']:.2f}")
        st.metric("交易天数", f"{metrics['trading_days']} 天")
    
    # 净值曲线图
    st.markdown("### 📈 组合净值走势")
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=portfolio_values['date'],
        y=portfolio_values['total_value'],
        mode='lines',
        name='组合净值',
        line=dict(color='#1f77b4', width=2)
    ))
    
    fig.update_layout(
        height=500,
        xaxis_title="日期",
        yaxis_title="净值 (元)",
        hovermode='x unified',
        template='plotly_white',
        xaxis=dict(
            tickformat='%Y-%m-%d',
            tickangle=45
        )
    )
    
    st.plotly_chart(fig, width='stretch')
    
    # 收益分布图 - 移动端优化：在小屏幕上垂直堆叠
    st.markdown("### 📈 图表分析")
    
    # 回撤走势
    st.markdown("#### 📉 回撤走势")
    cum_max = portfolio_values['total_value'].cummax()
    drawdown = (portfolio_values['total_value'] - cum_max) / cum_max * 100
    
    fig_dd = go.Figure()
    fig_dd.add_trace(go.Scatter(
        x=portfolio_values['date'],
        y=drawdown,
        mode='lines',
        name='回撤',
        line=dict(color='#d62728', width=1),
        fill='tozeroy'
    ))
    
    fig_dd.update_layout(
        height=400,
        xaxis_title="日期",
        yaxis_title="回撤 (%)",
        hovermode='x unified',
        template='plotly_white',
        xaxis=dict(
            tickformat='%Y-%m-%d',
            tickangle=45
        ),
        margin=dict(l=20, r=20, t=20, b=20)
    )
    
    st.plotly_chart(fig_dd, width='stretch')
    
    # 股票权重分布
    st.markdown("#### 📊 股票权重分布")
    weights = [s['weight'] * 100 for s in results['stocks']]
    labels = [f"{s['name']}\n{s['code']}" for s in results['stocks']]
    
    fig_pie = go.Figure(data=[go.Pie(
        labels=labels,
        values=weights,
        hole=0.3
    )])
    
    fig_pie.update_layout(
        height=400,
        template='plotly_white',
        margin=dict(l=20, r=20, t=20, b=20)
    )
    
    st.plotly_chart(fig_pie, width='stretch')
    
    # 再平衡日期标记
    st.markdown("### 📅 再平衡日期")
    if results['rebalance_dates']:
        rebalance_df = pd.DataFrame({
            '序号': range(1, len(results['rebalance_dates']) + 1),
            '日期': [d.strftime('%Y-%m-%d') for d in results['rebalance_dates']]
        })
        st.dataframe(rebalance_df, width='stretch', hide_index=True)
    
    # 导出数据按钮 - 移动端优化
    st.markdown("### 📥 导出数据")
    cols = st.columns(2)
    with cols[0]:
        if st.button("📥 导出净值数据", width='stretch'):
            portfolio_values.to_csv("portfolio_values.csv", index=False, encoding='utf-8-sig')
            st.success("已导出 portfolio_values.csv")
    
    with cols[1]:
        if st.button("📥 导出指标数据", width='stretch'):
            metrics_df = pd.DataFrame({
                '指标': list(metrics.keys()),
                '数值': list(metrics.values())
            })
            metrics_df.to_csv("backtest_metrics.csv", index=False, encoding='utf-8-sig')
            st.success("已导出 backtest_metrics.csv")


def main():
    """主函数"""
    init_session_state()
    render_sidebar()
    
    # 根据页面选择显示不同内容
    if st.session_state.get('current_page') == "策略选股回测":
        render_strategy_page()
    else:
        render_main_content()
    
    # 页脚
    st.markdown("---")
    st.caption("A 股组合再平衡回测系统 | 支持 Baostock/Akshare/Tushare 多数据源")


if __name__ == "__main__":
    main()
