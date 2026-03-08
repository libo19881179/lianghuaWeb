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
        st.session_state.saved_strategies = []


def render_sidebar():
    """渲染侧边栏（移动端优化）"""
    with st.sidebar:
        st.markdown("## ⚙️ 配置")
        
        # 页面选择
        st.markdown("### 📑 功能选择")
        page = st.radio(
            "选择功能页面",
            ["手动选股回测", "策略选股回测"],
            index=0
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
        if st.button("验证数据源连接", use_container_width=True):
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
                            if st.button("✓ 保存", use_container_width=True, key=f"save_edit_{i}"):
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
                            if st.button("✗ 取消", use_container_width=True, key=f"cancel_edit_{i}"):
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
        
        if st.button("添加股票", use_container_width=True):
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
        if st.button("归一化权重", use_container_width=True, help="将所有权重调整为总和为 1"):
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
      
        
        # 刷新缓存按钮
        col1, col2 = st.columns([3, 1])
        with col1:
            if st.button("🔄 智能刷新全部 A 股", use_container_width=True, help="智能更新全部 A 股数据，只获取缺失的股票和最新数据"):
                # 智能刷新全部 A 股
                with st.spinner("正在智能刷新全部 A 股数据，这可能需要几分钟..."):
                    # 创建进度条
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # 创建数据源
                    from data_sources import DataSourceManager
                    ds = DataSourceManager()
                    
                    try:
                        # 执行智能刷新
                        result = st.session_state.data_manager.refresh_all_stocks(ds)
                        
                        if result['success']:
                            st.success(f"✓ 刷新完成！总计：{result['total_stocks']}只 | 缓存：{result['cached_stocks']} | 新增：{result['new_stocks']} | 更新：{result['updated_stocks']}")
                            st.info(f"缓存完整率：{result['cached_stocks']/result['total_stocks']*100:.1f}% - 避免了重复数据请求")
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
        if st.button("🚀 开始回测", use_container_width=True, type="primary"):
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
    
    with col1:
        st.markdown("#### 💎 稳健价值多因子策略")
        st.info(strategy_info['value_multifactor']['description'])
        use_value = st.checkbox("启用此策略", value=True, key="use_value")
        if use_value:
            weight_value = st.slider("策略权重", 0.0, 1.0, 0.4, 0.1, key="weight_value")
            st.session_state.strategy_weights['value_multifactor'] = weight_value
    
    with col2:
        st.markdown("#### 📈 动量行业轮动策略")
        st.info(strategy_info['momentum_rotation']['description'])
        use_momentum = st.checkbox("启用此策略", value=True, key="use_momentum")
        if use_momentum:
            weight_momentum = st.slider("策略权重", 0.0, 1.0, 0.3, 0.1, key="weight_momentum")
            st.session_state.strategy_weights['momentum_rotation'] = weight_momentum
    
    with col3:
        st.markdown("#### 🛡️ 低波动防御性策略")
        st.info(strategy_info['low_volatility_defensive']['description'])
        use_lowvol = st.checkbox("启用此策略", value=True, key="use_lowvol")
        if use_lowvol:
            weight_lowvol = st.slider("策略权重", 0.0, 1.0, 0.3, 0.1, key="weight_lowvol")
            st.session_state.strategy_weights['low_volatility_defensive'] = weight_lowvol
    
    # 更新选中的策略列表
    st.session_state.selected_strategies = []
    if use_value:
        st.session_state.selected_strategies.append('value_multifactor')
    if use_momentum:
        st.session_state.selected_strategies.append('momentum_rotation')
    if use_lowvol:
        st.session_state.selected_strategies.append('low_volatility_defensive')
    
    # 归一化权重
    if st.session_state.selected_strategies:
        total_weight = sum(st.session_state.strategy_weights[k] for k in st.session_state.selected_strategies)
        if total_weight > 0:
            for k in st.session_state.selected_strategies:
                st.session_state.strategy_weights[k] = st.session_state.strategy_weights[k] / total_weight
    
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
    
    # 执行选股和回测按钮
    st.markdown("---")
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("🎯 执行策略选股", use_container_width=True, type="primary"):
            run_strategy_selection()
    
    with col2:
        if st.button("🚀 执行回测", use_container_width=True, type="secondary"):
            run_strategy_backtest()
    
    # 显示选股结果
    if st.session_state.strategy_results is not None:
        display_strategy_results()
    
    # 策略保存和加载
    st.markdown("---")
    st.markdown("### 💾 策略组合管理")
    
    col1, col2 = st.columns(2)
    
    with col1:
        strategy_name = st.text_input("策略组合名称", placeholder="输入名称保存当前配置")
        if st.button("💾 保存策略组合", use_container_width=True):
            if strategy_name:
                save_strategy_combination(strategy_name)
            else:
                st.warning("请输入策略组合名称")
    
    with col2:
        saved_strategy_names = [s['name'] for s in st.session_state.saved_strategies]
        if saved_strategy_names:
            selected_saved = st.selectbox("已保存的策略组合", saved_strategy_names)
            if st.button("📂 加载策略组合", use_container_width=True):
                load_strategy_combination(selected_saved)
        else:
            st.info("暂无保存的策略组合")


def run_strategy_selection():
    """执行策略选股（本地数据优先）"""
    if not st.session_state.selected_strategies:
        st.error("请至少选择一个策略")
        return
    
    with st.spinner("正在执行策略选股，请稍候..."):
        try:
            dm = st.session_state.data_manager
            ds = None
            
            st.info("📂 正在扫描本地数据...")
            
            local_stocks = []
            local_data_dir = Path("data/daily") if dm.use_new_storage else Path("cache")
            
            if dm.use_new_storage and local_data_dir.exists():
                for parquet_file in local_data_dir.glob("*.parquet"):
                    code = parquet_file.stem
                    local_stocks.append(code)
            
            if not local_stocks:
                st.warning("⚠️ 本地无数据，需要从网络获取股票池")
                ds = DataSourceManager()
                stock_pool = ds.get_all_a_stock_codes()
                
                if stock_pool is None or stock_pool.empty:
                    st.error("获取股票池失败")
                    if ds:
                        ds.logout()
                    return
                
                local_stocks = [row['code'] for _, row in stock_pool.iterrows()]
                st.info(f"📊 从网络获取到 {len(local_stocks)} 只股票")
            else:
                st.info(f"📊 本地已有 {len(local_stocks)} 只股票数据")
            
            start_date = st.session_state.start_date
            end_date = st.session_state.end_date
            
            st.info(f"📅 回测日期范围：{start_date} ~ {end_date}")
            
            data_dict = {}
            need_update_stocks = []
            local_valid_count = 0
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            MIN_DATA_DAYS = 63
            MAX_DATA_AGE_DAYS = 7
            
            from datetime import datetime as dt
            
            for i, code in enumerate(local_stocks):
                status_text.text(f"🔍 检查 {code} 数据完整性 ({i+1}/{len(local_stocks)})")
                
                df_local = dm.get_stock_data(code, start_date, end_date)
                
                is_valid = False
                need_update = False
                
                if df_local is not None and not df_local.empty:
                    data_days = len(df_local)
                    
                    if data_days >= MIN_DATA_DAYS:
                        cached_min, cached_max = dm.get_stock_date_range(code)
                        
                        if cached_max:
                            cached_max_date = pd.to_datetime(cached_max)
                            today = dt.now()
                            data_age = (today - cached_max_date).days
                            
                            if data_age <= MAX_DATA_AGE_DAYS:
                                is_valid = True
                                local_valid_count += 1
                                data_dict[code] = df_local
                            else:
                                need_update = True
                                need_update_stocks.append(code)
                        else:
                            is_valid = True
                            local_valid_count += 1
                            data_dict[code] = df_local
                    else:
                        need_update = True
                        need_update_stocks.append(code)
                else:
                    need_update = True
                    need_update_stocks.append(code)
                
                progress_bar.progress((i + 1) / len(local_stocks))
            
            st.info(f"✅ 本地有效数据：{local_valid_count} 只 | ⚠️ 需要更新：{len(need_update_stocks)} 只")
            
            if need_update_stocks:
                st.warning(f"🔄 有 {len(need_update_stocks)} 只股票数据需要更新")
                
                update_mode = st.radio(
                    "选择更新方式",
                    ["跳过更新，仅使用本地数据", "仅更新前50只", "更新全部（耗时较长）"],
                    index=0,
                    key="update_mode_radio"
                )
                
                if "跳过更新" in update_mode:
                    st.info("⏭️ 跳过网络更新，仅使用本地有效数据")
                else:
                    update_limit = 50 if "前50只" in update_mode else len(need_update_stocks)
                    
                    if ds is None:
                        ds = DataSourceManager()
                    
                    st.info(f"🌐 正在从网络更新 {min(update_limit, len(need_update_stocks))} 只股票数据...")
                    
                    update_progress = st.progress(0)
                    update_status = st.empty()
                    updated_count = 0
                    
                    for i, code in enumerate(need_update_stocks[:update_limit]):
                        update_status.text(f"📥 更新 {code} ({i+1}/{update_limit})")
                        
                        try:
                            df_new = ds.get_stock_data(code, start_date, end_date)
                            
                            if df_new is not None and not df_new.empty:
                                dm.save_stock_data(code, df_new)
                                data_dict[code] = df_new
                                updated_count += 1
                        except Exception as e:
                            pass
                        
                        update_progress.progress((i + 1) / update_limit)
                    
                    st.success(f"✅ 成功更新 {updated_count} 只股票数据")
            
            if not data_dict:
                st.error("❌ 没有可用的股票数据，请先刷新数据")
                if ds:
                    ds.logout()
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
                'local_valid_count': local_valid_count,
                'updated_count': len(need_update_stocks) if need_update_stocks and "跳过更新" not in update_mode else 0,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            st.success(f"🎉 选股完成！从 {len(data_dict)} 只股票中选出 {len(selected_stocks)} 只")
            
            if ds:
                ds.logout()
            
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
        
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📥 导出选股结果", use_container_width=True):
                df_export = pd.DataFrame(stocks)
                df_export.to_csv("strategy_selection_results.csv", index=False, encoding='utf-8-sig')
                st.success("已导出 strategy_selection_results.csv")
        
        with col2:
            if st.button("📊 查看权重分布", use_container_width=True):
                import plotly.express as px
                
                fig = px.pie(
                    df_display,
                    values=[s['weight'] for s in stocks],
                    names=[s['code'] for s in stocks],
                    title="股票权重分布"
                )
                st.plotly_chart(fig, use_container_width=True)


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
    
    st.plotly_chart(fig, use_container_width=True)
    
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
    
    st.plotly_chart(fig_dd, use_container_width=True)
    
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
    
    st.plotly_chart(fig_pie, use_container_width=True)
    
    # 再平衡日期标记
    st.markdown("### 📅 再平衡日期")
    if results['rebalance_dates']:
        rebalance_df = pd.DataFrame({
            '序号': range(1, len(results['rebalance_dates']) + 1),
            '日期': [d.strftime('%Y-%m-%d') for d in results['rebalance_dates']]
        })
        st.dataframe(rebalance_df, use_container_width=True, hide_index=True)
    
    # 导出数据按钮 - 移动端优化
    st.markdown("### 📥 导出数据")
    cols = st.columns(2)
    with cols[0]:
        if st.button("📥 导出净值数据", use_container_width=True):
            portfolio_values.to_csv("portfolio_values.csv", index=False, encoding='utf-8-sig')
            st.success("已导出 portfolio_values.csv")
    
    with cols[1]:
        if st.button("📥 导出指标数据", use_container_width=True):
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
