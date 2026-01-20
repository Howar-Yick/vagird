# event_driven_grid_strategy.py
# 版本号：GEMINI-3.4.0-Refactor
#
# 更新日志 (v3.4.0):
# 1. 【架构重构】引入 StrategyConfig 静态配置类，统一管理参数与热重载。
# 2. 【算法升级】ATR 算法从 SMA 升级为 EMA，并增加 10% 变化阈值过滤，防止间距频繁抖动。
# 3. 【鲁棒增强】ATR 计算增加“新股适应”与“历史记忆”机制，数据源波动时自动降级使用缓存。

import json
import logging
import math
import time
from collections import deque
from datetime import datetime
from datetime import time as dtime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

# ---------------- 全局句柄 ----------------
LOG_FH = None
LOG_DATE = None
__version__ = 'GEMINI-3.4.0-Refactor'

# ---------------- 配置管理类 (StrategyConfig) ----------------

class StrategyConfig:
    """
    策略静态配置类：收拢所有硬编码参数，支持从文件动态加载覆盖。
    """
    # --- 核心常量 ---
    MAX_SAVED_FILLED_IDS = 500
    TRANSACTION_COST = 0.00006  # 万分之六
    MAX_TRADE_AMOUNT = 5000     # 单笔网格交易最大金额（人民币）
    
    # --- 调试配置 ---
    DEBUG = SimpleNamespace()
    DEBUG.ENABLE = True
    DEBUG.RT_WINDOW_SEC = 60
    DEBUG.RT_PREVIEW = 8
    DEBUG.DELAY_AFTER_CANCEL = 2.0

    # --- VA (价值平均) 配置 ---
    VA = SimpleNamespace()
    VA.THRESHOLD_K = 1.0
    VA.MIN_UPDATE_INTERVAL_MIN = 60
    VA.MAX_UPDATES_PER_DAY = 3

    # --- 市场/风控配置 ---
    MARKET = SimpleNamespace()
    MARKET.HALT_SKIP_PLACE = True
    MARKET.HALT_SKIP_AFTER_SEC = 180
    MARKET.HALT_LOG_EVERY_MIN = 10
    
    # --- 启动配置 ---
    BOOT = SimpleNamespace()
    BOOT.GRACE_SECONDS = 180

    @classmethod
    def load(cls, context):
        """
        加载所有配置文件并覆盖默认参数。
        """
        cls._load_debug_config(context)
        cls._load_va_config(context)
        cls._load_market_config(context)
        cls._load_strategy_config(context) # 统一配置最后覆盖
        
        # 将关键参数注入到 context 以便兼容旧代码习惯（可选，逐步废弃）
        context.delay_after_cancel_seconds = cls.DEBUG.DELAY_AFTER_CANCEL
        
    @classmethod
    def _load_debug_config(cls, context):
        cfg_file = research_path('config', 'debug.json')
        if not cls._check_mtime(context, 'debug_cfg_mtime', cfg_file): return
        
        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if 'enable_debug_log' in j: cls.DEBUG.ENABLE = bool(j['enable_debug_log'])
            if 'rt_heartbeat_window_sec' in j: cls.DEBUG.RT_WINDOW_SEC = max(5, int(j['rt_heartbeat_window_sec']))
            if 'delay_after_cancel_seconds' in j: cls.DEBUG.DELAY_AFTER_CANCEL = max(0.0, float(j['delay_after_cancel_seconds']))
            info('⚙️ [Config] Debug配置已更新')
        except Exception as e:
            pass

    @classmethod
    def _load_va_config(cls, context):
        cfg_file = research_path('config', 'va.json')
        if not cls._check_mtime(context, 'va_cfg_mtime', cfg_file): return

        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if 'value_threshold_k' in j: cls.VA.THRESHOLD_K = float(j['value_threshold_k'])
            if 'max_updates_per_day' in j: cls.VA.MAX_UPDATES_PER_DAY = int(j['max_updates_per_day'])
            info('⚙️ [Config] VA配置已更新')
        except Exception as e:
            pass

    @classmethod
    def _load_market_config(cls, context):
        cfg_file = research_path('config', 'market.json')
        if not cls._check_mtime(context, 'market_cfg_mtime', cfg_file): return

        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if 'halt_skip_place' in j: cls.MARKET.HALT_SKIP_PLACE = bool(j['halt_skip_place'])
            if 'halt_skip_after_seconds' in j: cls.MARKET.HALT_SKIP_AFTER_SEC = int(j['halt_skip_after_seconds'])
            info('⚙️ [Config] Market配置已更新')
        except Exception as e:
            pass

    @classmethod
    def _load_strategy_config(cls, context):
        cfg_file = research_path('config', 'strategy.json')
        if not cls._check_mtime(context, 'strategy_cfg_mtime', cfg_file): return

        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            # 允许 strategy.json 覆盖所有子项
            dbg = j.get('debug', {})
            if 'delay_after_cancel_seconds' in dbg: cls.DEBUG.DELAY_AFTER_CANCEL = float(dbg['delay_after_cancel_seconds'])
            info('⚙️ [Config] Strategy统一配置已加载')
        except Exception as e:
            pass

    @classmethod
    def _check_mtime(cls, context, attr_name, path):
        """检查文件修改时间，决定是否重载"""
        try:
            mtime = path.stat().st_mtime if path.exists() else None
        except:
            mtime = None
        
        last_mtime = getattr(context, attr_name, None)
        if last_mtime == mtime:
            return False
        
        setattr(context, attr_name, mtime)
        return path.exists()

# ---------------- 工具类：OrderUtils ----------------

class OrderUtils:
    """
    订单处理工具类：统一处理对象/字典兼容性，封装通用逻辑。
    """
    @staticmethod
    def normalize(order):
        data = {}
        if isinstance(order, dict):
            raw_no = order.get('entrust_no')
            raw_sym = order.get('symbol') or order.get('stock_code')
            raw_status = order.get('status')
            raw_amt = order.get('amount')
            raw_price = order.get('price')
            raw_bs = order.get('entrust_bs')
        else:
            raw_no = getattr(order, 'entrust_no', None)
            raw_sym = getattr(order, 'symbol', None) or getattr(order, 'stock_code', None)
            raw_status = getattr(order, 'status', None)
            raw_amt = getattr(order, 'amount', None)
            raw_price = getattr(order, 'price', None)
            raw_bs = getattr(order, 'entrust_bs', None)

        data['entrust_no'] = str(raw_no) if raw_no is not None else ''
        data['raw_symbol'] = str(raw_sym) if raw_sym else ''
        data['std_symbol'] = convert_symbol_to_standard(data['raw_symbol'])
        data['status'] = str(raw_status) if raw_status is not None else ''
        data['amount'] = float(raw_amt or 0)
        data['price'] = float(raw_price or 0)
        data['entrust_bs'] = str(raw_bs) if raw_bs else ''
        data['original'] = order
        return data

    @staticmethod
    def is_active(order_dict):
        """判断是否为有效挂单 (状态2已报, 7部成)"""
        return order_dict['status'] in ['2', '7']

    @staticmethod
    def is_sell(order_dict):
        """判断是否为卖单"""
        return (order_dict['amount'] < 0) or (order_dict['entrust_bs'] == '2')

# ---------------- 通用路径与工具函数 ----------------

def research_path(*parts) -> Path:
    p = Path(get_research_path()).joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _ensure_daily_logfile():
    global LOG_FH, LOG_DATE
    today_str = datetime.now().strftime('%Y-%m-%d')
    if LOG_DATE != today_str or LOG_FH is None:
        try:
            if LOG_FH:
                LOG_FH.flush()
                LOG_FH.close()
        except:
            pass
        log_dir = research_path('logs')
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{today_str}_strategy.log"
        LOG_FH = open(log_path, 'a', encoding='utf-8')
        LOG_DATE = today_str
        try:
            log.info(f'🔍 日志切换到 {log_path}')
        except:
            pass
        return log_path
    return research_path('logs', f"{today_str}_strategy.log")

def info(msg, *args):
    text = msg.format(*args)
    log.info(text)
    _ensure_daily_logfile()
    if LOG_FH:
        LOG_FH.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} - INFO - {text}\n")
        LOG_FH.flush()

def get_saved_param(key, default=None):
    try:
        return get_parameter(key)
    except:
        return default

def set_saved_param(key, value):
    try:
        set_parameter(key, value)
    except:
        pass

def check_environment():
    try:
        u = str(get_user_name())
        if u == '55418810': return '回测'
        if u == '8887591588': return '实盘'
        return '模拟'
    except:
        return '未知'

def convert_symbol_to_standard(full_symbol):
    if not isinstance(full_symbol, str):
        return full_symbol
    if full_symbol.endswith('.XSHE'):
        return full_symbol.replace('.XSHE','.SZ')
    if full_symbol.endswith('.XSHG'):
        return full_symbol.replace('.XSHG','.SS')
    return full_symbol

# ---------------- 标的中文名 ----------------

def _load_symbol_names(context):
    name_map = {}
    try:
        names_file = research_path('config', 'names.json')
        if names_file.exists():
            j = json.loads(names_file.read_text(encoding='utf-8'))
            if isinstance(j, dict):
                name_map.update({k: str(v) for k, v in j.items() if isinstance(k, str)})
    except Exception as e:
        pass

    try:
        for sym, cfg in (getattr(context, 'symbol_config', {}) or {}).items():
            if isinstance(cfg, dict) and 'name' in cfg and cfg['name']:
                name_map[sym] = str(cfg['name'])
    except Exception as e:
        pass
    context.symbol_name_map = name_map

def dsym(context, symbol, style='short'):
    nm = (getattr(context, 'symbol_name_map', {}) or {}).get(symbol)
    if not nm:
        return symbol
    return f"{symbol} {nm}" if style == 'short' else f"{nm}({symbol})"

# ---------------- HALT-GUARD ----------------

def is_valid_price(x):
    try:
        if x is None: return False
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)): return False
        if x <= 0: return False
        return True
    except:
        return False

# ---------------- 状态保存 ----------------

def save_state(symbol, state):
    ids = list(state.get('filled_order_ids', set()))
    state['filled_order_ids'] = set(ids[-StrategyConfig.MAX_SAVED_FILLED_IDS:])
    store_keys = ['base_price', 'grid_unit', 'max_position', 'last_week_position', 'base_position', 
                  'used_atr_rate', 'cached_atr_ema'] # 【新增】持久化 ATR 状态
    store = {k: state.get(k) for k in store_keys}
    store['filled_order_ids'] = ids[-StrategyConfig.MAX_SAVED_FILLED_IDS:]
    store['trade_week_set'] = list(state.get('trade_week_set', []))
    set_saved_param(f'state_{symbol}', store)
    research_path('state', f'{symbol}.json').write_text(json.dumps(store, indent=2), encoding='utf-8')

def safe_save_state(symbol, state):
    try:
        save_state(symbol, state)
    except Exception as e:
        info('[{}] ⚠️ 状态保存失败: {}', symbol, e)

# ---------------- 初始化与时间窗口判断 ----------------

def initialize(context):
    log_file = _ensure_daily_logfile()
    log.info(f'🔍 日志同时写入到 {log_file}')
    context.env = check_environment()
    info("当前环境：{}", context.env)
    context.run_cycle = get_saved_param('run_cycle_seconds', 60)

    # 读取配置
    try:
        config_file = research_path('config', 'symbols.json')
        context.config_file_path = config_file
        if config_file.exists():
            context.symbol_config = json.loads(config_file.read_text(encoding='utf-8'))
            context.last_config_mod_time = config_file.stat().st_mtime
            info('✅ 从 {} 加载 {} 个标的配置', config_file, len(context.symbol_config))
        else:
            log.error(f"❌ 配置文件 {config_file} 不存在，请创建！")
            context.symbol_config = {}
    except Exception as e:
        log.error(f"❌ 加载配置文件失败：{e}")
        context.symbol_config = {}

    context.symbol_list = list(context.symbol_config.keys())
    _load_symbol_names(context)

    context.state = {}
    context.latest_data = {}
    context.should_place_order_map = {}
    context.mark_halted = {}
    context.last_valid_price = {}
    context.last_valid_ts = {sym: None for sym in context.symbol_list}
    context.pending_frozen = {} 
    
    context.intraday_metrics = {}
    context.recent_fill_ring = deque(maxlen=200)

    # 初始化每个标的状态
    for sym, cfg in context.symbol_config.items():
        state_file = research_path('state', f'{sym}.json')
        saved = json.loads(state_file.read_text(encoding='utf-8')) if state_file.exists() else get_saved_param(f'state_{sym}', {}) or {}
        st = {**cfg}
        st.update({
            'base_price': saved.get('base_price', cfg['base_price']),
            'grid_unit': saved.get('grid_unit', cfg['grid_unit']),
            'filled_order_ids': set(saved.get('filled_order_ids', [])),
            'trade_week_set': set(saved.get('trade_week_set', [])),
            
            'base_position': saved.get('base_position', cfg['initial_base_position']),
            'last_week_position': saved.get('last_week_position', cfg['initial_base_position']),
            
            'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
            'buy_grid_spacing': 0.005,
            'sell_grid_spacing': 0.005,
            
            'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20),
            
            # 【新增】ATR 记忆字段
            'used_atr_rate': saved.get('used_atr_rate', None),    # 当前生效的 ATR 率
            'cached_atr_ema': saved.get('cached_atr_ema', None),  # EMA 原始值
            
            'va_last_update_dt': None,
            '_halt_next_log_dt': None,
            '_oo_last': 0,
            '_recover_until': None,
            '_after_cancel_until': None,
            '_oo_drop_seen_ts': None,
            '_pos_jump_seen_ts': None,
            '_pos_confirm_deadline': None,
            '_rehang_due_ts': None,
            '_ignore_place_until': None,
            '_pending_ignore_ids': []
        })
        
        # 清理旧数据
        if 'scale_factor' in st: st.pop('scale_factor')
            
        context.state[sym] = st
        context.latest_data[sym] = st['base_price']
        context.should_place_order_map[sym] = True
        context.mark_halted[sym] = False
        context.last_valid_price[sym] = st['base_price']
        context.last_valid_ts[sym] = None
        context.pending_frozen[sym] = 0

    context.boot_dt = getattr(context, 'current_dt', None) or datetime.now()
    
    context.last_report_time = None
    context.initial_cleanup_done = False
    
    # 加载配置
    StrategyConfig.load(context)
    
    _repair_state_logic(context)
    
    if '回测' not in context.env:
        run_daily(context, place_auction_orders, time='9:15')
        run_daily(context, end_of_day, time='14:55')
        run_interval(context, check_pending_rehangs, seconds=3)
        info('✅ 事件驱动模式就绪 (Async State Machine Active)')

    # PnL 指标
    context.pnl_metrics_path = research_path('state', 'pnl_metrics.json')
    context.pnl_metrics = _load_pnl_metrics(context.pnl_metrics_path)
    
    info('✅ 初始化完成，版本:{}', __version__)

# ---------------- 数据自动修复逻辑 ----------------

def _repair_state_logic(context):
    info('🛠️ [Data Repair] 开始检查并修复潜在的底仓数据异常...')
    for sym in context.symbol_list:
        state = context.state[sym]
        weeks = len(state.get('trade_week_set', []))
        if weeks <= 0: continue
            
        d_base = state.get('dingtou_base', 0)
        d_rate = state.get('dingtou_rate', 0)
        acc_invest = sum(d_base * (1 + d_rate)**w for w in range(1, weeks + 1))
        target_val = state['initial_position_value'] + acc_invest
        price = state['base_price']
        if price <= 0: continue
            
        theoretical_pos = int(target_val / price / 100) * 100
        current_pos = state['base_position']
        
        if current_pos < theoretical_pos * 0.70 and theoretical_pos > state['initial_base_position']:
            info(f"[{dsym(context, sym)}] ⚠️ 发现底仓异常! 当前:{current_pos} vs 理论:{theoretical_pos} (周数:{weeks})... 正在执行自动修复。")
            state['base_position'] = theoretical_pos
            state['last_week_position'] = theoretical_pos
            state['max_position'] = theoretical_pos + state['grid_unit'] * 20
            safe_save_state(sym, state)
            info(f"[{dsym(context, sym)}] ✅ 修复完成。底仓已重置为 {theoretical_pos}")

def is_main_trading_time():
    now = datetime.now().time()
    return (dtime(9, 30) <= now <= dtime(11, 30)) or (dtime(13, 0) <= now <= dtime(15, 0))

def is_auction_time():
    now = datetime.now().time()
    return dtime(9, 15) <= now < dtime(9, 25)

def is_order_blocking_period():
    now = datetime.now().time()
    return dtime(9, 25) <= now < dtime(9, 30)

# ---------------- 启动后清理与收敛 ----------------

def before_trading_start(context, data):
    if '回测' not in context.env:
        info('🔄 [PnL Reset] 强制重置 PnL 状态并回溯补算...')
        context.pnl_metrics = {} 
        try:
            _calculate_local_pnl_lifo(context) 
        except Exception as e:
            info('⚠️ PnL 补算遇到轻微错误: {} (后续会重试)', e)
        generate_html_report(context)
        context.last_report_time = context.current_dt

    if context.initial_cleanup_done: return
    info('🔄 before_trading_start：清理遗留挂单')
    after_initialize_cleanup(context)
    current_time = context.current_dt.time()
    if dtime(9, 15) <= current_time < dtime(9, 30):
        info('⏭ 重启在集合竞价时段，补挂网格')
        place_auction_orders(context)
    else:
        info('⏸️ 重启时间{}不在集合竞价时段，跳过补挂网格', current_time.strftime('%H:%M:%S'))
    context.initial_cleanup_done = True

def after_initialize_cleanup(context):
    if '回测' in context.env or not hasattr(context, 'symbol_list'): return
    info('⚡ 执行全局启动清理 (Restart Cleanup)...')
    try:
        all_orders = get_all_orders()
        if not all_orders:
            info('🕊️ 账户无挂单，清理完毕。')
            return

        to_cancel = []
        for o in all_orders:
            order_info = OrderUtils.normalize(o)
            if order_info['std_symbol'] not in context.symbol_list: continue
            if OrderUtils.is_active(order_info): 
                to_cancel.append(order_info)
        
        if not to_cancel:
            info('🕊️ 无有效挂单(状态2/7)，清理完毕。')
        else:
            info('🧹 扫描到 {} 笔有效挂单(含部成)，正在批量撤销...', len(to_cancel))
            for o_info in to_cancel:
                try:
                    cancel_order_ex(o_info['original'])
                    if OrderUtils.is_sell(o_info):
                        o_sym = o_info['std_symbol']
                        if o_sym in context.pending_frozen:
                            frozen = abs(o_info['amount'])
                            context.pending_frozen[o_sym] = max(0, context.pending_frozen[o_sym] - frozen)
                except Exception as e:
                    pass

    except Exception as e:
        info('❌ 启动清理主流程异常: {}', e)
    
    for sym in context.symbol_list:
        context.pending_frozen[sym] = 0
    info('✅ 全局清理完成')

def _fast_cancel_all_orders_global(context):
    after_initialize_cleanup(context)

# ---------------- 订单与撤单工具 ----------------

def get_order_status(entrust_no):
    try:
        order_detail = get_order(entrust_no)
        return str(order_detail.get('status', '')) if order_detail else ''
    except Exception as e:
        return ''

def cancel_all_orders_by_symbol(context, symbol):
    current_open_orders = get_open_orders(symbol) or []
    total = 0
    cancelled_ids = set()
    
    if not hasattr(context, 'canceled_cache'):
        context.canceled_cache = {'date': None, 'orders': set()}
    today = context.current_dt.date()
    if context.canceled_cache.get('date') != today:
        context.canceled_cache = {'date': today, 'orders': set()}
    cache = context.canceled_cache['orders']

    for o in current_open_orders:
        order_info = OrderUtils.normalize(o)
        if order_info['std_symbol'] != symbol: continue

        entrust_no = order_info['entrust_no']
        if (not entrust_no
            or not OrderUtils.is_active(order_info)
            or entrust_no in context.state[symbol]['filled_order_ids']
            or entrust_no in cache):
            continue
            
        final_status = get_order_status(entrust_no)
        if final_status in ('8', '4', '5', '6'): continue
            
        cache.add(entrust_no)
        total += 1
        info('[{}] 👉 发现并尝试撤销遗留挂单 entrust_no={}', dsym(context, symbol), entrust_no)
        try:
            cancel_order_ex({'entrust_no': entrust_no, 'symbol': order_info['raw_symbol']})
            cancelled_ids.add(entrust_no)
            if OrderUtils.is_sell(order_info):
                frozen = abs(order_info['amount'])
                context.pending_frozen[symbol] = max(0, context.pending_frozen.get(symbol, 0) - frozen)
        except Exception as e:
            info('[{}] ⚠️ 撤单异常 entrust_no={}: {}', dsym(context, symbol), entrust_no, e)
            
    return cancelled_ids

# ---------------- 集合竞价挂单 ----------------

def place_auction_orders(context):
    if '回测' in context.env or not (is_auction_time() or is_main_trading_time()): return
    info('🆕 开始集合竞价挂单流程 (并发模式)...')
    _fast_cancel_all_orders_global(context)
    
    orders_batch = []
    for sym in context.symbol_list:
        state = context.state[sym]
        state.pop('_last_order_bp', None)
        state.pop('_last_order_ts', None)
        
        adjust_grid_unit(state)
        context.latest_data[sym] = state['base_price']
        
        base = state['base_price']
        unit = state['grid_unit']
        buy_p = round(base * (1 - state['buy_grid_spacing']), 3)
        sell_p = round(base * (1 + state['sell_grid_spacing']), 3)
        
        position = get_position(sym)
        pos = position.amount
        enable = position.enable_amount - context.pending_frozen.get(sym, 0)
        
        if pos + unit <= state['max_position']:
            orders_batch.append({'symbol': sym, 'side': 'buy', 'price': buy_p, 'amount': unit})
        if enable >= unit and pos - unit >= state['base_position']:
            orders_batch.append({'symbol': sym, 'side': 'sell', 'price': sell_p, 'amount': -unit})
            
        safe_save_state(sym, state)

    info('🚀 生成 {} 笔挂单任务，开始密集发送...', len(orders_batch))
    count = 0
    for task in orders_batch:
        try:
            if count > 0 and count % 5 == 0: time.sleep(0.05)
            order(task['symbol'], task['amount'], limit_price=task['price'])
            if task['amount'] < 0:
                sym = task['symbol']
                context.pending_frozen[sym] = context.pending_frozen.get(sym, 0) + abs(task['amount'])
            count += 1
        except Exception as e:
            pass

# ---------------- 实时价：快照获取 + 心跳日志 ----------------

def _fetch_quotes_via_snapshot(context):
    # 配置热重载
    StrategyConfig.load(context)

    symbols = list(getattr(context, 'symbol_list', []) or [])
    if not symbols: return

    snaps = {}
    try:
        snaps = get_snapshot(symbols) or {}
    except Exception as e:
        snaps = {}

    if isinstance(snaps, list):
        snaps = { (s.get('symbol') or s.get('stock_code') or s.get('security') or ''): s for s in snaps if isinstance(s, dict) }

    now_dt = context.current_dt
    got, miss_list = 0, []
    for sym in symbols:
        snap = snaps.get(sym)
        px = None
        if isinstance(snap, dict):
            px = snap.get('last_px')
            if not is_valid_price(px): px = snap.get('last') or snap.get('price')
        if is_valid_price(px):
            px = float(px)
            context.latest_data[sym] = px
            context.last_valid_price[sym] = px
            context.last_valid_ts[sym] = now_dt
            got += 1
        else:
            miss_list.append(sym)

    if StrategyConfig.DEBUG.ENABLE:
        need_log = False
        if not hasattr(context, 'last_rt_log_ts') or context.last_rt_log_ts is None:
            need_log = True
        else:
            winsec = StrategyConfig.DEBUG.RT_WINDOW_SEC
            need_log = (now_dt - context.last_rt_log_ts).total_seconds() >= winsec
        if need_log:
            context.last_rt_log_ts = now_dt
            preview_n = StrategyConfig.DEBUG.RT_PREVIEW
            miss_preview = ','.join(miss_list[:preview_n]) + ('...' if len(miss_list) > preview_n else '')
            info('💓 RT心跳 {} got:{}/{} miss:[{}]', now_dt.strftime('%H:%M'), got, len(symbols), miss_preview)

# ---------------- 小工具：成交去重 & 窗口判断 ----------------

def _make_fill_key(symbol, amount, price, when):
    side = 1 if amount > 0 else -1
    bucket = when.replace(second=0, microsecond=0)
    qty = abs(int(amount))
    px = round(float(price or 0), 3)
    return (symbol, side, qty, bucket, px)

def _is_dup_fill(context, key, ttl_sec=5):
    now = context.current_dt
    while context.recent_fill_ring:
        k, ts = context.recent_fill_ring[0]
        if (now - ts).total_seconds() > ttl_sec:
            context.recent_fill_ring.popleft()
        else:
            break
    for k, _ in context.recent_fill_ring:
        if k[:-1] == key[:-1]:
            return True
    return False

def _remember_fill(context, key):
    context.recent_fill_ring.append((key, context.current_dt))

def _in_reopen_window(now_t: dtime):
    anchors = [dtime(9,30,0), dtime(10,30,0), dtime(13,0,0)]
    for a in anchors:
        if abs((datetime.combine(datetime.today(), now_t) - datetime.combine(datetime.today(), a)).total_seconds()) <= 35:
            return True
    return False

# ---------------- 异步补单状态机 ----------------

def check_pending_rehangs(context):
    if context.current_dt.time() >= dtime(14, 55): return
    now_t = context.current_dt.time()
    if (now_t.hour == 9 and now_t.minute == 30) or (now_t.hour == 13 and now_t.minute == 0): return

    now_wall = datetime.now()
    for sym in context.symbol_list:
        if sym not in context.state: continue
        state = context.state[sym]
        rehang_ts = state.get('_rehang_due_ts')
        
        if rehang_ts and now_wall >= rehang_ts:
            info('[{}] ⏰ 补单冷却期已过, 触发挂单...', dsym(context, sym))
            state['_rehang_due_ts'] = None
            ignore_ids = set(state.get('_pending_ignore_ids', []))
            if '_pending_ignore_ids' in state: state.pop('_pending_ignore_ids')

            place_limit_orders(context, sym, state, ignore_cooldown=True, ignore_entrust_nos=ignore_ids)
            safe_save_state(sym, state)

def _recalc_pending_frozen(context, symbol):
    try:
        orders = get_open_orders(symbol) or []
        frozen = 0
        for o in orders:
            order_info = OrderUtils.normalize(o)
            if OrderUtils.is_active(order_info) and OrderUtils.is_sell(order_info):
                frozen += abs(order_info['amount'])
        context.pending_frozen[symbol] = frozen
        return frozen
    except Exception as e:
        if StrategyConfig.DEBUG.ENABLE:
            info('[{}] ⚠️ 同步冻结量失败: {}', dsym(context, symbol), e)
        return context.pending_frozen.get(symbol, 0)

# ---------------- 网格限价挂单主逻辑 ----------------

def place_limit_orders(context, symbol, state, ignore_cooldown=False, bypass_lock=False, ignore_entrust_nos=None):
    if context.current_dt.time() >= dtime(14, 55): return

    _recalc_pending_frozen(context, symbol)
    now_dt = context.current_dt
    
    if not bypass_lock:
        ignore_until = state.get('_ignore_place_until')
        if ignore_until and datetime.now() < ignore_until: return

    if state.get('_rehang_due_ts') is not None: return

    if (not ignore_cooldown) and state.get('_last_trade_ts') \
      and (now_dt - state['_last_trade_ts']).total_seconds() < 60:
        return

    if is_order_blocking_period(): return
    
    in_limit_window = is_auction_time() or (is_main_trading_time() and now_dt.time() < dtime(14, 55))
    if not in_limit_window: return

    if is_main_trading_time() and not is_auction_time():
        if StrategyConfig.MARKET.HALT_SKIP_PLACE:
            last_ts = context.last_valid_ts.get(symbol)
            halt_after = StrategyConfig.MARKET.HALT_SKIP_AFTER_SEC
            if context.mark_halted.get(symbol, False) and last_ts:
                if (now_dt - last_ts).total_seconds() >= halt_after:
                    next_log = state.get('_halt_next_log_dt')
                    if (not next_log) or now_dt >= next_log:
                        info('[{}] ⛔ 停牌/断流超过{}s：暂停新挂单。', dsym(context, symbol), halt_after)
                        state['_halt_next_log_dt'] = now_dt + timedelta(minutes=StrategyConfig.MARKET.HALT_LOG_EVERY_MIN)
                        safe_save_state(symbol, state)
                    return

    boot_grace = (now_dt - getattr(context, 'boot_dt', now_dt)).total_seconds() < StrategyConfig.BOOT.GRACE_SECONDS
    allow_tickless = boot_grace or is_auction_time()

    base = state['base_price']
    unit, buy_sp, sell_sp = state['grid_unit'], state['buy_grid_spacing'], state['sell_grid_spacing']
    buy_p, sell_p = round(base * (1 - buy_sp), 3), round(base * (1 + sell_sp), 3)
    
    if not is_valid_price(buy_p) or not is_valid_price(sell_p): return

    position = get_position(symbol)
    pos = position.amount 
    price = context.latest_data.get(symbol)
    ratchet_enabled = (not allow_tickless) and is_valid_price(price)

    if ratchet_enabled:
        if abs(price / base - 1) <= 0.10:
            is_in_low_pos_range   = (pos - unit <= state['base_position'])
            is_in_high_pos_range = (pos + unit >= state['max_position'])
            sell_p_curr = round(base * (1 + sell_sp), 3)
            buy_p_curr  = round(base * (1 - buy_sp), 3)
            ratchet_up   = is_in_low_pos_range  and price >= sell_p_curr
            ratchet_down = is_in_high_pos_range and price <= buy_p_curr
            
            if ratchet_up:
                info('[{}] 🚀 棘轮上移(pos={}): 触及卖价，基准抬至 {:.3f}', dsym(context, symbol), pos, sell_p_curr)
                state['base_price'] = sell_p_curr
                cancel_all_orders_by_symbol(context, symbol)
                buy_p, sell_p = round(sell_p_curr * (1 - buy_sp), 3), round(sell_p_curr * (1 + sell_sp), 3)
            elif ratchet_down:
                info('[{}] 🚀 棘轮下移(pos={}): 触及买价，基准降至 {:.3f}', dsym(context, symbol), pos, buy_p_curr)
                state['base_price'] = buy_p_curr
                cancel_all_orders_by_symbol(context, symbol)
                buy_p, sell_p = round(buy_p_curr * (1 - buy_sp), 3), round(buy_p_curr * (1 + sell_sp), 3)

    if not ignore_cooldown:
        last_ts = state.get('_last_order_ts')
        if last_ts and (now_dt - last_ts).seconds < 30: return
        last_bp = state.get('_last_order_bp')
        if last_bp and abs(base / last_bp - 1) < buy_sp / 2: return
    
    state['_last_order_ts'], state['_last_order_bp'] = now_dt, base

    try:
        raw_open_orders = get_open_orders(symbol) or []
        open_orders = []
        ignore_set = set(ignore_entrust_nos) if ignore_entrust_nos else set()
        filled_ids = state.get('filled_order_ids', set())
        
        for o in raw_open_orders:
             if str(getattr(o, 'status', '')) == '2':
                 eid = getattr(o, 'entrust_no', None)
                 if eid and eid in ignore_set: continue
                 if eid and eid in filled_ids: continue
                 open_orders.append(o)
        
        same_buy = any(o.amount > 0 for o in open_orders)
        same_sell = any(o.amount < 0 for o in open_orders)

        enable_amount = position.enable_amount
        state['_oo_last'] = len(open_orders)
        state['_last_pos_seen'] = pos 

        can_buy = not same_buy
        if can_buy and pos + unit <= state['max_position']:
            info('[{}] --> 发起买入委托: {}股 @ {:.3f}', dsym(context, symbol), unit, buy_p)
            order(symbol, unit, limit_price=buy_p)

        can_sell = not same_sell
        pending_frozen = context.pending_frozen.get(symbol, 0)
        real_enable = enable_amount - pending_frozen
        
        if can_sell and real_enable >= unit and pos - unit >= state['base_position']:
            info('[{}] --> 发起卖出委托: {}股 @ {:.3f} (可用:{}, 冻结:{})', dsym(context, symbol), unit, sell_p, enable_amount, pending_frozen)
            order(symbol, -unit, limit_price=sell_p)
            context.pending_frozen[symbol] = pending_frozen + unit

    except Exception as e:
        info('[{}] ⚠️ 限价挂单异常：{}', dsym(context, symbol), e)
    finally:
        state.pop('_rehang_bypass_once', None)
        safe_save_state(symbol, state)

# ---------------- 成交回报与后续挂单 ----------------

def on_trade_response(context, trade_list):
    for tr in trade_list:
        if str(tr.get('status')) != '8': continue
        sym = convert_symbol_to_standard(tr['stock_code'])
        entrust_no = tr['entrust_no']
        
        log_trade_details(context, sym, tr) 
        if sym not in context.state or entrust_no in context.state[sym]['filled_order_ids']: continue

        amount = tr['business_amount'] if tr['entrust_bs']=='1' else -tr['business_amount']
        price  = tr['business_price']
        key = _make_fill_key(sym, amount, price, context.current_dt)
        if _is_dup_fill(context, key): continue
        _remember_fill(context, key)

        context.state[sym]['filled_order_ids'].add(entrust_no)
        safe_save_state(sym, context.state[sym])
        order_obj = SimpleNamespace(order_id = entrust_no, amount = amount, filled = abs(amount), price = price)
        try:
            on_order_filled(context, sym, order_obj)
        except Exception as e:
            info('[{}] ❌ 成交处理失败：{}', dsym(context, sym), e)

def on_order_filled(context, symbol, order):
    state = context.state[symbol]
    if order.filled == 0: return
    
    if order.amount < 0:
        current_frozen = context.pending_frozen.get(symbol, 0)
        context.pending_frozen[symbol] = max(0, current_frozen - abs(order.filled))

    last_dt = state.get('_last_fill_dt')
    last_price = state.get('last_fill_price', 0)
    time_diff = (context.current_dt - last_dt).total_seconds() if last_dt else 999
    
    if last_dt and time_diff < 10 and last_price > 0:
        if abs(order.price - last_price) / last_price < 0.001: return

    trade_direction = "买入" if order.amount > 0 else "卖出"
    info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f}', dsym(context, symbol), trade_direction, order.filled, order.price)

    state['_last_trade_ts'] = context.current_dt
    state['_last_fill_dt'] = context.current_dt
    state['last_fill_price'] = order.price
    state['base_price'] = order.price

    cancelled_ids = cancel_all_orders_by_symbol(context, symbol)
    if cancelled_ids: state['_pending_ignore_ids'] = list(cancelled_ids)

    delay_s = StrategyConfig.DEBUG.DELAY_AFTER_CANCEL
    state['_rehang_due_ts'] = datetime.now() + timedelta(seconds=max(delay_s, 2.0))
    info('[{}] ⏳ 设置补单计划，将在 {} 后触发', dsym(context, symbol), state['_rehang_due_ts'].strftime('%H:%M:%S'))

    context.mark_halted[symbol] = False
    context.last_valid_price[symbol] = order.price
    context.latest_data[symbol] = order.price
    context.last_valid_ts[symbol] = context.current_dt

    state.pop('_last_order_ts', None)
    state.pop('_last_order_bp', None)
    context.should_place_order_map[symbol] = True
    
    try:
         state['_last_pos_seen'] = get_position(symbol).amount
    except:
         state['_last_pos_seen'] = None
         
    state['_oo_last'] = len([o for o in (get_open_orders(symbol) or []) if getattr(o, 'status', None) == '2'])
    state['_oo_drop_seen_ts'] = None
    state['_pos_jump_seen_ts'] = None
    state['_pos_confirm_deadline'] = None
    safe_save_state(symbol, state)

# ---------------- FILL-RECOVER ----------------

def _fill_recover_watch(context, symbol, state):
    now_dt = context.current_dt
    in_window = False
    
    if _in_reopen_window(now_dt.time()): in_window = True
    if state.get('_after_cancel_until') and now_dt <= state['_after_cancel_until']: in_window = True
    if state.get('_recover_until') and now_dt <= state['_recover_until']: in_window = True

    if not in_window:
        if state.get('_last_pos_seen') is None:
            try: state['_last_pos_seen'] = get_position(symbol).amount
            except: pass
        if state.get('_oo_drop_seen_ts') or state.get('_pos_jump_seen_ts'):
             state['_oo_drop_seen_ts'] = None
             state['_pos_jump_seen_ts'] = None
             state['_pos_confirm_deadline'] = None
        return

    try:
        oo = [o for o in (get_open_orders(symbol) or []) if getattr(o, 'status', None) == '2']
        oo_n = len(oo)
        pos_now = get_position(symbol).amount
    except Exception as e:
        return

    if state.get('_last_pos_seen') is None: state['_last_pos_seen'] = pos_now

    pos_delta = pos_now - state['_last_pos_seen']
    unit = max(1, int(state.get('grid_unit', 100)))
    
    oo_drop_now = (state.get('_oo_last', 0) > 0 and oo_n == 0)
    pos_jump_now = (abs(pos_delta) >= unit)
    
    if oo_drop_now and not pos_jump_now:
        if state.get('_oo_drop_seen_ts') is None:
            state['_oo_drop_seen_ts'] = now_dt
            state['_pos_confirm_deadline'] = now_dt + timedelta(seconds=2.0)
            info('[{}]     观察到订单簿掉单(无持仓跳变)，进入2s确认期', dsym(context, symbol))
        state['_pos_jump_seen_ts'] = None 
    
    elif pos_jump_now and not oo_drop_now:
        if state.get('_pos_jump_seen_ts') is None:
            state['_pos_jump_seen_ts'] = now_dt
            state['_pos_confirm_deadline'] = now_dt + timedelta(seconds=2.0)
            info('[{}]     观察到持仓跳变 posΔ={}(无掉单)，进入2s确认期', dsym(context, symbol), pos_delta)
        state['_oo_drop_seen_ts'] = None

    elif oo_drop_now and pos_jump_now:
        if state.get('_oo_drop_seen_ts') is None and state.get('_pos_jump_seen_ts') is None:
             state['_oo_drop_seen_ts'] = now_dt
             state['_pos_jump_seen_ts'] = now_dt
             state['_pos_confirm_deadline'] = now_dt + timedelta(seconds=2.0)
    
    else:
        if state.get('_oo_drop_seen_ts') or state.get('_pos_jump_seen_ts'):
            state['_oo_drop_seen_ts'] = None
            state['_pos_jump_seen_ts'] = None
            state['_pos_confirm_deadline'] = None
    
    deadline = state.get('_pos_confirm_deadline')
    if deadline is None or now_dt < deadline:
        state['_oo_last'] = oo_n
        state['_last_pos_seen'] = pos_now
        safe_save_state(symbol, state)
        return

    if state.get('_pos_jump_seen_ts') is not None:
        info('[{}] ✅ 持仓跳变(posΔ={})确认期结束，触发补偿', dsym(context, symbol), pos_delta)
        
        filled_qty = int(abs(pos_delta) // unit * unit)
        amount = filled_qty if pos_delta > 0 else -filled_qty
        price  = context.latest_data.get(symbol, state['base_price']) or state['base_price']
        key = _make_fill_key(symbol, amount, price, now_dt)
        
        if not _is_dup_fill(context, key):
            _remember_fill(context, key)
            synth = SimpleNamespace(order_id=f"SYN-{int(time.time())}", amount=amount, filled=abs(amount), price=price)
            try:
                on_order_filled(context, symbol, synth)
            except Exception as e:
                info('[{}] ❌ FILL-RECOVER 调用 on_order_filled 失败: {}', dsym(context, symbol), e)

    state['_oo_drop_seen_ts'] = None
    state['_pos_jump_seen_ts'] = None
    state['_pos_confirm_deadline'] = None
    state['_oo_last'] = oo_n
    state['_last_pos_seen'] = pos_now
    safe_save_state(symbol, state)

# ---------------- 主动巡检与修正 ----------------

def patrol_and_correct_orders(context, symbol, state):
    now_dt = context.current_dt
    if state.get('_last_trade_ts') and (now_dt - state['_last_trade_ts']).total_seconds() < 58: return
    if not (is_main_trading_time() and now_dt.time() < dtime(14, 55)): return 
    if context.mark_halted.get(symbol, False): return 
    if not is_valid_price(context.latest_data.get(symbol)): return 

    try:
        position = get_position(symbol)
        pos = position.amount 
        enable_amount = position.enable_amount
        open_orders = [o for o in (get_open_orders(symbol) or []) if getattr(o, 'status', None) == '2']

        base_pos = state['base_position']
        max_pos = state['max_position']
        unit = state['grid_unit']
        base_price = state['base_price']
        buy_p = round(base_price * (1 - state['buy_grid_spacing']), 3)
        sell_p = round(base_price * (1 + state['sell_grid_spacing']), 3)

        should_have_buy_order = (pos + unit <= max_pos)
        should_have_sell_order = (enable_amount >= unit and pos - unit >= base_pos)

        orders_to_cancel = []
        valid_buy_orders = []
        valid_sell_orders = []

        for o in open_orders:
            order_info = OrderUtils.normalize(o)
            entrust_no = order_info['entrust_no']
            o_price = order_info['price']
            if not entrust_no: continue
            
            is_wrong = False
            if not OrderUtils.is_sell(order_info): 
                if not should_have_buy_order: is_wrong = True 
                elif abs(o_price - buy_p) / (buy_p + 1e-9) >= 0.002: is_wrong = True 
                else: valid_buy_orders.append(o)
            else: 
                if not should_have_sell_order: is_wrong = True 
                elif abs(o_price - sell_p) / (sell_p + 1e-9) >= 0.002: is_wrong = True 
                else: valid_sell_orders.append(o)
            
            if is_wrong: orders_to_cancel.append(o)
        
        if len(valid_buy_orders) > 1:
            for o in valid_buy_orders[1:]: orders_to_cancel.append(o)
            valid_buy_orders = valid_buy_orders[:1]
        if len(valid_sell_orders) > 1:
            for o in valid_sell_orders[1:]: orders_to_cancel.append(o)
            valid_sell_orders = valid_sell_orders[:1]

        if orders_to_cancel:
            info('[{}] 🕵️ PATROL: 发现 {} 笔错误/重复挂单，正在撤销...', dsym(context, symbol), len(orders_to_cancel))
            state['_ignore_place_until'] = datetime.now() + timedelta(seconds=10)
            safe_save_state(symbol, state)
            
            if not hasattr(context, 'canceled_cache'):
                context.canceled_cache = {'date': None, 'orders': set()}
            if context.canceled_cache.get('date') != context.current_dt.date():
                context.canceled_cache = {'date': context.current_dt.date(), 'orders': set()}
            
            cancelled_ids = set()
            for o in orders_to_cancel:
                try:
                    order_info = OrderUtils.normalize(o)
                    entrust_no = order_info['entrust_no']
                    raw_sym = order_info['raw_symbol']
                    if entrust_no and raw_sym:
                        cancel_order_ex({'entrust_no': entrust_no, 'symbol': raw_sym})
                        cancelled_ids.add(entrust_no)
                        context.canceled_cache['orders'].add(entrust_no)
                except Exception as e:
                    pass
            
            state.pop('_last_order_ts', None)
            state.pop('_last_order_bp', None)
            place_limit_orders(context, symbol, state, ignore_cooldown=True, bypass_lock=True, ignore_entrust_nos=cancelled_ids)
            return 

        has_correct_buy_order = (len(valid_buy_orders) > 0)
        has_correct_sell_order = (len(valid_sell_orders) > 0)

        if (should_have_buy_order and not has_correct_buy_order) or \
           (should_have_sell_order and not has_correct_sell_order):
            info('[{}] 🕵️ PATROL: 发现缺失订单，准备补挂...', dsym(context, symbol))
            place_limit_orders(context, symbol, state, ignore_cooldown=True)

    except Exception as e:
        info('[{}] ⚠️ PATROL 巡检失败: {}', dsym(context, symbol), e)

# ---------------- 行情主循环 ----------------

def handle_data(context, data):
    now_dt = context.current_dt
    now = now_dt.time()

    _fetch_quotes_via_snapshot(context)
    
    if now_dt.minute % 5 == 0:
        last_update = getattr(context, 'last_report_time', None)
        if last_update is None or last_update.minute != now_dt.minute:
            try:
                reload_config_if_changed(context)
                _calculate_intraday_metrics(context)
                generate_html_report(context)
                context.last_report_time = now_dt
            except Exception as e:
                pass

    boot_grace = (now_dt - getattr(context, 'boot_dt', now_dt)).total_seconds() < StrategyConfig.BOOT.GRACE_SECONDS
    if not boot_grace:
        def _phase_start(now_t: dtime):
            if dtime(9, 15) <= now_t < dtime(9, 25): return dtime(9, 15)
            if dtime(9, 30) <= now_t <= dtime(11, 30): return dtime(9, 30)
            if dtime(13, 0) <= now_t <= dtime(15, 0): return dtime(13, 0)
            return None

        phase_start_t = _phase_start(now)
        if phase_start_t:
            phase_start_dt = datetime.combine(now_dt.date(), phase_start_t)
            grace_seconds = 120
            for sym in context.symbol_list:
                if sym not in context.state: continue
                
                was_halted = context.mark_halted.get(sym, False)
                last_ts = context.last_valid_ts.get(sym)
                is_now_halted = False
                if last_ts is None or last_ts < phase_start_dt:
                    is_now_halted = (now_dt >= phase_start_dt + timedelta(seconds=grace_seconds))
                else:
                    is_now_halted = ((now_dt - last_ts).total_seconds() > grace_seconds)
                
                context.mark_halted[sym] = is_now_halted

                if was_halted and not is_now_halted:
                    state = context.state[sym]
                    recover_window_seconds = 180 
                    state['_recover_until'] = now_dt + timedelta(seconds=recover_window_seconds)
                    info('[{}]     监测到复牌/行情恢复，开启 {}s 补偿成交检测窗口。', dsym(context, sym), recover_window_seconds)

    for sym in context.symbol_list:
        if sym not in context.state: continue
        st = context.state[sym]
        price = context.latest_data.get(sym)
        if is_valid_price(price):
            get_target_base_position(context, sym, st, price, now_dt)
            adjust_grid_unit(st)
            if now_dt.minute % 30 == 0 and now_dt.second < 5:
                update_grid_spacing_final(context, sym, st, get_position(sym).amount)

    is_patrol_time = (now_dt.minute % 30 == 0 and now_dt.second < 5)
    
    if not is_patrol_time and (is_auction_time() or (is_main_trading_time() and now < dtime(14, 55))):
        for sym in context.symbol_list:
            if sym in context.state:
                place_limit_orders(context, sym, context.state[sym], ignore_cooldown=False)

    for sym in context.symbol_list:
        st = context.state.get(sym)
        if not st: continue
        _fill_recover_watch(context, sym, st)

    if is_patrol_time:
        info('🧐 每30分钟状态巡检...')
        for sym in context.symbol_list:
            if sym in context.state:
                patrol_and_correct_orders(context, sym, context.state[sym])
                log_status(context, sym, context.state[sym], context.latest_data.get(sym))

# ---------------- 日内RV计算 ----------------

def _calculate_intraday_metrics(context):
    if not is_main_trading_time() and not is_auction_time(): return
    metrics = {}
    today_date = context.current_dt.date()
    
    for sym in context.symbol_list:
        try:
            hist = get_history(250, '1m', ['close'], security_list=[sym], include=True)
            df = hist.get(sym) if isinstance(hist, dict) else hist
            if df is None or df.empty: continue
                
            today_df = df[df.index.date == today_date].copy()
            if len(today_df) < 2: continue
                
            close_series = today_df['close']
            log_rets = np.log(close_series / close_series.shift(1))
            rv = log_rets.abs().sum()
            
            open_price = close_series.iloc[0]
            curr_price = close_series.iloc[-1]
            daily_return = (curr_price - open_price) / open_price
            efficiency = rv / max(abs(daily_return), 0.0001)
            
            metrics[sym] = {'rv': rv, 'efficiency': efficiency, 'daily_return': daily_return}
        except Exception as e:
            pass
    context.intraday_metrics = metrics

# ---------------- 监控输出 ----------------

def log_status(context, symbol, state, price):
    disp_price = context.last_valid_price.get(symbol, state['base_price'])
    if not is_valid_price(disp_price): return
    position = get_position(symbol)
    pos = position.amount
    pnl = (disp_price - position.cost_basis) * pos if position.cost_basis > 0 else 0
    info("📊 [{}] 状态: 价:{:.3f} 持仓:{}(可卖:{}) / 底仓:{} 成本:{:.3f} 盈亏:{:.2f} 网格:[买{:.2%},卖{:.2%}]",
         dsym(context, symbol), disp_price, pos, position.enable_amount, state['base_position'], position.cost_basis, pnl, state['buy_grid_spacing'], state['sell_grid_spacing'])

# ---------------- 动态网格间距 (Robust EMA + Cache + 10% Filter) ----------------

def calculate_atr(context, symbol, atr_period=14):
    """
    计算 ATR (EMA算法)，支持历史记忆与降级。
    """
    state = context.state[symbol]
    
    try:
        # 1. 尝试获取历史数据 (只要 > 1天即可计算)
        hist = get_history(atr_period + 5, '1d', ['high', 'low', 'close'], security_list=[symbol])
        df = hist.get(symbol) if isinstance(hist, dict) else hist
        
        current_atr_rate = None
        
        # 2. 如果数据有效且长度足够
        if df is not None and not df.empty and len(df) > 1:
            high = df['high']
            low = df['low']
            close = df['close']
            
            # 计算 TR (True Range)
            # TR = max(H-L, |H-Cp|, |L-Cp|)
            tr1 = high - low
            tr2 = (high - close.shift(1)).abs()
            tr3 = (low - close.shift(1)).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
            # 计算 EMA ATR
            atr_series = tr.ewm(span=atr_period, adjust=False).mean()
            last_atr_val = atr_series.iloc[-1]
            last_price = close.iloc[-1]
            
            # 存入原始计算值到缓存
            state['cached_atr_ema'] = float(last_atr_val)
            
            if is_valid_price(last_price):
                current_atr_rate = last_atr_val / last_price

    except Exception as e:
        if StrategyConfig.DEBUG.ENABLE:
            info('[{}] ATR计算异常: {} (将尝试使用缓存)', dsym(context, symbol), e)
    
    # 3. 决策逻辑：更新或维持
    used_rate = state.get('used_atr_rate')
    
    # 如果本次计算成功
    if current_atr_rate is not None and current_atr_rate > 0:
        # 首次初始化，或变化幅度超过 10%
        if used_rate is None or abs(current_atr_rate - used_rate) / used_rate > 0.10:
            state['used_atr_rate'] = current_atr_rate
            # 仅在发生变化时打印日志
            # info('[{}] ATR更新: {:.2%} -> {:.2%}', dsym(context, symbol), (used_rate or 0), current_atr_rate)
        return state['used_atr_rate']
    
    # 4. 如果计算失败（数据不足/报错），尝试使用缓存
    if used_rate is not None:
        return used_rate
        
    # 5. 彻底失败
    return None

def update_grid_spacing_final(context, symbol, state, curr_pos):
    pos = curr_pos 
    unit, base_pos = state['grid_unit'], state['base_position']
    
    # 调用新的 Robust ATR
    atr_pct = calculate_atr(context, symbol)
    
    base_spacing = 0.005
    if atr_pct is not None and not math.isnan(atr_pct):
        atr_multiplier = 0.25
        base_spacing = atr_pct * atr_multiplier
        
    min_spacing = StrategyConfig.TRANSACTION_COST * 5
    base_spacing = max(base_spacing, min_spacing)
    
    thresh_low = 5
    thresh_high = 15
    
    if pos <= base_pos + unit * thresh_low:
        new_buy, new_sell = base_spacing, base_spacing * 2
    elif pos > base_pos + unit * thresh_high:
        new_buy, new_sell = base_spacing * 2, base_spacing
    else:
        new_buy, new_sell = base_spacing, base_spacing
        
    max_spacing = 0.03
    new_buy  = round(min(new_buy,  max_spacing), 4)
    new_sell = round(min(new_sell, max_spacing), 4)
    
    if new_buy != state.get('buy_grid_spacing') or new_sell != state.get('sell_grid_spacing'):
        state['buy_grid_spacing'], state['sell_grid_spacing'] = new_buy, new_sell
        info('[{}] 网格动态调整 (ATR={:.2%}) -> [买{:.2%},卖{:.2%}]', 
             dsym(context, symbol), (atr_pct or 0.0), new_buy, new_sell)

# ---------------- 日终处理 ----------------

def end_of_day(context):
    info('✅ 日终处理 (Start @ 14:55) [GLOBAL BATCH CANCEL]')
    _fast_cancel_all_orders_global(context)
    for sym in context.symbol_list:
        if sym in context.state: safe_save_state(sym, context.state[sym])
    info('✅ 日终作业完成，PnL计算已推迟至盘后。')

# ---------------- VA & Tools ----------------

def get_target_base_position(context, symbol, state, price, dt):
    try:
        weeks = get_trade_weeks(context, symbol, state, dt)
        accumulated_investment = sum(state['dingtou_base'] * (1 + state['dingtou_rate'])**w for w in range(1, weeks + 1))
        target_val = state['initial_position_value'] + accumulated_investment
        current_val = state['base_position'] * price
        
        surplus = current_val - target_val
        grid_value = state['grid_unit'] * price
        
        if surplus >= StrategyConfig.VA.THRESHOLD_K * grid_value:
            release_amt = state['grid_unit']
            if state['base_position'] - release_amt >= state['initial_base_position'] * 0.5:
                state['base_position'] -= release_amt
                info('[{}] 💰 VA底仓盈余释放: 减少 {} 股', dsym(context, symbol), release_amt)

        delta_val = target_val - (state['last_week_position'] * price)
        if delta_val > 0:
            delta_pos = math.ceil(delta_val / price / 100) * 100
            new_pos = state['last_week_position'] + delta_pos
            min_base = round(state['initial_position_value'] / state['base_price'] / 100) * 100
            final_pos = round(max(min_base, new_pos) / 100) * 100
            
            if final_pos > state['base_position']:
                info('[{}] 📈 VA价值平均加仓: 底仓增加至 {}', dsym(context, symbol), final_pos)
                state['base_position'] = final_pos

        state['max_position'] = state['base_position'] + state['grid_unit'] * 20
    except Exception as e:
        pass
    return state['base_position']

def get_trade_weeks(context, symbol, state, dt):
    y, w, _ = dt.date().isocalendar()
    key = f"{y}_{w}"
    if 'trade_week_set' not in state: state['trade_week_set'] = set()
    if key not in state['trade_week_set']:
        state['trade_week_set'].add(key)
        state['last_week_position'] = state['base_position']
        safe_save_state(symbol, state)
    return len(state['trade_week_set'])

def adjust_grid_unit(state):
    if state['base_position'] > state['grid_unit'] * 20:
        theoretical_unit = math.ceil(state['base_position'] / 20 / 100) * 100
        price = state.get('base_price', 1.0)
        capped_unit_val = math.floor(StrategyConfig.MAX_TRADE_AMOUNT / price / 100) * 100
        capped_unit = max(100, capped_unit_val)
        
        new_unit = min(theoretical_unit, capped_unit)
        if new_unit > state['grid_unit']:
            state['grid_unit'] = new_unit
            info(f"[{state.get('symbol')}] 🔧 网格单位放大至 {new_unit}")
        state['max_position'] = state['base_position'] + state['grid_unit'] * 20

def _load_pnl_metrics(path):
    if path.exists(): return json.loads(path.read_text(encoding='utf-8'))
    return {}

def _save_pnl_metrics(context):
    if hasattr(context, 'pnl_metrics_path'):
        context.pnl_metrics_path.write_text(json.dumps(context.pnl_metrics, indent=2), encoding='utf-8')

def _calculate_local_pnl_lifo(context):
    info('🧮 启动本地 PnL 引擎 (LIFO)...')
    trade_log_path = research_path('reports', 'a_trade_details.csv')
    if not trade_log_path.exists(): return

    trades = []
    try:
        with open(trade_log_path, 'r', encoding='utf-8') as f:
            headers = f.readline().strip().split(',')
            for line in f:
                parts = line.strip().split(',')
                if len(parts) < 6: continue
                try: 
                    trades.append({
                        'time': parts[0],
                        'symbol': parts[1],
                        'qty': float(parts[3]),
                        'price': float(parts[4]),
                        'base_pos_at_trade': int(parts[5]) if parts[5].isdigit() else 0
                    })
                except: pass
    except: return

    trades.sort(key=lambda x: x['time'])
    pnl_metrics = getattr(context, 'pnl_metrics', {})
    
    for sym in context.symbol_list:
        if sym not in context.state: continue
        state = context.state[sym]
        initial_pos = state.get('initial_base_position', 0)
        initial_cost = state.get('base_price', 0)
        
        inventory = [] 
        if initial_pos > 0: inventory.append([initial_pos, initial_cost, 'base'])
        current_holding = initial_pos
        grid_pnl, base_pnl = 0.0, 0.0
        
        sym_trades = [t for t in trades if t['symbol'] == sym]
        for t in sym_trades:
            qty, price = t['qty'], t['price']
            target_base = t['base_pos_at_trade'] if t['base_pos_at_trade'] > 0 else state.get('base_position', 0)
            
            if qty > 0:
                remaining_buy = qty
                if current_holding < target_base:
                    needed = target_base - current_holding
                    fill_amt = min(remaining_buy, needed)
                    inventory.append([fill_amt, price, 'base'])
                    current_holding += fill_amt
                    remaining_buy -= fill_amt
                if remaining_buy > 0:
                    inventory.append([remaining_buy, price, 'grid'])
                    current_holding += remaining_buy
            elif qty < 0:
                sell_qty = abs(qty)
                current_holding -= sell_qty
                while sell_qty > 0.001 and inventory:
                    lot = inventory[-1]
                    matched = min(sell_qty, lot[0])
                    profit = (price - lot[1]) * matched
                    if lot[2] == 'base': base_pnl += profit
                    else: grid_pnl += profit
                    sell_qty -= matched
                    lot[0] -= matched
                    if lot[0] <= 0.001: inventory.pop()
        
        if sym not in pnl_metrics: pnl_metrics[sym] = {}
        pnl_metrics[sym]['realized_grid_pnl'] = grid_pnl
        pnl_metrics[sym]['realized_base_pnl'] = base_pnl
        pnl_metrics[sym]['total_realized_pnl'] = grid_pnl + base_pnl

    context.pnl_metrics = pnl_metrics
    _save_pnl_metrics(context)

def after_trading_end(context, data):
    if '回测' in context.env: return
    info('🏁 盘后作业开始...')
    try: _calculate_local_pnl_lifo(context)
    except: pass
    try:
        update_daily_reports(context, data)
        generate_html_report(context)
    except: pass
    info('✅ 盘后作业结束')

def reload_config_if_changed(context):
    try:
        current_mod_time = context.config_file_path.stat().st_mtime
        if current_mod_time == context.last_config_mod_time: return

        info('♻️ 检测到配置文件发生变更，开始热重载...')
        context.last_config_mod_time = current_mod_time
        new_config = json.loads(context.config_file_path.read_text(encoding='utf-8'))
        
        StrategyConfig.load(context) # 重新加载参数配置

        old_symbols = set(context.symbol_list)
        new_symbols = set(new_config.keys())

        for sym in old_symbols - new_symbols:
            cancel_all_orders_by_symbol(context, sym)
            context.symbol_list.remove(sym)
            if sym in context.state: del context.state[sym]
            if sym in context.latest_data: del context.latest_data[sym]
            context.mark_halted.pop(sym, None)

        for sym in new_symbols - old_symbols:
            cfg = new_config[sym]
            state_file = research_path('state', f'{sym}.json')
            saved = {}
            if state_file.exists():
                try: saved = json.loads(state_file.read_text(encoding='utf-8'))
                except: pass
            
            st = {**cfg}
            st.update({
                'base_price': saved.get('base_price', cfg['base_price']),
                'grid_unit': saved.get('grid_unit', cfg['grid_unit']),
                'filled_order_ids': set(saved.get('filled_order_ids', [])),
                'trade_week_set': set(saved.get('trade_week_set', [])),
                'base_position': saved.get('base_position', cfg['initial_base_position']),
                'last_week_position': saved.get('last_week_position', cfg['initial_base_position']),
                'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
                'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,
                'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20),
                'used_atr_rate': saved.get('used_atr_rate', None),
                'cached_atr_ema': saved.get('cached_atr_ema', None),
                '_pending_ignore_ids': []
            })
            if 'scale_factor' in st: st.pop('scale_factor')
                
            context.state[sym] = st
            context.latest_data[sym] = st['base_price']
            context.symbol_list.append(sym)
            context.mark_halted[sym] = False
            context.last_valid_price[sym] = st['base_price']
            context.last_valid_ts[sym] = None
            context.pending_frozen[sym] = 0
            context.should_place_order_map[sym] = True

        for sym in old_symbols.intersection(new_symbols):
            if context.symbol_config[sym] != new_config[sym]:
                state, new_params = context.state[sym], new_config[sym]
                state.update({
                    'grid_unit': new_params['grid_unit'],
                    'dingtou_base': new_params['dingtou_base'],
                    'dingtou_rate': new_params['dingtou_rate'],
                    'max_position': state['base_position'] + new_params['grid_unit'] * 20
                })

        context.symbol_config = new_config
        _load_symbol_names(context)
        info('✅ 配置文件热重载完成！')

    except Exception as e:
        info(f'❌ 配置文件热重载失败: {e}')

def log_trade_details(context, symbol, trade):
    try:
        trade_log_path = research_path('reports', 'a_trade_details.csv')
        is_new = not trade_log_path.exists()
        entrust_no = trade.get('entrust_no', 'N/A') 
        with open(trade_log_path, 'a', encoding='utf-8', newline='') as f:
            if is_new:
                headers = ["time", "symbol", "direction", "quantity", "price", "base_position_at_trade", "entrust_no"]
                f.write(",".join(headers) + "\n")
            direction = "BUY" if trade['entrust_bs'] == '1' else "SELL"
            base_position = context.state[symbol].get('base_position', 0)
            row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), symbol, direction, str(trade['business_amount']), f"{trade['business_price']:.3f}", str(base_position), entrust_no]
            f.write(",".join(row) + "\n")
    except: pass

def update_daily_reports(context, data):
    reports_dir = research_path('reports')
    reports_dir.mkdir(parents=True, exist_ok=True)
    current_date = context.current_dt.strftime("%Y-%m-%d")
    for symbol in context.symbol_list:
        report_file = reports_dir / f"{symbol}.csv"
        state = context.state[symbol]
        position = get_position(symbol)
        amount = position.amount
        cost_basis = getattr(position, 'cost_basis', state['base_price'])
        close_price = context.last_valid_price.get(symbol, state['base_price'])
        if not is_valid_price(close_price): close_price = state['base_price']
        
        weeks = len(state.get('trade_week_set', []))
        d_base = state['dingtou_base']
        d_rate = state['dingtou_rate']
        invest_should = d_base
        invest_actual = d_base * (1 + d_rate) ** weeks
        cumulative_invest = sum(d_base * (1 + d_rate) ** w for w in range(1, weeks+1))
        expected_value = state['initial_position_value'] + d_base * weeks
        last_week_val = state.get('last_week_position', 0) * close_price
        current_val = amount * close_price
        weekly_return = (current_val - last_week_val) / last_week_val if last_week_val>0 else 0.0
        total_return = (current_val - cumulative_invest) / cumulative_invest if cumulative_invest>0 else 0.0
        weekly_bottom_profit = (state['base_position'] - state.get('last_week_position', 0)) * close_price
        total_bottom_profit = state['base_position'] * close_price - state['initial_position_value']
        
        row = [
            current_date, f"{close_price:.3f}", str(weeks), str(weeks),
            f"{weekly_return:.2%}", f"{total_return:.2%}", f"{expected_value:.2f}",
            f"{invest_should:.0f}", f"{invest_actual:.0f}", f"{cumulative_invest:.0f}",
            str(state['initial_base_position']), str(state['base_position']),
            f"{state['base_position'] * close_price:.0f}", f"{weekly_bottom_profit:.0f}",
            f"{total_bottom_profit:.0f}", str(state['base_position']), str(amount),
            str(state['grid_unit']), str(max(0, amount - state['base_position'])), 
            str(state['base_position'] + state['grid_unit'] * 5),
            str(state['base_position'] + state['grid_unit'] * 15), str(state['max_position']), f"{cost_basis:.3f}",
            f"{(state['base_position'] - state.get('last_week_position', 0)) * close_price:.3f}", f"{(close_price - cost_basis) * amount:.0f}"
        ]
        is_new = not report_file.exists()
        with open(report_file, 'a', encoding='utf-8', newline='') as f:
            if is_new:
                f.write(",".join(["时间","市价","期数","次数","每期总收益率","盈亏比","应到价值","当周应投入金额","当周实际投入金额","实际累计投入金额","定投底仓份额","累计底仓份额","累计底仓价值","每期累计底仓盈利","总累计底仓盈利","底仓","股票余额","单次网格交易数量","可T数量","标准数量","中间数量","极限数量","成本价","对比定投成本","盈亏"]) + "\n")
            f.write(",".join(map(str, row)) + "\n")
        info('✅ [{}] 已更新每日CSV报表', dsym(context, symbol))

def generate_html_report(context):
    all_metrics = []
    total_market_value = total_unrealized_pnl = total_realized_pnl = 0
    pnl_metrics = getattr(context, 'pnl_metrics', {})
    intraday_metrics = getattr(context, 'intraday_metrics', {})

    for symbol in context.symbol_list:
        if symbol not in context.state: continue
        state = context.state[symbol]
        position = get_position(symbol)
        price = context.last_valid_price.get(symbol, state['base_price'])
        if not is_valid_price(price): price = state['base_price']
                
        market_value = position.amount * price
        unrealized_pnl = (price - position.cost_basis) * position.amount if position.cost_basis > 0 else 0
        total_market_value += market_value
        total_unrealized_pnl += unrealized_pnl
        
        # 显示 ATR (缓存的 EMA 值)
        atr_pct = state.get('used_atr_rate')
        
        sym_pnl = pnl_metrics.get(symbol, {})
        real_grid = sym_pnl.get('realized_grid_pnl', 0)
        real_base = sym_pnl.get('realized_base_pnl', 0)
        total_real = sym_pnl.get('total_realized_pnl', 0)
        total_realized_pnl += total_real
        
        rv_data = intraday_metrics.get(symbol, {})
        
        all_metrics.append({
            "symbol": symbol,
            "symbol_disp": dsym(context, symbol, style='long'),
            "position": f"{position.amount} ({position.enable_amount})",
            "cost_basis": f"{position.cost_basis:.3f}",
            "price": f"{price:.3f}",
            "market_value": f"{market_value:,.2f}",
            "unrealized_pnl": f"{unrealized_pnl:,.2f}",
            "realized_grid_pnl": f"{real_grid:,.2f}",
            "realized_base_pnl": f"{real_base:,.2f}",
            "total_realized_pnl": f"{total_real:,.2f}",
            "total_pnl": f"{(total_real + unrealized_pnl):,.2f}",
            "pnl_ratio": f"{(unrealized_pnl / (position.cost_basis * position.amount) * 100) if position.cost_basis * position.amount != 0 else 0:.2f}%",
            "base_position": state['base_position'],
            "grid_unit": state['grid_unit'],
            "atr_str": f"{atr_pct:.2%}" if atr_pct else "N/A",
            "rv_str": f"{rv_data.get('rv', 0):.2%}",
            "efficiency_str": f"{rv_data.get('efficiency', 0):.1f}"
        })
        
    try:
        template_file = research_path('config', 'dashboard_template.html')
        html_template = template_file.read_text(encoding='utf-8') if template_file.exists() else "<html><body><h1>Dashboard</h1></body></html>"
    except:
        html_template = "<html><body><h1>Error</h1></body></html>"

    table_rows = ""
    for m in all_metrics:
        table_rows += f"""
        <tr>
            <td>{m['symbol_disp']}</td><td>{m['position']}</td><td>{m['cost_basis']}</td><td>{m['price']}</td><td>{m['market_value']}</td>
            <td class="{'positive' if float(m['unrealized_pnl'].replace(',',''))>=0 else 'negative'}">{m['unrealized_pnl']}</td>
            <td class="{'positive' if float(m['unrealized_pnl'].replace(',',''))>=0 else 'negative'}">{m['pnl_ratio']}</td>
            <td class="{'positive' if float(m['realized_grid_pnl'].replace(',',''))>0 else ''}">{m['realized_grid_pnl']}</td>
            <td>{m['realized_base_pnl']}</td><td class="{'positive' if float(m['total_realized_pnl'].replace(',',''))>0 else ''}">{m['total_realized_pnl']}</td>
            <td class="{'positive' if float(m['total_pnl'].replace(',',''))>=0 else 'negative'}">{m['total_pnl']}</td>
            <td>{m['base_position']}</td><td>{m['grid_unit']}</td><td>{m['atr_str']}</td><td>{m['rv_str']}</td><td>{m['efficiency_str']}</td>
        </tr>"""

    try:
        final_html = html_template.format(
            update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total_market_value=f"{total_market_value:,.2f}",
            total_unrealized_pnl=f"{total_unrealized_pnl:,.2f}",
            unrealized_pnl_class="positive" if total_unrealized_pnl >= 0 else "negative",
            total_realized_pnl=f"{total_realized_pnl:,.2f}",
            realized_pnl_class="positive" if total_realized_pnl >= 0 else "negative",
            account_total_pnl=f"{(total_realized_pnl + total_unrealized_pnl):,.2f}",
            total_pnl_class="positive" if (total_realized_pnl + total_unrealized_pnl) >= 0 else "negative",
            total_realized_grid_pnl="0.00", grid_pnl_class="", total_realized_base_pnl="0.00", base_pnl_class="", # Placeholder
            table_rows=table_rows
        )
        research_path('reports', 'strategy_dashboard.html').write_text(final_html, encoding='utf-8')
    except: pass