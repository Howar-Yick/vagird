# event_driven_grid_strategy.py
# 版本号：GEMINI-3.13.5
#
# 更新日志 (v3.13.5):
# 1. 【彻底闭环】修复初始化时 initial_position_value 使用配置文件的旧 base_price 重新计算的 Bug。现在市值锚点完美从状态记忆中继承，彻底杜绝重启后 VA 引擎误判“虚假盈余”而导致的每分钟强制砍仓。
# 2. 【滴灌保护】修复热重载逻辑，移除对 dingtou_base 的覆盖，保护止盈后累积的滴灌现金池不被 config 文件重置。
#
# 更新日志 (v3.13.4):
# 1. 【架构升级】确立 state 文件在初始化时的绝对权威。系统优先从本地持久化文件加载 initial_base_position。
# 2. 【逻辑拨乱反正】止盈触发后直接同步更新 state 中的 initial_base_position。
#
# 更新日志 (v3.13.3):
# 1. 【自动化闭环】实现止盈锚点的“自我进化”。
#
# 更新日志 (v3.13.2):
# 1. 【安全加固】将 initial_base_position 纳入持久化白名单。
# 
# 更新日志 (v3.13.1):
# 1. 【滴灌重构】止盈回流机制升级。不再仅滴灌微薄的“纯利润”，而是将止盈卖出的“全部现金回报（本金+利润）”全量纳入滴灌池。
# 2. 【周期加固】适配总额滴灌，将 ATR 滴灌常数微调至 0.6。
#
# 更新日志 (v3.13.0):
# 1. 【逻辑重构】优化宏观止盈后的底仓重置机制。从原先的“归零重启”升级为“平衡位接力”。

import json
import logging
import math
import time
import heapq  # 引入堆队列算法
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
__version__ = 'GEMINI-3.13.5'

# ---------------- 配置管理类 ----------------

class StrategyConfig:
    """
    策略静态配置类：收拢所有硬编码参数，支持从文件动态加载覆盖。
    [v3.12.13 级联覆盖模式]：先读取底层分散 json，最后由 strategy.json 统一覆写防崩溃。
    """
    # --- 核心常量 ---
    MAX_SAVED_FILLED_IDS = 500
    TRANSACTION_COST = 0.00006  # 万分之六
    MAX_TRADE_AMOUNT = 5000     # 单笔网格交易最大金额（人民币）
    
    # --- 风控配置 ---
    CREDIT_LIMIT = 0            # 默认信用额度（0表示严格禁止亏损交易）

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

    # [v3.12.4 新增] 宏观止盈默认参数
    VA.TP_COOL_WEEKS = 4        
    VA.TP_MIN_WEEKS = 12        
    VA.TP_MIN_VALUE = 30000     

    # --- 市场/风控配置 ---
    MARKET = SimpleNamespace()
    MARKET.HALT_SKIP_PLACE = True
    MARKET.HALT_SKIP_AFTER_SEC = 180
    MARKET.HALT_LOG_EVERY_MIN = 10

    # [v3.8 新增] 天地锁破锁阈值 (ATR 的倍数)
    MARKET.UNLOCK_ATR_MULTIPLIER = 5.0
    
    # [v3.10 新增] 堆栈容量上限
    MARKET.MAX_STACK_SIZE = 5
    
    # --- 启动配置 ---
    BOOT = SimpleNamespace()
    BOOT.GRACE_SECONDS = 180

    @classmethod
    def load(cls, context):
        """
        加载所有配置文件并覆盖默认参数。
        """
        # 第一层：读取历史遗留的分散配置，返回是否发生了更新
        c1 = cls._load_debug_config(context)
        c2 = cls._load_va_config(context)
        c3 = cls._load_market_config(context)
        
        # 第二层：读取最高阶法典 strategy.json
        # 【核心修复】：只要底层任何一个文件变了，强迫 strategy.json 重新执行覆盖！
        cls._load_strategy_config(context, force=(c1 or c2 or c3))
        
        # 将关键参数注入到 context 以便兼容旧代码习惯
        context.delay_after_cancel_seconds = cls.DEBUG.DELAY_AFTER_CANCEL
        
    @classmethod
    def _load_debug_config(cls, context):
        cfg_file = research_path('config', 'debug.json')
        if not cls._check_mtime(context, 'debug_cfg_mtime', cfg_file): return False
        
        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if 'enable_debug_log' in j: cls.DEBUG.ENABLE = bool(j['enable_debug_log'])
            if 'rt_heartbeat_window_sec' in j: cls.DEBUG.RT_WINDOW_SEC = max(5, int(j['rt_heartbeat_window_sec']))
            if 'rt_heartbeat_preview' in j: cls.DEBUG.RT_PREVIEW = int(j['rt_heartbeat_preview']) # [补齐遗漏]
            if 'delay_after_cancel_seconds' in j: cls.DEBUG.DELAY_AFTER_CANCEL = max(0.0, float(j['delay_after_cancel_seconds']))
        except Exception: pass
        return True

    @classmethod
    def _load_va_config(cls, context):
        cfg_file = research_path('config', 'va.json')
        if not cls._check_mtime(context, 'va_cfg_mtime', cfg_file): return False

        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if 'value_threshold_k' in j: cls.VA.THRESHOLD_K = float(j['value_threshold_k'])
            if 'max_updates_per_day' in j: cls.VA.MAX_UPDATES_PER_DAY = int(j['max_updates_per_day'])
        except Exception: pass
        return True

    @classmethod
    def _load_market_config(cls, context):
        cfg_file = research_path('config', 'market.json')
        if not cls._check_mtime(context, 'market_cfg_mtime', cfg_file): return False

        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if 'halt_skip_place' in j: cls.MARKET.HALT_SKIP_PLACE = bool(j['halt_skip_place'])
            if 'halt_skip_after_seconds' in j: cls.MARKET.HALT_SKIP_AFTER_SEC = int(j['halt_skip_after_seconds'])
            if 'halt_log_every_minutes' in j: cls.MARKET.HALT_LOG_EVERY_MIN = int(j['halt_log_every_minutes']) # [补齐遗漏]
            if 'unlock_atr_multiplier' in j: cls.MARKET.UNLOCK_ATR_MULTIPLIER = float(j['unlock_atr_multiplier'])
            if 'max_stack_size' in j: cls.MARKET.MAX_STACK_SIZE = int(j['max_stack_size'])
        except Exception: pass
        return True

    @classmethod
    def _load_strategy_config(cls, context, force=False):
        cfg_file = research_path('config', 'strategy.json')
        changed = cls._check_mtime(context, 'strategy_cfg_mtime', cfg_file)
        
        # 如果自身没变，且底层也没变(force=False)，才安全退出
        if not changed and not force: return False

        try:
            if not cfg_file.exists(): return False
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            
            # 1. 覆盖 Debug 模块
            dbg = j.get('debug', {})
            if 'enable_debug_log' in dbg: cls.DEBUG.ENABLE = bool(dbg['enable_debug_log'])
            if 'rt_heartbeat_window_sec' in dbg: cls.DEBUG.RT_WINDOW_SEC = max(5, int(dbg['rt_heartbeat_window_sec']))
            if 'rt_heartbeat_preview' in dbg: cls.DEBUG.RT_PREVIEW = int(dbg['rt_heartbeat_preview'])
            if 'delay_after_cancel_seconds' in dbg: cls.DEBUG.DELAY_AFTER_CANCEL = max(0.0, float(dbg['delay_after_cancel_seconds']))

            # 2. 覆盖 VA 模块
            va = j.get('va', {})
            if 'value_threshold_k' in va: cls.VA.THRESHOLD_K = float(va['value_threshold_k'])
            if 'min_update_interval_minutes' in va: cls.VA.MIN_UPDATE_INTERVAL_MIN = int(va['min_update_interval_minutes'])
            if 'max_updates_per_day' in va: cls.VA.MAX_UPDATES_PER_DAY = int(va['max_updates_per_day'])

            # 3. 覆盖 Market 模块 (收编所有独立属性)
            mkt = j.get('market', {})
            if 'halt_skip_place' in mkt: cls.MARKET.HALT_SKIP_PLACE = bool(mkt['halt_skip_place'])
            if 'halt_skip_after_seconds' in mkt: cls.MARKET.HALT_SKIP_AFTER_SEC = int(mkt['halt_skip_after_seconds'])
            if 'halt_log_every_minutes' in mkt: cls.MARKET.HALT_LOG_EVERY_MIN = int(mkt['halt_log_every_minutes'])
            if 'unlock_atr_multiplier' in mkt: cls.MARKET.UNLOCK_ATR_MULTIPLIER = float(mkt['unlock_atr_multiplier'])
            if 'max_stack_size' in mkt: cls.MARKET.MAX_STACK_SIZE = int(mkt['max_stack_size'])

            # 4. 全局风控与其他
            if 'credit_limit' in j: cls.CREDIT_LIMIT = int(j['credit_limit'])
            
            info('⚙️ [Config] Strategy统一配置已完成全局覆盖加载')
        except Exception as e:
            if cls.DEBUG.ENABLE:
                info('⚠️ Strategy配置解析异常: {}', e)
        return True

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
    """
    [Global Ver: v3.13.5] [Func Ver: 2.7]
    """
    ids = list(state.get('filled_order_ids', set()))
    state['filled_order_ids'] = set(ids[-StrategyConfig.MAX_SAVED_FILLED_IDS:])
    
    # 确保保存新名分
    store_keys = ['symbol', 'base_price', 'grid_unit', 'max_position', 'last_week_position', 'base_position', 
                  'initial_base_position', 'initial_position_value',
                  'grid_atr_rate', 'macro_atr_rate', 'buy_stack', 'sell_stack', 'credit_limit', 
                  'history_pnl', '_fill_tracker', 
                  'dingtou_base', 'dingtou_rate', '_tp_hwm_ratio', '_tp_tier', '_macro_sell_ids',
                  'tp_cool_weeks', 'tp_min_weeks', 'tp_min_value', 'wm_map', 'wm_pnl'] 
    
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
        init_symbol_state(context, sym, cfg)

    context.boot_dt = getattr(context, 'current_dt', None) or datetime.now()
    context.last_report_time = None
    context.initial_cleanup_done = False
    
    StrategyConfig.load(context)
    _repair_state_logic(context)
    
    if '回测' not in context.env:
        run_daily(context, place_auction_orders, time='9:15')
        run_daily(context, end_of_day, time='14:55')
        run_interval(context, check_pending_rehangs, seconds=3)
        info('✅ 事件驱动模式就绪 (Async State Machine Active)')

    context.pnl_metrics_path = research_path('state', 'pnl_metrics.json')
    context.pnl_metrics = _load_pnl_metrics(context.pnl_metrics_path)
    
    info('✅ 初始化完成，版本:{}', __version__)

# ---------------- 初始化状态辅助函数 ----------------

def init_symbol_state(context, sym, cfg):
    """
    [Global Ver: v3.13.5] [Func Ver: 4.6]
    [Change]: 彻底修复重启初始化逻辑。从本地优先提取 initial_position_value 和 initial_base_position，不再被 cfg 中的原始配置污染。
    """
    state_file = research_path('state', f'{sym}.json')
    saved = json.loads(state_file.read_text(encoding='utf-8')) if state_file.exists() else get_saved_param(f'state_{sym}', {}) or {}
    
    st = {**cfg}
    
    # 🌟【彻底闭环核心】：事实优先级。如果本地有记录，无条件使用本地记录，杜绝重新从配置文件(cfg)计算！
    saved_initial_base = saved.get('initial_base_position')
    actual_initial_base = saved_initial_base if saved_initial_base is not None else cfg['initial_base_position']
    
    saved_initial_val = saved.get('initial_position_value')
    # 只有当 state 里真的没存过 initial_position_value 时，才用配置的 base_price 去推算
    actual_initial_val = saved_initial_val if saved_initial_val is not None else (actual_initial_base * cfg['base_price'])

    st.update({
        'symbol': sym, 
        'initial_base_position': actual_initial_base, # 确立事实起点
        'base_position': saved.get('base_position', actual_initial_base),
        'last_week_position': saved.get('last_week_position', actual_initial_base),
        'initial_position_value': actual_initial_val, # 使用继承的市值锚点
        
        'dingtou_base': saved.get('dingtou_base', cfg.get('dingtou_base', 0)),
        'dingtou_rate': saved.get('dingtou_rate', cfg.get('dingtou_rate', 0)),
        'base_price': saved.get('base_price', cfg['base_price']),
        'grid_unit': saved.get('grid_unit', cfg['grid_unit']),
        
        'tp_cool_weeks': cfg.get('tp_cool_weeks', saved.get('tp_cool_weeks', StrategyConfig.VA.TP_COOL_WEEKS)),
        'tp_min_weeks': cfg.get('tp_min_weeks', saved.get('tp_min_weeks', StrategyConfig.VA.TP_MIN_WEEKS)),
        'tp_min_value': cfg.get('tp_min_value', saved.get('tp_min_value', StrategyConfig.VA.TP_MIN_VALUE)),
        
        'filled_order_ids': set(saved.get('filled_order_ids', [])),
        'trade_week_set': set(saved.get('trade_week_set', [])),
        'max_position': saved.get('max_position', actual_initial_base + saved.get('grid_unit', cfg['grid_unit']) * 20),
        
        'grid_atr_rate': saved.get('grid_atr_rate', saved.get('used_atr_rate', None)),
        'macro_atr_rate': saved.get('macro_atr_rate', None),
        
        'buy_stack': [],
        'sell_stack': [],
        'credit_limit': cfg.get('credit_limit', saved.get('credit_limit', StrategyConfig.CREDIT_LIMIT)),
        '_fill_tracker': saved.get('_fill_tracker', {}), 
        'history_pnl': saved.get('history_pnl', 0.0),
        '_tp_hwm_ratio': saved.get('_tp_hwm_ratio', 0.0), 
        '_tp_tier': saved.get('_tp_tier', 0),             
        '_macro_sell_ids': saved.get('_macro_sell_ids', []),
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
        '_pending_ignore_ids': [],
        'wm_map': saved.get('wm_map', {}),
        'wm_pnl': saved.get('wm_pnl', 0.0)
    })

    for key in ['buy_stack', 'sell_stack']:
        raw = saved.get(key, [])
        for item in raw:
            st[key].append(tuple(item) if isinstance(item, (list, tuple)) else (item, st['grid_unit']))
        heapq.heapify(st[key])

    for k in ['scale_factor', 'pending_fill_amount', 'used_atr_rate', 'cached_atr_ema']:
        if k in st: st.pop(k)
        
    context.state[sym] = st
    context.latest_data[sym] = st['base_price']
    context.should_place_order_map[sym] = True
    context.mark_halted[sym] = False
    context.last_valid_price[sym] = st['base_price']
    context.last_valid_ts[sym] = None
    context.pending_frozen[sym] = 0
    audit_initial_consistency(context, sym)

def audit_initial_consistency(context, symbol):
    """启动审计：检查 159934 类似的账实不符问题"""
    try:
        p = get_position(symbol)
        actual_pos = p.amount if p else 0
        state = context.state[symbol]
        base_pos = state['base_position']
        
        # 理论上 Stack 里应该有多少股
        theoretical_stack_shares = actual_pos - base_pos
        # 实际上 Stack 里记录了多少股
        current_stack_shares = sum(item[1] for item in state['buy_stack'])
        
        if theoretical_stack_shares != current_stack_shares:
            info("⚠️ [{}] 审计异常: 实盘持仓网格部分 {} 股, 但 Stack 记录 {} 股。差额: {}。请检查 JSON。", 
                 dsym(context, symbol), theoretical_stack_shares, current_stack_shares, theoretical_stack_shares - current_stack_shares)
        else:
            info("✅ [{}] 数据对齐审计通过。", dsym(context, symbol))
    except: pass

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
        # 🌟 这里的 initial_position_value 已经是安全的、继承自记忆的锚点
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
    try:
        reload_config_if_changed(context)
        info('✅ [Pre-Market] 盘前配置同步完成，当前标的数量: {}', len(context.symbol_list))
    except Exception as e:
        info('⚠️ [Pre-Market] 盘前配置同步异常: {}', e)

    if '回测' not in context.env:
        info('🔄 [PnL Reset] 强制重置 PnL 状态并回溯补算 (Scope: 45 days)...')
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
    """使用 normalize 兼容字典/对象返回"""
    try:
        order_detail = get_order(entrust_no)
        if order_detail:
            normalized = OrderUtils.normalize(order_detail)
            return normalized.get('status', '')
        return ''
    except Exception:
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
    """
    [Global Ver: v3.8.0]
    [Update]: 在集合竞价计算出买卖价后，提前获取持仓，判定VA特权，并调用7参数守门员进行检查。
    """
    if '回测' in context.env or not (is_auction_time() or is_main_trading_time()): return
    info('🔄 开始集合竞价挂单流程 (并发模式)...')
    _fast_cancel_all_orders_global(context)
    
    orders_batch = []
    for sym in context.symbol_list:
        if sym not in context.state: continue
        state = context.state[sym]
        state.pop('_last_order_bp', None)
        state.pop('_last_order_ts', None)
        
        adjust_grid_unit(state)
        context.latest_data[sym] = state['base_price']
        
        base = state['base_price']
        unit = state['grid_unit']
        
        # 1. 原始计算
        buy_sp, sell_sp = state['buy_grid_spacing'], state['sell_grid_spacing']
        buy_p = round(base * (1 - buy_sp), 3)
        sell_p = round(base * (1 + sell_sp), 3)
        
        # [v3.8 同步升级] -----------------------------------------------
        # 提前获取持仓数据，判定 VA 建仓特权
        position = get_position(sym)
        pos = position.amount
        enable = position.enable_amount - context.pending_frozen.get(sym, 0)
        
        target_base_pos = state.get('base_position', 0)
        bypass_buy_block = (pos < target_base_pos + 5 * unit)
        
        # 调用守门员 (7参数)，传入特权标志
        buy_p, sell_p = _apply_price_guard(context, state, buy_p, sell_p, buy_sp, sell_sp, bypass_buy_block)
        # ---------------------------------------------------------------
        
        if pos + unit <= state['max_position']:
            orders_batch.append({'symbol': sym, 'side': 'buy', 'price': buy_p, 'amount': unit})
        
        if enable >= unit and pos - unit >= state['base_position']:
            orders_batch.append({'symbol': sym, 'side': 'sell', 'price': sell_p, 'amount': -unit})
            
        safe_save_state(sym, state)

    info('🚀 生成 {} 笔挂单任务，开始密集发送...', len(orders_batch))
    count = 0
    
    # 确保 Tracker 存在 (防止早盘漏单)
    for sym in context.symbol_list:
        if '_fill_tracker' not in context.state[sym]:
            context.state[sym]['_fill_tracker'] = {}

    for task in orders_batch:
        try:
            if count > 0 and count % 5 == 0: time.sleep(0.05)
            # 发单
            eid = order(task['symbol'], task['amount'], limit_price=task['price'])
            
            if eid:
                # 记录 Tracker
                sym = task['symbol']
                context.state[sym]['_fill_tracker'][str(eid)] = 0.0
                # 更新冻结
                if task['amount'] < 0:
                    context.pending_frozen[sym] = context.pending_frozen.get(sym, 0) + abs(task['amount'])
            
            count += 1
        except Exception:
            pass

# ---------------- 实时价：快照获取 + 心跳日志 ----------------

def _fetch_quotes_via_snapshot(context):
    StrategyConfig.load(context)
    symbols = list(getattr(context, 'symbol_list', []) or [])
    if not symbols: return

    snaps = {}
    try:
        snaps = get_snapshot(symbols) or {}
    except Exception:
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
            
            # 【核心】缓存物理涨跌停边界
            if sym in context.state:
                context.state[sym]['_up_limit'] = snap.get('p_up_price')
                context.state[sym]['_down_limit'] = snap.get('p_down_price')

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

# ---------------- 【核心】公共风控守门员 ----------------

def _apply_price_guard(context, state, buy_p, sell_p, buy_sp, sell_sp, bypass_buy_block=False):
    """
    [Global Ver: v3.12.14] 
    修复同价买卖摩擦漏洞：将边界判定从严格小于(<)改为小于等于(<=)，强制拉开最小利润空间。
    """
    final_buy_p, final_sell_p = buy_p, sell_p
    sym = state.get('symbol', 'Unknown')
    
    # 1. 守门员逻辑：买入检查 (防止高位追高接回空单)
    sell_stack = state.get('sell_stack', [])
    if sell_stack:
        max_sell_price = -sell_stack[0][0] 
        # 【核心修复】：改为 >= 1e-5，只要买价等于或高于上一笔卖价，强制向下修正
        if final_buy_p >= max_sell_price - 1e-5:
            credit = state.get('credit_limit', 0)
            if credit <= 0:
                corrected = round(max_sell_price - (max_sell_price * buy_sp), 3)
                if corrected < final_buy_p:
                    if bypass_buy_block:
                        info('[{}] 🛡️ 守门员(买): 触发【VA建仓特权】！无视历史卖飞价({:.3f})，放行挂单: {:.3f}', 
                             dsym(context, sym), max_sell_price, final_buy_p)
                    else:
                        info('[{}] 🛡️ 守门员拦截(买): 防止高位接回/同价摩擦. 原:{:.3f} 修正:{:.3f} (栈顶卖价:{:.3f})', 
                             dsym(context, sym), final_buy_p, corrected, max_sell_price)
                        final_buy_p = corrected

    # 2. 守门员逻辑：卖出检查 (防止低位割肉或同价白打工)
    buy_stack = state.get('buy_stack', [])
    if buy_stack:
        min_buy_price = buy_stack[0][0]
        # 【核心修复】：改为 <= 1e-5，只要卖价等于或低于上一笔买价，强制向上修正
        if final_sell_p <= min_buy_price + 1e-5:
            credit = state.get('credit_limit', 0)
            if credit <= 0:
                corrected = round(min_buy_price + (min_buy_price * sell_sp), 3)
                if corrected > final_sell_p:
                    info('[{}] 🛡️ 守门员拦截(卖): 防止低位割肉/同价摩擦. 原:{:.3f} 修正:{:.3f} (栈顶买价:{:.3f})', 
                         dsym(context, sym), final_sell_p, corrected, min_buy_price)
                    final_sell_p = corrected
                
    return final_buy_p, final_sell_p

# ---------------- 网格限价挂单主逻辑 ----------------

def place_limit_orders(context, symbol, state, ignore_cooldown=False, bypass_lock=False, ignore_entrust_nos=None):
    """
    [Global Ver: v3.11.0]
    增加 影子棘轮机制 (Ghost Ratchet)，在守门员拦截时基准价依然如影随形。
    """
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

    # 停牌检查
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
    
    # 提前获取持仓与缺口信息
    position = get_position(symbol)
    pos = position.amount 
    target_base_pos = state.get('base_position', 0)
    
    # 1. 原始计算 (网格理论挂单价)
    theo_buy_p = round(base * (1 - buy_sp), 3)
    theo_sell_p = round(base * (1 + sell_sp), 3)
    buy_p, sell_p = theo_buy_p, theo_sell_p
    
    if not is_valid_price(buy_p) or not is_valid_price(sell_p): return

    # ==========================================
    # v3.8 模块 A: 判定 VA 补仓特权
    # 实际持仓 < 目标持仓 + 5个网格单位
    # ==========================================
    bypass_buy_block = (pos < target_base_pos + 5 * unit)

    # [第一次守门] 携带 bypass_buy_block 标志
    buy_p, sell_p = _apply_price_guard(context, state, buy_p, sell_p, buy_sp, sell_sp, bypass_buy_block)

    # ==========================================
    # v3.9/v3.10 模块 B: ATR 天地锁破锁机制 (纯空间加权融合)
    # ==========================================
    if buy_p > 0 and sell_p > 0:
        gap_pct = (sell_p - buy_p) / buy_p
        
        # [V3.12.5 紧急修复] 破锁机制属于微观网格防御，对接高敏 Grid_ATR
        atr_pct = calculate_grid_atr(context, symbol, atr_period=14)
        if atr_pct is None or math.isnan(atr_pct) or atr_pct <= 0:
            atr_pct = 0.02
            
        UNLOCK_MULTIPLIER = StrategyConfig.MARKET.UNLOCK_ATR_MULTIPLIER 
        
        # 如果真空区大于 N 倍 ATR，判定为严重死锁
        if gap_pct > UNLOCK_MULTIPLIER * atr_pct:
            info('[{}] 🚨 死锁警报: GAP({:.2%}) > {}倍ATR({:.2%})', 
                 dsym(context, symbol), gap_pct, UNLOCK_MULTIPLIER, UNLOCK_MULTIPLIER * atr_pct)
            
            # 计算买卖盘被守门员扭曲的程度
            distortion_buy = theo_buy_p - buy_p
            distortion_sell = sell_p - theo_sell_p
            
            if distortion_buy > distortion_sell and state['sell_stack']:
                # 买盘扭曲严重，说明是历史卖飞单惹的祸 (处理 sell_stack)
                if len(state['sell_stack']) >= 2:
                    sorted_sells = sorted(state['sell_stack'], key=lambda x: x[0], reverse=True)
                    o1, o2 = sorted_sells[0], sorted_sells[1]
                    
                    state['sell_stack'].remove(o1)
                    state['sell_stack'].remove(o2)
                    
                    p1, v1 = -o1[0], o1[1]
                    p2, v2 = -o2[0], o2[1]
                    
                    # 核心：纯股数加权融合
                    p_merge = round((p1 * v1 + p2 * v2) / (v1 + v2), 3)
                    v_merge = v1 + v2
                    
                    # 重新压入栈 (转化回 -price)
                    heapq.heappush(state['sell_stack'], (-p_merge, v_merge))
                    info('[{}] 🧬 空间融合(软化空头): 极低卖飞单 {:.3f}({}股) 与 {:.3f}({}股) 融合为新防线: {:.3f}({}股)', 
                         dsym(context, symbol), p1, v1, p2, v2, p_merge, v_merge)
                else:
                    removed_record = state['sell_stack'].pop(0)
                    info('[{}] 🔪 破锁(清空头): 仅剩单笔极值，直接剔除极低卖飞单: 价:{:.3f} 量:{}', 
                         dsym(context, symbol), -removed_record[0], removed_record[1])
                     
            elif distortion_sell > distortion_buy and state['buy_stack']:
                # 卖盘扭曲严重，说明是历史套牢单惹的祸 (处理 buy_stack)
                if len(state['buy_stack']) >= 2:
                    sorted_buys = sorted(state['buy_stack'], key=lambda x: x[0], reverse=True)
                    o1, o2 = sorted_buys[0], sorted_buys[1]
                    
                    state['buy_stack'].remove(o1)
                    state['buy_stack'].remove(o2)
                    
                    p1, v1 = o1[0], o1[1]
                    p2, v2 = o2[0], o2[1]
                    
                    # 核心：纯股数加权融合
                    p_merge = round((p1 * v1 + p2 * v2) / (v1 + v2), 3)
                    v_merge = v1 + v2
                    
                    heapq.heappush(state['buy_stack'], (p_merge, v_merge))
                    info('[{}] 🧬 空间融合(软化多头): 极高套牢单 {:.3f}({}股) 与 {:.3f}({}股) 融合为新防线: {:.3f}({}股)', 
                         dsym(context, symbol), p1, v1, p2, v2, p_merge, v_merge)
                else:
                    removed_record = state['buy_stack'].pop(0)
                    info('[{}] 🔪 破锁(清多头): 仅剩单笔极值，移交极高套牢单至VA底仓: 价:{:.3f} 量:{}', 
                         dsym(context, symbol), removed_record[0], removed_record[1])
            
            # 清理后必须重新堆化
            heapq.heapify(state['sell_stack'])
            heapq.heapify(state['buy_stack'])
            
            # 融合软化了极值阻力后，重新过一次守门员，获取健康的网格挂单价
            buy_p, sell_p = _apply_price_guard(context, state, theo_buy_p, theo_sell_p, buy_sp, sell_sp, bypass_buy_block)
            info('[{}] ♻️ 融合破锁后重新排单: 买 {:.3f} | 卖 {:.3f}', dsym(context, symbol), buy_p, sell_p)

    up_limit = state.get('_up_limit')
    down_limit = state.get('_down_limit')
    can_place_buy = True
    can_place_sell = True

    if is_valid_price(up_limit) and is_valid_price(down_limit):
        if buy_p < down_limit:
            info('[{}] 🛡️ 空间封锁：买价 {:.3f} 低于跌停线 {:.3f}，暂停挂买。', dsym(context, symbol), buy_p, down_limit)
            can_place_buy = False
        if sell_p > up_limit:
            info('[{}] 🛡️ 空间封锁：卖价 {:.3f} 高于涨停线 {:.3f}，暂停挂卖。', dsym(context, symbol), sell_p, up_limit)
            can_place_sell = False

    # ==========================================
    # v3.11 模块 C: 影子棘轮机制 (Ghost Ratchet)
    # ==========================================
    price = context.latest_data.get(symbol)
    ratchet_enabled = (not allow_tickless) and is_valid_price(price)

    if ratchet_enabled:
        if abs(price / base - 1) <= 0.10:
            is_in_low_pos_range = (pos - unit <= state['base_position'])
            is_in_high_pos_range = (pos + unit >= state['max_position'])
            
            # 判定理论网格价是否被守门员强制扭曲拦截
            is_sell_blocked_by_guard = (sell_p > theo_sell_p)
            is_buy_blocked_by_guard = (buy_p < theo_buy_p)
            
            # 触发条件：不仅在极限仓位时跟随，在被守门员拦截时也如影随形地跟随
            ratchet_up = (price >= theo_sell_p) and (is_in_low_pos_range or is_sell_blocked_by_guard)
            ratchet_down = (price <= theo_buy_p) and (is_in_high_pos_range or is_buy_blocked_by_guard)
            
            if ratchet_up:
                info('[{}] 🚀 影子棘轮上移(拦截/空仓): 触及理论卖价 {:.3f}，基准抬至 {:.3f}', dsym(context, symbol), theo_sell_p, theo_sell_p)
                state['base_price'] = theo_sell_p
                cancelled_ids = cancel_all_orders_by_symbol(context, symbol)
                if cancelled_ids: state['_pending_ignore_ids'] = list(cancelled_ids)
                
                # 核心修复：把接力棒交给异步补单机制，延迟 2 秒让 API 消化撤单
                delay_s = StrategyConfig.DEBUG.DELAY_AFTER_CANCEL
                state['_rehang_due_ts'] = datetime.now() + timedelta(seconds=max(delay_s, 2.0))
                
                state.pop('_last_order_ts', None)
                state.pop('_last_order_bp', None)
                safe_save_state(symbol, state)
                return  # 直接返回，不往下执行了
                
            elif ratchet_down:
                info('[{}] ⚓ 影子棘轮下移(拦截/满仓): 触及理论买价 {:.3f}，基准降至 {:.3f}', dsym(context, symbol), theo_buy_p, theo_buy_p)
                state['base_price'] = theo_buy_p
                cancelled_ids = cancel_all_orders_by_symbol(context, symbol)
                if cancelled_ids: state['_pending_ignore_ids'] = list(cancelled_ids)
                
                # 核心修复：把接力棒交给异步补单机制
                delay_s = StrategyConfig.DEBUG.DELAY_AFTER_CANCEL
                state['_rehang_due_ts'] = datetime.now() + timedelta(seconds=max(delay_s, 2.0))
                
                state.pop('_last_order_ts', None)
                state.pop('_last_order_bp', None)
                safe_save_state(symbol, state)
                return  # 直接返回，不往下执行了

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
             order_info = OrderUtils.normalize(o)
             if OrderUtils.is_active(order_info):
                 eid = order_info['entrust_no']
                 if eid and eid in ignore_set: continue
                 if eid and eid in filled_ids: continue
                 open_orders.append(o)
        
        same_buy = any(o.amount > 0 for o in open_orders)
        same_sell = any(o.amount < 0 for o in open_orders)

        enable_amount = position.enable_amount
        state['_oo_last'] = len(open_orders)
        state['_last_pos_seen'] = pos 

        if '_fill_tracker' not in state: state['_fill_tracker'] = {}

        if can_place_buy and not same_buy and pos + unit <= state['max_position']:
            try:
                # buy_p 已被完美修正
                eid = order(symbol, unit, limit_price=buy_p)
                if eid: state['_fill_tracker'][str(eid)] = 0.0
                info('[{}] --> 发起买入委托: {}股 @ {:.3f}', dsym(context, symbol), unit, buy_p)
            except Exception as e:
                err_str = str(e)
                if "超过涨跌停范围" in err_str or "120162" in err_str:
                    info('[{}] ⛔ 瞬时触及边界：买单申报失败，进入静默冷却。', dsym(context, symbol))
                    state['_last_trade_ts'] = now_dt + timedelta(seconds=60)
                else: raise e

        can_sell = not same_sell
        pending_frozen = context.pending_frozen.get(symbol, 0)
        real_enable = enable_amount - pending_frozen
        
        if can_place_sell and can_sell and real_enable >= unit and pos - unit >= state['base_position']:
            try:
                # sell_p 已被完美修正
                eid = order(symbol, -unit, limit_price=sell_p)
                if eid: state['_fill_tracker'][str(eid)] = 0.0
                info('[{}] --> 发起卖出委托: {}股 @ {:.3f} (可用:{}, 冻结:{})', dsym(context, symbol), unit, sell_p, enable_amount, pending_frozen)
                context.pending_frozen[symbol] = pending_frozen + unit
            except Exception as e:
                err_str = str(e)
                if "超过涨跌停范围" in err_str or "120162" in err_str:
                    info('[{}] ⛔ 瞬时触及边界：卖单申报失败，进入静默冷却。', dsym(context, symbol))
                    state['_last_trade_ts'] = now_dt + timedelta(seconds=60)
                else: raise e

    except Exception as e:
        info('[{}] ⚠️ 限价挂单异常：{}', dsym(context, symbol), e)
    finally:
        state.pop('_rehang_bypass_once', None)
        safe_save_state(symbol, state)

# ---------------- 成交回报与后续挂单 ----------------

def on_trade_response(context, trade_list):
    """
    [Global Ver: v3.12.0] [Func Ver: 3.0]
    [Change]: 加入对宏观止盈大单的物理隔离(is_macro_sell)，防止其被误认为网格卖单压入堆栈。
    """
    if not hasattr(context, 'processed_business_ids'):
        context.processed_business_ids = deque(maxlen=2000)
        
    for tr in trade_list:
        status = str(tr.get('status'))
        if status not in ['7', '8']: continue
        
        bid = str(tr.get('business_id', ''))
        
        if bid:
            if bid in context.processed_business_ids: continue
            context.processed_business_ids.append(bid)
        else:
            pass 

        raw_amount = tr.get('business_amount', 0)
        raw_price = tr.get('business_price', 0)
        
        if abs(float(raw_amount)) <= 1e-5:
            continue
        if not is_valid_price(float(raw_price)):
            continue

        sym = convert_symbol_to_standard(tr['stock_code'])
        log_trade_details(context, sym, tr) 
        
        if sym not in context.state: continue
        state = context.state[sym]

        bs = str(tr.get('entrust_bs')) 
        if bs == '1':
            fill_amount = abs(raw_amount) 
            trade_dir = "买入"
        elif bs == '2':
            fill_amount = -abs(raw_amount) 
            trade_dir = "卖出"
        else: continue
            
        price = float(raw_price)
        entrust_no = str(tr.get('entrust_no', ''))

        # ==========================================
        # [v3.12.0] 多空物理隔离：如果是宏观止盈单，不走网格对冲逻辑
        # ==========================================
        is_macro_sell = entrust_no in state.get('_macro_sell_ids', [])
        
        if not is_macro_sell:
            process_trade_logic(context, sym, price, fill_amount)
        else:
            info('📦 [{}] 宏观止盈大单斩获成交! (ID:{}, 股数:{})，跳过底层网格堆栈记录。', dsym(context, sym), entrust_no[-6:], abs(fill_amount))

        if '_fill_tracker' not in state: state['_fill_tracker'] = {}
        if entrust_no:
            state['_fill_tracker'][entrust_no] = state['_fill_tracker'].get(entrust_no, 0.0) + abs(fill_amount)
        
        if not is_macro_sell:
            info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f} (ID:{}, Sts:{})', 
                 dsym(context, sym), trade_dir, abs(fill_amount), price, bid[-6:] if bid else 'N/A', status)

        is_fully_filled = (status == '8')

        if is_fully_filled:
            if entrust_no:
                state['filled_order_ids'].add(entrust_no)

            if not is_macro_sell:
                state['_last_trade_ts'] = context.current_dt
                state['_last_fill_dt'] = context.current_dt
                state['last_fill_price'] = price
                state['base_price'] = price

            cancelled_ids = cancel_all_orders_by_symbol(context, sym)
            if cancelled_ids: state['_pending_ignore_ids'] = list(cancelled_ids)

            delay_s = StrategyConfig.DEBUG.DELAY_AFTER_CANCEL
            state['_rehang_due_ts'] = datetime.now() + timedelta(seconds=max(delay_s, 2.0))
            
            context.mark_halted[sym] = False
            context.last_valid_price[sym] = price
            context.latest_data[sym] = price
            context.last_valid_ts[sym] = context.current_dt

            state.pop('_last_order_ts', None)
            state.pop('_last_order_bp', None)
            context.should_place_order_map[sym] = True
        else:
            if not is_macro_sell:
                info('⏳ [{}] 订单部成 (ID:{}), 仅记录筹码, 基准价保持不变, 剩余挂单继续排队...', dsym(context, sym), entrust_no)
        
        try: state['_last_pos_seen'] = get_position(sym).amount
        except: state['_last_pos_seen'] = None
            
        safe_save_state(sym, state)

def process_trade_logic(context, symbol, fill_price, fill_amount):
    """
    [Global Ver: v3.10.0] [Func Ver: 3.1]
    [Change]: 在余量入库后，增加堆栈容量上限 (MAX_STACK_SIZE) 检测与平滑融合裁剪机制。
    """
    state = context.state[symbol]
    
    # 方向判断
    is_buy = (fill_amount > 0)
    remaining_qty = abs(fill_amount)
    
    # -----------------------------------------------------------
    # 对冲循环 (Pairing Loop)
    # -----------------------------------------------------------
    while remaining_qty > 0:
        target_stack = state['sell_stack'] if is_buy else state['buy_stack']
        
        # 1. 如果对手库为空，直接跳出 (无对手可平)
        if not target_stack:
            break
            
        # 2. 取出对手单 (Peek)
        # SellStack存的是(-price, unit), BuyStack存的是(price, unit)
        if is_buy:
            top_record = target_stack[0] # peek
            stack_price = -top_record[0] # 还原正数
            stack_qty = top_record[1]
        else:
            top_record = target_stack[0] # peek
            stack_price = top_record[0]
            stack_qty = top_record[1]
            
        # 3. 计算配对利润 (Pnl Check)
        trade_pnl = (stack_price - fill_price) if is_buy else (fill_price - stack_price)
        
        # 4. 利润门槛判断 (Profit Guard)
        if trade_pnl <= 0:
            info('[{}] 🛑 停止配对: 对冲利润 {:.3f} <= 0 (Stack:{:.3f} vs Fill:{:.3f})', 
                 dsym(context, symbol), trade_pnl, stack_price, fill_price)
            break
            
        # 5. 执行抵扣 (Deduction)
        match_qty = min(remaining_qty, stack_qty)
        pnl_realized = trade_pnl * match_qty
        state['history_pnl'] = state.get('history_pnl', 0.0) + pnl_realized
        
        info('[{}] ⚖️ [对冲成功] {} {:.3f} (Qty:{}) vs Stack {:.3f}, PnL: {:.2f}', 
             dsym(context, symbol), "买入平空" if is_buy else "卖出平多", 
             fill_price, match_qty, stack_price, pnl_realized)
             
        # 更新堆栈
        heapq.heappop(target_stack) # 先弹出
        if stack_qty > match_qty:
            # 没吃完，把剩下的放回去
            left_qty = stack_qty - match_qty
            if is_buy:
                heapq.heappush(target_stack, (-stack_price, left_qty))
            else:
                heapq.heappush(target_stack, (stack_price, left_qty))
        
        remaining_qty -= match_qty
        
    # -----------------------------------------------------------
    # 余量入库 (Residual Push)
    # -----------------------------------------------------------
    if remaining_qty > 0.01: # 忽略浮点微小误差
        my_stack = state['buy_stack'] if is_buy else state['sell_stack']
        
        # 入库前查重 (避免同价位堆积)
        check_val = fill_price if is_buy else -fill_price
        
        if not any(abs(item[0] - check_val) < 1e-5 for item in my_stack):
            heapq.heappush(my_stack, (check_val, remaining_qty))
            info('[{}] 📥 [新单入库] {} Qty:{} @ {:.3f}', 
                 dsym(context, symbol), "买入开多" if is_buy else "卖出开空", remaining_qty, fill_price)
        else:
             for i, item in enumerate(my_stack):
                 if abs(item[0] - check_val) < 1e-5:
                     my_stack[i] = (item[0], item[1] + remaining_qty)
                     heapq.heapify(my_stack) # 重新堆化
                     info('[{}] ➕ [加仓合并] {} Qty:{} 合并入 {:.3f}', 
                          dsym(context, symbol), "买入" if is_buy else "卖出", remaining_qty, fill_price)
                     break

        # -----------------------------------------------------------
        # [v3.10.0] 容量裁剪防死锁 (Stack Size Limit Merging)
        # -----------------------------------------------------------
        max_size = StrategyConfig.MARKET.MAX_STACK_SIZE
        
        while len(my_stack) > max_size:
            if is_buy:
                # 处理 buy_stack: 找出实际价格最高的两个多单融合
                sorted_buys = sorted(my_stack, key=lambda x: x[0], reverse=True)
                o1, o2 = sorted_buys[0], sorted_buys[1]
                my_stack.remove(o1)
                my_stack.remove(o2)
                
                p1, v1 = o1[0], o1[1]
                p2, v2 = o2[0], o2[1]
                p_merge = round((p1 * v1 + p2 * v2) / (v1 + v2), 3)
                v_merge = v1 + v2
                
                heapq.heappush(my_stack, (p_merge, v_merge))
                info('[{}] 📦 容量裁剪(多头超载): 极高套牢单 {:.3f}({}股) 与 {:.3f}({}股) 融合为: {:.3f}({}股)', 
                     dsym(context, symbol), p1, v1, p2, v2, p_merge, v_merge)
            else:
                # 处理 sell_stack: 找出实际价格最低的两个空单融合 (存的是-price)
                sorted_sells = sorted(my_stack, key=lambda x: x[0], reverse=True)
                o1, o2 = sorted_sells[0], sorted_sells[1]
                my_stack.remove(o1)
                my_stack.remove(o2)
                
                p1, v1 = -o1[0], o1[1]
                p2, v2 = -o2[0], o2[1]
                p_merge = round((p1 * v1 + p2 * v2) / (v1 + v2), 3)
                v_merge = v1 + v2
                
                heapq.heappush(my_stack, (-p_merge, v_merge))
                info('[{}] 📦 容量裁剪(空头超载): 极低卖飞单 {:.3f}({}股) 与 {:.3f}({}股) 融合为: {:.3f}({}股)', 
                     dsym(context, symbol), p1, v1, p2, v2, p_merge, v_merge)
                     
        # 裁剪操作打乱了原本底层数组的顺序，必须重新堆化
        heapq.heapify(my_stack)

def on_order_filled(context, symbol, order):
    """
    [Global Ver: v3.12.13] [Func Ver: 2.2]
    [Change]: 同步增加宏观止盈大单的物理隔离，防止此回调路径污染网格堆栈。
    """
    state = context.state[symbol]
    if order.filled == 0: return
    
    # 更新冻结
    if order.amount < 0:
        current_frozen = context.pending_frozen.get(symbol, 0)
        context.pending_frozen[symbol] = max(0, current_frozen - abs(order.filled))

    # 🌟 修复点：物理隔离宏观止盈单
    entrust_no = str(getattr(order, 'entrust_no', ''))
    if entrust_no and entrust_no in state.get('_macro_sell_ids', []):
        # 已经被 on_trade_response 处理过或属于宏观单，直接跳过
        return

    # 直接调用新核心
    real_amount = order.filled if order.amount > 0 else -order.filled
    process_trade_logic(context, symbol, order.price, real_amount)
    
def _fill_recover_watch(context, symbol, state):
    now_dt = context.current_dt
    in_window = False
    if _in_reopen_window(now_dt.time()): in_window = True
    if state.get('_after_cancel_until') and now_dt <= state['_after_cancel_until']: in_window = True
    if state.get('_recover_until') and now_dt <= state['_recover_until']: in_window = True

    if not in_window:
        if state.get('_last_pos_seen') is None:
            try: state['_last_pos_seen'] = get_position(symbol).amount
            except Exception: pass
        if state.get('_oo_drop_seen_ts') or state.get('_pos_jump_seen_ts'):
             state['_oo_drop_seen_ts'] = None
             state['_pos_jump_seen_ts'] = None
             state['_pos_confirm_deadline'] = None
        return

    try:
        oo = [o for o in (get_open_orders(symbol) or []) if OrderUtils.is_active(OrderUtils.normalize(o))]
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
    """
    [Global Ver: v3.12.0] [Func Ver: 3.0]
    [Change]: 巡检漏单补录及废单清理逻辑中，增加对宏观大单(_macro_sell_ids)的免伤隔离。
    """
    now_dt = context.current_dt
    if is_main_trading_time():
        try:
            all_orders = get_orders(symbol) or []
            tracker = state.get('_fill_tracker', {})
            
            for o in all_orders:
                o_info = OrderUtils.normalize(o)
                eid = o_info['entrust_no']
                filled_qty = o.filled 
                
                if filled_qty <= 0: continue
                
                if eid not in tracker:
                    tracker[eid] = float(filled_qty)
                    continue
                    
                processed_qty = tracker[eid]
                delta = filled_qty - processed_qty
                
                if delta > 0.9: 
                    trade_price = o.trade_price if o.trade_price > 0 else o.price
                    direction = 1 if not OrderUtils.is_sell(o_info) else -1
                    real_amount = delta * direction
                    
                    info('🕵️ [{}] [补录] 发现漏单! 漏:{} (总成:{} vs 已记:{})', dsym(context, symbol), delta, filled_qty, processed_qty)
                    
                    # [V3.12.0] 如果是宏观止盈单，漏单补录也不入网格账本
                    if eid in state.get('_macro_sell_ids', []):
                         info('📦 [{}] 补录判定为宏观止盈单，跳过入栈。', dsym(context, symbol))
                    else:
                         process_trade_logic(context, symbol, trade_price, real_amount)
                    
                    tracker[eid] = float(filled_qty)
                    state['history_pnl'] = state.get('history_pnl', 0.0) 
                    
            state['_fill_tracker'] = tracker
        except Exception as e:
            info('[{}] ⚠️ FillPatrol 异常: {}', dsym(context, symbol), e)

    if state.get('_last_trade_ts') and (now_dt - state['_last_trade_ts']).total_seconds() < 58: return
    if not (is_main_trading_time() and now_dt.time() < dtime(14, 55)): return 
    if context.mark_halted.get(symbol, False): return 
    if not is_valid_price(context.latest_data.get(symbol)): return 

    try:
        position = get_position(symbol)
        pos = position.amount 
        enable_amount = position.enable_amount
        open_orders = [o for o in (get_open_orders(symbol) or []) if OrderUtils.is_active(OrderUtils.normalize(o))]

        base_pos = state['base_position']
        max_pos = state['max_position']
        unit = state['grid_unit']
        base_price = state['base_price']
        buy_sp, sell_sp = state['buy_grid_spacing'], state['sell_grid_spacing']
        buy_p = round(base_price * (1 - buy_sp), 3)
        sell_p = round(base_price * (1 + sell_sp), 3)
        
        bypass_buy_block = (pos < base_pos + 5 * unit)
        buy_p, sell_p = _apply_price_guard(context, state, buy_p, sell_p, buy_sp, sell_sp, bypass_buy_block)

        up_limit = state.get('_up_limit')
        down_limit = state.get('_down_limit')
        
        should_have_buy_order = (pos + unit <= max_pos)
        if is_valid_price(down_limit) and buy_p < down_limit:
            should_have_buy_order = False 

        pending_frozen = context.pending_frozen.get(symbol, 0)
        real_enable = enable_amount - pending_frozen
        should_have_sell_order = (real_enable >= unit and pos - unit >= base_pos)
        if is_valid_price(up_limit) and sell_p > up_limit:
            should_have_sell_order = False 

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
                # [V3.12.0] 宏观大单不属于被巡检撤销的范围，直接无视
                if entrust_no in state.get('_macro_sell_ids', []): continue
                
                if not should_have_sell_order: is_wrong = True 
                elif abs(o_price - sell_p) / (sell_p + 1e-9) >= 0.002: is_wrong = True 
                else: valid_sell_orders.append(o)
            
            if is_wrong: orders_to_cancel.append(o)
        
        has_correct_buy_order = (len(valid_buy_orders) > 0)
        has_correct_sell_order = (len(valid_sell_orders) > 0)

        if len(valid_buy_orders) > 1:
            for o in valid_buy_orders[1:]: orders_to_cancel.append(o)
            valid_buy_orders = valid_buy_orders[:1]
            has_correct_buy_order = True
        if len(valid_sell_orders) > 1:
            for o in valid_sell_orders[1:]: orders_to_cancel.append(o)
            valid_sell_orders = valid_sell_orders[:1]
            has_correct_sell_order = True

        if orders_to_cancel:
            info('[{}] 🛡️ PATROL: 发现 {} 笔错误/重复挂单，正在撤销...', dsym(context, symbol), len(orders_to_cancel))
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

        if (should_have_buy_order and not has_correct_buy_order) or \
           (should_have_sell_order and not has_correct_sell_order):
            place_limit_orders(context, symbol, state, ignore_cooldown=True)

    except Exception as e:
        info('[{}] ⚠️ PATROL 巡检失败: {}', dsym(context, symbol), e)

# ---------------- 【核心】宏观止盈引擎 ----------------

def _check_macro_take_profit(context, symbol, state, price, dt):
    """
    [Global Ver: v3.13.5] [Func Ver: 6.3]
    [Change]: 修正止盈拦截逻辑。将周数与金额的放行条件从“满足其一(and拦截)”改为“必须双双达标(or拦截)”。
    坚决杜绝小资金规模下的无意义止盈，确保宏观止盈只在“大资金+大波段”时触发。
    """
    try:
        pos = get_position(symbol)
        if pos.amount == 0 or pos.cost_basis <= 0: return
        config = getattr(context, 'symbol_config', {}).get(symbol, {})
        tp_cool_weeks, min_weeks, min_val = config.get('tp_cool_weeks', 4), config.get('tp_min_weeks', 12), config.get('tp_min_value', 30000)

        if len(state.get('trade_week_set', set())) < tp_cool_weeks: return
        
        # 🌟 核心拦截逻辑修改：只要“周数不够” 或者 “金额不够”，全部给我退回去继续定投！
        if len(state.get('trade_week_set', set())) < min_weeks or (pos.amount * price) < min_val: return

        atr = calculate_macro_atr(context, symbol, atr_period=60) or 0.02
        state['macro_atr_rate'] = atr  
        profit_ratio = (price - pos.cost_basis) / pos.cost_basis
        hwm = max(state.get('_tp_hwm_ratio', 0.0), profit_ratio)
        state['_tp_hwm_ratio'] = hwm

        tier = 0
        for t, thresh in {3: 30.0*atr, 2: 20.0*atr, 1: 10.0*atr}.items():
            if profit_ratio >= thresh: 
                tier = max(state.get('_tp_tier', 0), t); break
        if tier > state.get('_tp_tier', 0):
            state['_tp_tier'] = tier
            info('[{}] 🚀 宏观止盈警报升级: Tier {}', dsym(context, symbol), tier)

        if tier > 0 and (hwm - profit_ratio) >= {1: 3.0*atr, 2: 5.0*atr, 3: 8.0*atr}.get(tier, 0.05):
            sell_ratio = {1: 0.33, 2: 0.50, 3: 1.0}.get(tier, 0.33)
            sell_amount = pos.amount if tier == 3 else math.floor(pos.amount * sell_ratio / 100) * 100
            
            if sell_amount > 0:
                eid = order(symbol, -sell_amount, price)
                if eid:
                    state.setdefault('_macro_sell_ids', []).append(str(eid))
                    # 💧 资金总量回流滴灌逻辑
                    total_cash = sell_amount * price
                    drip_weeks = max(12, min(48, int(0.6 / atr))) 
                    state['dingtou_base'] += (total_cash / drip_weeks)
                    
                    # ♻️ 止盈再平衡核心：确立新的事实底仓
                    unit = state['grid_unit']
                    remaining_pos = pos.amount - sell_amount
                    new_base = max(state['initial_base_position'], remaining_pos - 10 * unit)
                    new_base = math.floor(new_base / 100) * 100
                    
                    state['base_position'] = new_base
                    state['last_week_position'] = new_base
                    state['initial_base_position'] = new_base
                    state['initial_position_value'] = new_base * price
                    
                    state['max_position'] = new_base + unit * 20
                    state['trade_week_set'] = set() 
                    state['_tp_hwm_ratio'], state['_tp_tier'] = 0.0, 0
                    
                    info('[{}] ♻️ 止盈重置成功：新事实底仓 {} 已锁定。', dsym(context, symbol), new_base)
                    safe_save_state(symbol, state)
                    
    except Exception as e:
        log.error(f"[{symbol}] 宏观止盈引擎执行异常: {e}")

# ---------------- 行情主循环 ----------------

def handle_data(context, data):
    """
    [Global Ver: v3.12.11] [Func Ver: 2.1 (Hotfix)]
    [Change]: 修复 _check_macro_take_profit 缺少 dt 参数导致的 TypeError 崩溃。
    """
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
            except Exception: pass

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

    for sym in context.symbol_list:
        if sym not in context.state: continue
        st = context.state[sym]
        price = context.latest_data.get(sym)
        if is_valid_price(price):
            # [V3.12.11 热修复]: 补齐 now_dt 参数
            _check_macro_take_profit(context, sym, st, price, now_dt)
            
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
        except Exception: pass
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

# ---------------- 动态网格间距 (双轨波动率引擎 V3.12.5) ----------------

def calculate_grid_atr(context, symbol, atr_period=14):
    """
    【微观防守引擎】
    纯原味短周期 EMA。极度灵敏，暴跌暴涨当天立刻放大网格间距，保障不被单边打穿。
    """
    state = context.state[symbol]
    try:
        hist = get_history(atr_period + 5, '1d', ['high', 'low', 'close'], security_list=[symbol])
        df = hist.get(symbol) if isinstance(hist, dict) else hist
        current_atr_rate = None
        if df is not None and not df.empty and len(df) > 1:
            high, low, close = df['high'], df['low'], df['close']
            tr1 = high - low
            tr2 = (high - close.shift(1)).abs()
            tr3 = (low - close.shift(1)).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr_series = tr.ewm(span=atr_period, adjust=False).mean()
            last_atr_val, last_price = atr_series.iloc[-1], close.iloc[-1]
            if is_valid_price(last_price): current_atr_rate = last_atr_val / last_price
    except Exception as e:
        pass
        
    used_rate = state.get('grid_atr_rate')
    if current_atr_rate is not None and current_atr_rate > 0:
        # 10% 刷新门槛，滤除微小杂波
        if used_rate is None or abs(current_atr_rate - used_rate) / used_rate > 0.10: 
            state['grid_atr_rate'] = current_atr_rate
        return state['grid_atr_rate']
    return used_rate


def calculate_macro_atr(context, symbol, atr_period=60):
    """
    【宏观收割引擎】
    带有截尾平滑处理 (Winsorizing) 的长周期 EMA。
    稳如泰山，单日极其夸张的暴涨暴跌会被强行削平，止盈门槛绝对不会变成“追着胡萝卜跑的驴”。
    """
    state = context.state[symbol]
    try:
        # 多取历史数据保证均值平稳
        hist = get_history(atr_period + 20, '1d', ['high', 'low', 'close'], security_list=[symbol])
        df = hist.get(symbol) if isinstance(hist, dict) else hist
        current_atr_rate = None
        if df is not None and not df.empty and len(df) > 1:
            high, low, close = df['high'], df['low'], df['close']
            tr1 = high - low
            tr2 = (high - close.shift(1)).abs()
            tr3 = (low - close.shift(1)).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
            # 🛡️ 核心防失真装甲：中位数截尾 (限制极端日波幅不超过过去中位数的3倍)
            tr_median = tr.rolling(window=atr_period, min_periods=1).median()
            tr_clipped = tr.clip(upper=tr_median * 3)
            
            # 使用削平后的健康数据计算 EMA
            atr_series = tr_clipped.ewm(span=atr_period, adjust=False).mean()
            last_atr_val, last_price = atr_series.iloc[-1], close.iloc[-1]
            if is_valid_price(last_price): current_atr_rate = last_atr_val / last_price
    except Exception as e:
        if StrategyConfig.DEBUG.ENABLE: info('[{}] 宏观ATR测算异常: {}', dsym(context, symbol), e)
        
    used_rate = state.get('macro_atr_rate')
    if current_atr_rate is not None and current_atr_rate > 0:
        # 宏观指标要求更严格，只需 5% 的偏移即刷新记录，保持准星精准
        if used_rate is None or abs(current_atr_rate - used_rate) / used_rate > 0.05: 
            state['macro_atr_rate'] = current_atr_rate
        return state['macro_atr_rate']
    return used_rate

def update_grid_spacing_final(context, symbol, state, curr_pos):
    pos, unit, base_pos = curr_pos, state['grid_unit'], state['base_position']
    
    # [V3.12.5] 采用高敏微观 ATR
    atr_pct = calculate_grid_atr(context, symbol, atr_period=14)
    
    base_spacing = 0.005
    if atr_pct is not None and not math.isnan(atr_pct): base_spacing = max(atr_pct * 0.25, StrategyConfig.TRANSACTION_COST * 5)
    thresh_low, thresh_high = 5, 15
    if pos <= base_pos + unit * thresh_low: new_buy, new_sell = base_spacing, base_spacing * 2
    elif pos > base_pos + unit * thresh_high: new_buy, new_sell = base_spacing * 2, base_spacing
    else: new_buy, new_sell = base_spacing, base_spacing
    new_buy, new_sell = round(min(new_buy, 0.03), 4), round(min(new_sell, 0.03), 4)
    if new_buy != state.get('buy_grid_spacing') or new_sell != state.get('sell_grid_spacing'):
        state['buy_grid_spacing'], state['sell_grid_spacing'] = new_buy, new_sell
        info('[{}] 网格动态调整 (Grid ATR={:.2%}) -> [买{:.2%},卖{:.2%}]', dsym(context, symbol), (atr_pct or 0.0), new_buy, new_sell)

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
        target_val, current_val = state['initial_position_value'] + accumulated_investment, state['base_position'] * price
        surplus, grid_value = current_val - target_val, state['grid_unit'] * price
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
    except Exception: pass
    return state['base_position']

def get_trade_weeks(context, symbol, state, dt):
    """
    [Global Ver: v3.13.0] [Func Ver: 2.5]
    [Change]: 适配止盈后的重置逻辑，确保周数计数能平滑重启。
    """
    y, w, _ = dt.date().isocalendar()
    key = f"{y}_{w}"
    
    if 'trade_week_set' not in state or not isinstance(state['trade_week_set'], set):
        state['trade_week_set'] = set()
        
    if key not in state['trade_week_set']:
        state['trade_week_set'].add(key)
        # 记录上周位置，用于计算本周 VA 差额
        state['last_week_position'] = state.get('base_position', 0)
        safe_save_state(symbol, state)
        
    # 如果集合为空（刚止盈），强制返回 0 以便 VA 重新起步
    return len(state['trade_week_set'])

def adjust_grid_unit(state):
    if state['base_position'] > state['grid_unit'] * 20:
        theoretical_unit = math.ceil(state['base_position'] / 20 / 100) * 100
        price = state.get('base_price', 1.0)
        capped_unit_val = math.floor(StrategyConfig.MAX_TRADE_AMOUNT / price / 100) * 100
        new_unit = min(theoretical_unit, max(100, capped_unit_val))
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
            f.readline()
            for line in f:
                parts = line.strip().split(',')
                if len(parts) < 6: continue
                trades.append({'time': parts[0],'symbol': parts[1],'qty': float(parts[3]),'price': float(parts[4]),'base_pos_at_trade': int(parts[5]) if parts[5].isdigit() else 0})
    except Exception: return
    trades.sort(key=lambda x: x['time'])
    pnl_metrics = getattr(context, 'pnl_metrics', {})
    for sym in context.symbol_list:
        if sym not in context.state: continue
        state, initial_pos, initial_cost = context.state[sym], context.state[sym].get('initial_base_position', 0), context.state[sym].get('base_price', 0)
        inventory, current_holding, grid_pnl, base_pnl = [], initial_pos, 0.0, 0.0
        if initial_pos > 0: inventory.append([initial_pos, initial_cost, 'base'])
        sym_trades = [t for t in trades if t['symbol'] == sym]
        for t in sym_trades:
            qty, price, target_base = t['qty'], t['price'], (t['base_pos_at_trade'] if t['base_pos_at_trade'] > 0 else state.get('base_position', 0))
            if qty > 0:
                rem = qty
                if current_holding < target_base:
                    fill = min(rem, target_base - current_holding)
                    inventory.append([fill, price, 'base']); current_holding += fill; rem -= fill
                if rem > 0: inventory.append([rem, price, 'grid']); current_holding += rem
            elif qty < 0:
                sell_q = abs(qty); current_holding -= sell_q
                while sell_q > 0.001 and inventory:
                    lot = inventory[-1]; matched = min(sell_q, lot[0]); profit = (price - lot[1]) * matched
                    if lot[2] == 'base': base_pnl += profit
                    else: grid_pnl += profit
                    sell_q -= matched; lot[0] -= matched
                    if lot[0] <= 0.001: inventory.pop()
        if sym not in pnl_metrics: pnl_metrics[sym] = {}
        pnl_metrics[sym].update({'realized_grid_pnl': grid_pnl, 'realized_base_pnl': base_pnl, 'total_realized_pnl': grid_pnl + base_pnl})
    context.pnl_metrics = pnl_metrics
    _save_pnl_metrics(context)

def after_trading_end(context, data):
    if '回测' in context.env: return
    info('🏁 盘后作业开始...')
    try: _calculate_local_pnl_lifo(context)
    except Exception: pass
    try:
        update_daily_reports(context, data)
        generate_html_report(context)
    except Exception: pass
    info('✅ 盘后作业结束')

def reload_config_if_changed(context):
    """
    [Global Ver: v3.13.5] [Func Ver: 2.2]
    [Change]: 修复热重载对 dingtou_base 的覆盖，保护止盈滴灌的累积成果。
    """
    try:
        current_mod_time = context.config_file_path.stat().st_mtime
        if current_mod_time == context.last_config_mod_time: return
        info('♻️ 检测到配置文件发生变更，开始热重载...')
        context.last_config_mod_time = current_mod_time
        new_config = json.loads(context.config_file_path.read_text(encoding='utf-8'))
        StrategyConfig.load(context)
        old_symbols, new_symbols = set(context.symbol_list), set(new_config.keys())
        
        for sym in old_symbols - new_symbols:
            info('[{}] 标的已从配置中移除，将清理其状态和挂单...', dsym(context, sym))
            cancel_all_orders_by_symbol(context, sym)
            context.symbol_list.remove(sym)
            if sym in context.state: del context.state[sym]
            if sym in context.latest_data: del context.latest_data[sym]
            context.mark_halted.pop(sym, None)
            context.last_valid_price.pop(sym, None)
            context.last_valid_ts.pop(sym, None)
            context.pending_frozen.pop(sym, None)
            context.should_place_order_map.pop(sym, None)

        for sym in new_symbols - old_symbols:
            info('[{}] 新增标的 (或重载)，正在初始化状态...', dsym(context, sym))
            cfg = new_config[sym]
            state_file = research_path('state', f'{sym}.json')
            saved = json.loads(state_file.read_text(encoding='utf-8')) if state_file.exists() else {}
            st = {**cfg}
            
            actual_initial_base = saved.get('initial_base_position', cfg['initial_base_position'])
            actual_initial_val = saved.get('initial_position_value', actual_initial_base * cfg['base_price'])
            
            st.update({
                'symbol': sym,
                'base_price': saved.get('base_price', cfg['base_price']),
                'grid_unit': saved.get('grid_unit', cfg['grid_unit']),
                'filled_order_ids': set(saved.get('filled_order_ids', [])),
                'trade_week_set': set(saved.get('trade_week_set', [])),
                'initial_base_position': actual_initial_base,
                'base_position': saved.get('base_position', actual_initial_base),
                'last_week_position': saved.get('last_week_position', actual_initial_base),
                'initial_position_value': actual_initial_val,
                'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,
                'max_position': saved.get('max_position', actual_initial_base + saved.get('grid_unit', cfg['grid_unit']) * 20),
                'used_atr_rate': saved.get('used_atr_rate', None), 'cached_atr_ema': saved.get('cached_atr_ema', None),
                'buy_stack': saved.get('buy_stack', []), 'sell_stack': saved.get('sell_stack', []),
                'credit_limit': cfg.get('credit_limit', saved.get('credit_limit', StrategyConfig.CREDIT_LIMIT)),
                '_pending_ignore_ids': [],
                'wm_map': saved.get('wm_map', {}),
                'wm_pnl': saved.get('wm_pnl', 0.0)
            })
            heapq.heapify(st['buy_stack'])
            heapq.heapify(st['sell_stack'])
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
                # 🌟 [V3.13.5] 修复：不覆盖 dingtou_base，只更新无损参数，保护滴灌记录
                state.update({
                    'grid_unit': new_params['grid_unit'], 
                    'dingtou_rate': new_params['dingtou_rate'], 
                    'max_position': state['base_position'] + new_params['grid_unit'] * 20
                })

                for key in ['tp_cool_weeks', 'tp_min_weeks', 'tp_min_value']:
                    if key in new_params: state[key] = new_params[key]                
                
                if 'credit_limit' in new_params:
                    new_limit = int(new_params['credit_limit'])
                    if state.get('credit_limit') != new_limit:
                        info('[{}] 🔧 信用额度更新: {} -> {}', dsym(context, sym), state.get('credit_limit'), new_limit)
                        state['credit_limit'] = new_limit
        context.symbol_config = new_config
        _load_symbol_names(context)
        info('✅ 配置文件热重载完成！')
    except Exception as e:
        info(f'❌ 配置文件热重载失败: {e}')

def log_trade_details(context, symbol, trade):
    try:
        trade_log_path = research_path('reports', 'a_trade_details.csv')
        is_new = not trade_log_path.exists()
        with open(trade_log_path, 'a', encoding='utf-8', newline='') as f:
            if is_new: f.write(",".join(["time", "symbol", "direction", "quantity", "price", "base_position_at_trade", "entrust_no"]) + "\n")
            dir_str, base_pos = ("BUY" if trade['entrust_bs'] == '1' else "SELL"), context.state[symbol].get('base_position', 0)
            f.write(",".join([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), symbol, dir_str, str(trade['business_amount']), f"{trade['business_price']:.3f}", str(base_pos), trade.get('entrust_no', 'N/A')]) + "\n")
    except Exception: pass

def update_daily_reports(context, data):
    reports_dir = research_path('reports')
    reports_dir.mkdir(parents=True, exist_ok=True)
    current_date = context.current_dt.strftime("%Y-%m-%d")
    for symbol in context.symbol_list:
        report_file, state, position = reports_dir / f"{symbol}.csv", context.state[symbol], get_position(symbol)
        amount, close_price = position.amount, context.last_valid_price.get(symbol, state['base_price'])
        if not is_valid_price(close_price): close_price = state['base_price']
        weeks, d_base, d_rate = len(state.get('trade_week_set', [])), state['dingtou_base'], state['dingtou_rate']
        cumulative_invest = sum(d_base * (1 + d_rate) ** w for w in range(1, weeks+1))
        row = [current_date, f"{close_price:.3f}", str(weeks), str(weeks), f"{(amount * close_price - state.get('last_week_position', 0) * close_price) / (state.get('last_week_position', 0) * close_price) if state.get('last_week_position', 0)>0 else 0.0:.2%}", f"{(amount * close_price - cumulative_invest) / cumulative_invest if cumulative_invest>0 else 0.0:.2%}", f"{state['initial_position_value'] + d_base * weeks:.2f}", f"{d_base:.0f}", f"{d_base * (1 + d_rate) ** weeks:.0f}", f"{cumulative_invest:.0f}", str(state['initial_base_position']), str(state['base_position']), f"{state['base_position'] * close_price:.0f}", f"{(state['base_position'] - state.get('last_week_position', 0)) * close_price:.0f}", f"{state['base_position'] * close_price - state['initial_position_value']:.0f}", str(state['base_position']), str(amount), str(state['grid_unit']), str(max(0, amount - state['base_position'])), str(state['base_position'] + state['grid_unit'] * 5), str(state['base_position'] + state['grid_unit'] * 15), str(state['max_position']), f"{getattr(position, 'cost_basis', state['base_price']):.3f}", f"{(state['base_position'] - state.get('last_week_position', 0)) * close_price:.3f}", f"{(close_price - getattr(position, 'cost_basis', state['base_price'])) * amount:.0f}"]
        is_new = not report_file.exists()
        with open(report_file, 'a', encoding='utf-8', newline='') as f:
            if is_new: f.write(",".join(["时间","市价","期数","次数","每期总收益率","盈亏比","应到价值","当周应投入金额","当周实际投入金额","实际累计投入金额","定投底仓份额","累计底仓份额","累计底仓价值","每期累计底仓盈利","总累计底仓盈利","底仓","股票余额","单次网格交易数量","可T数量","标准数量","中间数量","极限数量","成本价","对比定投成本","盈亏"]) + "\n")
            f.write(",".join(map(str, row)) + "\n")
        info('✅ [{}] 已更新每日CSV报表', dsym(context, symbol))

# ---------------- 【新增】水位线网格利润重构引擎 ----------------

def _calculate_watermark_grid_pnl(context, symbol, current_P, current_Q, current_PnL):
    """
    [Global Ver: v3.12.15]
    [HUD 雷达专用] 同档水位记录法 (State-Space Cost Reconstruction)
    不依赖任何历史流水，仅通过快照 (P, Q, PnL) 逆向提纯真实的网格 LIFO 利润。
    """
    state = context.state[symbol]
    
    # 1. 计算当前的 净投入本金 V (绝对守恒量)
    current_V = (current_P * current_Q) - current_PnL
    
    # 2. 初始化记忆账本 (字典) 和 累计利润
    if 'wm_map' not in state:
        state['wm_map'] = {}   # 记录 { "股数": 归一化本金 }
        state['wm_pnl'] = 0.0  # 累计提取的网格利润
    
    # 股数作为字典的 Key (剔除浮点误差)
    q_key = str(int(current_Q))
    
    # 3. 计算归一化本金 (把之前提走的利润加回来，用于公平对比)
    normalized_V = current_V + state['wm_pnl']
    
    # 4. 核心碰撞逻辑：查历史账本
    if q_key in state['wm_map']:
        past_V = state['wm_map'][q_key]
        
        # 如果今天同样拿着这么多股，但归一化本金变少了，说明网格套利成功！
        if normalized_V < past_V - 1e-4:  # 容差防浮点漂移
            new_profit = past_V - normalized_V
            
            # 提取真金白银
            state['wm_pnl'] += new_profit
            
            # 利润提取后，归一化本金会自动上升回到历史锚点
            normalized_V = current_V + state['wm_pnl'] 
            
            # [Fix] 调用规范的 StrategyConfig.DEBUG 避免 AttributeError
            if StrategyConfig.DEBUG.ENABLE:
                info('[{}] 💧 水位线解析成功！在 {} 股档位完成套利，重构网格利润: +{:.2f} 元', 
                     dsym(context, symbol), current_Q, new_profit)

    # 5. 刷新该股数档位的最新成本记忆
    state['wm_map'][q_key] = normalized_V
    
    return state['wm_pnl']

# ---------------- 【修改】监控与报表生成 (接入水位线引擎) ----------------

def generate_html_report(context):
    """
    [Global Ver: v3.12.15] [Func Ver: 6.3]
    [Change]: 接入水位线重构算法，在 HUD 大屏精准渲染纯净网格利润与单股降本额。
    """
    try:
        all_metrics = {'group1': [], 'group2': [], 'group3': []}
        total_market_value = 0
        total_unrealized_pnl = 0
        total_realized_pnl = 0
        
        portfolio_val = {'tech': 0, 'gold': 0, 'dividend': 0, 'other': 0}
        pnl_metrics = getattr(context, 'pnl_metrics', {})
        intraday_metrics = getattr(context, 'intraday_metrics', {})
        
        for symbol in context.symbol_list:
            if symbol not in context.state: continue
            state = context.state[symbol]
            position = get_position(symbol)
            
            price = context.last_valid_price.get(symbol, state['base_price'])
            if not is_valid_price(price): price = state['base_price']
            
            pos_amt = position.amount
            market_value = pos_amt * price
            unrealized_pnl = (price - position.cost_basis) * pos_amt if position.cost_basis > 0 else 0
            
            total_market_value += market_value
            total_unrealized_pnl += unrealized_pnl
            total_realized_pnl += pnl_metrics.get(symbol, {}).get('total_realized_pnl', 0)
            
            name_str = dsym(context, symbol, style='short')
            if any(k in name_str for k in ['纳指', '标普', '科技', '互联']): portfolio_val['tech'] += market_value
            elif '黄金' in name_str: portfolio_val['gold'] += market_value
            elif any(k in name_str for k in ['红利', '低波', '收息']): portfolio_val['dividend'] += market_value
            else: portfolio_val['other'] += market_value
            
            config = getattr(context, 'symbol_config', {}).get(symbol, {})
            tp_cool_weeks = state.get('tp_cool_weeks', config.get('tp_cool_weeks', 4))
            min_weeks = state.get('tp_min_weeks', config.get('tp_min_weeks', 12))
            min_val = state.get('tp_min_value', config.get('tp_min_value', 30000))
            
            trade_weeks = state.get('trade_week_set', set())
            current_weeks = len(trade_weeks)
            
            tier = state.get('_tp_tier', 0)
            hwm = state.get('_tp_hwm_ratio', 0.0)
            profit_ratio = (price - position.cost_basis) / position.cost_basis if position.cost_basis > 0 else 0
            atr = state.get('macro_atr_rate', 0.02)
            if not isinstance(atr, (int, float)) or math.isnan(atr): atr = 0.02
            
            max_pos = state.get('max_position', pos_amt + 100)
            
            status_html = ""
            radar_html = ""
            if current_weeks < tp_cool_weeks and min_weeks < 999:
                status_html = '<span class="badge badge-cooldown">❄️ 物理冷却期</span>'
                radar_html = f'<div style="width:110px;"><span class="text-dim">静默断代 (余 {tp_cool_weeks - current_weeks} 周)</span></div>'
            elif min_weeks >= 999:
                status_html = '<span class="badge badge-safe">🟢 信仰长拿</span>'
                radar_html = '<div style="width:110px;"><span class="text-dim">🔒 防线关闭</span></div>'
            elif current_weeks < min_weeks and market_value < min_val:
                status_html = '<span class="badge badge-seed">🌱 幼苗保护期</span>'
                progress = min(100, int((current_weeks / min_weeks) * 100))
                radar_html = f'<div style="width:110px;"><div class="progress-bg"><div class="progress-fill fill-seed" style="width: {progress}%;"></div></div><div class="text-dim" style="margin-top:4px;">养肥中 ({current_weeks}/{min_weeks}周)</div></div>'
            elif tier > 0:
                status_html = f'<span class="badge badge-alert">🔥 Tier {tier} 警戒!</span>'
                drawdown = hwm - profit_ratio
                limit = {1: 3.0 * atr, 2: 5.0 * atr, 3: 8.0 * atr}.get(tier, 0.05)
                risk_pct = min(100, max(0, int((drawdown / limit) * 100)))
                radar_html = f'<div style="width:110px;"><div class="progress-bg"><div class="progress-fill fill-alert" style="width: {risk_pct}%;"></div></div><div class="text-alert" style="margin-top:4px;">距回撤防线 {(limit - drawdown)*100:.1f}%</div></div>'
            else:
                status_html = '<span class="badge badge-safe">🟢 安全发育中</span>'
                tp_threshold = 10.0 * atr
                dist_pct = min(100, max(0, int((profit_ratio / tp_threshold) * 100))) if tp_threshold > 0 else 0
                radar_html = f'<div style="width:110px;"><div class="progress-bg"><div class="progress-fill fill-safe" style="width: {dist_pct}%;"></div></div><div class="text-dim" style="margin-top:4px;">距触发一阶 {(tp_threshold - profit_ratio)*100:.1f}%</div></div>'

            ammo_pct = min(100, int((pos_amt / max_pos) * 100)) if max_pos > 0 else 0
            if ammo_pct < 20: ammo_class, ammo_text = "fill-cooldown", "低仓警戒"
            elif ammo_pct > 85: ammo_class, ammo_text = "fill-alert", "弹药警告"
            else: ammo_class, ammo_text = "fill-safe", "健康"
            
            ammo_html = f'<div style="margin-bottom:4px; white-space:nowrap;"><div class="progress-bg" style="width:60px; display:inline-block; vertical-align:middle; margin-right:6px;"><div class="progress-fill {ammo_class}" style="width: {ammo_pct}%;"></div></div><span style="color:#9aa5ce; font-size:12px;">{ammo_pct}%({ammo_text})</span></div><div style="color:#9aa5ce; font-size:11px; white-space:nowrap;">持仓:{int(pos_amt)}/底仓:{int(state.get("base_position", 0))}</div>'

            grid_atr = state.get('grid_atr_rate')
            grid_atr_disp = f"{grid_atr*100:.2f}%" if isinstance(grid_atr, (int, float)) and not math.isnan(grid_atr) and grid_atr > 0 else "N/A"
            macro_val = state.get('macro_atr_rate')
            macro_atr_disp = "N/A" if min_weeks >= 999 else (f"{macro_val*100:.2f}%" if isinstance(macro_val, (int, float)) and not math.isnan(macro_val) and macro_val > 0 else "N/A")

            symbol_name = dsym(context, symbol, style='long')
            sym_id_js = symbol.replace('.', '_')
            
            symbol_html = f"<div style=\"cursor:pointer; color:#7aa2f7; font-weight:bold; font-size:14px; white-space:nowrap;\" onclick=\"toggleDrawer('{sym_id_js}')\">🔽 {symbol_name}</div><div style=\"color:#9aa5ce; font-size:11px; margin-left:22px; margin-top:2px; white-space:nowrap;\">定投: {current_weeks}周 | 网格: {int(state.get('grid_unit',0))}股</div>"

            # ==========================================
            # 🌟 [V3.12.15 核心升级]: 引入水位线引擎，解析真实网格利润与降本额
            # ==========================================
            broker_total_pnl = getattr(position, 'total_pnl', None)
            if broker_total_pnl is None:
                # 兼容方案：如果券商 API 无法直接提供 total_pnl，利用本地数据近似重构
                local_realized = pnl_metrics.get(symbol, {}).get('total_realized_pnl', 0)
                broker_total_pnl = unrealized_pnl + local_realized

            real_grid_pnl = 0.0
            cost_reduction = 0.0
            if pos_amt > 0:
                real_grid_pnl = _calculate_watermark_grid_pnl(context, symbol, price, pos_amt, broker_total_pnl)
                base_q = state.get('base_position', 100)
                cost_reduction = real_grid_pnl / base_q if base_q > 0 else 0.0

            # 更新 pnl_info 展示逻辑，加入网格截留和底仓降本
            pnl_info = f"""
            <span class="{'text-safe' if unrealized_pnl>=0 else 'text-alert'}">
                浮盈: {unrealized_pnl:,.2f} <br> <b>{(profit_ratio*100):.2f}%</b>
            </span><br>
            <span style="color:#9ece6a; font-size:11px; font-weight:bold;">
                💧网格: +{real_grid_pnl:,.2f}
            </span><br>
            <span style="color:#7dcfff; font-size:11px;">
                🛡️降本: -{cost_reduction:.3f}
            </span>
            """
            # ==========================================

            b_stack = state.get('buy_stack', [])
            s_stack = state.get('sell_stack', [])
            b_str = " | ".join([f"{p:.3f}({v}股)" for p, v in sorted(b_stack, key=lambda x: x[0], reverse=True)[:5]]) if b_stack else "无挂单 (下方真空)"
            s_str = " | ".join([f"{-p:.3f}({v}股)" for p, v in sorted(s_stack, key=lambda x: x[0], reverse=True)[:5]]) if s_stack else "天空毫无阻力 (无套牢单)"
            
            d_base = state.get('dingtou_base', 0)
            d_rate = state.get('dingtou_rate', 0)
            acc_invest = sum(d_base * (1 + d_rate)**w for w in range(1, current_weeks + 1))
            target_val = state.get('initial_position_value', 0) + acc_invest
            
            drawer_html = f"""
            <td colspan="7" style="padding: 0; border: none; white-space: normal;">
                <div id="drawer-{sym_id_js}" class="drawer-content" style="display: none; background: #1f2335; padding: 12px 15px; margin: 4px 10px 15px 10px; border-left: 3px solid #7aa2f7; border-radius: 4px; box-shadow: inset 0 2px 4px rgba(0,0,0,0.2);">
                    <div style="color: #c0caf5; font-size: 13px; margin-bottom: 6px;"><b>🧱 堆栈微观阵地 (Stack Radar):</b></div>
                    <div style="color: #f7768e; font-size: 12px; margin-left: 15px; margin-bottom: 4px;">🔴 <b>上方套牢阻力 (Sell Stack):</b> {s_str}</div>
                    <div style="color: #9ece6a; font-size: 12px; margin-left: 15px; margin-bottom: 8px;">🟢 <b>下方网格支撑 (Buy Stack):</b> {b_str}</div>
                    <div style="color: #c0caf5; font-size: 13px; margin-bottom: 6px;"><b>💧 VA 价值平均引擎 (Engine Status):</b></div>
                    <div style="color: #7dcfff; font-size: 12px; margin-left: 15px;">实际累计投入: {acc_invest:,.2f} 元 &nbsp; | &nbsp; 理论应到价值: {target_val:,.2f} 元</div>
                </div>
            </td>
            """

            item = {
                "symbol": symbol, "sym_id": sym_id_js,
                "symbol_html": symbol_html, "status": status_html, "ammo": ammo_html,
                "price_info": f"{position.cost_basis:.3f} / {price:.3f}", 
                "pnl_info": pnl_info,
                "atr_info": f"{grid_atr_disp} / <br>{macro_atr_disp}",
                "radar": radar_html, "drawer_html": drawer_html
            }
            
            if min_weeks >= 999: all_metrics['group1'].append(item)
            elif min_weeks <= 12: all_metrics['group2'].append(item)
            else: all_metrics['group3'].append(item)
            
        # --- 6. 全局资产比例雷达计算 ---
        try:
            if hasattr(context, 'portfolio') and context.portfolio:
                portfolio_val['other'] += getattr(context.portfolio, 'available_cash', 0)
        except Exception:
            pass

        total_port = sum(portfolio_val.values()) or 1.0
        p_tech = portfolio_val['tech'] / total_port * 100
        p_gold = portfolio_val['gold'] / total_port * 100
        p_div = portfolio_val['dividend'] / total_port * 100
        p_oth = portfolio_val['other'] / total_port * 100
        
        portfolio_html = f"""
        <div style="margin-top: 15px; color: #a9b1d6; font-size: 13px;">
            <div style="display:flex; align-items:center; margin-bottom:8px;">
                <span style="width:160px;">📈 科技/宽基 (纳指等):</span>
                <div style="width:250px; background:#16161e; height:12px; border-radius:6px; overflow:hidden; margin-right:15px;"><div style="width:{p_tech}%; background:#ff9e64; height:100%;"></div></div>
                <span>{p_tech:.1f}%</span>
            </div>
            <div style="display:flex; align-items:center; margin-bottom:8px;">
                <span style="width:160px;">🟨 避险资产 (黄金等):</span>
                <div style="width:250px; background:#16161e; height:12px; border-radius:6px; overflow:hidden; margin-right:15px;"><div style="width:{p_gold}%; background:#e0af68; height:100%;"></div></div>
                <span>{p_gold:.1f}%</span>
            </div>
            <div style="display:flex; align-items:center; margin-bottom:8px;">
                <span style="width:160px;">🟦 价值收息 (红利等):</span>
                <div style="width:250px; background:#16161e; height:12px; border-radius:6px; overflow:hidden; margin-right:15px;"><div style="width:{p_div}%; background:#7aa2f7; height:100%;"></div></div>
                <span>{p_div:.1f}%</span>
            </div>
            <div style="display:flex; align-items:center; margin-bottom:8px;">
                <span style="width:160px;">⬜ 现金与其他 (备用):</span>
                <div style="width:250px; background:#16161e; height:12px; border-radius:6px; overflow:hidden; margin-right:15px;"><div style="width:{p_oth}%; background:#a9b1d6; height:100%;"></div></div>
                <span>{p_oth:.1f}%</span>
            </div>
        </div>
        """
            
        template_file = research_path('config', 'dashboard_template.html')
        if not template_file.exists(): return
        html_template = template_file.read_text(encoding='utf-8')
        
        def render_table(items):
            if not items: return '<tr><td colspan="7" style="text-align:center; color:#565f89; padding: 20px;">暂无标的 / 正在初始化...</td></tr>'
            rows = ""
            for m in items:
                rows += f"<tr class=\"row-main\"><td>{m['symbol_html']}</td><td>{m['status']}</td><td>{m['ammo']}</td><td>{m['price_info']}</td><td>{m['pnl_info']}</td><td>{m['atr_info']}</td><td>{m['radar']}</td></tr>"
                rows += f"<tr id=\"tr-drawer-{m['sym_id']}\" style=\"display:none; background:transparent;\">{m['drawer_html']}</tr>"
            return rows

        final_html = html_template.replace('{update_time}', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        final_html = final_html.replace('{total_market_value}', f"{total_market_value:,.2f}")
        final_html = final_html.replace('{total_unrealized_pnl}', f"{total_unrealized_pnl:,.2f}")
        final_html = final_html.replace('{total_realized_pnl}', f"{total_realized_pnl:,.2f}")
        final_html = final_html.replace('{account_total_pnl}', f"{(total_realized_pnl + total_unrealized_pnl):,.2f}")
        final_html = final_html.replace('{portfolio_radar}', portfolio_html)
        final_html = final_html.replace('{g1_rows}', render_table(all_metrics['group1']))
        final_html = final_html.replace('{g2_rows}', render_table(all_metrics['group2']))
        final_html = final_html.replace('{g3_rows}', render_table(all_metrics['group3']))

        research_path('reports', 'strategy_dashboard.html').write_text(final_html, encoding='utf-8')
    except Exception as e:
        log.error(f"⚠️ 生成 HUD 面板异常: {e}")