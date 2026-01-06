# event_driven_grid_strategy.py
# 版本号：GEMINI-3.2.60-Final
# 
# 更新日志 (v3.2.60):
# 1. 【完整性】补全 update_daily_reports 函数，修复日报生成缺失。
# 2. 【Bug修复】修复 _calculate_local_pnl_lifo 中的变量引用错误 (sym_trades -> trades)。
# 3. 【集成】包含 v3.2.59 的所有修复 (日终撤单增强、14:55 时间互斥)。

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

import numpy as np  # 用于计算 RV
import pandas as pd

# ---------------- 全局句柄与常量 ----------------
LOG_FH = None
LOG_DATE = None
MAX_SAVED_FILLED_IDS = 500
__version__ = 'GEMINI-3.2.59-Fix-EOD-Strict'
TRANSACTION_COST = 0.00005

# ---- 调试默认 ----
DBG_ENABLE_DEFAULT = True
DBG_RT_WINDOW_SEC_DEFAULT = 60
DBG_RT_PREVIEW_DEFAULT = 8
DELAY_AFTER_CANCEL_SECONDS_DEFAULT = 2.0 

# ---- VA 参数 ----
VA_VALUE_THRESHOLD_K_DEFAULT = 1.0
VA_MIN_UPDATE_INTERVAL_MIN_DEFAULT = 60
VA_MAX_UPDATES_PER_DAY_DEFAULT = 3

# ---- 停牌参数 ----
MKT_HALT_SKIP_PLACE_DEFAULT = True
MKT_HALT_SKIP_AFTER_SECONDS_DEFAULT = 180
MKT_HALT_LOG_EVERY_MINUTES_DEFAULT = 10

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
        info('⚠️ 读取 config/names.json 失败: {}（忽略，继续）', e)

    try:
        for sym, cfg in (getattr(context, 'symbol_config', {}) or {}).items():
            if isinstance(cfg, dict) and 'name' in cfg and cfg['name']:
                name_map[sym] = str(cfg['name'])
    except Exception as e:
        info('⚠️ 解析 symbols.json 中的 name 字段失败: {}（忽略，继续）', e)

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
        if isinstance(x, float) and math.isnan(x): return False
        if x <= 0: return False
        return True
    except:
        return False

# ---------------- 状态保存 ----------------

def save_state(symbol, state):
    ids = list(state.get('filled_order_ids', set()))
    state['filled_order_ids'] = set(ids[-MAX_SAVED_FILLED_IDS:])
    store_keys = ['base_price', 'grid_unit', 'max_position', 'last_week_position', 'base_position']
    store = {k: state.get(k) for k in store_keys}
    store['filled_order_ids'] = ids[-MAX_SAVED_FILLED_IDS:]
    store['trade_week_set'] = list(state.get('trade_week_set', []))
    set_saved_param(f'state_{symbol}', store)
    research_path('state', f'{symbol}.json').write_text(json.dumps(store, indent=2), encoding='utf-8')

def safe_save_state(symbol, state):
    try:
        save_state(symbol, state)
    except Exception as e:
        info('[{}] ⚠️ 状态保存失败: {}', symbol, e)

# ---------------- 配置加载 ----------------

def _load_debug_config(context, force=False):
    cfg_file = research_path('config', 'debug.json')
    try:
        mtime = cfg_file.stat().st_mtime if cfg_file.exists() else None
    except:
        mtime = None

    if not force and hasattr(context, 'debug_cfg_mtime') and context.debug_cfg_mtime == mtime:
        return

    enable = DBG_ENABLE_DEFAULT
    winsec = DBG_RT_WINDOW_SEC_DEFAULT
    preview = DBG_RT_PREVIEW_DEFAULT
    delay_after_cancel = getattr(context, 'delay_after_cancel_seconds', DELAY_AFTER_CANCEL_SECONDS_DEFAULT)

    try:
        if cfg_file.exists():
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if isinstance(j, dict):
                if 'enable_debug_log' in j: enable = bool(j['enable_debug_log'])
                if 'rt_heartbeat_window_sec' in j:
                    try: winsec = max(5, int(j['rt_heartbeat_window_sec']))
                    except: pass
                if 'rt_heartbeat_preview' in j:
                    try: preview = max(1, int(j['rt_heartbeat_preview']))
                    except: pass
                if 'delay_after_cancel_seconds' in j:
                    try: delay_after_cancel = max(0.0, float(j['delay_after_cancel_seconds']))
                    except: pass
    except Exception as e:
        info('⚠️ 读取调试文件 config/debug.json 失败: {}（采用默认 enable={}, win={}s, preview={}）',
             e, enable, winsec, preview)

    context.enable_debug_log = enable
    context.rt_heartbeat_window_sec = winsec
    context.rt_heartbeat_preview = preview
    context.delay_after_cancel_seconds = delay_after_cancel
    context.debug_cfg_mtime = mtime
    context.last_rt_log_ts = None
    if enable:
        info('⚙️ 调试配置生效: enable={} window={}s preview={} delay_after_cancel={}s',
             enable, winsec, preview, delay_after_cancel)
    else:
        info('⚙️ 调试配置生效: enable=False（关闭心跳日志）')

# ---------------- VA 参数：config/va.json ----------------

def _load_va_config(context, force=False):
    cfg_file = research_path('config', 'va.json')
    try:
        mtime = cfg_file.stat().st_mtime if cfg_file.exists() else None
    except:
        mtime = None

    if (not force) and hasattr(context, 'va_cfg_mtime') and context.va_cfg_mtime == mtime:
        return

    k = VA_VALUE_THRESHOLD_K_DEFAULT
    min_int = VA_MIN_UPDATE_INTERVAL_MIN_DEFAULT
    max_per_day = VA_MAX_UPDATES_PER_DAY_DEFAULT
    try:
        if cfg_file.exists():
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if isinstance(j, dict):
                if 'value_threshold_k' in j:
                    try: k = max(0.0, float(j['value_threshold_k']))
                    except: pass
                if 'min_update_interval_minutes' in j:
                    try: min_int = max(0, int(j['min_update_interval_minutes']))
                    except: pass
                if 'max_updates_per_day' in j:
                    try: max_per_day = max(0, int(j['max_updates_per_day']))
                    except: pass
    except Exception as e:
        info('⚠️ 读取 VA 配置失败: {}（采用默认 k={}, minInt={}m, maxDaily={}）',
             e, k, min_int, max_per_day)

    context.va_value_threshold_k = k
    context.va_min_update_interval_minutes = min_int
    context.va_max_updates_per_day = max_per_day
    context.va_cfg_mtime = mtime
    info('⚙️ VA配置生效: k={} minInterval={}m maxDaily={}', k, min_int, max_per_day)

# ---------------- 市场参数：config/market.json ----------------

def _load_market_config(context, force=False):
    cfg_file = research_path('config', 'market.json')
    try:
        mtime = cfg_file.stat().st_mtime if cfg_file.exists() else None
    except:
        mtime = None

    if (not force) and hasattr(context, 'market_cfg_mtime') and context.market_cfg_mtime == mtime:
        return

    halt_skip = MKT_HALT_SKIP_PLACE_DEFAULT
    halt_after = MKT_HALT_SKIP_AFTER_SECONDS_DEFAULT
    halt_log_m = MKT_HALT_LOG_EVERY_MINUTES_DEFAULT
    try:
        if cfg_file.exists():
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if isinstance(j, dict):
                if 'halt_skip_place' in j:
                    halt_skip = bool(j['halt_skip_place'])
                if 'halt_skip_after_seconds' in j:
                    try: halt_after = max(0, int(j['halt_skip_after_seconds']))
                    except: pass
                if 'halt_log_every_minutes' in j:
                    try: halt_log_m = max(1, int(j['halt_log_every_minutes']))
                    except: pass
    except Exception as e:
        info('⚠️ 读取 market 配置失败: {}（采用默认 skip={} after={}s logEvery={}m）',
             e, halt_skip, halt_after, halt_log_m)

    context.halt_skip_place = halt_skip
    context.halt_skip_after_seconds = halt_after
    context.halt_log_every_minutes = halt_log_m
    context.market_cfg_mtime = mtime
    info('⚙️ 市场配置生效: haltSkip={} after={}s logEvery={}',
         halt_skip, halt_after, halt_log_m)

# ---------------- 统一参数：config/strategy.json ----------------
def _load_strategy_config(context, force=False):
    strat_file = research_path('config', 'strategy.json')
    try:
        mtime = strat_file.stat().st_mtime if strat_file.exists() else None
    except:
        mtime = None

    if (not force) and hasattr(context, 'strategy_cfg_mtime') and context.strategy_cfg_mtime == mtime:
        return

    if not strat_file.exists():
        context.strategy_cfg_mtime = None
        return

    try:
        j = json.loads(strat_file.read_text(encoding='utf-8')) or {}
    except Exception as e:
        info('⚠️ 读取统一配置 strategy.json 失败: {}（保留现有参数）', e)
        return

    # 覆盖 debug
    dbg = j.get('debug') or {}
    if isinstance(dbg, dict) and dbg:
        if 'enable_debug_log' in dbg: context.enable_debug_log = bool(dbg['enable_debug_log'])
        if 'rt_heartbeat_window_sec' in dbg:
            try: context.rt_heartbeat_window_sec = max(5, int(dbg['rt_heartbeat_window_sec']))
            except: pass
        if 'rt_heartbeat_preview' in dbg:
            try: context.rt_heartbeat_preview = max(1, int(dbg['rt_heartbeat_preview']))
            except: pass
        if 'delay_after_cancel_seconds' in dbg:
            try: context.delay_after_cancel_seconds = max(0.0, float(dbg['delay_after_cancel_seconds']))
            except: pass

    # 覆盖 va
    va = j.get('va') or {}
    if isinstance(va, dict) and va:
        if 'value_threshold_k' in va:
            try: context.va_value_threshold_k = max(0.0, float(va['value_threshold_k']))
            except: pass
        if 'min_update_interval_minutes' in va:
            try: context.va_min_update_interval_minutes = max(0, int(va['min_update_interval_minutes']))
            except: pass
        if 'max_updates_per_day' in va:
            try: context.va_max_updates_per_day = max(0, int(va['max_updates_per_day']))
            except: pass

    # 覆盖 market
    mk = j.get('market') or {}
    if isinstance(mk, dict) and mk:
        if 'halt_skip_place' in mk: context.halt_skip_place = bool(mk['halt_skip_place'])
        if 'halt_skip_after_seconds' in mk:
            try: context.halt_skip_after_seconds = max(0, int(mk['halt_skip_after_seconds']))
            except: pass
        if 'halt_log_every_minutes' in mk:
            try: context.halt_log_every_minutes = max(1, int(mk['halt_log_every_minutes']))
            except: pass

    context.strategy_cfg_mtime = mtime
    info('🛠️ 统一参数生效：读取 strategy.json 并覆盖子配置（delay_after_cancel={}s）',
         getattr(context, 'delay_after_cancel_seconds', DELAY_AFTER_CANCEL_SECONDS_DEFAULT))

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
    context.pending_frozen = {} # 虚拟冻结持仓
    
    # 【新增】存储日内RV等指标
    context.intraday_metrics = {}

    # 成交去重 ring（5s 有效）
    context.recent_fill_ring = deque(maxlen=200)

    # 初始化每个标的状态
    for sym, cfg in context.symbol_config.items():
        state_file = research_path('state', f'{sym}.json')
        # 【关键】初始化时正确读取磁盘上的 base_position
        saved = json.loads(state_file.read_text(encoding='utf-8')) if state_file.exists() else get_saved_param(f'state_{sym}', {}) or {}
        st = {**cfg}
        st.update({
            'base_price': saved.get('base_price', cfg['base_price']),
            'grid_unit': saved.get('grid_unit', cfg['grid_unit']),
            'filled_order_ids': set(saved.get('filled_order_ids', [])),
            'trade_week_set': set(saved.get('trade_week_set', [])),
            
            # 【关键】这里优先用 saved，否则用 initial
            'base_position': saved.get('base_position', cfg['initial_base_position']),
            'last_week_position': saved.get('last_week_position', cfg['initial_base_position']),
            
            'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
            'buy_grid_spacing': 0.005,
            'sell_grid_spacing': 0.005,
            'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20),
            'va_last_update_dt': None,
            'va_update_count_date': None,
            'va_updates_today': 0,
            '_halt_next_log_dt': None,
            '_oo_last': 0,
            '_recover_until': None,
            '_after_cancel_until': None,
            '_oo_drop_seen_ts': None,
            '_pos_jump_seen_ts': None,
            '_pos_confirm_deadline': None,
            
            # 【v3.2.51】: 使用 _rehang_due_ts 替代 _trade_lock_until
            '_rehang_due_ts': None,
            
            # 【v3.2.54】: 修正锁
            '_ignore_place_until': None,
            
            # 【v3.2.56】: 撤单ID缓存
            '_pending_ignore_ids': []
        })
        context.state[sym] = st
        context.latest_data[sym] = st['base_price']
        context.should_place_order_map[sym] = True
        context.mark_halted[sym] = False
        context.last_valid_price[sym] = st['base_price']
        context.last_valid_ts[sym] = None
        context.pending_frozen[sym] = 0 # 初始化冻结量
        
        if '_pos_change' in st: st.pop('_pos_change')
        if '_last_pos_seen' in st: st.pop('_last_pos_seen')
        if '_rehang_bypass_once' in st: st.pop('_rehang_bypass_once')
        # 清理旧版锁变量
        if '_trade_lock_until' in st: st.pop('_trade_lock_until')

    context.boot_dt = getattr(context, 'current_dt', None) or datetime.now()
    context.boot_grace_seconds = int(get_saved_param('boot_grace_seconds', 180))
    context.delay_after_cancel_seconds = DELAY_AFTER_CANCEL_SECONDS_DEFAULT
    
    # 看板上次更新时间
    context.last_report_time = None

    _load_debug_config(context, force=True)
    _load_va_config(context, force=True)
    _load_market_config(context, force=True)
    _load_strategy_config(context, force=True)

    context.initial_cleanup_done = False
    
    # 【新增】: 启动时进行数据修复检测
    _repair_state_logic(context)
    
    if '回测' not in context.env:
        run_daily(context, place_auction_orders, time='9:15')
        
        # 提前收盘处理时间到 14:55，防止撤单超时
        run_daily(context, end_of_day, time='14:55')
        
        # 【核心新增 v3.2.50】注册每3秒的高频巡检，用于处理补单（替代 sleep）
        run_interval(context, check_pending_rehangs, seconds=3)
        
        info('✅ 事件驱动模式就绪 (Async State Machine Active)')

    # --- 【PnL 指标初始化】 ---
    context.pnl_metrics_path = research_path('state', 'pnl_metrics.json')
    context.pnl_metrics = _load_pnl_metrics(context.pnl_metrics_path)
    
    info('✅ PnL 收益指标已加载（共 {} 个标的）', len(context.pnl_metrics))
    # --- PnL 初始化结束 ---
    
    info('✅ 初始化完成，版本:{}', __version__)

# ---------------- 【新增】数据自动修复逻辑 ----------------

def _repair_state_logic(context):
    info('🛠️ [Data Repair] 开始检查并修复潜在的底仓数据异常...')
    for sym in context.symbol_list:
        state = context.state[sym]
        weeks = len(state.get('trade_week_set', []))
        
        # 如果没有定投记录，或者刚开始，跳过
        if weeks <= 0:
            continue
            
        # 1. 计算理论应有的目标市值 (VA公式)
        # Target = Initial + Sum(Invest * (1+r)^w)
        d_base = state.get('dingtou_base', 0)
        d_rate = state.get('dingtou_rate', 0)
        acc_invest = sum(d_base * (1 + d_rate)**w for w in range(1, weeks + 1))
        target_val = state['initial_position_value'] + acc_invest
        
        # 2. 获取当前用于计算份额的参考价 (Base Price)
        price = state['base_price']
        if price <= 0: continue
            
        # 3. 计算理论底仓份额 (向下取整到100股，更保守)
        theoretical_pos = int(target_val / price / 100) * 100
        
        # 4. 检查偏差
        current_pos = state['base_position']
        # 容忍度：如果当前底仓 < 理论值的 70% (说明严重偏离/被重置)，且理论值大于初始底仓
        if current_pos < theoretical_pos * 0.70 and theoretical_pos > state['initial_base_position']:
            info(f"[{dsym(context, sym)}] ⚠️ 发现底仓异常! 当前:{current_pos} vs 理论:{theoretical_pos} (周数:{weeks})... 正在执行自动修复。")
            state['base_position'] = theoretical_pos
            state['last_week_position'] = theoretical_pos # 同步修复上周数据，防止下周跳变
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
    # 【PnL 修复 v3.2.24】：启动时强制重置，且大幅扩大回溯范围
    if '回测' not in context.env:
        info('🔄 [PnL Reset] 强制重置 PnL 状态并回溯补算 (Scope: 45 days)...')
        
        # 1. 强制清空内存中的指标
        context.pnl_metrics = {} 
        
        try:
            # 2. 调用补算，指定 lookback_days=45
            _calculate_local_pnl_lifo(context) # 使用 LIFO 引擎
        except Exception as e:
            info('⚠️ PnL 补算遇到轻微错误: {} (后续会重试)', e)
            
        # 3. 立即刷新看板
        generate_html_report(context)
        context.last_report_time = context.current_dt

    if context.initial_cleanup_done:
        return
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
    if '回测' in context.env or not hasattr(context, 'symbol_list'):
        return
    
    # 全局闪电撤单
    _fast_cancel_all_orders_global(context)
    
    # 清理内存状态
    for sym in context.symbol_list:
        if sym in context.state:
            context.state[sym].pop('_pos_change', None)
            context.state[sym].pop('_last_pos_seen', None)
            context.state[sym].pop('_trade_lock_until', None) # 清理旧版锁
            context.state[sym].pop('_rehang_due_ts', None) # 清理补单标记
            context.state[sym].pop('_pending_ignore_ids', None) # 清理ID缓存
        context.pending_frozen[sym] = 0
    info('✅ 全局清理完成')

def _fast_cancel_all_orders_global(context):
    """全局闪电撤单，一次IO获取所有，批量撤单"""
    info('⚡ 执行全局闪电撤单 (Flash Cancel)...')
    try:
        all_orders = get_all_orders()
        if not all_orders:
            return
        
        to_cancel = []
        for o in all_orders:
            sym = o.get('symbol') or o.get('stock_code')
            if convert_symbol_to_standard(sym) not in context.symbol_list:
                continue
            
            status = str(getattr(o, 'status', ''))
            if status in ['2', '7']: 
                to_cancel.append(o)
        
        if to_cancel:
            info('⚡ 扫描到 {} 笔有效挂单，瞬间并发撤销...', len(to_cancel))
            for o in to_cancel:
                try:
                    cancel_order_ex(o)
                    
                    o_amt = getattr(o, 'amount', 0)
                    if o_amt == 0 and isinstance(o, dict):
                        o_amt = o.get('amount', 0)
                    
                    o_sym = convert_symbol_to_standard(o.get('symbol') or o.get('stock_code'))
                    # 兼容对象和字典
                    bs = str(getattr(o, 'entrust_bs', ''))
                    is_sell = (o_amt < 0) or (bs == '2')
                    
                    if is_sell and o_sym in context.pending_frozen:
                        frozen = abs(o_amt)
                        context.pending_frozen[o_sym] = max(0, context.pending_frozen[o_sym] - frozen)

                except Exception:
                    pass 
        else:
            info('⚡ 无需撤单。')
            
    except Exception as e:
        info('⚠️ 全局撤单异常: {}', e)

# ---------------- 订单与撤单工具 ----------------

def get_order_status(entrust_no):
    try:
        order_detail = get_order(entrust_no)
        return str(order_detail.get('status', '')) if order_detail else ''
    except Exception as e:
        info('⚠️ 查询订单状态失败 entrust_no={}: {}', entrust_no, e)
        return ''

def cancel_all_orders_by_symbol(context, symbol):
    """
    [更新 v3.2.56] 返回已撤销的订单号集合，用于防止幽灵单。
    """
    all_orders = get_all_orders() or []
    total = 0
    cancelled_ids = set()
    
    if not hasattr(context, 'canceled_cache'):
        context.canceled_cache = {'date': None, 'orders': set()}
    today = context.current_dt.date()
    if context.canceled_cache.get('date') != today:
        context.canceled_cache = {'date': today, 'orders': set()}
    cache = context.canceled_cache['orders']

    for o in all_orders:
        api_sym = o.get('symbol') or o.get('stock_code') 
        if convert_symbol_to_standard(api_sym) != symbol:
            continue
        status = str(getattr(o, 'status', ''))
        entrust_no = o.get('entrust_no')
        
        if (not entrust_no
            or status in ('8', '5', '6', '4') 
            or entrust_no in context.state[symbol]['filled_order_ids']
            or entrust_no in cache):
            continue
            
        final_status = get_order_status(entrust_no)
        if final_status in ('8', '4', '5', '6'):
            continue
            
        cache.add(entrust_no)
        total += 1
        info('[{}] 👉 发现并尝试撤销遗留挂单 entrust_no={}', dsym(context, symbol), entrust_no)
        try:
            cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})
            if entrust_no:
                cancelled_ids.add(entrust_no)
            
            o_amt = getattr(o, 'amount', 0)
            if o_amt == 0 and isinstance(o, dict):
                o_amt = o.get('amount', 0)
                
            # 兼容对象和字典
            bs = str(getattr(o, 'entrust_bs', ''))
            is_sell = (o_amt < 0) or (bs == '2')
            
            if is_sell:
                frozen = abs(o_amt)
                context.pending_frozen[symbol] = max(0, context.pending_frozen.get(symbol, 0) - frozen)

        except Exception as e:
            info('[{}] ⚠️ 撤单异常 entrust_no={}: {}', dsym(context, symbol), entrust_no, e)
            
    return cancelled_ids

# ---------------- 集合竞价挂单 ----------------

def place_auction_orders(context):
    if '回测' in context.env or not (is_auction_time() or is_main_trading_time()):
        return
    info('🆕 开始集合竞价挂单流程 (并发模式)...')
    
    # 1. 全局清理 (Flash Cancel)
    _fast_cancel_all_orders_global(context)
    
    # 2. 准备挂单数据
    orders_batch = []
    
    for sym in context.symbol_list:
        state = context.state[sym]
        # 重置防抖
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
            orders_batch.append({
                'symbol': sym, 'side': 'buy', 'price': buy_p, 'amount': unit, 'desc': f'买入 {unit}'
            })
            
        if enable >= unit and pos - unit >= state['base_position']:
            orders_batch.append({
                'symbol': sym, 'side': 'sell', 'price': sell_p, 'amount': -unit, 'desc': f'卖出 {unit}'
            })
            
        safe_save_state(sym, state)

    # 3. 爆发式下单
    info('🚀 生成 {} 笔挂单任务，开始密集发送...', len(orders_batch))
    
    count = 0
    for task in orders_batch:
        try:
            if count > 0 and count % 5 == 0:
                time.sleep(0.05) # Flow Control
                
            order(task['symbol'], task['amount'], limit_price=task['price'])
            
            if task['amount'] < 0:
                sym = task['symbol']
                context.pending_frozen[sym] = context.pending_frozen.get(sym, 0) + abs(task['amount'])
            
            count += 1
        except Exception as e:
            info('⚠️ 下单失败 [{}]: {}', task['symbol'], e)

# ---------------- 实时价：快照获取 + 心跳日志 ----------------

def _fetch_quotes_via_snapshot(context):
    _load_debug_config(context, force=False)
    _load_va_config(context, force=False)
    _load_market_config(context, force=False)
    _load_strategy_config(context, force=False)

    symbols = list(getattr(context, 'symbol_list', []) or [])
    if not symbols:
        return

    snaps = {}
    try:
        snaps = get_snapshot(symbols) or {}
    except Exception as e:
        if getattr(context, 'enable_debug_log', False):
            info('💓 RT心跳 获取快照异常: {}', e)
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
            if not is_valid_price(px):
                px = snap.get('last') or snap.get('price')
        if is_valid_price(px):
            px = float(px)
            context.latest_data[sym] = px
            context.last_valid_price[sym] = px
            context.last_valid_ts[sym] = now_dt
            got += 1
        else:
            miss_list.append(sym)

    if getattr(context, 'enable_debug_log', False):
        need_log = False
        if not hasattr(context, 'last_rt_log_ts') or context.last_rt_log_ts is None:
            need_log = True
        else:
            winsec = int(getattr(context, 'rt_heartbeat_window_sec', DBG_RT_WINDOW_SEC_DEFAULT))
            need_log = (now_dt - context.last_rt_log_ts).total_seconds() >= winsec
        if need_log:
            context.last_rt_log_ts = now_dt
            preview_n = int(getattr(context, 'rt_heartbeat_preview', DBG_RT_PREVIEW_DEFAULT))
            miss_preview = ','.join(miss_list[:preview_n]) + ('...' if len(miss_list) > preview_n else '')
            info('💓 RT心跳 {} got:{}/{} miss:[{}]',
                 now_dt.strftime('%H:%M'), got, len(symbols), miss_preview)

# ---------------- 日志辅助：订单簿 dump ----------------

def _dump_open_orders(context, symbol, tag='DUMP'):
    try:
        oo = [o for o in (get_open_orders(symbol) or []) if getattr(o, 'status', None) == '2']
        if not oo:
            info('[{}]     OPEN-ORDERS {}: 空', dsym(context, symbol), tag)
            return
        lines = []
        for o in oo:
            lines.append(f"#{getattr(o,'entrust_no',None)} side={'B' if o.amount>0 else 'S'} px={getattr(o,'price',None)} amt={o.amount} status={getattr(o,'status',None)}")
        info('[{}]     OPEN-ORDERS {}: {} 笔 -> {}', dsym(context, symbol), tag, len(oo), ' | '.join(lines))
    except Exception as e:
        info('[{}] ⚠️ OPEN-ORDERS {} 读取失败: {}', dsym(context, symbol), tag, e)

# ---------------- 小工具：成交去重 & 窗口判断 ----------------

def _make_fill_key(symbol, amount, price, when):
    side = 1 if amount > 0 else -1
    bucket = when.replace(second=0, microsecond=0)  # 分钟桶
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
        if k[:-1] == key[:-1]:  # 忽略价的细微差异
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

# ---------------- 【核心更新】异步补单状态机 ----------------

def check_pending_rehangs(context):
    """
    每3秒运行一次。
    """
    # 【Fix v3.2.59】: 14:55:00 准时停止补单
    if context.current_dt.time() >= dtime(14, 55):
        return

    # 【Fix v3.2.53】避开 9:30 和 13:00 两个特殊分钟
    now_t = context.current_dt.time()
    if (now_t.hour == 9 and now_t.minute == 30) or \
       (now_t.hour == 13 and now_t.minute == 0):
        return

    # 【Fix v3.2.51】: 使用服务器真实时间进行检查
    now_wall = datetime.now()
    
    for sym in context.symbol_list:
        if sym not in context.state:
            continue
            
        state = context.state[sym]
        rehang_ts = state.get('_rehang_due_ts')
        
        # 如果设置了补单时间，且当前墙钟时间已达到
        if rehang_ts and now_wall >= rehang_ts:
            info('[{}] ⏰ 补单冷却期已过 (due={}), 触发挂单...', dsym(context, sym), rehang_ts.strftime('%H:%M:%S'))
            
            # 清除标记，防止 handle_data 再次跳过或重复执行
            state['_rehang_due_ts'] = None
            
            # 【修复 v3.2.56】获取并清理待忽略的ID列表
            ignore_ids = set(state.get('_pending_ignore_ids', []))
            if '_pending_ignore_ids' in state:
                state.pop('_pending_ignore_ids')

            # 执行补单 (ignore_cooldown=True)
            # 注意：此处不传入 bypass_lock=True，遵守修正锁
            place_limit_orders(context, sym, state, 
                               ignore_cooldown=True,
                               ignore_entrust_nos=ignore_ids)
            safe_save_state(sym, state)

# ---------------- 【核心新增】实时冻结量校准 ----------------

def _recalc_pending_frozen(context, symbol):
    """
    【v3.2.58 修复】: 兼容 Order 对象和字典，修复 .get() 报错。
    """
    try:
        orders = get_open_orders(symbol) or []
        frozen = 0
        for o in orders:
            # 兼容对象(getattr)和字典
            status = str(getattr(o, 'status', ''))
            
            # 状态: 2=已报, 7=部成 (PTRADE API)
            if status in ['2', '7']: 
                amt = getattr(o, 'amount', 0)
                # 如果是字典，getattr 可能拿到 0 或 None，需再次确认
                if amt == 0 and isinstance(o, dict):
                    amt = o.get('amount', 0)
                
                # 识别卖单
                bs = str(getattr(o, 'entrust_bs', ''))
                if isinstance(o, dict) and not bs:
                    bs = str(o.get('entrust_bs', ''))
                    
                is_sell = (amt < 0) or (bs == '2')
                if is_sell:
                    frozen += abs(amt)
        
        context.pending_frozen[symbol] = frozen
        return frozen
    except Exception as e:
        # 只有在调试模式下才频繁报错，否则降级处理
        if getattr(context, 'enable_debug_log', False):
            info('[{}] ⚠️ 同步冻结量失败: {}', dsym(context, symbol), e)
        return context.pending_frozen.get(symbol, 0)

# ---------------- 网格限价挂单主逻辑 ----------------

def place_limit_orders(context, symbol, state, ignore_cooldown=False, bypass_lock=False, ignore_entrust_nos=None):
    """
    Args:
        bypass_lock (bool): 是否绕过修正锁。只有 patrol 函数有权设为 True。
                            check_pending_rehangs 和 handle_data 必须为 False。
        ignore_entrust_nos (set): 需要主动忽略的订单号集合（用于过滤幽灵订单）
    """
    # 【Fix v3.2.59】: 绝对时间锁 - 14:55:00 毫秒级截止
    # 只要到了 14:55:00，强制退出，不给撤单后反向挂单任何机会
    if context.current_dt.time() >= dtime(14, 55):
        return

    # 【v3.2.57 Fix】: 无论何时进入下单逻辑，先校准冻结量
    _recalc_pending_frozen(context, symbol)
    
    now_dt = context.current_dt
    dbg_tag = f"[{dsym(context, symbol)}]"
    
    # 【Fix v3.2.54】: 修正锁检查 (Correction Lock)
    # 如果当前处于巡检修正期(撤单真空期)，且不是 patrol 主动调用，则避让
    if not bypass_lock:
        ignore_until = state.get('_ignore_place_until')
        if ignore_until and datetime.now() < ignore_until:
             return

    # 【Fix v3.2.51】: 严格互斥逻辑
    # 只要存在补单计划（_rehang_due_ts 不为空），无论时间是否到达，本函数（由handle_data/patrol调用）
    # 都必须避让，绝对不执行下单。只有 check_pending_rehangs 有权处理。
    if state.get('_rehang_due_ts') is not None:
        return

    if (not ignore_cooldown) and state.get('_last_trade_ts') \
      and (now_dt - state['_last_trade_ts']).total_seconds() < 60:
        return

    if is_order_blocking_period():
        info('{} ❎ PLACE-SKIP REASON=BLOCKING_PERIOD(9:25-9:30)', dbg_tag)
        return
    
    # 【Fix v3.2.59】: 逻辑时间窗检查
    in_limit_window = is_auction_time() or (is_main_trading_time() and now_dt.time() < dtime(14, 55))
    if not in_limit_window:
        return

    if is_main_trading_time() and not is_auction_time():
        if getattr(context, 'halt_skip_place', MKT_HALT_SKIP_PLACE_DEFAULT):
            last_ts = context.last_valid_ts.get(symbol)
            halt_after = int(getattr(context, 'halt_skip_after_seconds', MKT_HALT_SKIP_AFTER_SECONDS_DEFAULT))
            if context.mark_halted.get(symbol, False) and last_ts:
                if (now_dt - last_ts).total_seconds() >= halt_after:
                    next_log = state.get('_halt_next_log_dt')
                    if (not next_log) or now_dt >= next_log:
                        info('[{}] ⛔ 停牌/断流超过{}s：暂停新挂单（保留已挂单，不撤）。', dsym(context, symbol), halt_after)
                        state['_halt_next_log_dt'] = now_dt + timedelta(minutes=int(getattr(context, 'halt_log_every_minutes', MKT_HALT_LOG_EVERY_MINUTES_DEFAULT)))
                        safe_save_state(symbol, state)
                    return

    boot_grace = (now_dt - getattr(context, 'boot_dt', now_dt)).total_seconds() < getattr(context, 'boot_grace_seconds', 180)
    allow_tickless = boot_grace or is_auction_time()

    base = state['base_price']
    unit, buy_sp, sell_sp = state['grid_unit'], state['buy_grid_spacing'], state['sell_grid_spacing']
    buy_p, sell_p = round(base * (1 - buy_sp), 3), round(base * (1 + sell_sp), 3)

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
        if last_ts and (now_dt - last_ts).seconds < 30:
            return
        last_bp = state.get('_last_order_bp')
        if last_bp and abs(base / last_bp - 1) < buy_sp / 2:
            return
    
    state['_last_order_ts'], state['_last_order_bp'] = now_dt, base

    try:
        raw_open_orders = get_open_orders(symbol) or []
        open_orders = []
        
        # 【核心修复 v3.2.56】双重幽灵过滤
        ignore_set = set(ignore_entrust_nos) if ignore_entrust_nos else set()
        filled_ids = state.get('filled_order_ids', set())
        
        for o in raw_open_orders:
             if str(getattr(o, 'status', '')) == '2':
                 eid = getattr(o, 'entrust_no', None)
                 if eid and eid in ignore_set:
                     continue
                 if eid and eid in filled_ids:
                     continue
                     
                 open_orders.append(o)
        
        # 【Fix v3.2.54】: 严格有单即止
        same_buy   = any(o.amount > 0 for o in open_orders)
        same_sell = any(o.amount < 0 for o in open_orders)

        enable_amount = position.enable_amount
        state['_oo_last'] = len(open_orders)
        state['_last_pos_seen'] = pos 

        can_buy = not same_buy
        if can_buy and pos + unit <= state['max_position']:
            info('[{}] --> 发起买入委托: {}股 @ {:.3f}', dsym(context, symbol), unit, buy_p)
            order(symbol, unit, limit_price=buy_p)
        else:
            if not can_buy:
                pass
            elif pos + unit > state['max_position']:
                info('{} ❎ BUY-SKIP REASON=POS_CAP pos={} unit={} max_pos={}', dbg_tag, pos, unit, state['max_position'])

        can_sell = not same_sell
        
        pending_frozen = context.pending_frozen.get(symbol, 0)
        real_enable = enable_amount - pending_frozen
        
        if can_sell and real_enable >= unit and pos - unit >= state['base_position']:
            info('[{}] --> 发起卖出委托: {}股 @ {:.3f} (可用:{}, 冻结:{})', 
                 dsym(context, symbol), unit, sell_p, enable_amount, pending_frozen)
            order(symbol, -unit, limit_price=sell_p)
            context.pending_frozen[symbol] = pending_frozen + unit
        else:
            reasons = []
            if not can_sell:
                pass
            if real_enable < unit:
                reasons.append(f'ENABLE_LT_UNIT enable={enable_amount} frozen={pending_frozen} unit={unit}')
            if pos - unit < state['base_position']:
                reasons.append(f'BASE_GUARD pos={pos} base={state["base_position"]} unit={unit}')
            if reasons:
                info('{} ❎ SELL-SKIP REASONS={}', dbg_tag, ';'.join(reasons))

    except Exception as e:
        info('[{}] ⚠️ 限价挂单异常：{}', dsym(context, symbol), e)
    finally:
        state.pop('_rehang_bypass_once', None)
        safe_save_state(symbol, state)

# ---------------- 成交回报与后续挂单 ----------------

def on_trade_response(context, trade_list):
    for tr in trade_list:
        if str(tr.get('status')) != '8':
            continue
        sym = convert_symbol_to_standard(tr['stock_code'])
        entrust_no = tr['entrust_no']
        
        log_trade_details(context, sym, tr) 
        
        if sym not in context.state or entrust_no in context.state[sym]['filled_order_ids']:
            continue

        amount = tr['business_amount'] if tr['entrust_bs']=='1' else -tr['business_amount']
        price  = tr['business_price']
        key = _make_fill_key(sym, amount, price, context.current_dt)
        if _is_dup_fill(context, key):
            info('[{}]     DUP-TRADE 回报去重: amt={} px={}', dsym(context, sym), amount, price)
            continue
        _remember_fill(context, key)

        context.state[sym]['filled_order_ids'].add(entrust_no)
        safe_save_state(sym, context.state[sym])
        order_obj = SimpleNamespace(
            order_id = entrust_no,
            amount   = amount,
            filled   = abs(amount),
            price    = price
        )
        try:
            on_order_filled(context, sym, order_obj)
        except Exception as e:
            info('[{}] ❌ 成交处理失败：{}', dsym(context, sym), e)

def on_order_filled(context, symbol, order):
    state = context.state[symbol]
    if order.filled == 0:
        return
    
    if order.amount < 0: # 卖单
        current_frozen = context.pending_frozen.get(symbol, 0)
        context.pending_frozen[symbol] = max(0, current_frozen - abs(order.filled))

    last_dt = state.get('_last_fill_dt')
    last_price = state.get('last_fill_price', 0)
    time_diff = (context.current_dt - last_dt).total_seconds() if last_dt else 999
    
    is_jitter = False
    if last_dt and time_diff < 10 and last_price > 0:
        # 偏差率 < 0.1%
        if abs(order.price - last_price) / last_price < 0.001:
            is_jitter = True
            
    if is_jitter:
        info('[{}] ⏭️ 忽略短时微小价差抖动: 上次{:.3f} 当前{:.3f} (dt={:.1f}s)', 
             dsym(context, symbol), last_price, order.price, time_diff)
        return

    trade_direction = "买入" if order.amount > 0 else "卖出"
    info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f}', dsym(context, symbol), trade_direction, order.filled, order.price)

    state['_last_trade_ts'] = context.current_dt
    state['_last_fill_dt'] = context.current_dt
    state['last_fill_price'] = order.price
    state['base_price'] = order.price

    # 【修复 v3.2.56】保存撤销的ID，供 rehang 使用
    cancelled_ids = cancel_all_orders_by_symbol(context, symbol)
    if cancelled_ids:
        state['_pending_ignore_ids'] = list(cancelled_ids)

    # 【Fix v3.2.51】: 使用 datetime.now() 获取真实时间，避免 K线时间滞后导致锁失效
    delay_s = float(getattr(context, 'delay_after_cancel_seconds', DELAY_AFTER_CANCEL_SECONDS_DEFAULT))
    # 设置一个 2秒 后的墙钟时间戳
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

# ---------------- FILL-RECOVER：补偿式成交检测（含误判保护） ----------------

def _fill_recover_watch(context, symbol, state):
    now_dt = context.current_dt
    in_window = False
    
    if _in_reopen_window(now_dt.time()):
        in_window = True
    if state.get('_after_cancel_until') and now_dt <= state['_after_cancel_until']:
        in_window = True
    if state.get('_recover_until') and now_dt <= state['_recover_until']:
        in_window = True

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
        info('[{}] ⚠️ _fill_recover_watch 获取状态失败: {}', dsym(context, symbol), e)
        return

    if state.get('_last_pos_seen') is None:
        state['_last_pos_seen'] = pos_now

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
            info('[{}]     观察到持仓跳变 posΔ={}(无掉单)，进入2s确认期 (防鬼数据)', dsym(context, symbol), pos_delta)
        state['_oo_drop_seen_ts'] = None

    elif oo_drop_now and pos_jump_now:
        if state.get('_oo_drop_seen_ts') is None and state.get('_pos_jump_seen_ts') is None:
             state['_oo_drop_seen_ts'] = now_dt
             state['_pos_jump_seen_ts'] = now_dt
             state['_pos_confirm_deadline'] = now_dt + timedelta(seconds=2.0)
             info('[{}]     观察到掉单+持仓跳变 posΔ={}，进入2s确认期', dsym(context, symbol), pos_delta)
    
    else:
        if state.get('_oo_drop_seen_ts') or state.get('_pos_jump_seen_ts'):
            info('[{}] ✅ 掉单/持仓跳变状态恢复，清理确认期', dsym(context, symbol))
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
        
        if _is_dup_fill(context, key):
            info('[{}]     DUP-RECOVER 去重: amt={} px={}', dsym(context, symbol), amount, price)
        else:
            _remember_fill(context, key)
            info('[{}] 🚀 FILL-RECOVER 触发: posΔ={} (>=unit {}) | synth amt={} px={:.3f}',
                 dsym(context, symbol), pos_delta, unit, amount, price)
            synth = SimpleNamespace(order_id=f"SYN-{int(time.time())}",
                                    amount=amount,
                                    filled=abs(amount),
                                    price=price)
            try:
                on_order_filled(context, symbol, synth)
            except Exception as e:
                info('[{}] ❌ FILL-RECOVER 调用 on_order_filled 失败: {}', dsym(context, symbol), e)

    elif state.get('_oo_drop_seen_ts') is not None:
         info('[{}] ✅ 掉单确认结束(持仓未跳变)，判定为API空窗，不触发补偿', dsym(context, symbol))

    state['_oo_drop_seen_ts'] = None
    state['_pos_jump_seen_ts'] = None
    state['_pos_confirm_deadline'] = None
    state['_oo_last'] = oo_n
    state['_last_pos_seen'] = pos_now
    safe_save_state(symbol, state)

# ---------------- 【核心更新】主动巡检与修正 (含去重逻辑) ----------------

def patrol_and_correct_orders(context, symbol, state):
    now_dt = context.current_dt
    dbg_tag = f"[{dsym(context, symbol)}]"

    if state.get('_last_trade_ts') and (now_dt - state['_last_trade_ts']).total_seconds() < 58:
        return

    # 【Fix v3.2.59】: 14:55:00 准时停止
    if not (is_main_trading_time() and now_dt.time() < dtime(14, 55)):
        return 
    if context.mark_halted.get(symbol, False):
        return 
    if not is_valid_price(context.latest_data.get(symbol)):
        return 

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
        
        # 分组统计，用于去重
        valid_buy_orders = []
        valid_sell_orders = []

        for o in open_orders:
            api_sym = getattr(o, 'symbol', None) or getattr(o, 'stock_code', None)
            entrust_no = getattr(o, 'entrust_no', None)
            o_price = getattr(o, 'price', 0)
            if not entrust_no: continue
            
            is_wrong = False
            
            if o.amount > 0: # 买单
                if not should_have_buy_order:
                    is_wrong = True 
                elif abs(o_price - buy_p) / (buy_p + 1e-9) >= 0.002:
                    is_wrong = True 
                else:
                    valid_buy_orders.append(o)
            
            elif o.amount < 0: # 卖单
                if not should_have_sell_order:
                    is_wrong = True 
                elif abs(o_price - sell_p) / (sell_p + 1e-9) >= 0.002:
                    is_wrong = True 
                else:
                    valid_sell_orders.append(o)
            
            if is_wrong:
                orders_to_cancel.append(o)
        
        # 【去重逻辑】：如果存在多个合法的同方向订单，保留一个，其余视为重复并撤销
        if len(valid_buy_orders) > 1:
            info('{} ⚠️ 发现 {} 个重复有效买单，正在清理多余单...', dbg_tag, len(valid_buy_orders))
            # 保留第一个，其他的加入撤销列表
            for o in valid_buy_orders[1:]:
                orders_to_cancel.append(o)
            valid_buy_orders = valid_buy_orders[:1]

        if len(valid_sell_orders) > 1:
            info('{} ⚠️ 发现 {} 个重复有效卖单，正在清理多余单...', dbg_tag, len(valid_sell_orders))
            for o in valid_sell_orders[1:]:
                orders_to_cancel.append(o)
            valid_sell_orders = valid_sell_orders[:1]

        # 执行撤销
        if orders_to_cancel:
            info('{} 🕵️ PATROL: 发现 {} 笔错误/重复挂单，正在撤销...', dbg_tag, len(orders_to_cancel))
            
            # 【Fix v3.2.54】设置修正锁，防止撤单真空期内 check_pending_rehangs 乘虚而入
            # 锁定 10 秒
            state['_ignore_place_until'] = datetime.now() + timedelta(seconds=10)
            safe_save_state(symbol, state)
            
            # 【修复 v3.2.55】收集已撤销的订单号
            cancelled_ids = set()
            
            for o in orders_to_cancel:
                try:
                    entrust_no = getattr(o, 'entrust_no', None)
                    api_sym = getattr(o, 'symbol', None) or getattr(o, 'stock_code', None)
                    info('{} ... 正在撤销 #{}: {} @ {:.3f}', dbg_tag, entrust_no, o.amount, getattr(o, 'price', 0))
                    cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})
                    if entrust_no:
                        cancelled_ids.add(entrust_no)
                except Exception as e:
                    info('{} ⚠️ PATROL 撤单异常: {}', dbg_tag, e)
            
            # 撤单后立即刷新状态并尝试补单
            state.pop('_last_order_ts', None)
            state.pop('_last_order_bp', None)
            
            # 【Fix v3.2.54】传入 bypass_lock=True，允许 patrol 绕过自己刚设置的锁
            # 【Fix v3.2.55】传入 ignore_entrust_nos，强制忽略幽灵订单
            place_limit_orders(context, symbol, state, 
                               ignore_cooldown=True, 
                               bypass_lock=True,
                               ignore_entrust_nos=cancelled_ids)
            return 

        # 如果没有要撤的，但缺单，则补单
        has_correct_buy_order = (len(valid_buy_orders) > 0)
        has_correct_sell_order = (len(valid_sell_orders) > 0)

        if (should_have_buy_order and not has_correct_buy_order) or \
           (should_have_sell_order and not has_correct_sell_order):
            info('{} 🕵️ PATROL: 发现缺失订单，准备补挂...', dbg_tag)
            # 这里调用 place，但不需要绕过锁，因为并没有设置锁
            place_limit_orders(context, symbol, state, ignore_cooldown=True)

    except Exception as e:
        info('{} ⚠️ PATROL 巡检失败: {}', dbg_tag, e)

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
                
                # 【新增】计算 RV 与 效率比
                _calculate_intraday_metrics(context)
                
                generate_html_report(context)
                context.last_report_time = now_dt
                info("📊 定时更新看板成功 @ {}", now_dt.strftime('%H:%M'))
            except Exception as e:
                info("⚠️ 看板更新异常: {}", e)

    boot_grace = (now_dt - getattr(context, 'boot_dt', now_dt)).total_seconds() < getattr(context, 'boot_grace_seconds', 180)
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
                    info('[{}]     监测到复牌/行情恢复，开启 {}s 补偿成交检测窗口。', 
                         dsym(context, symbol), recover_window_seconds)

    for sym in context.symbol_list:
        if sym not in context.state:
            continue
        st = context.state[sym]
        price = context.latest_data.get(sym)
        if is_valid_price(price):
            get_target_base_position(context, sym, st, price, now_dt)
            adjust_grid_unit(st)
            if now_dt.minute % 30 == 0 and now_dt.second < 5:
                update_grid_spacing_final(context, sym, st, get_position(sym).amount)

    is_patrol_time = (now_dt.minute % 30 == 0 and now_dt.second < 5)
    
    # 【Fix v3.2.59】: 将交易截止时间恢复到 14:55:00
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

# ---------------- 【新增】日内RV计算函数 ----------------

def _calculate_intraday_metrics(context):
    """
    计算日内Realized Volatility (RV) 和 Grid Efficiency。
    """
    if not is_main_trading_time() and not is_auction_time():
        return

    metrics = {}
    today_date = context.current_dt.date()
    
    for sym in context.symbol_list:
        try:
            hist = get_history(250, '1m', ['close'], security_list=[sym], include=True)
            df = None
            if isinstance(hist, dict):
                df = hist.get(sym)
            else:
                df = hist
                
            if df is None or df.empty:
                continue
                
            today_df = df[df.index.date == today_date].copy()
            if len(today_df) < 2:
                continue
                
            close_series = today_df['close']
            log_rets = np.log(close_series / close_series.shift(1))
            rv = log_rets.abs().sum()
            
            open_price = close_series.iloc[0]
            curr_price = close_series.iloc[-1]
            daily_return = (curr_price - open_price) / open_price
            
            efficiency = rv / max(abs(daily_return), 0.0001)
            
            metrics[sym] = {
                'rv': rv,
                'efficiency': efficiency,
                'daily_return': daily_return
            }
            
        except Exception as e:
            pass
            
    context.intraday_metrics = metrics

# ---------------- 监控输出 ----------------

def log_status(context, symbol, state, price):
    disp_price = context.last_valid_price.get(symbol, state['base_price'])
    if not is_valid_price(disp_price):
        return
    
    position = get_position(symbol)
    pos = position.amount
    
    pnl = (disp_price - position.cost_basis) * pos if position.cost_basis > 0 else 0
    info("📊 [{}] 状态: 价:{:.3f} 持仓:{}(可卖:{}) / 底仓:{} 成本:{:.3f} 盈亏:{:.2f} 网格:[买{:.2%},卖{:.2%}]",
         dsym(context, symbol), disp_price, pos, position.enable_amount, state['base_position'], position.cost_basis, pnl, state['buy_grid_spacing'], state['sell_grid_spacing'])

# ---------------- 动态网格间距（ATR） ----------------

def update_grid_spacing_final(context, symbol, state, curr_pos):
    pos = curr_pos 
    unit, base_pos = state['grid_unit'], state['base_position']
    atr_pct = calculate_atr(context, symbol)
    base_spacing = 0.005
    if atr_pct is not None:
        atr_multiplier = 0.25
        base_spacing = atr_pct * atr_multiplier
    min_spacing = TRANSACTION_COST * 5
    base_spacing = max(base_spacing, min_spacing)
    
    if pos <= base_pos + unit * 5:
        new_buy, new_sell = base_spacing, base_spacing * 2
    elif pos > base_pos + unit * 15:
        new_buy, new_sell = base_spacing * 2, base_spacing
    else:
        new_buy, new_sell = base_spacing, base_spacing
        
    max_spacing = 0.03
    new_buy  = round(min(new_buy,  max_spacing), 4)
    new_sell = round(min(new_sell, max_spacing), 4)
    if new_buy != state.get('buy_grid_spacing') or new_sell != state.get('sell_grid_spacing'):
        state['buy_grid_spacing'], state['sell_grid_spacing'] = new_buy, new_sell
        info('[{}]     网格动态调整. (pos={}) ATR({:.2%}) -> 基础间距({:.2%}) -> 最终:[买{:.2%},卖{:.2%}]',
             dsym(context, symbol), pos, (atr_pct or 0.0), base_spacing, new_buy, new_sell)

def calculate_atr(context, symbol, atr_period=14):
    try:
        hist = get_history(atr_period + 1, '1d', ['high','low','close'], security_list=[symbol])
        if hist is None or hist.empty or len(hist) < atr_period + 1:
            info('[{}] ⚠️ ATR计算失败: get_history未能返回足够的数据。', dsym(context, symbol))
            return None
        high, low, close = hist['high'].values, hist['low'].values, hist['close'].values
        trs = [max(h - l, abs(h - pc), abs(l - pc)) for h, l, pc in zip(high[1:], low[1:], close[:-1])]
        if not trs:
            return None
        atr_value = sum(trs) / len(trs)
        current_price = context.last_valid_price.get(symbol, close[-1])
        if is_valid_price(current_price):
            return atr_value / current_price
        return None
    except Exception as e:
        info('[{}] ❌ ATR计算异常: {}', dsym(context, symbol), e)
        return None

# ---------------- 日终处理 ----------------

def end_of_day(context):
    info('✅ 日终处理 (Start @ 14:55) [GLOBAL BATCH CANCEL]')
    try:
        all_orders = get_all_orders()
        if not all_orders:
            info('🕊️ 账户无挂单，清理完毕。')
            return

        to_cancel = []
        for o in all_orders:
            # 【修复 1】: 双重兼容，防止 AttributeError
            sym = getattr(o, 'symbol', None) or getattr(o, 'stock_code', None)
            if sym is None and isinstance(o, dict):
                sym = o.get('symbol') or o.get('stock_code')
            
            if not sym: continue
            
            # 过滤非本策略标的
            if convert_symbol_to_standard(sym) not in context.symbol_list:
                continue
            
            # 【修复 2】: 获取状态更健壮
            status = str(getattr(o, 'status', ''))
            if not status and isinstance(o, dict):
                status = str(o.get('status', ''))
                
            # 【修复 3】: 覆盖部成单 '7'
            if status in ['2', '7']: 
                to_cancel.append(o)
        
        if not to_cancel:
            info('🕊️ 无有效挂单(状态2/7)，清理完毕。')
        else:
            info('🧹 扫描到 {} 笔有效挂单(含部成)，正在批量撤销...', len(to_cancel))
            
            for o in to_cancel:
                # 【修复 4】: 单笔异常捕获，防止崩盘
                try:
                    cancel_order_ex(o)
                    
                    # 维护虚拟冻结量（安全写法）
                    o_amt = getattr(o, 'amount', 0)
                    if o_amt == 0 and isinstance(o, dict):
                        o_amt = o.get('amount', 0)
                    
                    o_sym = convert_symbol_to_standard(getattr(o, 'symbol', None) or getattr(o, 'stock_code', None) or (o.get('symbol') if isinstance(o, dict) else None))
                    
                    bs = str(getattr(o, 'entrust_bs', ''))
                    if not bs and isinstance(o, dict):
                         bs = str(o.get('entrust_bs', ''))
                         
                    is_sell = (o_amt < 0) or (bs == '2')
                    
                    if is_sell and o_sym in context.pending_frozen:
                        frozen = abs(o_amt)
                        context.pending_frozen[o_sym] = max(0, context.pending_frozen[o_sym] - frozen)
                
                except Exception as e:
                    info('⚠️ 日终单笔撤单异常: {}', e)

    except Exception as e:
        info('❌ 日终撤单主流程异常: {}', e)

    # 保存状态
    for sym in context.symbol_list:
        if sym in context.state:
            safe_save_state(sym, context.state[sym])
            
    info('✅ 日终作业完成，PnL计算已推迟至盘后。')

# ---------------- VA & Tools (修复核心逻辑) ----------------

def get_target_base_position(context, symbol, state, price, dt):
    # 修复 v3.2.45: 恢复正确的定投目标计算逻辑
    try:
        # 1. 计算定投期数和目标价值 (恢复复利公式)
        weeks = get_trade_weeks(context, symbol, state, dt)
        
        # 目标市值 = 初始投入 + 累计定投(复利增长)
        accumulated_investment = sum(state['dingtou_base'] * (1 + state['dingtou_rate'])**w for w in range(1, weeks + 1))
        target_val = state['initial_position_value'] + accumulated_investment
        
        current_val = state['base_position'] * price
        
        # 2. 盈余释放 (VA 减仓) - 仅当大幅跑赢目标时触发
        surplus = current_val - target_val
        grid_value = state['grid_unit'] * price
        
        # 只有当盈余超过 1.0 个网格单位，且释放后底仓仍高于初始值的50%时才释放
        if surplus >= 1.0 * grid_value:
            release_amt = state['grid_unit']
            if state['base_position'] - release_amt >= state['initial_base_position'] * 0.5:
                state['base_position'] -= release_amt
                info('[{}] 💰 VA底仓盈余释放: 目标市值{:.0f} vs 当前{:.0f} -> 减少 {} 股', 
                     dsym(context, symbol), target_val, current_val, release_amt)

        # 3. 价值平均加仓 (原有逻辑)
        # 计算基于上周底仓的增量
        # 目标是：本周结束时，市值达到 target_val
        # 缺口 = 目标市值 - (上周底仓 * 当前价格)
        delta_val = target_val - (state['last_week_position'] * price)
        
        if delta_val > 0:
            # 需要增加的股数 (向上取整到100股)
            delta_pos = math.ceil(delta_val / price / 100) * 100
            new_pos = state['last_week_position'] + delta_pos
            
            # 确保不低于初始底仓对应的份额 (防止价格暴跌导致底仓归零)
            min_base = round(state['initial_position_value'] / state['base_price'] / 100) * 100
            final_pos = round(max(min_base, new_pos) / 100) * 100
            
            if final_pos > state['base_position']:
                info('[{}] 📈 VA价值平均加仓: 目标{:.0f} -> 底仓增加至 {}', dsym(context, symbol), target_val, final_pos)
                state['base_position'] = final_pos
                
    except Exception as e:
        info('[{}] ⚠️ 定投目标计算出错: {}', dsym(context, symbol), e)
        
    return state['base_position']

def get_trade_weeks(context, symbol, state, dt):
    # 简单的周数计算，确保 key 唯一
    y, w, _ = dt.date().isocalendar()
    key = f"{y}_{w}"
    
    if 'trade_week_set' not in state:
        state['trade_week_set'] = set()
        
    # 如果是新的一周，记录状态
    if key not in state['trade_week_set']:
        state['trade_week_set'].add(key)
        state['last_week_position'] = state['base_position']
        safe_save_state(symbol, state)
        
    return len(state['trade_week_set'])

def adjust_grid_unit(state):
    # 如果底仓大幅增加，适当放大网格单位
    if state['base_position'] > state['grid_unit'] * 20:
        new_unit = int(state['base_position'] / 20 / 100) * 100
        if new_unit > state['grid_unit']:
            state['grid_unit'] = new_unit
            info('[{}] 🔧 网格单位放大至 {}', state.get('symbol'), new_unit)

def _load_pnl_metrics(path):
    if path.exists(): return json.loads(path.read_text(encoding='utf-8'))
    return {}

def _save_pnl_metrics(context):
    if hasattr(context, 'pnl_metrics_path'):
        context.pnl_metrics_path.write_text(json.dumps(context.pnl_metrics, indent=2), encoding='utf-8')

# ---------------- 本地 PnL 引擎 (LIFO版) ----------------

def _calculate_local_pnl_lifo(context):
    info('🧮 启动本地 PnL 引擎 (LIFO Attribution Mode)...')
    trade_log_path = research_path('reports', 'a_trade_details.csv')
    if not trade_log_path.exists():
        info('⚠️ 无交易记录文件，跳过计算。')
        return

    trades = []
    try:
        with open(trade_log_path, 'r', encoding='utf-8') as f:
            headers = f.readline().strip().split(',')
            for line in f:
                parts = line.strip().split(',')
                if len(parts) < 6: continue
                base_pos_at_trade = 0
                try: base_pos_at_trade = int(parts[5]) 
                except: pass
                
                trades.append({
                    'time': parts[0],
                    'symbol': parts[1],
                    'direction': parts[2],
                    'qty': float(parts[3]),
                    'price': float(parts[4]),
                    'base_pos_at_trade': base_pos_at_trade
                })
    except Exception as e:
        info('❌ CSV 读取失败: {}', e)
        return

    trades.sort(key=lambda x: x['time'])
    pnl_metrics = getattr(context, 'pnl_metrics', {})
    
    for sym in context.symbol_list:
        if sym not in context.state: continue
        state = context.state[sym]
        initial_pos = state.get('initial_base_position', 0)
        initial_cost = state.get('base_price', 0)
        
        inventory = [] 
        if initial_pos > 0:
            inventory.append([initial_pos, initial_cost, 'base'])
            
        current_holding = initial_pos
        grid_pnl = 0.0
        base_pnl = 0.0
        
        sym_trades = [t for t in trades if t['symbol'] == sym]
        
        for t in sym_trades:
            qty = t['qty']
            price = t['price']
            target_base = t['base_pos_at_trade'] if t['base_pos_at_trade'] > 0 else state.get('base_position', 0)
            
            if qty > 0: # 买入
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
                    
            elif qty < 0: # 卖出
                sell_qty = abs(qty)
                current_holding -= sell_qty
                
                while sell_qty > 0.001 and inventory:
                    lot = inventory[-1] # LIFO: 取队尾
                    lot_qty, lot_price, lot_tag = lot[0], lot[1], lot[2]
                    
                    matched = min(sell_qty, lot_qty)
                    profit = (price - lot_price) * matched
                    
                    if lot_tag == 'base':
                        base_pnl += profit
                    else:
                        grid_pnl += profit
                        
                    sell_qty -= matched
                    lot[0] -= matched
                    
                    if lot[0] <= 0.001:
                        inventory.pop()
        
        if sym not in pnl_metrics: pnl_metrics[sym] = {}
        pnl_metrics[sym]['realized_grid_pnl'] = grid_pnl
        pnl_metrics[sym]['realized_base_pnl'] = base_pnl
        pnl_metrics[sym]['total_realized_pnl'] = grid_pnl + base_pnl
        
        if (grid_pnl + base_pnl) != 0:
            info('💰 [{}] LIFO归因完成: 网格盈亏={:.2f}, 底仓盈亏={:.2f}', sym, grid_pnl, base_pnl)

    context.pnl_metrics = pnl_metrics
    _save_pnl_metrics(context)

# ---------------- 盘后处理 ----------------

def after_trading_end(context, data):
    if '回测' in context.env: return
    info('🏁 盘后作业开始...')
    try:
        _calculate_local_pnl_lifo(context)
    except Exception as e:
        info('❌ PnL 计算失败: {}', e)
        
    try:
        update_daily_reports(context, data)
        generate_html_report(context)
    except Exception as e:
        info('❌ 报表生成失败: {}', e)
    info('✅ 盘后作业结束')

# ---------------- 配置热重载 (Fix:BasePosition) ----------------

def reload_config_if_changed(context):
    try:
        # 使用正确的变量名 context.config_file_path
        current_mod_time = context.config_file_path.stat().st_mtime
        if current_mod_time == context.last_config_mod_time:
            return

        info('♻️ 检测到配置文件发生变更，开始热重载...')
        context.last_config_mod_time = current_mod_time
        
        # 【修复点】这里原代码写成了 context.config_file，修正为 context.config_file_path
        new_config = json.loads(context.config_file_path.read_text(encoding='utf-8'))
        
        old_symbols = set(context.symbol_list)
        new_symbols = set(new_config.keys())

        # 处理移除
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

        # 处理新增 (Fix Base Position Logic Here)
        for sym in new_symbols - old_symbols:
            info('[{}] 新增标的 (或重载)，正在初始化状态...', dsym(context, sym))
            cfg = new_config[sym]
            
            # 【修复核心】先尝试从磁盘读取状态，而不是直接用 initial 覆盖
            state_file = research_path('state', f'{sym}.json')
            saved = {}
            if state_file.exists():
                try:
                    saved = json.loads(state_file.read_text(encoding='utf-8'))
                    info('[{}] 📂 发现历史状态文件，将恢复底仓数据...', dsym(context, sym))
                except Exception as e:
                    info('[{}] ⚠️ 读取历史状态失败: {}', dsym(context, sym), e)
            
            st = {**cfg}
            st.update({
                'base_price': saved.get('base_price', cfg['base_price']), # 优先用保存的基准价
                'grid_unit': saved.get('grid_unit', cfg['grid_unit']),
                'filled_order_ids': set(saved.get('filled_order_ids', [])),
                'trade_week_set': set(saved.get('trade_week_set', [])),
                
                # 【修复核心】底仓数据优先读取 saved，没有才用 initial
                'base_position': saved.get('base_position', cfg['initial_base_position']),
                'last_week_position': saved.get('last_week_position', cfg['initial_base_position']),
                
                'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
                'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,
                'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20),
                'va_last_update_dt': None,
                'va_update_count_date': None,
                'va_updates_today': 0,
                '_halt_next_log_dt': None,
                '_oo_last': 0, 
                '_recover_until': None, '_after_cancel_until': None, 
                '_oo_drop_seen_ts': None, '_pos_jump_seen_ts': None, 
                '_pos_confirm_deadline': None,
                '_rehang_due_ts': None, # 使用新版时间锁变量
                
                # 【v3.2.54】: 修正锁
                '_ignore_place_until': None,
                
                # 【v3.2.56】: 撤单ID缓存
                '_pending_ignore_ids': []
            })
            context.state[sym] = st
            context.latest_data[sym] = st['base_price']
            context.symbol_list.append(sym)
            context.mark_halted[sym] = False
            context.last_valid_price[sym] = st['base_price']
            context.last_valid_ts[sym] = None
            context.pending_frozen[sym] = 0
            
            # 【重要】新增标的后，立即标记为需要挂单
            context.should_place_order_map[sym] = True

        # 处理更新
        for sym in old_symbols.intersection(new_symbols):
            if context.symbol_config[sym] != new_config[sym]:
                info('[{}] 参数发生变更，正在更新...', dsym(context, sym))
                state, new_params = context.state[sym], new_config[sym]
                state.update({
                    'grid_unit': new_params['grid_unit'],
                    'dingtou_base': new_params['dingtou_base'],
                    'dingtou_rate': new_params['dingtou_rate'],
                    'max_position': state['base_position'] + new_params['grid_unit'] * 20
                })

        context.symbol_config = new_config
        _load_symbol_names(context)
        info('✅ 配置文件热重载完成！当前监控标的: {}', context.symbol_list)

    except Exception as e:
        info(f'❌ 配置文件热重载失败: {e}')

# ---------------- 日志辅助函数 ----------------

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
            row = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                symbol,
                direction,
                str(trade['business_amount']),
                f"{trade['business_price']:.3f}",
                str(base_position),
                entrust_no 
            ]
            f.write(",".join(row) + "\n")
    except Exception as e:
        info('❌ [{}] 记录交易日志失败: {}', dsym(context, symbol), e)

# ---------------- 日报/报表 ----------------

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
        try:
            if not is_valid_price(close_price):
                close_price = cost_basis if cost_basis > 0 else state['base_price']
                if not is_valid_price(close_price): close_price = 1.0
        except:
            close_price = state['base_price']
        weeks = len(state.get('trade_week_set', []))
        count = weeks
        d_base = state['dingtou_base']
        d_rate = state['dingtou_rate']
        invest_should = d_base
        invest_actual = d_base * (1 + d_rate) ** weeks
        cumulative_invest = sum(d_base * (1 + d_rate) ** w for w in range(1, weeks+1))
        expected_value = state['initial_position_value'] + d_base * weeks
        last_week_val = state.get('last_week_position', 0) * close_price
        current_val   = amount * close_price
        weekly_return = (current_val - last_week_val) / last_week_val if last_week_val>0 else 0.0
        total_return  = (current_val - cumulative_invest) / cumulative_invest if cumulative_invest>0 else 0.0
        weekly_bottom_profit = (state['base_position'] - state.get('last_week_position', 0)) * close_price
        total_bottom_profit  = state['base_position'] * close_price - state['initial_position_value']
        standard_qty    = state['base_position'] + state['grid_unit'] * 5
        intermediate_qty= state['base_position'] + state['grid_unit'] * 15
        added_base      = state['base_position'] - state.get('last_week_position', 0)
        compare_cost    = added_base * close_price
        profit_all      = (close_price - cost_basis) * amount if cost_basis > 0 else 0
        t_quantity = max(0, amount - state['base_position'])
        row = [
            current_date, f"{close_price:.3f}", str(weeks), str(count),
            f"{weekly_return:.2%}", f"{total_return:.2%}", f"{expected_value:.2f}",
            f"{invest_should:.0f}", f"{invest_actual:.0f}", f"{cumulative_invest:.0f}",
            str(state['initial_base_position']), str(state['base_position']),
            f"{state['base_position'] * close_price:.0f}", f"{weekly_bottom_profit:.0f}",
            f"{total_bottom_profit:.0f}", str(state['base_position']), str(amount),
            str(state['grid_unit']), str(t_quantity), str(standard_qty),
            str(intermediate_qty), str(state['max_position']), f"{cost_basis:.3f}",
            f"{compare_cost:.3f}", f"{profit_all:.0f}"
        ]
        is_new = not report_file.exists()
        with open(report_file, 'a', encoding='utf-8', newline='') as f:
            if is_new:
                headers = [
                    "时间","市价","期数","次数","每期总收益率","盈亏比","应到价值",
                    "当周应投入金额","当周实际投入金额","实际累计投入金额","定投底仓份额",
                    "累计底仓份额","累计底仓价值","每期累计底仓盈利","总累计底仓盈利",
                    "底仓","股票余额","单次网格交易数量","可T数量","标准数量","中间数量",
                    "极限数量","成本价","对比定投成本","盈亏"
                ]
                f.write(",".join(headers) + "\n")
            f.write(",".join(map(str, row)) + "\n")
        info('✅ [{}] 已更新每日CSV报表：{}', dsym(context, symbol), report_file)


# ---------------- HTML 看板生成 (使用模板) ----------------

def generate_html_report(context):
    all_metrics = []
    total_market_value = 0
    total_unrealized_pnl = 0
    total_realized_grid_pnl = 0
    total_realized_base_pnl = 0
    total_realized_pnl = 0
    pnl_metrics = getattr(context, 'pnl_metrics', {})
    
    # 获取日内指标数据
    intraday_metrics = getattr(context, 'intraday_metrics', {})

    for symbol in context.symbol_list:
        if symbol not in context.state:
            continue
        state = context.state[symbol]
        position = get_position(symbol)
        pos = position.amount 
        price = context.last_valid_price.get(symbol, state['base_price'])
        halted = context.mark_halted.get(symbol, False)
        
        if not is_valid_price(price):
            price = position.cost_basis if position.cost_basis > 0 else state['base_price']
            if not is_valid_price(price): price = 1.0
                
        market_value = pos * price
        unrealized_pnl = (price - position.cost_basis) * pos if position.cost_basis > 0 else 0
        total_market_value += market_value
        total_unrealized_pnl += unrealized_pnl
        
        atr_pct = calculate_atr(context, symbol)
        name_price = f"{price:.3f}" + (" (停牌)" if halted else "")
        disp_name = dsym(context, symbol, style='long')
        
        sym_pnl = pnl_metrics.get(symbol, {})
        realized_grid_pnl = sym_pnl.get('realized_grid_pnl', 0)
        realized_base_pnl = sym_pnl.get('realized_base_pnl', 0)
        total_realized_sym_pnl = sym_pnl.get('total_realized_pnl', 0)
        total_sym_pnl = total_realized_sym_pnl + unrealized_pnl
        
        total_realized_grid_pnl += realized_grid_pnl
        total_realized_base_pnl += realized_base_pnl
        total_realized_pnl += total_realized_sym_pnl
        
        # 获取 RV 和 效率
        rv_data = intraday_metrics.get(symbol, {})
        rv_val = rv_data.get('rv', 0)
        efficiency_val = rv_data.get('efficiency', 0)
        
        all_metrics.append({
            "symbol": symbol,
            "symbol_disp": disp_name,
            "position": f"{pos} ({position.enable_amount})",
            "cost_basis": f"{position.cost_basis:.3f}",
            "price": name_price,
            "market_value": f"{market_value:,.2f}",
            "unrealized_pnl": f"{unrealized_pnl:,.2f}",
            "realized_grid_pnl": f"{realized_grid_pnl:,.2f}",
            "realized_base_pnl": f"{realized_base_pnl:,.2f}",
            "total_realized_pnl": f"{total_realized_sym_pnl:,.2f}",
            "total_pnl": f"{total_sym_pnl:,.2f}",
            "pnl_ratio": f"{(unrealized_pnl / (position.cost_basis * pos) * 100) if position.cost_basis * pos != 0 else 0:.2f}%",
            "base_position": state['base_position'],
            "grid_unit": state['grid_unit'],
            "grid_spacing": f"{state['buy_grid_spacing']:.2%} / {state['sell_grid_spacing']:.2%}",
            "atr_str": f"{atr_pct:.2%}" if atr_pct is not None else "N/A",
            "rv_str": f"{rv_val:.2%}",
            "efficiency_val": efficiency_val,
            "efficiency_str": f"{efficiency_val:.1f}"
        })
        
    account_total_pnl = total_realized_pnl + total_unrealized_pnl
    
    # 动态读取外部模板
    try:
        template_file = research_path('config', 'dashboard_template.html')
        if not template_file.exists():
            default_tpl = "<html><body><h1>Dashboard Template Missing!</h1><p>Please check config/dashboard_template.html</p></body></html>"
            template_file.write_text(default_tpl, encoding='utf-8')
            info('⚠️ 未找到看板模板，已生成默认文件: {}', template_file)
            
        html_template = template_file.read_text(encoding='utf-8')
        
    except Exception as e:
        info('❌ 读取看板模板失败: {}，将使用极简模式', e)
        html_template = "<html><body><h1>Error loading template</h1><p>{}</p></body></html>".format(e)

    table_rows = ""
    for m in all_metrics:
        try: pnl_val = float(m["unrealized_pnl"].replace(",", ""))
        except: pnl_val = 0.0
        pnl_class = "positive" if pnl_val >= 0 else "negative"
        
        try: total_pnl_val = float(m["total_pnl"].replace(",", ""))
        except: total_pnl_val = 0.0
        total_pnl_class = "positive" if total_pnl_val >= 0 else "negative"
        
        try: grid_pnl_val = float(m["realized_grid_pnl"].replace(",", ""))
        except: grid_pnl_val = 0.0
        grid_pnl_class = "positive" if grid_pnl_val > 0 else ("negative" if grid_pnl_val < 0 else "")
        
        try: base_pnl_val = float(m["realized_base_pnl"].replace(",", ""))
        except: base_pnl_val = 0.0
        base_pnl_class = "positive" if base_pnl_val > 0 else ("negative" if base_pnl_val < 0 else "")
        
        try: total_realized_val = float(m["total_realized_pnl"].replace(",", ""))
        except: total_realized_val = 0.0
        total_realized_class = "positive" if total_realized_val > 0 else ("negative" if total_realized_val < 0 else "")

        eff_val = m['efficiency_val']
        eff_class = "neutral"
        if eff_val > 3.0: eff_class = "excellent"
        elif eff_val < 1.5 and eff_val > 0.1: eff_class = "warning"

        table_rows += f"""
        <tr>
            <td>{m['symbol_disp']}</td>
            <td>{m['position']}</td>
            <td>{m['cost_basis']}</td>
            <td>{m['price']}</td>
            <td>{m['market_value']}</td>
            <td class="{pnl_class}">{m['unrealized_pnl']}</td>
            <td class="{pnl_class}">{m['pnl_ratio']}</td>
            <td class="{grid_pnl_class}">{m['realized_grid_pnl']}</td>
            <td class="{base_pnl_class}">{m['realized_base_pnl']}</td>
            <td class="{total_realized_class}">{m['total_realized_pnl']}</td>
            <td class="{total_pnl_class}">{m['total_pnl']}</td>
            <td>{m['base_position']}</td>
            <td>{m['grid_unit']}</td>
            <td>{m['atr_str']}</td>
            <td>{m['rv_str']}</td>
            <td class="{eff_class}">{m['efficiency_str']}</td>
        </tr>
        """

    try:
        final_html = html_template.format(
            update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total_market_value=f"{total_market_value:,.2f}",
            total_unrealized_pnl=f"{total_unrealized_pnl:,.2f}",
            unrealized_pnl_class="positive" if total_unrealized_pnl >= 0 else "negative",
            total_realized_pnl=f"{total_realized_pnl:,.2f}",
            realized_pnl_class="positive" if total_realized_pnl >= 0 else "negative",
            account_total_pnl=f"{account_total_pnl:,.2f}",
            total_pnl_class="positive" if account_total_pnl >= 0 else "negative",
            total_realized_grid_pnl=f"{total_realized_grid_pnl:,.2f}",
            grid_pnl_class="positive" if total_realized_grid_pnl > 0 else ("negative" if total_realized_grid_pnl < 0 else ""),
            total_realized_base_pnl=f"{total_realized_base_pnl:,.2f}",
            base_pnl_class="positive" if total_realized_base_pnl > 0 else ("negative" if total_realized_base_pnl < 0 else ""),
            table_rows=table_rows
        )
        report_path = research_path('reports', 'strategy_dashboard.html')
        report_path.write_text(final_html, encoding='utf-8')
    except Exception as e:
        info(f'❌ 生成HTML看板失败: {e}')