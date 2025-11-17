# event_driven_grid_strategy.py
# 版本号：GEMINI-3.2.16-Deliver-FIX
# 相对 3.2.15 的新增/修改要点：
# - 【PnL 归因修复】:
#     - 修复了 _update_realized_pnl_from_deliver 函数中的一个崩溃Bug。
#     - Bug原因: 当 get_deliver() 返回空列表 '[]' (当天无交割) 时，
#       代码尝试访问 .empty 属性 (DataFrame才有) 导致崩溃。
#     - 修复方案: 增加对 'isinstance(deliver_df, list)' 的检查。
# - 【VA 逻辑】: (保留 3.2.15 的 VA-FIX)

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

# ---------------- 全局句柄与常量 ----------------
LOG_FH = None
LOG_DATE = None
MAX_SAVED_FILLED_IDS = 500
__version__ = 'GEMINI-3.2.16-Deliver-FIX' # <-- 版本号升级
TRANSACTION_COST = 0.00005

# ---- 调试默认（可被 config/debug.json / strategy.json 覆盖）----
DBG_ENABLE_DEFAULT = True
DBG_RT_WINDOW_SEC_DEFAULT = 60
DBG_RT_PREVIEW_DEFAULT = 8
DELAY_AFTER_CANCEL_SECONDS_DEFAULT = 1.0

# ---- VA 去抖动与限频 默认参数 ----
VA_VALUE_THRESHOLD_K_DEFAULT = 1.0
VA_MIN_UPDATE_INTERVAL_MIN_DEFAULT = 60
VA_MAX_UPDATES_PER_DAY_DEFAULT = 3

# ---- 停牌下单保护默认 ----
MKT_HALT_SKIP_PLACE_DEFAULT = True
MKT_HALT_SKIP_AFTER_SECONDS_DEFAULT = 180
MKT_HALT_LOG_EVERY_MINUTES_DEFAULT = 10

# ---------------- 通用路径与工具函数 ----------------

def research_path(*parts) -> Path:
    p = Path(get_research_path()).joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _ensure_daily_logfile():
    """确保 LOG_FH 指向当日文件；跨日自动切换。"""
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
            log.info(f' 日志切换到 {log_path}')
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

# ---------------- HALT-GUARD：有效价与停牌标记 ----------------

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

# ---------------- 调试配置：config/debug.json ----------------

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
        info(' 调试配置生效: enable={} window={}s preview={} delay_after_cancel={}s',
             enable, winsec, preview, delay_after_cancel)
    else:
        info(' 调试配置生效: enable=False（关闭心跳日志）')

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

# ---------------- 统一参数：config/strategy.json（优先级最高） ----------------
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
    info(' 统一参数生效：读取 strategy.json 并覆盖子配置（delay_after_cancel={}s）',
         getattr(context, 'delay_after_cancel_seconds', DELAY_AFTER_CANCEL_SECONDS_DEFAULT))

# ---------------- 初始化与时间窗口判断 ----------------

def initialize(context):
    log_file = _ensure_daily_logfile()
    log.info(f' 日志同时写入到 {log_file}')
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

    # 成交去重 ring（5s 有效）
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
            # —— VA 限频状态 —— 
            'va_last_update_dt': None,
            'va_update_count_date': None,
            'va_updates_today': 0,
            # —— 停牌日志压频（每标的）——
            '_halt_next_log_dt': None,
            # —— FILL-RECOVER 运行态 —— 
            # 【!! 修复 !!】: 移除 _pos_change, _last_pos_seen
            '_oo_last': 0,             # 上次看到的进行中挂单笔数
            '_recover_until': None,    # 复牌/时窗补偿截止
            '_after_cancel_until': None, # 撤单后补偿截止
            '_oo_drop_seen_ts': None,    # 订单簿“掉单”首次发现时间
            '_pos_jump_seen_ts': None,   # 持仓“跳变”首次发现时间
            '_pos_confirm_deadline': None # 二次确认截止时刻
        })
        context.state[sym] = st
        context.latest_data[sym] = st['base_price']
        context.should_place_order_map[sym] = True
        context.mark_halted[sym] = False
        context.last_valid_price[sym] = st['base_price']
        
        # 【!! 修复 !!】: 启动时强行清空错误的补偿值
        if '_pos_change' in st:
            st.pop('_pos_change')
        if '_last_pos_seen' in st:
            st.pop('_last_pos_seen')

    context.boot_dt = getattr(context, 'current_dt', None) or datetime.now()
    context.boot_grace_seconds = int(get_saved_param('boot_grace_seconds', 180))
    context.delay_after_cancel_seconds = DELAY_AFTER_CANCEL_SECONDS_DEFAULT

    _load_debug_config(context, force=True)
    _load_va_config(context, force=True)
    _load_market_config(context, force=True)
    _load_strategy_config(context, force=True)

    context.initial_cleanup_done = False
    if '回测' not in context.env:
        run_daily(context, place_auction_orders, time='9:15')
        run_daily(context, end_of_day, time='14:56')
        info('✅ 事件驱动模式就绪')

    # --- 【新增 PnL v3.2.14】PnL 指标初始化 ---
    context.pnl_metrics_path = research_path('state', 'pnl_metrics.json')
    context.pnl_metrics = _load_pnl_metrics(context.pnl_metrics_path)
    info('✅ PnL 收益指标已加载（共 {} 个标的）', len(context.pnl_metrics))
    # --- PnL 初始化结束 ---
    
    info('✅ 初始化完成，版本:{}', __version__)

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
    if context.initial_cleanup_done:
        return
    info(' before_trading_start：清理遗留挂单')
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
    info(' 按品种清理所有遗留挂单')
    for sym in context.symbol_list:
        cancel_all_orders_by_symbol(context, sym)
        # 【!! 修复 !!】: 启动时强行清空错误的补偿值
        if sym in context.state:
            context.state[sym].pop('_pos_change', None)
            context.state[sym].pop('_last_pos_seen', None)
    info('✅ 按品种清理完成')

# ---------------- 订单与撤单工具 ----------------

def get_order_status(entrust_no):
    try:
        order_detail = get_order(entrust_no)
        return str(order_detail.get('status', '')) if order_detail else ''
    except Exception as e:
        info('⚠️ 查询订单状态失败 entrust_no={}: {}', entrust_no, e)
        return ''

def cancel_all_orders_by_symbol(context, symbol):
    all_orders = get_all_orders() or []
    total = 0
    if not hasattr(context, 'canceled_cache'):
        context.canceled_cache = {'date': None, 'orders': set()}
    today = context.current_dt.date()
    if context.canceled_cache.get('date') != today:
        context.canceled_cache = {'date': today, 'orders': set()}
    cache = context.canceled_cache['orders']

    for o in all_orders:
        # get_all_orders() 返回的是 dict 列表, .get() 方式是正确的
        api_sym = o.get('symbol') or o.get('stock_code') 
        if convert_symbol_to_standard(api_sym) != symbol:
            continue
        status = str(o.get('status', ''))
        entrust_no = o.get('entrust_no')
        if (not entrust_no
            or status != '2'
            or entrust_no in context.state[symbol]['filled_order_ids']
            or entrust_no in cache):
            continue
        final_status = get_order_status(entrust_no)
        if final_status in ('4', '5', '6', '8'):
            continue
        cache.add(entrust_no)
        total += 1
        info('[{}]  发现并尝试撤销遗留挂单 entrust_no={}', dsym(context, symbol), entrust_no)
        try:
            cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})
        except Exception as e:
            info('[{}] ⚠️ 撤单异常 entrust_no={}: {}', dsym(context, symbol), entrust_no, e)
    if total > 0:
        info('[{}] 共{}笔遗留挂单尝试撤销完毕（将于下一次 get_open_orders 快照核验）', dsym(context, symbol), total)

# ---------------- 集合竞价挂单 ----------------

def place_auction_orders(context):
    if '回测' in context.env or not (is_auction_time() or is_main_trading_time()):
        return
    info(' 清空防抖缓存，开始集合竞价挂单（按 base_price 补挂）')
    for st in context.state.values():
        st.pop('_last_order_bp', None); st.pop('_last_order_ts', None)
    for sym in context.symbol_list:
        state = context.state[sym]
        adjust_grid_unit(state)
        cancel_all_orders_by_symbol(context, sym)
        context.latest_data[sym] = state['base_price']
        place_limit_orders(context, sym, state)
        safe_save_state(sym, state)

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
            info(' RT心跳 获取快照异常: {}', e)
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
            # context.mark_halted[sym] = False # <-- 不在此处设置，交由 handle_data 统一判断
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
            info(' RT心跳 {} got:{}/{} miss:[{}]',
                 now_dt.strftime('%H:%M'), got, len(symbols), miss_preview)
            # try:
            #     for ksym in symbols:
            #         lts = context.last_valid_ts.get(ksym)
            #         gap = (now_dt - lts).total_seconds() if lts else -1
            #         info('[{}] ⏱️ last_valid_ts={} gap_sec={:.1f}', dsym(context, ksym), lts, gap)
            # except Exception:
            #     pass

# ---------------- 日志辅助：订单簿 dump ----------------

def _dump_open_orders(context, symbol, tag='DUMP'):
    try:
        # get_open_orders() 返回 Order 对象, 使用 getattr()
        oo = [o for o in (get_open_orders(symbol) or []) if getattr(o, 'status', None) == '2']
        if not oo:
            info('[{}]  OPEN-ORDERS {}: 空', dsym(context, symbol), tag)
            return
        lines = []
        for o in oo:
            lines.append(f"#{getattr(o,'entrust_no',None)} side={'B' if o.amount>0 else 'S'} px={getattr(o,'price',None)} amt={o.amount} status={getattr(o,'status',None)}")
        info('[{}]  OPEN-ORDERS {}: {} 笔 -> {}', dsym(context, symbol), tag, len(oo), ' | '.join(lines))
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
    # 清理过期
    now = context.current_dt
    while context.recent_fill_ring:
        k, ts = context.recent_fill_ring[0]
        if (now - ts).total_seconds() > ttl_sec:
            context.recent_fill_ring.popleft()
        else:
            break
    # 查重
    for k, _ in context.recent_fill_ring:
        if k[:-1] == key[:-1]:  # 忽略价的细微差异，按 symbol/side/qty/minute 做粗去重
            return True
    return False

def _remember_fill(context, key):
    context.recent_fill_ring.append((key, context.current_dt))

def _in_reopen_window(now_t: dtime):
    # 复牌/关键时刻：9:30、10:30、13:00，窗口 ±35s
    anchors = [dtime(9,30,0), dtime(10,30,0), dtime(13,0,0)]
    for a in anchors:
        if abs((datetime.combine(datetime.today(), now_t) - datetime.combine(datetime.today(), a)).total_seconds()) <= 35:
            return True
    return False

# ---------------- 网格限价挂单主逻辑 ----------------

def place_limit_orders(context, symbol, state):
    now_dt = context.current_dt
    dbg_tag = f"[{dsym(context, symbol)}]"

    rehang_bypass = bool(state.get('_rehang_bypass_once'))
    if (not rehang_bypass) and state.get('_last_trade_ts') \
      and (now_dt - state['_last_trade_ts']).total_seconds() < 60:
        # info('{} ❎ PLACE-SKIP REASON=COOLDOWN ...', dbg_tag)
        return

    if is_order_blocking_period():
        info('{} ❎ PLACE-SKIP REASON=BLOCKING_PERIOD(9:25-9:30)', dbg_tag)
        return
    in_limit_window = is_auction_time() or (is_main_trading_time() and now_dt.time() < dtime(14, 56))
    if not in_limit_window:
        # info('{} ❎ PLACE-SKIP REASON=OUT_OF_LIMIT_WINDOW now={}', dbg_tag, now_dt.time())
        return

    if is_main_trading_time() and not is_auction_time():
        if getattr(context, 'halt_skip_place', MKT_HALT_SKIP_PLACE_DEFAULT):
            last_ts = context.last_valid_ts.get(symbol)
            halt_after = int(getattr(context, 'halt_skip_after_seconds', MKT_HALT_SKIP_AFTER_SECONDS_DEFAULT))
            if context.mark_halted.get(symbol, False) and last_ts:
                if (now_dt - last_ts).total_seconds() >= halt_after:
                    # info('{} ❎ PLACE-SKIP REASON=HALT_GUARD ...', dbg_tag)
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
    
    # --- 【!!! 关键修复：移除补偿 !!!】 ---
    pos = position.amount # 默认使用API持仓
    # --- 【!!! 修复结束 !!!】 ---

    price = context.latest_data.get(symbol)
    ratchet_enabled = (not allow_tickless) and is_valid_price(price)

    # info('{} ▶ PLACE-CHECK ctx: ... ratchet={}', dbg_tag, ratchet_enabled)

    if ratchet_enabled:
        if abs(price / base - 1) <= 0.10:
            # 【重要】棘轮判断使用补偿后的 pos
            is_in_low_pos_range  = (pos - unit <= state['base_position'])
            is_in_high_pos_range = (pos + unit >= state['max_position'])
            sell_p_curr = round(base * (1 + sell_sp), 3)
            buy_p_curr  = round(base * (1 - buy_sp), 3)
            ratchet_up   = is_in_low_pos_range  and price >= sell_p_curr
            ratchet_down = is_in_high_pos_range and price <= buy_p_curr
            
            if ratchet_up:
                info('[{}] 棘轮上移(补偿后pos={}): 触及卖价，基准抬至 {:.3f}', dsym(context, symbol), pos, sell_p_curr)
                state['base_price'] = sell_p_curr
                cancel_all_orders_by_symbol(context, symbol)
                buy_p, sell_p = round(sell_p_curr * (1 - buy_sp), 3), round(sell_p_curr * (1 + sell_sp), 3)
            elif ratchet_down:
                info('[{}] 棘轮下移(补偿后pos={}): 触及买价，基准降至 {:.3f}', dsym(context, symbol), pos, buy_p_curr)
                state['base_price'] = buy_p_curr
                cancel_all_orders_by_symbol(context, symbol)
                buy_p, sell_p = round(buy_p_curr * (1 - buy_sp), 3), round(buy_p_curr * (1 + sell_sp), 3)

    last_ts = state.get('_last_order_ts')
    if (not rehang_bypass) and last_ts and (now_dt - last_ts).seconds < 30:
        # info('{} ❎ PLACE-SKIP REASON=THROTTLE_TIME ...', dbg_tag)
        return
    last_bp = state.get('_last_order_bp')
    if (not rehang_bypass) and last_bp and abs(base / last_bp - 1) < buy_sp / 2:
        # info('{} ❎ PLACE-SKIP REASON=THROTTLE_BASE_BP ...', dbg_tag)
        return
    state['_last_order_ts'], state['_last_order_bp'] = now_dt, base

    try:
        # 【!! BUG FIX v3.2.10 !!】
        # get_open_orders() 返回 Order 对象, 使用 getattr()
        open_orders = [o for o in (get_open_orders(symbol) or []) if getattr(o, 'status', None) == '2']
        same_buy  = any(o.amount > 0 and abs(getattr(o, 'price', 0) - buy_p)  < 1e-3 for o in open_orders)
        same_sell = any(o.amount < 0 and abs(getattr(o, 'price', 0) - sell_p) < 1e-3 for o in open_orders)

        enable_amount = position.enable_amount
        
        # 【!! 修复 !!】: 移除 pos_change != 0 的判断
        # 仅用于日志：显示API原始持仓
        # if pos_change != 0:
        #     info('{} ▶ STATE (API_pos={}) (Compensated_pos={}) enable={} base_pos={} ...',
        #          dbg_tag, position.amount, pos, enable_amount, state['base_position'])
        
        # 记录订单簿与持仓快照（供 FILL-RECOVER）
        state['_oo_last'] = len(open_orders)
        # 【重要】使用 API 持仓作为 FILL-RECOVER 的基线
        state['_last_pos_seen'] = pos 

        can_buy = not same_buy
        if can_buy and pos + unit <= state['max_position']:
            info('[{}] --> 发起买入委托: {}股 @ {:.3f}', dsym(context, symbol), unit, buy_p)
            order(symbol, unit, limit_price=buy_p)
        else:
            if not can_buy:
                # info('{} ❎ BUY-SKIP REASON=DUP_SAME_PRICE ...', dbg_tag)
                pass
            elif pos + unit > state['max_position']:
                info('{} ❎ BUY-SKIP REASON=POS_CAP pos={} unit={} max_pos={}', dbg_tag, pos, unit, state['max_position'])

        can_sell = not same_sell
        if can_sell and enable_amount >= unit and pos - unit >= state['base_position']:
            info('[{}] --> 发起卖出委托: {}股 @ {:.3f}', dsym(context, symbol), unit, sell_p)
            order(symbol, -unit, limit_price=sell_p)
        else:
            reasons = []
            if not can_sell:
                # reasons.append('DUP_SAME_PRICE')
                pass
            if enable_amount < unit:
                reasons.append(f'ENABLE_LT_UNIT enable={enable_amount} unit={unit}')
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
        
        # 【!!! PnL v3.2.14: 关键依赖 !!!】
        # log_trade_details 必须在 PnL 归因之前调用，以记录 'base_position_at_trade'
        log_trade_details(context, sym, tr) 
        
        if sym not in context.state or entrust_no in context.state[sym]['filled_order_ids']:
            continue

        amount = tr['business_amount'] if tr['entrust_bs']=='1' else -tr['business_amount']
        price  = tr['business_price']
        key = _make_fill_key(sym, amount, price, context.current_dt)
        if _is_dup_fill(context, key):
            info('[{}]  DUP-TRADE 回报去重: amt={} px={}', dsym(context, sym), amount, price)
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
            info('[{}] ❌ 成交处理失败：{}', dsym(context, symbol), e)

def on_order_filled(context, symbol, order):
    state = context.state[symbol]
    if order.filled == 0:
        return
    last_dt = state.get('_last_fill_dt')
    if state.get('last_fill_price') == order.price and last_dt and (context.current_dt - last_dt).seconds < 5:
        return
    trade_direction = "买入" if order.amount > 0 else "卖出"
    info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f}', dsym(context, symbol), trade_direction, order.filled, order.price)
    
    # --- 【!!! 关键修复：移除补偿 !!!】 ---
    # state['_pos_change'] = 0
    # state['_last_pos_seen'] = None
    # [移除所有 _pos_change 和 _last_pos_seen 的设置]
    # --- 【!!! 修复结束 !!!】 ---

    state['_last_trade_ts'] = context.current_dt
    state['_last_fill_dt'] = context.current_dt
    state['last_fill_price'] = order.price
    state['base_price'] = order.price

    cancel_all_orders_by_symbol(context, symbol)

    # 撤单后快照（T+0s）
    try:
        _oo = [o for o in (get_open_orders(symbol) or []) if getattr(o, 'status', None) == '2']
        pend_buy  = sum(o.amount for o in _oo if o.amount > 0)
        pend_sell = sum(o.amount for o in _oo if o.amount < 0) # 卖单 amount 是负数
        info('[{}]  AFTER-CANCEL open_orders={} pend_buy={} pend_sell={}', dsym(context, symbol), len(_oo), pend_buy, abs(pend_sell))
        # _dump_open_orders(context, symbol, tag='AFTER-CANCEL-T+0s')
    except Exception as _e:
        info('[{}] ⚠️ AFTER-CANCEL snapshot error: {}', dsym(context, symbol), _e)

    # 固定延时（仅日志辅助）+ 撤单后补偿窗
    try:
        delay_s = float(getattr(context, 'delay_after_cancel_seconds', DELAY_AFTER_CANCEL_SECONDS_DEFAULT))
        state['_after_cancel_until'] = context.current_dt + timedelta(seconds=max(delay_s, 2.5))
        if delay_s > 0:
            time.sleep(delay_s)
            # _dump_open_orders(context, symbol, tag=f'AFTER-CANCEL-T+{delay_s:.1f}s')
    except Exception as _e:
        info('[{}] ⚠️ 微确认延时失败：{}（忽略，继续）', dsym(context, symbol), _e)

    context.mark_halted[symbol] = False
    context.last_valid_price[symbol] = order.price
    context.latest_data[symbol] = order.price
    context.last_valid_ts[symbol] = context.current_dt

    state['_rehang_bypass_once'] = True
    state.pop('_last_order_ts', None)
    state.pop('_last_order_bp', None)

    if is_order_blocking_period():
        info('[{}] 处于9:25-9:30挂单冻结期，成交后仅更新状态，推迟挂单至9:30后。', dsym(context, symbol))
    elif context.current_dt.time() < dtime(14, 56):
        info('[{}] ▶ FILL->REHANG base_price={:.3f} rehang_bypass_once={} now={}', dsym(context, symbol), state['base_price'], state.get('_rehang_bypass_once'), context.current_dt.time())
        place_limit_orders(context, symbol, state)

    context.should_place_order_map[symbol] = True
    
    # 【!! 修复 !!】: 更新 FILL-RECOVER 参考快照（使用 API 持仓）
    try:
         state['_last_pos_seen'] = get_position(symbol).amount
    except:
         state['_last_pos_seen'] = None # 获取失败则重置
         
    state['_oo_last'] = len([o for o in (get_open_orders(symbol) or []) if getattr(o, 'status', None) == '2'])
    
    # 清理掉单/跳变确认状态
    state['_oo_drop_seen_ts'] = None
    state['_pos_jump_seen_ts'] = None
    state['_pos_confirm_deadline'] = None
    safe_save_state(symbol, state)

# ---------------- FILL-RECOVER：补偿式成交检测（含误判保护） ----------------

def _fill_recover_watch(context, symbol, state):
    """
    【强化版 v3.2.8】
    在复牌/关键时段 & 撤单后短窗内，核验订单簿与持仓，补偿漏回报。
    误判保护：
      1) “订单簿掉单(oo_last>0 -> 当前=0)” 必须持续 >= 2s 才有效。
      2) “持仓跳变(pos_delta >= unit)” 必须持续 >= 2s 才有效。
      3) 瞬时( < 2s )的 API 错误数据（鬼数据）将被忽略。
    """
    now_dt = context.current_dt
    in_window = False
    
    # 关键时段：复牌/开盘/午后开市 ±35s
    if _in_reopen_window(now_dt.time()):
        in_window = True

    # 撤单后短窗
    if state.get('_after_cancel_until') and now_dt <= state['_after_cancel_until']:
        in_window = True

    # 主动开启的“复牌监控窗”
    if state.get('_recover_until') and now_dt <= state['_recover_until']:
        in_window = True

    # 【!! 修复 !!】: 移除 _pos_change 检查
    # if state.get('_pos_change', 0) != 0:
    #     return

    if not in_window:
        if state.get('_last_pos_seen') is None:
            try: state['_last_pos_seen'] = get_position(symbol).amount
            except: pass
        # 清理可能残留的确认状态
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
    
    # --- 状态监测 ---
    oo_drop_now = (state.get('_oo_last', 0) > 0 and oo_n == 0)
    pos_jump_now = (abs(pos_delta) >= unit)
    
    # 场景1：仅订单簿掉单（无持仓跳变）
    if oo_drop_now and not pos_jump_now:
        if state.get('_oo_drop_seen_ts') is None:
            state['_oo_drop_seen_ts'] = now_dt
            state['_pos_confirm_deadline'] = now_dt + timedelta(seconds=2.0)
            info('[{}]  观察到订单簿掉单(无持仓跳变)，进入2s确认期', dsym(context, symbol))
        # 清理可能存在的“持仓跳变”观测
        state['_pos_jump_seen_ts'] = None 
    
    # 场景2：仅持仓跳变（订单簿未掉单，或本就为空）
    elif pos_jump_now and not oo_drop_now:
        if state.get('_pos_jump_seen_ts') is None:
            state['_pos_jump_seen_ts'] = now_dt
            state['_pos_confirm_deadline'] = now_dt + timedelta(seconds=2.0)
            info('[{}]  观察到持仓跳变 posΔ={}(无掉单)，进入2s确认期 (防鬼数据)', dsym(context, symbol), pos_delta)
        # 清理可能存在的“掉单”观测
        state['_oo_drop_seen_ts'] = None

    # 场景3：两者同时发生 (掉单 + 持仓跳变)
    elif oo_drop_now and pos_jump_now:
        if state.get('_oo_drop_seen_ts') is None and state.get('_pos_jump_seen_ts') is None:
             state['_oo_drop_seen_ts'] = now_dt
             state['_pos_jump_seen_ts'] = now_dt
             state['_pos_confirm_deadline'] = now_dt + timedelta(seconds=2.0)
             info('[{}]  观察到掉单+持仓跳变 posΔ={}，进入2s确认期', dsym(context, symbol), pos_delta)
    
    # 场景4：一切正常（未掉单，未跳变）
    else:
        if state.get('_oo_drop_seen_ts') or state.get('_pos_jump_seen_ts'):
            info('[{}] ✅ 掉单/持仓跳变状态恢复，清理确认期', dsym(context, symbol))
            state['_oo_drop_seen_ts'] = None
            state['_pos_jump_seen_ts'] = None
            state['_pos_confirm_deadline'] = None
    
    # --- 决策：是否触发虚拟成交 ---
    
    # 检查确认期是否结束
    deadline = state.get('_pos_confirm_deadline')
    if deadline is None or now_dt < deadline:
        # 仍在确认期或无需确认
        state['_oo_last'] = oo_n
        state['_last_pos_seen'] = pos_now
        safe_save_state(symbol, state)
        return

    # 确认期已过(>=2s)，检查状态是否仍然满足
    
    # 必须是“持仓跳变”状态持续了 >= 2s
    if state.get('_pos_jump_seen_ts') is not None:
        info('[{}] ✅ 持仓跳变(posΔ={})确认期结束，触发补偿', dsym(context, symbol), pos_delta)
        
        filled_qty = int(abs(pos_delta) // unit * unit)
        amount = filled_qty if pos_delta > 0 else -filled_qty
        price  = context.latest_data.get(symbol, state['base_price']) or state['base_price']
        key = _make_fill_key(symbol, amount, price, now_dt)
        
        if _is_dup_fill(context, key):
            info('[{}]  DUP-RECOVER 去重: amt={} px={}', dsym(context, symbol), amount, price)
        else:
            _remember_fill(context, key)
            info('[{}] ️ FILL-RECOVER 触发: posΔ={} (>=unit {}) | synth amt={} px={:.3f}',
                 dsym(context, symbol), pos_delta, unit, amount, price)
            synth = SimpleNamespace(order_id=f"SYN-{int(time.time())}",
                                    amount=amount,
                                    filled=abs(amount),
                                    price=price)
            try:
                on_order_filled(context, symbol, synth)
            except Exception as e:
                info('[{}] ❌ FILL-RECOVER 调用 on_order_filled 失败: {}', dsym(context, symbol), e)

    # 如果只是“掉单”状态持续了 >= 2s，但“持仓”始终未变
    elif state.get('_oo_drop_seen_ts') is not None:
         info('[{}] ✅ 掉单确认结束(持仓未跳变)，判定为API空窗，不触发补偿', dsym(context, symbol))

    # 清理确认状态
    state['_oo_drop_seen_ts'] = None
    state['_pos_jump_seen_ts'] = None
    state['_pos_confirm_deadline'] = None
    state['_oo_last'] = oo_n
    state['_last_pos_seen'] = pos_now
    safe_save_state(symbol, state)

# ---------------- 【!! 修正 !!】主动巡检与修正 ----------------

def patrol_and_correct_orders(context, symbol, state):
    """
    (每30分钟)主动巡检：
    1. 撤销所有价格错误、或不该存在的挂单。
    2. 补挂所有缺失的、正确的网格单。
    """
    now_dt = context.current_dt
    dbg_tag = f"[{dsym(context, symbol)}]"

    # --- 【!!! 新增修复：回避成交冷静期 !!!】 ---
    # 如果刚成交 (e.g., 60s内)，on_order_filled 正在处理，巡检应回避
    # 我们检查 58s 而不是 60s，留出 2s 缓冲
    if state.get('_last_trade_ts') and (now_dt - state['_last_trade_ts']).total_seconds() < 58:
        info('{} ️ PATROL-SKIP REASON=COOLDOWN (on_order_filled is active)', dbg_tag)
        return
    # --- 【!!! 修复结束 !!!】 ---
    
    # 1. 检查是否满足巡检条件
    if not (is_main_trading_time() and now_dt.time() < dtime(14, 56)):
        return # 非主交易时间不巡检
    if context.mark_halted.get(symbol, False):
        # info('{} ️ PATROL-SKIP REASON=HALT', dbg_tag)
        return # 停牌不巡检
    if not is_valid_price(context.latest_data.get(symbol)):
        # info('{} ️ PATROL-SKIP REASON=INVALID_PRICE', dbg_tag)
        return # 价格失效不巡检

    try:
        # 2. 获取真实状态 (!!! 使用补偿后的持仓 !!!)
        position = get_position(symbol)
        
        # --- 【!!! 关键修复：移除补偿 !!!】 ---
        pos = position.amount # 默认
        # --- 【!!! 修复结束 !!!】 ---
        
        enable_amount = position.enable_amount
        # 【!! BUG FIX v3.2.10 !!】
        # get_open_orders() 返回 Order 对象, 使用 getattr()
        open_orders = [o for o in (get_open_orders(symbol) or []) if getattr(o, 'status', None) == '2']

        # 3. 获取期望状态
        base_pos = state['base_position']
        max_pos = state['max_position']
        unit = state['grid_unit']
        base_price = state['base_price']
        buy_p = round(base_price * (1 - state['buy_grid_spacing']), 3)
        sell_p = round(base_price * (1 + state['sell_grid_spacing']), 3)

        # 4. 计算应挂状态 (!!! 使用补偿后的 pos !!!)
        should_have_buy_order = (pos + unit <= max_pos)
        should_have_sell_order = (enable_amount >= unit and pos - unit >= base_pos)

        # 5. 查找需撤销的单
        orders_to_cancel = []
        has_correct_buy_order = False
        has_correct_sell_order = False

        for o in open_orders:
            # 【!! BUG FIX v3.2.10 !!】: 使用 getattr() 访问对象属性
            api_sym = getattr(o, 'symbol', None) or getattr(o, 'stock_code', None)
            entrust_no = getattr(o, 'entrust_no', None)
            o_price = getattr(o, 'price', 0)
            if not entrust_no: continue
            
            is_wrong = False
            if o.amount > 0: # 这是一个买单
                if not should_have_buy_order:
                    is_wrong = True # 仓位满了，不该有买单
                elif abs(o_price - buy_p) >= 1e-3:
                    is_wrong = True # 价格不对
                else:
                    has_correct_buy_order = True # 找到了正确的买单
            
            elif o.amount < 0: # 这是一个卖单
                if not should_have_sell_order:
                    is_wrong = True # 仓位到底了，不该有卖单
                elif abs(o_price - sell_p) >= 1e-3:
                    is_wrong = True # 价格不对
                else:
                    has_correct_sell_order = True # 找到了正确的卖单
            
            if is_wrong:
                orders_to_cancel.append((entrust_no, api_sym, o_price, o.amount))

        # 6. 执行撤单
        if orders_to_cancel:
            info('{} ️ PATROL: 发现 {} 笔错误/陈旧挂单，正在撤销...', dbg_tag, len(orders_to_cancel))
            for entrust_no, api_sym, price, amount in orders_to_cancel:
                try:
                    info('{} ... 正在撤销 #{}: {} @ {:.3f}', dbg_tag, entrust_no, amount, price)
                    cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})
                except Exception as e:
                    info('{} ⚠️ PATROL 撤单 #{] 失败: {}', dbg_tag, entrust_no, e)
            # 撤单后，重置节流，允许立即补挂
            state.pop('_last_order_ts', None)
            state.pop('_last_order_bp', None)
            state['_rehang_bypass_once'] = True # 强制重新挂单

        # 7. 执行补挂（交由 place_limit_orders 执行）
        if (should_have_buy_order and not has_correct_buy_order) or \
           (should_have_sell_order and not has_correct_sell_order):
            
            if orders_to_cancel:
                info('{} ️ PATROL: 撤单完成，准备补挂缺失订单...', dbg_tag)
                # 延迟一小下，等撤单生效
                time.sleep(float(getattr(context, 'delay_after_cancel_seconds', 1.0)))
            else:
                info('{} ️ PATROL: 发现缺失订单，准备补挂...', dbg_tag)
            
            # place_limit_orders 内部会处理 _pos_change
            place_limit_orders(context, symbol, state)

    except Exception as e:
        info('{} ⚠️ PATROL 巡检失败: {}', dbg_tag, e)

# ---------------- 行情主循环 ----------------

def handle_data(context, data):
    now_dt = context.current_dt
    now = now_dt.time()

    _fetch_quotes_via_snapshot(context)

    # 每 5 分钟看板
    if now_dt.minute % 5 == 0 and now_dt.second < 5:
        reload_config_if_changed(context)
        generate_html_report(context)

    # 停牌/断流标记维护 & 【!!! 修复：复牌事件监测 !!!】
    boot_grace = (now_dt - getattr(context, 'boot_dt', now_dt)).total_seconds() < getattr(context, 'boot_grace_seconds', 180)
    if not boot_grace:
        def _phase_start(now_t: dtime):
            if dtime(9, 15) <= now_t < dtime(9, 25):
                return dtime(9, 15)
            if dtime(9, 30) <= now_t <= dtime(11, 30):
                return dtime(9, 30)
            if dtime(13, 0) <= now_t <= dtime(15, 0):
                return dtime(13, 0)
            return None

        phase_start_t = _phase_start(now)
        if phase_start_t:
            phase_start_dt = datetime.combine(now_dt.date(), phase_start_t)
            grace_seconds = 120
            for sym in context.symbol_list:
                if sym not in context.state: # 安全检查
                    continue
                
                # 1. 记录旧状态
                was_halted = context.mark_halted.get(sym, False)

                # 2. 计算新状态
                last_ts = context.last_valid_ts.get(sym)
                is_now_halted = False
                if last_ts is None or last_ts < phase_start_dt:
                    is_now_halted = (now_dt >= phase_start_dt + timedelta(seconds=grace_seconds))
                else:
                    is_now_halted = ((now_dt - last_ts).total_seconds() > grace_seconds)
                
                # 3. 更新状态
                context.mark_halted[sym] = is_now_halted

                # 4. 检查状态切换：停牌 -> 恢复
                if was_halted and not is_now_halted:
                    # 这是一个复牌事件！
                    state = context.state[sym]
                    recover_window_seconds = 180 # 开启3分钟补偿窗口
                    state['_recover_until'] = now_dt + timedelta(seconds=recover_window_seconds)
                    info('[{}]  监测到复牌/行情恢复，开启 {}s 补偿成交检测窗口。', 
                         dsym(context, sym), recover_window_seconds)

    # 目标底仓 / ATR 间距
    for sym in context.symbol_list:
        if sym not in context.state:
            continue
        st = context.state[sym]
        price = context.latest_data.get(sym)
        if is_valid_price(price):
            # 【!!!】调用修复后的 VA 逻辑
            get_target_base_position(context, sym, st, price, now_dt)
            
            adjust_grid_unit(st)
            if now_dt.minute % 30 == 0 and now_dt.second < 5:
                # 传递 get_position() 的原始值，函数内部会处理补偿
                update_grid_spacing_final(context, sym, st, get_position(sym).amount)

    # 下单
    if is_auction_time() or (is_main_trading_time() and now < dtime(14, 56)):
        for sym in context.symbol_list:
            if sym in context.state:
                place_limit_orders(context, sym, context.state[sym])

    # FILL-RECOVER：关键时段 & 撤单后补偿窗（带误判保护）
    for sym in context.symbol_list:
        st = context.state.get(sym)
        if not st:
            continue
        _fill_recover_watch(context, sym, st)

    # 每 30 分钟巡检
    if now_dt.minute % 30 == 0 and now_dt.second < 5:
        info(' 每30分钟状态巡检...')
        for sym in context.symbol_list:
            if sym in context.state:
                # 【!!! 新增：主动巡检 !!!】
                patrol_and_correct_orders(context, sym, context.state[sym])
                # 巡检后打印最新状态
                log_status(context, sym, context.state[sym], context.latest_data.get(sym))

# ---------------- 监控输出 ----------------

def log_status(context, symbol, state, price):
    disp_price = context.last_valid_price.get(symbol, state['base_price'])
    if not is_valid_price(disp_price):
        return
    
    # 【!!! 巡检日志使用补偿后持仓 !!!】
    position = get_position(symbol)
    
    # --- 【!!! 关键修复：移除补偿 !!!】 ---
    pos = position.amount
    # --- 【!!! 修复结束 !!!】 ---
    
    pnl = (disp_price - position.cost_basis) * pos if position.cost_basis > 0 else 0
    info(" [{}] 状态: 价:{:.3f} 持仓:{}(可卖:{}) / 底仓:{} 成本:{:.3f} 盈亏:{:.2f} 网格:[买{:.2%},卖{:.2%}]",
         dsym(context, symbol), disp_price, pos, position.enable_amount, state['base_position'], position.cost_basis, pnl, state['buy_grid_spacing'], state['sell_grid_spacing'])

# ---------------- 动态网格间距（ATR） ----------------

def update_grid_spacing_final(context, symbol, state, curr_pos):
    # curr_pos 来自 get_position().amount，可能滞后
    # 在动态调整间距时，使用最新的补偿后持仓
    
    # --- 【!!! 关键修复：移除补偿 !!!】 ---
    pos = curr_pos # 默认
    # --- 【!!! 修复结束 !!!】 ---

    unit, base_pos = state['grid_unit'], state['base_position']
    atr_pct = calculate_atr(context, symbol)
    base_spacing = 0.005
    if atr_pct is not None:
        atr_multiplier = 0.25
        base_spacing = atr_pct * atr_multiplier
    min_spacing = TRANSACTION_COST * 5
    base_spacing = max(base_spacing, min_spacing)
    
    # 【重要】使用补偿后的 pos
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
        info('[{}]  网格动态调整. (pos={}) ATR({:.2%}) -> 基础间距({:.2%}) -> 最终:[买{:.2%},卖{:.2%}]',
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

# ---------------- 日终动作（14:56） ----------------

def end_of_day(context):
    info('✅ 日终处理开始(14:56)...')
    after_initialize_cleanup(context)
    generate_html_report(context)
    
    # --- 【新增 PnL v3.2.14】保存 PnL 指标 ---
    _save_pnl_metrics(context) 
    # --- PnL 保存结束 ---

    for sym in context.symbol_list:
        if sym in context.state:
            safe_save_state(sym, context.state[sym])
            context.should_place_order_map[sym] = True
    info('✅ 日终保存状态完成')

# ---------------- 价值平均（VA） ----------------

def get_target_base_position(context, symbol, state, price, dt):
    """
    【!!! 关键修复 v3.2.15 (VA-FIX) !!!】
    保持 V-3.2.14 的“实时、双向、节流”VA逻辑，但修复导致“振荡锁死”的Bug。
    Bug原因：VA调仓步长错误地使用了 grid_unit，导致严重超调。
    修复方案：VA调仓步长应使用 100 股（最小单位），而不是 grid_unit。
    """

    if not is_valid_price(price):
        info('[{}] ⚠️ 停牌/无有效价，跳过VA计算，底仓维持 {}', dsym(context, symbol), state['base_position'])
        return state['base_position']

    weeks = get_trade_weeks(context, symbol, state, dt)
    target_val = state['initial_position_value'] + sum(state['dingtou_base'] * (1 + state['dingtou_rate'])**w for w in range(1, weeks + 1))
    
    if price <= 0:
        return state['base_position']

    today = dt.date()
    if state.get('va_update_count_date') != today:
        state['va_update_count_date'] = today
        state['va_updates_today'] = 0

    k = float(getattr(context, 'va_value_threshold_k', VA_VALUE_THRESHOLD_K_DEFAULT))
    min_int_min = int(getattr(context, 'va_min_update_interval_minutes', VA_MIN_UPDATE_INTERVAL_MIN_DEFAULT))
    max_daily = int(getattr(context, 'va_max_updates_per_day', VA_MAX_UPDATES_PER_DAY_DEFAULT))

    # 1. 计算价值缺口 (逻辑不变)
    current_val = state['base_position'] * price
    delta_val = target_val - current_val
    
    # 2. 节流：未达阈值 (使用 grid_unit 作为阈值是合理的)
    grid_value = state['grid_unit'] * price
    if abs(delta_val) < k * grid_value:
        return state['base_position'] # 未达阈值

    # 3. 节流：冷却中
    last_dt = state.get('va_last_update_dt')
    if last_dt is not None and (dt - last_dt).total_seconds() < min_int_min * 60:
        return state['base_position'] # 冷却中

    # 4. 节流：今日达上限
    if state.get('va_updates_today', 0) >= max_daily:
        return state['base_position'] # 今日达上限

    # 5. 【!!! 核心修复 !!!】
    #    计算理想调整份额
    desired_shares = delta_val / price
    
    #    【原BUG代码 (V-3.2.14)】: 
    #    step = state['grid_unit'] # <--- 这是Bug的根源
    #    steps = int(round(desired_shares / step))
    #    if steps == 0:
    #        steps = 1 if desired_shares > 0 else -1
    #    adj_shares = steps * step # <--- 导致严重超调
    
    #    【修复后 (V-3.2.15)】: 
    #    VA调仓应以 100 股为单位，精细调整，防止超调。
    if desired_shares > 0:
        # 向上取整到 100 股
        adj_shares = math.ceil(desired_shares / 100) * 100
    elif desired_shares < 0:
        # 向下取整到 100 股
        adj_shares = math.floor(desired_shares / 100) * 100
    else:
        adj_shares = 0

    if adj_shares == 0:
        return state['base_position'] # 调整量为0

    # 6. 确保调整后的仓位合法 (逻辑不变)
    min_base = round(state['initial_position_value'] / state['base_price'] / 100) * 100 if state['base_price'] > 0 else 0
    new_base_pos = max(min_base, state['base_position'] + adj_shares)
    
    if new_base_pos == state['base_position']:
        return state['base_position'] # 调整后未变

    # 7. 执行调整 (逻辑不变)
    info('[{}] 价值平均(VA-FIX): 目标底仓从 {} 调整至 {} (Δ{}股, 单位:100). [目标市值:{:.2f}, 当前底仓市值:{:.2f}, 缺口:{:.2f}]',
         dsym(context, symbol),
         state['base_position'], new_base_pos, (new_base_pos - state['base_position']),
         target_val, current_val, delta_val)

    state['base_position'] = new_base_pos
    state['max_position'] = new_base_pos + state['grid_unit'] * 20
    state['va_last_update_dt'] = dt
    state['va_updates_today'] = int(state.get('va_updates_today', 0)) + 1

    return new_base_pos

def get_trade_weeks(context, symbol, state, dt):
    y, w, _ = dt.date().isocalendar()
    key = f"{y}_{w}"
    if key not in state.get('trade_week_set', set()):
        if 'trade_week_set' not in state:
            state['trade_week_set'] = set()
        state['trade_week_set'].add(key)
        state['last_week_position'] = state['base_position']
        safe_save_state(symbol, state)
    return len(state['trade_week_set'])

def adjust_grid_unit(state):
    orig, base_pos = state['grid_unit'], state['base_position']
    if base_pos >= orig * 20:
        new_u = math.ceil(orig * 1.2 / 100) * 100
        if new_u != orig:
            state['grid_unit'] = new_u
            state['max_position'] = base_pos + new_u * 20
            info(' [{}] 底仓增加，网格单位放大: {}->{}', state.get('symbol',''), orig, new_u)

# ---------------- 【新增 PnL v3.2.14】PnL 指标加载函数 ----------------
def _load_pnl_metrics(path: Path):
    """从 state/pnl_metrics.json 加载持久化的收益指标"""
    if not path.exists():
        info('⚠️ 未找到 PnL 配置文件，将创建新的: {}', path)
        return {}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(data, dict):
            raise ValueError("PnL 配置文件格式错误，应为 dict")
        return data
    except Exception as e:
        info('❌ 加载 PnL 配置文件失败: {}（将使用空数据启动）', e)
        return {}

# ---------------- 【新增 PnL v3.2.14】PnL 指标保存函数 ----------------
def _save_pnl_metrics(context):
    """在日终保存 PnL 指标到 state/pnl_metrics.json"""
    if hasattr(context, 'pnl_metrics_path') and hasattr(context, 'pnl_metrics'):
        try:
            context.pnl_metrics_path.write_text(
                json.dumps(context.pnl_metrics, indent=2), 
                encoding='utf-8'
            )
            # info('✅ PnL 收益指标已保存') # (日终日志太多，可选)
        except Exception as e:
            info('❌ 保存 PnL 收益指标失败: {}', e)

# ---------------- 【!!! 修复 v3.2.16 !!!】日终 PnL 归因计算 ----------------
def _update_realized_pnl_from_deliver(context):
    """
    【!!! 核心收益计算 !!!】
    使用 get_deliver() 和 a_trade_details.csv 进行日终收益归因。
    """
    info('PnL: 开始日终收益归因计算 (基于 get_deliver)...')
    
    # 1. 加载 PnL 指标库 (如果不存在则创建)
    pnl_metrics = getattr(context, 'pnl_metrics', {})

    # 2. 加载“归因依据”：a_trade_details.csv
    #    我们需要 (entrust_no) -> (base_position_at_trade) 的映射
    attribution_map = {}
    trade_log_path = research_path('reports', 'a_trade_details.csv')
    if not trade_log_path.exists():
        info('PnL: ⚠️ 未找到 a_trade_details.csv，无法进行收益归因。')
        return

    try:
        with open(trade_log_path, 'r', encoding='utf-8') as f:
            f.readline() # 跳过表头
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 7: # 确保行中有 entrust_no
                    try:
                        entrust_no = parts[6].strip()
                        base_pos_at_trade = int(parts[5].strip())
                        if entrust_no and entrust_no != 'N/A':
                            attribution_map[entrust_no] = base_pos_at_trade
                    except:
                        continue # 跳过格式错误的行
        info('PnL: 成功加载 {} 条交易归因记录。', len(attribution_map))
    except Exception as e:
        info('PnL: ❌ 加载 a_trade_details.csv 失败: {}', e)
        return

    # 3. 加载 PTRADE 官方交割单 (get_deliver)
    try:
        # PTRADE API: get_deliver() 返回一个 DataFrame
        # 我们只获取当天的交割单，防止重复计算
        today_str = datetime.now().strftime('%Y%m%d')
        deliver_df = get_deliver(start_date=today_str, end_date=today_str)
        
        # --- 【!!! 核心修复 v3.2.16 (Deliver-FIX) !!!】 ---
        # PTRADE 的 get_deliver() 在没有数据时可能返回 list '[]' 而不是 DataFrame
        # 必须同时检查 'is None', 'DataFrame.empty', 和 'isinstance(list) and not list'
        
        is_empty = False
        if deliver_df is None:
            is_empty = True
        elif isinstance(deliver_df, list):
            if not deliver_df: # Check if list is empty
                is_empty = True
        elif hasattr(deliver_df, 'empty'): # Check if it's a DataFrame
            if deliver_df.empty:
                is_empty = True
        else:
            # Handle other unexpected types, like an empty list that wasn't caught
            if not deliver_df:
                is_empty = True

        if is_empty:
            info('PnL: get_deliver() 未返回当日交割单 (返回了 None, empty list, or empty DataFrame)。')
            return
        # --- 【!!! 修复结束 !!!】 ---

        info('PnL: get_deliver() 成功获取 {} 条当日交割记录。', len(deliver_df))

    except Exception as e:
        info('PnL: ❌ 调用 get_deliver() 失败: {}', e)
        return

    # 4. 遍历交割单 (此时 deliver_df 必为非空 DataFrame)，进行 PnL 归因
    new_pnl_count = 0
    for _, row in deliver_df.iterrows():
        try:
            entrust_no = str(row['entrust_no']).strip()
            symbol_std = convert_symbol_to_standard(row['stock_code'])
            
            # (1) 初始化标的 PnL 库
            if symbol_std not in pnl_metrics:
                pnl_metrics[symbol_std] = {'realized_grid_pnl': 0, 'realized_base_pnl': 0, 'total_realized_pnl': 0, '_processed_entrust': []}
            
            pnl_data = pnl_metrics[symbol_std]
            
            # (2) 检查是否已处理过这笔交割
            if entrust_no in pnl_data.get('_processed_entrust', []):
                continue # 跳过已处理的

            # (3) 查找归因依据
            base_pos_at_trade = attribution_map.get(entrust_no)
            if base_pos_at_trade is None:
                # info('PnL: ⚠️ 委托号 {} 在 .csv 中未找到归因，跳过...', entrust_no)
                continue # 找不到归因（可能是旧版日志），无法计算

            # (4) 获取净结算金额 (关键!)
            # settle_amount: 卖出为正(收入), 买入为负(支出)
            settle_amount = row['settle_amount'] 
            trade_type = row['trade_type'] # 'B' or 'S'
            trade_qty = row['trade_amount'] # 始终为正
            
            # (5) PnL 归因 (基于 `a_trade_details.csv` 的记录)
            pos_now = get_position(symbol_std).amount
            
            is_grid_trade = False
            if trade_type == 'S':
                # 卖出永远是网格交易 (因为策略不卖底仓)
                is_grid_trade = True
            elif trade_type == 'B':
                # 买入时，如果持仓 > 底仓，是网格回补
                # 我们用 (pos_now - trade_qty) 作为买入前的持仓近似
                if (pos_now - trade_qty) > base_pos_at_trade:
                    is_grid_trade = True
                else:
                    is_grid_trade = False # 否则是底仓(VA)买入
            
            if is_grid_trade:
                pnl_data['realized_grid_pnl'] += settle_amount
                # info('PnL: 归因 [网格] {} 净额: {:.2f}', symbol_std, settle_amount)
            else:
                pnl_data['realized_base_pnl'] += settle_amount
                # info('PnL: 归因 [底仓] {} 净额: {:.2f}', symbol_std, settle_amount)

            pnl_data['total_realized_pnl'] = pnl_data['realized_grid_pnl'] + pnl_data['realized_base_pnl']
            pnl_data.setdefault('_processed_entrust', []).append(entrust_no)
            new_pnl_count += 1

        except Exception as e:
            info('PnL: ❌ 归因计算失败 (EntrustNO: {}): {}', entrust_no, e)

    info('PnL: ✅ 日终收益归因完成，新处理 {} 笔交割单。', new_pnl_count)
    context.pnl_metrics = pnl_metrics
    _save_pnl_metrics(context) # 保存计算结果
            
# ---------------- 交易结束回调（平台触发） ----------------

def after_trading_end(context, data):
    if '回测' in context.env:
        return
    info('⏰ 系统调用交易结束处理')
    
    # --- 【新增 PnL v3.2.14】调用 PnL 日终结算 ---
    _update_realized_pnl_from_deliver(context)
    # --- PnL 计算结束 ---
    
    update_daily_reports(context, data)
    info('✅ 交易结束处理完成')

# ---------------- 配置热重载 ----------------

def reload_config_if_changed(context):
    try:
        current_mod_time = context.config_file_path.stat().st_mtime
        if current_mod_time == context.last_config_mod_time:
            return
        info(' 检测到配置文件发生变更，开始热重载...')
        context.last_config_mod_time = current_mod_time
        new_config = json.loads(context.config_file_path.read_text(encoding='utf-8'))
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

        for sym in new_symbols - old_symbols:
            info('[{}] 新增标的，正在初始化状态...', dsym(context, sym))
            cfg = new_config[sym]
            st = {**cfg}
            st.update({
                'base_price': cfg['base_price'], 'grid_unit': cfg['grid_unit'],
                'filled_order_ids': set(), 'trade_week_set': set(),
                'base_position': cfg['initial_base_position'],
                'last_week_position': cfg['initial_base_position'],
                'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
                'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,
                'max_position': cfg['initial_base_position'] + cfg['grid_unit'] * 20,
                'va_last_update_dt': None,
                'va_update_count_date': None,
                'va_updates_today': 0,
                '_halt_next_log_dt': None,
                # 【!! 修复 !!】: 移除 _pos_change, _last_pos_seen
                '_oo_last': 0, 
                '_recover_until': None, '_after_cancel_until': None, 
                '_oo_drop_seen_ts': None, '_pos_jump_seen_ts': None, 
                '_pos_confirm_deadline': None
            })
            context.state[sym] = st
            context.latest_data[sym] = st['base_price']
            context.symbol_list.append(sym)
            context.mark_halted[sym] = False
            context.last_valid_price[sym] = st['base_price']
            context.last_valid_ts[sym] = None

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

# ---------------- 日报/报表 ----------------

def update_daily_reports(context, data):
    reports_dir = research_path('reports')
    reports_dir.mkdir(parents=True, exist_ok=True)
    current_date = context.current_dt.strftime("%Y-%m-%d")
    for symbol in context.symbol_list:
        report_file = reports_dir / f"{symbol}.csv"
        state = context.state[symbol]
        
        # 【重要】报表使用补偿后的持仓
        position = get_position(symbol)

        # --- 【!!! 关键修复：移除补偿 !!!】 ---
        amount = position.amount # 默认
        # --- 【!!! 修复结束 !!!】 ---
        
        cost_basis = getattr(position, 'cost_basis', state['base_price'])
        close_price = context.last_valid_price.get(symbol, state['base_price'])
        try:
            if not is_valid_price(close_price):
                close_price = cost_basis if cost_basis > 0 else state['base_price']
                if not is_valid_price(close_price):
                    close_price = 1.0
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

# ---------------- 【!!! 升级 v3.2.14 !!!】成交明细日志 ----------------

def log_trade_details(context, symbol, trade):
    """
    【!!! 升级 v3.2.14 !!!】
    为了支持 get_deliver 归因，必须增加 entrust_no 作为主键。
    """
    try:
        trade_log_path = research_path('reports', 'a_trade_details.csv')
        is_new = not trade_log_path.exists()
        
        # 【新增】从 trade 字典中获取 entrust_no
        entrust_no = trade.get('entrust_no', 'N/A') 
        
        with open(trade_log_path, 'a', encoding='utf-8', newline='') as f:
            if is_new:
                # 【新增】在表头增加 entrust_no
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
                entrust_no # 【新增】在行尾增加 entrust_no
            ]
            f.write(",".join(row) + "\n")
    except Exception as e:
        info('❌ [{}] 记录交易日志失败: {}', dsym(context, symbol), e)

# ---------------- 【!!! 升级 v3.2.14 !!!】HTML 看板 ----------------

def generate_html_report(context):
    all_metrics = []
    total_market_value = 0
    total_unrealized_pnl = 0
    
    # --- 【新增】PnL 指标汇总 ---
    total_realized_grid_pnl = 0
    total_realized_base_pnl = 0
    total_realized_pnl = 0
    # 【!!!】从 context.pnl_metrics (内存) 中读取，而不是在看板中计算
    pnl_metrics = getattr(context, 'pnl_metrics', {})
    # --- PnL 汇总结束 ---

    for symbol in context.symbol_list:
        if symbol not in context.state:
            continue
        state = context.state[symbol]
        
        # 【重要】看板使用补偿后的持仓
        position = get_position(symbol)
        
        # --- 【!!! 关键修复：移除补偿 !!!】 ---
        pos = position.amount # 默认
        # --- 【!!! 修复结束 !!!】 ---
        
        price = context.last_valid_price.get(symbol, state['base_price'])
        halted = context.mark_halted.get(symbol, False)
        if not is_valid_price(price):
            price = position.cost_basis if position.cost_basis > 0 else state['base_price']
            if not is_valid_price(price):
                price = 1.0
                
        market_value = pos * price
        unrealized_pnl = (price - position.cost_basis) * pos if position.cost_basis > 0 else 0
        total_market_value += market_value
        total_unrealized_pnl += unrealized_pnl
        
        atr_pct = calculate_atr(context, symbol)
        name_price = f"{price:.3f}" + (" (停牌)" if halted else "")
        disp_name = dsym(context, symbol, style='long')
        
        # --- 【新增】读取标的的 PnL 指标 ---
        sym_pnl = pnl_metrics.get(symbol, {})
        realized_grid_pnl = sym_pnl.get('realized_grid_pnl', 0)
        realized_base_pnl = sym_pnl.get('realized_base_pnl', 0)
        total_realized_sym_pnl = sym_pnl.get('total_realized_pnl', 0)
        
        # 计算总收益（已实现 + 未实现）
        total_sym_pnl = total_realized_sym_pnl + unrealized_pnl
        
        # 累加到账户总和
        total_realized_grid_pnl += realized_grid_pnl
        total_realized_base_pnl += realized_base_pnl
        total_realized_pnl += total_realized_sym_pnl
        # --- PnL 读取结束 ---
        
        all_metrics.append({
            "symbol": symbol,
            "symbol_disp": disp_name,
            "position": f"{pos} ({position.enable_amount})",
            "cost_basis": f"{position.cost_basis:.3f}",
            "price": name_price,
            "market_value": f"{market_value:,.2f}",
            # --- 【新增】收益指标 ---
            "unrealized_pnl": f"{unrealized_pnl:,.2f}",
            "realized_grid_pnl": f"{realized_grid_pnl:,.2f}",
            "realized_base_pnl": f"{realized_base_pnl:,.2f}",
            "total_realized_pnl": f"{total_realized_sym_pnl:,.2f}",
            "total_pnl": f"{total_sym_pnl:,.2f}",
            # ---
            "pnl_ratio": f"{(unrealized_pnl / (position.cost_basis * pos) * 100) if position.cost_basis * pos != 0 else 0:.2f}%",
            "base_position": state['base_position'],
            "grid_unit": state['grid_unit'],
            "grid_spacing": f"{state['buy_grid_spacing']:.2%} / {state['sell_grid_spacing']:.2%}",
            "atr_str": f"{atr_pct:.2%}" if atr_pct is not None else "N/A"
        })
        
    # --- 【新增】计算账户总收益 ---
    account_total_pnl = total_realized_pnl + total_unrealized_pnl
    
    html_template = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>策略运行看板 (v3.2.16-Deliver-FIX)</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                background-color: #121212;
                color: #e0e0e0;
                margin: 0;
                padding: 16px;
            }}
            .container {{ max-width: 1600px; margin: auto; }}
            h1, h2 {{ text-align: center; color: #ffffff; border-bottom: 2px solid #333; padding-bottom: 10px; margin-top: 20px; }}
            h1 {{ margin-top: 0; }}
            .update-time {{ text-align: center; color: #888; margin-top: -10px; margin-bottom: 20px; }}
            .summary-cards {{ display: flex; flex-wrap: wrap; gap: 16px; justify-content: center; margin-bottom: 30px; }}
            .card {{ background-color: #1e1e1e; padding: 16px 20px; border-radius: 8px; text-align: center; border: 1px solid #333; min-width: 200px; }}
            .card h3 {{ margin: 0 0 10px 0; color: #aaa; font-weight: normal; text-transform: uppercase; font-size: 0.9em; }}
            .card .value {{ font-size: 1.8em; font-weight: bold; }}
            .data-table {{ width: 100%; border-collapse: collapse; background-color: #1e1e1e; box-shadow: 0 2px 5px rgba(0,0,0,0.3); font-size: 0.9em; }}
            .data-table th, .data-table td {{ border: 1px solid #333; padding: 10px 12px; text-align: right; }}
            .data-table th {{ background-color: #2a2a2a; color: #ffffff; font-weight: bold; }}
            .data-table tbody tr:nth-child(even) {{ background-color: #242424; }}
            .data-table tbody tr:hover {{ background-color: #383838; }}
            .data-table td:first-child {{ text-align: left; font-weight: bold; white-space: nowrap; }}
            .positive {{ color: #4caf50; }}
            .negative {{ color: #f44336; }}
            .footer {{ text-align: center; margin-top: 20px; color: #888; font-size: 12px; }}
            .placeholder {{ text-align: center; padding: 40px; color: #666; font-style: italic; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>策略运行看板 (Deliver-FIX)</h1>
            <p class="update-time">最后更新时间: {update_time}</p>
            
            <div class="summary-cards">
                <div class="card">
                    <h3>总市值</h3>
                    <p class="value">{total_market_value}</p>
                </div>
                <div class="card">
                    <h3>总浮动盈亏</h3>
                    <p class="value {unrealized_pnl_class}">{total_unrealized_pnl}</p>
                </div>
                <div class="card">
                    <h3>总已实现盈亏</h3>
                    <p class="value {realized_pnl_class}">{total_realized_pnl}</p>
                </div>
                <div class="card">
                    <h3>账户总收益</h3>
                    <p class="value {total_pnl_class}">{account_total_pnl}</p>
                </div>
                <div class="card">
                    <h3>已实现(网格)</h3>
                    <p class="value {grid_pnl_class}">{total_realized_grid_pnl}</p>
                </div>
                <div class="card">
                    <h3>已实现(底仓)</h3>
                    <p class="value {base_pnl_class}">{total_realized_base_pnl}</p>
                </div>
            </div>
            
            <table class="data-table">
                <thead>
                    <tr>
                        <th style="text-align:left;">标的</th>
                        <th>持仓(可用)</th>
                        <th>成本</th>
                        <th>市价</th>
                        <th>市值</th>
                        <th>浮动盈亏</th>
                        <th>浮动盈K率</th>
                        <th>已实现(网格)</th>
                        <th>已实现(底仓)</th>
                        <th>总已实现</th>
                        <th>总收益</th>
                        <th>目标底仓</th>
                        <th>网格单位</th>
                        <th>买/卖间距</th>
                        <th>ATR(14d)</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>

            <h2>业绩归因分析 (v3.2.14-PnL-Deliver)</h2>
            <div class="placeholder">
                “资金回报率” (TWRR/MWRR) 需结合 `get_fundjour()` (历史资金流水) 进行计算，暂未实现。
            </div>

            <p class="footer">看板由策略每5分钟更新一次。请在PTRADE中手动刷新查看。</p>
        </div>
    </body>
    </html>
    """
    
    table_rows = ""
    for m in all_metrics:
        try: pnl_val = float(m["unrealized_pnl"].replace(",", ""))
        except Exception: pnl_val = 0.0
        pnl_class = "positive" if pnl_val >= 0 else "negative"
        
        try: total_pnl_val = float(m["total_pnl"].replace(",", ""))
        except Exception: total_pnl_val = 0.0
        total_pnl_class = "positive" if total_pnl_val >= 0 else "negative"
        
        try: grid_pnl_val = float(m["realized_grid_pnl"].replace(",", ""))
        except Exception: grid_pnl_val = 0.0
        grid_pnl_class = "positive" if grid_pnl_val > 0 else ("negative" if grid_pnl_val < 0 else "")

        try: base_pnl_val = float(m["realized_base_pnl"].replace(",", ""))
        except Exception: base_pnl_val = 0.0
        base_pnl_class = "positive" if base_pnl_val > 0 else ("negative" if base_pnl_val < 0 else "")
        
        try: total_realized_val = float(m["total_realized_pnl"].replace(",", ""))
        except Exception: total_realized_val = 0.0
        total_realized_class = "positive" if total_realized_val > 0 else ("negative" if total_realized_val < 0 else "")

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
            <td>{m['grid_spacing']}</td>
            <td>{m['atr_str']}</td>
        </tr>
        """
        
    final_html = html_template.format(
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total_market_value=f"{total_market_value:,.2f}",
        total_unrealized_pnl=f"{total_unrealized_pnl:,.2f}",
        unrealized_pnl_class="positive" if total_unrealized_pnl >= 0 else "negative",
        # --- 【新增】卡片数据 ---
        total_realized_pnl=f"{total_realized_pnl:,.2f}",
        realized_pnl_class="positive" if total_realized_pnl >= 0 else "negative",
        account_total_pnl=f"{account_total_pnl:,.2f}",
        total_pnl_class="positive" if account_total_pnl >= 0 else "negative",
        total_realized_grid_pnl=f"{total_realized_grid_pnl:,.2f}",
        grid_pnl_class="positive" if total_realized_grid_pnl > 0 else ("negative" if total_realized_grid_pnl < 0 else ""),
        total_realized_base_pnl=f"{total_realized_base_pnl:,.2f}",
        base_pnl_class="positive" if total_realized_base_pnl > 0 else ("negative" if total_realized_base_pnl < 0 else ""),
        # ---
        table_rows=table_rows
    )
    try:
        report_path = research_path('reports', 'strategy_dashboard.html')
        report_path.write_text(final_html, encoding='utf-8')
    except Exception as e:
        info(f'❌ 生成HTML看板失败: {e}')