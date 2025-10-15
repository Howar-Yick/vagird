# event_driven_grid_strategy.py
# 版本号：CHATGPT-3.2.1-20251014-HALT-GUARD-MKT-OFF1456-fix3d+cnames-hotfix
# 变更点（在 HALT-GUARD 基础上的最小改动）：
# 1) ❌ 不改市价单（仍然完全移除14:55市价触发）；
# 2) ⏰ 限价挂单窗口至14:56（保持既有逻辑）；
# 3) 🧹 日终统一撤单14:56（保持既有逻辑）；
# 4) 📴 14:56后不再发起新挂单（保持既有逻辑）；
# 5) 🪫 重启/竞价按 base_price 补挂网格；无实时价不阻断挂单（保持既有逻辑）；
# 6) 🕒 启动宽限期 boot_grace（默认180秒，可配置参数保留）；
# 7) 🛡️ 停牌仅影响展示，不拦截挂单（保持既有逻辑）；
# 8) 🔧 ✅ 实时价获取：用 get_snapshot(...) 的 last_px 更新 latest_data/last_valid_*；
# 9) 🧪 ✅ 实时价心跳：每“窗口秒”输出 got/miss；调试开关从【研究目录/config/debug.json】热加载：
#       原始结构：
#       {
#         "enable_debug_log": true,
#         "rt_heartbeat_window_sec": 65,
#         "rt_heartbeat_preview": 8
#       }
#       *保持向后兼容*: 若出现旧版临时键（"debug_rt_log","rt_log_interval_seconds"），也会被识别，但优先使用上述“原始结构”。
# 10) ⚙️ 棘轮：仅在连续竞价且拿到有效实时价时启用；无价时仍按 base_price 挂单但不移动基准。
# 11) 🈶️【新增，最小改动】日志与看板显示中文名称（来自 config/names.json 与 symbols.json 的 name 字段；仅影响展示，不改业务）
# 12) 🧯【热修】修正 update_daily_reports 中 t_quantity 一行的右括号手误（] -> )）

import json
import logging
import math
from datetime import datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace

# ---------------- 全局句柄与常量 ----------------
LOG_FH = None
MAX_SAVED_FILLED_IDS = 500
__version__ = 'CHATGPT-3.2.1-20251014-HALT-GUARD-MKT-OFF1456-fix3d+cnames-hotfix'
TRANSACTION_COST = 0.00005

# ---- 调试默认（可被 config/debug.json 覆盖）----
DBG_ENABLE_DEFAULT = True
DBG_RT_WINDOW_SEC_DEFAULT = 60
DBG_RT_PREVIEW_DEFAULT = 8

# ---------------- 通用路径与工具函数 ----------------

def research_path(*parts) -> Path:
    p = Path(get_research_path()).joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def info(msg, *args):
    text = msg.format(*args)
    log.info(text)
    if LOG_FH:
        LOG_FH.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} INFO {text}\n")
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

# ---------------- 标的中文名：可选读取 + 显示辅助（新增，最小改动） ----------------

def _load_symbol_names(context):
    """
    从两个地方读取中文名（有则用，无则忽略，不影响其它功能）：
    1) 研究目录 config/names.json   -> 形如 {"513230.SS":"标普500", ...}
    2) symbols.json 中每个标的可选字段 name -> 覆盖 names.json 的同名项
    """
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
    """
    返回用于日志/看板展示的标的名：
      style='short' -> "513230.SS 标普500"
      style='long'  -> "标普500(513230.SS)"
    若无中文名，仅返回代码本身。
    """
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

# ---------------- 调试配置：从研究目录 config/debug.json 读取 + 热加载 ----------------

def _load_debug_config(context, force=False):
    """
    读取/热加载 config/debug.json：
    原始结构（优先）：
      enable_debug_log: bool
      rt_heartbeat_window_sec: int
      rt_heartbeat_preview: int
    兼容字段（次优先）：
      debug_rt_log: bool
      rt_log_interval_seconds: int   （兼容为窗口秒）
      rt_log_preview: int            （若有人写错键名时也兜底）
    """
    cfg_file = research_path('config', 'debug.json')
    try:
        mtime = cfg_file.stat().st_mtime if cfg_file.exists() else None
    except:
        mtime = None

    if not force and hasattr(context, 'debug_cfg_mtime') and context.debug_cfg_mtime == mtime:
        return  # 无变化不重读

    # 默认值
    enable = DBG_ENABLE_DEFAULT
    winsec = DBG_RT_WINDOW_SEC_DEFAULT
    preview = DBG_RT_PREVIEW_DEFAULT

    try:
        if cfg_file.exists():
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if isinstance(j, dict):
                # 优先原始结构
                if 'enable_debug_log' in j: enable = bool(j['enable_debug_log'])
                if 'rt_heartbeat_window_sec' in j:
                    try: winsec = max(5, int(j['rt_heartbeat_window_sec']))
                    except: pass
                if 'rt_heartbeat_preview' in j:
                    try: preview = max(1, int(j['rt_heartbeat_preview']))
                    except: pass

                # 兼容老临时键（仅在原始结构缺失时生效）
                if 'enable_debug_log' not in j and 'debug_rt_log' in j:
                    enable = bool(j['debug_rt_log'])
                if 'rt_heartbeat_window_sec' not in j and 'rt_log_interval_seconds' in j:
                    try: winsec = max(5, int(j['rt_log_interval_seconds']))
                    except: pass
                if 'rt_heartbeat_preview' not in j and 'rt_log_preview' in j:
                    try: preview = max(1, int(j['rt_log_preview']))
                    except: pass
    except Exception as e:
        info('⚠️ 读取调试文件 config/debug.json 失败: {}（采用默认 enable={}, win={}s, preview={}）',
             e, enable, winsec, preview)

    context.enable_debug_log = enable
    context.rt_heartbeat_window_sec = winsec
    context.rt_heartbeat_preview = preview
    context.debug_cfg_mtime = mtime
    # 首次加载或配置变化时，允许立即打一条心跳
    context.last_rt_log_ts = None
    if enable:
        info('🧪 调试配置生效: enable={} window={}s preview={}', enable, winsec, preview)
    else:
        info('🧪 调试配置生效: enable=False（关闭心跳日志）')

# ---------------- 初始化与时间窗口判断 ----------------

def initialize(context):
    global LOG_FH
    log_file = research_path('logs', 'event_driven_strategy.log')
    LOG_FH = open(log_file, 'a', encoding='utf-8')
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

    # 容器
    context.symbol_list = list(context.symbol_config.keys())
    # 新增：加载中文名映射
    _load_symbol_names(context)

    context.state = {}
    context.latest_data = {}
    context.should_place_order_map = {}   # 占位（不用于市价触发）
    context.mark_halted = {}
    context.last_valid_price = {}
    context.last_valid_ts = {sym: None for sym in context.symbol_list}

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
            'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20)
        })
        context.state[sym] = st
        context.latest_data[sym] = st['base_price']
        context.should_place_order_map[sym] = True
        context.mark_halted[sym] = False
        context.last_valid_price[sym] = st['base_price']

    # 启动宽限期
    context.boot_dt = getattr(context, 'current_dt', None) or datetime.now()
    context.boot_grace_seconds = int(get_saved_param('boot_grace_seconds', 180))

    # 调试配置（首次加载）
    _load_debug_config(context, force=True)

    # 绑定定时任务
    context.initial_cleanup_done = False
    if '回测' not in context.env:
        run_daily(context, place_auction_orders, time='9:15')
        run_daily(context, end_of_day, time='14:56')
        info('✅ 事件驱动模式就绪')
    info('✅ 初始化完成，版本:{}', __version__)

def is_main_trading_time():
    now = datetime.now().time()
    return (time(9, 30) <= now <= time(11, 30)) or (time(13, 0) <= now <= time(15, 0))

def is_auction_time():
    now = datetime.now().time()
    return time(9, 15) <= now < time(9, 25)

def is_order_blocking_period():
    now = datetime.now().time()
    return time(9, 25) <= now < time(9, 30)

# ---------------- 启动后清理与收敛 ----------------

def before_trading_start(context, data):
    if context.initial_cleanup_done:
        return
    info('🔁 before_trading_start：清理遗留挂单')
    after_initialize_cleanup(context)
    current_time = context.current_dt.time()
    if time(9, 15) <= current_time < time(9, 30):
        info('⏭ 重启在集合竞价时段，补挂网格')
        place_auction_orders(context)
    else:
        info('⏸️ 重启时间{}不在集合竞价时段，跳过补挂网格', current_time.strftime('%H:%M:%S'))
    context.initial_cleanup_done = True

def after_initialize_cleanup(context):
    if '回测' in context.env or not hasattr(context, 'symbol_list'):
        return
    info('🧼 按品种清理所有遗留挂单')
    for sym in context.symbol_list:
        cancel_all_orders_by_symbol(context, sym)
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
        info('[{}] 👉 发现并尝试撤销遗留挂单 entrust_no={}', dsym(context, symbol), entrust_no)
        try:
            cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})
        except Exception as e:
            info('[{}] ⚠️ 撤单异常 entrust_no={}: {}', dsym(context, symbol), entrust_no, e)
    if total > 0:
        info('[{}] 共{}笔遗留挂单尝试撤销完毕', dsym(context, symbol), total)

# ---------------- 集合竞价挂单 ----------------

def place_auction_orders(context):
    if '回测' in context.env or not (is_auction_time() or is_main_trading_time()):
        return
    info('🆕 清空防抖缓存，开始集合竞价挂单（按 base_price 补挂）')
    for st in context.state.values():
        st.pop('_last_order_bp', None); st.pop('_last_order_ts', None)
    for sym in context.symbol_list:
        state = context.state[sym]
        adjust_grid_unit(state)
        cancel_all_orders_by_symbol(context, sym)
        context.latest_data[sym] = state['base_price']   # 不依赖新价
        place_limit_orders(context, sym, state)
        safe_save_state(sym, state)

# ---------------- 实时价：快照获取 + 心跳日志（支持热加载配置） ----------------

def _fetch_quotes_via_snapshot(context):
    """
    用 PTRADE get_snapshot 拉取快照（字段 last_px ）更新价格缓存。
    并按 config/debug.json 中的开关/窗口打印心跳 got/miss。
    同时在调用前做 debug 配置热加载（mtime 变更即生效）。
    """
    _load_debug_config(context, force=False)

    symbols = list(getattr(context, 'symbol_list', []) or [])
    if not symbols:
        return

    snaps = {}
    try:
        snaps = get_snapshot(symbols) or {}
    except Exception as e:
        if getattr(context, 'enable_debug_log', False):
            info('🧪 RT心跳 获取快照异常: {}', e)
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
            context.mark_halted[sym] = False
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
            info('🧪 RT心跳 {} got:{}/{} miss:[{}]',
                 now_dt.strftime('%H:%M'), got, len(symbols), miss_preview)

# ---------------- 网格限价挂单主逻辑 ----------------

def place_limit_orders(context, symbol, state):
    """
    限价挂单主函数（含“棘轮”与节流）。
    - 集合竞价/启动宽限期：允许“无价”按 base_price 挂单，不启用棘轮；
    - 连续竞价阶段：仅当拿到有效实时价才启用棘轮；无价也允许按 base_price 挂单。
    """
    now_dt = context.current_dt

    if state.get('_last_trade_ts') and (now_dt - state['_last_trade_ts']).total_seconds() < 60:
        return
    if is_order_blocking_period():
        return
    in_limit_window = is_auction_time() or (is_main_trading_time() and now_dt.time() < time(14, 56))
    if not in_limit_window:
        return

    boot_grace = (now_dt - getattr(context, 'boot_dt', now_dt)).total_seconds() < getattr(context, 'boot_grace_seconds', 180)
    allow_tickless = boot_grace or is_auction_time()

    base = state['base_price']
    unit, buy_sp, sell_sp = state['grid_unit'], state['buy_grid_spacing'], state['sell_grid_spacing']
    buy_p, sell_p = round(base * (1 - buy_sp), 3), round(base * (1 + sell_sp), 3)

    position = get_position(symbol)
    pos = position.amount + state.get('_pos_change', 0)

    price = context.latest_data.get(symbol)
    ratchet_enabled = (not allow_tickless) and is_valid_price(price)

    if ratchet_enabled:
        if abs(price / base - 1) <= 0.10:
            is_in_low_pos_range  = (pos - unit <= state['base_position'])
            is_in_high_pos_range = (pos + unit >= state['max_position'])
            sell_p_curr = round(base * (1 + sell_sp), 3)
            buy_p_curr  = round(base * (1 - buy_sp), 3)
            ratchet_up   = is_in_low_pos_range  and price >= sell_p_curr
            ratchet_down = is_in_high_pos_range and price <= buy_p_curr
            if ratchet_up:
                state['base_price'] = sell_p_curr
                info('[{}] 棘轮上移: 触及卖价，基准抬至 {:.3f}', dsym(context, symbol), sell_p_curr)
                cancel_all_orders_by_symbol(context, symbol)
                buy_p, sell_p = round(sell_p_curr * (1 - buy_sp), 3), round(sell_p_curr * (1 + sell_sp), 3)
            elif ratchet_down:
                state['base_price'] = buy_p_curr
                info('[{}] 棘轮下移: 触及买价，基准降至 {:.3f}', dsym(context, symbol), buy_p_curr)
                cancel_all_orders_by_symbol(context, symbol)
                buy_p, sell_p = round(buy_p_curr * (1 - buy_sp), 3), round(buy_p_curr * (1 + sell_sp), 3)

    last_ts = state.get('_last_order_ts')
    if last_ts and (now_dt - last_ts).seconds < 30:
        return
    last_bp = state.get('_last_order_bp')
    if last_bp and abs(base / last_bp - 1) < buy_sp / 2:
        return
    state['_last_order_ts'], state['_last_order_bp'] = now_dt, base

    try:
        open_orders = [o for o in get_open_orders(symbol) or [] if o.status == '2']
        enable_amount = position.enable_amount
        state.pop('_pos_change', None)

        can_buy = not any(o.amount > 0 and abs(o.price - buy_p) < 1e-3 for o in open_orders)
        if can_buy and pos + unit <= state['max_position']:
            info('[{}] --> 发起买入委托: {}股 @ {:.3f}', dsym(context, symbol), unit, buy_p)
            order(symbol, unit, limit_price=buy_p)

        can_sell = not any(o.amount < 0 and abs(o.price - sell_p) < 1e-3 for o in open_orders)
        if can_sell and enable_amount >= unit and pos - unit >= state['base_position']:
            info('[{}] --> 发起卖出委托: {}股 @ {:.3f}', dsym(context, symbol), unit, sell_p)
            order(symbol, -unit, limit_price=sell_p)

    except Exception as e:
        info('[{}] ⚠️ 限价挂单异常：{}', dsym(context, symbol), e)
    finally:
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
        context.state[sym]['filled_order_ids'].add(entrust_no)
        safe_save_state(sym, context.state[sym])
        order_obj = SimpleNamespace(
            order_id = entrust_no,
            amount   = tr['business_amount'] if tr['entrust_bs']=='1' else -tr['business_amount'],
            filled   = tr['business_amount'],
            price    = tr['business_price']
        )
        try:
            on_order_filled(context, sym, order_obj)
        except Exception as e:
            info('[{}] ❌ 成交处理失败：{}', dsym(context, sym), e)

def on_order_filled(context, symbol, order):
    state = context.state[symbol]
    if order.filled == 0:
        return
    last_dt = state.get('_last_fill_dt')
    if state.get('last_fill_price') == order.price and last_dt and (context.current_dt - last_dt).seconds < 5:
        return
    trade_direction = "买入" if order.amount > 0 else "卖出"
    info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f}', dsym(context, symbol), trade_direction, order.filled, order.price)
    state['_last_trade_ts'] = context.current_dt
    state['_last_fill_dt'] = context.current_dt
    state['last_fill_price'] = order.price
    state['base_price'] = order.price
    state['_pos_change'] = order.amount
    cancel_all_orders_by_symbol(context, symbol)

    context.mark_halted[symbol] = False
    context.last_valid_price[symbol] = order.price
    context.latest_data[symbol] = order.price
    context.last_valid_ts[symbol] = context.current_dt

    if is_order_blocking_period():
        info('[{}] 处于9:25-9:30挂单冻结期，成交后仅更新状态，推迟挂单至9:30后。', dsym(context, symbol))
    elif context.current_dt.time() < time(14, 56):
        place_limit_orders(context, symbol, state)

    context.should_place_order_map[symbol] = True
    safe_save_state(symbol, state)

# ---------------- 行情主循环 ----------------

def handle_data(context, data):
    now_dt = context.current_dt
    now = now_dt.time()

    _fetch_quotes_via_snapshot(context)

    if now_dt.minute % 5 == 0 and now_dt.second < 5:
        reload_config_if_changed(context)
        generate_html_report(context)

    boot_grace = (now_dt - getattr(context, 'boot_dt', now_dt)).total_seconds() < getattr(context, 'boot_grace_seconds', 180)
    if not boot_grace:
        def _phase_start(now_t: time):
            if time(9, 15) <= now_t < time(9, 25):
                return time(9, 15)
            if time(9, 30) <= now_t <= time(11, 30):
                return time(9, 30)
            if time(13, 0) <= now_t <= time(15, 0):
                return time(13, 0)
            return None

        phase_start_t = _phase_start(now)
        if phase_start_t:
            phase_start_dt = datetime.combine(now_dt.date(), phase_start_t)
            grace_seconds = 120
            for sym in context.symbol_list:
                last_ts = context.last_valid_ts.get(sym)
                if last_ts is None or last_ts < phase_start_dt:
                    context.mark_halted[sym] = (now_dt >= phase_start_dt + timedelta(seconds=grace_seconds))
                else:
                    context.mark_halted[sym] = ((now_dt - last_ts).total_seconds() > grace_seconds)

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

    if is_auction_time() or (is_main_trading_time() and now < time(14, 56)):
        for sym in context.symbol_list:
            if sym in context.state:
                place_limit_orders(context, sym, context.state[sym])

    if now_dt.minute % 30 == 0 and now_dt.second < 5:
        info('📌 每30分钟状态巡检...')
        for sym in context.symbol_list:
            if sym in context.state:
                log_status(context, sym, context.state[sym], context.latest_data.get(sym))

# ---------------- 监控输出 ----------------

def log_status(context, symbol, state, price):
    disp_price = context.last_valid_price.get(symbol, state['base_price'])
    if not is_valid_price(disp_price):
        return
    pos = get_position(symbol)
    pnl = (disp_price - pos.cost_basis) * pos.amount if pos.cost_basis > 0 else 0
    info("📊 [{}] 状态: 价:{:.3f} 持仓:{}(可卖:{}) / 底仓:{} 成本:{:.3f} 盈亏:{:.2f} 网格:[买{:.2%},卖{:.2%}]",
         dsym(context, symbol), disp_price, pos.amount, pos.enable_amount, state['base_position'], pos.cost_basis, pnl, state['buy_grid_spacing'], state['sell_grid_spacing'])

# ---------------- 动态网格间距（ATR） ----------------

def update_grid_spacing_final(context, symbol, state, curr_pos):
    unit, base_pos = state['grid_unit'], state['base_position']
    atr_pct = calculate_atr(context, symbol)
    base_spacing = 0.005
    if atr_pct is not None:
        atr_multiplier = 0.25
        base_spacing = atr_pct * atr_multiplier
    min_spacing = TRANSACTION_COST * 5
    base_spacing = max(base_spacing, min_spacing)
    if curr_pos <= base_pos + unit * 5:
        new_buy, new_sell = base_spacing, base_spacing * 2
    elif curr_pos > base_pos + unit * 15:
        new_buy, new_sell = base_spacing * 2, base_spacing
    else:
        new_buy, new_sell = base_spacing, base_spacing
    max_spacing = 0.03
    new_buy  = round(min(new_buy,  max_spacing), 4)
    new_sell = round(min(new_sell, max_spacing), 4)
    if new_buy != state.get('buy_grid_spacing') or new_sell != state.get('sell_grid_spacing'):
        state['buy_grid_spacing'], state['sell_grid_spacing'] = new_buy, new_sell
        info('[{}] 🌀 网格动态调整. ATR({:.2%}) -> 基础间距({:.2%}) -> 最终:[买{:.2%},卖{:.2%}]',
             dsym(context, symbol), (atr_pct or 0.0), base_spacing, new_buy, new_sell)

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
    """14:56 统一撤单 + 看板 + 状态保存（不再触发任何市价单）"""
    info('✅ 日终处理开始(14:56)...')
    after_initialize_cleanup(context)
    generate_html_report(context)
    for sym in context.symbol_list:
        if sym in context.state:
            safe_save_state(sym, context.state[sym])
            context.should_place_order_map[sym] = True
    info('✅ 日终保存状态完成')

# ---------------- 价值平均（VA） ----------------

def get_target_base_position(context, symbol, state, price, dt):
    if not is_valid_price(price):
        info('[{}] ⚠️ 停牌/无有效价，跳过VA计算，底仓维持 {}', dsym(context, symbol), state['base_position'])
        return state['base_position']
    weeks = get_trade_weeks(context, symbol, state, dt)
    target_val = state['initial_position_value'] + sum(state['dingtou_base'] * (1 + state['dingtou_rate'])**w for w in range(1, weeks + 1))
    if price <= 0:
        return state['base_position']
    new_pos = target_val / price
    min_base = round(state['initial_position_value'] / state['base_price'] / 100) * 100 if state['base_price'] > 0 else 0
    final_pos = round(max(min_base, new_pos) / 100) * 100
    if final_pos != state['base_position']:
        current_val = state['base_position'] * price
        delta_val = target_val - current_val
        info('[{}] 价值平均: 目标底仓从 {} 调整至 {}. (目标市值: {:.2f}, 当前市值: {:.2f}, 市值缺口: {:.2f})',
             dsym(context, symbol), state['base_position'], final_pos, target_val, current_val, delta_val)
        state['base_position'] = final_pos
        state['max_position'] = final_pos + state['grid_unit'] * 20
    return final_pos

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
            info('🔧 [{}] 底仓增加，网格单位放大: {}->{}', state.get('symbol',''), orig, new_u)

# ---------------- 交易结束回调（平台触发） ----------------

def after_trading_end(context, data):
    if '回测' in context.env:
        return
    info('⏰ 系统调用交易结束处理')
    update_daily_reports(context, data)
    info('✅ 交易结束处理完成')

# ---------------- 配置热重载 ----------------

def reload_config_if_changed(context):
    try:
        current_mod_time = context.config_file_path.stat().st_mtime
        if current_mod_time == context.last_config_mod_time:
            return
        info('🔄 检测到配置文件发生变更，开始热重载...')
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
                'max_position': cfg['initial_base_position'] + cfg['grid_unit'] * 20
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
        _load_symbol_names(context)  # 热更新中文名
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
        pos_obj = get_position(symbol)
        amount = getattr(pos_obj, 'amount', 0)
        cost_basis = getattr(pos_obj, 'cost_basis', state['base_price'])
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
        # 🔧 Hotfix: 右括号修正
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

# ---------------- 成交明细日志 ----------------

def log_trade_details(context, symbol, trade):
    try:
        trade_log_path = research_path('reports', 'a_trade_details.csv')
        is_new = not trade_log_path.exists()
        with open(trade_log_path, 'a', encoding='utf-8', newline='') as f:
            if is_new:
                headers = ["time", "symbol", "direction", "quantity", "price", "base_position_at_trade"]
                f.write(",".join(headers) + "\n")
            direction = "BUY" if trade['entrust_bs'] == '1' else "SELL"
            base_position = context.state[symbol].get('base_position', 0)
            row = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                symbol,
                direction,
                str(trade['business_amount']),
                f"{trade['business_price']:.3f}",
                str(base_position)
            ]
            f.write(",".join(row) + "\n")
    except Exception as e:
        info('❌ [{}] 记录交易日志失败: {}', dsym(context, symbol), e)

# ---------------- HTML 看板 ----------------

def generate_html_report(context):
    all_metrics = []
    total_market_value = 0
    total_unrealized_pnl = 0
    for symbol in context.symbol_list:
        if symbol not in context.state:
            continue
        state = context.state[symbol]
        pos = get_position(symbol)
        price = context.last_valid_price.get(symbol, state['base_price'])
        halted = context.mark_halted.get(symbol, False)
        if not is_valid_price(price):
            price = pos.cost_basis if pos.cost_basis > 0 else state['base_price']
            if not is_valid_price(price):
                price = 1.0
        market_value = pos.amount * price
        unrealized_pnl = (price - pos.cost_basis) * pos.amount if pos.cost_basis > 0 else 0
        total_market_value += market_value
        total_unrealized_pnl += unrealized_pnl
        atr_pct = calculate_atr(context, symbol)
        name_price = f"{price:.3f}" + (" (停牌)" if halted else "")
        disp_name = dsym(context, symbol, style='long')
        all_metrics.append({
            "symbol": symbol,
            "symbol_disp": disp_name,
            "position": f"{pos.amount} ({pos.enable_amount})",
            "cost_basis": f"{pos.cost_basis:.3f}",
            "price": name_price,
            "market_value": f"{market_value:,.2f}",
            "unrealized_pnl": f"{unrealized_pnl:,.2f}",
            "pnl_ratio": f"{(unrealized_pnl / (pos.cost_basis * pos.amount) * 100) if pos.cost_basis * pos.amount != 0 else 0:.2f}%",
            "base_position": state['base_position'],
            "grid_unit": state['grid_unit'],
            "grid_spacing": f"{state['buy_grid_spacing']:.2%} / {state['sell_grid_spacing']:.2%}",
            "atr_str": f"{atr_pct:.2%}" if atr_pct is not None else "N/A"
        })
    html_template = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>策略运行看板</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                background-color: #121212;
                color: #e0e0e0;
                margin: 0;
                padding: 20px;
            }}
            .container {{ max-width: 1400px; margin: auto; }}
            h1, h2 {{ text-align: center; color: #ffffff; border-bottom: 2px solid #333; padding-bottom: 10px; margin-top: 20px; }}
            h1 {{ margin-top: 0; }}
            .update-time {{ text-align: center; color: #888; margin-top: -10px; margin-bottom: 20px; }}
            .summary-cards {{ display: flex; gap: 20px; justify-content: center; margin-bottom: 30px; }}
            .card {{ background-color: #1e1e1e; padding: 20px; border-radius: 8px; text-align: center; border: 1px solid #333; min-width: 250px; }}
            .card h3 {{ margin: 0 0 10px 0; color: #aaa; font-weight: normal; text-transform: uppercase; font-size: 1em; }}
            .card .value {{ font-size: 2em; font-weight: bold; }}
            .data-table {{ width: 100%; border-collapse: collapse; background-color: #1e1e1e; box-shadow: 0 2px 5px rgba(0,0,0,0.3); }}
            .data-table th, .data-table td {{ border: 1px solid #333; padding: 12px 15px; text-align: right; }}
            .data-table th {{ background-color: #2a2a2a; color: #ffffff; font-weight: bold; }}
            .data-table tbody tr:nth-child(even) {{ background-color: #242424; }}
            .data-table tbody tr:hover {{ background-color: #383838; }}
            .data-table td:first-child {{ text-align: left; font-weight: bold; }}
            .positive {{ color: #4caf50; }}
            .negative {{ color: #f44336; }}
            .footer {{ text-align: center; margin-top: 20px; color: #888; font-size: 12px; }}
            .placeholder {{ text-align: center; padding: 40px; color: #666; font-style: italic; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>策略运行看板</h1>
            <p class="update-time">最后更新时间: {update_time}</p>
            <div class="summary-cards">
                <div class="card">
                    <h3>总市值</h3>
                    <p class="value">{total_market_value}</p>
                </div>
                <div class="card">
                    <h3>总浮动盈亏</h3>
                    <p class="value {pnl_class}">{total_unrealized_pnl}</p>
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
                        <th>盈亏率</th>
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

            <h2>业绩归因分析</h2>
            <div class="placeholder">
                数据采集中... 未来版本将在此处展示详细的盈亏归因分析。
            </div>

            <p class="footer">看板由策略每5分钟更新一次。请在PTRADE中手动刷新查看。</p>
        </div>
    </body>
    </html>
    """
    table_rows = ""
    for m in all_metrics:
        pnl_class = "positive" if float(m["unrealized_pnl"].replace(",", "")) >= 0 else "negative"
        table_rows += f"""
        <tr>
            <td>{m['symbol_disp']}</td>
            <td>{m['position']}</td>
            <td>{m['cost_basis']}</td>
            <td>{m['price']}</td>
            <td>{m['market_value']}</td>
            <td class="{pnl_class}">{m['unrealized_pnl']}</td>
            <td class="{pnl_class}">{m['pnl_ratio']}</td>
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
        pnl_class="positive" if total_unrealized_pnl >= 0 else "negative",
        table_rows=table_rows
    )
    try:
        report_path = research_path('reports', 'strategy_dashboard.html')
        report_path.write_text(final_html, encoding='utf-8')
    except Exception as e:
        info(f'❌ 生成HTML看板失败: {e}')
