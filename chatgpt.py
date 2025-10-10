# event_driven_grid_strategy.py                               # 文件：事件驱动网格策略（含VA与“棘轮”）
# 版本号：CHATGPT-3.1-20251010-MIN-SPACING-CFG               # 版本：3.1 / 日期：2025-10-10 / 修改：最小间距可配+安全增强
# 变更摘要：
# 1) 固化“最小网格间距=单边手续费×因子(默认5)”并允许每标的用 min_spacing_factor 覆盖；
# 2) 统一时间来源 _now_dt(context)，所有时段判断函数改为使用它（防环境偏差）；
# 3) 修复 CSV 写入的小笔误（join 写法小错误）；
# 4) 订单/持仓字段在 dict/对象两种返回类型下的安全访问封装（_safe_order_field / _safe_position_fields）；
# 5) 价差与委托价统一保留到小数点后三位，匹配沪深ETF最小价位单位 0.001（tick 对齐）。

import json  # JSON 读写
import logging  # 日志
import math  # 数学函数
from datetime import datetime, time  # 时间、时刻
from pathlib import Path  # 路径
from types import SimpleNamespace  # 简易对象

# ===== 全局常量与缓存 =====
LOG_FH = None                                                  # 日志文件句柄
MAX_SAVED_FILLED_IDS = 500                                     # 成交去重的最大保存数
__version__ = 'CHATGPT-3.1-20251010-MIN-SPACING-CFG'           # 【本次版本号】务必与头部注释一致
TRANSACTION_COST = 0.00005                                     # 单边交易成本（示例：0.005%）
_ATR_CACHE = {}                                                # ATR 百分比缓存（分钟粒度），减算力

# ===== 路径与通用工具 =====
def research_path(*parts) -> Path:
    """研究目录根 + 子路径；确保文件夹存在。"""
    p = Path(get_research_path()).joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def info(msg, *args):
    """统一 info 日志到控制台与文件。"""
    text = msg.format(*args)
    log.info(text)
    if LOG_FH:
        LOG_FH.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} INFO {text}\n")
        LOG_FH.flush()

def get_saved_param(key, default=None):
    """从参数存储读取；失败返回默认。"""
    try: return get_parameter(key)
    except: return default

def set_saved_param(key, value):
    """写入参数存储；失败忽略。"""
    try: set_parameter(key, value)
    except: pass

def check_environment():
    """判定运行环境：回测 / 实盘 / 模拟 / 未知。"""
    try:
        u = str(get_user_name())
        if u == '55418810': return '回测'
        if u == '8887591588': return '实盘'
        return '模拟'
    except:
        return '未知'

def convert_symbol_to_standard(full_symbol):
    """将 .XSHE/.XSHG 标准化为 .SZ/.SS；其他原样返回。"""
    if not isinstance(full_symbol, str): return full_symbol
    if full_symbol.endswith('.XSHE'): return full_symbol.replace('.XSHE','.SZ')
    if full_symbol.endswith('.XSHG'): return full_symbol.replace('.XSHG','.SS')
    return full_symbol

def _safe_order_field(o, k, default=None):
    """订单字段安全读取：兼容 dict / 对象 两种返回。"""
    try:
        if isinstance(o, dict): return o.get(k, default)
        return getattr(o, k, default)
    except:
        return default

def _safe_position_fields(p):
    """持仓字段安全读取，返回 (amount, enable_amount, cost_basis)。"""
    try:
        amount = getattr(p, 'amount', 0)
        enable = getattr(p, 'enable_amount', 0)
        cost   = getattr(p, 'cost_basis', 0.0)
        return amount, enable, cost
    except:
        return 0, 0, 0.0

def _now_dt(context):
    """统一时间来源，优先 context.current_dt，其次系统时间。"""
    try:
        return getattr(context, 'current_dt', None) or datetime.now()
    except:
        return datetime.now()

# ===== 状态持久化 =====
def save_state(symbol, state):
    """将关键状态同步到参数存储与 state/<symbol>.json。"""
    ids = list(state.get('filled_order_ids', set()))
    state['filled_order_ids'] = set(ids[-MAX_SAVED_FILLED_IDS:])
    store_keys = ['base_price', 'grid_unit', 'max_position', 'last_week_position', 'base_position']
    store = {k: state.get(k) for k in store_keys}
    store['filled_order_ids'] = ids[-MAX_SAVED_FILLED_IDS:]
    store['trade_week_set'] = list(state.get('trade_week_set', []))
    set_saved_param(f'state_{symbol}', store)
    research_path('state', f'{symbol}.json').write_text(json.dumps(store, indent=2), encoding='utf-8')

def safe_save_state(symbol, state):
    """保存状态，异常时仅记录日志。"""
    try: save_state(symbol, state)
    except Exception as e: info('[{}] ⚠️ 状态保存失败: {}', symbol, e)

# ===== 交易时段判定（全部使用 _now_dt(context)）=====
def is_main_trading_time(context=None):
    """盘中主时段：9:30-11:30, 13:00-15:00。"""
    now = _now_dt(context).time()
    return (time(9, 30) <= now <= time(11, 30)) or (time(13, 0) <= now <= time(15, 0))

def is_auction_time(context=None):
    """集合竞价时段：9:15-9:25。"""
    now = _now_dt(context).time()
    return time(9, 15) <= now < time(9, 25)

def is_order_blocking_period(context=None):
    """挂单冻结时段：9:25-9:30。"""
    now = _now_dt(context).time()
    return time(9, 25) <= now < time(9, 30)

# ===== 初始化 =====
def initialize(context):
    """策略初始化：加载配置、建立状态、注册日内事件。"""
    global LOG_FH
    log_file = research_path('logs', 'event_driven_strategy.log')
    LOG_FH = open(log_file, 'a', encoding='utf-8')
    log.info(f'🔍 日志同时写入到 {log_file}')
    context.env = check_environment()
    info("当前环境：{}", context.env)
    context.run_cycle = get_saved_param('run_cycle_seconds', 60)

    # 读取 symbols.json
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
    context.state = {}
    context.latest_data = {}
    context.should_place_order_map = {}

    # 初始化每个标的 state（兼容无新字段的配置）
    for sym, cfg in context.symbol_config.items():
        state_file = research_path('state', f'{sym}.json')
        saved = json.loads(state_file.read_text(encoding='utf-8')) if state_file.exists() else get_saved_param(f'state_{sym}', {}) or {}
        st = {**cfg}
        st.update({
            'symbol': sym,
            'base_price': saved.get('base_price', cfg['base_price']),
            'grid_unit': saved.get('grid_unit', cfg['grid_unit']),
            'filled_order_ids': set(saved.get('filled_order_ids', [])),
            'trade_week_set': set(saved.get('trade_week_set', [])),
            'base_position': saved.get('base_position', cfg['initial_base_position']),
            'last_week_position': saved.get('last_week_position', cfg['initial_base_position']),
            'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
            # 初始网格间距，如果没配则给 0.5%
            'buy_grid_spacing': cfg.get('buy_grid_spacing', 0.005),
            'sell_grid_spacing': cfg.get('sell_grid_spacing', 0.005),
            # 最大仓默认：底仓 + 20 格
            'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20),
            # 价格偏离保护（相对 base_price），默认 10%
            'max_deviation': cfg.get('max_deviation', 0.10),
            # ATR 与间距相关参数（全可选）
            'atr_period': cfg.get('atr_period', 14),
            'atr_multiplier': cfg.get('atr_multiplier', 0.25),
            'spacing_cap': cfg.get('spacing_cap', 0.03),
            # ——最小网格间距保护：min_spacing = 单边交易成本 × 因子（默认 5）——
            'min_spacing_factor': cfg.get('min_spacing_factor', 5),
        })
        context.state[sym] = st
        context.latest_data[sym] = st['base_price']
        context.should_place_order_map[sym] = True

    # 注册事件
    context.initial_cleanup_done = False
    if '回测' not in context.env:
        run_daily(context, place_auction_orders, time='9:15')
        run_daily(context, end_of_day, time='14:55')
        info('✅ 事件驱动模式就绪')
    info('✅ 初始化完成，版本:{}', __version__)

# ===== 开盘前清理与竞价补挂 =====
def before_trading_start(context, data):
    """开盘前：清理遗留挂单，必要时在竞价补挂。"""
    if context.initial_cleanup_done: return
    info('🔁 before_trading_start：清理遗留挂单')
    after_initialize_cleanup(context)
    current_time = _now_dt(context).time()
    if time(9, 15) <= current_time < time(9, 30):
        info('⏭ 重启在集合竞价时段，补挂网格')
        place_auction_orders(context)
    else:
        info('⏸️ 重启时间{}不在集合竞价时段，跳过补挂网格', current_time.strftime('%H:%M:%S'))
    context.initial_cleanup_done = True

def after_initialize_cleanup(context):
    """启动后按品种撤所有遗留“活动挂单”。"""
    if '回测' in context.env or not hasattr(context, 'symbol_list'): return
    info('🧼 按品种清理所有遗留挂单')
    for sym in context.symbol_list:
        cancel_all_orders_by_symbol(context, sym)
    info('✅ 按品种清理完成')

def get_order_status(entrust_no):
    """保守查询订单最终状态，失败不抛错。"""
    try:
        order_detail = get_order(entrust_no)
        return str(order_detail.get('status', '')) if order_detail else ''
    except Exception as e:
        info('⚠️ 查询订单状态失败 entrust_no={}: {}', entrust_no, e)
        return ''

def cancel_all_orders_by_symbol(context, symbol):
    """撤销指定标的的所有“活动挂单”（status=='2'）。"""
    all_orders = get_all_orders() or []
    total = 0
    if not hasattr(context, 'canceled_cache'):
        context.canceled_cache = {'date': None, 'orders': set()}
    today = _now_dt(context).date()
    if context.canceled_cache.get('date') != today:
        context.canceled_cache = {'date': today, 'orders': set()}
    cache = context.canceled_cache['orders']
    for o in all_orders:
        api_sym = _safe_order_field(o, 'symbol') or _safe_order_field(o, 'stock_code')
        if convert_symbol_to_standard(api_sym) != symbol: continue
        status = str(_safe_order_field(o, 'status', ''))
        entrust_no = _safe_order_field(o, 'entrust_no')
        if (not entrust_no) or status != '2' or entrust_no in context.state[symbol]['filled_order_ids'] or entrust_no in cache:
            continue
        final_status = get_order_status(entrust_no)
        if final_status in ('4', '5', '6', '8'):  # 已撤/废/部撤/已成 等
            continue
        cache.add(entrust_no)
        total += 1
        info('[{}] 👉 发现并尝试撤销遗留挂单 entrust_no={}', symbol, entrust_no)
        try:
            cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})
        except Exception as e:
            info('[{}] ⚠️ 撤单异常 entrust_no={}: {}', symbol, entrust_no, e)
    if total > 0:
        info('[{}] 共{}笔遗留挂单尝试撤销完毕', symbol, total)

def place_auction_orders(context):
    """集合竞价阶段统一撤旧单并补挂当日第一组网格。"""
    if '回测' in context.env or not (is_auction_time(context) or is_main_trading_time(context)): return
    info('🆕 清空防抖缓存，开始集合竞价挂单')
    for st in context.state.values():
        st.pop('_last_order_bp', None)
        st.pop('_last_order_ts', None)
    for sym in context.symbol_list:
        state = context.state[sym]
        adjust_grid_unit(state)
        cancel_all_orders_by_symbol(context, sym)
        context.latest_data[sym] = state['base_price']
        place_limit_orders(context, sym, state)
        safe_save_state(sym, state)

# ===== 限价网格（含“棘轮”）=====
def place_limit_orders(context, symbol, state):
    """
    网格限价挂单主流程：
    1) 棘轮触发（只买不卖触顶 / 只卖不买触底）→ 立即更新基准价并重算网格；
    2) 否则走 30s 节流 + 半格内不重挂的防抖；
    3) 同价位去重后下单。
    """
    now_dt = _now_dt(context)

    # 前置约束
    if state.get('_last_trade_ts') and (now_dt - state['_last_trade_ts']).total_seconds() < 60: return
    if is_order_blocking_period(context): return
    if not (is_auction_time(context) or (is_main_trading_time(context) and now_dt.time() < time(14, 50))): return

    # 价格与偏离保护
    price = context.latest_data.get(symbol)
    if not (price and price > 0): return
    base = state['base_price']
    if abs(price / base - 1) > state.get('max_deviation', 0.10): return

    # 网格关键变量（委托价与间距均保留到千分位，匹配 ETF tick）
    unit = state['grid_unit']
    buy_sp, sell_sp = state['buy_grid_spacing'], state['sell_grid_spacing']
    buy_p, sell_p = round(base * (1 - buy_sp), 3), round(base * (1 + sell_sp), 3)

    # 当前持仓（含最近一次成交的 _pos_change 消费前预估）
    position = get_position(symbol)
    pos, enable, _ = _safe_position_fields(position)
    pos = pos + state.get('_pos_change', 0)

    # 棘轮触发条件：靠近下沿只买不卖→触及卖价上移；靠近上沿只卖不买→触及买价下移
    is_in_low_pos_range  = (pos - unit <= state['base_position'])
    ratchet_up   = is_in_low_pos_range and price >= sell_p
    is_in_high_pos_range = (pos + unit >= state['max_position'])
    ratchet_down = is_in_high_pos_range and price <= buy_p

    # 非棘轮 → 节流/防抖
    if not (ratchet_up or ratchet_down):
        last_ts = state.get('_last_order_ts')
        if last_ts and (now_dt - last_ts).seconds < 30: return
        last_bp = state.get('_last_order_bp')
        if last_bp and abs(base / last_bp - 1) < buy_sp / 2: return
        state['_last_order_ts'], state['_last_order_bp'] = now_dt, base

    # 棘轮：更新基准价并重算网格上下沿
    if ratchet_up:
        state['base_price'] = sell_p
        info('[{}] 棘轮上移: 价格上涨触及卖价，基准价上移至 {:.3f}', symbol, sell_p)
        cancel_all_orders_by_symbol(context, symbol)
        buy_p  = round(sell_p * (1 - state['buy_grid_spacing']), 3)
        sell_p = round(sell_p * (1 + state['sell_grid_spacing']), 3)
    elif ratchet_down:
        state['base_price'] = buy_p
        info('[{}] 棘轮下移: 价格下跌触及买价，基准价下移至 {:.3f}', symbol, buy_p)
        cancel_all_orders_by_symbol(context, symbol)
        buy_p  = round(buy_p * (1 - state['buy_grid_spacing']), 3)
        sell_p = round(buy_p * (1 + state['sell_grid_spacing']), 3)

    # 下单（同价去重）
    try:
        open_orders = [o for o in (get_open_orders(symbol) or []) if str(_safe_order_field(o, 'status')) == '2']
        state.pop('_pos_change', None)  # 消费后清理
        # 买单同价去重
        can_buy = not any((_safe_order_field(o,'amount',0) > 0) and (abs(float(_safe_order_field(o,'price',0))-buy_p) < 1e-3) for o in open_orders)
        if can_buy and pos + unit <= state['max_position']:
            info('[{}] --> 发起买入委托: {}股 @ {:.3f}', symbol, unit, buy_p)
            order(symbol, unit, limit_price=buy_p)
        # 卖单同价去重
        can_sell = not any((_safe_order_field(o,'amount',0) < 0) and (abs(float(_safe_order_field(o,'price',0))-sell_p) < 1e-3) for o in open_orders)
        if can_sell and enable >= unit and pos - unit >= state['base_position']:
            info('[{}] --> 发起卖出委托: {}股 @ {:.3f}', symbol, unit, sell_p)
            order(symbol, -unit, limit_price=sell_p)
    except Exception as e:
        info('[{}] ⚠️ 限价挂单异常：{}', symbol, e)
    finally:
        safe_save_state(symbol, state)

# ===== 成交回报处理 =====
def on_trade_response(context, trade_list):
    """撮合回报入口：仅处理 status=='8' 的成交。"""
    for tr in trade_list:
        if str(tr.get('status')) != '8': continue
        sym = convert_symbol_to_standard(tr['stock_code'])
        entrust_no = tr['entrust_no']
        log_trade_details(context, sym, tr)
        if sym not in context.state or entrust_no in context.state[sym]['filled_order_ids']:
            continue
        context.state[sym]['filled_order_ids'].add(entrust_no)
        safe_save_state(sym, context.state[sym])
        signed_amount = tr['business_amount'] if tr['entrust_bs']=='1' else -tr['business_amount']
        order_obj = SimpleNamespace(order_id=entrust_no, amount=signed_amount, filled=tr['business_amount'], price=tr['business_price'])
        try:
            on_order_filled(context, sym, order_obj)
        except Exception as e:
            info('[{}] ❌ 成交处理失败：{}', sym, e)

def on_order_filled(context, symbol, order):
    """单笔成交落地：更新基准价、消单、必要时重布网格。"""
    state = context.state[symbol]
    if order.filled == 0: return
    last_dt = state.get('_last_fill_dt')
    if state.get('last_fill_price') == order.price and last_dt and (_now_dt(context) - last_dt).seconds < 5:
        return
    trade_direction = "买入" if order.amount > 0 else "卖出"
    info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f}', symbol, trade_direction, order.filled, order.price)
    now_dt = _now_dt(context)
    state['_last_trade_ts'] = now_dt
    state['_last_fill_dt']  = now_dt
    state['last_fill_price'] = order.price
    state['base_price'] = order.price
    info('[{}] 🔄 成交后基准价更新为 {:.3f}', symbol, order.price)
    state['_pos_change'] = order.amount
    cancel_all_orders_by_symbol(context, symbol)
    if is_order_blocking_period(context):
        info('[{}] 处于9:25-9:30挂单冻结期，成交后仅更新状态，推迟挂单至9:30后。', symbol)
    elif now_dt.time() < time(14, 50):
        place_limit_orders(context, symbol, state)
    context.should_place_order_map[symbol] = True
    safe_save_state(symbol, state)

# ===== 主循环 =====
def handle_data(context, data):
    """每个周期被调用：更新行情、VA、网格参数与必要的挂单。"""
    now_dt = _now_dt(context)
    now = now_dt.time()

    # 每5分钟：热重载 + 看板
    if now_dt.minute % 5 == 0 and now_dt.second < 5:
        reload_config_if_changed(context)
        generate_html_report(context)

    # 行情更新（data 可能不含全部标的）
    latest = {}
    for sym in context.symbol_list:
        if sym in data and getattr(data[sym], 'price', None):
            latest[sym] = data[sym].price
        else:
            latest[sym] = context.latest_data.get(sym, context.state[sym]['base_price'])
    context.latest_data = latest

    # 更新 VA 目标 & 网格单位 & 动态间距
    for sym in context.symbol_list:
        st = context.state.get(sym)
        price = context.latest_data.get(sym)
        if not st or not price: continue
        get_target_base_position(context, sym, st, price, now_dt)
        adjust_grid_unit(st)
        if now_dt.minute % 30 == 0 and now_dt.second < 5:
            update_grid_spacing_final(context, sym, st, get_position(sym).amount)

    # 竞价或盘中（14:50 前）布网格
    if is_auction_time(context) or (is_main_trading_time(context) and now < time(14, 50)):
        for sym in context.symbol_list:
            if sym in context.state:
                place_limit_orders(context, sym, context.state[sym])

    # 14:55-14:57 盘尾的市价兜底
    if time(14, 55) <= now < time(14, 57):
        for sym in context.symbol_list:
            if sym in context.state:
                place_market_orders_if_triggered(context, sym, context.state[sym])

    # 每30分钟状态巡检
    if now_dt.minute % 30 == 0 and now_dt.second < 5:
        info('📌 每30分钟状态巡检...')
        for sym in context.symbol_list:
            if sym in context.state:
                log_status(context, sym, context.state[sym], context.latest_data.get(sym))

# ===== 盘尾市价兜底 =====
def place_market_orders_if_triggered(context, symbol, state):
    """在 14:55-14:57 之间，如果触价则以市价完成该网格的买/卖。"""
    if not is_main_trading_time(context): return
    price = context.latest_data.get(symbol)
    if not (price and price > 0): return
    base = state['base_price']
    if abs(price / base - 1) > state.get('max_deviation', 0.10): return
    adjust_grid_unit(state)
    pos, enable, _ = _safe_position_fields(get_position(symbol))
    unit = state['grid_unit']
    buy_p  = round(base * (1 - state['buy_grid_spacing']), 3)
    sell_p = round(base * (1 + state['sell_grid_spacing']), 3)
    if not context.should_place_order_map.get(symbol, True): return
    try:
        if price <= buy_p and pos + unit <= state['max_position']:
            info('[{}] 市价买触发: {}股 @ {:.3f}', symbol, unit, price)
            order_market(symbol, unit, market_type='0')
            state['base_price'] = buy_p
        elif price >= sell_p and pos - unit >= state['base_position']:
            info('[{}] 市价卖触发: {}股 @ {:.3f}', symbol, unit, price)
            order_market(symbol, -unit, market_type='0')
            state['base_price'] = sell_p
    except Exception as e:
        info('[{}] ⚠️ 市价挂单异常：{}', symbol, e)
    finally:
        context.should_place_order_map[symbol] = False
        safe_save_state(symbol, state)

# ===== 状态日志 =====
def log_status(context, symbol, state, price):
    """打印单标的关键运行状态。"""
    if not price: return
    pos_obj = get_position(symbol)
    amount, enable, cost = _safe_position_fields(pos_obj)
    pnl = (price - cost) * amount if cost > 0 else 0
    info("📊 [{}] 状态: 价:{:.3f} 持仓:{}(可卖:{}) / 底仓:{} 成本:{:.3f} 盈亏:{:.2f} 网格:[买{:.2%},卖{:.2%}]",
         symbol, price, amount, enable, state['base_position'], cost, pnl,
         state['buy_grid_spacing'], state['sell_grid_spacing'])

# ===== 动态网格间距（ATR 驱动 + 最小保护）=====
def update_grid_spacing_final(context, symbol, state, curr_pos):
    """
    计算最终网格间距：
    1) 若拿到 ATR，则 base_spacing = ATR% × atr_multiplier，否则沿用当前买间距；
    2) 最小保护：base_spacing = max(base_spacing, TRANSACTION_COST × min_spacing_factor)；
    3) 仓位分层：低仓位→(买近,卖远)、高仓位→(买远,卖近)、中性→对称；
    4) 用 spacing_cap（默认3%）封顶，并保留到小数点后4位（百分比精度）。
    """
    unit, base_pos = state['grid_unit'], state['base_position']

    # 1) ATR 百分比（分钟缓存）
    atr_pct = calculate_atr(context, symbol, atr_period=state.get('atr_period', 14))

    # 2) 基础间距
    base_spacing = state.get('buy_grid_spacing', 0.005)
    if atr_pct is not None:
        base_spacing = atr_pct * state.get('atr_multiplier', 0.25)

    # ——最小保护：不低于 单边成本 × 因子（默认5）——
    # 如需“往返覆盖”，可将 TRANSACTION_COST 改为 TRANSACTION_COST*2。
    min_spacing = TRANSACTION_COST * state.get('min_spacing_factor', 5)
    base_spacing = max(base_spacing, min_spacing)

    # 3) 仓位分层偏置
    if curr_pos <= base_pos + unit * 5:
        new_buy, new_sell = base_spacing, base_spacing * 2
    elif curr_pos > base_pos + unit * 15:
        new_buy, new_sell = base_spacing * 2, base_spacing
    else:
        new_buy, new_sell = base_spacing, base_spacing

    # 4) 上限与小数精度（百分比四位，价格端千分位）
    cap = state.get('spacing_cap', 0.03)
    new_buy  = round(min(new_buy,  cap), 4)
    new_sell = round(min(new_sell, cap), 4)

    # 实际更新
    if new_buy != state.get('buy_grid_spacing') or new_sell != state.get('sell_grid_spacing'):
        state['buy_grid_spacing'], state['sell_grid_spacing'] = new_buy, new_sell
        info('[{}] 🌀 网格动态调整. ATR({:.2%}) -> 基础间距({:.2%}) -> 最终:[买{:.2%},卖{:.2%}]',
             symbol, (atr_pct or 0.0), base_spacing, new_buy, new_sell)

# ===== ATR 计算（带缓存与列适配）=====
def _extract_series(df, col):
    """兼容单层列与 MultiIndex 列的取值。"""
    if col in df.columns: return df[col].values
    try:
        lvl0 = [c for c in df.columns if (isinstance(c, tuple) and c[0] == col)]
        if lvl0: return df[lvl0[0]].values
    except:
        pass
    raise KeyError(f'missing column {col}')

def calculate_atr(context, symbol, atr_period=14):
    """使用 get_history 计算 ATR（TR 简单均值版），失败返回 None。"""
    try:
        now = _now_dt(context)
        cache_key = (symbol, atr_period, now.strftime('%Y%m%d%H%M'))
        if cache_key in _ATR_CACHE:
            return _ATR_CACHE[cache_key]

        hist = get_history(atr_period + 1, '1d', ['high','low','close'], security_list=[symbol])
        if hist is None:
            info('[{}] ⚠️ ATR计算失败: get_history 返回 None。', symbol); return None
        try:
            n = len(hist)
        except:
            info('[{}] ⚠️ ATR计算失败: 历史数据不可迭代。', symbol); return None
        if n < atr_period + 1:
            info('[{}] ⚠️ ATR计算失败: 历史数据不足 {} 条。', symbol, atr_period + 1); return None

        try:
            high  = _extract_series(hist, 'high')
            low   = _extract_series(hist, 'low')
            close = _extract_series(hist, 'close')
        except Exception as e:
            info('[{}] ⚠️ ATR列解析失败: {}', symbol, e); return None

        trs = []
        for i in range(1, len(high)):
            h, l, pc = high[i], low[i], close[i-1]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        if not trs: return None

        atr_value = sum(trs[-atr_period:]) / atr_period
        current_price = context.latest_data.get(symbol, close[-1])
        if current_price and current_price > 0:
            atr_pct = atr_value / current_price
            _ATR_CACHE[cache_key] = atr_pct
            return atr_pct
        return None
    except Exception as e:
        info('[{}] ❌ ATR计算异常: {}', symbol, e)
        return None

# ===== VA（价值平均）与收盘处理、热重载、报表 =====
def end_of_day(context):
    """收盘后流程：撤单、看板、保存状态。"""
    info('✅ 日终处理开始...')
    after_initialize_cleanup(context)
    generate_html_report(context)
    for sym in context.symbol_list:
        if sym in context.state:
            safe_save_state(sym, context.state[sym])
            context.should_place_order_map[sym] = True
    info('✅ 日终保存状态完成')

def get_trade_weeks(context, symbol, state, dt):
    """返回从启动以来经历过的 ISO 周总数，并在周切换时刷新 last_week_position。"""
    y, w, _ = dt.date().isocalendar()
    key = f"{y}_{w}"
    if key not in state.get('trade_week_set', set()):
        if 'trade_week_set' not in state: state['trade_week_set'] = set()
        state['trade_week_set'].add(key)
        state['last_week_position'] = state['base_position']
        safe_save_state(symbol, state)
    return len(state['trade_week_set'])

def get_target_base_position(context, symbol, state, price, dt):
    """计算 VA 目标底仓，并在变化时同步 max_position。"""
    weeks = get_trade_weeks(context, symbol, state, dt)
    target_val = state['initial_position_value'] + sum(state['dingtou_base'] * (1 + state['dingtou_rate'])**w for w in range(1, weeks + 1))
    if price <= 0: return state['base_position']
    new_pos = target_val / price
    min_base = round(state['initial_position_value'] / state['base_price'] / 100) * 100 if state['base_price'] > 0 else 0
    final_pos = round(max(min_base, new_pos) / 100) * 100
    if final_pos != state['base_position']:
        current_val = state['base_position'] * price
        delta_val = target_val - current_val
        info('[{}] 价值平均: 目标底仓从 {} 调整至 {}. (目标市值: {:.2f}, 当前市值: {:.2f}, 市值缺口: {:.2f})',
             symbol, state['base_position'], final_pos, target_val, current_val, delta_val)
        state['base_position'] = final_pos
        state['max_position']  = final_pos + state['grid_unit'] * 20
    return final_pos

def adjust_grid_unit(state):
    """当底仓规模显著增大时，按 1.2× 放大 grid_unit（向上取整到百股），保持网格效率。"""
    orig, base_pos = state['grid_unit'], state['base_position']
    if base_pos >= orig * 20:
        new_u = math.ceil(orig * 1.2 / 100) * 100
        if new_u != orig:
            state['grid_unit'] = new_u
            state['max_position'] = base_pos + new_u * 20
            info('🔧 [{}] 底仓增加，网格单位放大: {}->{}', state.get('symbol',''), orig, new_u)

def reload_config_if_changed(context):
    """监控 symbols.json 的 mtime，热重载新增/变更/移除标的。"""
    try:
        current_mod_time = context.config_file_path.stat().st_mtime
        if current_mod_time == context.last_config_mod_time: return
        info('🔄 检测到配置文件发生变更，开始热重载...')
        context.last_config_mod_time = current_mod_time
        new_config = json.loads(context.config_file_path.read_text(encoding='utf-8'))
        old_symbols, new_symbols = set(context.symbol_list), set(new_config.keys())

        # 移除
        for sym in old_symbols - new_symbols:
            info(f'[{sym}] 标的已从配置中移除，将清理其状态和挂单...')
            cancel_all_orders_by_symbol(context, sym)
            context.symbol_list.remove(sym)
            if sym in context.state: del context.state[sym]
            if sym in context.latest_data: del context.latest_data[sym]

        # 新增
        for sym in new_symbols - old_symbols:
            info(f'[{sym}] 新增标的，正在初始化状态...')
            cfg = new_config[sym]
            st = {**cfg}
            st.update({
                'symbol': sym,
                'base_price': cfg['base_price'],
                'grid_unit': cfg['grid_unit'],
                'filled_order_ids': set(),
                'trade_week_set': set(),
                'base_position': cfg['initial_base_position'],
                'last_week_position': cfg['initial_base_position'],
                'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
                'buy_grid_spacing': cfg.get('buy_grid_spacing', 0.005),
                'sell_grid_spacing': cfg.get('sell_grid_spacing', 0.005),
                'max_position': cfg['initial_base_position'] + cfg['grid_unit'] * 20,
                'max_deviation': cfg.get('max_deviation', 0.10),
                'atr_period': cfg.get('atr_period', 14),
                'atr_multiplier': cfg.get('atr_multiplier', 0.25),
                'spacing_cap': cfg.get('spacing_cap', 0.03),
                'min_spacing_factor': cfg.get('min_spacing_factor', 5),
            })
            context.state[sym] = st
            context.latest_data[sym] = st['base_price']
            context.symbol_list.append(sym)

        # 参数变更
        for sym in old_symbols.intersection(new_symbols):
            if context.symbol_config[sym] != new_config[sym]:
                info(f'[{sym}] 参数发生变更，正在更新...')
                state, new_params = context.state[sym], new_config[sym]
                state.update({
                    'grid_unit': new_params['grid_unit'],
                    'dingtou_base': new_params['dingtou_base'],
                    'dingtou_rate': new_params['dingtou_rate'],
                    'max_deviation': new_params.get('max_deviation', state.get('max_deviation', 0.10)),
                    'buy_grid_spacing': new_params.get('buy_grid_spacing', state.get('buy_grid_spacing', 0.005)),
                    'sell_grid_spacing': new_params.get('sell_grid_spacing', state.get('sell_grid_spacing', 0.005)),
                    'atr_period': new_params.get('atr_period', state.get('atr_period', 14)),
                    'atr_multiplier': new_params.get('atr_multiplier', state.get('atr_multiplier', 0.25)),
                    'spacing_cap': new_params.get('spacing_cap', state.get('spacing_cap', 0.03)),
                    'min_spacing_factor': new_params.get('min_spacing_factor', state.get('min_spacing_factor', 5)),
                    'max_position': state['base_position'] + new_params['grid_unit'] * 20
                })

        context.symbol_config = new_config
        info('✅ 配置文件热重载完成！当前监控标的: {}', context.symbol_list)
    except Exception as e:
        info(f'❌ 配置文件热重载失败: {e}')

def update_daily_reports(context, data):
    """为每个标的维护 CSV 报表；每个交易日收盘后追加一行。"""
    reports_dir = research_path('reports')
    reports_dir.mkdir(parents=True, exist_ok=True)
    current_date = _now_dt(context).strftime("%Y-%m-%d")
    for symbol in context.symbol_list:
        report_file = reports_dir / f"{symbol}.csv"
        state       = context.state[symbol]
        pos_obj     = get_position(symbol)
        amount, _, cost_basis = _safe_position_fields(pos_obj)
        close_price = context.latest_data.get(symbol, state['base_price'])
        try:
            close_price = getattr(close_price, 'price', close_price)
        except:
            close_price = state['base_price']

        weeks       = len(state.get('trade_week_set', []))
        d_base      = state['dingtou_base']
        d_rate      = state['dingtou_rate']

        # VA 口径
        invest_should   = d_base                                      # 当周应投（线性提示）
        invest_actual   = d_base * (1 + d_rate) ** weeks              # 当周实投（几何）
        cumulative_invest = sum(d_base * (1 + d_rate) ** w for w in range(1, weeks+1))
        expected_value  = state['initial_position_value'] + cumulative_invest

        last_week_val   = state.get('last_week_position', 0) * close_price
        current_val     = amount * close_price
        weekly_return   = (current_val - last_week_val) / last_week_val if last_week_val>0 else 0.0
        total_return    = (current_val - cumulative_invest) / cumulative_invest if cumulative_invest>0 else 0.0
        weekly_bottom_profit = (state['base_position'] - state.get('last_week_position', 0)) * close_price
        total_bottom_profit  = state['base_position'] * close_price - state['initial_position_value']
        standard_qty    = state['base_position'] + state['grid_unit'] * 5
        intermediate_qty= state['base_position'] + state['grid_unit'] * 15
        added_base      = state['base_position'] - state.get('last_week_position', 0)
        compare_cost    = added_base * close_price
        profit_all      = (close_price - cost_basis) * amount if cost_basis > 0 else 0
        t_quantity      = max(0, amount - state['base_position'])

        row = [
            current_date, f"{close_price:.3f}", str(weeks),
            str(weeks),
            f"{weekly_return:.2%}", f"{total_return:.2%}", f"{expected_value:.2f}",
            f"{invest_should:.0f}", f"{invest_actual:.0f}", f"{cumulative_invest:.0f}",
            str(state['base_position']), str(state['base_position']),
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
                    "当周应投入金额(线性)","当周实际投入金额(几何)","实际累计投入金额(几何)",
                    "定投底仓份额","累计底仓份额","累计底仓价值","每期累计底仓盈利","总累计底仓盈利",
                    "底仓","股票余额","单次网格交易数量","可T数量","标准数量","中间数量",
                    "极限数量","成本价","对比定投成本(近似)","盈亏(总)"
                ]
                f.write(",".join(headers) + "\n")
            f.write(",".join(map(str, row)) + "\n")
        info(f'✅ [{symbol}] 已更新每日CSV报表：{report_file}')

def log_trade_details(context, symbol, trade):
    """记录每一笔成交的精简日志到 reports/a_trade_details.csv。"""
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
        info(f'❌ 记录交易日志失败: {e}')

def generate_html_report(context):
    """生成一个深色主题的 HTML 看板（每5分钟重写一次）。"""
    all_metrics = []
    total_market_value = 0
    total_unrealized_pnl = 0

    for symbol in context.symbol_list:
        if symbol not in context.state: continue
        state = context.state[symbol]
        pos = get_position(symbol)
        price = context.latest_data.get(symbol, 0)
        amount, enable, cost = _safe_position_fields(pos)
        market_value = amount * price
        unrealized_pnl = (price - cost) * amount if cost > 0 else 0
        total_market_value += market_value
        total_unrealized_pnl += unrealized_pnl
        atr_pct = calculate_atr(context, symbol, atr_period=state.get('atr_period',14))
        all_metrics.append({
            "symbol": symbol,
            "position": f"{amount} ({enable})",
            "cost_basis": f"{cost:.3f}",
            "price": f"{price:.3f}",
            "market_value": f"{market_value:,.2f}",
            "unrealized_pnl": f"{unrealized_pnl:,.2f}",
            "pnl_ratio": f"{(unrealized_pnl / (cost * amount) * 100) if cost * amount != 0 else 0:.2f}%",
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
            <td>{m['symbol']}</td>
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
        update_time=_now_dt(context).strftime("%Y-%m-%d %H:%M:%S"),
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
