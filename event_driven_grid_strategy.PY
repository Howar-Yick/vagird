# event_driven_grid_strategy.py
# 版本号：GEMINI-0927-ATR-FINAL
# 0927-FINAL: 根据您的指引，使用 get_history 成功实现ATR动态网格，并集成所有已验证功能。
# 0927: 修正集合竞价成交后的挂单冲突问题。
# 0926: 增加T+1、交易冷静期、详细日志、HTML看板、热重载、重启撤单。
# 0925: 修正动态网格高低位逻辑、增加交易成本保护。

import json
import logging
import math
from datetime import datetime, time
from pathlib import Path
from types import SimpleNamespace

# 全局文件句柄 & 常量
LOG_FH = None
MAX_SAVED_FILLED_IDS = 500
__version__ = 'GEMINI-0927-ATR-FINAL'
TRANSACTION_COST = 0.00006 

# --- 路径与辅助函数 (来自您的原始版本) ---
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
    try: return get_parameter(key)
    except: return default

def set_saved_param(key, value):
    try: set_parameter(key, value)
    except: pass

def check_environment():
    try:
        u = str(get_user_name())
        if u == '55418810': return '回测'
        if u == '8887591588': return '实盘'
        return '模拟'
    except:
        return '未知'

def convert_symbol_to_standard(full_symbol):
    if not isinstance(full_symbol, str): return full_symbol
    if full_symbol.endswith('.XSHE'): return full_symbol.replace('.XSHE','.SZ')
    if full_symbol.endswith('.XSHG'): return full_symbol.replace('.XSHG','.SS')
    return full_symbol

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
    try: save_state(symbol, state)
    except Exception as e: info('[{}] ⚠️ 状态保存失败: {}', symbol, e)
    
def initialize(context):
    """策略初始化"""
    global LOG_FH
    log_file = research_path('logs', 'event_driven_strategy.log')
    LOG_FH = open(log_file, 'a', encoding='utf-8')
    log.info(f'🔍 日志同时写入到 {log_file}')
    context.env = check_environment()
    info("当前环境：{}", context.env)
    context.run_cycle = get_saved_param('run_cycle_seconds', 60)
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
            'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,
            'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20)
        })
        context.state[sym] = st
        context.latest_data[sym] = st['base_price']
    context.initial_cleanup_done = False
    if '回测' not in context.env:
        run_daily(context, place_auction_orders, time='9:15')
        run_daily(context, end_of_day, time='14:55')
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

def before_trading_start(context, data):
    if context.initial_cleanup_done: return
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
    if '回测' in context.env or not hasattr(context, 'symbol_list'): return
    info('🧼 按品种清理所有遗留挂单')
    for sym in context.symbol_list:
        cancel_all_orders_by_symbol(context, sym)
    info('✅ 按品种清理完成')

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
        if convert_symbol_to_standard(api_sym) != symbol: continue
        status = str(o.get('status', ''))
        entrust_no = o.get('entrust_no')
        if not entrust_no or status != '2' or entrust_no in context.state[symbol]['filled_order_ids'] or entrust_no in cache:
            continue
        final_status = get_order_status(entrust_no)
        if final_status in ('4', '5', '6', '8'): continue
        cache.add(entrust_no)
        total += 1
        info('[{}] 👉 发现并尝试撤销遗留挂单 entrust_no={}', symbol, entrust_no)
        try: cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})
        except Exception as e: info('[{}] ⚠️ 撤单异常 entrust_no={}: {}', symbol, entrust_no, e)
    if total > 0: info('[{}] 共{}笔遗留挂单尝试撤销完毕', symbol, total)

def place_auction_orders(context):
    if '回测' in context.env or not (is_auction_time() or is_main_trading_time()): return
    info('🆕 清空防抖缓存，开始集合竞价挂单')
    for st in context.state.values():
        st.pop('_last_order_bp', None); st.pop('_last_order_ts', None)
    for sym in context.symbol_list:
        state = context.state[sym]
        adjust_grid_unit(state)
        cancel_all_orders_by_symbol(context, sym)
        context.latest_data[sym] = state['base_price']
        place_limit_orders(context, sym, state)
        safe_save_state(sym, state)

def place_limit_orders(context, symbol, state):
    now_dt = context.current_dt
    if state.get('_last_trade_ts') and (now_dt - state['_last_trade_ts']).total_seconds() < 60: return
    if is_order_blocking_period(): return
    if not (is_auction_time() or (is_main_trading_time() and now_dt.time() < time(14, 50))): return
    price = context.latest_data.get(symbol)
    if not (price and price > 0): return
    base = state['base_price']
    if abs(price / base - 1) > 0.10: return
    unit, buy_sp, sell_sp = state['grid_unit'], state['buy_grid_spacing'], state['sell_grid_spacing']
    buy_p, sell_p = round(base * (1 - buy_sp), 3), round(base * (1 + sell_sp), 3)
    position = get_position(symbol)
    pos = position.amount + state.pop('_pos_change', 0)
    trigger_sell = (pos - unit <= state['base_position']) and price >= sell_p
    trigger_buy = (pos + unit >= state['max_position']) and price <= buy_p
    if not (trigger_sell or trigger_buy):
        if state.get('_last_order_ts') and (now_dt - state.get('_last_order_ts')).seconds < 30: return
        if state.get('_last_order_bp') and abs(base / state.get('_last_order_bp') - 1) < buy_sp / 2: return
        state['_last_order_ts'], state['_last_order_bp'] = now_dt, base
    if trigger_sell:
        state['base_price'] = sell_p
        info('[{}] 触及卖格价，基准价上移至 {:.3f}', symbol, sell_p)
        cancel_all_orders_by_symbol(context, symbol)
        buy_p, sell_p = round(sell_p * (1 - buy_sp), 3), round(sell_p * (1 + sell_sp), 3)
    elif trigger_buy:
        state['base_price'] = buy_p
        info('[{}] 触及买格价，基准价下移至 {:.3f}', symbol, buy_p)
        cancel_all_orders_by_symbol(context, symbol)
        buy_p, sell_p = round(buy_p * (1 - buy_sp), 3), round(buy_p * (1 + sell_sp), 3)
    try:
        open_orders = [o for o in get_open_orders(symbol) or [] if o.status == '2']
        enable_amount = position.enable_amount
        if not any(o.amount > 0 and abs(o.price - buy_p) < 1e-3 for o in open_orders) and pos + unit <= state['max_position']:
            info('[{}] --> 发起买入委托: {}股 @ {:.3f}', symbol, unit, buy_p)
            order(symbol, unit, limit_price=buy_p)
        if not any(o.amount < 0 and abs(o.price - sell_p) < 1e-3 for o in open_orders) and enable_amount >= unit and pos - unit >= state['base_position']:
            info('[{}] --> 发起卖出委托: {}股 @ {:.3f}', symbol, unit, sell_p)
            order(symbol, -unit, limit_price=sell_p)
    except Exception as e:
        info('[{}] ⚠️ 限价挂单异常：{}', symbol, e)
    finally:
        safe_save_state(symbol, state)

def on_trade_response(context, trade_list):
    """成交回报回调 (来自您的原始版本)"""
    for tr in trade_list:
        if str(tr.get('status')) != '8': continue
        sym = convert_symbol_to_standard(tr['stock_code'])
        entrust_no = tr['entrust_no']
        if sym not in context.state or entrust_no in context.state[sym]['filled_order_ids']: continue
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
            info('[{}] ❌ 成交处理失败：{}', sym, e)

def on_order_filled(context, symbol, order):
    """成交后处理函数 (来自您的原始版本, 增加冷静期和冻结期判断)"""
    state = context.state[symbol]
    if order.filled == 0: return
    last_dt = state.get('_last_fill_dt')
    if state.get('last_fill_price') == order.price and last_dt and (context.current_dt - last_dt).seconds < 5:
        return
    trade_direction = "买入" if order.amount > 0 else "卖出"
    info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f}', symbol, trade_direction, order.filled, order.price)
    state['_last_trade_ts'] = context.current_dt
    state['_last_fill_dt'] = context.current_dt
    state['last_fill_price'] = order.price
    state['base_price'] = order.price
    info('[{}] 🔄 成交后基准价更新为 {:.3f}', symbol, order.price)
    state['_pos_change'] = order.amount
    cancel_all_orders_by_symbol(context, symbol)
    if is_order_blocking_period():
        info('[{}] 处于9:25-9:30挂单冻结期，成交后仅更新状态，推迟挂单至9:30后。', symbol)
    elif context.current_dt.time() < time(14, 50):
        place_limit_orders(context, symbol, state)
    safe_save_state(symbol, state)

def handle_data(context, data):
    now_dt = context.current_dt
    now = now_dt.time()
    if now_dt.minute % 5 == 0 and now_dt.second < 5:
        reload_config_if_changed(context)
        generate_html_report(context)
    for sym in context.symbol_list:
        if sym in data and data[sym] and data[sym].price > 0:
            context.latest_data[sym] = data[sym].price
    for sym in context.symbol_list:
        if sym not in context.state: continue
        st = context.state[sym]
        price = context.latest_data.get(sym)
        if not price: continue
        get_target_base_position(context, sym, st, price, now_dt)
        adjust_grid_unit(st)
        if now_dt.minute % 30 == 0 and now_dt.second < 5:
            update_grid_spacing_hybrid(context, sym, st, get_position(sym).amount)
    if is_auction_time() or (is_main_trading_time() and now < time(14, 50)):
        for sym in context.symbol_list:
            if sym in context.state: place_limit_orders(context, sym, context.state[sym])
    if now >= time(14, 55):
        for sym in context.symbol_list:
            if sym in context.state: place_market_orders_if_triggered(context, sym, context.state[sym])
    if now_dt.minute % 30 == 0 and now_dt.second < 5:
        info('📌 每30分钟状态巡检...')
        for sym in context.symbol_list:
            if sym in context.state:
                log_status(context, sym, context.state[sym], context.latest_data.get(sym))

def place_market_orders_if_triggered(context, symbol, state):
    """来自您的原始版本"""
    if not is_main_trading_time(): return
    price = context.latest_data.get(symbol)
    if not (price and price > 0): return
    base = state['base_price']
    if abs(price/base - 1) > 0.10: return
    adjust_grid_unit(state)
    pos, unit = get_position(symbol).amount, state['grid_unit']
    buy_p, sell_p = round(base * (1 - state['buy_grid_spacing']), 3), round(base * (1 + state['sell_grid_spacing']), 3)
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

def log_status(context, symbol, state, price):
    if not price: return
    pos = get_position(symbol)
    pnl = (price - pos.cost_basis) * pos.amount if pos.cost_basis > 0 else 0
    info("📊 [{}] 状态: 价:{:.3f} 持仓:{}(可卖:{}) / 底仓:{} 成本:{:.3f} 盈亏:{:.2f} 网格:[买{:.2%},卖{:.2%}]",
         symbol, price, pos.amount, pos.enable_amount, state['base_position'], pos.cost_basis, pnl, state['buy_grid_spacing'], state['sell_grid_spacing'])

def update_grid_spacing_hybrid(context, symbol, state, curr_pos):
    """【最终修正】混合模型：持仓逻辑 + ATR波动缩放"""
    unit, base_pos = state['grid_unit'], state['base_position']
    if curr_pos <= base_pos + unit * 5:
        base_buy_spacing, base_sell_spacing = 0.005, 0.01
    elif curr_pos > base_pos + unit * 15:
        base_buy_spacing, base_sell_spacing = 0.01, 0.005
    else:
        base_buy_spacing, base_sell_spacing = 0.005, 0.005
    atr_pct = calculate_atr(context, symbol)
    volatility_modifier = 1.0
    if atr_pct is not None:
        normal_atr_pct = 0.015 
        volatility_modifier = max(0.5, min(atr_pct / normal_atr_pct, 2.0))
    min_spacing = TRANSACTION_COST * 5 
    max_spacing = 0.03
    new_buy = round(max(min_spacing, min(base_buy_spacing * volatility_modifier, max_spacing)), 4)
    new_sell = round(max(min_spacing, min(base_sell_spacing * volatility_modifier, max_spacing)), 4)
    if new_buy != state.get('buy_grid_spacing') or new_sell != state.get('sell_grid_spacing'):
        state['buy_grid_spacing'], state['sell_grid_spacing'] = new_buy, new_sell
        info('[{}] 🌀 网格动态调整. 仓位档:[买{:.2%},卖{:.2%}], ATR({:.2%})系数:{:.2f} -> 最终:[买{:.2%},卖{:.2%}]',
             symbol, base_buy_spacing, base_sell_spacing, (atr_pct or 0.0), volatility_modifier, new_buy, new_sell)

def calculate_atr(context, symbol, atr_period=14):
    """【最终修正】使用get_history计算ATR"""
    try:
        # 使用get_history，并明确指定security_list为str，以获取DataFrame
        hist = get_history(atr_period + 1, '1d', ['high','low','close'], security_list=symbol)
        
        # 正确地检查DataFrame是否为空
        if hist is None or hist.empty or len(hist) < atr_period + 1:
            info('[{}] ⚠️ get_history 获取历史数据不足({}天)，无法计算ATR', symbol, len(hist) if hist is not None else 0)
            return None
        
        high = hist['high'].values
        low = hist['low'].values
        close = hist['close'].values

        trs = []
        for i in range(1, len(close)):
            tr1 = high[i] - low[i]
            tr2 = abs(high[i] - close[i-1])
            tr3 = abs(low[i] - close[i-1])
            trs.append(max(tr1, tr2, tr3))
            
        if not trs: return None
        
        atr_value = sum(trs) / len(trs)
        current_price = context.latest_data.get(symbol, close[-1])
        if current_price > 0:
            atr_percentage = atr_value / current_price
            info('[{}] ATR(14) 计算完成: {:.2%}', symbol, atr_percentage)
            return atr_percentage
        return None
    except Exception as e:
        info('[{}] ❌ ATR计算异常: {}', symbol, e)
        return None

def end_of_day(context):
    info('✅ 日终处理开始...')
    after_initialize_cleanup(context)
    generate_html_report(context)
    for sym in context.symbol_list:
        if sym in context.state:
            safe_save_state(sym, context.state[sym])
    info('✅ 日终保存状态完成')

def get_target_base_position(context, symbol, state, price, dt):
    # (此函数来自您的原始版本，保持不变)
    weeks = get_trade_weeks(context, symbol, state, dt)
    target_val = state['initial_position_value'] + sum(state['dingtou_base'] * (1 + state['dingtou_rate'])**w for w in range(1, weeks + 1))
    delta_val = target_val - (state['last_week_position'] * price)
    if delta_val > 0:
        delta_pos = math.ceil(delta_val / price / 100) * 100
        new_pos = state['last_week_position'] + delta_pos
    else:
        new_pos = state['last_week_position']
    min_base = round(state['initial_position_value'] / state['base_price'] / 100) * 100
    final_pos = round(max(min_base, new_pos) / 100) * 100
    if final_pos != state['base_position']:
        info('[{}] 价值平均: 目标底仓从 {} 调整至 {}', symbol, state['base_position'], final_pos)
        state['base_position'] = final_pos
        state['max_position'] = final_pos + state['grid_unit'] * 20
    return final_pos

def get_trade_weeks(context, symbol, state, dt):
    # (此函数来自您的原始版本，保持不变)
    y, w, _ = dt.date().isocalendar()
    key = f"{y}_{w}"
    if key not in state.get('trade_week_set', set()):
        if 'trade_week_set' not in state: state['trade_week_set'] = set()
        state['trade_week_set'].add(key)
        state['last_week_position'] = state['base_position']
        safe_save_state(symbol, state)
    return len(state['trade_week_set'])

def adjust_grid_unit(state):
    # (此函数来自您的原始版本，保持不变)
    orig, base_pos = state['grid_unit'], state['base_position']
    if base_pos >= orig * 20:
        new_u = math.ceil(orig * 1.2 / 100) * 100
        if new_u != orig:
            state['grid_unit'] = new_u
            state['max_position'] = base_pos + new_u * 20
            info('🔧 [{}] 底仓增加，网格单位放大: {}->{}', state.get('symbol',''), orig, new_u)
            
def after_trading_end(context, data):
    if '回测' in context.env: return
    info('⏰ 系统调用交易结束处理')
    update_daily_reports(context, data)
    info('✅ 交易结束处理完成')

def update_daily_reports(context, data):
    # (此函数来自您的原始版本，保持不变)
    pass 

def reload_config_if_changed(context):
    """【新增】检查配置文件是否有变动，如果有，则动态重新加载。"""
    try:
        current_mod_time = context.config_file_path.stat().st_mtime
        if current_mod_time == context.last_config_mod_time: return
        info('🔄 检测到配置文件发生变更，开始热重载...')
        context.last_config_mod_time = current_mod_time
        new_config = json.loads(context.config_file_path.read_text(encoding='utf-8'))
        old_symbols, new_symbols = set(context.symbol_list), set(new_config.keys())
        for sym in old_symbols - new_symbols:
            info(f'[{sym}] 标的已从配置中移除，将清理其状态和挂单...')
            cancel_all_orders_by_symbol(context, sym)
            context.symbol_list.remove(sym)
            if sym in context.state: del context.state[sym]
            if sym in context.latest_data: del context.latest_data[sym]
        for sym in new_symbols - old_symbols:
            info(f'[{sym}] 新增标的，正在初始化状态...')
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
        for sym in old_symbols.intersection(new_symbols):
            if context.symbol_config[sym] != new_config[sym]:
                info(f'[{sym}] 参数发生变更，正在更新...')
                state, new_params = context.state[sym], new_config[sym]
                state.update({
                    'grid_unit': new_params['grid_unit'],
                    'dingtou_base': new_params['dingtou_base'],
                    'dingtou_rate': new_params['dingtou_rate'],
                    'max_position': state['base_position'] + new_params['grid_unit'] * 20
                })
        context.symbol_config = new_config
        info('✅ 配置文件热重载完成！当前监控标的: {}', context.symbol_list)
    except Exception as e:
        info(f'❌ 配置文件热重载失败: {e}')

def generate_html_report(context):
    """【新增】生成一个HTML格式的策略状态看板，并写入文件。"""
    all_metrics = []
    total_market_value, total_unrealized_pnl = 0, 0
    for symbol in context.symbol_list:
        if symbol not in context.state: continue
        state, pos, price = context.state[symbol], get_position(symbol), context.latest_data.get(symbol, 0)
        market_value = pos.amount * price
        unrealized_pnl = (price - pos.cost_basis) * pos.amount if pos.cost_basis > 0 else 0
        total_market_value += market_value
        total_unrealized_pnl += unrealized_pnl
        all_metrics.append({
            "symbol": symbol, "position": f"{pos.amount} ({pos.enable_amount})",
            "cost_basis": f"{pos.cost_basis:.3f}", "price": f"{price:.3f}",
            "market_value": f"{market_value:,.2f}", "unrealized_pnl": f"{unrealized_pnl:,.2f}",
            "pnl_ratio": f"{(unrealized_pnl / (pos.cost_basis * pos.amount) * 100) if pos.cost_basis * pos.amount != 0 else 0:.2f}%",
            "base_position": state['base_position'], "grid_spacing": f"{state['buy_grid_spacing']:.2%} / {state['sell_grid_spacing']:.2%}"
        })
    html_template = """<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><title>PTRADE策略实时看板</title><meta http-equiv="refresh" content="60">
    <style>body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;background-color:#f5f5f5;color:#333;margin:20px}}h1,h2{{color:#1a1a1a;border-bottom:2px solid #e0e0e0;padding-bottom:10px}}table{{width:100%;border-collapse:collapse;margin-top:20px;box-shadow:0 2px 5px rgba(0,0,0,0.1)}}th,td{{padding:12px 15px;text-align:left;border-bottom:1px solid #ddd}}th{{background-color:#e9eff2;font-weight:bold}}tr:nth-child(even){{background-color:#fafafa}}tr:hover{{background-color:#f1f1f1}}.positive{{color:#d9534f}}.negative{{color:#5cb85c}}.summary-card{{background-color:white;padding:20px;border-radius:8px;box-shadow:0 2px 5px rgba(0,0,0,0.1);margin-bottom:20px}}.summary-item{{display:inline-block;width:48%;text-align:center}}.summary-item h3{{margin:0;color:#555}}.summary-item p{{margin:5px 0 0 0;font-size:24px;font-weight:bold}}.footer{{text-align:center;margin-top:20px;color:#888;font-size:12px}}</style>
    </head><body><h1>PTRADE策略实时看板</h1><p class="footer">最后更新时间: {update_time}</p><div class="summary-card"><div class="summary-item"><h3>总市值 (CNY)</h3><p>{total_market_value}</p></div><div class="summary-item"><h3>总浮动盈亏 (CNY)</h3><p class="{pnl_class}">{total_unrealized_pnl}</p></div></div>
    <h2>各标的详细状态</h2><table><thead><tr><th>标的代码</th><th>持仓 (可卖)</th><th>成本价</th><th>市价</th><th>市值</th><th>浮动盈亏</th><th>盈亏比例</th><th>目标底仓</th><th>网格间距(买/卖)</th></tr></thead><tbody>{table_rows}</tbody></table>
    <p class="footer">本页面每60秒自动刷新一次。</p></body></html>
    """
    table_rows = ""
    for m in all_metrics:
        pnl_class = "positive" if float(m["unrealized_pnl"].replace(",", "")) >= 0 else "negative"
        table_rows += f"""<tr><td>{m['symbol']}</td><td>{m['position']}</td><td>{m['cost_basis']}</td><td>{m['price']}</td><td>{m['market_value']}</td><td class="{pnl_class}">{m['unrealized_pnl']}</td><td class="{pnl_class}">{m['pnl_ratio']}</td><td>{m['base_position']}</td><td>{m['grid_spacing']}</td></tr>"""
    final_html = html_template.format(
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total_market_value=f"{total_market_value:,.2f}", total_unrealized_pnl=f"{total_unrealized_pnl:,.2f}",
        pnl_class="positive" if total_unrealized_pnl >= 0 else "negative", table_rows=table_rows
    )
    try:
        report_path = research_path('reports', 'strategy_dashboard.html')
        report_path.write_text(final_html, encoding='utf-8')
    except Exception as e:
        info(f'❌ 生成HTML看板失败: {e}')