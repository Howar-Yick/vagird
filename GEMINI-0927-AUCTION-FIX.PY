# event_driven_grid_strategy.py
# 版本号：GEMINI-0927-AUCTION-FIX
# 0927: 新增9:25-9:30下单冻结期逻辑，解决集合竞价成交后的挂单冲突问题。
# 0926-R4: 严格基于您的原始文件(VCHATGPT-0708)进行修改，确保所有函数完整，并正确集成所有新功能。
# 0926-LOGIC-FIX: 修正动态网格中高低仓位区的判断逻辑。
# 0926: 增加T+1判断、交易冷静期、详细日志。
# 0925: 实现配置文件与动态ATR网格。

import json
import logging
import math
from datetime import datetime, time
from pathlib import Path
from types import SimpleNamespace

# 全局文件句柄 & 常量
LOG_FH = None
MAX_SAVED_FILLED_IDS = 500
__version__ = 'GEMINI-0927-AUCTION-FIX'

# --- 【新增】交易成本常量 ---
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
    return time(9, 15) <= now < time(9, 25) # 精确到9:25之前

# --- 【新增】9:25-9:30下单冻结期判断 ---
def is_order_blocking_period():
    """判断是否为交易所不接受新订单的集合竞价撮合阶段 (9:25-9:30)"""
    now = datetime.now().time()
    return time(9, 25) <= now < time(9, 30)

def before_trading_start(context, data):
    if context.initial_cleanup_done: return
    info('🔁 before_trading_start：清理遗留挂单')
    after_initialize_cleanup(context)
    current_time = context.current_dt.time()
    if time(9, 15) <= current_time < time(9, 30): # 这里保持宽松判断
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
    if is_order_blocking_period(): return # 如果在冻结期，不挂单
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
    for tr in trade_list:
        if str(tr.get('status')) != '8': continue
        sym, entrust_no = convert_symbol_to_standard(tr['stock_code']), tr['entrust_no']
        if sym not in context.state or entrust_no in context.state[sym]['filled_order_ids']: continue
        context.state[sym]['filled_order_ids'].add(entrust_no)
        safe_save_state(sym, context.state[sym])
        trade_direction = "买入" if tr['entrust_bs'] == '1' else "卖出"
        info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f}', sym, trade_direction, tr['business_amount'], tr['business_price'])
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
    """【逻辑修正】成交后处理函数，增加冻结期判断"""
    state = context.state[symbol]
    if order.filled == 0: return
    last_dt = state.get('_last_fill_dt')
    if state.get('last_fill_price') == order.price and last_dt and (context.current_dt - last_dt).seconds < 5:
        info('[{}] ⏭️ 忽略5s内同价重复成交', symbol)
        return
    state['_last_trade_ts'] = context.current_dt
    state['_last_fill_dt'] = context.current_dt
    state['last_fill_price'] = order.price
    state['base_price'] = order.price
    info('[{}] 🔄 成交后基准价更新为 {:.3f}', symbol, order.price)
    state['_pos_change'] = order.amount
    cancel_all_orders_by_symbol(context, symbol)
    
    # 【核心修正】在下单前，检查是否处于9:25-9:30的下单冻结期
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
    if now_dt.minute % 30 == 0 and now_dt.second < 5:
        info('📌 每30分钟状态巡检...')
        for sym in context.symbol_list:
            if sym in context.state:
                log_status(context, sym, context.state[sym], context.latest_data.get(sym))

def log_status(context, symbol, state, price):
    if not price: return
    pos = get_position(symbol)
    pnl = (price - pos.cost_basis) * pos.amount if pos.cost_basis > 0 else 0
    info("📊 [{}] 状态: 价:{:.3f} 持仓:{}(可卖:{}) / 底仓:{} 成本:{:.3f} 盈亏:{:.2f} 网格:[买{:.2%},卖{:.2%}]",
         symbol, price, pos.amount, pos.enable_amount, state['base_position'], pos.cost_basis, pnl, state['buy_grid_spacing'], state['sell_grid_spacing'])

def update_grid_spacing_hybrid(context, symbol, state, curr_pos):
    """混合模型：根据持仓决定基础档位，再根据ATR进行缩放"""
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
    try:
        hist = attribute_history(symbol, count=atr_period + 2, unit='1d', fields=['high', 'low', 'close'])
        if not (hist and len(hist['high']) >= atr_period + 2): return None
        tr_list = [max(h - l, abs(h - pc), abs(l - pc)) for h, l, pc in zip(hist['high'][1:], hist['low'][1:], hist['close'][:-1])]
        if not tr_list: return None
        atr_value = sum(tr_list[-atr_period:]) / atr_period
        current_price = context.latest_data.get(symbol, hist['close'][-1])
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
    for sym in context.symbol_list:
        if sym in context.state:
            safe_save_state(sym, context.state[sym])
    info('✅ 日终保存状态完成')

def get_target_base_position(context, symbol, state, price, dt):
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
    y, w, _ = dt.date().isocalendar()
    key = f"{y}_{w}"
    if key not in state.get('trade_week_set', set()):
        if 'trade_week_set' not in state: state['trade_week_set'] = set()
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
            
def after_trading_end(context, data):
    if '回测' in context.env: return
    info('⏰ 系统调用交易结束处理')
    update_daily_reports(context, data)
    info('✅ 交易结束处理完成')

def update_daily_reports(context, data):
    reports_dir = research_path('reports')
    current_date = context.current_dt.strftime("%Y-%m-%d")
    for symbol in context.symbol_list:
        if symbol not in context.state: continue
        report_file = reports_dir / f"{symbol}.csv"
        state = context.state[symbol]
        pos_obj = get_position(symbol)
        amount = pos_obj.amount
        cost_basis = pos_obj.cost_basis if pos_obj.cost_basis > 0 else state['base_price']
        close_price = context.latest_data.get(symbol, state['base_price'])
        weeks = len(state.get('trade_week_set', []))
        d_base = state['dingtou_base']
        d_rate = state['dingtou_rate']
        cumulative_invest = sum(d_base * (1 + d_rate) ** w for w in range(1, weeks+1))
        expected_value = state['initial_position_value'] + d_base * weeks
        current_val = amount * close_price
        total_return = (current_val - cumulative_invest) / cumulative_invest if cumulative_invest > 0 else 0.0
        profit_all = (close_price - cost_basis) * amount if cost_basis > 0 else 0
        row = [
            current_date, f"{close_price:.3f}", str(weeks), str(weeks),
            f"{(current_val - (state.get('last_week_position', 0) * close_price)) / (state.get('last_week_position', 0) * close_price) if state.get('last_week_position', 0) > 0 and close_price > 0 else 0.0:.2%}",
            f"{total_return:.2%}", f"{expected_value:.2f}",
            f"{d_base:.0f}", f"{d_base * (1 + d_rate) ** weeks:.0f}", f"{cumulative_invest:.0f}",
            str(state['initial_base_position']), str(state['base_position']),
            f"{state['base_position'] * close_price:.0f}",
            f"{(state['base_position'] - state.get('last_week_position', 0)) * close_price:.0f}",
            f"{state['base_position'] * close_price - state['initial_position_value']:.0f}",
            str(state['base_position']), str(amount), str(state['grid_unit']),
            str(max(0, amount - state['base_position'])),
            str(state['base_position'] + state['grid_unit'] * 5),
            str(state['base_position'] + state['grid_unit'] * 15),
            str(state['max_position']), f"{cost_basis:.3f}",
            f"{(state['base_position'] - state.get('last_week_position', 0)) * close_price:.3f}",
            f"{profit_all:.0f}"
        ]
        is_new = not report_file.exists()
        with open(report_file, 'a', encoding='utf-8', newline='') as f:
            if is_new:
                headers = ["时间","市价","期数","次数","每期总收益率","盈亏比","应到价值","当周应投入金额","当周实际投入金额","实际累计投入金额","定投底仓份额","累计底仓份额","累计底仓价值","每期累计底仓盈利","总累计底仓盈利","底仓","股票余额","单次网格交易数量","可T数量","标准数量","中间数量","极限数量","成本价","对比定投成本","盈亏"]
                f.write(",".join(headers) + "\n")
            f.write(",".join(map(str, row)) + "\n")
        info(f'✅ [{symbol}] 已更新每日报表：{report_file}')
        
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
                state = context.state[sym]; new_params = new_config[sym]
                state['grid_unit'] = new_params['grid_unit']
                state['dingtou_base'] = new_params['dingtou_base']
                state['dingtou_rate'] = new_params['dingtou_rate']
                state['max_position'] = state['base_position'] + new_params['grid_unit'] * 20
        context.symbol_config = new_config
        info('✅ 配置文件热重载完成！当前监控标的: {}', context.symbol_list)
    except Exception as e:
        info(f'❌ 配置文件热重载失败: {e}')