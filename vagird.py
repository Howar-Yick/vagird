# event_driven_grid_strategy.py
#版本号：VCHATGPT-0801
#0801-修正成交后不及时更新仓位的问题。

from datetime import datetime, time
from types import SimpleNamespace
import math, json
from pathlib import Path
import logging

# 全局文件句柄 & 常量
LOG_FH = None
MAX_SAVED_FILLED_IDS = 500
__version__ = 'v2025-06-16-fix-init-order-callback-v4'

# --- 路径工具 ---
def research_path(*parts) -> Path:
    """研究目录根 + 子路径，确保文件夹存在"""
    p = Path(get_research_path()).joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

# 判断运行环境：回测、实盘、模拟盘
def check_environment():
    try:
        u = str(get_user_name())
        if u == '55418810': return '回测'
        if u == '8887591588': return '实盘'
        return '模拟'
    except:
        return '未知'

def initialize(context):
    """策略初始化：打开日志、恢复状态、注册定时与事件回调"""
    global LOG_FH
    # 打开日志文件，追加模式
    log_file = research_path('logs', 'event_driven_strategy.log')
    LOG_FH = open(log_file, 'a', encoding='utf-8')
    log.info(f'🔍 日志同时写入到 {log_file}')

    # 环境检测（回测/模拟/实盘）
    context.env = check_environment()
    info("当前环境：{}", context.env)

    # 废弃轮询周期，仅兼容历史参数
    context.run_cycle = get_saved_param('run_cycle_seconds', 60)

    # 标的配置（与原脚本一致）
    context.symbol_config = {
        '513850.SS': {'grid_unit':600,'initial_base_position':0,'base_price':1.382,'dingtou_base':850,'dingtou_rate':0.0058},
        '159509.SZ': {'grid_unit':500,'initial_base_position':0,'base_price':1.559,'dingtou_base':850,'dingtou_rate':0.0058},
        '161129.SZ': {'grid_unit':700,'initial_base_position':0,'base_price':1.242,'dingtou_base':900,'dingtou_rate':0.0058},
        '518850.SS': {  # 与图片中的"黄金ETF华夏"匹配
            'grid_unit': 100,            # 图片中的委托量
            'initial_base_position': 0,   # 初始底仓
            'base_price': 7.419,          # 基准价
            'dingtou_base': 850,          # 定投基础值
            'dingtou_rate': 0.0058        # 定投增长率  # 此处修正拼写错误
        },
        '159934.SZ': { #黄金ETF
            'grid_unit': 100,
            'initial_base_position': 500,
            'base_price': 7.113,
            'dingtou_base': 850,
            'dingtou_rate': 0.0058
        },
        '162415.SZ': {  #美国消费LOF
            'grid_unit': 300,
            'initial_base_position': 4900,
            'base_price': 2.797,
            'dingtou_base': 850,
            'dingtou_rate': 0.0058
        }, 
        '159612.SZ': {  # 国泰标普500
            'grid_unit': 400,
            'initial_base_position': 1000,  # 目标持仓清晰定义为1000
            'base_price': 1.819,
            'dingtou_base': 850,
            'dingtou_rate': 0.0058,
        },
        '161130.SZ': { #纳斯达克100LOF
            'grid_unit': 200,
            'initial_base_position': 500,
            'base_price': 3.436,
            'dingtou_base': 850,
            'dingtou_rate': 0.0058
        },
        '501312.SS': { #海外科技LOF
            'grid_unit': 500,
            'initial_base_position': 0,
            'base_price': 1.572,
            'dingtou_base': 850,
            'dingtou_rate': 0.0058
        },
        '513400.SS': { #道琼斯ETF
            'grid_unit': 700,
            'initial_base_position': 0,
            'base_price': 1.096,
            'dingtou_base': 850,
            'dingtou_rate': 0.0058
        },
        '161125.SZ': { #标普500LOF
            'grid_unit': 300,
            'initial_base_position': 0,
            'base_price': 2.570,
            'dingtou_base': 850,
            'dingtou_rate': 0.0058
        },
        '161128.SZ': { #标普科技
            'grid_unit': 200,
            'initial_base_position': 0,
            'base_price': 4.772,
            'dingtou_base': 1000,
            'dingtou_rate': 0.0058
        }, 
        '513300.SS': { #纳斯达克ETF
            'grid_unit': 400,
            'initial_base_position': 0,
            'base_price': 1.933,
            'dingtou_base': 850,
            'dingtou_rate': 0.0058
        },         
        '513230.SS': { #港股消费ETF
            'grid_unit':800,
            'initial_base_position': 0,
            'base_price': 1.073,
            'dingtou_base': 900,
            'dingtou_rate': 0.0058
        },    
        '161116.SZ': { #黄金主题LOF
            'grid_unit':600,
            'initial_base_position': 0,
            'base_price': 1.347,
            'dingtou_base': 850,
            'dingtou_rate': 0.0058
        },        
    }

    # 提取所有标的列表
    context.symbol_list = list(context.symbol_config)

    # 初始化状态字典与挂单控制开关
    context.state = {}
    context.should_place_order_map = {}

    # 恢复或初始化状态
    for sym, cfg in context.symbol_config.items():
        state_file = research_path('state', f'{sym}.json')
        if state_file.exists():
            saved = json.loads(state_file.read_text(encoding='utf-8'))
        else:
            saved = get_saved_param(f'state_{sym}', {}) or {}

        # 基准价和网格单位可恢复
        initial_price = saved.get('base_price', cfg['base_price'])

        st = {
            **cfg,
            'base_price': initial_price,
            'grid_unit': saved.get('grid_unit', cfg['grid_unit']),
            'filled_order_ids': set(saved.get('filled_order_ids', [])),
            'trade_week_set': set(saved.get('trade_week_set', [])),
            'last_week_position': saved.get('last_week_position', cfg['initial_base_position']),
            'base_position': saved.get('base_position', cfg['initial_base_position']),
            'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
            'buy_grid_spacing': 0.005,
            'sell_grid_spacing': 0.005,
        }
        # 最大仓位 = 底仓 + 20格
        st['max_position'] = saved.get('max_position', st['base_position'] + st['grid_unit']*20)
        context.state[sym] = st
        context.should_place_order_map[sym] = True

    # 初始化最新行情缓存（符号 → float）
    context.latest_data = {
        sym: cfg['base_price']
        for sym, cfg in context.symbol_config.items()
    }

    # 标记是否已完成首次挂单清理（用于实盘重启后撤单）
    context.initial_cleanup_done = False
    context.last_trade_day = None


    # 非回测环境：注册集合竞价、日清理和回调
    if '回测' not in context.env:
        run_daily(context, place_auction_orders, time='9:15')    # 集合竞价挂单
        run_daily(context, end_of_day,          time='14:55')    # 日终清理
        info('✅ 事件驱动模式就绪：on_order_response / on_trade_response')

    info('✅ 初始化完成，版本:{}', __version__)

def is_main_trading_time():
    """主交易时段：9:30–11:30 和 13:00–15:00"""
    now = datetime.now().time()
    return (time(9, 30) <= now <= time(11, 30)) or (time(13, 0) <= now <= time(15, 0))

def is_auction_time():
    """集合竞价时段：9:15–9:30"""
    now = datetime.now().time()
    return time(9, 15) <= now < time(9, 30)

def is_trading_time():
    """主交易时段：9:30–11:30 和 13:00–15:00"""
    now = datetime.now().time()
    return (time(9, 30) <= now <= time(11, 30)) or (time(13, 0) <= now <= time(15, 0))

def before_trading_start(context, data):
    if context.initial_cleanup_done:
        return

    info('🔁 before_trading_start：开始清理遗留挂单')
    after_initialize_cleanup(context)

    current_time = context.current_dt.time()

    # 仅在盘前 9:15–9:30 之间，才补挂集合竞价网格
    if time(9,15) <= current_time < time(9,30):
        info('⏭ 重启在集合竞价时段(9:15-9:30)，补挂集合竞价网格')
        place_auction_orders(context)
    else:
        info('⏸️ 重启时间{}不在集合竞价时段(9:15-9:30)，跳过补挂网格',
             current_time.strftime('%H:%M:%S'))

    context.initial_cleanup_done = True


# --- 启动 & 清理 ---
def after_initialize_cleanup(context):
    """
    重启后或日终调用：按标的逐个清理所有遗留未成交挂单。
    跳过已在 filled_order_ids 里的（已经成交过）的委托。
    """
    if '回测' in context.env:
        return
    if not hasattr(context, 'symbol_list'):
        return

    info('🧼 重启/日终清理遗留挂单（按品种）')
    for sym in context.symbol_list:
        cancel_all_orders_by_symbol(context, sym)
    info('✅ 按品种清理完成')


# --- 集合竞价 & 限价挂单 ---
def place_auction_orders(context):
    """
    只在集合竞价(9:15–9:30)或主交易时段内，才执行一次撤单＋网格限价挂单。
    其他时间一律跳过。
    """
    if '回测' in context.env:
        return

    if not (is_auction_time() or is_main_trading_time()):
        info('⏸️ 非集合竞价/主交易时段，跳过集合竞价挂单')
        return

    # ── 新增：清空“防抖”和“节流”缓存，保证今日首次集合竞价能下单 ──
    for st in context.state.values():
        st.pop('_last_order_bp', None)
        st.pop('_last_order_ts',  None)
    # （可选）打个日志确认一下
    info('🆕 防抖/节流缓存已清空，开始集合竞价挂单')

    for sym in context.symbol_list:
        state = context.state[sym]
        adjust_grid_unit(state)
        # 先撤掉所有残留挂单（包括对手方向）
        cancel_all_orders_by_symbol(context, sym)

        # 按照“基准价”重新下双向网格
        base = state['base_price']
        context.latest_data[sym] = base
        place_limit_orders(context, sym, state)

        safe_save_state(sym, state)
        # 本次集合竞价挂单后，关掉本周期防抖/节流
        context.should_place_order_map[sym] = False
        safe_save_state(sym, state)



# 限价挂单主函数：根据持仓和网格判断是否买入/卖出
def place_limit_orders(context, symbol, state):
    """
    限价挂单主函数：带节流与防抖，
    新增：对“只买不卖/只卖不买触及格价”的情形不做节流防抖，立即触发。
    新增：当即时价偏离基准价 >10% 时拦截。
    """
    from datetime import time

    now_dt   = context.current_dt
    now_time = now_dt.time()

    # 1) 时段限定
    if not (is_auction_time() or (is_main_trading_time() and now_time < time(14, 50))):
        info('[{}] ⏸️ 非挂单时段，跳过限价网格', symbol)
        return

    # 2) 价格有效性检查
    price = context.latest_data.get(symbol)
    if price is None or price <= 0 or math.isnan(price):
        info('[{}] ⚠️ 当前价格无效，跳过网格挂单: {}', symbol, price)
        return

    # 新增：如果价格和基准偏离超过10%，拦截（可能停牌）
    base = state['base_price']
    if price > base * 1.10 or price < base * 0.90:
        info('[{}] ⚠️ 价格偏离基准超10%，跳过挂单：当前{} 基准{}', symbol, price, base)
        return

    unit   = state['grid_unit']
    buy_p  = round(base * (1 - state['buy_grid_spacing']), 3)
    sell_p = round(base * (1 + state['sell_grid_spacing']), 3)

    # 3) 获取最新持仓（含成交偏移）
    pos = get_position(symbol).amount
    pos += state.pop('_pos_change', 0)

    # 4) 只买不卖 / 只卖不买 判断
    is_buy_only  = (pos - unit) <= state['base_position']
    is_sell_only = (pos + unit) >= state['max_position']

    # ——【关键改动：触发路径绕过节流/防抖】——
    trigger_sell_only = is_buy_only  and price >= sell_p
    trigger_buy_only  = is_sell_only and price <= buy_p

    # 5) 普通节流/防抖，只有在非触发路径才应用
    if not (trigger_sell_only or trigger_buy_only):
        last_ts = state.get('_last_order_ts')
        if last_ts and (now_dt - last_ts).seconds < 30:
            return
        last_bp  = state.get('_last_order_bp')
        half_pct = state['buy_grid_spacing'] / 2
        if last_bp and abs(base - last_bp) / last_bp < half_pct:
            return
        state['_last_order_ts'] = now_dt
        state['_last_order_bp'] = base

    # 6) 触发路径：更新基准、撤单
    if trigger_sell_only:
        state['base_price'] = sell_p
        info('[{}] 🔄 只买不卖触及卖格价 → 基准价 {:.3f}', symbol, sell_p)
        cancel_all_orders_by_symbol(context, symbol)
        base, buy_p, sell_p = sell_p, \
            round(sell_p*(1-state['buy_grid_spacing']),3), \
            round(sell_p*(1+state['sell_grid_spacing']),3)
    elif trigger_buy_only:
        state['base_price'] = buy_p
        info('[{}] 🔄 只卖不买触及买格价 → 基准价 {:.3f}', symbol, buy_p)
        cancel_all_orders_by_symbol(context, symbol)
        base, buy_p, sell_p = buy_p, \
            round(buy_p*(1-state['buy_grid_spacing']),3), \
            round(buy_p*(1+state['sell_grid_spacing']),3)

    # 7) 重挂双向网格
    try:
        open_orders = [o for o in get_open_orders(symbol) or [] if getattr(o,'status',None)=='2']
        exists_buy  = any(o.amount>0 and abs(o.price-buy_p)<1e-3 for o in open_orders)
        exists_sell = any(o.amount<0 and abs(o.price-sell_p)<1e-3 for o in open_orders)

        if not exists_buy and pos + unit <= state['max_position']:
            oid = order(symbol, unit, limit_price=buy_p)
            info('[{}] 限价买 单ID:{} {}@{}', symbol, oid, unit, buy_p)
        if not exists_sell and pos - unit >= state['base_position']:
            oid = order(symbol, -unit, limit_price=sell_p)
            info('[{}] 限价卖 单ID:{} {}@{}', symbol, oid, unit, sell_p)
    except Exception as e:
        msg = str(e)
        if '120147' in msg:
            info('[{}] ⏸️ 非允许时段，拒绝限价单', symbol)
        else:
            info('[{}] ⚠️ 限价挂单异常：{}', symbol, e)
    finally:
        safe_save_state(symbol, state)




# 日内 14:50 后进入市价撮合模式，判断是否触及网格成交
def place_market_orders_if_triggered(context, symbol, state):
    """
    下午14:55后市价撮合，同样只在主交易时段有效，
    新增：当即时价偏离基准价 >10% 时拦截。
    """
    if not is_main_trading_time():
        info('[{}] ⏸️ 非主交易时段，跳过市价触发', symbol)
        return

    price = context.latest_data.get(symbol)
    if price is None or price <= 0 or math.isnan(price):
        info('[{}] ⚠️ 当前价格无效，跳过市价触发: {}', symbol, price)
        return

    # 新增：偏离基准超10%拦截
    base = state['base_price']
    if price > base * 1.10 or price < base * 0.90:
        info('[{}] ⚠️ 市价触发时价格偏离基准超10%，跳过: 当前{} 基准{}', symbol, price, base)
        return

    adjust_grid_unit(state)
    pos  = get_position(symbol).amount
    unit = state['grid_unit']
    bp   = base
    buy_p  = round(bp * (1 - state['buy_grid_spacing']), 3)
    sell_p = round(bp * (1 + state['sell_grid_spacing']), 3)

    if not context.should_place_order_map.get(symbol, True):
        return

    try:
        if price <= buy_p and pos + unit <= state['max_position']:
            info('[{}] 市价买触发 {}@{}', symbol, unit, price)
            order_market(symbol, unit, market_type='0')
            state['base_price'] = buy_p
        elif price >= sell_p and pos - unit >= state['base_position']:
            info('[{}] 市价卖触发 {}@{}', symbol, unit, price)
            order_market(symbol, -unit, market_type='0')
            state['base_price'] = sell_p
    except Exception as e:
        msg = str(e)
        if '120147' in msg:
            info('[{}] ⏸️ 非允许时段拒单，跳过市价挂单', symbol)
        else:
            info('[{}] ⚠️ 市价挂单异常：{}', symbol, e)
    finally:
        context.should_place_order_map[symbol] = False
        safe_save_state(symbol, state)


def get_order_status(entrust_no):
    """获取订单实时状态 (新增函数)"""
    try:
        # 关键修改：直接传递委托号，不使用关键字参数
        order_detail = get_order(entrust_no)
        if order_detail:
            return str(order_detail.get('status', ''))
    except Exception as e:
        info('⚠️ 查询订单状态失败 entrust_no={}: {}', entrust_no, e)
    return ''

# 成交后处理函数（必须由成交回调触发）
def on_order_filled(context, symbol, order):
    """
    成交回调：更新基准价、撤单、再挂网格，并重置开关
    新增：同价重复成交回调只处理一次。
    """
    state = context.state[symbol]
    if order.filled == 0:
        return

    # ── 新增：同价重复成交回调只处理一次──
    last_dt    = state.get('_last_fill_dt')
    last_price = state.get('last_fill_price')
    # 如果和上次成交价相同且在5秒内，则跳过
    if last_price == order.price and last_dt and (context.current_dt - last_dt).seconds < 5:
        info('[{}] ⏭️ 重复成交回调，跳过', symbol)
        return
    # 记录这次成交时间
    state['_last_fill_dt'] = context.current_dt

    # 清空当日撤单缓存，避免沉积
    if hasattr(context, 'canceled_cache'):
        context.canceled_cache = {'date': None, 'orders': set()}

    # ── 更新基准价 ──
    state['last_fill_price'] = order.price
    state['base_price']      = order.price
    info('[{}] 🔄 成交后基准价更新为 {:.3f}', symbol, order.price)

    # ── 新增：缓存这次成交量，用于下一次挂单时更新持仓──
    state['_pos_change'] = order.amount

    # 撤掉所有残单
    cancel_all_orders_by_symbol(context, symbol)

    # 符合时段则立即重挂
    if context.current_dt.time() < time(14, 50):
        place_limit_orders(context, symbol, state)

    # 持久化 & 重置挂单开关
    safe_save_state(symbol, state)
    context.should_place_order_map[symbol] = True


# 委托状态更新回调（可选）
def on_order_response(context, order_list):
    # 委托状态更新，仅作日志或监控用，不再触发新的网格挂单
    for order in order_list:
        sym = convert_symbol_to_standard(order['stock_code'])
        info('[{}] on_order_response status={} entrust_no={}', sym, order['status'], order['entrust_no'])


# 成交回报回调（主要使用该函数）
def on_trade_response(context, trade_list):
    """
    成交回报回调：只处理完全成交(status=='8')，
    且保证同一 entrust_no 只处理一次。
    """
    for tr in trade_list:
        # 只处理完全成交
        if str(tr.get('status')) != '8':
            info('[{}] ⏩ 忽略非完全成交：status={}',
                 convert_symbol_to_standard(tr['stock_code']),
                 tr.get('status'))
            continue

        sym = convert_symbol_to_standard(tr['stock_code'])
        entrust_no = tr['entrust_no']

        # 如果已经标记过了，就跳过
        if entrust_no in context.state[sym]['filled_order_ids']:
            info('[{}] ⏩ 已处理过的成交：entrust_no={}', sym, entrust_no)
            continue

        # ——【改动】先标记再处理，防止重复触发——
        context.state[sym]['filled_order_ids'].add(entrust_no)
        safe_save_state(sym, context.state[sym])

        # 构造一个简单的 order 对象给 on_order_filled
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


# 撤销某标的所有挂单（跳过无委托号的订单）
def cancel_all_orders_by_symbol(context, symbol):
    """
    撤销某标的所有遗留挂单（只对状态=='2' 的单），
    跳过已在 filled_order_ids 里的（已经成交或已处理过的），
    跳过已撤(4)、部撤(5)、撤单中(6)或已成交(8)的。
    对 251020 错误码不再当成异常抛出。
    """
    all_orders = get_all_orders() or []
    total = 0

    # 每日缓存，避免重复撤单
    if not hasattr(context, 'canceled_cache'):
        context.canceled_cache = {'date': None, 'orders': set()}
    today = context.current_dt.date()
    if context.canceled_cache['date'] != today:
        context.canceled_cache = {'date': today, 'orders': set()}
    cache = context.canceled_cache['orders']

    for o in all_orders:
        # 提取 symbol/status/entrust_no
        if isinstance(o, dict):
            api_sym    = o.get('symbol') or o.get('stock_code')
            status     = str(o.get('status', ''))
            entrust_no = o.get('entrust_no')
        else:
            api_sym    = getattr(o, 'symbol', None) or getattr(o, 'stock_code', None)
            status     = str(getattr(o, 'status', ''))
            entrust_no = getattr(o, 'entrust_no', None)

        if status != '2' or not api_sym or not entrust_no:
            continue

        sym2 = convert_symbol_to_standard(api_sym)
        if sym2 != symbol or entrust_no in context.state[symbol]['filled_order_ids']:
            continue

        # 最终状态检查，跳过已撤/部撤/撤单中/已成交
        final_status = get_order_status(entrust_no)
        if final_status in ('4', '5', '6', '8'):
            info('[{}] ⏭️ 跳过无法撤单的状态 entrust_no:{} status={}', symbol, entrust_no, final_status)
            continue

        # 跳过今天已经撤过的
        if entrust_no in cache:
            info('[{}] ⏭️ 今日已处理过撤单 entrust_no:{}，跳过', symbol, entrust_no)
            continue

        cache.add(entrust_no)
        total += 1
        info('[{}] 👉 撤销遗留挂单 entrust_no={} api_symbol={}', symbol, entrust_no, api_sym)

        # 真正发起撤单
        try:
            resp = cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})
            info('[{}] cancel_order_ex 返回 → {}', symbol, resp)
            err = resp.get('error_info') if resp else None
            if not err:
                info('[{}] ✅ 撤单成功 entrust_no:{}', symbol, entrust_no)
            elif '251020' in err:
                # 已经是不可撤状态，吞掉
                info('[{}] ⏭️ 撤单跳过（不可撤）entrust_no:{} info:{}', symbol, entrust_no, err)
            else:
                info('[{}] ⚠️ 撤单失败 entrust_no:{} info:{}', symbol, entrust_no, err)
        except Exception as e:
            # 有时 API 会直接抛出异常，此处也专门识别 251020
            msg = str(e)
            if '251020' in msg:
                info('[{}] ⏭️ 撤单异常跳过（不可撤）entrust_no:{} err:{}', symbol, entrust_no, msg)
            else:
                info('[{}] ⚠️ 撤单异常 entrust_no:{} err:{}', symbol, entrust_no, msg)

    info('[{}] 共{}笔遗留挂单尝试撤销完毕', symbol, total)




# 撤销一组标的所有挂单
def cancel_all_residual_orders(context, symbol_list):
    """
    扫描所有 get_all_orders() 返回的订单，
    并撤销 symbol_list 中所有 status=='2' 且不在 filled_order_ids 的挂单。
    """
    all_orders = get_all_orders() or []
    total = cancelled = skipped = 0

    for o in all_orders:
        # 安全地取属性
        api_sym    = getattr(o, 'symbol', '')
        sym        = convert_symbol_to_standard(api_sym)
        entrust_no = getattr(o, 'entrust_no', None)
        status     = str(getattr(o, 'status', ''))

        # 只对我们的标的、未成交状态、且不在已成交缓存里的单做撤销
        if (sym in symbol_list
            and status == '2'
            and entrust_no
            and entrust_no not in context.state[sym]['filled_order_ids']
        ):
            total += 1
            info('[{}] 撤残留挂单 entrust_no:{}', sym, entrust_no)

            resp = cancel_order_ex({
                'entrust_no': entrust_no,
                'symbol': api_sym
            })
            info('[{}] cancel_order_ex 返回 → {}', sym, resp)

            err = resp.get('error_info') if resp else None
            if not err:
                cancelled += 1
            elif '251020' in err:
                skipped += 1
            else:
                skipped += 1

    info('✅ 清理残留挂单 共:{} 成功:{} 跳过:{}',
         total, cancelled, skipped)


# 每日 14:55 调用：清理挂单 + 保存状态 + 重置挂单开关
def end_of_day(context):
    after_initialize_cleanup(context)
    for sym in context.symbol_list:
        safe_save_state(sym, context.state[sym])
        context.should_place_order_map[sym] = True
    info('✅ 日终保存状态完成')

# --- 通用 & 辅助函数 ---
def save_state(symbol, state):
    """持久化 state 到参数和磁盘"""
    ids = list(state['filled_order_ids'])
    if len(ids) > MAX_SAVED_FILLED_IDS:
        ids = ids[-MAX_SAVED_FILLED_IDS:]
        state['filled_order_ids'] = set(ids)
    store = {
        'base_price': state['base_price'],
        'grid_unit': state['grid_unit'],
        'max_position': state['max_position'],
        'filled_order_ids': ids,
        'trade_week_set': list(state['trade_week_set']),
        'last_week_position': state['last_week_position'],
        'base_position': state['base_position'],
    }
    set_saved_param(f'state_{symbol}', store)
    path = research_path('state', f'{symbol}.json')
    path.write_text(json.dumps(store), encoding='utf-8')


def info(msg, *args):
    """统一日志：平台 + 文件"""
    text = msg.format(*args)
    log.info(text)
    if LOG_FH:
        LOG_FH.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} INFO {text}\n")
        LOG_FH.flush()


def safe_save_state(symbol, state):
    """捕获异常的保存"""
    try:
        save_state(symbol, state)
    except Exception:
        info('[{}] ⚠️ 状态保存失败', symbol)


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


def convert_symbol_to_standard(full_symbol):
    """API 合约符号转 .SZ/.SS 形式"""
    if full_symbol.endswith('.XSHE'): return full_symbol.replace('.XSHE','.SZ')
    if full_symbol.endswith('.XSHG'): return full_symbol.replace('.XSHG','.SS')
    return full_symbol


# --- 网格辅助函数 ---
def get_trade_weeks(context, symbol, state, dt):
    """计算当前是第几交易周，并首次进入新周时保存"""
    today = dt.date()
    y, w = today.isocalendar()[:2]
    key = f"{y}_{w}"
    if key not in state['trade_week_set']:
        state['trade_week_set'].add(key)
        # 新周开始时，记录上周末的基准仓位
        state['last_week_position'] = state['base_position']
        # 立刻保存，以便重启后恢复
        safe_save_state(symbol, state)
    return len(state['trade_week_set'])

def get_target_base_position(context, symbol, state, price, dt):
    """计算定投目标仓位并更新底仓/max仓位"""
    weeks = get_trade_weeks(context, symbol, state, dt)
    target = state['initial_position_value'] + state['dingtou_base'] * weeks * ((1 + state['dingtou_rate'])**weeks)
    last_val = state['last_week_position'] * price
    delta = target - last_val

    # ← 用 math.ceil 保证任何微小不足都向上补
    delta_pos = math.ceil(delta / price / 100) * 100
    min_base = round((state['initial_position_value']/state['base_price'])/100)*100
    new_pos = max(min_base, state['last_week_position'] + delta_pos)
    new_pos = round(new_pos/100)*100
    state['base_position'] = new_pos
    state['max_position']   = new_pos + state['grid_unit']*20
    return new_pos


def update_grid_spacing(symbol, state, curr_pos):
    """
    根据 当前仓位 动态调整买卖网格间距：
    — 当仓位靠近底仓时：买0.5%，卖1%
    — 中间区间：买卖都0.5%
    — 超过15格后：买1%，卖0.5%
    仅在 spacing 有变化时才打印日志。
    """
    # 记录旧的 spacing
    old_buy  = state.get('buy_grid_spacing')
    old_sell = state.get('sell_grid_spacing')

    unit      = state['grid_unit']
    base_pos  = state['base_position']

    # 计算新的 spacing
    if curr_pos <= base_pos + unit * 5:
        new_buy, new_sell = 0.005, 0.01
    elif curr_pos <= base_pos + unit * 15:
        new_buy, new_sell = 0.005, 0.005
    else:
        new_buy, new_sell = 0.01, 0.005

    # 只有在真正变化时才更新并打印日志
    if new_buy != old_buy or new_sell != old_sell:
        state['buy_grid_spacing']  = new_buy
        state['sell_grid_spacing'] = new_sell
        info(
            '[{}] GridSpacing 变更 → 买{:.2%} 卖{:.2%}',
            symbol, new_buy, new_sell
        )
    # 否则，不做任何日志输出

        
def adjust_grid_unit(state):
    """
    当底仓 >= 原定义 20 格时，适量放大网格单位（放大20%，向上凑整到百股），
    并更新 max_position。
    """
    orig = state['grid_unit']
    base_pos = state['base_position']

    # 修改：当底仓大于等于 orig*20 时触发（原来是严格大于）
    if base_pos >= orig * 20:
        # 放大 20%，向上凑整到整百
        new_u = math.ceil(orig * 1.2 / 100) * 100
        if new_u != orig:
            state['grid_unit'] = new_u
            state['max_position'] = base_pos + new_u * 20
            info('🔧 网格单位 {}→{}，新 max_position→{}', orig, new_u, state['max_position'])


def log_status(context, symbol, state, price, dt):
    """输出当前网格状态日志"""
    weeks = get_trade_weeks(context, symbol, state, dt)
    this_val = state['dingtou_base']*weeks*((1+state['dingtou_rate'])**weeks)
    total    = sum(state['dingtou_base']*w*((1+state['dingtou_rate'])**w) for w in range(1, weeks+1))
    pos = get_position(symbol)
    pnl = (price - pos.cost_basis)*pos.amount
    info(
        "📊 [{}] 价:{:.3f} 周:{} 本期:{:.2f} 累计:{:.2f} 目标:{} 持仓:{} 成本:{:.3f} 盈亏:{:.2f}",
        symbol, price, weeks, this_val, total,
        state['base_position'], pos.amount, pos.cost_basis, pnl
    )

def get_target_position(symbol):
    """
    返回某个标的的目标底仓仓位（基于价值平均定投路径计算）
    该值需提前在 context.state[symbol]["target_position"] 中被正确设置
    """
    try:
        return context.state[symbol].get("target_position", 0)
    except Exception as e:
        log.error(f"[{symbol}] 读取目标持仓异常: {e}")
        return 0


def handle_data(context, data):
    """
    每分钟执行一次：
    1) 更新最新行情
    2) 按周计算并更新底仓 & 最大仓位
    3) 根据底仓 & 当前持仓 调整网格单位与网格间距
    4) 实时限价挂单（集合竞价 & 主交易时段<14:50，每分钟都尝试）
    5) 每30分钟一次日志保存
    6) 下午14:55后市价撮合触发
    """
    import math

    now_dt = context.current_dt

    # 1) 更新最新行情缓存
    context.latest_data = {
        sym: data[sym].price
        for sym in context.symbol_list
    }

    # 2) 对每个标的：先更新本周目标底仓，然后放大网格单位，再更新网格间距
    for sym in context.symbol_list:
        st    = context.state[sym]
        price = context.latest_data[sym]

        # 跳过无效价格
        if price is None or math.isnan(price) or price <= 0:
            info('[{}] ⚠️ 跳过无效价格：{}', sym, price)
            continue

        # 2.1) 更新本周目标底仓 & 最大仓位
        get_target_base_position(context, sym, st, price, now_dt)

        # 2.1.x) 放大网格单位（原定义20格触发）
        adjust_grid_unit(st)

        # 2.2) 根据最新底仓 & 当前持仓 更新网格间距
        curr_pos = get_position(sym).amount
        update_grid_spacing(sym, st, curr_pos)

    # 3) 实时限价挂单：集合竞价或主交易时段(<14:50)每分钟都尝试下单/更新基准价
    now = now_dt.time()
    if is_auction_time() or (is_main_trading_time() and now < time(14, 50)):
        for sym in context.symbol_list:
            place_limit_orders(context, sym, context.state[sym])

    # 4) 每30分钟一次日志保存（同时也重新调整网格单位与间距）
    if now_dt.minute % 30 == 0 and now_dt.second < context.run_cycle:
        for sym in context.symbol_list:
            st    = context.state[sym]
            price = context.latest_data.get(sym, st['base_price'])
            pos   = get_position(sym).amount

            adjust_grid_unit(st)
            update_grid_spacing(sym, st, pos)
            info('[{}] 即时价:{:.3f}  基准价:{:.3f}', sym, price, st['base_price'])
            log_status(context, sym, st, price, now_dt)
            info('📌 [{}] 状态已保存', sym)

    # 5) 下午14:55后市价撮合触发
    if now >= time(14, 55):
        for sym in context.symbol_list:
            place_market_orders_if_triggered(context, sym, context.state[sym])



# -------- 新增：交易日结束回调（PTRADE 系统自动调用）--------
def after_trading_end(context, data):
    """PTRADE 系统在交易结束后自动调用，用于更新每日报表"""
    # 仅在非回测环境运行
    if '回测' in context.env:
        return

    info('⏰ 系统调用交易结束处理')
    update_daily_reports(context, data)
    info('✅ 交易结束处理完成')


# -------- 新增：每日报表更新模块（完全匹配图片格式 + 新公式）--------
def update_daily_reports(context, data):
    """为每个标的维护一个 CSV 文件，每日收盘后追加一行"""
    reports_dir = research_path('reports')
    reports_dir.mkdir(parents=True, exist_ok=True)
    current_date = context.current_dt.strftime("%Y-%m-%d")

    for symbol in context.symbol_list:
        report_file = reports_dir / f"{symbol}.csv"
        state       = context.state[symbol]
        pos_obj     = get_position(symbol)
        amount      = getattr(pos_obj, 'amount', 0)
        cost_basis  = getattr(pos_obj, 'cost_basis', state['base_price'])

        # 收盘价：最新行情缓存中的数值
        close_price = context.latest_data.get(symbol, state['base_price'])
        try:
            close_price = getattr(close_price, 'price', close_price)
        except:
            close_price = state['base_price']

        # 周数、累计投入
        weeks       = len(state['trade_week_set'])
        count       = weeks
        d_base      = state['dingtou_base']
        d_rate      = state['dingtou_rate']
        # 当期应投 & 实投
        invest_should = d_base
        invest_actual = d_base * (1 + d_rate) ** weeks
        # 累计实投
        cumulative_invest = sum(d_base * (1 + d_rate) ** w for w in range(1, weeks+1))
        # 应到价值
        expected_value = state['initial_position_value'] + d_base * weeks

        # 上周组合市值（用上周底仓×本周收盘价近似）
        last_week_val = state['last_week_position'] * close_price
        # 本周组合市值
        current_val   = amount * close_price

        # 1️⃣ 每期总收益率 = (本周组合市值 - 上周组合市值) / 上周组合市值
        weekly_return = (current_val - last_week_val) / last_week_val if last_week_val>0 else 0.0
        # 2️⃣ 盈亏比 = (当前组合市值 - 累计实投) / 累计实投
        total_return  = (current_val - cumulative_invest) / cumulative_invest if cumulative_invest>0 else 0.0

        # 3️⃣ 每期累计底仓盈利 = (本周底仓份额 - 上周底仓份额) * 本周收盘价
        weekly_bottom_profit = (state['base_position'] - state['last_week_position']) * close_price
        # 4️⃣ 总累计底仓盈利 = 本周底仓份额 * 本周收盘价 - 初始投入价值
        total_bottom_profit  = state['base_position'] * close_price - state['initial_position_value']

        # 5️⃣ 标准数量 = 底仓 + 单次网格交易数量 * 5
        standard_qty    = state['base_position'] + state['grid_unit'] * 5
        # 6️⃣ 中间数量 = 底仓 + 单次网格交易数量 * 15
        intermediate_qty= state['base_position'] + state['grid_unit'] * 15

        # 7️⃣ 对比定投成本 = 本周增加的目标底仓份额 * 本周收盘价
        added_base      = state['base_position'] - state['last_week_position']
        compare_cost    = added_base * close_price

        # 8️⃣ 盈亏 = 全部仓位的盈亏 = (收盘价 - 成本价) * 持仓
        profit_all      = (close_price - cost_basis) * amount

        # 可T数量
        t_quantity = max(0, amount - state['base_position'])

        row = [
            current_date,
            f"{close_price:.3f}",
            str(weeks),
            str(count),
            f"{weekly_return:.2%}",
            f"{total_return:.2%}",
            f"{expected_value:.2f}",
            f"{invest_should:.0f}",
            f"{invest_actual:.0f}",
            f"{cumulative_invest:.0f}",
            str(state['initial_base_position']),
            str(state['base_position']),
            f"{state['base_position'] * close_price:.0f}",
            f"{weekly_bottom_profit:.0f}",
            f"{total_bottom_profit:.0f}",
            str(state['base_position']),
            str(amount),
            str(state['grid_unit']),
            str(t_quantity),
            str(standard_qty),
            str(intermediate_qty),
            str(state['max_position']),
            f"{cost_basis:.3f}",
            f"{compare_cost:.3f}",
            f"{profit_all:.0f}"
        ]

        # 写入 CSV
        is_new = not report_file.exists()
        with open(report_file, 'a', encoding='utf-8') as f:
            if is_new:
                headers = [
                    "时间","市价","期数","次数","每期总收益率","盈亏比","应到价值",
                    "当周应投入金额","当周实际投入金额","实际累计投入金额","定投底仓份额",
                    "累计底仓份额","累计底仓价值","每期累计底仓盈利","总累计底仓盈利",
                    "底仓","股票余额","单次网格交易数量","可T数量","标准数量","中间数量",
                    "极限数量","成本价","对比定投成本","盈亏"
                ]
                f.write(",".join(headers) + "\n")
            f.write(",".join(row) + "\n")

        info(f'✅ [{symbol}] 已更新每日报表：{report_file}')