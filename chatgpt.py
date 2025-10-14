# event_driven_grid_strategy.py
# 版本号：CHATGPT-3.2.1-20251014-HALT-GUARD
# 变更点：为“停牌/无价”行情增加统一防护（HALT-GUARD）：
# 1) VA(价值平均) 在无效价时跳过，不调整底仓，避免被算成0；
# 2) 行情缓存在无效价时不覆盖上一笔有效价，并标记 mark_halted；
# 3) 下单路径在停牌标记下直接返回，不做棘轮/挂单；
# 4) 看板/报表计算用 last_valid_price，避免 NaN 传染；标注停牌。
# 备注：不改变核心交易逻辑与网格参数，仅在“无价/停牌”场景加护栏。

import json                              # 标准库：JSON 读写
import logging                           # 标准库：日志
import math                              # 标准库：数学函数（用于ceil等）
from datetime import datetime, time      # 标准库：时间处理
from pathlib import Path                 # 标准库：跨平台路径
from types import SimpleNamespace        # 简单对象封装（撮合回报包装）

# ---------------- 全局句柄与常量 ----------------
LOG_FH = None                            # 研究日志文件句柄（写入研究目录）
MAX_SAVED_FILLED_IDS = 500               # 成交订单ID的持久化保存上限
__version__ = 'CHATGPT-3.2.1-20251014-HALT-GUARD'  # 当前策略版本号（按你的规范）
TRANSACTION_COST = 0.00005               # 交易成本（万分之0.5，仅用于间距下限）

# --------------- 通用路径与工具函数 ---------------

def research_path(*parts) -> Path:
    """研究目录根 + 子路径，确保父目录存在"""
    p = Path(get_research_path()).joinpath(*parts)  # 拼接到研究目录
    p.parent.mkdir(parents=True, exist_ok=True)     # 确保父目录存在
    return p                                        # 返回路径对象

def info(msg, *args):
    """统一日志输出到平台 log 与研究日志文件"""
    text = msg.format(*args)                        # 格式化文本
    log.info(text)                                  # 输出到平台日志
    if LOG_FH:                                      # 同步写研究日志文件
        LOG_FH.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} INFO {text}\n")
        LOG_FH.flush()

def get_saved_param(key, default=None):
    """读取平台级持久化参数（失败则回退默认）"""
    try: 
        return get_parameter(key)                   # 平台参数
    except: 
        return default                              # 异常返回默认

def set_saved_param(key, value):
    """写入平台级持久化参数（失败忽略）"""
    try: 
        set_parameter(key, value)                   # 平台参数写入
    except: 
        pass                                        # 忽略异常

def check_environment():
    """根据账户ID识别运行环境（回测/实盘/模拟/未知）"""
    try:
        u = str(get_user_name())                    # 取账户或用户标识
        if u == '55418810': return '回测'           # 你的回测标识
        if u == '8887591588': return '实盘'         # 你的实盘标识
        return '模拟'                                # 其他默认模拟
    except:
        return '未知'                                # 取值异常则未知

def convert_symbol_to_standard(full_symbol):
    """将 XSHE/XSHG 转为 SZ/SS，内部使用统一标准"""
    if not isinstance(full_symbol, str): 
        return full_symbol                          # 非字符串原样返回
    if full_symbol.endswith('.XSHE'): 
        return full_symbol.replace('.XSHE','.SZ')   # 深交所
    if full_symbol.endswith('.XSHG'): 
        return full_symbol.replace('.XSHG','.SS')   # 上交所
    return full_symbol                              # 已是标准则返回

# ---------------- HALT-GUARD：有效价与停牌标记 ----------------

def is_valid_price(x):
    """判定是否为有效价：非None、非NaN、>0"""
    try:
        if x is None: 
            return False                            # None 无效
        if isinstance(x, float) and math.isnan(x): 
            return False                            # NaN 无效
        if x <= 0: 
            return False                            # 非正数无效
        return True                                 # 其余为有效
    except:
        return False                                # 异常视为无效

# ---------------- 状态保存 ----------------

def save_state(symbol, state):
    """保存重要状态到参数与JSON文件（裁剪成交ID长度）"""
    ids = list(state.get('filled_order_ids', set()))               # 成交ID集合转列表
    state['filled_order_ids'] = set(ids[-MAX_SAVED_FILLED_IDS:])   # 状态内也裁剪
    store_keys = ['base_price', 'grid_unit', 'max_position', 'last_week_position', 'base_position']  # 需要持久化的键
    store = {k: state.get(k) for k in store_keys}                  # 构造持久化字典
    store['filled_order_ids'] = ids[-MAX_SAVED_FILLED_IDS:]        # 成交ID保存列表
    store['trade_week_set'] = list(state.get('trade_week_set', []))# 周期集合转列表
    set_saved_param(f'state_{symbol}', store)                      # 写平台参数
    research_path('state', f'{symbol}.json').write_text(           # 写JSON文件
        json.dumps(store, indent=2), encoding='utf-8'
    )

def safe_save_state(symbol, state):
    """保存状态带容错"""
    try: 
        save_state(symbol, state)                                  # 正常保存
    except Exception as e: 
        info('[{}] ⚠️ 状态保存失败: {}', symbol, e)               # 失败打点

# ---------------- 初始化与时间窗口判断 ----------------

def initialize(context):
    """策略初始化入口"""
    global LOG_FH                                             # 使用全局日志句柄
    log_file = research_path('logs', 'event_driven_strategy.log')  # 研究日志文件路径
    LOG_FH = open(log_file, 'a', encoding='utf-8')            # 打开文件句柄
    log.info(f'🔍 日志同时写入到 {log_file}')                   # 平台提示
    context.env = check_environment()                         # 识别环境
    info("当前环境：{}", context.env)                          # 输出环境
    context.run_cycle = get_saved_param('run_cycle_seconds', 60)   # 拉起周期（预留）

    # ----- 读取配置 -----
    try:
        config_file = research_path('config', 'symbols.json') # 配置文件路径
        context.config_file_path = config_file                # 挂到上下文
        if config_file.exists():                              # 若存在
            context.symbol_config = json.loads(config_file.read_text(encoding='utf-8'))  # 读配置
            context.last_config_mod_time = config_file.stat().st_mtime                    # 记录修改时间
            info('✅ 从 {} 加载 {} 个标的配置', config_file, len(context.symbol_config))   # 打点
        else:
            log.error(f"❌ 配置文件 {config_file} 不存在，请创建！")  # 报错提示
            context.symbol_config = {}                        # 置空
    except Exception as e:
        log.error(f"❌ 加载配置文件失败：{e}")                    # 异常
        context.symbol_config = {}                            # 置空

    # ----- 初始化容器 -----
    context.symbol_list = list(context.symbol_config.keys())  # 监控标的列表
    context.state = {}                                        # 每标的状态字典
    context.latest_data = {}                                  # 最新价缓存（有效价才更新）
    context.should_place_order_map = {}                       # 市价触发的节流标记

    # HALT-GUARD：为每个标的准备“停牌标记/最后有效价”容器
    context.mark_halted = {}                                  # 是否停牌/无价
    context.last_valid_price = {}                             # 最后一次有效价（用于看板/报表/对比）

    # ----- 用配置初始化每个标的状态 -----
    for sym, cfg in context.symbol_config.items():            # 遍历配置表
        state_file = research_path('state', f'{sym}.json')    # 该标的状态文件
        saved = json.loads(state_file.read_text(encoding='utf-8')) if state_file.exists() else get_saved_param(f'state_{sym}', {}) or {}  # 读取历史
        st = {**cfg}                                          # 基于配置复制初始
        st.update({
            'base_price': saved.get('base_price', cfg['base_price']),                               # 基准价
            'grid_unit': saved.get('grid_unit', cfg['grid_unit']),                                  # 网格单位
            'filled_order_ids': set(saved.get('filled_order_ids', [])),                             # 成交ID集合
            'trade_week_set': set(saved.get('trade_week_set', [])),                                 # 触发过的周集合
            'base_position': saved.get('base_position', cfg['initial_base_position']),              # 当前底仓
            'last_week_position': saved.get('last_week_position', cfg['initial_base_position']),    # 上周底仓
            'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],             # 初始底仓市值
            'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,                                  # 初始买卖间距
            'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20)  # 最大仓
        })
        context.state[sym] = st                               # 写入状态
        context.latest_data[sym] = st['base_price']           # 最新价先放基准价（会被有效价覆盖）
        context.should_place_order_map[sym] = True            # 市价触发开关
        context.mark_halted[sym] = False                      # 初始认为未停牌
        context.last_valid_price[sym] = st['base_price']      # 最后有效价先置为基准价

    # ----- 绑定定时任务 -----
    context.initial_cleanup_done = False                      # 启动清理未完成
    if '回测' not in context.env:                             # 实盘/模拟才绑定
        run_daily(context, place_auction_orders, time='9:15') # 集合竞价补挂
        run_daily(context, end_of_day, time='14:55')          # 日终动作
        info('✅ 事件驱动模式就绪')                            # 打点
    info('✅ 初始化完成，版本:{}', __version__)                 # 打版本

def is_main_trading_time():
    """主盘时间：09:30-11:30 & 13:00-15:00"""
    now = datetime.now().time()                               # 当前时间
    return (time(9, 30) <= now <= time(11, 30)) or (time(13, 0) <= now <= time(15, 0))  # 区间判断

def is_auction_time():
    """集合竞价时间：09:15-09:25"""
    now = datetime.now().time()
    return time(9, 15) <= now < time(9, 25)

def is_order_blocking_period():
    """撮合冻结时间：09:25-09:30（不下单）"""
    now = datetime.now().time()
    return time(9, 25) <= now < time(9, 30)

# ---------------- 启动后清理与收敛 ----------------

def before_trading_start(context, data):
    """开盘前回调：清理遗留挂单，并在竞价时补挂"""
    if context.initial_cleanup_done: 
        return                                               # 已处理则返回
    info('🔁 before_trading_start：清理遗留挂单')               # 打点
    after_initialize_cleanup(context)                        # 清理全部挂单
    current_time = context.current_dt.time()                 # 当前时间
    if time(9, 15) <= current_time < time(9, 30):            # 若在竞价时段
        info('⏭ 重启在集合竞价时段，补挂网格')                   # 打点
        place_auction_orders(context)                        # 挂竞价单
    else:
        info('⏸️ 重启时间{}不在集合竞价时段，跳过补挂网格', current_time.strftime('%H:%M:%S'))  # 提示
    context.initial_cleanup_done = True                      # 标记完成

def after_initialize_cleanup(context):
    """启动后的全品种撤单清理（避免残留挂单干扰）"""
    if '回测' in context.env or not hasattr(context, 'symbol_list'): 
        return                                               # 回测不需要
    info('🧼 按品种清理所有遗留挂单')                           # 打点
    for sym in context.symbol_list:                          # 遍历撤单
        cancel_all_orders_by_symbol(context, sym)
    info('✅ 按品种清理完成')                                   # 完成提示

# ---------------- 订单与撤单工具 ----------------

def get_order_status(entrust_no):
    """查询订单状态，失败返回空串"""
    try:
        order_detail = get_order(entrust_no)                 # 查询
        return str(order_detail.get('status', '')) if order_detail else ''  # 取状态
    except Exception as e:
        info('⚠️ 查询订单状态失败 entrust_no={}: {}', entrust_no, e)  # 打点
        return ''                                            # 失败返回空

def cancel_all_orders_by_symbol(context, symbol):
    """按标的撤销所有未完成挂单（过滤已成/已撤等）"""
    all_orders = get_all_orders() or []                      # 拉取所有订单
    total = 0                                                # 计数
    if not hasattr(context, 'canceled_cache'):
        context.canceled_cache = {'date': None, 'orders': set()}  # 初始化撤单缓存
    today = context.current_dt.date()                        # 今日
    if context.canceled_cache.get('date') != today:
        context.canceled_cache = {'date': today, 'orders': set()} # 跨日重置缓存
    cache = context.canceled_cache['orders']                 # 拿到缓存集合
    for o in all_orders:                                     # 遍历订单
        api_sym = o.get('symbol') or o.get('stock_code')     # API返回的代码
        if convert_symbol_to_standard(api_sym) != symbol: 
            continue                                         # 非本标的跳过
        status = str(o.get('status', ''))                    # 订单状态
        entrust_no = o.get('entrust_no')                     # 委托号
        if not entrust_no or status != '2' or entrust_no in context.state[symbol]['filled_order_ids'] or entrust_no in cache:
            continue                                         # 非“已报”或已成/已撤/缓存命中过滤
        final_status = get_order_status(entrust_no)          # 再查一次状态
        if final_status in ('4', '5', '6', '8'): 
            continue                                         # 已拒/撤/部成/全成不撤
        cache.add(entrust_no)                                # 写入缓存避免重复
        total += 1                                           # 计数+1
        info('[{}] 👉 发现并尝试撤销遗留挂单 entrust_no={}', symbol, entrust_no)  # 打点
        try: 
            cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})  # 撤单
        except Exception as e: 
            info('[{}] ⚠️ 撤单异常 entrust_no={}: {}', symbol, entrust_no, e)  # 异常
    if total > 0: 
        info('[{}] 共{}笔遗留挂单尝试撤销完毕', symbol, total)   # 统计提示

# ---------------- 集合竞价挂单 ----------------

def place_auction_orders(context):
    """集合竞价/盘中首次：清空防抖并补挂网格"""
    if '回测' in context.env or not (is_auction_time() or is_main_trading_time()): 
        return                                               # 回测或非交易时段跳过
    info('🆕 清空防抖缓存，开始集合竞价挂单')                       # 打点
    for st in context.state.values():                        # 清空最近挂单节流
        st.pop('_last_order_bp', None); st.pop('_last_order_ts', None)
    for sym in context.symbol_list:                          # 遍历标的
        state = context.state[sym]                           # 状态引用
        adjust_grid_unit(state)                              # 自适应放大网格单位
        cancel_all_orders_by_symbol(context, sym)            # 先撤旧单
        context.latest_data[sym] = state['base_price']       # 重置最新价为基准（竞价数据稍后刷新）
        place_limit_orders(context, sym, state)              # 依网格下限价单
        safe_save_state(sym, state)                          # 保存状态

# ---------------- 网格限价挂单主逻辑 ----------------

def place_limit_orders(context, symbol, state):
    """
    限价挂单主函数（含“棘轮”与节流）。
    HALT-GUARD：若停牌/无价，直接返回，不做任何基准价/棘轮移动。
    """
    now_dt = context.current_dt                              # 当前回调时间

    # --- 停牌/无价保护：发现停牌标记则直接返回 ---
    if context.mark_halted.get(symbol, False):               # 若被标记停牌
        return                                               # 不做任何操作

    # --- 前置节流与时间窗 ---
    if state.get('_last_trade_ts') and (now_dt - state['_last_trade_ts']).total_seconds() < 60: 
        return                                               # 成交后60秒内不重复挂
    if is_order_blocking_period(): 
        return                                               # 09:25-09:30 不下单
    if not (is_auction_time() or (is_main_trading_time() and now_dt.time() < time(14, 50))): 
        return                                               # 14:50后仅考虑市价触发

    # --- 行情与有效价检查 ---
    price = context.latest_data.get(symbol)                  # 最新价（仅在有效时更新）
    if not is_valid_price(price): 
        return                                               # 无效价直接返回（防守）
    base = state['base_price']                               # 当前基准价
    if abs(price / base - 1) > 0.10: 
        return                                               # 与基准偏离>10%保护

    # --- 网格关键变量 ---
    unit, buy_sp, sell_sp = state['grid_unit'], state['buy_grid_spacing'], state['sell_grid_spacing']  # 单位与间距
    buy_p, sell_p = round(base * (1 - buy_sp), 3), round(base * (1 + sell_sp), 3)                      # 买卖价位

    position = get_position(symbol)                           # 查询持仓
    pos = position.amount + state.get('_pos_change', 0)       # 使用成交尚未入账的 _pos_change 补偿

    # --- 棘轮触发条件 ---
    is_in_low_pos_range  = (pos - unit <= state['base_position'])          # 低位区：仅买不卖的下边界
    ratchet_up   = is_in_low_pos_range and price >= sell_p                 # 向上棘轮（价触卖位且低位区）

    is_in_high_pos_range = (pos + unit >= state['max_position'])           # 高位区：仅卖不买的上边界
    ratchet_down = is_in_high_pos_range and price <= buy_p                 # 向下棘轮（价触买位且高位区）

    # --- 常规节流（非棘轮） ---
    if not (ratchet_up or ratchet_down):                    # 若未触发棘轮
        last_ts = state.get('_last_order_ts')               # 最近下单时间
        if last_ts and (now_dt - last_ts).seconds < 30: 
            return                                          # 30秒节流
        last_bp = state.get('_last_order_bp')               # 最近下单时的基准
        if last_bp and abs(base / last_bp - 1) < buy_sp / 2:
            return                                          # 基准变化太小则不重复挂
        state['_last_order_ts'], state['_last_order_bp'] = now_dt, base  # 更新节流锚点

    # --- 棘轮：即时上移/下移基准价 ---
    if ratchet_up:
        state['base_price'] = sell_p                        # 基准抬到卖价
        info('[{}] 棘轮上移: 价格上涨触及卖价，基准价上移至 {:.3f}', symbol, sell_p)
        cancel_all_orders_by_symbol(context, symbol)        # 刷单
        buy_p, sell_p = round(sell_p * (1 - state['buy_grid_spacing']), 3), round(sell_p * (1 + state['sell_grid_spacing']), 3)  # 重新计算新网格
    elif ratchet_down:
        state['base_price'] = buy_p                         # 基准下到买价
        info('[{}] 棘轮下移: 价格下跌触及买价，基准价下移至 {:.3f}', symbol, buy_p)
        cancel_all_orders_by_symbol(context, symbol)        # 刷单
        buy_p, sell_p = round(buy_p * (1 - state['buy_grid_spacing']), 3), round(buy_p * (1 + state['sell_grid_spacing']), 3)     # 重新计算

    # --- 执行挂单 ---
    try:
        open_orders = [o for o in get_open_orders(symbol) or [] if o.status == '2']  # 取在途“已报”订单
        enable_amount = position.enable_amount                # 可卖数量
        state.pop('_pos_change', None)                        # 消费临时持仓变化

        can_buy = not any(o.amount > 0 and abs(o.price - buy_p) < 1e-3 for o in open_orders)  # 避免重复价位
        if can_buy and pos + unit <= state['max_position']:   # 不超过最大仓
            info('[{}] --> 发起买入委托: {}股 @ {:.3f}', symbol, unit, buy_p)
            order(symbol, unit, limit_price=buy_p)            # 下买单（限价）

        can_sell = not any(o.amount < 0 and abs(o.price - sell_p) < 1e-3 for o in open_orders) # 避免重复价位
        if can_sell and enable_amount >= unit and pos - unit >= state['base_position']:  # 不低于底仓
            info('[{}] --> 发起卖出委托: {}股 @ {:.3f}', symbol, unit, sell_p)
            order(symbol, -unit, limit_price=sell_p)          # 下卖单（限价）

    except Exception as e:
        info('[{}] ⚠️ 限价挂单异常：{}', symbol, e)              # 异常提示
    finally:
        safe_save_state(symbol, state)                         # 收尾保存

# ---------------- 成交回报与后续挂单 ----------------

def on_trade_response(context, trade_list):
    """撮合回报：转为 on_order_filled 处理"""
    for tr in trade_list:                                     # 遍历回报
        if str(tr.get('status')) != '8': 
            continue                                          # 非全部成交不处理
        sym = convert_symbol_to_standard(tr['stock_code'])    # 统一代码
        entrust_no = tr['entrust_no']                         # 委托号
        log_trade_details(context, sym, tr)                   # 详单落盘
        if sym not in context.state or entrust_no in context.state[sym]['filled_order_ids']: 
            continue                                          # 无状态或已处理过跳过
        context.state[sym]['filled_order_ids'].add(entrust_no)# 记录已成
        safe_save_state(sym, context.state[sym])              # 保存一次
        order_obj = SimpleNamespace(                          # 打包订单对象
            order_id = entrust_no,
            amount   = tr['business_amount'] if tr['entrust_bs']=='1' else -tr['business_amount'],
            filled   = tr['business_amount'],
            price    = tr['business_price']
        )
        try:
            on_order_filled(context, sym, order_obj)          # 进入成交处理
        except Exception as e:
            info('[{}] ❌ 成交处理失败：{}', sym, e)               # 异常

def on_order_filled(context, symbol, order):
    """统一的成交处理：更新基准、清单、尝试继续挂网格"""
    state = context.state[symbol]                             # 取状态
    if order.filled == 0: 
        return                                                # 无成交数量则返回
    last_dt = state.get('_last_fill_dt')                      # 上次成交时间
    if state.get('last_fill_price') == order.price and last_dt and (context.current_dt - last_dt).seconds < 5:
        return                                                # 5秒内同价重复回报去重
    trade_direction = "买入" if order.amount > 0 else "卖出"    # 方向
    info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f}', symbol, trade_direction, order.filled, order.price)  # 打点
    state['_last_trade_ts'] = context.current_dt              # 成交时间
    state['_last_fill_dt'] = context.current_dt               # 最近填充时间
    state['last_fill_price'] = order.price                    # 最近成交价
    state['base_price'] = order.price                         # 将基准价更新为成交价
    info('[{}] 🔄 成交后基准价更新为 {:.3f}', symbol, order.price)  # 提示
    state['_pos_change'] = order.amount                       # 记录持仓临时变化
    cancel_all_orders_by_symbol(context, symbol)              # 刷新在途挂单

    # 成交即视为存在有效价，复位 HALT 标记与最后有效价
    context.mark_halted[symbol] = False                       # 清停牌
    context.last_valid_price[symbol] = order.price            # 更新最后有效价
    context.latest_data[symbol] = order.price                 # 最新价覆盖为成交价

    if is_order_blocking_period():                            # 若在冻结期
        info('[{}] 处于9:25-9:30挂单冻结期，成交后仅更新状态，推迟挂单至9:30后。', symbol)
    elif context.current_dt.time() < time(14, 50):            # 正常交易时段
        place_limit_orders(context, symbol, state)            # 继续挂限价
    context.should_place_order_map[symbol] = True             # 恢复市价触发允许
    safe_save_state(symbol, state)                            # 保存状态

# ---------------- 行情主循环 ----------------

def handle_data(context, data):
    """分时回调：刷新行情、动态网格、触发下单、看板/报表"""
    now_dt = context.current_dt                               # 当前时间
    now = now_dt.time()                                       # 当前时分秒

    # 每5分钟：重载配置 + 更新看板
    if now_dt.minute % 5 == 0 and now_dt.second < 5:
        reload_config_if_changed(context)                     # 热重载配置
        generate_html_report(context)                         # 看板更新

    # ---------- HALT-GUARD：更新最新行情并标记停牌 ----------
    # 对每个标的：如果这一笔价无效，则不覆盖最新价，并标记为停牌；如有效则更新并清停牌。
    for sym in context.symbol_list:
        if sym in data and data[sym] and is_valid_price(getattr(data[sym], 'price', None)):
            px = float(data[sym].price)                       # 取有效价
            context.latest_data[sym] = px                     # 覆盖最新价
            context.last_valid_price[sym] = px                # 更新最后有效价
            context.mark_halted[sym] = False                  # 清停牌标记
        else:
            # 无价/停牌：不覆盖 latest_data，打标记（保持上一笔有效价供比较/展示）
            context.mark_halted[sym] = True                   # 置停牌

    # ---------- 动态目标底仓与网格间距 ----------
    for sym in context.symbol_list:
        if sym not in context.state: 
            continue                                          # 无状态略过
        st = context.state[sym]                               # 引用状态
        price = context.latest_data.get(sym)                  # 最新价（可能是上一笔有效价）
        if not is_valid_price(price): 
            continue                                          # 无效价不做任何计算
        get_target_base_position(context, sym, st, price, now_dt) # VA 更新（内部含停牌保护）
        adjust_grid_unit(st)                                  # 放大网格单位
        if now_dt.minute % 30 == 0 and now_dt.second < 5:     # 每30分钟一次
            update_grid_spacing_final(context, sym, st, get_position(sym).amount)  # 动态间距

    # ---------- 下单路径 ----------
    if is_auction_time() or (is_main_trading_time() and now < time(14, 50)):  # 限价阶段
        for sym in context.symbol_list:
            if sym in context.state:
                place_limit_orders(context, sym, context.state[sym])           # 限价挂单
    if time(14, 55) <= now < time(14, 57):                                     # 收盘前市价触发
        for sym in context.symbol_list:
            if sym in context.state:
                place_market_orders_if_triggered(context, sym, context.state[sym])  # 市价触发

    # ---------- 状态巡检 ----------
    if now_dt.minute % 30 == 0 and now_dt.second < 5:
        info('📌 每30分钟状态巡检...')                         # 打点
        for sym in context.symbol_list:
            if sym in context.state:
                log_status(context, sym, context.state[sym], context.latest_data.get(sym))  # 概览

# ---------------- 收盘前市价触发 ----------------

def place_market_orders_if_triggered(context, symbol, state):
    """14:55-14:57 收盘前的市价触发下单（避免资金被限价单占用过夜）"""
    if not is_main_trading_time(): 
        return                                                # 非交易时段保护

    # 停牌/无价保护：不触发市价单
    if context.mark_halted.get(symbol, False): 
        return                                                # 停牌不触发

    price = context.latest_data.get(symbol)                   # 最新价
    if not is_valid_price(price): 
        return                                                # 无效价不触发
    base = state['base_price']                                # 基准价
    if abs(price/base - 1) > 0.10: 
        return                                                # 偏离>10%保护

    adjust_grid_unit(state)                                   # 放大网格单位
    pos, unit = get_position(symbol).amount, state['grid_unit']  # 当前持仓与单位
    buy_p  = round(base * (1 - state['buy_grid_spacing']), 3)    # 买位
    sell_p = round(base * (1 + state['sell_grid_spacing']), 3)    # 卖位
    if not context.should_place_order_map.get(symbol, True): 
        return                                                # 市价节流未解除

    try:
        # 仅当“价格真实触达网格价位”时才触发（你的既定规则）
        if price <= buy_p and pos + unit <= state['max_position']:             # 触发买
            info('[{}] 市价买触发: {}股 @ {:.3f}', symbol, unit, price)
            order_market(symbol, unit, market_type='0')                         # 平台市价买
            state['base_price'] = buy_p                                        # 成交前先把基准锚到买位
        elif price >= sell_p and pos - unit >= state['base_position']:         # 触发卖
            info('[{}] 市价卖触发: {}股 @ {:.3f}', symbol, unit, price)
            order_market(symbol, -unit, market_type='0')                        # 平台市价卖
            state['base_price'] = sell_p                                       # 成交前锚到卖位
    except Exception as e:
        info('[{}] ⚠️ 市价挂单异常：{}', symbol, e)             # 异常提示
    finally:
        context.should_place_order_map[symbol] = False         # 本轮触发后关闭开关
        safe_save_state(symbol, state)                         # 保存

# ---------------- 监控输出 ----------------

def log_status(context, symbol, state, price):
    """控制台状态简报（用最后有效价计算，避免NaN）"""
    # 选择展示用价格：优先用 last_valid_price，退化到 state.base_price
    disp_price = context.last_valid_price.get(symbol, state['base_price'])     # 展示价
    if not is_valid_price(disp_price): 
        return                                                # 仍无效则不打印
    pos = get_position(symbol)                                # 取持仓
    pnl = (disp_price - pos.cost_basis) * pos.amount if pos.cost_basis > 0 else 0  # 浮盈
    info("📊 [{}] 状态: 价:{:.3f} 持仓:{}(可卖:{}) / 底仓:{} 成本:{:.3f} 盈亏:{:.2f} 网格:[买{:.2%},卖{:.2%}]",
         symbol, disp_price, pos.amount, pos.enable_amount, state['base_position'], pos.cost_basis, pnl, state['buy_grid_spacing'], state['sell_grid_spacing'])

# ---------------- 动态网格间距（ATR） ----------------

def update_grid_spacing_final(context, symbol, state, curr_pos):
    """依据 ATR 调整买/卖间距（含交易成本下限）"""
    unit, base_pos = state['grid_unit'], state['base_position']  # 网格单位与底仓
    atr_pct = calculate_atr(context, symbol)                     # 计算ATR%
    base_spacing = 0.005                                         # 默认0.5%
    if atr_pct is not None:
        atr_multiplier = 0.25                                    # ATR 权重
        base_spacing = atr_pct * atr_multiplier                  # 得到基础间距
    min_spacing = TRANSACTION_COST * 5                           # 最小保护=成本*5
    base_spacing = max(base_spacing, min_spacing)                # 应用下限
    if curr_pos <= base_pos + unit * 5:
        new_buy, new_sell = base_spacing, base_spacing * 2       # 低仓：买小卖大
    elif curr_pos > base_pos + unit * 15:
        new_buy, new_sell = base_spacing * 2, base_spacing       # 高仓：买大卖小
    else:
        new_buy, new_sell = base_spacing, base_spacing           # 中间：对称
    max_spacing = 0.03                                           # 最大3%
    new_buy  = round(min(new_buy,  max_spacing), 4)              # 裁剪并四位小数
    new_sell = round(min(new_sell, max_spacing), 4)
    if new_buy != state.get('buy_grid_spacing') or new_sell != state.get('sell_grid_spacing'):
        state['buy_grid_spacing'], state['sell_grid_spacing'] = new_buy, new_sell # 应用
        info('[{}] 🌀 网格动态调整. ATR({:.2%}) -> 基础间距({:.2%}) -> 最终:[买{:.2%},卖{:.2%}]',
             symbol, (atr_pct or 0.0), base_spacing, new_buy, new_sell)

def calculate_atr(context, symbol, atr_period=14):
    """使用 get_history 计算 ATR；若无足够数据或价无效则返回 None"""
    try:
        hist = get_history(atr_period + 1, '1d', ['high','low','close'], security_list=[symbol])  # 拉历史K
        if hist is None or hist.empty or len(hist) < atr_period + 1:
            info('[{}] ⚠️ ATR计算失败: get_history未能返回足够的数据。', symbol)                  # 数据不足
            return None
        high, low, close = hist['high'].values, hist['low'].values, hist['close'].values          # 数组
        trs = [max(h - l, abs(h - pc), abs(l - pc)) for h, l, pc in zip(high[1:], low[1:], close[:-1])]  # TR
        if not trs: 
            return None                                      # 无TR
        atr_value = sum(trs) / len(trs)                      # 平均TR
        # 使用最后有效价作为当前价，若无则用最近收盘
        current_price = context.last_valid_price.get(symbol, close[-1])  # 展示价或前收
        if is_valid_price(current_price):
            return atr_value / current_price                 # ATR 百分比
        return None                                          # 无效则 None
    except Exception as e:
        info('[{}] ❌ ATR计算异常: {}', symbol, e)             # 异常
        return None                                          # 返回 None

# ---------------- 日终动作 ----------------

def end_of_day(context):
    """日终：撤单、看板、保存状态"""
    info('✅ 日终处理开始...')                                 # 打点
    after_initialize_cleanup(context)                        # 清理挂单
    generate_html_report(context)                            # 刷新看板
    for sym in context.symbol_list:
        if sym in context.state:
            safe_save_state(sym, context.state[sym])         # 保存状态
            context.should_place_order_map[sym] = True       # 重开市价触发
    info('✅ 日终保存状态完成')                                 # 提示

# ---------------- 价值平均（VA） ----------------

def get_target_base_position(context, symbol, state, price, dt):
    """计算定投目标底仓并更新底仓与max仓位（含停牌守护）"""
    # 若价无效（停牌/无价），直接跳过 VA，维持原底仓
    if not is_valid_price(price):
        info('[{}] ⚠️ 停牌/无有效价，跳过VA计算，底仓维持 {}', symbol, state['base_position'])  # 打点
        return state['base_position']                        # 返回原底仓

    weeks = get_trade_weeks(context, symbol, state, dt)      # 已交易周数
    # 目标市值 = 初始市值 + 每周定投的复利累和
    target_val = state['initial_position_value'] + sum(state['dingtou_base'] * (1 + state['dingtou_rate'])**w for w in range(1, weeks + 1))
    if price <= 0: 
        return state['base_position']                        # 保护（理论上不会到这）

    new_pos = target_val / price                             # 市值转份额
    min_base = round(state['initial_position_value'] / state['base_price'] / 100) * 100 if state['base_price'] > 0 else 0  # 初始底仓等值最小阈
    final_pos = round(max(min_base, new_pos) / 100) * 100    # 取较大并取整到百股

    if final_pos != state['base_position']:                  # 若发生变化
        current_val = state['base_position'] * price         # 当前底仓市值
        delta_val = target_val - current_val                 # 市值缺口
        info('[{}] 价值平均: 目标底仓从 {} 调整至 {}. (目标市值: {:.2f}, 当前市值: {:.2f}, 市值缺口: {:.2f})', 
             symbol, state['base_position'], final_pos, target_val, current_val, delta_val)  # 打点
        state['base_position'] = final_pos                   # 应用底仓
        state['max_position'] = final_pos + state['grid_unit'] * 20  # 同步最大仓
    return final_pos                                         # 返回最新底仓

def get_trade_weeks(context, symbol, state, dt):
    """按自然周统计触发次数，用于VA进度"""
    y, w, _ = dt.date().isocalendar()                        # ISO 周
    key = f"{y}_{w}"                                         # 周键
    if key not in state.get('trade_week_set', set()):        # 若首次进入本周
        if 'trade_week_set' not in state: 
            state['trade_week_set'] = set()                  # 补初始化
        state['trade_week_set'].add(key)                     # 记录本周
        state['last_week_position'] = state['base_position'] # 记录当周初底仓
        safe_save_state(symbol, state)                       # 保存
    return len(state['trade_week_set'])                      # 返回累计周数

def adjust_grid_unit(state):
    """底仓扩大时放大网格单位，保持交易颗粒相对稳定"""
    orig, base_pos = state['grid_unit'], state['base_position']  # 原网格单位与底仓
    if base_pos >= orig * 20:                                 # 当底仓≥20个单位
        new_u = math.ceil(orig * 1.2 / 100) * 100             # 上调20%并向上取百股
        if new_u != orig:                                     # 若确有变化
            state['grid_unit'] = new_u                        # 应用新单位
            state['max_position'] = base_pos + new_u * 20     # 同步最大仓
            info('🔧 [{}] 底仓增加，网格单位放大: {}->{}', state.get('symbol',''), orig, new_u)  # 打点

# ---------------- 交易结束回调（平台触发） ----------------

def after_trading_end(context, data):
    """平台交易结束后回调：更新日报"""
    if '回测' in context.env: 
        return                                                # 回测不做
    info('⏰ 系统调用交易结束处理')                              # 打点
    update_daily_reports(context, data)                       # 写日报
    info('✅ 交易结束处理完成')                                  # 提示

# ---------------- 配置热重载 ----------------

def reload_config_if_changed(context):
    """检测 symbols.json 是否被修改，变化则热重载并差量更新状态"""
    try:
        current_mod_time = context.config_file_path.stat().st_mtime  # 取修改时间
        if current_mod_time == context.last_config_mod_time: 
            return                                          # 未变化
        info('🔄 检测到配置文件发生变更，开始热重载...')              # 打点
        context.last_config_mod_time = current_mod_time     # 记录时间
        new_config = json.loads(context.config_file_path.read_text(encoding='utf-8'))  # 读新配置
        old_symbols, new_symbols = set(context.symbol_list), set(new_config.keys())     # 新旧集合

        # 移除的标的：撤单、清理状态
        for sym in old_symbols - new_symbols:
            info(f'[{sym}] 标的已从配置中移除，将清理其状态和挂单...')   # 提示
            cancel_all_orders_by_symbol(context, sym)       # 撤单
            context.symbol_list.remove(sym)                 # 列表移除
            if sym in context.state: del context.state[sym] # 状态删
            if sym in context.latest_data: del context.latest_data[sym]   # 行情删
            context.mark_halted.pop(sym, None)              # 停牌标记删
            context.last_valid_price.pop(sym, None)         # 最后有效价删

        # 新增的标的：初始化状态
        for sym in new_symbols - old_symbols:
            info(f'[{sym}] 新增标的，正在初始化状态...')              # 提示
            cfg = new_config[sym]                          # 新配置
            st = {**cfg}                                   # 复制
            st.update({
                'base_price': cfg['base_price'], 'grid_unit': cfg['grid_unit'],        # 基准与单位
                'filled_order_ids': set(), 'trade_week_set': set(),                    # 空集合
                'base_position': cfg['initial_base_position'],                         # 初始底仓
                'last_week_position': cfg['initial_base_position'],                    # 上周底仓
                'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],  # 初始市值
                'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,                 # 初始间距
                'max_position': cfg['initial_base_position'] + cfg['grid_unit'] * 20   # 最大仓
            })
            context.state[sym] = st                       # 写状态
            context.latest_data[sym] = st['base_price']   # 初始价
            context.symbol_list.append(sym)               # 加入监控
            context.mark_halted[sym] = False              # 停牌标记
            context.last_valid_price[sym] = st['base_price']  # 最后有效价

        # 参数变更的标的：差量更新
        for sym in old_symbols.intersection(new_symbols):
            if context.symbol_config[sym] != new_config[sym]:  # 有差异
                info(f'[{sym}] 参数发生变更，正在更新...')        # 提示
                state, new_params = context.state[sym], new_config[sym]  # 取对象
                state.update({
                    'grid_unit': new_params['grid_unit'],                  # 新单位
                    'dingtou_base': new_params['dingtou_base'],            # 新定投额
                    'dingtou_rate': new_params['dingtou_rate'],            # 新增长率
                    'max_position': state['base_position'] + new_params['grid_unit'] * 20  # 同步最大仓
                })
        context.symbol_config = new_config               # 替换配置
        info('✅ 配置文件热重载完成！当前监控标的: {}', context.symbol_list)  # 完成提示
    except Exception as e:
        info(f'❌ 配置文件热重载失败: {e}')              # 异常提示

# ---------------- 日报/报表 ----------------

def update_daily_reports(context, data):
    """每个标的维护一份 CSV，收盘后追加一行（使用最后有效价以避开 NaN）"""
    reports_dir = research_path('reports')                       # 报表目录
    reports_dir.mkdir(parents=True, exist_ok=True)               # 确保存在
    current_date = context.current_dt.strftime("%Y-%m-%d")       # 日期字符串
    for symbol in context.symbol_list:                           # 遍历
        report_file = reports_dir / f"{symbol}.csv"              # 文件名
        state       = context.state[symbol]                      # 状态
        pos_obj     = get_position(symbol)                       # 持仓对象
        amount      = getattr(pos_obj, 'amount', 0)              # 总仓
        cost_basis  = getattr(pos_obj, 'cost_basis', state['base_price'])   # 成本
        # 使用最后有效价优先，退化到基准价
        close_price = context.last_valid_price.get(symbol, state['base_price'])  
        try:
            # 防守：若close_price仍无效，则使用成本或1避免除0
            if not is_valid_price(close_price):
                close_price = cost_basis if cost_basis > 0 else state['base_price']
                if not is_valid_price(close_price):
                    close_price = 1.0
        except:
            close_price = state['base_price']

        weeks       = len(state.get('trade_week_set', []))       # 期数
        count       = weeks                                      # 次数=期数
        d_base      = state['dingtou_base']                      # 定投额
        d_rate      = state['dingtou_rate']                      # 定投增长率
        invest_should = d_base                                   # 当周应投
        invest_actual = d_base * (1 + d_rate) ** weeks           # 当周实投（按VA口径）
        cumulative_invest = sum(d_base * (1 + d_rate) ** w for w in range(1, weeks+1))  # 累计实投
        expected_value = state['initial_position_value'] + d_base * weeks               # 简化应到价值
        last_week_val = state.get('last_week_position', 0) * close_price                # 上周底仓市值
        current_val   = amount * close_price                                            # 当前仓位市值
        weekly_return = (current_val - last_week_val) / last_week_val if last_week_val>0 else 0.0  # 周收益
        total_return  = (current_val - cumulative_invest) / cumulative_invest if cumulative_invest>0 else 0.0  # 总收益
        weekly_bottom_profit = (state['base_position'] - state.get('last_week_position', 0)) * close_price     # 周底仓盈利
        total_bottom_profit  = state['base_position'] * close_price - state['initial_position_value']          # 底仓累计盈利
        standard_qty    = state['base_position'] + state['grid_unit'] * 5   # 标准数量
        intermediate_qty= state['base_position'] + state['grid_unit'] * 15  # 中间数量
        added_base      = state['base_position'] - state.get('last_week_position', 0)  # 本周增底仓
        compare_cost    = added_base * close_price                   # 对比投入
        profit_all      = (close_price - cost_basis) * amount if cost_basis > 0 else 0  # 浮盈
        t_quantity = max(0, amount - state['base_position'])        # 可T数量（超底仓部分）

        row = [                                                    # 写一行
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

        is_new = not report_file.exists()                          # 是否新文件
        with open(report_file, 'a', encoding='utf-8', newline='') as f:
            if is_new:                                            # 首次写入表头
                headers = [
                    "时间","市价","期数","次数","每期总收益率","盈亏比","应到价值",
                    "当周应投入金额","当周实际投入金额","实际累计投入金额","定投底仓份额",
                    "累计底仓份额","累计底仓价值","每期累计底仓盈利","总累计底仓盈利",
                    "底仓","股票余额","单次网格交易数量","可T数量","标准数量","中间数量",
                    "极限数量","成本价","对比定投成本","盈亏"
                ]
                f.write(",".join(headers) + "\n")
            f.write(",".join(map(str, row)) + "\n")               # 写数据行
        info(f'✅ [{symbol}] 已更新每日CSV报表：{report_file}')       # 打点

# ---------------- 成交明细日志 ----------------

def log_trade_details(context, symbol, trade):
    """把每笔成交写到 a_trade_details.csv"""
    try:
        trade_log_path = research_path('reports', 'a_trade_details.csv')  # 文件
        is_new = not trade_log_path.exists()                              # 新文件判定
        with open(trade_log_path, 'a', encoding='utf-8', newline='') as f:
            if is_new:
                headers = ["time", "symbol", "direction", "quantity", "price", "base_position_at_trade"]  # 表头
                f.write(",".join(headers) + "\n")
            direction = "BUY" if trade['entrust_bs'] == '1' else "SELL"   # 方向
            base_position = context.state[symbol].get('base_position', 0) # 当时底仓
            row = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                symbol,
                direction,
                str(trade['business_amount']),
                f"{trade['business_price']:.3f}",
                str(base_position)
            ]
            f.write(",".join(row) + "\n")                                  # 写行
    except Exception as e:
        info(f'❌ 记录交易日志失败: {e}')                                   # 异常

# ---------------- HTML 看板 ----------------

def generate_html_report(context):
    """生成HTML看板；展示价格使用 last_valid_price，停牌时显示“停牌”标签"""
    all_metrics = []                                                     # 汇总数据
    total_market_value = 0                                               # 总市值
    total_unrealized_pnl = 0                                            # 总浮盈

    for symbol in context.symbol_list:                                   # 遍历
        if symbol not in context.state: 
            continue                                                     # 无状态略过
        state = context.state[symbol]                                    # 状态
        pos = get_position(symbol)                                       # 持仓
        # 展示价：优先最后有效价，退到基准价；并标记是否停牌
        price = context.last_valid_price.get(symbol, state['base_price'])# 展示价
        halted = context.mark_halted.get(symbol, False)                  # 停牌标记
        # 防守：若展示价仍无效，则退到成本或1
        if not is_valid_price(price):
            price = pos.cost_basis if pos.cost_basis > 0 else state['base_price']
            if not is_valid_price(price):
                price = 1.0

        market_value = pos.amount * price                                # 市值
        unrealized_pnl = (price - pos.cost_basis) * pos.amount if pos.cost_basis > 0 else 0  # 浮盈
        total_market_value += market_value                                # 汇总
        total_unrealized_pnl += unrealized_pnl

        atr_pct = calculate_atr(context, symbol)                          # ATR%
        name_price = f"{price:.3f}" + (" (停牌)" if halted else "")       # 价格字段加停牌标识

        all_metrics.append({                                             # 收集行
            "symbol": symbol,
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

    # —— 下面模板保持与你原来的风格一致（略） —— 
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
    table_rows = ""                                                      # HTML 行字符串
    for m in all_metrics:
        pnl_class = "positive" if float(m["unrealized_pnl"].replace(",", "")) >= 0 else "negative"  # 盈亏颜色
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

    final_html = html_template.format(                                   # 填充模板
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total_market_value=f"{total_market_value:,.2f}",
        total_unrealized_pnl=f"{total_unrealized_pnl:,.2f}",
        pnl_class="positive" if total_unrealized_pnl >= 0 else "negative",
        table_rows=table_rows
    )
    try:
        report_path = research_path('reports', 'strategy_dashboard.html')  # 输出路径
        report_path.write_text(final_html, encoding='utf-8')               # 写HTML
    except Exception as e:
        info(f'❌ 生成HTML看板失败: {e}')                                     # 异常
