# event_driven_grid_strategy.py
# 版本号：CHATGPT-3.2-20251013-SSE-MKT-PROTECT
# 变更摘要：
# - 为沪市（.SS）市价委托增加“保护限价”（protect limit），满足券商接口约束并控制滑点；
# - 保护价基于网格参考价±N tick（默认 N=2，tick=0.001，可配置），一次性放宽 +1 tick 重试；
# - 日志增强：市价触发时打印“触发价/网格价/保护价(及是否重试)”三元信息；
# - 深市（.SZ）仍走纯市价，保持不占资特性；如需统一为可成交限价，可在后续版本切换。

import json  # 导入 json 用于读写配置/状态
import logging  # 导入 logging 以使用平台日志
import math  # 导入 math 用于取整等运算
from datetime import datetime, time  # 导入时间相关类型
from pathlib import Path  # 导入 Path 操作文件路径
from types import SimpleNamespace  # 导入 SimpleNamespace 构造简单对象

# 全局文件句柄 & 常量
LOG_FH = None                                      # 文件日志句柄（用于双写日志）
MAX_SAVED_FILLED_IDS = 500                         # 成交订单号最多持久化数量（环形裁剪）
__version__ = 'CHATGPT-3.2-20251013-SSE-MKT-PROTECT'  # 当前策略版本号（按你要求命名）
TRANSACTION_COST = 0.00005                         # 估算交易费率（用于最小网格间距下限）

# ===【新增：市价保护参数，可通过参数存储覆盖】===
DEFAULT_PROTECT_TICK_SIZE = 0.001                  # 默认 tick（ETF 普遍 0.001）
DEFAULT_PROTECT_TICKS = 2                          # 保护价缓冲 tick 数（建议 1~2）
DEFAULT_PROTECT_RETRY_ENABLED = True               # 是否开启一次+1tick 的重试
# ===【新增 end】===

# --- 路径与辅助函数 ---
def research_path(*parts) -> Path:
    """研究目录根 + 子路径，确保文件夹存在"""
    p = Path(get_research_path()).joinpath(*parts)        # 拼接研究目录路径
    p.parent.mkdir(parents=True, exist_ok=True)            # 确保父目录存在
    return p                                               # 返回完整路径

def info(msg, *args):
    """统一日志输出到平台与本地文件"""
    text = msg.format(*args)                               # 格式化日志文本
    log.info(text)                                         # 平台日志
    if LOG_FH:                                             # 若开启了文件双写
        LOG_FH.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} INFO {text}\n")  # 写入时间戳+级别
        LOG_FH.flush()                                     # 立即落盘

def get_saved_param(key, default=None):
    """从平台参数存储读取值（异常返回默认）"""
    try: 
        return get_parameter(key)                          # 尝试读取
    except: 
        return default                                     # 失败返回默认

def set_saved_param(key, value):
    """写入平台参数存储（忽略异常）"""
    try: 
        set_parameter(key, value)                          # 持久化参数
    except: 
        pass                                               # 忽略错误

def check_environment():
    """根据账户号识别当前运行环境（仅用于标记日志）"""
    try:
        u = str(get_user_name())                           # 获取用户号
        if u == '55418810': return '回测'                   # 指定账号视作回测
        if u == '8887591588': return '实盘'                 # 指定账号视作实盘
        return '模拟'                                      # 其他视作模拟
    except:
        return '未知'                                      # 获取失败返回未知

def convert_symbol_to_standard(full_symbol):
    """将平台返回的交易所后缀转换为统一标准（.SS/.SZ）"""
    if not isinstance(full_symbol, str): return full_symbol            # 非字符串直接返回
    if full_symbol.endswith('.XSHE'): return full_symbol.replace('.XSHE','.SZ')  # 深市统一为 .SZ
    if full_symbol.endswith('.XSHG'): return full_symbol.replace('.XSHG','.SS')  # 沪市统一为 .SS
    return full_symbol                                                 # 其他保持不变

def save_state(symbol, state):
    """将关键状态持久化到参数存储与本地 JSON"""
    ids = list(state.get('filled_order_ids', set()))                   # 成交订单号集合转列表
    state['filled_order_ids'] = set(ids[-MAX_SAVED_FILLED_IDS:])       # 内存中也裁剪为最近 N 个
    store_keys = ['base_price', 'grid_unit', 'max_position', 'last_week_position', 'base_position']  # 需要持久化的键
    store = {k: state.get(k) for k in store_keys}                      # 取出要持久化的键值
    store['filled_order_ids'] = ids[-MAX_SAVED_FILLED_IDS:]            # 成交订单号列表
    store['trade_week_set'] = list(state.get('trade_week_set', []))    # 交易周集合转列表
    set_saved_param(f'state_{symbol}', store)                          # 写入平台参数存储
    research_path('state', f'{symbol}.json').write_text(               # 同步落地到研究目录
        json.dumps(store, indent=2), encoding='utf-8'
    )

def safe_save_state(symbol, state):
    """保存状态的安全包装（不因异常中断主流程）"""
    try: 
        save_state(symbol, state)                                      # 正常保存
    except Exception as e: 
        info('[{}] ⚠️ 状态保存失败: {}', symbol, e)                    # 失败记录日志

def initialize(context):
    """策略初始化"""
    global LOG_FH                                                     # 使用全局文件句柄
    log_file = research_path('logs', 'event_driven_strategy.log')     # 日志文件路径
    LOG_FH = open(log_file, 'a', encoding='utf-8')                    # 以追加模式打开
    log.info(f'🔍 日志同时写入到 {log_file}')                           # 首条提示
    context.env = check_environment()                                 # 标记当前环境
    info("当前环境：{}", context.env)                                  # 打印环境
    context.run_cycle = get_saved_param('run_cycle_seconds', 60)      # 读取运行周期（秒），默认 60

    # ===【新增：读取保护价相关配置，可被平台参数覆盖】===
    context.protect_tick_size = float(get_saved_param('protect_tick_size', DEFAULT_PROTECT_TICK_SIZE))  # tick 大小
    context.protect_ticks = int(get_saved_param('protect_ticks', DEFAULT_PROTECT_TICKS))                 # 保护 tick 数
    context.protect_retry_enabled = bool(get_saved_param('protect_retry_enabled', DEFAULT_PROTECT_RETRY_ENABLED))  # 是否重试
    # ===【新增 end】===

    try:
        config_file = research_path('config', 'symbols.json')          # 配置文件路径
        context.config_file_path = config_file                         # 存入上下文以便热重载
        if config_file.exists():                                       # 若文件存在
            context.symbol_config = json.loads(config_file.read_text(encoding='utf-8'))  # 读取 JSON
            context.last_config_mod_time = config_file.stat().st_mtime                    # 记录修改时间
            info('✅ 从 {} 加载 {} 个标的配置', config_file, len(context.symbol_config))  # 打印加载数量
        else:
            log.error(f"❌ 配置文件 {config_file} 不存在，请创建！")     # 提示缺失
            context.symbol_config = {}                                 # 置空配置
    except Exception as e:
        log.error(f"❌ 加载配置文件失败：{e}")                           # 读取异常
        context.symbol_config = {}                                     # 置空配置

    context.symbol_list = list(context.symbol_config.keys())           # 记录标的列表
    context.state = {}                                                 # 每标的运行状态字典
    context.latest_data = {}                                           # 每标的最新行情缓存
    context.should_place_order_map = {}                                # 市价触发的防抖控制（14:55 用）

    # 初始化每个标的的状态
    for sym, cfg in context.symbol_config.items():
        state_file = research_path('state', f'{sym}.json')             # 对应状态文件
        saved = json.loads(state_file.read_text(encoding='utf-8')) if state_file.exists() else get_saved_param(f'state_{sym}', {}) or {}  # 读取已存状态
        st = {**cfg}                                                   # 以配置为底
        st.update({                                                    # 合并运行期变量
            'base_price': saved.get('base_price', cfg['base_price']),                      # 基准价
            'grid_unit': saved.get('grid_unit', cfg['grid_unit']),                         # 网格单位
            'filled_order_ids': set(saved.get('filled_order_ids', [])),                    # 成交订单号集合
            'trade_week_set': set(saved.get('trade_week_set', [])),                        # 已经交易过的周集合
            'base_position': saved.get('base_position', cfg['initial_base_position']),     # 当前底仓
            'last_week_position': saved.get('last_week_position', cfg['initial_base_position']),  # 上周底仓
            'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],    # 初始底仓市值（用于VA）
            'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,                         # 初始网格间距（百分比）
            'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20)  # 最大仓位
        })
        context.state[sym] = st                                        # 缓存到上下文
        context.latest_data[sym] = st['base_price']                     # 初始化最新价为基准价
        context.should_place_order_map[sym] = True                      # 市价触发允许

    context.initial_cleanup_done = False                                # 标记开盘前清理是否完成
    if '回测' not in context.env:                                       # 非回测环境才注册日内事件
        run_daily(context, place_auction_orders, time='9:15')           # 9:15 集合竞价前置挂单
        run_daily(context, end_of_day, time='14:55')                    # 14:55 日终处理（含清挂单）
        info('✅ 事件驱动模式就绪')                                       # 提示就绪
    info('✅ 初始化完成，版本:{}', __version__)                           # 打印版本

def is_main_trading_time():
    """是否主交易时段（9:30-11:30 或 13:00-15:00）"""
    now = datetime.now().time()                                         # 当前时间
    return (time(9, 30) <= now <= time(11, 30)) or (time(13, 0) <= now <= time(15, 0))  # 判断区间

def is_auction_time():
    """是否集合竞价时段（9:15-9:25）"""
    now = datetime.now().time()                                         # 当前时间
    return time(9, 15) <= now < time(9, 25)                             # 判断区间

def is_order_blocking_period():
    """是否 9:25-9:30 冻结挂单时段"""
    now = datetime.now().time()                                         # 当前时间
    return time(9, 25) <= now < time(9, 30)                             # 判断区间

def before_trading_start(context, data):
    """开盘前清理与集合竞价补挂"""
    if context.initial_cleanup_done: return                             # 已清理则跳过
    info('🔁 before_trading_start：清理遗留挂单')                         # 打印动作
    after_initialize_cleanup(context)                                   # 按标的撤销遗留单
    current_time = context.current_dt.time()                            # 获取当前时间
    if time(9, 15) <= current_time < time(9, 30):                       # 若重启发生在竞价时段
        info('⏭ 重启在集合竞价时段，补挂网格')                           # 日志提示
        place_auction_orders(context)                                    # 执行补挂
    else:
        info('⏸️ 重启时间{}不在集合竞价时段，跳过补挂网格', current_time.strftime('%H:%M:%S'))  # 非竞价时段跳过
    context.initial_cleanup_done = True                                  # 标记完成

def after_initialize_cleanup(context):
    """启动/日终前，对每个标的清理所有未成挂单"""
    if '回测' in context.env or not hasattr(context, 'symbol_list'): return  # 回测或无标的直接返回
    info('🧼 按品种清理所有遗留挂单')                                      # 提示开始
    for sym in context.symbol_list:                                      # 遍历标的
        cancel_all_orders_by_symbol(context, sym)                         # 撤销该标的一切挂单
    info('✅ 按品种清理完成')                                              # 提示完成

def get_order_status(entrust_no):
    """查询单笔订单的最终状态（异常返回空）"""
    try:
        order_detail = get_order(entrust_no)                              # 调用平台查询
        return str(order_detail.get('status', '')) if order_detail else ''# 提取状态代码
    except Exception as e:
        info('⚠️ 查询订单状态失败 entrust_no={}: {}', entrust_no, e)        # 打印异常
        return ''                                                         # 返回空字符串

def cancel_all_orders_by_symbol(context, symbol):
    """按标的撤销所有已报未成的挂单（过滤已成与已撤）"""
    all_orders = get_all_orders() or []                                   # 取全局挂单列表
    total = 0                                                             # 计数器
    if not hasattr(context, 'canceled_cache'):                             # 初始化同日撤单缓存
        context.canceled_cache = {'date': None, 'orders': set()}
    today = context.current_dt.date()                                      # 今日日期
    if context.canceled_cache.get('date') != today:                        # 跨日则重置缓存
        context.canceled_cache = {'date': today, 'orders': set()}
    cache = context.canceled_cache['orders']                               # 当日撤单号集合
    for o in all_orders:                                                  # 遍历所有挂单
        api_sym = o.get('symbol') or o.get('stock_code')                  # 取平台标的字段
        if convert_symbol_to_standard(api_sym) != symbol: continue        # 过滤其他标的
        status = str(o.get('status', ''))                                 # 当前状态
        entrust_no = o.get('entrust_no')                                  # 委托号
        if not entrust_no or status != '2' or entrust_no in context.state[symbol]['filled_order_ids'] or entrust_no in cache:
            continue                                                      # 非“已报待成(2)”或已成/已撤/已撤过的跳过
        final_status = get_order_status(entrust_no)                       # 再查最终状态
        if final_status in ('4', '5', '6', '8'): continue                 # 已撤(4/5/6)/已成(8)跳过
        cache.add(entrust_no)                                             # 加入缓存避免重复
        total += 1                                                        # 计数+1
        info('[{}] 👉 发现并尝试撤销遗留挂单 entrust_no={}', symbol, entrust_no)  # 日志
        try: 
            cancel_order_ex({'entrust_no': entrust_no, 'symbol': api_sym})# 发起撤单
        except Exception as e: 
            info('[{}] ⚠️ 撤单异常 entrust_no={}: {}', symbol, entrust_no, e)    # 撤单失败记录
    if total > 0: 
        info('[{}] 共{}笔遗留挂单尝试撤销完毕', symbol, total)                # 汇总日志

def place_auction_orders(context):
    """集合竞价时段的网格挂单（非回测且在竞价或主时段时才执行）"""
    if '回测' in context.env or not (is_auction_time() or is_main_trading_time()): return  # 条件不满足返回
    info('🆕 清空防抖缓存，开始集合竞价挂单')                                 # 提示开始
    for st in context.state.values():                                      # 遍历状态
        st.pop('_last_order_bp', None); st.pop('_last_order_ts', None)     # 清理上次节流缓存
    for sym in context.symbol_list:                                        # 遍历标的
        state = context.state[sym]                                         # 取状态
        adjust_grid_unit(state)                                            # 根据底仓动态放大网格单位
        cancel_all_orders_by_symbol(context, sym)                          # 清空该标的挂单
        context.latest_data[sym] = state['base_price']                     # 竞价前把最新价回置为基准
        place_limit_orders(context, sym, state)                            # 依网格挂限价
        safe_save_state(sym, state)                                        # 保存状态

def place_limit_orders(context, symbol, state):
    """
    限价挂单主函数（集合竞价/主时段用）。本段逻辑来自 VCHATGPT-0708 的结构，含“棘轮”更新。
    关键点：当只买不卖/只卖不买触及对侧价时，先提升/下移基准价，然后再挂新的网格，保证不会“卡住”。
    """
    now_dt = context.current_dt                                           # 当前时间戳对象

    # --- 前置检查 ---
    if state.get('_last_trade_ts') and (now_dt - state['_last_trade_ts']).total_seconds() < 60: return  # 成交后 60s 内不重复挂
    if is_order_blocking_period(): return                                 # 冻结时段不挂
    if not (is_auction_time() or (is_main_trading_time() and now_dt.time() < time(14, 50))): return  # 14:50 以后不再挂新限价
    
    price = context.latest_data.get(symbol)                               # 最新行情快照价
    if not (price and price > 0): return                                  # 无效价格直接返回
    base = state['base_price']                                            # 当前基准价
    if abs(price / base - 1) > 0.10: return                               # 偏离过大（>10%）不挂

    # --- 核心变量 ---
    unit, buy_sp, sell_sp = state['grid_unit'], state['buy_grid_spacing'], state['sell_grid_spacing']  # 网格单位&买卖间距
    buy_p, sell_p = round(base * (1 - buy_sp), 3), round(base * (1 + sell_sp), 3)                      # 计算买/卖网格价（3位小数）

    position = get_position(symbol)                                      # 拉取持仓信息
    pos = position.amount + state.get('_pos_change', 0)                  # 结合临时成交变动得到即时仓位

    # --- 检查“棘轮”触发条件 ---
    is_in_low_pos_range = (pos - unit <= state['base_position'])         # 仓位靠近底部
    ratchet_up = is_in_low_pos_range and price >= sell_p                 # 价格触及卖带且仓位偏低 → 上移基准

    is_in_high_pos_range = (pos + unit >= state['max_position'])         # 仓位接近上限
    ratchet_down = is_in_high_pos_range and price <= buy_p               # 价格触及买带且仓位偏高 → 下移基准

    # --- 常规节流/防抖（棘轮不走节流） ---
    if not (ratchet_up or ratchet_down):                                 # 非棘轮行情
        last_ts = state.get('_last_order_ts')                            # 最近挂单时间
        if last_ts and (now_dt - last_ts).seconds < 30:                  # 30s 内节流
            return
        last_bp = state.get('_last_order_bp')                            # 最近挂单时的基准价
        if last_bp and abs(base / last_bp - 1) < buy_sp / 2:             # 基准价变化不足半格不重挂
            return
        state['_last_order_ts'], state['_last_order_bp'] = now_dt, base  # 记录节流状态

    # --- 棘轮处理：更新基准+撤单+重算网格 ---
    if ratchet_up:
        state['base_price'] = sell_p                                     # 上移基准至卖带
        info('[{}] 棘轮上移: 价格上涨触及卖价，基准价上移至 {:.3f}', symbol, sell_p)  # 打印
        cancel_all_orders_by_symbol(context, symbol)                      # 撤现有挂单
        buy_p, sell_p = round(sell_p * (1 - state['buy_grid_spacing']), 3), round(sell_p * (1 + state['sell_grid_spacing']), 3)  # 重算网格
    elif ratchet_down:
        state['base_price'] = buy_p                                      # 下移基准至买带
        info('[{}] 棘轮下移: 价格下跌触及买价，基准价下移至 {:.3f}', symbol, buy_p)   # 打印
        cancel_all_orders_by_symbol(context, symbol)                      # 撤现有挂单
        buy_p, sell_p = round(buy_p * (1 - state['buy_grid_spacing']), 3), round(buy_p * (1 + state['sell_grid_spacing']), 3)    # 重算网格

    # --- 实际挂单 ---
    try:
        open_orders = [o for o in get_open_orders(symbol) or [] if o.status == '2']  # 取该标的在途挂单
        enable_amount = position.enable_amount                         # 可卖数量（用于校验可卖）

        state.pop('_pos_change', None)                                 # 消费一次临时仓位变更，避免重复计算

        can_buy = not any(o.amount > 0 and abs(o.price - buy_p) < 1e-3 for o in open_orders)  # 没有同价位买单
        if can_buy and pos + unit <= state['max_position']:            # 仓位未超过上限
            info('[{}] --> 发起买入委托: {}股 @ {:.3f}', symbol, unit, buy_p)              # 打印计划买单
            order(symbol, unit, limit_price=buy_p)                      # 下买入限价

        can_sell = not any(o.amount < 0 and abs(o.price - sell_p) < 1e-3 for o in open_orders) # 没有同价位卖单
        if can_sell and enable_amount >= unit and pos - unit >= state['base_position']:        # 可卖充足且不低于底仓
            info('[{}] --> 发起卖出委托: {}股 @ {:.3f}', symbol, unit, sell_p)              # 打印计划卖单
            order(symbol, -unit, limit_price=sell_p)                     # 下卖出限价

    except Exception as e:
        info('[{}] ⚠️ 限价挂单异常：{}', symbol, e)                        # 捕获异常
    finally:
        safe_save_state(symbol, state)                                    # 最后保存状态

def on_trade_response(context, trade_list):
    """成交回报处理：落地日志、去重、转调 on_order_filled"""
    for tr in trade_list:                                                 # 遍历成交列表
        if str(tr.get('status')) != '8': continue                         # 非已成(8)跳过
        sym = convert_symbol_to_standard(tr['stock_code'])                # 转换为标准后缀
        entrust_no = tr['entrust_no']                                     # 获取委托号
        log_trade_details(context, sym, tr)                               # 记录到成交明细 CSV
        if sym not in context.state or entrust_no in context.state[sym]['filled_order_ids']: continue  # 状态缺失或已处理跳过
        context.state[sym]['filled_order_ids'].add(entrust_no)            # 将该委托号加入已成集合
        safe_save_state(sym, context.state[sym])                          # 保存状态
        order_obj = SimpleNamespace(                                      # 构造简化的订单对象
            order_id = entrust_no,
            amount   = tr['business_amount'] if tr['entrust_bs']=='1' else -tr['business_amount'], # 买为正卖为负
            filled   = tr['business_amount'],                              # 成交数量
            price    = tr['business_price']                                # 成交价格
        )
        try:
            on_order_filled(context, sym, order_obj)                       # 调用成交后处理
        except Exception as e:
            info('[{}] ❌ 成交处理失败：{}', sym, e)                        # 捕获异常

def on_order_filled(context, symbol, order):
    """单笔订单成交后的内务处理：更新基准价、撤单、必要时立即重挂"""
    state = context.state[symbol]                                         # 取状态
    if order.filled == 0: return                                          # 0 成交保护
    last_dt = state.get('_last_fill_dt')                                  # 上次成交时间
    if state.get('last_fill_price') == order.price and last_dt and (context.current_dt - last_dt).seconds < 5:
        return                                                            # 短时间内相同价重复回报，忽略
    trade_direction = "买入" if order.amount > 0 else "卖出"               # 方向字符串
    info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f}', symbol, trade_direction, order.filled, order.price)  # 打印成交
    state['_last_trade_ts'] = context.current_dt                          # 记录最近成交时间
    state['_last_fill_dt'] = context.current_dt                           # 记录最近回报时间
    state['last_fill_price'] = order.price                                # 记录最近成交价
    state['base_price'] = order.price                                     # 将基准价更新为成交价（保持网格随成交对齐）
    info('[{}] 🔄 成交后基准价更新为 {:.3f}', symbol, order.price)           # 打印更新
    state['_pos_change'] = order.amount                                   # 暂存仓位变化（下一次挂单消费）
    cancel_all_orders_by_symbol(context, symbol)                          # 撤掉旧挂单
    if is_order_blocking_period():                                        # 若处于冻结期
        info('[{}] 处于9:25-9:30挂单冻结期，成交后仅更新状态，推迟挂单至9:30后。', symbol)  # 打印提示
    elif context.current_dt.time() < time(14, 50):                        # 若在 14:50 前
        place_limit_orders(context, symbol, state)                         # 立即按新基准补挂
    context.should_place_order_map[symbol] = True                          # 允许下一次市价触发
    safe_save_state(symbol, state)                                         # 保存状态

def handle_data(context, data):
    """分钟级主循环：更新行情、动态参数、在特定时间触发市价兜底等"""
    now_dt = context.current_dt                                            # 当前时刻
    now = now_dt.time()                                                    # 当前时间

    if now_dt.minute % 5 == 0 and now_dt.second < 5:                       # 每 5 分钟
        reload_config_if_changed(context)                                   # 热重载配置
        generate_html_report(context)                                       # 刷新 HTML 看板
    
    # === 实时价格缓存：改为每轮覆盖，保证最新 ===
    context.latest_data = {                                                # 构造最新价字典
        sym: data[sym].price                                               # 从 data 取 price
        for sym in context.symbol_list
    }

    # 动态调整网格与 VA 目标
    for sym in context.symbol_list:                                        # 遍历标的
        if sym not in context.state: continue                              # 无状态跳过
        st = context.state[sym]                                            # 取状态
        price = context.latest_data.get(sym)                               # 最新价
        if not price: continue                                             # 无价跳过
        get_target_base_position(context, sym, st, price, now_dt)          # 更新底仓/最大仓位（VA）
        adjust_grid_unit(st)                                               # 根据底仓放大网格
        if now_dt.minute % 30 == 0 and now_dt.second < 5:                  # 每 30 分钟
            update_grid_spacing_final(context, sym, st, get_position(sym).amount)  # 动态网格间距

    # 竞价/主时段限价挂网格（14:50 前）
    if is_auction_time() or (is_main_trading_time() and now < time(14, 50)):
        for sym in context.symbol_list:                                    # 遍历标的
            if sym in context.state: 
                place_limit_orders(context, sym, context.state[sym])       # 执行挂单

    # ===【关键】14:55 起的市价兜底触发（含保护价逻辑）===
    if time(14, 55) <= now < time(14, 57):                                 # 收盘前两分钟窗口
        for sym in context.symbol_list:                                    # 遍历所有标的
            if sym in context.state:
                place_market_orders_if_triggered(context, sym, context.state[sym])  # 触发市价下单（新增保护价）

    # 每 30 分钟巡检日志
    if now_dt.minute % 30 == 0 and now_dt.second < 5:
        info('📌 每30分钟状态巡检...')                                      # 打印提示
        for sym in context.symbol_list:                                    # 遍历标的
            if sym in context.state:
                log_status(context, sym, context.state[sym], context.latest_data.get(sym))  # 打印关键信息

# ===【新增】工具函数：判断是否沪市、给出 tick、构造保护价，并带一次性+1tick 重试 ===
def _is_shanghai(symbol: str) -> bool:
    """判断是否沪市 .SS 标的"""
    return isinstance(symbol, str) and symbol.endswith('.SS')              # 以后缀判断

def _get_tick_size(context, symbol: str) -> float:
    """获取该标的最小变动价位（默认 0.001；可在参数存储覆盖全局）"""
    return float(get_saved_param(f'tick_size_{symbol}', context.protect_tick_size))  # 支持 per-symbol 覆盖

def _market_order_with_protect(context, symbol: str, qty: int, 
                               side: str,  # 'BUY' or 'SELL'
                               price_snapshot: float, 
                               buy_p: float, sell_p: float,
                               state: dict) -> bool:
    """
    统一的市价下单封装：
    - 深市：直接走纯市价（与旧行为一致）；
    - 沪市：必须带保护价；若被拒或未成可放宽 +1 tick 重试一次；
    返回 True 表示已成功发出订单请求（不保证立刻成），False 表示两次均失败。
    """
    is_ss = _is_shanghai(symbol)                                           # 是否沪市
    tick = _get_tick_size(context, symbol)                                 # 取 tick
    pticks = max(0, int(context.protect_ticks))                            # 保护 tick 数（非负）
    retry_enabled = bool(context.protect_retry_enabled)                    # 是否允许重试

    # 计算保护价（贴近网格价 ± N tick）
    if side == 'BUY':
        protect = round(buy_p + pticks * tick, 3)                          # 买单保护上限价
    else:
        protect = round(sell_p - pticks * tick, 3)                         # 卖单保护下限价

    # 打印触发三元组日志（触发价/网格价/保护价）
    if side == 'BUY':
        info('[{}] 市价买触发: {}股 触发价={:.3f} 网格买={:.3f} 保护价={:.3f}{}', 
             symbol, abs(qty), price_snapshot, buy_p, protect, '（沪市需保护）' if is_ss else '')
    else:
        info('[{}] 市价卖触发: {}股 触发价={:.3f} 网格卖={:.3f} 保护价={:.3f}{}', 
             symbol, abs(qty), price_snapshot, sell_p, protect, '（沪市需保护）' if is_ss else '')

    # 根据交易所分别调用
    try:
        if is_ss:
            # 沪市：必须带保护价，否则券商拒绝；沿用原 market_type='0'
            order_market(symbol, qty if side=='BUY' else -abs(qty), market_type='0', limit_price=protect)
            return True                                                   # 请求已发出
        else:
            # 深市：保持纯市价（不带保护价），与旧行为一致；若你希望统一控制滑点，可在后续版本改为也带保护价。
            order_market(symbol, qty if side=='BUY' else -abs(qty), market_type='0')
            return True                                                   # 请求已发出
    except Exception as e:
        # 首次失败（多见于沪市缺保护或过紧未接受），可选择放宽 1 tick 再试一次
        err = str(e)
        info('[{}] ⚠️ 市价下单异常：{}', symbol, err)                     # 打印异常
        if not is_ss or not retry_enabled:                                # 仅沪市且允许重试才进入
            return False                                                  # 直接失败
        # 放宽 1 tick
        if side == 'BUY':
            protect_retry = round(protect + tick, 3)                      # 买：再抬 1 tick
        else:
            protect_retry = round(protect - tick, 3)                      # 卖：再降 1 tick
        info('[{}] 🔁 保护价放宽+1tick 重试: 新保护价={:.3f}', symbol, protect_retry)  # 打印重试信息
        try:
            order_market(symbol, qty if side=='BUY' else -abs(qty), market_type='0', limit_price=protect_retry)  # 带放宽保护价重试
            return True                                                   # 重试已发出
        except Exception as e2:
            info('[{}] ❌ 重试仍失败：{}（放弃，不占资）', symbol, e2)          # 二次失败放弃
            return False                                                  # 返回失败
# ===【新增 end】===

def place_market_orders_if_triggered(context, symbol, state):
    """
    收盘前 14:55~14:57 的“市价兜底”：
    - 仅当价格触及网格买/卖价才触发；
    - 深市：保持纯市价；沪市：带保护限价（贴网格±N tick），必要时放宽 1 tick 重试一次；
    - 成功发出后，基准价仍按原逻辑对齐对应网格价（buy_p/sell_p）。
    """
    if not is_main_trading_time(): return                                  # 非主时段返回
    price = context.latest_data.get(symbol)                                 # 最新快照价
    if not (price and price > 0): return                                    # 无效价返回
    base = state['base_price']                                              # 当前基准
    if abs(price/base - 1) > 0.10: return                                   # 超 10% 偏离保护

    adjust_grid_unit(state)                                                 # 根据底仓动态放大网格单位
    pos, unit = get_position(symbol).amount, state['grid_unit']             # 读取持仓与网格单位
    buy_p, sell_p = round(base * (1 - state['buy_grid_spacing']), 3), round(base * (1 + state['sell_grid_spacing']), 3)  # 计算网格价

    if not context.should_place_order_map.get(symbol, True): return         # 防抖：若上一轮已触发则跳过

    try:
        # === 买触发：快照价 ≤ 买网格价 且加一笔不会超上限 ===
        if price <= buy_p and pos + unit <= state['max_position']:
            # 记录“市价买触发”的详细日志在 _market_order_with_protect 内完成
            ok = _market_order_with_protect(context, symbol, unit, 'BUY', price, buy_p, sell_p, state)  # 调用统一封装
            if ok: 
                state['base_price'] = buy_p                                   # 触发后先把基准对齐到买带（与旧逻辑一致）
        # === 卖触发：快照价 ≥ 卖网格价 且不跌破底仓 ===
        elif price >= sell_p and pos - unit >= state['base_position']:
            ok = _market_order_with_protect(context, symbol, unit, 'SELL', price, buy_p, sell_p, state) # 调用统一封装
            if ok:
                state['base_price'] = sell_p                                  # 触发后把基准对齐到卖带
    except Exception as e:
        info('[{}] ⚠️ 市价挂单异常：{}', symbol, e)                          # 捕获异常
    finally:
        context.should_place_order_map[symbol] = False                        # 本分钟只触发一次
        safe_save_state(symbol, state)                                        # 保存状态

def log_status(context, symbol, state, price):
    """打印单标的状态巡检信息（便于日内观察）"""
    if not price: return                                                     # 无价不打
    pos = get_position(symbol)                                               # 持仓对象
    pnl = (price - pos.cost_basis) * pos.amount if pos.cost_basis > 0 else 0 # 浮盈计算
    info("📊 [{}] 状态: 价:{:.3f} 持仓:{}(可卖:{}) / 底仓:{} 成本:{:.3f} 盈亏:{:.2f} 网格:[买{:.2%},卖{:.2%}]",
         symbol, price, pos.amount, pos.enable_amount, state['base_position'], pos.cost_basis, pnl, state['buy_grid_spacing'], state['sell_grid_spacing'])  # 逐项打印

def update_grid_spacing_final(context, symbol, state, curr_pos):
    """根据 ATR 和仓位区间动态调整买/卖间距（保留原逻辑）"""
    unit, base_pos = state['grid_unit'], state['base_position']             # 读取变量
    atr_pct = calculate_atr(context, symbol)                                 # 计算 ATR 百分比
    base_spacing = 0.005                                                     # 默认基础间距 0.5%
    if atr_pct is not None:                                                  # 若 ATR 可得
        atr_multiplier = 0.25                                                # ATR 乘数（经验值）
        base_spacing = atr_pct * atr_multiplier                              # ATR 推导的间距
    min_spacing = TRANSACTION_COST * 5                                       # 最小间距保护（费率 5 倍）
    base_spacing = max(base_spacing, min_spacing)                            # 取二者较大
    if curr_pos <= base_pos + unit * 5:                                      # 仓位偏低：买紧卖松
        new_buy, new_sell = base_spacing, base_spacing * 2
    elif curr_pos > base_pos + unit * 15:                                    # 仓位偏高：买松卖紧
        new_buy, new_sell = base_spacing * 2, base_spacing
    else:
        new_buy, new_sell = base_spacing, base_spacing                       # 居中：对称
    max_spacing = 0.03                                                       # 上限 3%
    new_buy = round(min(new_buy, max_spacing), 4)                            # 四舍五入 4 位
    new_sell = round(min(new_sell, max_spacing), 4)                          # 四舍五入 4 位
    if new_buy != state.get('buy_grid_spacing') or new_sell != state.get('sell_grid_spacing'):  # 若发生变化
        state['buy_grid_spacing'], state['sell_grid_spacing'] = new_buy, new_sell               # 写回状态
        info('[{}] 🌀 网格动态调整. ATR({:.2%}) -> 基础间距({:.2%}) -> 最终:[买{:.2%},卖{:.2%}]',
             symbol, (atr_pct or 0.0), base_spacing, new_buy, new_sell)     # 打印调整信息

def calculate_atr(context, symbol, atr_period=14):
    """使用 get_history 计算 ATR（异常有详细日志）"""
    try:
        hist = get_history(atr_period + 1, '1d', ['high','low','close'], security_list=[symbol])  # 拉取历史高低收
        if hist is None or hist.empty or len(hist) < atr_period + 1:                               # 数据不足
            info('[{}] ⚠️ ATR计算失败: get_history未能返回足够的数据。', symbol)           # 打印警告
            return None                                                                            # 返回 None
        high, low, close = hist['high'].values, hist['low'].values, hist['close'].values          # 提取序列
        trs = [max(h - l, abs(h - pc), abs(l - pc)) for h, l, pc in zip(high[1:], low[1:], close[:-1])]  # TR 计算
        if not trs: return None                                                                     # 防御返回
        atr_value = sum(trs) / len(trs)                                                             # 简单均值
        current_price = context.latest_data.get(symbol, close[-1])                                  # 当前价格
        if current_price > 0:
            return atr_value / current_price                                                        # ATR 百分比
        return None                                                                                 # 防御
    except Exception as e:
        info('[{}] ❌ ATR计算异常: {}', symbol, e)                                                   # 打印异常
        return None                                                                                 # 返回 None

def end_of_day(context):
    """14:55 定时任务：清理挂单、刷新报表、保存状态"""
    info('✅ 日终处理开始...')                                                                    # 开始提示
    after_initialize_cleanup(context)                                                               # 撤掉所有挂单（不占资）
    generate_html_report(context)                                                                   # 刷新 HTML 看板
    for sym in context.symbol_list:                                                                 # 遍历标的
        if sym in context.state:
            safe_save_state(sym, context.state[sym])                                                # 保存状态
            context.should_place_order_map[sym] = True                                              # 重置市价触发允许
    info('✅ 日终保存状态完成')                                                                      # 完成提示

def get_target_base_position(context, symbol, state, price, dt):
    """计算 VA 目标底仓并更新底仓/max 仓位（保留原正确 VA 算法）"""
    weeks = get_trade_weeks(context, symbol, state, dt)                                             # 已交易周数
    target_val = state['initial_position_value'] + sum(state['dingtou_base'] * (1 + state['dingtou_rate'])**w for w in range(1, weeks + 1))  # 目标价值曲线
    if price <= 0: return state['base_position']                                                    # 无效价返回当前底仓
    new_pos = target_val / price                                                                    # 目标份额
    min_base = round(state['initial_position_value'] / state['base_price'] / 100) * 100 if state['base_price'] > 0 else 0  # 初始最低底仓（100股对齐）
    final_pos = round(max(min_base, new_pos) / 100) * 100                                           # 四舍五入到手数
    if final_pos != state['base_position']:                                                         # 若目标发生变化
        current_val = state['base_position'] * price                                                # 现底仓市值
        delta_val = target_val - current_val                                                        # 价值缺口
        info('[{}] 价值平均: 目标底仓从 {} 调整至 {}. (目标市值: {:.2f}, 当前市值: {:.2f}, 市值缺口: {:.2f})', 
             symbol, state['base_position'], final_pos, target_val, current_val, delta_val)         # 打印调整
        state['base_position'] = final_pos                                                          # 写回底仓
        state['max_position'] = final_pos + state['grid_unit'] * 20                                 # 同步最大仓位
    return final_pos                                                                                # 返回新底仓

def get_trade_weeks(context, symbol, state, dt):
    """将本周加入交易周集合，并返回累计交易周数"""
    y, w, _ = dt.date().isocalendar()                                                               # 年-周编号
    key = f"{y}_{w}"                                                                                # 组装键
    if key not in state.get('trade_week_set', set()):                                               # 若本周未出现过
        if 'trade_week_set' not in state: state['trade_week_set'] = set()                           # 初始化集合
        state['trade_week_set'].add(key)                                                            # 加入集合
        state['last_week_position'] = state['base_position']                                        # 记录上周底仓
        safe_save_state(symbol, state)                                                              # 保存状态
    return len(state['trade_week_set'])                                                             # 返回累计周数

def adjust_grid_unit(state):
    """当底仓增长到一定级别时，放大网格单位（减少交易碎片化）"""
    orig, base_pos = state['grid_unit'], state['base_position']                                     # 读取原网格与当前底仓
    if base_pos >= orig * 20:                                                                       # 底仓达 20 格以上
        new_u = math.ceil(orig * 1.2 / 100) * 100                                                   # 网格单位放大 20%
        if new_u != orig:                                                                           # 若发生变化
            state['grid_unit'] = new_u                                                               # 写回新的网格单位
            state['max_position'] = base_pos + new_u * 20                                           # 同步最大仓位
            info('🔧 [{}] 底仓增加，网格单位放大: {}->{}', state.get('symbol',''), orig, new_u)  # 打印调整

def after_trading_end(context, data):
    """平台交易结束时回调（非回测）"""
    if '回测' in context.env: return                                                                 # 回测跳过
    info('⏰ 系统调用交易结束处理')                                                                   # 提示
    update_daily_reports(context, data)                                                               # 刷新日频 CSV
    info('✅ 交易结束处理完成')                                                                       # 完成

def reload_config_if_changed(context):
    """热重载 symbols.json：新增/移除/变更参数即时生效（保留原逻辑）"""
    try:
        current_mod_time = context.config_file_path.stat().st_mtime                                   # 读取修改时间
        if current_mod_time == context.last_config_mod_time: return                                   # 无变化返回
        info('🔄 检测到配置文件发生变更，开始热重载...')                                                # 提示
        context.last_config_mod_time = current_mod_time                                               # 更新时间戳
        new_config = json.loads(context.config_file_path.read_text(encoding='utf-8'))                # 读取新配置
        old_symbols, new_symbols = set(context.symbol_list), set(new_config.keys())                   # 计算增删

        # 处理移除的标的
        for sym in old_symbols - new_symbols:
            info(f'[{sym}] 标的已从配置中移除，将清理其状态和挂单...')                                   # 提示
            cancel_all_orders_by_symbol(context, sym)                                                 # 撤单
            context.symbol_list.remove(sym)                                                           # 移出列表
            if sym in context.state: del context.state[sym]                                           # 删除状态
            if sym in context.latest_data: del context.latest_data[sym]                               # 删除价格缓存

        # 处理新增的标的
        for sym in new_symbols - old_symbols:
            info(f'[{sym}] 新增标的，正在初始化状态...')                                                # 提示
            cfg = new_config[sym]                                                                     # 取配置
            st = {**cfg}                                                                              # 拷贝
            st.update({                                                                               # 初始化状态
                'base_price': cfg['base_price'], 'grid_unit': cfg['grid_unit'],
                'filled_order_ids': set(), 'trade_week_set': set(),
                'base_position': cfg['initial_base_position'],
                'last_week_position': cfg['initial_base_position'],
                'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
                'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,
                'max_position': cfg['initial_base_position'] + cfg['grid_unit'] * 20
            })
            context.state[sym] = st                                                                   # 写入状态
            context.latest_data[sym] = st['base_price']                                               # 初始化价格
            context.symbol_list.append(sym)                                                           # 加入列表

        # 处理参数变更
        for sym in old_symbols.intersection(new_symbols):
            if context.symbol_config[sym] != new_config[sym]:                                         # 若参数变更
                info(f'[{sym}] 参数发生变更，正在更新...')                                              # 提示
                state, new_params = context.state[sym], new_config[sym]                              # 取状态与新参
                state.update({
                    'grid_unit': new_params['grid_unit'],
                    'dingtou_base': new_params['dingtou_base'],
                    'dingtou_rate': new_params['dingtou_rate'],
                    'max_position': state['base_position'] + new_params['grid_unit'] * 20
                })                                                                                    # 写回关键参数
        context.symbol_config = new_config                                                            # 覆盖配置
        info('✅ 配置文件热重载完成！当前监控标的: {}', context.symbol_list)                           # 完成提示
    except Exception as e:
        info(f'❌ 配置文件热重载失败: {e}')                                                            # 打印异常

def update_daily_reports(context, data):
    """为每个标的维护一个 CSV 文件，每日收盘后追加一行（保留原逻辑）"""
    reports_dir = research_path('reports')                                                            # 报表目录
    reports_dir.mkdir(parents=True, exist_ok=True)                                                    # 确保存在
    current_date = context.current_dt.strftime("%Y-%m-%d")                                            # 今天日期
    for symbol in context.symbol_list:                                                                # 遍历标的
        report_file = reports_dir / f"{symbol}.csv"                                                   # 对应 CSV 路径
        state       = context.state[symbol]                                                           # 取状态
        pos_obj     = get_position(symbol)                                                            # 持仓对象
        amount      = getattr(pos_obj, 'amount', 0)                                                   # 当前持仓量
        cost_basis  = getattr(pos_obj, 'cost_basis', state['base_price'])                             # 成本价
        close_price = context.latest_data.get(symbol, state['base_price'])                            # 收盘价估算
        try:
            close_price = getattr(close_price, 'price', close_price)                                  # 兼容对象/数值
        except:
            close_price = state['base_price']                                                         # 失败采用基准
        weeks       = len(state.get('trade_week_set', []))                                            # 已交易周数
        count       = weeks                                                                           # 次数（等于周数）
        d_base      = state['dingtou_base']                                                           # VA 每期资金
        d_rate      = state['dingtou_rate']                                                           # VA 增长率
        invest_should = d_base                                                                        # 当周应投
        invest_actual = d_base * (1 + d_rate) ** weeks                                                # 当周实际投（示例）
        cumulative_invest = sum(d_base * (1 + d_rate) ** w for w in range(1, weeks+1))                # 累计投入
        expected_value = state['initial_position_value'] + d_base * weeks                             # 期望价值（示例）
        last_week_val = state.get('last_week_position', 0) * close_price                              # 上周底仓市值
        current_val   = amount * close_price                                                          # 当前持仓市值
        weekly_return = (current_val - last_week_val) / last_week_val if last_week_val>0 else 0.0     # 周收益率
        total_return  = (current_val - cumulative_invest) / cumulative_invest if cumulative_invest>0 else 0.0  # 总盈亏比
        weekly_bottom_profit = (state['base_position'] - state.get('last_week_position', 0)) * close_price     # 底仓变动盈利
        total_bottom_profit  = state['base_position'] * close_price - state['initial_position_value']         # 底仓累计盈利
        standard_qty    = state['base_position'] + state['grid_unit'] * 5                                      # 参考数量
        intermediate_qty= state['base_position'] + state['grid_unit'] * 15                                     # 中间数量
        added_base      = state['base_position'] - state.get('last_week_position', 0)                          # 近一周新增底仓
        compare_cost    = added_base * close_price                                                              # 对比成本
        profit_all      = (close_price - cost_basis) * amount if cost_basis > 0 else 0                         # 浮动盈亏
        t_quantity = max(0, amount - state['base_position'])                                                   # 可 T 数量
        row = [                                                                                                 # 组装行
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
        is_new = not report_file.exists()                                                                        # 新文件判断
        with open(report_file, 'a', encoding='utf-8', newline='') as f:                                          # 追加写入
            if is_new:                                                                                           # 首次写表头
                headers = [
                    "时间","市价","期数","次数","每期总收益率","盈亏比","应到价值",
                    "当周应投入金额","当周实际投入金额","实际累计投入金额","定投底仓份额",
                    "累计底仓份额","累计底仓价值","每期累计底仓盈利","总累计底仓盈利",
                    "底仓","股票余额","单次网格交易数量","可T数量","标准数量","中间数量",
                    "极限数量","成本价","对比定投成本","盈亏"
                ]
                f.write(",".join(headers) + "\n")                                                               # 写入表头
            f.write(",".join(map(str, row)) + "\n")                                                             # 写入数据行
        info(f'✅ [{symbol}] 已更新每日CSV报表：{report_file}')                                                  # 打印提示

def log_trade_details(context, symbol, trade):
    """记录每一笔成交到 a_trade_details.csv（保留原逻辑）"""
    try:
        trade_log_path = research_path('reports', 'a_trade_details.csv')                                        # 成交明细路径
        is_new = not trade_log_path.exists()                                                                     # 新文件判定
        with open(trade_log_path, 'a', encoding='utf-8', newline='') as f:                                       # 追加写
            if is_new:
                headers = ["time", "symbol", "direction", "quantity", "price", "base_position_at_trade"]        # 表头
                f.write(",".join(headers) + "\n")                                                                # 落地表头
            direction = "BUY" if trade['entrust_bs'] == '1' else "SELL"                                         # 方向
            base_position = context.state[symbol].get('base_position', 0)                                       # 当时底仓
            row = [                                                                                              # 数据行
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                symbol,
                direction,
                str(trade['business_amount']),
                f"{trade['business_price']:.3f}",
                str(base_position)
            ]
            f.write(",".join(row) + "\n")                                                                        # 写入行
    except Exception as e:
        info(f'❌ 记录交易日志失败: {e}')                                                                           # 异常提示

def generate_html_report(context):
    """生成 HTML 看板（保留原逻辑，仅做轻微健壮性处理）"""
    all_metrics = []                                                                                             # 指标列表
    total_market_value = 0                                                                                       # 组合总市值
    total_unrealized_pnl = 0                                                                                     # 组合总浮盈
    for symbol in context.symbol_list:                                                                           # 遍历标的
        if symbol not in context.state: continue                                                                 # 无状态跳过
        state = context.state[symbol]                                                                            # 取状态
        pos = get_position(symbol)                                                                               # 持仓对象
        price = context.latest_data.get(symbol, 0)                                                                # 最新价
        market_value = pos.amount * price                                                                         # 市值
        unrealized_pnl = (price - pos.cost_basis) * pos.amount if pos.cost_basis > 0 else 0                      # 浮盈
        total_market_value += market_value                                                                        # 累加
        total_unrealized_pnl += unrealized_pnl                                                                    # 累加
        atr_pct = calculate_atr(context, symbol)                                                                  # ATR 百分比
        all_metrics.append({                                                                                      # 收集行
            "symbol": symbol,
            "position": f"{pos.amount} ({pos.enable_amount})",
            "cost_basis": f"{pos.cost_basis:.3f}",
            "price": f"{price:.3f}",
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
            .summary-cards {{ display: flex; gap: 20px; justify-content: center; margin-bottom: 30px; flex-wrap: wrap; }}
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
    table_rows = ""                                                                                 # 初始化表格行
    for m in all_metrics:                                                                           # 遍历指标
        pnl_class = "positive" if float(m["unrealized_pnl"].replace(",", "")) >= 0 else "negative"  # 正负颜色
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
        """                                                                                          # 追加一行
    final_html = html_template.format(                                                               # 渲染模板
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total_market_value=f"{total_market_value:,.2f}",
        total_unrealized_pnl=f"{total_unrealized_pnl:,.2f}",
        pnl_class="positive" if total_unrealized_pnl >= 0 else "negative",
        table_rows=table_rows
    )
    try:
        report_path = research_path('reports', 'strategy_dashboard.html')                            # 写入路径
        report_path.write_text(final_html, encoding='utf-8')                                         # 落盘 HTML
    except Exception as e:
        info(f'❌ 生成HTML看板失败: {e}')                                                              # 失败提示
