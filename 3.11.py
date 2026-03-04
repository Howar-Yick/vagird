# event_driven_grid_strategy.py
# 版本号：GEMINI-3.11.0
#
# 更新日志 (v3.11.0):
# 1. 【活性破局】创设 影子棘轮 (Ghost Ratchet) 机制，彻底根治网格钝化: 
#    - 核心痛点：此前当挂单价被“守门员”强行抬高或压低时，基准价(base_price)会停滞不前，导致即便现价已突破理论网格线，系统依然“装死”，造成资金利用率归零。
#    - 破局逻辑：当市价突破【理论网格价】，且该方向被守门员拦截时，触发“影子跟随”。物理挂单虽未成交，但系统的灵魂基准价（base_price）立刻贴合市价移动。
# 2. 【双擎联动】配合 MAX_STACK_SIZE 引发化骨绵掌:
#    - 影子棘轮的移动会带动反向网格逼近现价，促成高频“步步紧逼”式成交。这种高频成交会迅速撑爆容量上限 (MAX_STACK_SIZE)，从而极其高效地触发“堆栈加权融合”，以兵不血刃的方式将历史套牢极值快速拉平。
# 3. (v3.10.0 回顾): 引入 堆栈容量上限 (Max Stack Size) 空间融合裁剪机制。

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
__version__ = 'GEMINI-3.11.0'

# ---------------- 配置管理类 ----------------

class StrategyConfig:
    """
    策略静态配置类：收拢所有硬编码参数，支持从文件动态加载覆盖。
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

    # --- 市场/风控配置 ---
    MARKET = SimpleNamespace()
    MARKET.HALT_SKIP_PLACE = True
    MARKET.HALT_SKIP_AFTER_SEC = 180
    MARKET.HALT_LOG_EVERY_MIN = 10

    # [v3.8 新增] 天地锁破锁阈值 (ATR 的倍数)
    MARKET.UNLOCK_ATR_MULTIPLIER = 5.0
    
    # [v3.10 新增] 堆栈容量上限，防止碎片化和慢牛/慢熊死锁
    MARKET.MAX_STACK_SIZE = 5
    
    # --- 启动配置 ---
    BOOT = SimpleNamespace()
    BOOT.GRACE_SECONDS = 180

    @classmethod
    def load(cls, context):
        """
        加载所有配置文件并覆盖默认参数。
        """
        cls._load_debug_config(context)
        cls._load_va_config(context)
        cls._load_market_config(context)
        cls._load_strategy_config(context)
        
        # 将关键参数注入到 context 以便兼容旧代码习惯
        context.delay_after_cancel_seconds = cls.DEBUG.DELAY_AFTER_CANCEL
        
    @classmethod
    def _load_debug_config(cls, context):
        cfg_file = research_path('config', 'debug.json')
        if not cls._check_mtime(context, 'debug_cfg_mtime', cfg_file): return
        
        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if 'enable_debug_log' in j: cls.DEBUG.ENABLE = bool(j['enable_debug_log'])
            if 'rt_heartbeat_window_sec' in j: cls.DEBUG.RT_WINDOW_SEC = max(5, int(j['rt_heartbeat_window_sec']))
            if 'delay_after_cancel_seconds' in j: cls.DEBUG.DELAY_AFTER_CANCEL = max(0.0, float(j['delay_after_cancel_seconds']))
            info('⚙️ [Config] Debug配置已更新')
        except Exception as e:
            pass

    @classmethod
    def _load_va_config(cls, context):
        cfg_file = research_path('config', 'va.json')
        if not cls._check_mtime(context, 'va_cfg_mtime', cfg_file): return

        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if 'value_threshold_k' in j: cls.VA.THRESHOLD_K = float(j['value_threshold_k'])
            if 'max_updates_per_day' in j: cls.VA.MAX_UPDATES_PER_DAY = int(j['max_updates_per_day'])
            info('⚙️ [Config] VA配置已更新')
        except Exception as e:
            pass

    @classmethod
    def _load_market_config(cls, context):
        cfg_file = research_path('config', 'market.json')
        if not cls._check_mtime(context, 'market_cfg_mtime', cfg_file): return

        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            if 'halt_skip_place' in j: cls.MARKET.HALT_SKIP_PLACE = bool(j['halt_skip_place'])
            if 'halt_skip_after_seconds' in j: cls.MARKET.HALT_SKIP_AFTER_SEC = int(j['halt_skip_after_seconds'])
            if 'unlock_atr_multiplier' in j: cls.MARKET.UNLOCK_ATR_MULTIPLIER = float(j['unlock_atr_multiplier'])
            # [v3.10 新增] 支持从 market.json 热重载容量上限
            if 'max_stack_size' in j: cls.MARKET.MAX_STACK_SIZE = int(j['max_stack_size'])
            
            info('⚙️ [Config] Market配置已更新')
        except Exception as e:
            pass

    @classmethod
    def _load_strategy_config(cls, context):
        cfg_file = research_path('config', 'strategy.json')
        if not cls._check_mtime(context, 'strategy_cfg_mtime', cfg_file): return

        try:
            j = json.loads(cfg_file.read_text(encoding='utf-8'))
            dbg = j.get('debug', {})
            if 'delay_after_cancel_seconds' in dbg: cls.DEBUG.DELAY_AFTER_CANCEL = float(dbg['delay_after_cancel_seconds'])
            if 'credit_limit' in j: cls.CREDIT_LIMIT = int(j['credit_limit'])
            info('⚙️ [Config] Strategy统一配置已加载')
        except Exception as e:
            pass

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
    ids = list(state.get('filled_order_ids', set()))
    state['filled_order_ids'] = set(ids[-StrategyConfig.MAX_SAVED_FILLED_IDS:])
    
    # [v3.6.4 Fix]: 加入 _fill_tracker，确保补录进度持久化
    store_keys = ['symbol', 'base_price', 'grid_unit', 'max_position', 'last_week_position', 'base_position', 
                  'used_atr_rate', 'cached_atr_ema', 'buy_stack', 'sell_stack', 'credit_limit', 
                  'history_pnl', '_fill_tracker'] 
    
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

# ---------------- 初始化状态辅助函数 (3.5.7) ----------------

def init_symbol_state(context, sym, cfg):
    """
    [Global Ver: v3.6.3] [Func Ver: 2.2]
    [Change]: 新增 _fill_tracker 用于成交量核对
    """
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
        'buy_grid_spacing': 0.005,
        'sell_grid_spacing': 0.005,
        'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20),
        'used_atr_rate': saved.get('used_atr_rate', None),
        'cached_atr_ema': saved.get('cached_atr_ema', None),
        'buy_stack': [],
        'sell_stack': [],
        'credit_limit': cfg.get('credit_limit', saved.get('credit_limit', StrategyConfig.CREDIT_LIMIT)),
        '_fill_tracker': saved.get('_fill_tracker', {}), # 新增：订单成交量追踪
        'history_pnl': saved.get('history_pnl', 0.0),
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
        '_pending_ignore_ids': []
    })

    for key in ['buy_stack', 'sell_stack']:
        raw = saved.get(key, [])
        for item in raw:
            if isinstance(item, (list, tuple)):
                st[key].append(tuple(item))
            else:
                st[key].append((item, st['grid_unit']))
        heapq.heapify(st[key])

    for k in ['scale_factor', 'pending_fill_amount']:
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
    [Global Ver: v3.8.0]
    公共风控逻辑：检查库存栈，必要时修正买卖价格。
    引入 bypass_buy_block(VA建仓特权)，允许在额度不足时强行无视历史卖飞单进行按网格价买入。
    返回修正后的 (final_buy_p, final_sell_p)
    """
    final_buy_p, final_sell_p = buy_p, sell_p
    sym = state.get('symbol', 'Unknown')
    
    # 1. 守门员逻辑：买入检查 (防止高位追高接回空单)
    sell_stack = state.get('sell_stack', [])
    if sell_stack:
        # 3.5.7 升级：元组结构 (-price, unit)
        max_sell_price = -sell_stack[0][0] 
        if final_buy_p > max_sell_price:
            credit = state.get('credit_limit', 0)
            if credit <= 0:
                corrected = round(max_sell_price - (max_sell_price * buy_sp), 3)
                if corrected < final_buy_p:
                    # 🌟 v3.8 核心：VA 特权放行
                    if bypass_buy_block:
                        info('[{}] 🛡️ 守门员(买): 触发【VA建仓特权】！无视历史卖飞价({:.3f})，放行挂单: {:.3f}', 
                             dsym(context, sym), max_sell_price, final_buy_p)
                    else:
                        info('[{}] 🛡️ 守门员拦截(买): 防止高位接回. 原:{:.3f} 修正:{:.3f} (栈顶卖价:{:.3f})', 
                             dsym(context, sym), final_buy_p, corrected, max_sell_price)
                        final_buy_p = corrected

    # 2. 守门员逻辑：卖出检查 (防止低位割肉卖出持仓)
    buy_stack = state.get('buy_stack', [])
    if buy_stack:
        # 3.5.7 升级：元组结构 (price, unit)
        min_buy_price = buy_stack[0][0]
        if final_sell_p < min_buy_price:
            credit = state.get('credit_limit', 0)
            if credit <= 0:
                corrected = round(min_buy_price + (min_buy_price * sell_sp), 3)
                if corrected > final_sell_p:
                    info('[{}] 🛡️ 守门员拦截(卖): 防止低位割肉. 原:{:.3f} 修正:{:.3f} (栈顶买价:{:.3f})', 
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
        
        # 复用计算 ATR 
        atr_pct = calculate_atr(context, symbol)
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
                cancel_all_orders_by_symbol(context, symbol)
                
                # 重算
                buy_p = round(theo_sell_p * (1 - buy_sp), 3)
                sell_p = round(theo_sell_p * (1 + sell_sp), 3)
                # 再次守门，传入 VA 特权
                buy_p, sell_p = _apply_price_guard(context, state, buy_p, sell_p, buy_sp, sell_sp, bypass_buy_block)
                
            elif ratchet_down:
                info('[{}] ⚓ 影子棘轮下移(拦截/满仓): 触及理论买价 {:.3f}，基准降至 {:.3f}', dsym(context, symbol), theo_buy_p, theo_buy_p)
                state['base_price'] = theo_buy_p
                cancel_all_orders_by_symbol(context, symbol)
                
                # 重算
                buy_p = round(theo_buy_p * (1 - buy_sp), 3)
                sell_p = round(theo_buy_p * (1 + sell_sp), 3)
                # 再次守门，传入 VA 特权
                buy_p, sell_p = _apply_price_guard(context, state, buy_p, sell_p, buy_sp, sell_sp, bypass_buy_block)

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
    [Global Ver: v3.6.4] [Func Ver: 2.5]
    [Change]: 放行 status='7' (部成) 的回报，防止分笔成交被过滤。
    """
    if not hasattr(context, 'processed_business_ids'):
        context.processed_business_ids = deque(maxlen=2000)
        
    for tr in trade_list:
        # [v3.6.4 Fix]: 放行 '7'(部成) 和 '8'(已成)
        status = str(tr.get('status'))
        if status not in ['7', '8']: continue
        
        bid = str(tr.get('business_id', ''))
        
        # 3. 精准去重逻辑 (Business ID 是分笔成交的唯一标识)
        if bid:
            if bid in context.processed_business_ids: continue
            context.processed_business_ids.append(bid)
        else:
            pass 

        sym = convert_symbol_to_standard(tr['stock_code'])
        log_trade_details(context, sym, tr) 
        
        if sym not in context.state: continue
        state = context.state[sym]

        bs = str(tr.get('entrust_bs')) 
        if bs == '1':
            fill_amount = abs(tr['business_amount']) 
            trade_dir = "买入"
        elif bs == '2':
            fill_amount = -abs(tr['business_amount']) 
            trade_dir = "卖出"
        else: continue
            
        price = tr['business_price']

        # 5. 调用核心逻辑
        process_trade_logic(context, sym, price, fill_amount)
        
        # 6. 更新追踪器
        if '_fill_tracker' not in state: state['_fill_tracker'] = {}
        entrust_no = str(tr['entrust_no'])
        state['_fill_tracker'][entrust_no] = state['_fill_tracker'].get(entrust_no, 0.0) + abs(fill_amount)
        
        info('✅ [{}] 成交回报! 方向: {}, 数量: {}, 价格: {:.3f} (ID:{}, Sts:{})', 
             dsym(context, sym), trade_dir, abs(fill_amount), price, bid[-6:] if bid else 'N/A', status)

        # 7. 更新状态
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
    [Global Ver: v3.6.0] [Func Ver: 2.1]
    [Change]: 适配 v3.6.0，调用 process_trade_logic
    """
    state = context.state[symbol]
    if order.filled == 0: return
    
    # 更新冻结
    if order.amount < 0:
        current_frozen = context.pending_frozen.get(symbol, 0)
        context.pending_frozen[symbol] = max(0, current_frozen - abs(order.filled))

    # 直接调用新核心
    # 注意：order.amount 在 PTrade 回报里可能是正也可能是负，这里我们用 filled (正数) 配合 amount 符号
    real_amount = order.filled if order.amount > 0 else -order.filled
    process_trade_logic(context, symbol, order.price, real_amount)
    
    # info('✅ [{}] 补录成交! 数量: {}, 价格: {:.3f}', dsym(context, symbol), real_amount, order.price)
# ---------------- FILL-RECOVER ----------------

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
    [Global Ver: v3.8.0]
    主动巡检，同步接入 VA特权 判定，防止巡检机制误撤带有特权的买单。
    """
    now_dt = context.current_dt
    # 主动补录逻辑 (Fill Patrol)
    if is_main_trading_time():
        try:
            # 1. 获取该标的的所有当日委托 (包括已成交和未成交)
            all_orders = get_orders(symbol) or []
            tracker = state.get('_fill_tracker', {})
            
            for o in all_orders:
                o_info = OrderUtils.normalize(o)
                eid = o_info['entrust_no']
                filled_qty = o.filled # 实际成交量
                
                # 如果是废单或未成交，跳过
                if filled_qty <= 0: continue
                
                # 检查是否存在记录
                if eid not in tracker:
                    tracker[eid] = float(filled_qty)
                    continue
                    
                processed_qty = tracker[eid]
                delta = filled_qty - processed_qty
                
                # 发现漏单！(实际成交 > 已处理)
                if delta > 0.9: # 忽略浮点误差
                    trade_price = o.trade_price if o.trade_price > 0 else o.price
                    direction = 1 if not OrderUtils.is_sell(o_info) else -1
                    real_amount = delta * direction
                    
                    info('🕵️ [补录] 发现漏单! ID:{} 漏:{} (总成:{} vs 已记:{})', eid, delta, filled_qty, processed_qty)
                    
                    # 补录核心逻辑
                    process_trade_logic(context, symbol, trade_price, real_amount)
                    
                    # 更新账本
                    tracker[eid] = float(filled_qty)
                    state['history_pnl'] = state.get('history_pnl', 0.0) # 触发保存
                    
            state['_fill_tracker'] = tracker
        except Exception as e:
            info('[{}] ⚠️ FillPatrol 异常: {}', dsym(context, symbol), e)

    # 巡检冷却与状态检查
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
        
        # [v3.8 同步升级] -----------------------------------------------
        # 判定 VA 特权，防止巡检系统误撤特权单
        bypass_buy_block = (pos < base_pos + 5 * unit)
        buy_p, sell_p = _apply_price_guard(context, state, buy_p, sell_p, buy_sp, sell_sp, bypass_buy_block)
        # ---------------------------------------------------------------

        # 巡检逻辑同步空间限制
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
            info('[{}] 🛡️ PATROL: 发现缺失订单，准备补挂...', dsym(context, symbol))
            place_limit_orders(context, symbol, state, ignore_cooldown=True)

    except Exception as e:
        info('[{}] ⚠️ PATROL 巡检失败: {}', dsym(context, symbol), e)

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
                    info('[{}]     监测到复牌/行情恢复，开启 {}s 补偿成交检测窗口。', dsym(context, symbol), recover_window_seconds)

    for sym in context.symbol_list:
        if sym not in context.state: continue
        st = context.state[sym]
        price = context.latest_data.get(sym)
        if is_valid_price(price):
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
        info('🧐 每30分钟状态巡检...')
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

# ---------------- 动态网格间距 (Robust EMA + Cache + 10% Filter) ----------------

def calculate_atr(context, symbol, atr_period=14):
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
            state['cached_atr_ema'] = float(last_atr_val)
            if is_valid_price(last_price): current_atr_rate = last_atr_val / last_price
    except Exception as e:
        if StrategyConfig.DEBUG.ENABLE: info('[{}] ATR计算异常: {} (将尝试使用缓存)', dsym(context, symbol), e)
    used_rate = state.get('used_atr_rate')
    if current_atr_rate is not None and current_atr_rate > 0:
        if used_rate is None or abs(current_atr_rate - used_rate) / used_rate > 0.10: state['used_atr_rate'] = current_atr_rate
        return state['used_atr_rate']
    return used_rate

def update_grid_spacing_final(context, symbol, state, curr_pos):
    pos, unit, base_pos = curr_pos, state['grid_unit'], state['base_position']
    atr_pct = calculate_atr(context, symbol)
    base_spacing = 0.005
    if atr_pct is not None and not math.isnan(atr_pct): base_spacing = max(atr_pct * 0.25, StrategyConfig.TRANSACTION_COST * 5)
    thresh_low, thresh_high = 5, 15
    if pos <= base_pos + unit * thresh_low: new_buy, new_sell = base_spacing, base_spacing * 2
    elif pos > base_pos + unit * thresh_high: new_buy, new_sell = base_spacing * 2, base_spacing
    else: new_buy, new_sell = base_spacing, base_spacing
    new_buy, new_sell = round(min(new_buy, 0.03), 4), round(min(new_sell, 0.03), 4)
    if new_buy != state.get('buy_grid_spacing') or new_sell != state.get('sell_grid_spacing'):
        state['buy_grid_spacing'], state['sell_grid_spacing'] = new_buy, new_sell
        info('[{}] 网格动态调整 (ATR={:.2%}) -> [买{:.2%},卖{:.2%}]', dsym(context, symbol), (atr_pct or 0.0), new_buy, new_sell)

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
    y, w, _ = dt.date().isocalendar()
    key = f"{y}_{w}"
    if 'trade_week_set' not in state: state['trade_week_set'] = set()
    if key not in state['trade_week_set']:
        state['trade_week_set'].add(key)
        state['last_week_position'] = state['base_position']
        safe_save_state(symbol, state)
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
            st.update({
                'symbol': sym,
                'base_price': saved.get('base_price', cfg['base_price']),
                'grid_unit': saved.get('grid_unit', cfg['grid_unit']),
                'filled_order_ids': set(saved.get('filled_order_ids', [])),
                'trade_week_set': set(saved.get('trade_week_set', [])),
                'base_position': saved.get('base_position', cfg['initial_base_position']),
                'last_week_position': saved.get('last_week_position', cfg['initial_base_position']),
                'initial_position_value': cfg['initial_base_position'] * cfg['base_price'],
                'buy_grid_spacing': 0.005, 'sell_grid_spacing': 0.005,
                'max_position': saved.get('max_position', saved.get('base_position', cfg['initial_base_position']) + saved.get('grid_unit', cfg['grid_unit']) * 20),
                'used_atr_rate': saved.get('used_atr_rate', None), 'cached_atr_ema': saved.get('cached_atr_ema', None),
                'buy_stack': saved.get('buy_stack', []), 'sell_stack': saved.get('sell_stack', []),
                'credit_limit': cfg.get('credit_limit', saved.get('credit_limit', StrategyConfig.CREDIT_LIMIT)),
                '_pending_ignore_ids': [],
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
                state.update({'grid_unit': new_params['grid_unit'], 'dingtou_base': new_params['dingtou_base'], 'dingtou_rate': new_params['dingtou_rate'], 'max_position': state['base_position'] + new_params['grid_unit'] * 20})
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

def generate_html_report(context):
    all_metrics, total_market_value, total_unrealized_pnl, total_realized_pnl, pnl_metrics, intraday_metrics = [], 0, 0, 0, getattr(context, 'pnl_metrics', {}), getattr(context, 'intraday_metrics', {})
    for symbol in context.symbol_list:
        if symbol not in context.state: continue
        state, position, price = context.state[symbol], get_position(symbol), context.last_valid_price.get(symbol, context.state[symbol]['base_price'])
        if not is_valid_price(price): price = state['base_price']
        market_value = position.amount * price; unrealized_pnl = (price - position.cost_basis) * position.amount if position.cost_basis > 0 else 0
        total_market_value += market_value; total_unrealized_pnl += unrealized_pnl
        sym_pnl, rv_data = pnl_metrics.get(symbol, {}), intraday_metrics.get(symbol, {})
        total_real = sym_pnl.get('total_realized_pnl', 0); total_realized_pnl += total_real
        all_metrics.append({"symbol": symbol, "symbol_disp": dsym(context, symbol, style='long'), "position": f"{position.amount} ({position.enable_amount})", "cost_basis": f"{position.cost_basis:.3f}", "price": f"{price:.3f}", "market_value": f"{market_value:,.2f}", "unrealized_pnl": f"{unrealized_pnl:,.2f}", "realized_grid_pnl": f"{sym_pnl.get('realized_grid_pnl', 0):,.2f}", "realized_base_pnl": f"{sym_pnl.get('realized_base_pnl', 0):,.2f}", "total_realized_pnl": f"{total_real:,.2f}", "total_pnl": f"{(total_real + unrealized_pnl):,.2f}", "pnl_ratio": f"{(unrealized_pnl / (position.cost_basis * position.amount) * 100) if position.cost_basis * position.amount != 0 else 0:.2f}%", "base_position": state['base_position'], "grid_unit": state['grid_unit'], "atr_str": f"{state.get('used_atr_rate'):.2%}" if state.get('used_atr_rate') else "N/A", "rv_str": f"{rv_data.get('rv', 0):.2%}", "efficiency_str": f"{rv_data.get('efficiency', 0):.1f}"})
    try:
        template_file = research_path('config', 'dashboard_template.html')
        html_template = template_file.read_text(encoding='utf-8') if template_file.exists() else "<html><body><h1>Dashboard</h1></body></html>"
        table_rows = ""
        for m in all_metrics:
            table_rows += f"<tr><td>{m['symbol_disp']}</td><td>{m['position']}</td><td>{m['cost_basis']}</td><td>{m['price']}</td><td>{m['market_value']}</td><td class=\"{'positive' if float(m['unrealized_pnl'].replace(',',''))>=0 else 'negative'}\">{m['unrealized_pnl']}</td><td class=\"{'positive' if float(m['unrealized_pnl'].replace(',',''))>=0 else 'negative'}\">{m['pnl_ratio']}</td><td class=\"{'positive' if float(m['realized_grid_pnl'].replace(',',''))>0 else ''}\">{m['realized_grid_pnl']}</td><td>{m['realized_base_pnl']}</td><td class=\"{'positive' if float(m['total_realized_pnl'].replace(',',''))>0 else ''}\">{m['total_realized_pnl']}</td><td class=\"{'positive' if float(m['total_pnl'].replace(',',''))>=0 else 'negative'}\">{m['total_pnl']}</td><td>{m['base_position']}</td><td>{m['grid_unit']}</td><td>{m['atr_str']}</td><td>{m['rv_str']}</td><td>{m['efficiency_str']}</td></tr>"
        research_path('reports', 'strategy_dashboard.html').write_text(html_template.format(update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), total_market_value=f"{total_market_value:,.2f}", total_unrealized_pnl=f"{total_unrealized_pnl:,.2f}", unrealized_pnl_class="positive" if total_unrealized_pnl >= 0 else "negative", total_realized_pnl=f"{total_realized_pnl:,.2f}", realized_pnl_class="positive" if total_realized_pnl >= 0 else "negative", account_total_pnl=f"{(total_realized_pnl + total_unrealized_pnl):,.2f}", total_pnl_class="positive" if (total_realized_pnl + total_unrealized_pnl) >= 0 else "negative", total_realized_grid_pnl="0.00", grid_pnl_class="", total_realized_base_pnl="0.00", base_pnl_class="", table_rows=table_rows), encoding='utf-8')
    except Exception: pass