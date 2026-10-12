"""Offline only: all broker entry points are mocks; no PTRADE connection."""
import ast
import copy
import hashlib
import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

ROOT = Path(__file__).resolve().parents[1]
SYM = 'TEST.SS'
POLICY = dict(enabled=True, reference_assets=1_000_000, cap_at_reference=5000,
              capital_budget_ratio=0.5, unit_budget_fraction=0.02,
              cash_reserve=10_000, fee_buffer_rate=0.001)


def load(version):
    spec = importlib.util.spec_from_file_location('strategy_' + version.replace('.', '_'), ROOT / (version + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.info = lambda *args: None
    module.log = NS(info=lambda *a: None, error=lambda *a: None)
    module.safe_save_state = lambda *a: None
    module.order = lambda *a, **kw: pytest.fail('Unmocked broker order')
    return module


@pytest.fixture
def new():
    return load('3.14.16')


@pytest.fixture
def old():
    return load('3.14.15')


def state(**kw):
    result = dict(symbol=SYM, base_price=10.0, grid_unit=100, max_grid_count=10,
                  base_position=1000, initial_base_position=1000, last_week_position=1000,
                  initial_position_value=10_000.0, dingtou_base=1000.0, dingtou_rate=0.01,
                  trade_week_set=set(), va_enabled=True, buy_stack=[], sell_stack=[],
                  filled_order_ids=set(), history_pnl=123.0, wm_pnl=45.0,
                  _drip_amount=0.0, _drip_remain_weeks=0)
    result.update(kw)
    return result


def context(st, cash=30_000, assets=1_000_000):
    return NS(state={st['symbol']: st}, symbol_list=[st['symbol']],
              current_dt=datetime(2026, 10, 12, 10), symbol_config={},
              portfolio=NS(total_value=assets, available_cash=cash),
              latest_data={st['symbol']: 10.0}, last_valid_price={st['symbol']: 10.0},
              pending_frozen={}, should_place_order_map={}, mark_halted={},
              last_valid_ts={}, pnl_metrics={}, symbol_names={})


def ready(new, **kw):
    st = state(**kw)
    st['_va_budget'] = new._new_va_budget()
    return st


def fingerprint(saved):
    return hashlib.sha256(json.dumps(saved, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


@pytest.mark.parametrize('rate', [0, 0.001, 0.0058])
@pytest.mark.parametrize('initial', [0, 1000])
def test_always_on_same_inputs_as_old(new, old, rate, initial):
    a = ready(new, dingtou_rate=rate, initial_base_position=initial,
              initial_position_value=initial * 10.0, base_position=initial, last_week_position=initial)
    b = copy.deepcopy(a)
    ca, cb = context(a), context(b)
    for week in range(12):
        for price in [10.0, 9.5, 10.4, 8.0]:
            dt = datetime(2026, 10, 12, 10) + timedelta(weeks=week)
            new.get_target_base_position(ca, SYM, a, price, dt)
            old.get_target_base_position(cb, SYM, b, price, dt)
            for key in ['base_position', 'max_position', 'last_week_position', 'initial_position_value', 'trade_week_set']:
                assert a[key] == b[key], (week, price, key)
            expected = sum(1000 * (1 + rate) ** i for i in range(1, week + 2))
            assert a['_va_budget']['total'] == expected


def test_pause_resume_excludes_budget_but_trade_clock_and_drip_continue(new):
    st = ready(new, _drip_amount=200, _drip_remain_weeks=6)
    c = context(st)
    start = c.current_dt
    for week, enabled in enumerate([True, False, False, True, False, True]):
        st['va_enabled'] = enabled
        before = st['base_position']
        new.get_target_base_position(c, SYM, st, 10.0, start + timedelta(weeks=week))
        if not enabled:
            assert st['base_position'] == before
    assert len(st['trade_week_set']) == 6
    assert st['_va_budget']['periods'] == 3
    assert st['_va_budget']['total'] == sum(1000 * 1.01 ** i for i in range(1, 4))
    assert st['initial_position_value'] == 11_200
    assert st['_drip_remain_weeks'] == 0
    assert new._va_report_view(st, start + timedelta(weeks=5))['target'] == 11_200 + st['_va_budget']['total']


def test_same_week_toggles_and_restart(new):
    st = ready(new)
    c = context(st)
    for enabled in [False, True, False, True, True, False, True]:
        st['va_enabled'] = enabled
        new.get_target_base_position(c, SYM, st, 10, c.current_dt)
        saved = json.loads(json.dumps(st, default=lambda x: sorted(x) if isinstance(x, set) else x))
        new._init_va_budget(st, saved, {})
    assert st['_va_budget']['periods'] == 1
    assert st['_va_budget']['total'] == 1010
    assert len(st['trade_week_set']) == 1


def test_cross_year_multiple_pauses_missing_weeks_and_parameter_change(new):
    st = ready(new)
    c = context(st)
    start = datetime(2026, 12, 28, 10)  # ISO week 2026_53
    for offset, enabled in [(0, True), (1, False), (4, True), (5, False), (7, True)]:
        st['va_enabled'] = enabled
        new.get_target_base_position(c, SYM, st, 10, start + timedelta(weeks=offset))
    assert set(st['_va_budget']['weekly_budgets']) == {'2026_53', '2027_4', '2027_7'}
    original = st['_va_budget']['total']
    st['dingtou_base'] = 2000
    new.get_target_base_position(c, SYM, st, 10, start + timedelta(weeks=8))
    assert st['_va_budget']['total'] == original + 2000 * 1.01 ** 4


def test_legacy_always_on_migration_and_old_startup_repair_equal(new, old):
    saved = json.loads(json.dumps(state(trade_week_set=['2026_1', '2026_2', '2026_3']), default=list))
    st = state(trade_week_set=set(saved['trade_week_set']), base_position=100)
    cfg = {'va_migration': {'mode': 'always_enabled', 'source_sha256': fingerprint(saved), 'evidence': 'verified history'}}
    new._init_va_budget(st, saved, cfg)
    assert st['_va_budget']['total'] == sum(1000 * 1.01 ** i for i in range(1, 4))
    prior = copy.deepcopy(st)
    for m, s in [(new, st), (old, prior)]:
        m.get_position = lambda sym: NS(amount=100, cost_basis=10)
        m._repair_state_logic(context(s))
    assert st['base_position'] == prior['base_position']


@pytest.mark.parametrize('cfg', [{}, {'va_migration': {'mode': 'always_enabled', 'source_sha256': 'bad', 'evidence': 'x'}}])
def test_unconfirmed_old_paused_state_never_recovers_budget(new, cfg):
    saved = dict(trade_week_set=['2026_40', '2026_41'], va_enabled=False, base_position=7600)
    st = state(base_position=7600, last_week_position=7600, trade_week_set=set(saved['trade_week_set']))
    new._init_va_budget(st, saved, cfg)
    c = context(st)
    before = st['base_position']
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    new._repair_state_logic(c)
    assert st['base_position'] == before
    assert new._va_budget_view(st) == (None, None)


def test_verified_paused_migration_preserves_budget_and_growth_index(new):
    saved = dict(trade_week_set=['2026_38', '2026_39', '2026_40', '2026_41'], va_enabled=False)
    st = state(va_enabled=False, trade_week_set=set(saved['trade_week_set']))
    cfg = {'va_migration': {'mode': 'verified_budget', 'source_sha256': fingerprint(saved),
                            'evidence': 'old states and switch logs',
                            'weekly_budgets': {'2026_38': 1010.0, '2026_39': 1020.1}}}
    new._init_va_budget(st, saved, cfg)
    assert st['_va_budget']['excluded_weeks'] == ['2026_40', '2026_41']
    st['va_enabled'] = True
    new.get_target_base_position(context(st), SYM, st, 10, datetime(2026, 10, 12))
    assert st['_va_budget']['periods'] == 3
    assert st['_va_budget']['total'] == pytest.approx(1010 + 1020.1 + 1000 * 1.01 ** 3)


def test_pending_restart_keeps_original_fingerprint_and_can_confirm(new):
    saved = dict(trade_week_set=['2026_40'], va_enabled=False)
    st = state(va_enabled=False, trade_week_set={'2026_40'})
    new._init_va_budget(st, saved, {})
    new.get_target_base_position(context(st), SYM, st, 10, datetime(2026, 10, 9))
    restarted = json.loads(json.dumps(st, default=list))
    cfg = {'va_migration': {'mode': 'verified_budget', 'source_sha256': fingerprint(saved),
                            'evidence': 'verified', 'weekly_budgets': {'2026_40': 1010}}}
    new._init_va_budget(st, restarted, cfg)
    assert st['_va_budget']['status'] == 'ready'
    assert '2026_41' in st['_va_budget']['excluded_weeks']


def test_invalid_budget_blocks_and_does_not_erase_original(new):
    st = ready(new)
    st['_va_budget']['total'] = float('nan')
    new.get_target_base_position(context(st), SYM, st, 10, datetime(2026, 10, 9))
    assert st['base_position'] == 1000
    assert new._va_budget_view(st) == (None, None)


def test_macro_finalize_reset_cooldown_drip_and_no_same_week_duplicate(new, old):
    a = ready(new)
    ca = context(a)
    new.get_target_base_position(ca, SYM, a, 10, ca.current_dt)
    a.update(_drip_amount=100, _drip_remain_weeks=3, buy_stack=[(9, 100)], sell_stack=[(-11, 100)])
    task = dict(status='filled', planned_qty=500, filled_qty=500, filled_cash=5500,
                pre_pos=3000, unreleased_cash=300, drip_weeks=4)
    a['_macro_tp_task'] = task
    b = copy.deepcopy(a)
    cb = context(b)
    new._finalize_macro_tp_task(ca, SYM, a, 11)
    old._finalize_macro_tp_task(cb, SYM, b, 11)
    for key in ['base_position', 'initial_position_value', '_drip_amount', '_drip_remain_weeks',
                'trade_week_set', '_tp_hwm_ratio', '_tp_tier', 'history_pnl', 'wm_pnl', 'buy_stack', 'sell_stack']:
        assert a[key] == b[key]
    assert a['_va_budget']['total'] == 0
    assert a['_va_budget']['cycles'][0]['total'] == 1010
    assert a['_drip_amount'] == 1450
    initial = a['initial_position_value']
    for _ in range(3):
        new.get_target_base_position(ca, SYM, a, 11, ca.current_dt)
    assert a['_va_budget']['periods'] == 0
    assert a['initial_position_value'] == initial + 1450
    new.get_target_base_position(ca, SYM, a, 11, ca.current_dt + timedelta(weeks=1))
    assert a['_va_budget']['periods'] == 1
    assert a['_va_budget']['total'] == 1010
    assert a['initial_position_value'] == initial + 2900
    assert new._get_macro_tp_tracking_guard(ca, SYM, a, pos=NS(amount=2500, cost_basis=10), price=11)['allow_tracking'] is False


def test_save_and_real_init_preserve_new_budget_and_old_accounting(new, tmp_path):
    new.get_research_path = lambda: str(tmp_path)
    new.get_saved_param = lambda *a: {}
    new.set_saved_param = lambda *a: None
    new.get_position = lambda sym: NS(amount=1000)
    st = ready(new, buy_stack=[(9, 200)], archived_buy_anchor=(8, 100))
    new.get_target_base_position(context(st), SYM, st, 10, datetime(2026, 10, 9))
    new.save_state(SYM, st)
    saved = json.loads((tmp_path / 'state' / (SYM + '.json')).read_text())
    c = context(st)
    new.init_symbol_state(c, SYM, dict(base_price=10, initial_base_position=1000))
    resumed = c.state[SYM]
    for key in ['_va_budget', 'history_pnl', 'wm_pnl', 'buy_stack', 'archived_buy_anchor', 'base_position']:
        assert resumed[key] == st[key]
    assert '_va_budget' in saved


def test_hot_reload_switch_params_and_pending_migration(new, tmp_path):
    new.get_research_path = lambda: str(tmp_path)
    st = ready(new)
    c = context(st)
    c.config_file_path = tmp_path / 'symbols.json'
    c.symbol_config = {SYM: {'va_enabled': True}}
    new._load_symbol_names = lambda c: None
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    before = st['_va_budget']['total']
    for i, enabled in enumerate([False, True, False, True]):
        c.config_file_path.write_text(json.dumps({SYM: {'va_enabled': enabled, 'dingtou_base': 2000}}))
        c.last_config_mod_time = -1
        new.reload_config_if_changed(c)
        assert st['va_enabled'] is enabled
        new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    assert st['_va_budget']['total'] == before
    new.get_target_base_position(c, SYM, st, 10, c.current_dt + timedelta(weeks=1))
    assert st['_va_budget']['total'] == before + 2000 * 1.01 ** 2


@pytest.mark.parametrize('symbol', ['159934.SZ', '159545.SZ', '159605.SZ', '513010.SS'])
def test_closed_symbols_missing_config_stay_closed(new, symbol):
    assert new._configured_va_enabled(symbol, {}) is False
    assert new._configured_va_enabled(symbol, {'va_enabled': False}) is False
    assert new._configured_va_enabled(symbol, {'va_enabled': True}) is True  # explicit future instruction


def test_current_enabled_settings_and_generic_saved_off_preserved(new):
    for sym in ['518850.SS', '520810.SS']:
        assert new._configured_va_enabled(sym, {}, {'va_enabled': True}) is True
    assert new._configured_va_enabled(SYM, {}, {'va_enabled': False}) is False


def test_report_pending_and_paused_never_labels_theory_as_actual(new, tmp_path):
    st = ready(new, va_enabled=False)
    c = context(st)
    new.get_research_path = lambda: str(tmp_path)
    new.get_position = lambda sym: NS(amount=1000, cost_basis=9.0)
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    (tmp_path / 'reports').mkdir(exist_ok=True)
    historical = tmp_path / 'reports' / (SYM + '.csv')
    historical.write_text('old data')
    new.update_daily_reports(c, {})
    text = (tmp_path / 'reports' / (SYM + '.va31416.csv')).read_text()
    assert '累计VA预算非成交投入' in text
    assert '实际累计投入' not in text
    assert historical.read_text() == 'old data'
    assert new._va_report_view(st, c.current_dt)['total'] == 0
    st['_va_budget']['status'] = 'pending'
    new.update_daily_reports(c, {})
    assert '待迁移' in (tmp_path / 'reports' / (SYM + '.va31416.csv')).read_text()


def setup_cash(new, cash=30_000, assets=1_000_000, positions=50_000):
    st = ready(new, base_position=100_000, grid_unit=500)
    c = context(st, cash, assets)
    new.StrategyConfig.GRID_LIMITS = dict(POLICY)
    new.get_position = lambda sym: NS(amount=positions, enable_amount=positions, cost_basis=10)
    new.get_open_orders = lambda *a: []
    new.get_order_status = lambda eid: ''
    new.get_order = lambda eid: None
    calls = []
    def order(sym, qty, limit_price):
        calls.append((sym, qty, limit_price))
        return str(len(calls))
    new.order = order
    return st, c, calls


@pytest.mark.parametrize('assets,expected', [(500_000, 2500), (1_000_000, 5000), (2_000_000, 10_000)])
def test_dynamic_assets_and_both_unit_paths(new, assets, expected):
    st, c, _ = setup_cash(new, assets=assets, positions=200_000)
    cap, error = new._grid_capacity_cap(c)
    assert error is None and cap == expected
    new.adjust_grid_unit(st, c)
    assert st['grid_unit'] == new._calc_grid_unit_for_base(st['base_position'], 10, 10, c)
    assert st['grid_unit'] * 10 == expected // 1000 * 1000


def test_liquidity_capacity_constrains_assets_and_gc_not_cash(new):
    st, c, calls = setup_cash(new, cash=0, assets=2_000_000, positions=20_000)
    c.portfolio.reverse_repo_value = 1_000_000  # never read as cash
    c.state['204001.SS'] = state(symbol='204001.SS')
    c.latest_data['204001.SS'] = 100
    cap, error = new._grid_capacity_cap(c)
    assert cap == 4000 and error is None
    new.adjust_grid_unit(st, c)
    assert st['grid_unit'] == 400
    assert new._submit_grid_order(c, SYM, 400, 10) is None
    assert new._submit_grid_order(c, SYM, -400, 10) == '1'
    assert calls == [(SYM, -400, 10)]


def test_multiple_symbols_share_cash_and_actual_price_rounding(new):
    st, c, calls = setup_cash(new, cash=17_000)
    assert new._submit_grid_order(c, SYM, 500, 10.01) == '1'
    assert calls[0][1] == 400  # actual amount <= 5000, not base-price approximation
    assert new._submit_grid_order(c, 'OTHER.SS', 500, 10) == '2'
    assert calls[1][1] == 200
    assert new._submit_grid_order(c, 'THIRD.SS', 500, 10) is None
    assert sum(q * p * 1.001 for _, q, p in calls) <= 7000
    assert st['grid_unit'] == 500


def test_cash_clipping_does_not_bypass_existing_request_duplicate_guard(new):
    _, c, calls = setup_cash(new, cash=17_000)
    assert new._submit_grid_order(c, SYM, 500, 10.01) == '1'
    assert calls[0][1] == 400
    # Its second affordable quantity would differ; the original request is still a duplicate.
    assert new._submit_grid_order(c, SYM, 500, 10.01) is None
    assert len(calls) == 1


def test_existing_frozen_cash_not_subtracted_twice(new):
    _, c, calls = setup_cash(new, cash=15_000)
    new.get_open_orders = lambda *a: [dict(entrust_no='old', amount=1000, price=10, status=2, filled=0)]
    assert new._submit_grid_order(c, SYM, 500, 10) == '1'
    assert calls[0][1] == 400  # 5005 incl fee is greater than 5000 free; frozen10000 is already excluded


def test_api_cash_lag_visible_orders_do_not_create_extra_budget(new):
    _, c, calls = setup_cash(new, cash=17_000)
    new._submit_grid_order(c, SYM, 500, 10)
    # Order became visible before broker cash decreased.
    new.get_open_orders = lambda *a: [dict(entrust_no='1', amount=500, price=10, status=2)]
    new._submit_grid_order(c, 'OTHER.SS', 500, 10)
    assert calls[1][1] == 100
    assert sum(q * p * 1.001 for _, q, p in calls) <= 7000


def test_reflected_freeze_and_terminal_release(new):
    _, c, calls = setup_cash(new, cash=17_000)
    new._submit_grid_order(c, SYM, 500, 10)
    c.portfolio.available_cash = 11_995
    new.get_open_orders = lambda *a: [dict(entrust_no='1', amount=500, price=10, status=2)]
    assert new._grid_cash_available(c, POLICY) == pytest.approx(1995)
    new.get_open_orders = lambda *a: []
    new.get_order_status = lambda eid: '6'  # cancelled
    new.get_order = lambda eid: dict(entrust_no=eid, status=6, amount=500, price=10, filled=0)
    c.portfolio.available_cash = 17_000
    c.current_dt += timedelta(seconds=5)
    assert new._submit_grid_order(c, SYM, 500, 10) == '2'
    assert len(calls) == 2


def test_terminal_fill_cash_lag_cannot_reuse_spent_money(new):
    _, c, calls = setup_cash(new, cash=17_000)
    new._submit_grid_order(c, SYM, 500, 10)
    new.get_order_status = lambda eid: '8'
    # Broker says fully filled while available_cash still shows its old value.
    assert new._submit_grid_order(c, 'OTHER.SS', 500, 10) == '2'
    assert calls[1][1] == 100
    assert sum(q * p * 1.001 for _, q, p in calls) <= 7000


def test_partial_fill_then_cancel_does_not_double_count_frozen_or_spent(new):
    _, c, _ = setup_cash(new, cash=17_000)
    new._submit_grid_order(c, SYM, 500, 10)
    new.get_open_orders = lambda *a: [dict(entrust_no='1', amount=500, price=10, status=7, filled=200)]
    c.portfolio.available_cash = 11_995
    assert new._grid_cash_available(c, POLICY) == pytest.approx(1995)
    assert c._grid_cash_ledger['spent'] == pytest.approx(2002)
    new.get_open_orders = lambda *a: []
    new.get_order_status = lambda eid: '5'
    new.get_order = lambda eid: dict(entrust_no=eid, amount=500, price=10, status=5, filled=200)
    c.portfolio.available_cash = 14_998
    assert new._grid_cash_available(c, POLICY) == pytest.approx(4998)


def test_restart_reconstructs_broker_frozen_without_reusing_it(new):
    _, c, _ = setup_cash(new, cash=17_000)
    new._submit_grid_order(c, SYM, 500, 10)
    c.portfolio.available_cash = 11_995
    del c._grid_cash_ledger
    new.get_open_orders = lambda *a: [dict(entrust_no='1', amount=500, price=10, status=2, filled=0)]
    assert new._grid_cash_available(c, POLICY) == pytest.approx(1995)


@pytest.mark.parametrize('entry', ['auction', 'limit'])
def test_real_grid_entry_tracks_actual_rounded_sell_freeze(new, entry):
    st, c, calls = setup_cash(new, cash=400_000, positions=11_000)
    st.update(base_position=10_000, max_position=15_000, buy_grid_spacing=0.005, sell_grid_spacing=0.005)
    c.env = '模拟'
    c.boot_dt = c.current_dt
    new.is_auction_time = lambda: entry == 'auction'
    new.is_main_trading_time = lambda: True
    new.is_order_blocking_period = lambda: False
    new._fast_cancel_all_orders_global = lambda c: None
    new._ensure_intraday_edge_state = lambda *a: None
    new.update_grid_spacing_final = lambda *a: None
    new._try_merge_back_archived_anchors = lambda *a, **k: None
    new.calculate_grid_atr = lambda *a, **k: 0.02
    new._apply_price_guard = lambda c, s, buy, sell, *args: (buy, sell)
    if entry == 'auction':
        new.place_auction_orders(c)
    else:
        new.place_limit_orders(c, SYM, st)
    assert calls == [(SYM, 500, 9.95), (SYM, -400, 10.05)]
    assert c.pending_frozen[SYM] == 400
    assert st['grid_unit'] == 500
    assert set(st['_fill_tracker']) == {'1', '2'}


def test_out_of_order_dates_do_not_reintroduce_paused_week(new):
    st = ready(new)
    c = context(st)
    st['va_enabled'] = False
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    st['va_enabled'] = True
    new.get_target_base_position(c, SYM, st, 10, c.current_dt + timedelta(weeks=2))
    budget = st['_va_budget']['total']
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    assert st['_va_budget']['total'] == budget
    assert st['_va_budget']['periods'] == 1


def test_macro_reset_cannot_confirm_corrupt_or_pending_migration(new):
    st = ready(new)
    st['_va_budget']['total'] = 1234  # invalid: no matching weekly ledger
    new._reset_va_cycle(st)
    assert st['_va_budget']['status'] == 'pending'
    assert st['_va_budget']['total'] == 0
    assert st['_va_budget']['cycles'][0]['total'] == 1234


def test_strategy_config_invalid_policy_not_silently_reused(new, tmp_path):
    new.get_research_path = lambda: str(tmp_path)
    (tmp_path / 'config').mkdir()
    p = tmp_path / 'config/strategy.json'
    c = context(ready(new))
    p.write_text(json.dumps({'grid_limits': POLICY}))
    new.StrategyConfig._load_strategy_config(c)
    assert new._grid_limit_policy()['enabled'] is True
    p.write_text(json.dumps({'grid_limits': 'invalid'}))
    new.StrategyConfig._load_strategy_config(c, force=True)
    with pytest.raises(ValueError):
        new._grid_limit_policy()
    p.unlink()
    new.StrategyConfig._load_strategy_config(c, force=True)
    assert new._grid_limit_policy() is None


@pytest.mark.parametrize('cash,price', [(10_900, 10), (11_000, 10), (30_000, 6000)])
def test_cash_minimum_lot_and_ceiling_conflicts(new, cash, price):
    st, c, calls = setup_cash(new, cash=cash)
    before = st['grid_unit']
    assert new._submit_grid_order(c, SYM, 500, price) is None
    assert not calls
    assert st['grid_unit'] == before


@pytest.mark.parametrize('bad', ['assets', 'price', 'cash', 'policy', 'orders'])
def test_bad_data_or_policy_blocks_buy_with_reason(new, bad):
    _, c, calls = setup_cash(new)
    messages = []
    new.info = lambda *a: messages.append(a)
    if bad == 'assets': c.portfolio.total_value = float('nan')
    if bad == 'price': c.latest_data[SYM] = float('inf')
    if bad == 'cash': c.portfolio.available_cash = -1
    if bad == 'policy': new.StrategyConfig.GRID_LIMITS['capital_budget_ratio'] = 2
    if bad == 'orders': new.get_open_orders = lambda *a: None
    assert new._submit_grid_order(c, SYM, 500, 10) is None
    assert not calls and messages


def test_uncertain_submission_blocks_later_buys_preserves_existing_error(new):
    _, c, _ = setup_cash(new)
    new.order = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('120162'))
    with pytest.raises(RuntimeError, match='120162'):
        new._submit_grid_order(c, SYM, 500, 10)
    assert c._grid_submit_uncertain is True
    assert new._submit_grid_order(c, 'OTHER.SS', 500, 10) is None


def test_dynamic_disabled_retains_old_units_and_duplicate_guard(new, old):
    st = ready(new)
    c = context(st)
    for price in [0.8, 1.463, 9, 12, 6000]:
        for base in [0, 7600, 37700, 500000]:
            a = copy.deepcopy(st); a.update(base_price=price, base_position=base)
            b = copy.deepcopy(a)
            new.adjust_grid_unit(a, c)
            old.adjust_grid_unit(b)
            assert (a['grid_unit'], a['max_position']) == (b['grid_unit'], b['max_position'])
    calls = []
    new.order = lambda *a, **k: calls.append((a, k)) or 'x'
    assert new._submit_grid_order(c, SYM, 100, 10) == 'x'
    assert new._submit_grid_order(c, SYM, 100, 10) is None
    assert len(calls) == 1


def test_untouched_functions_ast_equal_including_trade_clock():
    def functions(version):
        tree = ast.parse((ROOT / (version + '.py')).read_text())
        return {node.name: ast.dump(node, include_attributes=False) for node in tree.body if isinstance(node, ast.FunctionDef)}
    prior, current = functions('3.14.15'), functions('3.14.16')
    changed = {'save_state', 'init_symbol_state', '_repair_state_logic', 'place_auction_orders',
               'place_limit_orders', '_calc_grid_unit_for_base', '_reanchor_base_after_macro_tp',
               '_finalize_macro_tp_task', 'handle_data', 'get_target_base_position',
               'adjust_grid_unit', 'reload_config_if_changed', 'update_daily_reports', 'generate_html_report'}
    actual = {name for name in prior if prior[name] != current[name]}
    assert actual == changed
    for name in prior.keys() - changed:
        assert current[name] == prior[name]


@pytest.mark.parametrize('fill_price,qty', [(10, 150), (8, 300), (12, -150), (10, -300)])
def test_stack_pairing_and_pnl_unchanged(new, old, fill_price, qty):
    a = ready(new, buy_stack=[(9, 100), (11, 200)], sell_stack=[(-12, 100), (-9, 200)])
    b = copy.deepcopy(a)
    new.process_trade_logic(context(a), SYM, fill_price, qty, count_edge_flow=False)
    old.process_trade_logic(context(b), SYM, fill_price, qty, count_edge_flow=False)
    assert a == b
