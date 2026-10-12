"""Revision coverage: forward audit, isolated cycles and documented PTrade objects."""
import copy
import json
from datetime import timedelta
from types import SimpleNamespace as NS

import pytest

from test_3_14_16 import load, state, context, ready, setup_cash, POLICY, SYM


@pytest.fixture
def new():
    return load('3.14.16')


def test_no_proof_gate_for_normal_legacy_and_hot_parameter_target(new):
    st = state(trade_week_set={'2026_39', '2026_40', '2026_41'})
    saved = json.loads(json.dumps(st, default=list))
    c = context(st)
    new._init_va_budget(st, saved, {}, c)
    assert new._va_budget_view(st) == (3, sum(1000 * 1.01 ** n for n in range(1, 4)))
    st['dingtou_base'] = 2000
    new._va_sync_legacy(st)
    assert new._va_target_value(st) == 10_000 + sum(2000 * 1.01 ** n for n in range(1, 4))


def test_unknown_pause_forward_audit_drip_and_duplicate_observation(new):
    st = state(va_enabled=False, trade_week_set={'2026_40', '2026_41'}, _drip_amount=200, _drip_remain_weeks=4)
    c = context(st)
    saved = json.loads(json.dumps(st, default=list))
    new._init_va_budget(st, saved, {}, c)
    for n in [0, 0, 1, 1]:
        new.get_target_base_position(c, SYM, st, 10, c.current_dt + timedelta(weeks=n))
    pause = st['_va_budget']['pauses'][0]
    assert pause['historical_start_at'] is None and pause['confirmed_start_at'] is None
    assert pause['periods_before_pause'] is None and pause['budget_before_pause'] is None
    assert pause['base_position'] == 1000 and pause['drip_amount'] == 200 and pause['drip_remain_weeks'] == 4
    assert len(pause['skipped_weeks']) == 2
    assert sum(v['drip_released'] for v in pause['skipped_weeks'].values()) == 400
    assert st['_drip_remain_weeks'] == 2 and st['initial_position_value'] == 10_400
    assert st['base_position'] == 1000 and new._va_budget_view(st) == (None, None)
    assert st['_va_budget']['legacy_reference']['theoretical_budget'] > 0  # explicitly audit-only


def test_unknown_resume_anchor_uses_only_base_and_released_drip_once(new):
    st = state(va_enabled=False, trade_week_set={'2026_1', '2026_2'}, initial_position_value=99_000,
               initial_base_position=50_000, _drip_amount=100, _drip_remain_weeks=3)
    c = context(st)
    new._init_va_budget(st, json.loads(json.dumps(st, default=list)), {}, c)
    before = copy.deepcopy(st)
    st['va_enabled'] = True
    new._va_observe_switch(st, c.current_dt, 10)
    assert new._va_target_value(st) == 10_000  # never actual holdings/grid shares, never 99000 old anchor
    assert st['base_position'] == 1000
    for key in ['initial_base_position', 'initial_position_value', 'base_price', 'trade_week_set', '_drip_amount', '_drip_remain_weeks']:
        assert st[key] == before[key]
    for _ in range(3): new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    assert st['_va_budget']['total'] == 1010
    assert new._va_target_value(st) == 11_110
    assert st['initial_position_value'] == 99_100
    assert st['_drip_remain_weeks'] == 2
    assert st['base_position'] <= 1200


def test_same_week_many_pauses_do_not_repeat_period_or_audit_budget(new):
    st = ready(new); c = context(st)
    for enabled in [False, True, False, True, False, True]:
        st['va_enabled'] = enabled
        new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    assert st['_va_budget']['periods'] == 1 and st['_va_budget']['total'] == 1010
    assert new._va_report_view(st, c.current_dt)['skipped_weeks'] == 1
    assert len([e for e in st['_va_budget']['events'] if e['kind'] == 'switch']) == 6


def test_anchor_waits_for_valid_current_price_without_repeated_cycle(new):
    st = state(va_enabled=False, trade_week_set={'2026_1'})
    c = context(st)
    new._init_va_budget(st, json.loads(json.dumps(st, default=list)), {}, c)
    st['va_enabled'] = True
    new.get_target_base_position(c, SYM, st, float('nan'), c.current_dt)
    cycle = st['_va_budget']['cycle_id']
    for enabled in [False, True, True]:
        st['va_enabled'] = enabled
        new.get_target_base_position(c, SYM, st, float('nan'), c.current_dt)
    assert st['_va_budget']['cycle_id'] == cycle and st['base_position'] == 1000
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    assert st['_va_budget']['known'] and st['_va_budget']['periods'] == 1


def test_initialization_price_memory_cannot_be_used_as_fresh_resume_quote(new):
    st = state(va_enabled=False, trade_week_set={'2026_41'})
    c = context(st)
    new._init_va_budget(st, json.loads(json.dumps(st, default=list)), {}, c)
    st['va_enabled'] = True
    c.last_valid_ts[SYM] = None
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    assert not st['_va_budget']['known'] and st['base_position'] == 1000
    cycle = st['_va_budget']['cycle_id']
    c.last_valid_ts[SYM] = c.current_dt
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    assert st['_va_budget']['known'] and st['_va_budget']['cycle_id'] == cycle
    assert st['_va_budget']['periods'] == 1


def test_corrupt_state_is_isolated_and_explicit_recovery_stays_confirmed(new):
    bad = ready(new); bad['_va_budget']['total'] = 99999
    healthy = state(symbol='HEALTHY.SS', trade_week_set={'2026_41'})
    cb, ch = context(bad), context(healthy)
    new._init_va_budget(bad, json.loads(json.dumps(bad, default=list)), {}, cb)
    new._init_va_budget(healthy, json.loads(json.dumps(healthy, default=list)), {}, ch)
    assert new._va_budget_view(bad) == (None, None)
    new.get_target_base_position(ch, healthy['symbol'], healthy, 10, ch.current_dt)
    assert healthy['_va_budget']['periods'] == 2
    bad['va_enabled'] = False; new.get_target_base_position(cb, SYM, bad, 10, cb.current_dt)
    bad['va_enabled'] = True; new.get_target_base_position(cb, SYM, bad, 10, cb.current_dt)
    assert bad['_va_budget']['known'] and bad['_va_budget']['status'] == 'ready'
    budget = copy.deepcopy(bad['_va_budget'])
    new._init_va_budget(bad, json.loads(json.dumps(bad, default=list)), {}, cb)
    assert bad['_va_budget'] == budget


def test_missing_current_drip_not_inferred_or_consumed(new):
    st = state(va_enabled=False, _drip_amount=0, _drip_remain_weeks=4)
    saved = json.loads(json.dumps(st, default=list)); del saved['_drip_amount']
    c = context(st); new._init_va_budget(st, saved, {}, c)
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    assert st['_drip_remain_weeks'] == 4
    assert new._va_report_view(st, c.current_dt)['drip_amount'] is None


def test_missing_current_drip_blocks_only_new_macro_task_without_touching_hwm(new):
    st = ready(new, _tp_hwm_ratio=0.5, _tp_tier=2)
    st['_va_budget']['drip_current_valid'] = False
    c = context(st)
    new.get_position = lambda sym: NS(amount=1000, cost_basis=5)
    assert new._check_macro_take_profit(c, SYM, st, 10, c.current_dt) is False
    assert st['_tp_hwm_ratio'] == 0.5 and st['_tp_tier'] == 2


def test_missing_macro_carry_never_creates_fake_zero_drip_pool(new):
    st = ready(new, _drip_amount=200, _drip_remain_weeks=4)
    st['_macro_tp_task'] = dict(status='pending', filled_qty=100, filled_cash=1000, pre_pos=2000)
    c = context(st)
    new._finalize_macro_tp_task(c, SYM, st, 10)
    assert st['_macro_tp_task']['status'] == 'abnormal'
    assert st['_drip_amount'] == 200 and st['_drip_remain_weeks'] == 4


def test_initial_value_error_does_not_change_other_startup_states(new):
    bad = ready(new, initial_position_value=float('nan'), trade_week_set={'2026_41'})
    healthy = state(symbol='GOOD.SS', trade_week_set={'2026_41'}, base_position=100)
    c = context(bad); c.state[healthy['symbol']] = healthy; c.symbol_list.append(healthy['symbol']); c.latest_data[healthy['symbol']] = 10
    new._init_va_budget(healthy, json.loads(json.dumps(healthy, default=list)), {}, c)
    new.get_position = lambda sym: NS(amount=100, cost_basis=10)
    new._repair_state_logic(c)
    assert healthy['base_position'] > 100
    assert bad['base_position'] == 1000


def test_unreadable_financial_state_does_not_abort_healthy_initialization(new, tmp_path):
    new.get_research_path = lambda: str(tmp_path)
    new.get_saved_param = lambda *a: {}
    new.get_position = lambda sym: NS(amount=1000)
    (tmp_path / 'state').mkdir()
    bad = tmp_path / 'state/BAD.SS.json'; bad.write_text('{truncated')
    c = context(state()); c.symbol_list = ['BAD.SS', SYM]
    assert new.init_symbol_state(c, 'BAD.SS', {'base_price': 10}) is False
    new.init_symbol_state(c, SYM, {'base_price': 10, 'initial_base_position': 1000})
    assert SYM in c.state and 'BAD.SS' not in c.state
    assert c.symbol_list == [SYM] and 'BAD.SS' in c.quarantined_symbols
    assert bad.read_text() == '{truncated'


def test_official_order_id_mapping_deduplicates_account_freeze(new):
    _, c, _ = setup_cash(new, cash=17_000)
    new._submit_grid_order(c, SYM, 500, 10)
    c.portfolio.cash = 11_995
    new.get_open_orders = lambda: [NS(id='1', entrust_no='700001', amount=500, filled=0, limit=10, status='2')]
    new.get_all_orders = lambda: [dict(entrust_no='700001', amount=500, filled_amount=0, price=10, status='2', entrust_bs=1)]
    assert new._grid_cash_available(c, POLICY) == pytest.approx(1995)


@pytest.mark.parametrize('status', ['3', '4', '0', '1', '+', '-', 'V'])
def test_pending_order_status_never_releases_unfilled_reservation(new, status):
    _, c, _ = setup_cash(new, cash=17_000)
    new._submit_grid_order(c, SYM, 500, 10)
    new.get_open_orders = lambda: [NS(id='1', entrust_no='700001', amount=500, filled=200, limit=10, status=status)]
    c.portfolio.cash = 11_995
    assert new._grid_cash_available(c, POLICY) == pytest.approx(1995)
    assert '1' in c._grid_cash_ledger['local']


def test_rejected_order_id_does_not_lock_cash_forever(new):
    _, c, calls = setup_cash(new, cash=17_000)
    new._submit_grid_order(c, SYM, 500, 10)
    new.get_order = lambda eid: [NS(id=eid, entrust_no='700001', amount=500, filled=0, limit=10, status='9')]
    assert new._grid_cash_available(c, POLICY) == 7000
    assert not c._grid_cash_ledger['local']
    assert new._submit_grid_order(c, 'SECOND.SS', 500, 10) == '2'
    assert len(calls) == 2


def test_queued_unseen_buy_is_not_resubmitted_after_short_guard_expires(new):
    _, c, calls = setup_cash(new, cash=30_000)
    new._submit_grid_order(c, SYM, 500, 10)
    c.current_dt += timedelta(seconds=5)
    assert new._submit_grid_order(c, SYM, 500, 10.01) is None
    assert len(calls) == 1


def test_get_order_status_unwraps_list_and_maps_entrust_fallback(new):
    obj = NS(id='ORDER-ID', entrust_no=700001, amount=500, filled=200, limit=10, status='4')
    new.get_order = lambda key: [obj] if key == 'ORDER-ID' else (_ for _ in ()).throw(ValueError('not an id'))
    new.get_orders = lambda: [obj]
    assert new.get_order_status('ORDER-ID') == '4'
    assert new.get_order_status('700001') == '4'


def test_actual_sell_credit_waits_for_cash_and_is_never_readded(new):
    _, c, _ = setup_cash(new, cash=17_000)
    trades = {}
    new.get_trades = lambda: trades
    new._submit_grid_order(c, SYM, 500, 10)
    c.portfolio.cash = 11_995
    trades['SELL-ID'] = [[1001, 700010, '600570.XSHG', '卖', 200, 10, 2000, c.current_dt]]
    assert new._grid_cash_available(c, POLICY) == pytest.approx(1995)
    c.portfolio.cash = 13_990
    assert new._grid_cash_available(c, POLICY) == pytest.approx(3990)


def test_minimum_fee_cannot_cross_cash_safety_line(new):
    _, c, calls = setup_cash(new, cash=11_003)
    assert new._submit_grid_order(c, SYM, 100, 10) is None
    assert not calls  # 1000 notional + assumed 5 minimum fee > 1003 available above reserve


def test_partial_sell_minimum_fee_is_charged_once_per_order(new):
    _, c, _ = setup_cash(new, cash=17_000)
    trades = {}
    new.get_trades = lambda: trades
    new._submit_grid_order(c, SYM, 500, 10)
    trades['SALE'] = [[1, 700010, '600570.XSHG', '卖', 100, 10, 1000, c.current_dt]]
    c.portfolio.cash = 12_990
    assert new._grid_cash_available(c, POLICY) == pytest.approx(2990)
    trades['SALE'].append([2, 700010, '600570.XSHG', '卖', 100, 10, 1000, c.current_dt])
    c.portfolio.cash = 13_990
    assert new._grid_cash_available(c, POLICY) == pytest.approx(3990)
    assert c._grid_cash_ledger['sale_credit'] == 1995
    assert new._grid_cash_available(c, POLICY) == pytest.approx(3990)
    assert c._grid_cash_ledger['sale_credit'] == pytest.approx(1995)
    trades['GC001-ID'] = [[1002, 700011, '204001.XSHG', '卖', 1000, 100, 100000, c.current_dt]]
    assert new._grid_cash_available(c, POLICY) == pytest.approx(3990)


def test_new_va_reports_are_frozen_in_end_of_day_archive(new, tmp_path):
    st = ready(new); c = context(st)
    new.get_research_path = lambda: str(tmp_path)
    new.get_position = lambda sym: NS(amount=1000, cost_basis=10)
    new.set_saved_param = lambda *a: None
    for name in ['state', 'logs', 'config', 'reports']: (tmp_path / name).mkdir()
    (tmp_path / 'config/symbols.json').write_text(json.dumps({SYM: {'va_enabled': True}}))
    (tmp_path / 'logs/2026-10-12_strategy.log').write_text('offline log')
    new.get_target_base_position(c, SYM, st, 10, c.current_dt)
    new.save_state(SYM, st)
    new.update_daily_reports(c, {})
    manifest = new._archive_daily_snapshot(c)
    assert manifest['status'] == 'SUCCESS'
    names = {f['relative_path'] for f in manifest['files']}
    assert 'reports/' + SYM + '.va31416.v2.csv' in names
    assert 'reports/' + SYM + '.va31416.audit.json' in names
    root = tmp_path / 'eod_archive/2026-10-12'
    audit = next(root.rglob(SYM + '.va31416.audit.json'))
    assert json.loads(audit.read_text())['lifecycle']['schema'] == 2
