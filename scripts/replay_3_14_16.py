"""Read-only snapshot replay. Broker APIs are replaced before any strategy calls.

Usage: python scripts/replay_3_14_16.py SNAPSHOT_DIR --output RESULT_JSON
This compares identical input paths; an EOD state cannot reconstruct intraday state.
"""
import argparse
import copy
import csv
import hashlib
import importlib.util
import json
import re
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace as NS

ROOT = Path(__file__).resolve().parents[1]
CONFIRMED_ON = {'159509.SZ', '518850.SS', '159612.SZ', '159655.SZ',
                '159501.SZ', '159696.SZ', '520810.SS', '513850.SS'}
OFF = {'159934.SZ', '159545.SZ', '159605.SZ', '513010.SS'}


def load(version):
    spec = importlib.util.spec_from_file_location('offline_' + version.replace('.', '_'), ROOT / (version + '.py'))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.info = lambda *a: None
    m.log = NS(info=lambda *a: None, error=lambda *a: None)
    m.safe_save_state = lambda *a: None
    m.order = lambda *a, **k: (_ for _ in ()).throw(AssertionError('broker orders forbidden'))
    return m


def run(snapshot):
    manifest = json.loads((snapshot / 'manifest.json').read_text())
    for item in manifest['files']:
        path = (snapshot / item['relative_path']).resolve()
        assert path.is_relative_to(snapshot.resolve())
        assert path.is_file() and path.stat().st_size == item['size'], path.name
    assert manifest['status'] == 'SUCCESS' and not manifest['validation_errors']
    cfg = json.loads((snapshot / 'config/symbols.json').read_text())
    assert set(cfg) == CONFIRMED_ON | OFF
    new, old = load('3.14.16'), load('3.14.15')
    merged = copy.deepcopy(cfg)
    states, final, trajectories, audit, positions = {}, {}, {}, [], {}
    log_path = snapshot / 'logs/2026-10-09_strategy.log'
    log_text = log_path.read_text(encoding='utf-8')  # full file, not a tail/head sample
    pattern = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*?\[([0-9]{6}\.(?:SZ|SS))[^\]]*\] 状态: 价:([0-9.]+)', re.M)
    for timestamp, sym, price in pattern.findall(log_text):
        trajectories.setdefault(sym, []).append((datetime.fromisoformat(timestamp), float(price)))
    for sym, config in cfg.items():
        saved = json.loads((snapshot / 'state' / (sym + '.json')).read_text())
        with (snapshot / 'reports' / (sym + '.csv')).open(encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
        last = rows[-1]
        assert last['时间'] == '2026-10-09'
        price, amount = float(last['市价']), float(last['股票余额'])
        final[sym] = price
        positions[sym] = NS(amount=amount, enable_amount=amount, cost_basis=float(last['成本价']))
        st = copy.deepcopy(saved)
        st['trade_week_set'] = set(st['trade_week_set'])
        st['filled_order_ids'] = set(st['filled_order_ids'])
        digest = hashlib.sha256(json.dumps(saved, sort_keys=True, ensure_ascii=True).encode()).hexdigest()
        merged[sym]['va_enabled'] = sym in CONFIRMED_ON
        if sym in CONFIRMED_ON:
            merged[sym]['va_migration'] = dict(mode='always_enabled', source_sha256=digest,
                                             evidence='用户2026-10-12确认：本标的当前止盈周期内一直开启，无暂停')
        new._init_va_budget(st, saved, merged[sym])
        states[sym] = st
        source_weeks = len(saved['trade_week_set'])
        legacy_budget = sum(saved['dingtou_base'] * (1 + saved['dingtou_rate']) ** i for i in range(1, source_weeks + 1))
        old_st = copy.deepcopy(st)
        new_st = copy.deepcopy(st)
        if sym in OFF:
            assert new._va_budget_view(st) == (None, None)
        else:
            assert new._va_budget_view(st) == (source_weeks, legacy_budget)
        # Feed observed status prices into identical EOD seeds: differential simulation, not intraday reconstruction.
        samples = trajectories.get(sym, [(datetime(2026, 10, 9, 15), price)])
        for dt, observed in samples:
            for m, target in [(new, new_st), (old, old_st)]:
                c = NS(current_dt=dt, state={sym: target})
                m.get_target_base_position(c, sym, target, observed, dt)
            assert new_st['base_position'] == old_st['base_position'], (sym, dt)
            assert new_st['initial_position_value'] == old_st['initial_position_value']
        audit.append(dict(symbol=sym, enabled=st['va_enabled'], source_weeks=source_weeks,
                          base_position=st['base_position'], migration=st['_va_budget']['status'],
                          source_sha256=digest, baseline_theoretical_budget=round(legacy_budget, 2),
                          confirmed_budget=None if sym in OFF else round(st['_va_budget']['total'], 2),
                          logged_price_samples=len(samples), differential_base_matches=True))
    policy = json.loads((ROOT / 'config/examples/grid_limits.recommended.json').read_text())['grid_limits']
    policy['enabled'] = True  # sensitivity simulation only; never writes runtime configuration
    new.StrategyConfig.GRID_LIMITS = policy
    new.get_position = lambda sym: positions.get(sym, NS(amount=0))
    scenario_results = []
    for assets, cash in [(500_000, 0), (1_000_000, 25_000), (2_000_000, 100_000)]:
        c = NS(state=copy.deepcopy(states), symbol_list=list(states), latest_data=dict(final),
               portfolio=NS(total_value=assets, available_cash=cash), current_dt=datetime(2026, 10, 12, 10))
        cap, error = new._grid_capacity_cap(c)
        assert error is None
        calls = []
        # These are local recordings, with deliberately lagging broker cash/order mocks.
        new.order = lambda sym, qty, limit_price: calls.append((sym, qty, limit_price)) or str(len(calls))
        new.get_open_orders = lambda *a: []
        new.get_order_status = lambda eid: ''
        result = []
        for sym, st in c.state.items():
            new.adjust_grid_unit(st, c)
            buy_eid = new._submit_grid_order(c, sym, st['grid_unit'], final[sym])
            sell_eid = new._submit_grid_order(c, sym, -st['grid_unit'], final[sym])
            result.append(dict(symbol=sym, unit=st['grid_unit'], buy_sent=bool(buy_eid), sell_sent=bool(sell_eid)))
        buy_cost = sum(q * p * (1 + policy['fee_buffer_rate']) for _, q, p in calls if q > 0)
        assert buy_cost <= max(0, cash - policy['cash_reserve']) + 1e-6
        assert all(abs(q) % 100 == 0 and 1000 <= abs(q) * p <= cap + 1e-6 for _, q, p in calls)
        scenario_results.append(dict(hypothetical_assets=assets, hypothetical_available_cash=cash,
                                     capacity_cap=round(cap, 2), allocated_buy_cost=round(buy_cost, 2),
                                     orders=result))
    summary = dict(snapshot_version=manifest['strategy_version'], files_size_verified=len(manifest['files']),
                   log_lines=len(log_text.splitlines()), symbols=audit,
                   limitations=['仅10/9当日日志，关闭标的首次暂停历史未确认',
                                'dashboard_data.json是2025-09-26旧数据，不参与计算',
                                'EOD种子+日志价格为同输入差分模拟，非完整历史回测、非实盘验证',
                                '账户总资产/即时现金不在快照内；动态上限场景为假设敏感性分析'],
                   dynamic_scenarios=scenario_results)
    return summary, merged


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--migration-config-output', type=Path)
    args = parser.parse_args()
    summary, merged = run(args.snapshot)
    args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    if args.migration_config_output:
        args.migration_config_output.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + '\n')
    print('PASS: snapshot integrity, confirmed-VA migration, identical-input differential simulation, shared-cash sensitivity checks')
