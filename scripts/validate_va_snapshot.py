"""Offline regression against full redacted real state; never contacts PTrade.

python scripts/validate_va_snapshot.py SNAPSHOT_DIR --output RESULT.json --sanitized-output DIR
"""
import argparse
import copy
import csv
import hashlib
import importlib.util
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace as NS

ROOT = Path(__file__).resolve().parents[1]


def redact_state(data):
    aliases = {}
    def hide(value):
        value = str(value)
        return aliases.setdefault(value, hashlib.sha256(('offline:' + value).encode()).hexdigest()[:32])
    result = copy.deepcopy(data)
    for field in ['filled_order_ids', '_macro_sell_ids', '_pending_ignore_ids']:
        if isinstance(result.get(field), list): result[field] = [hide(v) for v in result[field]]
    if isinstance(result.get('_fill_tracker'), dict):
        result['_fill_tracker'] = {hide(k): v for k, v in result['_fill_tracker'].items()}
    for key in ['_macro_tp_task', '_last_macro_tp_task']:
        task = result.get(key)
        if isinstance(task, dict):
            for field in ['order_id', 'task_id']:
                if task.get(field): task[field] = hide(task[field])
            if isinstance(task.get('excluded_entrust_nos'), list):
                task['excluded_entrust_nos'] = [hide(v) for v in task['excluded_entrust_nos']]
    return result


def module(version, root, positions):
    spec = importlib.util.spec_from_file_location('offline_' + version.replace('.', '_'), ROOT / (version + '.py'))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.info = lambda *a: None
    m.get_research_path = lambda: str(root)
    m.get_saved_param = lambda *a: {}
    m.get_position = lambda sym: positions[sym]
    m.safe_save_state = lambda *a: None
    m.order = lambda *a, **k: (_ for _ in ()).throw(AssertionError('real order forbidden in offline validation'))
    return m


def context(config, dt):
    return NS(current_dt=dt, state={}, symbol_list=list(config), symbol_config=copy.deepcopy(config),
              latest_data={}, last_valid_price={}, should_place_order_map={}, mark_halted={},
              last_valid_ts={}, pending_frozen={})


def validate(snapshot, sanitized_output):
    manifest = json.loads((snapshot / 'manifest.json').read_text())
    assert manifest['status'] == 'SUCCESS' and not manifest['validation_errors']
    for item in manifest['files']:
        p = (snapshot / item['relative_path']).resolve()
        assert p.is_relative_to(snapshot.resolve()) and p.stat().st_size == item['size']
    config = json.loads((snapshot / 'config/symbols.json').read_text())
    dt = datetime.fromisoformat(manifest['trade_date'] + ' 15:30:00')
    saved, positions, prices, hashes = {}, {}, {}, {}
    logs = [(p.name, len(p.read_text(encoding='utf-8').splitlines())) for p in (snapshot / 'logs').glob('*.log')]
    for sym in config:
        raw = json.loads((snapshot / 'state' / (sym + '.json')).read_text())
        hashes[sym] = hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
        saved[sym] = redact_state(raw)
        with (snapshot / 'reports' / (sym + '.csv')).open(encoding='utf-8') as f:
            row = list(csv.DictReader(f))[-1]
        assert row['时间'] == manifest['trade_date']
        prices[sym] = float(row['市价'])
        positions[sym] = NS(amount=float(row['股票余额']), enable_amount=float(row['股票余额']), cost_basis=float(row['成本价']))
    with tempfile.TemporaryDirectory(prefix='va-redacted-') as folder:
        root = Path(folder)
        (root / 'state').mkdir()
        for sym, value in saved.items(): (root / 'state' / (sym + '.json')).write_text(json.dumps(value))
        old, new = module('3.14.15', root, positions), module('3.14.16', root, positions)
        ca, cb = context(config, dt), context(config, dt)
        for sym, cfg in config.items():
            new.init_symbol_state(ca, sym, cfg)
            old.init_symbol_state(cb, sym, cfg)
        initial_states = copy.deepcopy(ca.state)
        new._repair_state_logic(ca)
        old._repair_state_logic(cb)
        result = []
        for sym in config:
            a, b = ca.state[sym], cb.state[sym]
            for k in ['base_position', 'initial_base_position', 'initial_position_value', 'history_pnl', 'wm_pnl',
                      'buy_stack', 'sell_stack', '_fill_tracker', '_macro_tp_task', '_last_macro_tp_task']:
                assert a[k] == b[k], (sym, k)
            n = len(b['trade_week_set'])
            budget = sum(b['dingtou_base'] * (1 + b['dingtou_rate']) ** w for w in range(1, n + 1))
            view = new._va_report_view(a, dt)
            if a['va_enabled']:
                assert view['periods'] == n and view['total'] == budget
                assert view['target'] == b['initial_position_value'] + budget
            else:
                assert view['total'] is None and a['_va_budget']['pauses'][0]['historical_start_at'] is None
            entry = dict(symbol=sym, va_enabled=a['va_enabled'], old_trade_weeks=n, effective_periods=view['periods'],
                         baseline_theoretical_budget=round(budget, 2), baseline_target=round(b['initial_position_value'] + budget, 2),
                         new_effective_budget=view['total'], new_target=view['target'], base_position=a['base_position'],
                         drip_amount=a['_drip_amount'], drip_remain=a['_drip_remain_weeks'], source_sha256=hashes[sym])
            # Same week and next effective trading week, three price scenarios; compare actual paths, not syntax.
            for offset in [0, 7]:
                for factor in [1.0, 0.9, 1.1]:
                    when, price = dt + timedelta(days=offset), prices[sym] * factor
                    new.get_target_base_position(ca, sym, a, price, when)
                    old.get_target_base_position(cb, sym, b, price, when)
                    for k in ['base_position', 'last_week_position', 'max_position', 'initial_position_value', '_drip_amount', '_drip_remain_weeks', 'trade_week_set']:
                        assert a[k] == b[k], (sym, offset, factor, k)
                    if a['va_enabled']:
                        weeks = len(b['trade_week_set'])
                        expected = b['initial_position_value'] + sum(b['dingtou_base'] * (1 + b['dingtou_rate']) ** w for w in range(1, weeks + 1))
                        assert new._va_target_value(a) == expected
            entry['same_week_and_next_week_match'] = True
            result.append(entry)
        # Unknown paused legacy -> explicit restart cycle, preserving all original finance fields and drip pool.
        recoveries = []
        for sym in [s for s in config if not saved[s]['va_enabled']]:
            st = copy.deepcopy(initial_states[sym]); c = context(config, dt)
            protected = {k: copy.deepcopy(st[k]) for k in ['initial_position_value', 'initial_base_position', 'base_price',
                'buy_stack', 'sell_stack', 'history_pnl', 'wm_pnl', 'trade_week_set', '_macro_tp_task', '_tp_hwm_ratio', '_tp_tier', '_drip_amount', '_drip_remain_weeks']}
            prior_base = st['base_position']; st['va_enabled'] = True
            new._va_observe_switch(st, dt, prices[sym])
            assert new._va_target_value(st) == prior_base * prices[sym]  # instant anchor: no jump/no historic budget
            assert st['base_position'] == prior_base
            for k, v in protected.items(): assert st[k] == v
            new.get_target_base_position(c, sym, st, prices[sym], dt)
            assert st['_va_budget']['periods'] == 1
            assert st['_va_budget']['total'] == st['dingtou_base'] * (1 + st['dingtou_rate'])
            cycle = st['_va_budget']['cycle_id']; total = st['_va_budget']['total']
            restarted = json.loads(json.dumps(st, default=lambda obj: list(obj)))
            new._init_va_budget(st, restarted, {}, c)
            new.get_target_base_position(c, sym, st, prices[sym], dt)
            assert st['_va_budget']['cycle_id'] == cycle and st['_va_budget']['total'] == total
            recoveries.append(dict(symbol=sym, instant_anchor=prior_base * prices[sym], first_period_budget=total, old_fields_preserved=True, restart_no_duplicate=True))
    if sanitized_output:
        sanitized_output.mkdir(parents=True, exist_ok=False)
        (sanitized_output / 'state').mkdir()
        (sanitized_output / 'config').mkdir()
        for sym, value in saved.items(): (sanitized_output / 'state' / (sym + '.json')).write_text(json.dumps(value, indent=2))
        (sanitized_output / 'config/symbols.json').write_text(json.dumps(config, ensure_ascii=False, indent=2))
    return dict(snapshot_date=manifest['trade_date'], actual_version=manifest['strategy_version'], manifest_files=len(manifest['files']),
                logs=logs, symbols=result, unknown_resume=recoveries, validation='offline full-redacted-state differential regression',
                limitations=['未部署候选；无原始Portfolio/Order查询返回包；官方对象模型用于订单模拟，不是券商实测'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--sanitized-output', type=Path)
    args = parser.parse_args()
    result = validate(args.snapshot, args.sanitized_output)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print('PASS: 12 full redacted states; continuous VA / next week; paused audit / unknown resume / restart')
