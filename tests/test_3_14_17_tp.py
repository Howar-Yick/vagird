"""Focused, broker-free regression for the 3.14.17 macro take-profit change."""
import importlib.util
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


STRATEGY = Path(__file__).resolve().parents[1] / '3.14.17.py'
spec = importlib.util.spec_from_file_location('strategy_3_14_17_tp', STRATEGY)
strategy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(strategy)


class MacroTakeProfitTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 21, 10, 0)
        self.context = SimpleNamespace(current_dt=self.now, pending_frozen={})
        self.symbol = '159934.SZ'
        self.state = {'_tp_hwm_ratio': .90, '_tp_tier': 3,
                      '_va_budget': {'drip_current_valid': True},
                      '_drip_amount': 10.0, '_drip_remain_weeks': 30,
                      '_fill_tracker': {}, 'trade_week_set': {1, 2, 3, 4}}
        self.pos = SimpleNamespace(amount=10000, enable_amount=10000, cost_basis=10.0)
        self.orders = []
        self.logs = []
        self.saved = []
        self.allowed = True
        self.reason = 'cooldown'
        self.weeks = 4

        def guard(*args, **kwargs):
            return {'allow_tracking': self.allowed, 'reason': self.reason,
                    'weeks': self.weeks, 'cool_active': not self.allowed,
                    'min_weeks': 4, 'min_val': 30000,
                    'market_value': self.pos.amount * 17.0}

        def info(template, *args):
            self.logs.append((template, args))

        def order(symbol, quantity, price):
            self.orders.append((symbol, quantity, price))
            return 'ORDER-17'

        replacements = {
            'get_position': lambda symbol: self.pos,
            '_get_macro_tp_tracking_guard': guard,
            'calculate_macro_atr': lambda *args, **kwargs: .02,
            'is_main_trading_time': lambda: True,
            'is_auction_time': lambda: False,
            'cancel_all_orders_by_symbol': lambda *args: [],
            '_should_skip_duplicate_submit': lambda *args, **kwargs: False,
            '_mark_order_submit_guard': lambda *args: None,
            'order': order,
            'safe_save_state': lambda symbol, state: self.saved.append(symbol),
            'dsym': lambda context, symbol: symbol,
            'info': info,
            'log': SimpleNamespace(error=lambda message: self.fail(message)),
        }
        self.patches = [patch.object(strategy, key, value, create=True)
                        for key, value in replacements.items()]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def check(self, price):
        return strategy._check_macro_take_profit(
            self.context, self.symbol, self.state, price, self.context.current_dt)

    def test_all_three_tiers_and_order_task(self):
        for tier, price, hwm, expected, weeks in (
                (1, 12.5, .32, 3300, 16),
                (2, 14.5, .56, 5000, 24),
                (3, 17.0, .90, 8000, 52)):
            with self.subTest(tier=tier):
                self.state['_tp_tier'] = tier
                self.state['_tp_hwm_ratio'] = hwm
                self.state['_macro_tp_task'] = None
                self.orders.clear()
                self.assertTrue(self.check(price))
                self.assertEqual(self.orders, [(self.symbol, -expected, price)])
                task = self.state['_macro_tp_task']
                self.assertEqual((task['planned_qty'], task['pre_pos'], task['drip_weeks']),
                                 (expected, 10000, weeks))

    def test_tier_three_rounds_down_and_keeps_shares(self):
        self.pos.amount = self.pos.enable_amount = 9114
        self.assertTrue(self.check(17.0))
        self.assertEqual(self.orders[0][1], -7200)
        self.assertEqual(self.pos.amount + self.orders[0][1], 1914)

    def test_small_holding_and_insufficient_sellable_shares(self):
        self.pos.amount = self.pos.enable_amount = 100
        self.assertFalse(self.check(17.0))
        self.assertFalse(self.orders)
        self.pos.amount, self.pos.enable_amount = 10000, 7900
        self.assertFalse(self.check(17.0))
        self.assertFalse(self.orders)

    def test_actual_fill_sets_drip_pool_and_remaining_position(self):
        self.assertTrue(self.check(17.0))
        remaining = []
        with patch.object(strategy, '_reanchor_base_after_macro_tp',
                          side_effect=lambda ctx, sym, st, rem, px: remaining.append(rem)), \
             patch.object(strategy, '_reset_va_cycle', lambda *args: None):
            strategy._record_macro_tp_fill(self.context, self.symbol, self.state, 3000, 17)
            self.assertEqual(self.state['_macro_tp_task']['status'], 'partial_filled')
            strategy._record_macro_tp_fill(self.context, self.symbol, self.state, 5000, 17)
        self.assertEqual(remaining, [2000])
        self.assertEqual(self.state['_drip_remain_weeks'], 52)
        self.assertAlmostEqual(self.state['_drip_amount'], (8000 * 17 + 300) / 52)
        self.assertEqual(self.state['_last_macro_tp_task']['status'], 'finalized')
        self.assertIsNone(self.state['_macro_tp_task'])

    def test_blocked_log_once_per_state_and_day_with_state_reset_intact(self):
        self.allowed = False
        for _ in range(50):
            self.assertFalse(self.check(17.0))
        self.assertEqual(self._blocked_log_count(), 1)
        self.assertEqual((self.state['_tp_hwm_ratio'], self.state['_tp_tier']), (0.0, 0))
        self.weeks = 5
        self.check(17.0)
        self.assertEqual(self._blocked_log_count(), 2)
        self.context.current_dt += timedelta(days=1)
        self.check(17.0)
        self.assertEqual(self._blocked_log_count(), 3)
        self.allowed = True
        self.check(10.0)
        self.allowed = False
        self.check(17.0)
        self.assertEqual(self._blocked_log_count(), 4)

    def _blocked_log_count(self):
        return sum('宏观止盈追踪关闭:' in template for template, _ in self.logs)


class DynamicGridCandidateTests(unittest.TestCase):
    def test_asset_and_liquid_capacity_share_one_limit(self):
        policy = dict(enabled=True, reference_assets=840000, cap_at_reference=5000,
                      capital_budget_ratio=.5, unit_budget_fraction=.02,
                      cash_reserve=10000, fee_buffer_rate=.001, minimum_fee=5)
        st = dict(symbol='159934.SZ', base_position=100000,
                  base_price=10.0, max_grid_count=10, grid_unit=500)
        ctx = SimpleNamespace(portfolio=SimpleNamespace(portfolio_value=836512.40, cash=783.01),
                              state={'159934.SZ': st}, latest_data={'159934.SZ': 10.0})
        holding = SimpleNamespace(amount=61530)
        with patch.object(strategy.StrategyConfig, 'GRID_LIMITS', policy), \
             patch.object(strategy, 'get_position',
                          lambda symbol: holding, create=True):
            cap, reason = strategy._grid_capacity_cap(ctx)
            self.assertIsNone(reason)
            self.assertAlmostEqual(cap, 5000 * 836512.40 / 840000)
            with patch.object(strategy, 'info', lambda *args: None):
                strategy.adjust_grid_unit(st, ctx)
            self.assertEqual(st['grid_unit'],
                             strategy._calc_grid_unit_for_base(100000, 10, 10, ctx))
            ctx.portfolio.portfolio_value = 2_000_000
            ctx.portfolio.cash = 0
            holding.amount = 20000
            cap, reason = strategy._grid_capacity_cap(ctx)
            self.assertIsNone(reason)
            self.assertEqual(cap, 4000)  # no cash; liquid ETF budget * 0.02
            ctx.latest_data.clear()
            cap, reason = strategy._grid_capacity_cap(ctx)
            self.assertEqual(cap, 5000)
            self.assertIn('行情异常', reason)


if __name__ == '__main__':
    unittest.main()
