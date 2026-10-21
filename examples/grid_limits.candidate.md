# Dynamic grid cap candidate (test branch only)

`grid_limits.candidate.json` is a **fragment** for `config/strategy.json`, not a replacement for that file. Do not overwrite live configuration with this fragment. The 3.14.16/3.14.17 strategy loads `grid_limits` from its existing configuration and defaults to disabled when absent.

The candidate anchors ¥5,000 to ¥840,000 of total assets. Capacity is the lesser of `5000 × assets / 840000` and `min(assets × 0.5, liquid strategy ETF holdings + max(cash − 10000, 0)) × 0.02`. Actual grid orders are rounded to lots of 100, and new buys are independently gated by current available cash and fees. Invalid account or quote data falls back to the old unit cap and blocks new buys.

Using the last verified 2026-10-20 snapshot (assets ¥836,512.40, ETF market value ¥615,302.28, cash ¥783.01), the modeled structure cap is ¥4,979.24, subject to live quotes and rounding. This is a **candidate calculation**, not a claim of successful live execution or parameter approval. Before enabling it, compare order replay, liquid holdings, cash behavior after reverse repo maturity, and ordinary VA/grid orders against the running baseline. Keep the current `grid_limits.enabled=false` until the candidate passes PTrade trading-hour and day-end validation; rollback is to restore the prior `grid_limits` object.
