# Data Dictionary

## `nse_equity_2015_2026.parquet`

One row represents one NSE `EQ` security on one exchange session. Join and aggregate by `isin`, `date`, and `series`.

| column | type | meaning |
|---|---|---|
| `date` | date | NSE trading session |
| `exchange` | category | Always `NSE` |
| `symbol` | category | NSE symbol on that session |
| `series` | category | Always `EQ` in the current dataset |
| `isin` | category | International Securities Identification Number |
| `name` | category | Instrument name where supplied by the source file |
| `open/high/low/close` | float | Raw, unadjusted prices in ₹ |
| `prev_close` | float | Exchange-published previous close |
| `last` | float | Last traded price |
| `avg_price` | float | Average traded price; turnover divided by volume |
| `volume` | integer | Shares traded |
| `turnover` | float | Traded value in ₹ |
| `trades` | integer | Number of trades |
| `deliv_qty` | float | Delivered quantity where available |
| `deliv_pct` | float | Delivered quantity as a percentage of traded quantity |
| `adj_cap_factor` | float | Cumulative capital-action factor used to calculate adjusted OHLC fields |
| `adj_tr_factor` | float | Cumulative capital-action and dividend factor used for the total-return close |
| `factor_gap_estimated` | bool | True when a factor includes an estimate from an observed price gap |
| `adj_open/adj_high/adj_low/adj_close` | float | Raw OHLC divided by `adj_cap_factor` |
| `adj_close_tr` | float | Raw close divided by `adj_tr_factor` |

### Usage notes

- Use `adj_close` for price-return calculations that should account for capital actions.
- Use `adj_close_tr` for total-return calculations that include parsed dividends.
- Use raw `close`, `volume`, and `turnover` for as-traded price and liquidity filters.
- Delivery fields are missing where no delivery record was available. Check the latest QA report for coverage and exceptions.
- Older price files do not contain names for every row. Use `security_master.parquet` for the latest mapped name.

## `security_master.parquet`

One row per ISIN and observed symbol segment:

- `isin`: stable security identity.
- `symbol`: symbol used during the segment.
- `seg_start`, `seg_end`: first and last observed dates for that symbol.
- `latest_name`: latest available name for the ISIN.
- `exchange`: always `NSE`.

`security_master_nse.parquet` is an equivalent NSE-named copy. `symbol_changes.csv` contains ISINs with more than one observed symbol.

## `corporate_actions.parquet`

One row per NSE `(isin, ex_date)` pair after notices for the same event are consolidated.

- `cap_factor`: parsed capital-action factor.
- `div_amount`: parsed dividend amount in ₹ per share.
- `rights`: parsed rights information when available.
- `reorg`: structural reorganization flag. These events are recorded but not automatically adjusted.
- `unresolved_capital`: action was identified but its ratio was not fully parsed.
- `div_unresolved`: dividend was identified but its amount could not be resolved.
- `sources`: NSE source record marker.
- `purpose_raw`: source description retained for review.

## `corporate_actions_reconciliation.parquet`

Compares each applicable parsed capital action with the observed ex-date price movement. `recon_ok` means the observed ratio is within the pipeline's configured tolerance. Failed and estimated cases remain visible for review.

## `spike_row_flags.parquet`

One row per price row with:

- `lock_up` / `lock_dn`: frozen-at-limit directional flags.
- `band`: recorded price-band percentage for the ISIN and year, or missing when unknown.

## `market_tape.parquet`

One row per session:

- `up_frac`: fraction of NSE `EQ` rows with a positive close-to-close adjusted return.
- `mom5`: median five-session adjusted drift.

## Reports

- `data_quality_report.md`: current headline checks and counts.
- `qa_summary.json`: machine-readable version of the QA results.
- `health.json`: updater status and failed checks.
- `undocumented_gaps.csv`: adjusted price gaps above the configured threshold without a matched parsed action.
- `prev_close_mismatch_NSE.csv`: rows where published `prev_close` differs from the prior observed row beyond the configured tolerance.
