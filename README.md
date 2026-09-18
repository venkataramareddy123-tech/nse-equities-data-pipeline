# NSE Indian Equities Dataset (2015–2026)

Daily NSE equity data keyed by ISIN, with raw OHLCV fields, delivery fields, corporate-action-derived adjustment factors, and a small set of derived market features.

The files in `data/final/` are generated from NSE-published price, delivery, and corporate-action files downloaded by this repository. The current price universe is the NSE `EQ` series.

## Scope and sources

- Exchange: NSE only.
- Price history: NSE historical bhavcopy files and NSE UDiFF bhavcopy files.
- Delivery history: NSE MTO files, supplemented by `sec_bhavdata_full` files where available.
- Corporate actions: NSE corporate-action feed records with parseable dates.
- Identity: rows use ISIN as the stable join key; the symbol is retained as it appeared on the session date.
- Current snapshot boundaries and row counts are reported in `reports/data_quality_report.md`.

## Derived fields

- `adj_*` fields are calculated from parsed corporate actions and are not original exchange fields.
- `factor_gap_estimated` identifies rows affected by a factor estimated from an observed price gap because an action ratio was unavailable.
- `reorg` actions are recorded for review and are not automatically adjusted.
- `market_tape.parquet` contains session-level breadth and five-session drift features.
- `spike_row_flags.parquet` contains circuit-band and frozen-at-limit flags.

## Validation

The validation scripts check schema, key uniqueness, OHLC relationships, numeric bounds, turnover arithmetic, delivery coverage, calendar coverage, symbol/ISIN relationships, and corporate-action reconciliation. Results are written to `reports/`.

The reports intentionally retain exceptions instead of hiding them. Read the latest report before using the data for research or publishing it.

## Layout

```
data/
  raw/                    # downloaded NSE source files
  interim/                # yearly standardized files and parsed actions
  final/                  # generated dataset files
reports/                  # QA reports, health status, and anomaly lists
pipeline/
  common/                 # shared utilities and download infrastructure
  ingestion/              # NSE downloaders and fetchers
  processing/             # standardization, actions, identity, adjustments
  features/               # derived market features and row flags
  validation/             # QA reports, audits, and spot checks
  recon/                  # NSE archive format probes
  roll_update.py          # incremental updater
tests/                    # automated regression tests
UPDATE_DATA.bat           # Windows update shortcut
DATA_DICTIONARY.md        # column definitions
KAGGLE_OPENSOURCE_GUIDE.md # publication notes
```

## Rolling updates

```bash
python pipeline/roll_update.py --auto [--full] [--check-only] [--skip-downloads]
```

The updater downloads missing NSE files, rebuilds affected outputs, writes files atomically, and records its status in `reports/health.json`. Environment overrides include `ROLL_TODAY=YYYY-MM-DD`, `ROLL_FULL=1`, and `ROLL_FORCE_YEARS=2020,2021`.

## Rebuild order

```bash
python pipeline/ingestion/download_nse.py
python pipeline/ingestion/fetch_nse_ca.py
python pipeline/processing/parse_standardize.py
python pipeline/processing/corp_actions_parse.py
python pipeline/processing/build_master.py
python pipeline/processing/adjust.py
python pipeline/features/market_tape.py
python pipeline/features/row_flags.py
python pipeline/validation/qa_report.py
python pipeline/validation/spotcheck.py
python pipeline/validation/audit.py
```

The raw source files are not committed to the repository. Rebuilding therefore depends on the availability of the NSE source URLs and on compliance with their applicable terms.
