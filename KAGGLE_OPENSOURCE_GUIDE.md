# Publication Guide

This guide describes how to publish the NSE-only dataset to Kaggle and the pipeline code to GitHub after the current outputs have been rebuilt and reviewed.

## Kaggle files

Upload the files from `data/final/` that are listed below:

- `nse_equity_2015_2026.parquet`: primary daily NSE `EQ` panel.
- `corporate_actions.parquet`: consolidated NSE corporate-action records.
- `corporate_actions_reconciliation.parquet`: capital-action gap checks.
- `security_master.parquet`: ISIN and symbol history.
- `market_tape.parquet`: session-level derived features.
- `spike_row_flags.parquet`: row-level circuit flags.
- `symbol_changes.csv`: observed symbol changes.

Before publishing, replace the placeholder Kaggle username in `data/final/dataset-metadata.json` and review the latest files in `reports/`.

Confirm that public redistribution of the source-derived files is permitted under the applicable NSE terms. The repository does not grant ownership of the source market data.

## Description template

```markdown
# NSE Indian Equities Dataset (2015–2026)

Daily NSE equity data keyed by ISIN. The dataset contains raw OHLCV fields, delivery fields where available, parsed corporate-action records, derived adjustment factors, and session-level features.

## Scope

- Exchange: NSE only.
- Series: EQ only in the current release.
- Date range, row count, delivery coverage, and validation results: see the attached `reports` files.

## Source files

The pipeline reads NSE historical bhavcopy files, NSE UDiFF bhavcopy files, NSE delivery files, and NSE corporate-action feed records.

## Important notes

- `adj_*` columns are derived by this pipeline and are not original exchange fields.
- Some action ratios may be estimated from observed price gaps and are marked by `factor_gap_estimated`.
- Reorganization events are flagged and are not automatically adjusted.
- Missing values and validation exceptions are retained and documented in the reports.

## Example

```python
import polars as pl

df = pl.scan_parquet("nse_equity_2015_2026.parquet")
reliance = (
    df.filter(pl.col("isin") == "INE002A01018")
      .select(["date", "symbol", "close", "adj_close", "deliv_pct", "turnover"])
      .collect()
)
print(reliance.tail(10))
```

The processing code and tests are available in the linked GitHub repository.
```

## GitHub repository

Keep the raw downloads and intermediate files out of GitHub. Commit the pipeline, tests, documentation, and reports. Add the final Parquet files to Kaggle or another approved data host rather than committing files larger than GitHub's normal file limit.

```bash
git add .gitignore README.md DATA_DICTIONARY.md UPDATE_DATA.bat KAGGLE_OPENSOURCE_GUIDE.md pipeline/ tests/ reports/
git commit -m "docs: publish NSE-only dataset pipeline"
git remote add origin https://github.com/YOUR_USERNAME/nse-equities-pipeline.git
git branch -M main
git push -u origin main
```
