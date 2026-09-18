"""NSE Indian Equities Data Pipeline (2015-2026).

Subpackages:
  pipeline.common      - Core utilities, atomic file IO, logging, and HTTP download harness.
  pipeline.ingestion   - Data downloaders and scrapers for official exchange archives.
  pipeline.processing  - Standardization, corporate action parsing, security master, and price adjustment.
  pipeline.features    - Quant features: circuit limit detection and market regime indicators.
  pipeline.validation  - Automated data quality reporting, hostile audits, and spot-checks.
  pipeline.recon       - Historical URL and format exploration tools.
"""
