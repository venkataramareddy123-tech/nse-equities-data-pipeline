# Data Quality Report
_Generated 2026-09-18 21:09_

## NSE
- securities (ISINs): **3,872**, symbols: 3,777, duplicate rows: 0
- calendar: 2015-01-01 -> 2026-09-11, 2888 sessions, max gap 5 days, gaps>4d: 5
- prev_close mismatches >1%: 10,170

| check | value |
|---|---|
| close<=0 | 0 |
| close NaN | 0 |
| high<low | 0 |
| close>high | 0 |
| close<low | 0 |
| open>high | 0 |
| open<low | 0 |
| volume<0 | 0 |
| volume==0 rows | 0 |
| deliv_pct coverage | 0.9978650877761096 |
| deliv_pct outside [0,100] | 0 |
| deliv_qty > volume | 8 |

| year | sessions | securities (ISIN) | rows | turnover (cr) |
|---|---|---|---|---|
| 2015 | 247 | 1,628 | 355,279 | 4,312,650 |
| 2016 | 246 | 1,659 | 369,146 | 4,654,631 |
| 2017 | 248 | 1,705 | 368,537 | 6,562,174 |
| 2018 | 246 | 1,682 | 367,880 | 7,964,234 |
| 2019 | 244 | 1,697 | 367,105 | 8,353,833 |
| 2020 | 250 | 1,740 | 376,705 | 13,416,592 |
| 2021 | 248 | 1,918 | 389,770 | 17,038,181 |
| 2022 | 248 | 2,060 | 438,894 | 13,921,607 |
| 2023 | 245 | 2,161 | 445,186 | 16,153,146 |
| 2024 | 246 | 2,338 | 461,929 | 28,506,795 |
| 2025 | 248 | 2,555 | 529,758 | 24,361,299 |
| 2026 | 172 | 2,931 | 421,347 | 21,271,793 |

## Corporate action reconciliation
- capital actions reconciled: 677
- within 12% tolerance: 599
- gap-estimated factors: 12
- reorg-flagged: 387
