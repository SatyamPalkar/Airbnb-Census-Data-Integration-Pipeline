# Rental Atlas — Airbnb × Census

A reproducible historical NSW market explorer with real listing snapshots,
explicit data-quality checks, SQLite analytical tables and a free static dashboard.

The analytics edition turns the original coursework into a bounded, verifiable
project. It does not require Airflow, a cloud account, a paid database or a model API.

## What it demonstrates

- Processing 412,122 source rows across 12 monthly files, May 2020–April 2021.
- An auditable bronze → silver → gold workflow with source-file hashes.
- Date/price validation, listing-month uniqueness and explicit LGA geography.
- Joining accepted observations to 2016 ABS Census G01/G02 data.
- SQL analysis and weighted aggregation, with three reproducible findings.
- A responsive dashboard with month/LGA/room-type filters, charts and CSV export.

**[Read the findings](FINDINGS.md)** · **[Data provenance and limitations](DATA.md)**

## Reproduce the published analysis — no original files needed

Python 3.9+ and the standard library are sufficient:

```sh
git clone https://github.com/SatyamPalkar/Airbnb-Census-Data-Integration-Pipeline.git
cd Airbnb-Census-Data-Integration-Pipeline
python3 -m unittest discover -s tests -v
python3 scripts/analyze_market.py
python3 -m http.server 8014 --directory docs
```

Open http://127.0.0.1:8014. `analyze_market.py` verifies the committed aggregate
checksum and row reconciliation, loads the anonymous groups into SQLite in memory,
and executes `sql/market_analysis.sql` to reproduce `FINDINGS.md`.

## Rebuild from original raw data

Original listing files are not redistributed. If you have the coursework inputs,
provide this directory structure:

```text
raw-data/
  listings/05_2020.csv ... 04_2021.csv
  Census LGA/2016Census_G01_NSW_LGA.csv
  Census LGA/2016Census_G02_NSW_LGA.csv
  NSW_LGA/NSW_LGA_CODE.csv
  NSW_LGA/NSW_LGA_SUBURB.csv
```

```sh
python3 scripts/build_market.py --raw-root /path/to/raw-data --geography-level lga
python3 scripts/analyze_market.py
```

Outputs are a local ignored `data/market.sqlite` warehouse and the public anonymous
`docs/data/market.json` aggregates. Raw source files are read in place. Gold grain
is **month × LGA × room type**. Repeated builds from identical inputs produce the
same aggregate-data SHA-256; generation timestamps can differ.

The geography flag is deliberate: these listing snapshots contain **LGA labels**
in `LISTING_NEIGHBOURHOOD`. Mapping them as suburbs would misassign names such as
Strathfield and Fairfield and leave other LGAs unmatched. For a genuinely
suburb-grain dataset, explicitly choose `--geography-level suburb`; ambiguous
suburbs remain unmatched rather than multiplying rows or guessing an LGA.

## Data-quality results

| Check | Result |
|---|---:|
| Input listing rows | 412,122 |
| Accepted listing-month observations | 407,330 |
| Source-month mismatches rejected | 4,780 |
| Invalid/nonpositive prices rejected | 12 |
| Missing LGA/Census links among accepted rows | 0 |
| Anonymous aggregate groups | 1,207 |
| Local government areas | 29 |

July's file contains 4,780 September-dated records. They are rejected using the
source-file month contract. This does not reconstruct missing July observations.
The dashboard includes reconciliation counts and flags the 120 retained prices
above AUD 10,000, which can influence means.

## Interpret the analysis accurately

Prices are advertised nightly amounts in AUD, **not revenue**. Availability is
not occupancy. Counts across multiple months are repeated snapshot observations,
not distinct properties. Census values are from 2016; listing observations are
from the pandemic period in 2020–21. Geographic correlations are descriptive,
not evidence of causation or visitor characteristics.

The minimum count in the UI affects LGA rankings, scatter points and comparative
observations. The KPI cards and CSV export retain all records matching the other
filters. Means are computed using sum(price)/count, not means of group means.

## Publishing and maintenance

GitHub Pages publishes `docs/` on `main`. Assets and data use relative paths; no
server process runs on Pages. Rebuild aggregates after raw-data changes, rerun
tests and analysis, and commit the updated public JSON plus findings and source.
Never commit the raw data, local warehouse, host names, listing identifiers or
database credentials to the published directory.

The original Airflow DAGs, PostgreSQL/dbt models, SQL coursework and reports remain
available for architecture reference. Their schemas and cloud setup are not the
verified local build path. Original notes are preserved in
[LEGACY_PIPELINE.md](LEGACY_PIPELINE.md); historical report outputs may differ from
the current 12-month, quality-checked analysis.
