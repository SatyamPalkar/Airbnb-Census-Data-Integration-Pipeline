"""Reproduce SQL findings using only the committed anonymous aggregate JSON."""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3


def analyze(path, output):
    payload = json.loads(Path(path).read_text())
    rows = payload["rows"]
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    if digest != payload["dataset_sha256"]:
        raise ValueError("Aggregate data checksum mismatch")
    if sum(r["listing_count"] for r in rows) != payload["audit"]["accepted_rows"]:
        raise ValueError("Aggregate record counts do not reconcile")
    keys = [(r["month"], r["lga_code"], r["room_type"]) for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate aggregate grain")
    with sqlite3.connect(":memory:") as db:
        # All field names and values are defined by the checked-in aggregate contract.
        columns = list(rows[0])
        db.execute("CREATE TABLE gold_lga_month_room (" + ",".join(f'"{name}"' for name in columns) + ")")
        db.executemany("INSERT INTO gold_lga_month_room VALUES(" + ",".join("?" for _ in columns) + ")", [[r[k] for k in columns] for r in rows])
        queries = Path(__file__).resolve().parents[1].joinpath("sql/market_analysis.sql").read_text().splitlines(keepends=True)
        # Statements are maintained in the source repository, not supplied by site visitors.
        results = []
        statement = ""
        for line in queries:
            statement += line
            if sqlite3.complete_statement(statement):
                cursor = db.execute(statement)
                if cursor.description:
                    results.append(([c[0] for c in cursor.description], cursor.fetchall()))
                statement = ""
    lgas, rooms, months = [result[1] for result in results]
    high, low = lgas[0], lgas[-1]
    room_lookup = {r[0]: r for r in rooms}
    first, last = months[0], months[-1]
    audit = payload["audit"]
    text = f'''# Historical NSW Airbnb × Census findings

These findings are reproducible from the committed anonymous aggregates using
`python3 scripts/analyze_market.py`. The source queries are in `sql/market_analysis.sql`.

## 1. Latest-snapshot price spread

For {last[0]}, among LGAs with at least 30 listing records, **{high[1]}** had
the highest mean advertised nightly price (**AUD {high[3]:.2f}**, n={high[2]:,}).
**{low[1]}** had the lowest (**AUD {low[3]:.2f}**, n={low[2]:,}). The observed
ratio is **{high[3]/low[3]:.2f}×**. Property, location and room mix are not controlled.

## 2. Room type changes the comparison

In {last[0]}, entire-home listings averaged
**AUD {room_lookup['Entire home/apt'][2]:.2f}** (n={room_lookup['Entire home/apt'][1]:,});
private rooms averaged **AUD {room_lookup['Private room'][2]:.2f}**
(n={room_lookup['Private room'][1]:,}). These are asking prices, not realized revenue.

## 3. Snapshot composition changes over time

The accepted snapshot count changed from **{first[1]:,}** records in {first[0]}
to **{last[1]:,}** in {last[0]}. Mean advertised prices were **AUD {first[2]:.2f}**
and **AUD {last[2]:.2f}**, respectively. This is not a repeat-property price index:
listings may enter/leave the data and the period overlaps pandemic conditions.

## Data quality is part of the result

- {audit['input_rows']:,} input rows reconcile to {audit['accepted_rows']:,} accepted observations
  and {audit['input_rows']-audit['accepted_rows']:,} rejected rows.
- {audit.get('rejected_source_month_mismatch',0):,} rows in the July file carry September dates;
  they are rejected rather than silently assigned to July or duplicated in September.
- {audit.get('rejected_invalid_price',0):,} nonpositive/invalid prices are rejected.
- The listing neighbourhood field contains LGA labels. Explicit LGA matching avoids
  misassigning same-named suburbs such as Strathfield and Fairfield.
- {audit.get('missing_census_rows',0):,} accepted observations lack Census linkage after this join.
- {audit.get('price_above_10000_rows',0):,} asking prices over AUD 10,000 are retained. Means can
  be sensitive to these values; no winsorization or occupancy inference is hidden.
- Census values are from 2016; listings span 2020–21. Geographic associations do not
  identify causation or visitor demographics.

Aggregate data SHA-256: `{digest}`.

## Latest-snapshot LGA results

| LGA | Listing records | Mean asking price (AUD/night) | 2016 median age |
|---|---:|---:|---:|
'''
    text += "\n".join(f"| {r[1]} | {r[2]:,} | {r[3]:.2f} | {r[4]} |" for r in lgas) + "\n"
    Path(output).write_text(text)
    print(f"Verified {len(rows)} aggregate groups; reproduced three SQL analyses into {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="docs/data/market.json")
    parser.add_argument("--output", default="FINDINGS.md")
    args = parser.parse_args()
    analyze(args.input, args.output)
