"""Build a local SQLite warehouse and anonymous dashboard aggregates from source CSVs."""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE bronze_listings (
 source_file TEXT, source_month TEXT, listing_id TEXT, scraped_date TEXT,
 suburb TEXT, room_type TEXT, price TEXT, availability_30 TEXT
);
CREATE TABLE census (lga_code TEXT PRIMARY KEY, population REAL, median_age REAL,
 median_weekly_rent REAL, household_size REAL);
CREATE TABLE silver_listings (
 listing_id TEXT, month TEXT, lga_code TEXT, lga_name TEXT, room_type TEXT,
 price REAL CHECK(price > 0), availability_30 REAL,
 PRIMARY KEY(listing_id, month)
);
CREATE VIEW gold_lga_month_room AS
 SELECT s.month, s.lga_code, s.lga_name, s.room_type,
 COUNT(*) AS listing_count, SUM(s.price) AS sum_price, AVG(s.price) AS avg_price,
 MIN(s.price) AS min_price, MAX(s.price) AS max_price,
 SUM(COALESCE(s.availability_30, 0)) AS sum_availability,
 COUNT(s.availability_30) AS availability_count,
 c.population, c.median_age, c.median_weekly_rent, c.household_size
 FROM silver_listings s LEFT JOIN census c ON c.lga_code = s.lga_code
 GROUP BY s.month, s.lga_code, s.lga_name, s.room_type;
"""


def normalize(value):
    return " ".join(value.strip().casefold().split())


def lga_code(value):
    value = value.strip().upper().removeprefix("LGA")
    return str(int(value)) if value.isdigit() else value


def number(value):
    try:
        result = float(str(value).replace("$", "").replace(",", "").strip())
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def csv_rows(path, required):
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        missing = set(required) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name}: missing required columns {sorted(missing)}")
        yield from reader


def build(raw_root, warehouse, output, geography_level="lga"):
    if geography_level not in ("lga", "suburb"):
        raise ValueError("geography_level must be lga or suburb")
    raw_root, warehouse, output = map(Path, (raw_root, warehouse, output))
    listing_files = sorted((raw_root / "listings").glob("*.csv"))
    if not listing_files:
        raise ValueError("No listings/*.csv files found. Supply the original coursework raw-data directory.")
    g01 = raw_root / "Census LGA/2016Census_G01_NSW_LGA.csv"
    g02 = raw_root / "Census LGA/2016Census_G02_NSW_LGA.csv"
    codes_file = raw_root / "NSW_LGA/NSW_LGA_CODE.csv"
    suburbs_file = raw_root / "NSW_LGA/NSW_LGA_SUBURB.csv"
    sources = []
    for path in [*listing_files, g01, g02, codes_file, suburbs_file]:
        sources.append({"file": path.relative_to(raw_root).as_posix(), "bytes": path.stat().st_size,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    temporary = warehouse.with_suffix(".tmp")
    if temporary.exists():
        temporary.unlink()
    audit = collections.Counter()
    per_file = []
    conn = sqlite3.connect(temporary)
    try:
        conn.executescript(SCHEMA)
        populations = {}
        for row in csv_rows(g01, ["LGA_CODE_2016", "Tot_P_P"]):
            code = lga_code(row["LGA_CODE_2016"])
            if code in populations:
                raise ValueError("Duplicate Census G01 LGA codes")
            populations[code] = number(row["Tot_P_P"])
        for row in csv_rows(g02, ["LGA_CODE_2016", "Median_age_persons", "Median_rent_weekly", "Average_household_size"]):
            code = lga_code(row["LGA_CODE_2016"])
            conn.execute("INSERT INTO census VALUES(?,?,?,?,?)", (code, populations.get(code),
                number(row["Median_age_persons"]), number(row["Median_rent_weekly"]), number(row["Average_household_size"])))
        census_codes = {r[0] for r in conn.execute("SELECT lga_code FROM census WHERE median_age IS NOT NULL")}
        codes = {}
        for row in csv_rows(codes_file, ["LGA_CODE", "LGA_NAME"]):
            name, code = normalize(row["LGA_NAME"]), lga_code(row["LGA_CODE"])
            if name in codes and codes[name] != code:
                raise ValueError("Conflicting LGA name/code mapping")
            codes[name] = code
        suburb_map = collections.defaultdict(set)
        for row in csv_rows(suburbs_file, ["SUBURB_NAME", "LGA_NAME"]):
            suburb_map[normalize(row["SUBURB_NAME"])].add(normalize(row["LGA_NAME"]))
        required = ["LISTING_ID", "SCRAPED_DATE", "LISTING_NEIGHBOURHOOD", "ROOM_TYPE", "PRICE", "AVAILABILITY_30"]
        for path in listing_files:
            expected = datetime.strptime(path.stem, "%m_%Y").strftime("%Y-%m")
            counts = collections.Counter()
            for row in csv_rows(path, required):
                counts["input_rows"] += 1
                conn.execute("INSERT INTO bronze_listings VALUES(?,?,?,?,?,?,?,?)", (
                    path.name, expected, row["LISTING_ID"], row["SCRAPED_DATE"], row["LISTING_NEIGHBOURHOOD"],
                    row["ROOM_TYPE"], row["PRICE"], row["AVAILABILITY_30"]))
                try:
                    listing = str(int(row["LISTING_ID"]))
                    if int(listing) <= 0:
                        raise ValueError()
                except ValueError:
                    counts["rejected_invalid_id"] += 1
                    continue
                try:
                    month = datetime.strptime(row["SCRAPED_DATE"], "%Y-%m-%d").strftime("%Y-%m")
                except ValueError:
                    counts["rejected_invalid_date"] += 1
                    continue
                if month != expected:
                    counts["rejected_source_month_mismatch"] += 1
                    continue
                price = number(row["PRICE"])
                if price is None or price <= 0:
                    counts["rejected_invalid_price"] += 1
                    continue
                geography = normalize(row["LISTING_NEIGHBOURHOOD"])
                # These snapshots contain LGA labels despite the column's neighbourhood name.
                # A suburb lookup would misassign names such as Fairfield and Strathfield.
                options = ({geography} if geography in codes else set()) if geography_level == "lga" else suburb_map.get(geography, set())
                name = next(iter(options)) if len(options) == 1 else "unmapped suburb"
                code = codes.get(name, "UNMAPPED")
                availability = number(row["AVAILABILITY_30"])
                if availability is not None and not 0 <= availability <= 30:
                    availability = None
                try:
                    conn.execute("INSERT INTO silver_listings VALUES(?,?,?,?,?,?,?)", (
                        listing, month, code, name, row["ROOM_TYPE"].strip() or "Not stated", price, availability))
                except sqlite3.IntegrityError:
                    counts["rejected_duplicate_listing_month"] += 1
                    continue
                counts["accepted_rows"] += 1
                if len(options) > 1:
                    counts["ambiguous_suburb_rows"] += 1
                if code == "UNMAPPED":
                    counts["unmapped_rows"] += 1
                if code not in census_codes:
                    counts["missing_census_rows"] += 1
                if availability is None:
                    counts["invalid_availability_rows"] += 1
                if price > 10000:
                    counts["price_above_10000_rows"] += 1
            conn.commit()
            audit.update(counts)
            per_file.append({"file": path.name, **dict(counts)})
        rejected = sum(count for reason, count in audit.items() if reason.startswith("rejected_"))
        if audit["input_rows"] != audit["accepted_rows"] + rejected or not audit["accepted_rows"]:
            raise ValueError("Row reconciliation failed or no valid listings remain")
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute("SELECT * FROM gold_lga_month_room ORDER BY month,lga_code,room_type")]
        if sum(row["listing_count"] for row in rows) != audit["accepted_rows"]:
            raise ValueError("Gold aggregates do not reconcile with silver rows")
        payload = {
            "schema_version": 1, "source": "Coursework-provided NSW Airbnb snapshots and 2016 ABS Census tables",
            "geography_level": geography_level,
            "census_year": 2016, "generated_utc": datetime.now(timezone.utc).isoformat(),
            "months": sorted({row["month"] for row in rows}), "audit": dict(audit),
            "files": per_file, "sources": sources, "rows": rows,
            "dataset_sha256": hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest(),
            "limitations": [
                "Advertised nightly prices in AUD are not revenue; availability is not occupancy.",
                "Listings are repeated monthly snapshots, not distinct listings when months are combined.",
                "2016 Census values predate 2020–21 listings; boundary/name mismatches are recorded as missing joins.",
                "Historical pandemic-era observations are not current market estimates or causal evidence.",
                "Raw source provenance is the coursework dataset; original listing publisher and redistribution terms were not recorded in the source repository.",
            ],
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output_tmp = output.with_suffix(".tmp")
        output_tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n")
        conn.close()
        os.replace(temporary, warehouse)
        os.replace(output_tmp, output)
        return payload
    except Exception:
        conn.close()
        temporary.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--warehouse", type=Path, default=Path("data/market.sqlite"))
    parser.add_argument("--output", type=Path, default=Path("docs/data/market.json"))
    parser.add_argument("--geography-level", choices=["lga", "suburb"], default="lga")
    args = parser.parse_args()
    result = build(args.raw_root, args.warehouse, args.output, args.geography_level)
    print(json.dumps({"months": result["months"], "audit": result["audit"], "groups": len(result["rows"]),
                      "dataset_sha256": result["dataset_sha256"]}, indent=2))


if __name__ == "__main__":
    main()
