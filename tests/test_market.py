import csv
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from scripts.build_market import build


class MarketBuildTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.raw = self.root / "raw"
        self.warehouse = self.root / "warehouse.sqlite"
        self.output = self.root / "market.json"
        self.write("Census LGA/2016Census_G01_NSW_LGA.csv", ["LGA_CODE_2016", "Tot_P_P"], [["LGA100", 1000], ["LGA200", 2000]])
        self.write("Census LGA/2016Census_G02_NSW_LGA.csv", ["LGA_CODE_2016", "Median_age_persons", "Median_rent_weekly", "Average_household_size"], [["LGA100", 35, 300, 2], ["LGA200", 40, 400, 3]])
        self.write("NSW_LGA/NSW_LGA_CODE.csv", ["LGA_CODE", "LGA_NAME"], [[100, "Strathfield"], [200, "Burwood"]])
        # A same-named suburb maps elsewhere: direct LGA semantics must not use it.
        self.write("NSW_LGA/NSW_LGA_SUBURB.csv", ["SUBURB_NAME", "LGA_NAME"], [["Strathfield", "Burwood"]])

    def write(self, name, header, rows):
        path = self.raw / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8-sig") as file:
            writer = csv.writer(file); writer.writerow(header); writer.writerows(rows)

    def listings(self, rows):
        self.write("listings/05_2020.csv", ["LISTING_ID", "SCRAPED_DATE", "LISTING_NEIGHBOURHOOD", "ROOM_TYPE", "PRICE", "AVAILABILITY_30"], rows)

    def run_build(self, geography="lga"):
        return build(self.raw, self.warehouse, self.output, geography)

    def test_direct_lga_mapping_avoids_same_named_suburb_misassignment(self):
        self.listings([[1, "2020-05-01", "  STRATHFIELD ", "Private room", "$100.00", 20]])
        result = self.run_build()
        self.assertEqual(result["rows"][0]["lga_code"], "100")
        self.assertEqual(result["rows"][0]["median_age"], 35)

    def test_rejects_wrong_snapshot_duplicate_invalid_price_and_bad_id(self):
        self.listings([[1, "2020-05-01", "Strathfield", "Private room", 100, 20],
                       [1, "2020-05-01", "Strathfield", "Private room", 100, 20],
                       [2, "2020-09-01", "Strathfield", "Private room", 100, 20],
                       [3, "2020-05-01", "Strathfield", "Private room", "nan", 20],
                       ["bad", "2020-05-01", "Strathfield", "Private room", 100, 20],
                       [4, "2020-05-01", "Burwood", "Private room", 200, 30]])
        result = self.run_build()
        self.assertEqual(result["audit"]["input_rows"], 6)
        self.assertEqual(result["audit"]["accepted_rows"], 2)
        for reason in ["rejected_duplicate_listing_month", "rejected_source_month_mismatch", "rejected_invalid_price", "rejected_invalid_id"]:
            self.assertEqual(result["audit"][reason], 1)
        with sqlite3.connect(self.warehouse) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM bronze_listings").fetchone()[0], 6)
            self.assertEqual(db.execute("SELECT SUM(listing_count) FROM gold_lga_month_room").fetchone()[0], 2)

    def test_ambiguous_suburb_does_not_multiply_listing_rows(self):
        self.write("NSW_LGA/NSW_LGA_SUBURB.csv", ["SUBURB_NAME", "LGA_NAME"], [["Place", "Strathfield"], ["Place", "Burwood"]])
        self.listings([[1, "2020-05-01", "Place", "Private room", 100, 20]])
        result = self.run_build("suburb")
        self.assertEqual(result["audit"]["accepted_rows"], 1)
        self.assertEqual(result["audit"]["ambiguous_suburb_rows"], 1)
        self.assertEqual(result["rows"][0]["lga_code"], "UNMAPPED")

    def test_repeat_builds_produce_same_data_hash_and_no_listing_identifiers_in_export(self):
        self.listings([[99887766, "2020-05-01", "Strathfield", "Private room", 100, 20]])
        first = self.run_build(); second = self.run_build()
        self.assertEqual(first["dataset_sha256"], second["dataset_sha256"])
        self.assertNotIn("99887766", self.output.read_text())
        self.assertNotIn("listing_id", json.dumps(second["rows"]))

    def test_invalid_input_keeps_previous_published_data_and_warehouse(self):
        self.listings([[1, "2020-05-01", "Strathfield", "Private room", 100, 20]])
        self.run_build(); old = self.output.read_bytes(); warehouse = self.warehouse.read_bytes()
        self.write("listings/05_2020.csv", ["PRICE"], [[100]])
        with self.assertRaises(ValueError): self.run_build()
        self.assertEqual(self.output.read_bytes(), old)
        self.assertEqual(self.warehouse.read_bytes(), warehouse)

    def test_invalid_availability_is_missing_not_zero_or_occupancy(self):
        self.listings([[1, "2020-05-01", "Strathfield", "Private room", 100, 999]])
        result = self.run_build()
        self.assertEqual(result["audit"]["accepted_rows"], 1)
        self.assertEqual(result["rows"][0]["availability_count"], 0)

    def test_unknown_lga_is_preserved_with_explicit_missing_census(self):
        self.listings([[1, "2020-05-01", "Unknown", "Private room", 100, 20]])
        result = self.run_build()
        self.assertEqual(result["audit"]["missing_census_rows"], 1)
        self.assertIsNone(result["rows"][0]["median_age"])


if __name__ == "__main__":
    unittest.main()
