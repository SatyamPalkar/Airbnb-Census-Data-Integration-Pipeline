# Historical NSW Airbnb × Census findings

These findings are reproducible from the committed anonymous aggregates using
`python3 scripts/analyze_market.py`. The source queries are in `sql/market_analysis.sql`.

## 1. Latest-snapshot price spread

For 2021-04, among LGAs with at least 30 listing records, **mosman** had
the highest mean advertised nightly price (**AUD 438.37**, n=461).
**canterbury-bankstown** had the lowest (**AUD 96.88**, n=577). The observed
ratio is **4.53×**. Property, location and room mix are not controlled.

## 2. Room type changes the comparison

In 2021-04, entire-home listings averaged
**AUD 296.19** (n=19,716);
private rooms averaged **AUD 110.96**
(n=12,150). These are asking prices, not realized revenue.

## 3. Snapshot composition changes over time

The accepted snapshot count changed from **37,558** records in 2020-05
to **32,679** in 2021-04. Mean advertised prices were **AUD 228.40**
and **AUD 222.68**, respectively. This is not a repeat-property price index:
listings may enter/leave the data and the period overlaps pandemic conditions.

## Data quality is part of the result

- 412,122 input rows reconcile to 407,330 accepted observations
  and 4,792 rejected rows.
- 4,780 rows in the July file carry September dates;
  they are rejected rather than silently assigned to July or duplicated in September.
- 12 nonpositive/invalid prices are rejected.
- The listing neighbourhood field contains LGA labels. Explicit LGA matching avoids
  misassigning same-named suburbs such as Strathfield and Fairfield.
- 0 accepted observations lack Census linkage after this join.
- 120 asking prices over AUD 10,000 are retained. Means can
  be sensitive to these values; no winsorization or occupancy inference is hidden.
- Census values are from 2016; listings span 2020–21. Geographic associations do not
  identify causation or visitor demographics.

Aggregate data SHA-256: `804ceeeefe14041cb8e971b047ee6443cc12a11146ea70df41de099894aeb3f7`.

## Latest-snapshot LGA results

| LGA | Listing records | Mean asking price (AUD/night) | 2016 median age |
|---|---:|---:|---:|
| mosman | 461 | 438.37 | 42.0 |
| northern beaches | 4,712 | 378.90 | 40.0 |
| woollahra | 1,312 | 362.27 | 39.0 |
| hunters hill | 54 | 339.24 | 43.0 |
| waverley | 4,622 | 243.14 | 35.0 |
| sutherland shire | 507 | 231.38 | 40.0 |
| the hills shire | 278 | 231.06 | 38.0 |
| willoughby | 470 | 217.94 | 37.0 |
| north sydney | 1,184 | 211.18 | 37.0 |
| lane cove | 273 | 206.72 | 36.0 |
| cumberland | 444 | 190.28 | 32.0 |
| randwick | 2,989 | 187.96 | 34.0 |
| sydney | 7,773 | 181.72 | 32.0 |
| penrith | 168 | 176.88 | 34.0 |
| hornsby | 423 | 166.56 | 40.0 |
| inner west | 2,123 | 163.65 | 36.0 |
| canada bay | 375 | 158.64 | 36.0 |
| campbelltown | 106 | 132.80 | 34.0 |
| strathfield | 177 | 131.86 | 32.0 |
| liverpool | 122 | 126.48 | 33.0 |
| bayside | 1,436 | 123.01 | 35.0 |
| ryde | 555 | 116.61 | 36.0 |
| georges river | 377 | 112.56 | 37.0 |
| parramatta | 515 | 112.06 | 34.0 |
| camden | 50 | 109.24 | 33.0 |
| fairfield | 93 | 107.89 | 36.0 |
| burwood | 228 | 100.54 | 33.0 |
| blacktown | 275 | 98.08 | 33.0 |
| canterbury-bankstown | 577 | 96.88 | 35.0 |
