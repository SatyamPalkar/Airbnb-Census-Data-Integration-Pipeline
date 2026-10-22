# Data contract and provenance

The dashboard is derived from the original coursework CSVs found in the project
owner's assignment files. The original listing publisher and raw redistribution
license were not recorded in the source repository. Raw listings are therefore
not included in this release. Source filenames, sizes and SHA-256 hashes are
recorded in `docs/data/market.json` for someone with the same files to verify.

## Inputs and grain

- Twelve `MM_YYYY.csv` listing snapshots, May 2020–April 2021, containing listing
  identifiers, snapshot dates, LGA labels, room types, asking prices and availability.
- `NSW_LGA_CODE.csv`: normalized LGA name → code mapping.
- `NSW_LGA_SUBURB.csv`: optional suburb-grain mapping; not used to infer the
  geography of the current LGA-labelled listing snapshots.
- 2016 Census G01: total population by LGA.
- 2016 Census G02: median age, median weekly rent and average household size.

Original raw files remain outside the repository. The local warehouse keeps only
the fields needed for processing; host names and host IDs are not ingested.
Listing IDs are used only locally for uniqueness, never exported to the website.

## Transformation contract

1. Record source-file checksums and load a restricted bronze projection.
2. Require a positive listing ID, valid ISO date and positive finite asking price.
3. Require each row's snapshot month to match its `MM_YYYY.csv` source file.
4. Enforce `(listing_id, month)` uniqueness. First valid row wins in sorted file
   order; any duplicate is counted as rejected.
5. Explicitly map the LGA-labelled field to normalized LGA codes. Unknown geography
   is retained as `UNMAPPED`, with missing Census fields. Optional suburb mode
   rejects ambiguity as an unmatched join rather than choosing an arbitrary LGA.
6. Treat availability outside 0–30, or invalid availability, as missing rather
   than zero. Retain very high prices and report their count.
7. Join Census by code and group by month, LGA and room type.
8. Reconcile bronze input = accepted silver + rejected rows; reconcile gold
   `SUM(listing_count)` with silver count. Checksum the sorted aggregate rows.

Public fields include counts, price sums/means/min/max, availability sums/counts,
and static LGA Census context. Recombining groups uses sums and counts; medians
cannot be reconstructed from these aggregates and are not displayed.

## Coverage and caveats

The result covers 407,330 listing-month observations and 29 LGAs. The July source
contains 4,780 September-dated rows; exclusion prevents misdated observations but
does not recreate a complete July snapshot. All accepted records join to Census
in this dataset; other datasets may have unmatched or changed boundaries.

The input values span 2016 Census geography and 2020–21 listings. Names matching
does not prove all historical boundaries are perfectly comparable. Pandemic
conditions, changing listing composition, missing observations and price outliers
limit interpretation. No causal, revenue, occupancy or current-market claims are
made by the dashboard. Claims in historical coursework reports are not revalidated
simply by retaining those files in the repository.
