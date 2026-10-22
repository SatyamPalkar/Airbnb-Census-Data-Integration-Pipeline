-- SQLite-compatible queries over gold_lga_month_room. Prices are not revenue.
-- 1. Latest-snapshot LGA comparison, excluding groups below 30 listing records.
SELECT lga_code, lga_name, SUM(listing_count) AS records,
       SUM(sum_price) / SUM(listing_count) AS mean_asking_price,
       MAX(median_age) AS census_2016_median_age
FROM gold_lga_month_room
WHERE month = (SELECT MAX(month) FROM gold_lga_month_room)
GROUP BY lga_code, lga_name
HAVING SUM(listing_count) >= 30
ORDER BY mean_asking_price DESC;

-- 2. Room-type comparison, weighted by records (not average-of-averages).
SELECT room_type, SUM(listing_count) AS records,
       SUM(sum_price) / SUM(listing_count) AS mean_asking_price
FROM gold_lga_month_room
WHERE month = (SELECT MAX(month) FROM gold_lga_month_room)
GROUP BY room_type
ORDER BY mean_asking_price DESC;

-- 3. Monthly observations and mean prices. Counts are not unique properties
-- when summed across snapshots; changes may reflect composition, not repricing.
SELECT month, SUM(listing_count) AS listing_month_records,
       SUM(sum_price) / SUM(listing_count) AS mean_asking_price,
       SUM(sum_availability) / NULLIF(SUM(availability_count), 0) AS mean_days_available
FROM gold_lga_month_room GROUP BY month ORDER BY month;
