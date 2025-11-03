
SELECT 
    lga_name,
    ROUND(AVG(avg_price), 2) AS avg_revenue,
    ROUND(AVG(avg_median_age), 2) AS median_age,
    ROUND(AVG(avg_household_size), 2) AS household_size,
    ROUND(AVG(avg_median_rent), 2) AS median_rent
FROM gold.dm_lga_summary
GROUP BY lga_name
HAVING lga_name IN (
    
    SELECT lga_name
    FROM gold.dm_lga_summary
    GROUP BY lga_name
    ORDER BY AVG(avg_price) DESC
    LIMIT 3
)
OR lga_name IN (
    
    SELECT lga_name
    FROM gold.dm_lga_summary
    GROUP BY lga_name
    ORDER BY AVG(avg_price) ASC
    LIMIT 3
)
ORDER BY avg_revenue DESC;
-- Check correlation between median age and revenue per active listing
SELECT
    ROUND(CORR(avg_price, avg_median_age)::numeric, 3) AS corr_medianage_revenue
FROM gold.dm_lga_summary;
-- Find best property and room type combinations for top 5 neighbourhoods
WITH revenue_by_neighbourhood AS (
    SELECT
        suburb AS listing_neighbourhood,
        AVG(price) AS avg_revenue_per_listing
    FROM gold.airbnb_census_gold_joined
    WHERE price > 0
    GROUP BY suburb
),
top5_neighbourhoods AS (
    SELECT listing_neighbourhood
    FROM revenue_by_neighbourhood
    ORDER BY avg_revenue_per_listing DESC
    LIMIT 5
)
SELECT 
    a.suburb AS listing_neighbourhood,
    a.property_type,
    a.room_type,
    COUNT(a.listing_id) AS total_listings,
    ROUND(AVG(a.price), 2) AS avg_revenue,
    ROUND(AVG(a.num_reviews), 2) AS avg_reviews  -- proxy for number of stays
FROM gold.airbnb_census_gold_joined a
JOIN top5_neighbourhoods t
    ON a.suburb = t.listing_neighbourhood
GROUP BY a.suburb, a.property_type, a.room_type
ORDER BY avg_revenue DESC, avg_reviews DESC
LIMIT 10;
-- Part 4(d): Determine if multi-listing hosts concentrate within same LGA or across different LGAs
WITH host_lga_distribution AS (
    SELECT
        host_id,
        COUNT(DISTINCT listing_id) AS total_listings,
        COUNT(DISTINCT lga_name) AS distinct_lgas
    FROM gold.airbnb_census_gold_joined
    GROUP BY host_id
    HAVING COUNT(DISTINCT listing_id) > 1   -- Only consider hosts with multiple listings
)
SELECT
    CASE 
        WHEN distinct_lgas = 1 THEN 'Concentrated in Single LGA'
        ELSE 'Spread Across Multiple LGAs'
    END AS host_distribution_type,
    COUNT(*) AS num_hosts,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM host_lga_distribution
GROUP BY host_distribution_type
ORDER BY percentage DESC;

WITH single_listing_hosts AS (
    SELECT 
        host_id,
        lga_name,
        ROUND(AVG(price) * 30 * 12, 2) AS est_annual_revenue
    FROM gold.airbnb_census_gold_joined
    GROUP BY host_id, lga_name
    HAVING COUNT(DISTINCT listing_id) = 1
),
lga_rent_proxy AS (
    SELECT 
        lga_name,
        ROUND(AVG(avg_median_rent) * 52, 2) AS annual_rent_proxy
    FROM gold.dm_lga_summary
    GROUP BY lga_name
)
SELECT
    s.lga_name,
    ROUND(AVG(s.est_annual_revenue), 2) AS est_annual_revenue,
    r.annual_rent_proxy,
    ROUND(100.0 * SUM(CASE WHEN s.est_annual_revenue >= r.annual_rent_proxy THEN 1 ELSE 0 END)
                / COUNT(*), 2) AS pct_hosts_cover_mortgage
FROM single_listing_hosts s
JOIN lga_rent_proxy r 
  ON LOWER(TRIM(s.lga_name)) = LOWER(TRIM(r.lga_name))
GROUP BY s.lga_name, r.annual_rent_proxy
ORDER BY pct_hosts_cover_mortgage DESC;
