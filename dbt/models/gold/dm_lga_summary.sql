{{ config(materialized='view', schema='gold') }}

-- Purpose: Summarize Airbnb and Census data at the LGA level for analytical reporting.

WITH fact AS (
    SELECT 
        lga_code,
        lga_name,
        price,
        num_reviews,
        review_rating,
        host_is_superhost,
        total_population,
        median_age,
        median_rent,
        avg_household_size,
        scraped_date
    FROM {{ ref('airbnb_census_gold_joined') }}
    WHERE price > 0
),

aggregated AS (
    SELECT
        lga_code,
        lga_name,
        DATE_TRUNC('month', scraped_date) AS month,
        COUNT(*) AS total_listings,
        AVG(price) AS avg_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
        AVG(review_rating) AS avg_review_rating,
        SUM(CASE WHEN host_is_superhost = 't' THEN 1 ELSE 0 END)::NUMERIC 
            / NULLIF(COUNT(*), 0) * 100 AS superhost_rate,
        SUM(num_reviews) AS total_reviews,
        AVG(total_population) AS avg_population,
        AVG(median_age) AS avg_median_age,
        AVG(median_rent) AS avg_median_rent,
        AVG(avg_household_size) AS avg_household_size
    FROM fact
    GROUP BY 1, 2, 3
)

SELECT * 
FROM aggregated
