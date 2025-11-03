{{ config(materialized='table', schema='gold') }}

WITH airbnb AS (
    SELECT 
        listing_id,
        host_id,
        host_name,
        suburb,
        property_type,
        room_type,
        price,
        has_availability,          
        availability_30,           
        num_reviews,
        review_rating,
        review_cleanliness,
        review_checkin,
        review_communication,
        review_value,
        host_is_superhost,
        scraped_date
    FROM {{ ref('airbnb_cleaned') }}
),

mapping AS (
    SELECT 
        TRIM(LOWER(suburb)) AS suburb,
        TRIM(LOWER(lga_code)) AS lga_code,
        TRIM(LOWER(lga_name)) AS lga_name
    FROM {{ ref('mapping_enriched') }}
),

census AS (
    SELECT 
        -- Strip the "LGA" prefix so it matches mapping codes (e.g. LGA10050 → 10050)
        TRIM(REPLACE(LOWER(lga_code_2016), 'lga', '')) AS lga_code,
        "Tot_P_P"::NUMERIC AS total_population,
        "Median_age_persons"::NUMERIC AS median_age,
        "Median_rent_weekly"::NUMERIC AS median_rent,
        "Average_household_size"::NUMERIC AS avg_household_size
    FROM {{ ref('census_enriched') }}
    WHERE lga_code_2016 IS NOT NULL
)

SELECT 
    a.listing_id,
    a.host_id,
    a.host_name,
    a.suburb,
    m.lga_code,
    m.lga_name,
    a.property_type,
    a.room_type,
    a.price,
    a.has_availability,         
    a.availability_30,           
    a.num_reviews,
    a.review_rating,
    a.review_cleanliness,
    a.review_checkin,
    a.review_communication,
    a.review_value,
    a.host_is_superhost,
    a.scraped_date,
    c.total_population,
    c.median_age,
    c.median_rent,
    c.avg_household_size
FROM airbnb a
LEFT JOIN mapping m 
    ON TRIM(LOWER(a.suburb)) = m.suburb
LEFT JOIN census c 
    ON m.lga_code = c.lga_code
WHERE a.price IS NOT NULL AND a.price > 0
