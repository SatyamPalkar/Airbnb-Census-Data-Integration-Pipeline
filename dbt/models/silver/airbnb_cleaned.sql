{{ config(materialized='table', schema='silver') }}

WITH base AS (
    SELECT
        CAST("LISTING_ID" AS BIGINT)                  AS listing_id,
        CAST("HOST_ID" AS BIGINT)                     AS host_id,
        "HOST_NAME"                                   AS host_name,
        "HOST_IS_SUPERHOST"                           AS host_is_superhost,
        "HOST_NEIGHBOURHOOD"                          AS host_neighbourhood,
        "LISTING_NEIGHBOURHOOD"                       AS suburb,
        "PROPERTY_TYPE"                               AS property_type,
        "ROOM_TYPE"                                   AS room_type,
        NULLIF("ACCOMMODATES", '')::INT               AS accommodates,  -- ✅ Added line
        NULLIF("PRICE", '')::NUMERIC                  AS price,
        "HAS_AVAILABILITY"                            AS has_availability,
        NULLIF("AVAILABILITY_30", '')::INT            AS availability_30,
        NULLIF("NUMBER_OF_REVIEWS", '')::INT          AS num_reviews,
        NULLIF("REVIEW_SCORES_RATING", '')::NUMERIC   AS review_rating,
        NULLIF("REVIEW_SCORES_ACCURACY", '')::NUMERIC AS review_accuracy,
        NULLIF("REVIEW_SCORES_CLEANLINESS", '')::NUMERIC AS review_cleanliness,
        NULLIF("REVIEW_SCORES_CHECKIN", '')::NUMERIC  AS review_checkin,
        NULLIF("REVIEW_SCORES_COMMUNICATION", '')::NUMERIC AS review_communication,
        NULLIF("REVIEW_SCORES_VALUE", '')::NUMERIC    AS review_value,
        TO_DATE("SCRAPED_DATE", 'YYYY-MM-DD')         AS scraped_date
    FROM {{ source('bronze', 'airbnb') }}
)

SELECT *
FROM base
WHERE price IS NOT NULL
  AND price > 0
