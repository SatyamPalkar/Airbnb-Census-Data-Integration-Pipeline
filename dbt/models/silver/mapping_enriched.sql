-- models/silver/mapping_enriched.sql
-- Purpose: Combine suburb and LGA mapping data into a unified dataset.

WITH suburb_map AS (
    SELECT 
        INITCAP(TRIM("SUBURB_NAME")) AS suburb,
        INITCAP(TRIM("LGA_NAME")) AS lga_name
    FROM "postgres"."bronze"."mapping_suburb"
    WHERE "SUBURB_NAME" IS NOT NULL
),

lga_map AS (
    SELECT 
        TRIM("LGA_CODE") AS lga_code,
        INITCAP(TRIM("LGA_NAME")) AS lga_name
    FROM "postgres"."bronze"."mapping_lga"
)

SELECT 
    s.suburb,
    l.lga_code,
    s.lga_name
FROM suburb_map s
LEFT JOIN lga_map l
    ON s.lga_name = l.lga_name
WHERE s.suburb IS NOT NULL
