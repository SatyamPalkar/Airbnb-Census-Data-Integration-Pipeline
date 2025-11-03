{{ config(materialized='table', schema='silver') }}

WITH g01 AS (
    SELECT 
        "LGA_CODE_2016" AS lga_code_2016,
        *
    FROM {{ source('bronze', 'census_g01') }}
),
g02 AS (
    SELECT 
        "LGA_CODE_2016" AS lga_code_2016,
        *
    FROM {{ source('bronze', 'census_g02') }}
)

SELECT
    COALESCE(g01.lga_code_2016, g02.lga_code_2016) AS lga_code_2016,
    -- Select only non-duplicate columns from g01 and g02
    g01."Tot_P_M",
    g01."Tot_P_F",
    g01."Tot_P_P",
    g01."Age_0_4_yr_M",
    g01."Age_0_4_yr_F",
    g01."Age_0_4_yr_P",
    g01."Age_5_14_yr_M",
    g01."Age_5_14_yr_F",
    g01."Age_5_14_yr_P",
    g02."Median_age_persons",
    g02."Median_mortgage_repay_monthly",
    g02."Median_tot_prsnl_inc_weekly",
    g02."Median_rent_weekly",
    g02."Median_tot_fam_inc_weekly",
    g02."Average_num_psns_per_bedroom",
    g02."Median_tot_hhd_inc_weekly",
    g02."Average_household_size"
FROM g01
FULL JOIN g02 
    ON g01.lga_code_2016 = g02.lga_code_2016
