{% snapshot dim_lga_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='lga_code_2016',
    strategy='timestamp',
    updated_at='scraped_date'
) }}

SELECT DISTINCT
    c.lga_code_2016,
    c."Tot_P_P" AS total_population,
    c."Median_age_persons" AS median_age,
    c."Median_rent_weekly" AS median_rent,
    c."Average_household_size" AS avg_household_size,
    CURRENT_DATE AS scraped_date
FROM {{ ref('census_enriched') }} c
{% endsnapshot %}
