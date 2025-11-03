{% snapshot dim_suburb_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='suburb',
    strategy='timestamp',
    updated_at='scraped_date'
) }}

SELECT DISTINCT
    a.suburb,
    m.lga_code,
    m.lga_name,
    a.scraped_date
FROM {{ ref('airbnb_cleaned') }} a
LEFT JOIN {{ ref('mapping_enriched') }} m
    ON UPPER(TRIM(a.suburb)) = UPPER(TRIM(m.suburb))
{% endsnapshot %}
