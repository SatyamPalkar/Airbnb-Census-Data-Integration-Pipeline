{% snapshot dim_host_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='host_id',
    strategy='check',
    check_cols=['host_name', 'host_is_superhost']
) }}

SELECT DISTINCT
    host_id,
    host_name,
    host_is_superhost,
    scraped_date
FROM {{ ref('airbnb_cleaned') }}

{% endsnapshot %}
