{% snapshot dim_property_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='property_type',
    strategy='timestamp',
    updated_at='scraped_date'
) }}

SELECT DISTINCT
    property_type,
    room_type,
    accommodates,
    scraped_date
FROM {{ ref('airbnb_cleaned') }}
{% endsnapshot %}
