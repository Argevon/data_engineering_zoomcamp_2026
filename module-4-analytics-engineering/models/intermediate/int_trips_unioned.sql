-- Union green and yellow taxi data into a single dataset
-- Demonstrates how to combine data from multiple sources with slightly different schemas

with green_trips as (
    select
        cast(vendor_id as integer) as vendor_id,
        cast(rate_code_id as integer) as rate_code_id,
        cast(pickup_location_id as integer) as pickup_location_id,
        cast(dropoff_location_id as integer) as dropoff_location_id,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        cast(trip_type as integer) as trip_type,
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(ehail_fee as numeric) as ehail_fee,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(payment_type as integer) as payment_type,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),

yellow_trips as (
    select
        cast(vendor_id as integer) as vendor_id,
        cast(rate_code_id as integer) as rate_code_id,
        cast(pickup_location_id as integer) as pickup_location_id,
        cast(dropoff_location_id as integer) as dropoff_location_id,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        cast(1 as integer) as trip_type,  -- Yellow taxis only do street-hail (code 1)
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(0 as numeric) as ehail_fee,  -- Yellow taxis don't have ehail_fee
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(payment_type as integer) as payment_type,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
)

select * from green_trips
union all
select * from yellow_trips