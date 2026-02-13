with source as (
    select * from {{ source('fhv_raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(affiliated_base_number as string) as affiliated_base_number,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- location ids
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- trip flags
        cast(sr_flag as integer) as sr_flag
    from source
    where dispatching_base_num is not null
)

select * from renamed
