{{ config(materialized='table') }}


with airports_info as
(
	select 
		*,
		airport_name ->> 'en' as english_airport_name,
		airport_name ->> 'ru' as russian_airport_name,
		city ->> 'en' as english_city_name,
		city ->> 'ru' as russian_city_name
	from {{ source('dim_stg','airports_data') }}
	where 1=1
)
select 
	airport_code,
	coordinates,
	timezone,
	english_airport_name,
	russian_airport_name,
	english_city_name,
	russian_city_name,
    '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
from airports_info