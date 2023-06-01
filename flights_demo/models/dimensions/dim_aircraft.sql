{{ config(materialized='table')}}


with air_raw as
(
	select *,
	case when range >= 5600 then 'High'
	else 'Low'
	end "hights",
	model ->> 'en' as Model_English,
	model ->> 'ru' as Model_Russian,  
    '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
 	from {{source('dim_stg', 'aircrafts_data')}} as aircrafts
),
seat_raw as
(
	select *
	from {{source('dim_stg', 'seats')}}  as seats
),
air_seat as
(
select ar.*, 
sr.seat_no,
sr.fare_conditions
from air_raw ar
inner join seat_raw sr
	on sr.aircraft_code = ar.aircraft_code
)
select 
aircraft_code,
model,
Model_English,
Model_Russian,
range,
hights,
seat_no,
fare_conditions,
dbt_time
from air_seat