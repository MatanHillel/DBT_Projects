{{ config(materialized='incremental',unique_key='flight_id') }}


with flights as
(
	select *,
	age(scheduled_arrival,scheduled_departure) as flight_duration_scheduled,
	age(actual_arrival,actual_departure) as flight_duration_actual
	from {{ source('fact_stg','flights' )}}
	where 1=1
),
dim_aircrafts as 
(
	select * from {{ ref('dim_aircraft') }}
),
dim_airports as 
(
	select * from {{ ref('dim_airport') }}
),
aggs_flights as
(
	select 
		flights.flight_id,
	 	flights.flight_no,
	 	flights.aircraft_code,
	 	flights.departure_airport,
	 	flights.scheduled_departure,
	 	flights.actual_departure,
	 	flights.arrival_airport,
	 	flights.scheduled_arrival,
	 	flights.actual_arrival,
	 	flights.flight_duration_scheduled,
	 	flights.flight_duration_actual,
		-- dim_airports.coordinates,
		-- dim_airports.timezone,
		-- dim_airports.english_airport_name,
		-- dim_airports.russian_airport_name,
		-- dim_airports.english_city_name,
		-- dim_airports.russian_city_name,
		-- dim_aircrafts.Model_English,
		-- dim_aircrafts.Model_Russian,
		-- dim_aircrafts.range,
		-- dim_aircrafts.hights,
		-- dim_aircrafts.seat_no,
		-- dim_aircrafts.fare_conditions,
		case when
		flights.flight_duration_actual is null then 'no_info'
		else 'check_flight_duration_actual'
		end as departed_or_not,
		case when
		flights.flight_duration_scheduled < flights.flight_duration_actual then 'longer'
		when flights.flight_duration_scheduled > flights.flight_duration_actual then 'shorter'
		when flights.flight_duration_actual = flights.flight_duration_scheduled then 'as_expected'
		else 'no_info'
		end as in_time_or_not,
        '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
	from flights
	left join dim_aircrafts
	on dim_aircrafts.aircraft_code = flights.aircraft_code
	left join dim_airports
	on dim_airports.airport_code = flights.departure_airport
)
select 
distinct *
from aggs_flights



-- 	 aggs_flights.flight_id,
-- 	 aggs_flights.flight_no,
-- 	 aggs_flights.aircraft_code,
-- 	 aggs_flights.departure_airport,
-- 	 scheduled_departure,
-- 	 actual_departure,
-- 	 arrival_airport,
-- 	 scheduled_arrival,
-- 	 actual_arrival,
-- 	 flight_duration_scheduled,
-- 	 flight_duration_actual,
-- 	 departed_or_not,
-- 	 in_time_or_not,
--      aggs_flights.dbt_time
-- from aggs_flights