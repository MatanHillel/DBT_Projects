{{ config(materialized='incremental',unique_key='ticket_no') }}


with boarding_passes as
(
	select *
	from {{source('fact_stg','boarding_passes')}}
	where 1=1
),
ticket_flights as
(
	select *
	from {{source('fact_stg','ticket_flights')}}
	where 1=1
),
fact_tickets as
(
    select *
    from {{ref('fact_tickets')}}
),
joins_boarding_tickets as
(
	select 
		 ticket_flights.fare_conditions,
		 ticket_flights.amount,
		 boarding_passes.ticket_no,
		 boarding_passes.flight_id,
		 boarding_passes.boarding_no,
		 boarding_passes.seat_no
	from boarding_passes
	left join ticket_flights 
	on boarding_passes.ticket_no = ticket_flights.ticket_no 
	where 1=1
)
select joins_boarding_tickets.*
from joins_boarding_tickets
left join fact_tickets on fact_tickets.ticket_no = joins_boarding_tickets.ticket_no
where 1=1
