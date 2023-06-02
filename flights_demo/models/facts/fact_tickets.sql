{{ config(materialized='incremental',unique_key='ticket_no') }}


with bookings as
(
	select 
	book_date,
	total_amount,
	book_ref
	from {{ source('fact_stg','bookings') }} 
), 
tickets as
(
	select 
    *,
	contact_data ->> 'phone' as phone_number,
	contact_data ->> 'email' as email_adress
	from {{ source('fact_stg','tickets') }}  
),
joins as
(
	select 	
    ticket_no,
	bookings.book_ref,
	passenger_id,
	passenger_name,
    phone_number,
    email_adress,
    '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
	from bookings 
	left join tickets  on bookings.book_ref = tickets.book_ref 
)
select*
from joins
