{{ config(materialized = 'incremental') }}

with sp500_init as (
    select call_at,
           case 
             when extract(minute from call_at) <= 14
              and extract(second from call_at) <= 59
            then datetime(
              extract(year from call_at),
              extract(month from call_at),
              extract(day from call_at),
              extract(hour from call_at),
              00,
              00
            )
             when extract(minute from call_at) between 15 and 44
              and extract(second from call_at) <= 59
            then datetime(
              extract(year from call_at),
              extract(month from call_at),
              extract(day from call_at),
              extract(hour from call_at),
              30,
              00
            )
            when extract(minute from call_at) between 45 and 59
              and extract(second from call_at) <= 59
            then datetime(
              extract(year from timestamp_add(call_at, interval 1 hour)),
              extract(month from timestamp_add(call_at, interval 1 hour)),
              extract(day from timestamp_add(call_at, interval 1 hour)),
              extract(hour from timestamp_add(call_at, interval 1 hour)),
              00,
              00
            )
           end as aprx_datetime, 
           extract(year from call_at) as call_at_year, 
           extract(quarter from call_at) as call_at_quarter,
           format_timestamp('%B', call_at) as call_at_month,
           extract(week from call_at) as call_at_week,
           format_timestamp('%A', call_at) as call_at_weekday,
           ticker_symbol,
           stock_name,
           sector,
           industry,
           split(hq_location, ', ')[offset(0)] as hq_city,
           case
             when split(hq_location, ', ')[offset(1)] in unnest({{ var('USA_states')}})
             then split(hq_location, ', ')[offset(1)]
           end as hq_state,
           case
             when split(hq_location, ', ')[offset(1)] in unnest({{ var('USA_states')}})
             then 'United States of America'
             else split(hq_location, ', ')[offset(1)]
           end as hq_country,
           case 
             when total_vol like "%K"
             then cast(substr(total_vol, 1, length(total_vol)-1) as float64)*1000
             when total_vol like "%M"
             then cast(substr(total_vol, 1, length(total_vol)-1) as float64)*1000000
           end as total_volume,
           price, 
           change_point,
           change_percentage  
    from {{ source('bq_instance', 'sp500') }}       
)

select s.*,
       m.market_cap,
       round(
             (m.market_cap/
             (select sum(market_cap) from {{ source('bq_instance', 'market_caps') }})
             )*s.price,
             2
            ) as stock_index 
from sp500_init s
join {{ source('bq_instance', 'market_caps') }} m
 on s.ticker_symbol = m.ticker_symbol


{% if is_incremental() %}

where s.call_at > (select max(call_at) from {{ this }})

{% endif %}