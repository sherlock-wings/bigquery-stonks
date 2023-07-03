with stg_sp500 as (
    select call_at,
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
    from {{source('realStonks_api_data', 'sp500')}}       
)

select * from stg_sp500
