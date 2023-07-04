{{ config(schema='byVolume') }}

with tbl as 
(select date(max(call_at)) as date,
        ticker_symbol,
        stock_name,
        industry,
        round(avg(total_volume), 0) as avg_volume
 from {{source('realStonks_api_data', 'stg_sp500')}} 
 where date(call_at) = date(current_date()) 
 group by ticker_symbol, stock_name, industry
 order by avg_volume desc)
select dense_rank() over(order by tbl.avg_volume desc)
       as stock_volume_rank,
       tbl.* 
from tbl
order by stock_volume_rank
