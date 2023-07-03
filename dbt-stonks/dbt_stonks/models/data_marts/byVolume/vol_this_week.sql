{{ config(schema='byVolume') }}

with tbl as 
(select extract(year from max(call_at)) as year,
        extract(quarter from max(call_at)) as quarter,
        format_timestamp('%B', max(call_at)) as month,
        extract(week from max(call_at)) as week,
        ticker_symbol,
        stock_name,
        industry,
        avg(total_volume) as avg_volume
 from {{source('realStonks_api_data', 'stg_sp500')}} 
 where extract(quarter from call_at) = extract(quarter from current_date()) 
 group by ticker_symbol, stock_name, industry
 order by avg_volume desc
 limit 25)
select dense_rank() over(order by tbl.avg_volume desc)
       as stock_volume_rank,
       tbl.* 
from tbl
order by stock_volume_rank 
