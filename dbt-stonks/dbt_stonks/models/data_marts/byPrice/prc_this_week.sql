{{ config(schema='byPrice') }}

with tbl as 
(select extract(year from max(call_at)) as year,
        extract(quarter from max(call_at)) as quarter,
        format_timestamp('%B', max(call_at)) as month,
        extract(week from max(call_at)) as week,
        ticker_symbol,
        stock_name,
        industry,
        round(avg(price), 2) as avg_value_USD
 from {{source('realStonks_api_data', 'stg_sp500')}} 
 where extract(quarter from call_at) = extract(quarter from current_date()) 
 group by ticker_symbol, stock_name, industry
 order by avg_value_USD desc
 limit 25)
select dense_rank() over(order by tbl.avg_value_USD desc)
       as stock_value_rank,
       tbl.* 
from tbl
order by stock_value_rank 
