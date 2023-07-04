{{ config(schema='byPrice') }}

with tbl as 
(select date(max(call_at)) as date,
        ticker_symbol,
        stock_name,
        industry,
        round(avg(price), 0) as avg_value_USD
 from  {{ ref('stg_sp500') }} 
 where date(call_at) = current_date() 
 group by ticker_symbol, stock_name, industry)
select dense_rank() over(order by tbl.avg_value_USD desc)
       as stock_value_rank,
       tbl.* 
from tbl
order by stock_value_rank 