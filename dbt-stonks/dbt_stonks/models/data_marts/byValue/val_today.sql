{{ config(schema='byValue') }}

with tbl as 
(select date(max(call_at)) as date,
        ticker_symbol,
        stock_name,
        industry,
        round(avg(stock_index), 0) as avg_index
 from  {{ ref('stg_sp500') }} 
 where date(call_at) = current_date() 
 group by ticker_symbol, stock_name, industry)
select dense_rank() over(order by tbl.avg_index desc)
       as stock_value_rank,
       tbl.* 
from tbl
order by stock_value_rank 