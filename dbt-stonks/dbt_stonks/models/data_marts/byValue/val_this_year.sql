{{ config(schema='byValue') }}

with tbl as 
(select extract(year from max(call_at)) as year,
        ticker_symbol,
        stock_name,
        industry,
        round(avg(stock_index), 2) as avg_index
 from {{ ref('stg_sp500') }}  
 where extract(year from call_at) = extract(year from current_date()) 
 group by ticker_symbol, stock_name, industry
 order by avg_index desc)
select dense_rank() over(order by tbl.avg_index desc)
       as stock_value_rank,
       tbl.* 
from tbl
order by stock_value_rank 
