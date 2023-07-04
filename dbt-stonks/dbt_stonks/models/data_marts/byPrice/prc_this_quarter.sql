{{ config(schema='byPrice') }}

with tbl as 
(select extract(year from max(call_at)) as year,
        extract(quarter from max(call_at)) as quarter,
        ticker_symbol,
        stock_name,
        industry,
        round(avg(price), 2) as avg_value_USD
 from {{ ref('stg_sp500') }}  
 where format_date('%G-%Q', call_at) = format_date('%G-%Q', current_date()) 
 group by ticker_symbol, stock_name, industry
 order by avg_value_USD desc)
select dense_rank() over(order by tbl.avg_value_USD desc)
       as stock_value_rank,
       tbl.* 
from tbl
order by stock_value_rank 
