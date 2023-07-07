{{ config(schema='byValue') }}

with tbl as 
(select extract(year from max(call_at)) as year,
        extract(quarter from max(call_at)) as quarter,
        format_timestamp('%B', max(call_at)) as month,
        ticker_symbol,
        stock_name,
        industry,
        round(avg(stock_index), 2) as avg_index
 from {{ ref('stg_sp500') }} 
 where format_date('%G-%m', call_at) = format_date('%G-%m', current_date()) 
 group by ticker_symbol, stock_name, industry
 order by avg_index desc)
select dense_rank() over(order by tbl.avg_index desc)
       as stock_value_rank,
       tbl.* 
from tbl
order by stock_value_rank 
