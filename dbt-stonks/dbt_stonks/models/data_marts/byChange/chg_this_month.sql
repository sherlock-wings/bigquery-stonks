{{ config(schema='byChange') }}

with bg as(
  select min(call_at) as window_start,
         ticker_symbol,
         stock_name,
         industry
  from {{ ref('stg_sp500') }}
  where format_date('%G-%m', call_at) = format_date('%G-%m', current_date())
  group by 2, 3, 4 
),

ed as (
  select max(call_at) as window_end,
         ticker_symbol,
         stock_name,
         industry
  from {{ ref('stg_sp500') }}
  where format_date('%G-%m', call_at) = format_date('%G-%m', current_date())
  group by 2, 3, 4 
),

bte as (
    select bg.ticker_symbol,
           bg.stock_name,
           bg.industry,
           bg.window_start,
           ed.window_end
    from bg
    join ed on bg.ticker_symbol = ed.ticker_symbol 
),

chg as (
    select bte.ticker_symbol,
           bte.stock_name,
           bte.industry,
           bte.window_start,
           s1.price as starting_price,
           bte.window_end,
           s2.price as ending_price,
           round(s2.price-s1.price, 2) as point_change,
           round(((s2.price-s1.price)/s1.price)*100, 1) as pcnt_change
    from bte
    join {{ ref('stg_sp500') }} s1
      on s1.ticker_symbol = bte.ticker_symbol
       and s1.call_at = bte.window_start
    join {{ ref('stg_sp500') }}s2
      on s2.ticker_symbol = bte.ticker_symbol
       and s2.call_at = bte.window_end 
)

select dense_rank() over(order by pcnt_change desc)
       as value_change_rank,
       extract(year from chg.window_start) as year,
       extract(quarter from chg.window_start) as quarter,
       extract(month from chg.window_start) as month,
       chg.ticker_symbol,
       chg.stock_name,
       chg.industry,
       chg.starting_price,
       chg.ending_price,
       chg.point_change,
       chg.pcnt_change
from chg
order by value_change_rank