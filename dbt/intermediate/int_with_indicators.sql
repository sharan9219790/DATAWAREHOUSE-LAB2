with base as (
  select * from {{ ref('stg_stock_prices') }}
),
ma as (
  select
    symbol,
    date,
    close,
    avg(close) over (partition by symbol order by date
                     rows between 19 preceding and current row) as ma20,
    avg(close) over (partition by symbol order by date
                     rows between 49 preceding and current row) as ma50
  from base
),
rsi_calc as (
  select
    symbol,
    date,
    close,
    ma20,
    ma50,
    greatest(close - lag(close) over (partition by symbol order by date), 0) as gain,
    greatest(lag(close) over (partition by symbol order by date) - close, 0) as loss
  from ma
),
rsi_smoothed as (
  select
    symbol,
    date,
    close,
    ma20,
    ma50,
    avg(gain) over (partition by symbol order by date rows between 13 preceding and current row) as avg_gain,
    avg(loss) over (partition by symbol order by date rows between 13 preceding and current row) as avg_loss
  from rsi_calc
)
select
  symbol, date, close, ma20, ma50,
  case when avg_loss = 0 then 100
       else 100 - (100 / (1 + (avg_gain / nullif(avg_loss,0))))
  end as rsi14
from rsi_smoothed
