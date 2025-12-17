select
  cast(date as date) as date,
  cast(open as float) as open,
  cast(high as float) as high,
  cast(low as float) as low,
  cast(close as float) as close,
  cast(adj_close as float) as adj_close,
  cast(volume as number) as volume,
  upper(symbol) as symbol
from {{ source('raw', 'STOCK_PRICES') }}

