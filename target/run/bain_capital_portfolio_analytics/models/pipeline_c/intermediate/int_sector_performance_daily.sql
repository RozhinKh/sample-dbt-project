
  create or replace   view BAIN_ANALYTICS.DEV.int_sector_performance_daily
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_sector_performance_daily.sql
-- Purpose: Daily sector allocation and performance



with sectors as (
    select * from BAIN_ANALYTICS.DEV.int_sector_allocation
)

select
    portfolio_id,
    position_date,
    sector,
    position_count,
    round(sector_value, 2) as sector_value,
    round(total_quantity, 2) as total_quantity,
    round(sector_weight, 4) as sector_weight,
    round(sector_weight * 100, 2) as sector_weight_pct,
    lag(sector_weight) over (partition by portfolio_id, sector order by position_date) as prev_sector_weight,
    round((sector_weight - lag(sector_weight) over (partition by portfolio_id, sector order by position_date)) * 100, 2) as sector_weight_change_pct
from sectors
  );

