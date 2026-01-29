
  create or replace   view BAIN_ANALYTICS.DEV.int_position_lifecycle
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_position_lifecycle.sql
-- Purpose: Track position lifecycle stages and transitions



with pos_history as (
    select
        position_id,
        portfolio_id,
        security_id,
        ticker,
        security_name,
        asset_class,
        sector,
        position_date,
        quantity,
        market_value_usd,
        daily_return_pct,
        row_number() over (partition by position_id order by position_date) as position_day_number,
        max(position_date) over (partition by position_id) as last_date,
        min(position_date) over (partition by position_id) as first_date,
        lag(quantity) over (partition by position_id order by position_date) as prev_quantity,
        lag(market_value_usd) over (partition by position_id order by position_date) as prev_market_value,
        lead(quantity) over (partition by position_id order by position_date) as next_quantity
    from BAIN_ANALYTICS.DEV.int_position_returns
)

select
    position_id,
    portfolio_id,
    security_id,
    ticker,
    security_name,
    asset_class,
    sector,
    position_date,
    quantity,
    market_value_usd,
    daily_return_pct,
    position_day_number,
    datediff(day, first_date, position_date) as days_in_portfolio,
    datediff(day, first_date, last_date) as position_duration_days,
    case
        when position_day_number = 1 then 'OPENED'
        when next_quantity is null then 'CLOSED'
        when quantity > prev_quantity then 'INCREASED'
        when quantity < prev_quantity then 'DECREASED'
        when quantity = prev_quantity and market_value_usd != prev_market_value then 'REVALUED'
        else 'HELD'
    end as position_stage,
    case
        when position_day_number <= 5 then 'EARLY'
        when datediff(day, first_date, position_date) < datediff(day, first_date, last_date) / 2 then 'GROWTH'
        when datediff(day, first_date, position_date) >= datediff(day, first_date, last_date) / 2
             and next_quantity is not null then 'MATURITY'
        else 'EXIT'
    end as lifecycle_stage
from pos_history
  );

