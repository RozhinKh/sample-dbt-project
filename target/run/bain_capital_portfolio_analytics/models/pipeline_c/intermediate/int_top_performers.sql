
  create or replace   view BAIN_ANALYTICS.DEV.int_top_performers
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_top_performers.sql
-- Purpose: Identify and rank top and bottom performing positions and sectors



with pos_perf as (
    select * from BAIN_ANALYTICS.DEV.int_position_returns
),

pos_metrics as (
    select
        position_id,
        portfolio_id,
        security_id,
        ticker,
        security_name,
        asset_class,
        sector,
        position_date,
        market_value_usd,
        daily_pnl,
        daily_return_pct,
        rank() over (partition by position_date order by daily_return_pct desc) as daily_return_rank_asc,
        rank() over (partition by position_date order by daily_return_pct asc) as daily_return_rank_desc,
        rank() over (partition by position_date order by daily_pnl desc) as daily_pnl_rank_asc,
        row_number() over (partition by position_date order by daily_return_pct desc) as return_rn_asc,
        row_number() over (partition by position_date order by daily_return_pct asc) as return_rn_desc,
        sum(daily_return_pct) over (
            partition by position_id
            order by position_date
            rows between 19 preceding and current row
        ) as return_20d_cumulative
    from pos_perf
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
    market_value_usd,
    daily_pnl,
    daily_return_pct,
    round(100 * daily_return_pct, 4) as daily_return_pct_display,
    round(100 * return_20d_cumulative, 4) as return_20d_cumulative_pct,
    daily_return_rank_asc,
    daily_return_rank_desc,
    daily_pnl_rank_asc,
    case
        when return_rn_asc <= 5 then 'TOP_5_PERFORMERS'
        when return_rn_asc <= 10 then 'TOP_10_PERFORMERS'
        when return_rn_desc <= 5 then 'BOTTOM_5_PERFORMERS'
        when return_rn_desc <= 10 then 'BOTTOM_10_PERFORMERS'
        else 'MIDDLE_PERFORMERS'
    end as performance_tier
from pos_metrics
  );

