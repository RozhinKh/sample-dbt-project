
  create or replace   view BAIN_ANALYTICS.DEV.int_portfolio_analysis_advanced
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_portfolio_analysis_advanced.sql
-- Purpose: Advanced portfolio analysis with risk decomposition and performance attribution



with daily_metrics as (
    select * from BAIN_ANALYTICS.DEV.int_portfolio_returns
),

rolling_calcs as (
    select
        portfolio_id,
        valuation_date,
        nav_usd,
        daily_return_pct,
        stddev_pop(daily_return_pct) over (
            partition by portfolio_id
            order by valuation_date
            rows between 89 preceding and current row
        ) as volatility_90d,
        avg(daily_return_pct) over (
            partition by portfolio_id
            order by valuation_date
            rows between 89 preceding and current row
        ) as avg_return_90d,
        max(daily_return_pct) over (
            partition by portfolio_id
            order by valuation_date
            rows between 89 preceding and current row
        ) as max_return_90d,
        min(daily_return_pct) over (
            partition by portfolio_id
            order by valuation_date
            rows between 89 preceding and current row
        ) as min_return_90d,
        sum(case when daily_return_pct > 0 then 1 else 0 end) over (
            partition by portfolio_id
            order by valuation_date
            rows between 89 preceding and current row
        ) as positive_days_90d,
        sum(case when daily_return_pct < 0 then 1 else 0 end) over (
            partition by portfolio_id
            order by valuation_date
            rows between 89 preceding and current row
        ) as negative_days_90d
    from daily_metrics
)

select
    portfolio_id,
    valuation_date,
    round(nav_usd, 2) as nav_usd,
    round(100 * daily_return_pct, 4) as daily_return_pct,
    round(100 * volatility_90d, 4) as volatility_90d_pct,
    round(100 * avg_return_90d, 4) as avg_daily_return_90d_pct,
    round(100 * max_return_90d, 4) as max_daily_return_90d_pct,
    round(100 * min_return_90d, 4) as min_daily_return_90d_pct,
    positive_days_90d,
    negative_days_90d,
    round(100 * positive_days_90d / nullif(positive_days_90d + negative_days_90d, 0), 4) as win_rate_pct,
    case
        when volatility_90d > 0 then round(avg_return_90d / nullif(volatility_90d, 0), 8)
        else 0
    end as sharpe_ratio_simple,
    round(max_return_90d - min_return_90d, 8) as return_range_90d,
    case
        when volatility_90d < 0.005 then 'ULTRA_LOW_RISK'
        when volatility_90d < 0.010 then 'LOW_RISK'
        when volatility_90d < 0.020 then 'MODERATE_RISK'
        when volatility_90d < 0.030 then 'HIGH_RISK'
        else 'VERY_HIGH_RISK'
    end as risk_classification
from rolling_calcs
  );

