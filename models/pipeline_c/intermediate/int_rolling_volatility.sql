-- Pipeline C: Intermediate Layer
-- int_rolling_volatility.sql
-- Purpose: Calculate rolling volatility over different time windows

{{ config(
    materialized='view',
    tags=['intermediate', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'intermediate'}
) }}

with daily_returns as (
    select * from {{ ref('int_portfolio_returns') }}
),

rolling_stats as (
    select
        portfolio_id,
        valuation_date,
        nav_usd,
        daily_return_pct,
        stddev_pop(daily_return_pct) over (
            partition by portfolio_id
            order by valuation_date
            rows between 29 preceding and current row
        ) as volatility_30d,
        stddev_pop(daily_return_pct) over (
            partition by portfolio_id
            order by valuation_date
            rows between 89 preceding and current row
        ) as volatility_90d,
        stddev_pop(daily_return_pct) over (
            partition by portfolio_id
            order by valuation_date
            rows between 199 preceding and current row
        ) as volatility_200d,
        avg(daily_return_pct) over (
            partition by portfolio_id
            order by valuation_date
            rows between 29 preceding and current row
        ) as avg_return_30d,
        avg(daily_return_pct) over (
            partition by portfolio_id
            order by valuation_date
            rows between 89 preceding and current row
        ) as avg_return_90d
    from daily_returns
)

select
    portfolio_id,
    valuation_date,
    nav_usd,
    daily_return_pct,
    round(volatility_30d, 8) as volatility_30d,
    round(volatility_90d, 8) as volatility_90d,
    round(volatility_200d, 8) as volatility_200d,
    round(avg_return_30d, 8) as avg_return_30d,
    round(avg_return_90d, 8) as avg_return_90d,
    round(100 * volatility_30d, 4) as volatility_30d_pct,
    round(100 * volatility_90d, 4) as volatility_90d_pct,
    round(100 * volatility_200d, 4) as volatility_200d_pct,
    round(100 * avg_return_30d, 4) as avg_return_30d_pct,
    round(100 * avg_return_90d, 4) as avg_return_90d_pct
from rolling_stats
