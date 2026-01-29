-- Pipeline C: Intermediate Layer
-- int_position_momentum.sql
-- Purpose: Analyze position-level momentum and trend metrics



with pos_perf as (
    select * from BAIN_ANALYTICS.DEV.int_position_returns
),

momentum_calc as (
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
        daily_return_pct,
        sum(daily_return_pct) over (
            partition by position_id
            order by position_date
            rows between 4 preceding and current row
        ) as return_5d,
        sum(daily_return_pct) over (
            partition by position_id
            order by position_date
            rows between 19 preceding and current row
        ) as return_20d,
        sum(daily_return_pct) over (
            partition by position_id
            order by position_date
            rows between 89 preceding and current row
        ) as return_90d,
        lag(market_value_usd) over (partition by position_id order by position_date) as prev_market_value,
        lead(market_value_usd) over (partition by position_id order by position_date) as next_market_value,
        rank() over (partition by position_id order by daily_return_pct desc) as return_rank_desc,
        row_number() over (partition by position_id order by position_date) as day_number
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
    daily_return_pct,
    round(return_5d, 8) as return_5d,
    round(return_20d, 8) as return_20d,
    round(return_90d, 8) as return_90d,
    round(100 * return_5d, 4) as return_5d_pct,
    round(100 * return_20d, 4) as return_20d_pct,
    round(100 * return_90d, 4) as return_90d_pct,
    round((market_value_usd - prev_market_value) / nullif(prev_market_value, 0), 8) as value_change_pct,
    case
        when return_90d > 0.10 then 'STRONG_POSITIVE'
        when return_90d > 0.05 then 'POSITIVE'
        when return_90d > -0.05 then 'NEUTRAL'
        when return_90d > -0.10 then 'NEGATIVE'
        else 'STRONG_NEGATIVE'
    end as momentum_rating
from momentum_calc