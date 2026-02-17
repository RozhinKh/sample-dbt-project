-- Pipeline C: Intermediate Layer
-- int_position_returns.sql



with enriched as (
    select * from BAIN_ANALYTICS.DEV.int_position_enriched
),

cast_enriched as (
    select
        position_id,
        portfolio_id,
        security_id,
        position_date,
        cast(quantity as numeric(18, 2)) as quantity,
        cast(market_value_usd as numeric(18, 2)) as market_value_usd,
        ticker,
        security_name,
        asset_class,
        sector
    from enriched
),

returns as (
    select
        position_id,
        portfolio_id,
        security_id,
        position_date,
        quantity,
        market_value_usd,
        ticker,
        security_name,
        asset_class,
        sector,
        lag(market_value_usd) over (partition by security_id order by position_date) as prev_value,
        market_value_usd - lag(market_value_usd) over (partition by security_id order by position_date) as daily_pnl,
        case
            when lag(market_value_usd) over (partition by security_id order by position_date) > 0
            then (market_value_usd - lag(market_value_usd) over (partition by security_id order by position_date)) / lag(market_value_usd) over (partition by security_id order by position_date)
            else 0
        end as daily_return_pct
    from cast_enriched
)

select * from returns