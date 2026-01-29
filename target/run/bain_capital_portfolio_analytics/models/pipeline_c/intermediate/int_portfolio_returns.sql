
  create or replace   view BAIN_ANALYTICS.DEV.int_portfolio_returns
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_portfolio_returns.sql



with valuations as (
    select * from BAIN_ANALYTICS.DEV.stg_valuations
),

returns as (
    select
        portfolio_id,
        valuation_date,
        nav,
        nav_usd,
        lag(nav_usd) over (partition by portfolio_id order by valuation_date) as prev_nav,
        nav_usd - lag(nav_usd) over (partition by portfolio_id order by valuation_date) as daily_pnl,
        case
            when lag(nav_usd) over (partition by portfolio_id order by valuation_date) > 0
            then (nav_usd - lag(nav_usd) over (partition by portfolio_id order by valuation_date)) / lag(nav_usd) over (partition by portfolio_id order by valuation_date)
            else 0
        end as daily_return_pct,
        extract(year from valuation_date) as valuation_year,
        extract(month from valuation_date) as valuation_month,
        extract(quarter from valuation_date) as valuation_quarter
    from valuations
)

select * from returns
  );

