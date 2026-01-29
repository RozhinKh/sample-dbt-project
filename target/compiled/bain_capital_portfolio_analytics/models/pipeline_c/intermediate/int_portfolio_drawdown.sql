-- Pipeline C: Intermediate Layer
-- int_portfolio_drawdown.sql
-- Purpose: Calculate maximum drawdown and recovery metrics



with pr as (
    select * from BAIN_ANALYTICS.DEV.int_portfolio_returns
),

running_max as (
    select
        portfolio_id,
        valuation_date,
        nav_usd,
        max(nav_usd) over (partition by portfolio_id order by valuation_date) as running_max_nav
    from pr
)

select
    portfolio_id,
    valuation_date,
    round(nav_usd, 2) as nav_usd,
    round(running_max_nav, 2) as running_max_nav,
    round((nav_usd - running_max_nav) / nullif(running_max_nav, 0), 8) as drawdown_pct,
    round(100 * (nav_usd - running_max_nav) / nullif(running_max_nav, 0), 4) as drawdown_pct_display,
    case
        when (nav_usd - running_max_nav) / nullif(running_max_nav, 0) < -0.10 then 'SEVERE'
        when (nav_usd - running_max_nav) / nullif(running_max_nav, 0) < -0.05 then 'MODERATE'
        when (nav_usd - running_max_nav) / nullif(running_max_nav, 0) < 0 then 'MILD'
        else 'NO_DRAWDOWN'
    end as drawdown_severity
from running_max