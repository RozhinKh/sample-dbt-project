-- Pipeline C: Intermediate Layer
-- int_sector_rotation_analysis.sql
-- Purpose: Analyze sector allocation changes and rotation patterns



with sector_daily as (
    select
        portfolio_id,
        position_date,
        sector,
        sum(market_value_usd) as sector_value,
        sum(daily_pnl) as sector_pnl,
        count(distinct position_id) as position_count
    from BAIN_ANALYTICS.DEV.int_position_returns
    group by 1, 2, 3
),

port_totals as (
    select
        portfolio_id,
        position_date,
        sum(market_value_usd) as total_value
    from BAIN_ANALYTICS.DEV.int_position_returns
    group by 1, 2
),

with_weights as (
    select
        sd.portfolio_id,
        sd.position_date,
        sd.sector,
        sd.sector_value,
        sd.position_count,
        round(sd.sector_value / nullif(pt.total_value, 0), 8) as current_weight,
        lag(sd.sector_value / nullif(pt.total_value, 0)) over (
            partition by sd.portfolio_id, sd.sector
            order by sd.position_date
        ) as prior_weight,
        lead(sd.sector_value / nullif(pt.total_value, 0)) over (
            partition by sd.portfolio_id, sd.sector
            order by sd.position_date
        ) as next_weight,
        sd.sector_pnl,
        pt.total_value
    from sector_daily sd
    join port_totals pt
        on sd.portfolio_id = pt.portfolio_id
        and sd.position_date = pt.position_date
)

select
    portfolio_id,
    position_date,
    sector,
    sector_value,
    position_count,
    round(100 * current_weight, 4) as current_weight_pct,
    round(100 * prior_weight, 4) as prior_weight_pct,
    round(100 * (current_weight - nullif(prior_weight, 0)), 4) as weight_change_pct,
    sector_pnl,
    round(sector_pnl / nullif(total_value, 0), 8) as sector_contribution,
    case
        when current_weight > nullif(prior_weight, current_weight) then 'INCREASED'
        when current_weight < nullif(prior_weight, current_weight) then 'DECREASED'
        else 'STABLE'
    end as allocation_change,
    case
        when current_weight > 0.25 then 'OVERWEIGHT'
        when current_weight > 0.15 then 'NEUTRAL_HIGH'
        when current_weight > 0.08 then 'NEUTRAL'
        when current_weight > 0.03 then 'NEUTRAL_LOW'
        else 'UNDERWEIGHT'
    end as allocation_stance,
    round(100 * sector_pnl / nullif(sector_value, 0), 4) as sector_return_pct
from with_weights