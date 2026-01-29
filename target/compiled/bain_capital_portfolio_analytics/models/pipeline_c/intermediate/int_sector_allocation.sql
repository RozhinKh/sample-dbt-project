-- Pipeline C: Intermediate Layer
-- int_sector_allocation.sql



with enriched as (
    select * from BAIN_ANALYTICS.DEV.int_position_enriched
),

cast_enriched as (
    select
        portfolio_id,
        position_date,
        sector,
        cast(quantity as numeric(18, 2)) as quantity,
        cast(market_value_usd as numeric(18, 2)) as market_value_usd
    from enriched
),

sector_totals as (
    select
        portfolio_id,
        position_date,
        sector,
        count(*) as position_count,
        sum(market_value_usd) as sector_value,
        sum(quantity) as total_quantity
    from cast_enriched
    group by portfolio_id, position_date, sector
),

portfolio_totals as (
    select
        portfolio_id,
        position_date,
        sum(market_value_usd) as portfolio_value
    from cast_enriched
    group by portfolio_id, position_date
)

select
    st.portfolio_id,
    st.position_date,
    st.sector,
    st.position_count,
    st.sector_value,
    st.total_quantity,
    st.sector_value / nullif(pt.portfolio_value, 0) as sector_weight
from sector_totals st
join portfolio_totals pt on st.portfolio_id = pt.portfolio_id and st.position_date = pt.position_date