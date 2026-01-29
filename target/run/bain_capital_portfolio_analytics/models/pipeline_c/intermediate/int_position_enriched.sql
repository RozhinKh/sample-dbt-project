
  create or replace   view BAIN_ANALYTICS.DEV.int_position_enriched
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_position_enriched.sql



with positions as (
    select * from BAIN_ANALYTICS.DEV.stg_positions_daily
),
securities as (
    select * from BAIN_ANALYTICS.DEV.stg_securities
)

select
    p.position_id,
    p.portfolio_id,
    p.security_id,
    p.position_date,
    p.quantity,
    p.market_value_usd,
    s.ticker,
    s.security_name,
    s.asset_class,
    s.sector
from positions p
left join securities s on p.security_id = s.security_id
  );

