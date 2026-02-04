
  create or replace   view BAIN_ANALYTICS.DEV.int_trade_metrics
  
  
  
  
  as (
    -- Pipeline B: Intermediate Layer
-- int_trade_metrics.sql
-- Purpose: Calculate trade-level metrics and PnL



with enriched as (
    select * from BAIN_ANALYTICS.DEV.int_trades_enriched
),

trade_values as (
    select
        trade_id,
        portfolio_id,
        security_id,
        broker_id,
        trade_date,
        trade_type,
        quantity,
        price,
        commission,
        ticker,
        security_name,
        asset_class,
        sector,
        broker_name,
        round(quantity * price, 2) as trade_value,
        round(quantity * price - commission, 2) as net_trade_value,
        round(case
            when trade_type = 'BUY' then -1 * (quantity * price)
            when trade_type = 'SELL' then quantity * price
            else 0
        end, 2) as position_impact
    from enriched
)

select
    trade_id,
    portfolio_id,
    security_id,
    broker_id,
    trade_date,
    trade_type,
    quantity,
    price,
    commission,
    ticker,
    security_name,
    asset_class,
    sector,
    broker_name,
    trade_value,
    net_trade_value,
    position_impact
from trade_values
  );

