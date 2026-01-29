
  create or replace   view BAIN_ANALYTICS.DEV.fact_trades
  
  
  
  
  as (
    -- Pipeline B: Fact Layer
-- fact_trades.sql
-- Purpose: Detailed trade fact table with all metrics



select * from BAIN_ANALYTICS.DEV.int_trade_metrics
  );

