
  create or replace   view BAIN_ANALYTICS.DEV.report_trading_performance
  
  
  
  
  as (
    -- Pipeline B: Report Layer
-- report_trading_performance.sql
-- Purpose: Trading performance summary by portfolio and security



select * from BAIN_ANALYTICS.DEV.int_trade_summary
  );

