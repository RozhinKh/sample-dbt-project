
  create or replace   view BAIN_ANALYTICS.DEV.mart_security_analysis
  
  
  
  
  as (
    -- Pipeline B: Mart Layer
-- mart_security_analysis.sql
-- Purpose: Security-level trading analysis



select * from BAIN_ANALYTICS.DEV.int_security_performance
  );

