
  create or replace   view BAIN_ANALYTICS.DEV.report_sector_analysis
  
  
  
  
  as (
    -- Pipeline C: Report Layer
-- report_sector_analysis.sql
-- Purpose: Sector-level performance and contribution analysis



select
    portfolio_id,
    position_date,
    sector,
    position_count,
    sector_value,
    total_portfolio_value,
    sector_weight_pct,
    sector_pnl,
    sector_return_pct,
    sector_contribution_to_portfolio,
    sector_pnl_contribution_pct
from BAIN_ANALYTICS.DEV.int_sector_performance_attribution
  );

