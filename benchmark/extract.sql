-- Golden Interface: Extract full report from FACT_CASHFLOW_SUMMARY
-- Output format: JSON
-- This is the definitive output that must be preserved across optimizations

SELECT * FROM BAIN_ANALYTICS.DEV.FACT_CASHFLOW_SUMMARY
ORDER BY portfolio_id, cashflow_month, cashflow_type
