-- Pipeline C: Report Layer
-- report_calendar_performance.sql
-- Purpose: Performance analysis by calendar periods (daily, monthly, quarterly, yearly)



select
    portfolio_id,
    valuation_date,
    perf_year,
    perf_month,
    perf_quarter,
    perf_week,
    day_name,
    nav_usd,
    daily_return_pct_display,
    month_to_date_return_pct,
    quarter_to_date_return_pct,
    year_to_date_return_pct
from BAIN_ANALYTICS.DEV.int_calendar_performance