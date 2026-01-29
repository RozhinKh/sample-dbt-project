-- Pipeline C: Intermediate Layer
-- int_calendar_performance.sql
-- Purpose: Analyze performance by calendar periods (daily, monthly, quarterly, yearly)



with port_perf as (
    select * from BAIN_ANALYTICS.DEV.int_portfolio_returns
),

daily_perf as (
    select
        portfolio_id,
        valuation_date,
        extract(year from valuation_date) as perf_year,
        extract(month from valuation_date) as perf_month,
        extract(quarter from valuation_date) as perf_quarter,
        extract(week from valuation_date) as perf_week,
        dayofweek(valuation_date) as day_of_week,
        nav_usd,
        daily_return_pct,
        sum(daily_return_pct) over (
            partition by portfolio_id, extract(year from valuation_date), extract(month from valuation_date)
            order by valuation_date
        ) as month_to_date_return,
        sum(daily_return_pct) over (
            partition by portfolio_id, extract(year from valuation_date), extract(quarter from valuation_date)
            order by valuation_date
        ) as quarter_to_date_return,
        sum(daily_return_pct) over (
            partition by portfolio_id, extract(year from valuation_date)
            order by valuation_date
        ) as year_to_date_return
    from port_perf
)

select
    portfolio_id,
    valuation_date,
    perf_year,
    perf_month,
    perf_quarter,
    perf_week,
    day_of_week,
    case when day_of_week = 1 then 'Monday'
         when day_of_week = 2 then 'Tuesday'
         when day_of_week = 3 then 'Wednesday'
         when day_of_week = 4 then 'Thursday'
         when day_of_week = 5 then 'Friday'
         when day_of_week = 6 then 'Saturday'
         else 'Sunday'
    end as day_name,
    nav_usd,
    daily_return_pct,
    round(month_to_date_return, 8) as month_to_date_return,
    round(quarter_to_date_return, 8) as quarter_to_date_return,
    round(year_to_date_return, 8) as year_to_date_return,
    round(100 * daily_return_pct, 4) as daily_return_pct_display,
    round(100 * month_to_date_return, 4) as month_to_date_return_pct,
    round(100 * quarter_to_date_return, 4) as quarter_to_date_return_pct,
    round(100 * year_to_date_return, 4) as year_to_date_return_pct
from daily_perf