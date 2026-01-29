-- Pipeline C: Intermediate Layer
-- int_relative_performance.sql



with portfolio_rets as (
    select * from BAIN_ANALYTICS.DEV.int_portfolio_returns
),
pb_map as (
    select * from BAIN_ANALYTICS.DEV.stg_portfolio_benchmarks
),
benchmark_metrics as (
    select * from BAIN_ANALYTICS.DEV.int_benchmark_metrics
)

select
    pr.portfolio_id,
    pr.valuation_date,
    pr.nav_usd,
    pr.daily_return_pct as portfolio_return,
    bm.daily_return as benchmark_return,
    pr.daily_return_pct - bm.daily_return as excess_return,
    avg(pr.daily_return_pct - bm.daily_return) over (partition by pr.portfolio_id order by pr.valuation_date rows between 89 preceding and current row) as excess_return_90d
from portfolio_rets pr
left join pb_map pbm on pr.portfolio_id = pbm.portfolio_id and pbm.is_primary = true
left join benchmark_metrics bm on pbm.benchmark_id = bm.benchmark_id and pr.valuation_date = bm.return_date