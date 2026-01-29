-- Pipeline C: Intermediate Layer
-- int_portfolio_vs_benchmark.sql
-- Purpose: Compare portfolio performance against benchmarks



with port_perf as (
    select * from BAIN_ANALYTICS.DEV.int_portfolio_returns
),

bench_perf as (
    select
        benchmark_id,
        valuation_date,
        benchmark_return_pct
    from BAIN_ANALYTICS.DEV.stg_benchmark_returns
),

port_bench_map as (
    select
        portfolio_id,
        benchmark_id,
        valuation_date
    from BAIN_ANALYTICS.DEV.stg_portfolio_benchmarks
)

select
    pp.portfolio_id,
    pp.valuation_date,
    pp.nav_usd as portfolio_nav,
    pp.daily_return_pct as portfolio_return_pct,
    bp.benchmark_return_pct,
    round(pp.daily_return_pct - bp.benchmark_return_pct, 8) as excess_return_pct,
    round(100 * (pp.daily_return_pct - bp.benchmark_return_pct), 4) as excess_return_bps,
    case
        when pp.daily_return_pct > bp.benchmark_return_pct then 'OUTPERFORM'
        when pp.daily_return_pct < bp.benchmark_return_pct then 'UNDERPERFORM'
        else 'IN_LINE'
    end as performance_vs_benchmark,
    pbm.benchmark_id
from port_perf pp
left join port_bench_map pbm
    on pp.portfolio_id = pbm.portfolio_id
    and pp.valuation_date = pbm.valuation_date
left join bench_perf bp
    on pbm.benchmark_id = bp.benchmark_id
    and pp.valuation_date = bp.valuation_date