
  create or replace   view BAIN_ANALYTICS.DEV.int_benchmark_metrics
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_benchmark_metrics.sql



with returns as (
    select * from BAIN_ANALYTICS.DEV.stg_benchmark_returns
),

metrics as (
    select
        benchmark_id,
        return_date,
        daily_return,
        avg(daily_return) over (partition by benchmark_id order by return_date rows between 89 preceding and current row) as return_90d,
        avg(daily_return) over (partition by benchmark_id order by return_date rows between 364 preceding and current row) as return_1y,
        stddev(daily_return) over (partition by benchmark_id order by return_date rows between 89 preceding and current row) as volatility_90d
    from returns
)

select * from metrics
  );

