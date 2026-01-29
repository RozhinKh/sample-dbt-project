-- Pipeline C: Fact Layer
-- fact_sector_performance.sql



select
    portfolio_id,
    position_date,
    sector,
    position_count,
    sector_value,
    total_quantity,
    sector_weight
from BAIN_ANALYTICS.DEV.int_sector_allocation