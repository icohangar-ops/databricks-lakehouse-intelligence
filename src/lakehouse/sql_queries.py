"""SQL query templates for Lakehouse Intelligence dashboards."""

from .config import LakehouseConfig


def get_top_signals_query(
    config: LakehouseConfig | None = None,
    limit: int = 10,
) -> str:
    """Top companies by weighted signal score."""
    cfg = config or LakehouseConfig()
    return f"""
SELECT
    company_name,
    period,
    grade_score,
    cost_score,
    production_score,
    growth_score,
    esg_score,
    weighted_signal,
    tier,
    rank
FROM {cfg.gold_signals_fqn}
WHERE period = (SELECT MAX(period) FROM {cfg.gold_signals_fqn})
ORDER BY weighted_signal DESC
LIMIT {limit};
"""


def get_signal_trend_query(
    config: LakehouseConfig | None = None,
    company_name: str | None = None,
) -> str:
    """Signal score trend over time."""
    cfg = config or LakehouseConfig()
    where_clause = f"WHERE company_name = '{company_name}'" if company_name else ""
    return f"""
SELECT
    period,
    company_name,
    weighted_signal,
    grade_score,
    cost_score,
    production_score,
    growth_score,
    esg_score,
    tier
FROM {cfg.gold_signals_fqn}
{where_clause}
ORDER BY company_name, period;
"""


def get_dimension_distribution_query(
    config: LakehouseConfig | None = None,
    dimension: str = "grade_score",
) -> str:
    """Distribution of a single signal dimension."""
    cfg = config or LakehouseConfig()
    return f"""
SELECT
    CASE
        WHEN {dimension} >= 80 THEN 'A (80-100)'
        WHEN {dimension} >= 65 THEN 'B (65-79)'
        WHEN {dimension} >= 50 THEN 'C (50-64)'
        ELSE 'D (0-49)'
    END AS bucket,
    COUNT(*) AS company_count,
    ROUND(AVG({dimension}), 1) AS avg_score
FROM {cfg.gold_signals_fqn}
WHERE period = (SELECT MAX(period) FROM {cfg.gold_signals_fqn})
GROUP BY bucket
ORDER BY avg_score DESC;
"""


def get_production_summary_query(
    config: LakehouseConfig | None = None,
) -> str:
    """Production volume summary by commodity."""
    cfg = config or LakehouseConfig()
    return f"""
SELECT
    c.commodity,
    COUNT(DISTINCT p.company_id) AS company_count,
    SUM(p.volume_tonnes) AS total_volume,
    ROUND(AVG(p.cost_per_tonne_usd), 2) AS avg_cost_per_tonne,
    ROUND(AVG(p.grade_recovery_pct), 1) AS avg_recovery_pct
FROM {cfg.silver_production_fqn} p
JOIN {cfg.silver_companies_fqn} c
    ON p.company_id = c.company_id
GROUP BY c.commodity
ORDER BY total_volume DESC;
"""


def get_financial_health_query(
    config: LakehouseConfig | None = None,
    limit: int = 10,
) -> str:
    """Financial health metrics summary."""
    cfg = config or LakehouseConfig()
    return f"""
SELECT
    c.name AS company_name,
    f.quarter,
    f.revenue_millions,
    f.ebitda_millions,
    ROUND(f.ebitda_millions / NULLIF(f.revenue_millions, 0) * 100, 1) AS ebitda_margin_pct,
    f.free_cash_flow_millions,
    f.debt_to_equity,
    f.roic_pct,
    f.dividend_yield_pct
FROM {cfg.silver_financials_fqn} f
JOIN {cfg.silver_companies_fqn} c
    ON f.company_id = c.company_id
WHERE f.quarter = (SELECT MAX(quarter) FROM {cfg.silver_financials_fqn})
ORDER BY f.ebitda_millions DESC
LIMIT {limit};
"""


def get_tier_breakdown_query(
    config: LakehouseConfig | None = None,
) -> str:
    """Tier distribution for latest period."""
    cfg = config or LakehouseConfig()
    return f"""
SELECT
    tier,
    COUNT(*) AS company_count,
    ROUND(AVG(weighted_signal), 1) AS avg_signal,
    ROUND(MIN(weighted_signal), 1) AS min_signal,
    ROUND(MAX(weighted_signal), 1) AS max_signal
FROM {cfg.gold_signals_fqn}
WHERE period = (SELECT MAX(period) FROM {cfg.gold_signals_fqn})
GROUP BY tier
ORDER BY tier;
"""


def get_bronze_row_counts_query(
    config: LakehouseConfig | None = None,
) -> str:
    """Row counts for bronze tables."""
    cfg = config or LakehouseConfig()
    return f"""
SELECT 'mining_companies' AS table_name, COUNT(*) AS row_count FROM {cfg.bronze_companies_fqn}
UNION ALL
SELECT 'production_records', COUNT(*) FROM {cfg.bronze_production_fqn}
UNION ALL
SELECT 'financial_metrics', COUNT(*) FROM {cfg.bronze_financials_fqn};
"""
