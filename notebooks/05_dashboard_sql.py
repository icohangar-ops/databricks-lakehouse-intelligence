# Databricks notebook source
# SQL Dashboard Queries for Lakeview Dashboards
from pyspark.sql.functions import *
CATALOG = "workspace"
print("Dashboard SQL - Analytics Queries")

print("--- Top Companies by Signal Score ---")
spark.sql(f"SELECT company_name,ticker,composite_score,signal_band FROM {CATALOG}.lakehouse_gold.mining_signal_scores ORDER BY composite_score DESC").show(truncate=False)

print("--- AISC Benchmark by Commodity ---")
spark.sql(f"SELECT commodity,COUNT(DISTINCT company_id) as producers,ROUND(AVG(aisc_usd_per_t),0) as avg_aisc FROM {CATALOG}.lakehouse_silver.production_records GROUP BY commodity ORDER BY avg_aisc").show(truncate=False)

print("--- Signal Distribution ---")
spark.sql(f"SELECT signal_band,COUNT(*) as cnt,ROUND(AVG(composite_score),1) as avg_score FROM {CATALOG}.lakehouse_gold.mining_signal_scores GROUP BY signal_band ORDER BY avg_score DESC").show(truncate=False)

print("--- Cross-Domain: Mining + Financial ---")
spark.sql(f"SELECT c.company_name,c.composite_score,f.revenue_m_usd,f.ebitda_margin,f.debt_to_equity FROM {CATALOG}.lakehouse_gold.cross_domain_intelligence c ORDER BY c.composite_score DESC LIMIT 10").show(truncate=False)
print("Dashboard queries ready for Lakeview deployment")
