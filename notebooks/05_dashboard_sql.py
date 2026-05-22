# Databricks notebook source
# Dashboard Queries for Lakeview Dashboards (Serverless Compatible)
from pyspark.sql.functions import *
CATALOG = "workspace"
print("Dashboard SQL - Analytics Queries")

try:
    print("--- Top Companies by Signal Score ---")
    df = spark.read.table(f"{CATALOG}.lakehouse_gold.mining_signal_scores")
    df.select("company_name", "ticker", "composite_score", "signal_band").orderBy(desc("composite_score")).show(truncate=False)
except Exception as e:
    print(f"Error reading mining_signal_scores: {e}")

try:
    print("--- AISC Benchmark by Commodity ---")
    df2 = spark.read.table(f"{CATALOG}.lakehouse_silver.production_records")
    df2.groupBy("commodity").agg(
        countDistinct("company_id").alias("producers"),
        round(avg("aisc_usd_per_t"), 0).alias("avg_aisc")
    ).orderBy("avg_aisc").show(truncate=False)
except Exception as e:
    print(f"Error reading production_records: {e}")

try:
    print("--- Signal Distribution ---")
    df.groupBy("signal_band").agg(
        count("*").alias("cnt"),
        round(avg("composite_score"), 1).alias("avg_score")
    ).orderBy(desc("avg_score")).show(truncate=False)
except Exception as e:
    print(f"Error signal distribution: {e}")

try:
    print("--- Cross-Domain: Mining + Financial ---")
    df3 = spark.read.table(f"{CATALOG}.lakehouse_gold.cross_domain_intelligence")
    df3.select("company_name", "composite_score", "revenue_m_usd", "ebitda_margin", "debt_to_equity").orderBy(desc("composite_score")).show(10, truncate=False)
except Exception as e:
    print(f"Error reading cross_domain_intelligence: {e}")

print("Dashboard queries complete")
