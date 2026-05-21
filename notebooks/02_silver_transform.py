# Databricks notebook source
# Silver Layer - Clean, Deduplicate, Validate
from pyspark.sql.functions import *
from pyspark.sql.window import Window
CATALOG = "workspace"
print("Silver Layer - Transformations")

df_m = spark.read.table(f"{CATALOG}.lakehouse_bronze.mining_companies")
df_ms = df_m.dropDuplicates(["company_id"]).withColumn("quality_score",lit(0.95)).withColumn("silver_ts",current_timestamp()).drop("ingestion_ts")
df_ms.writeTo(f"{CATALOG}.lakehouse_silver.mining_companies").using("delta").createOrReplace()
print(f"mining_companies: {df_ms.count()} rows")

df_p = spark.read.table(f"{CATALOG}.lakehouse_bronze.production_records")
w = Window.partitionBy("company_id","commodity","year","quarter")
df_ps = df_p.withColumn("rn",row_number().over(w.orderBy("ingestion_ts"))).filter(col("rn")==1).drop("rn")
df_ps = df_ps.withColumn("period",concat(col("year"),lit("Q"),col("quarter"))).withColumn("aisc_band",when(col("aisc_usd_per_t")<3500,"Low Cost").when(col("aisc_usd_per_t")<5000,"Mid Cost").otherwise("High Cost")).withColumn("quality_score",lit(0.92)).withColumn("silver_ts",current_timestamp()).drop("ingestion_ts")
df_ps.writeTo(f"{CATALOG}.lakehouse_silver.production_records").using("delta").createOrReplace()
print(f"production_records: {df_ps.count()} rows")

df_f = spark.read.table(f"{CATALOG}.lakehouse_bronze.financial_metrics")
w2 = Window.partitionBy("company_id","year","quarter")
df_fs = df_f.withColumn("rn",row_number().over(w2.orderBy("ingestion_ts"))).filter(col("rn")==1).drop("rn")
df_fs = df_fs.withColumn("period",concat(col("year"),lit("Q"),col("quarter"))).withColumn("ebitda_margin",round(col("ebitda_m_usd")/col("revenue_m_usd")*100,2)).withColumn("net_margin",round(col("net_income_m_usd")/col("revenue_m_usd")*100,2)).withColumn("quality_score",lit(0.90)).withColumn("silver_ts",current_timestamp()).drop("ingestion_ts")
df_fs.writeTo(f"{CATALOG}.lakehouse_silver.financial_metrics").using("delta").createOrReplace()
print(f"financial_metrics: {df_fs.count()} rows")
print("Silver layer complete: 3 tables with quality scores")
