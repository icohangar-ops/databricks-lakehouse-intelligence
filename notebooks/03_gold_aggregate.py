# Databricks notebook source
# Gold Layer - Signal Scores (0-100) across 5 dimensions
# Dimensions: Grade, Cost, Production, Growth, ESG
from pyspark.sql.functions import *
CATALOG = "workspace"
print("Gold Layer - Signal Score Aggregation")

df_c = spark.read.table(f"{CATALOG}.lakehouse_silver.mining_companies")
df_p = spark.read.table(f"{CATALOG}.lakehouse_silver.production_records")
df_f = spark.read.table(f"{CATALOG}.lakehouse_silver.financial_metrics")

df_g = df_p.groupBy("company_id").agg(avg("aisc_usd_per_t").alias("avg_aisc")).withColumn("grade_score",when(col("avg_aisc")<3000,95).when(col("avg_aisc")<4000,80).when(col("avg_aisc")<5000,65).when(col("avg_aisc")<7000,50).otherwise(35)).withColumn("cost_score",when(col("avg_aisc")<3500,90).when(col("avg_aisc")<5000,72).when(col("avg_aisc")<8000,55).otherwise(40))
df_pr = df_p.groupBy("company_id").agg(sum("production_kt").alias("total_prod")).withColumn("production_score",when(col("total_prod")>50000,95).when(col("total_prod")>10000,80).when(col("total_prod")>1000,65).otherwise(50))
df_gr = df_f.withColumn("growth_score",(col("ebitda_margin")*1.5+col("roe_pct")*0.8)).groupBy("company_id").agg(avg("growth_score").alias("growth_score")).withColumn("growth_score",least(100,greatest(0,col("growth_score"))))
df_e = df_f.withColumn("esg_score",when(col("debt_to_equity")<0.30,85).when(col("debt_to_equity")<0.40,75).when(col("debt_to_equity")<0.50,60).otherwise(45)).groupBy("company_id").agg(avg("esg_score").alias("esg_score"))

df_signal = df_c.select("company_id","company_name","ticker","commodity_focus").join(df_g.select("company_id","grade_score","cost_score"),"company_id","left").join(df_pr.select("company_id","production_score"),"company_id","left").join(df_gr,"company_id","left").join(df_e,"company_id","left").fillna(50,subset=["grade_score","cost_score","production_score","growth_score","esg_score"]).withColumn("composite_score",round(col("grade_score")*0.20+col("cost_score")*0.25+col("production_score")*0.20+col("growth_score")*0.20+col("esg_score")*0.15,1)).withColumn("signal_band",when(col("composite_score")>=80,"Strong Buy").when(col("composite_score")>=65,"Buy").when(col("composite_score")>=50,"Hold").when(col("composite_score")>=35,"Sell").otherwise("Strong Sell")).withColumn("gold_ts",current_timestamp())

df_signal.writeTo(f"{CATALOG}.lakehouse_gold.mining_signal_scores").using("delta").createOrReplace()
print("mining_signal_scores:")
df_signal.select("company_name","composite_score","signal_band").orderBy(desc("composite_score")).show(truncate=False)

df_cross = df_signal.join(df_f.select("company_id","revenue_m_usd","ebitda_margin","net_margin","debt_to_equity"),"company_id","left").withColumn("gold_ts",current_timestamp())
df_cross.writeTo(f"{CATALOG}.lakehouse_gold.cross_domain_intelligence").using("delta").createOrReplace()
print("cross_domain_intelligence: created")
print("Gold layer complete")
