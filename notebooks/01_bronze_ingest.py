# Databricks notebook source
# Bronze Layer - Ingest Raw Data
# Creates 3 Delta tables: mining_companies, production_records, financial_metrics

from pyspark.sql.types import *
from pyspark.sql.functions import *
CATALOG = "workspace"
print("Bronze Layer - Data Ingestion")

mc = StructType([StructField("company_id",StringType(),False),StructField("company_name",StringType(),False),StructField("ticker",StringType(),False),StructField("commodity_focus",StringType(),False),StructField("country",StringType(),False),StructField("tier",StringType(),False)])
md = [("MC001","Freeport-McMoRan","FCX","Copper/Gold","USA","Tier 1"),("MC002","Glencore","GLEN.L","Cobalt/Nickel","Switzerland","Tier 1"),("MC003","BHP Group","BHP","Iron Ore/Nickel","Australia","Tier 1"),("MC004","Rio Tinto","RIO","Iron Ore/Lithium","UK","Tier 1"),("MC005","Southern Copper","SCCO","Copper","USA","Tier 2"),("MC006","Vale SA","VALE","Iron Ore/Nickel","Brazil","Tier 1"),("MC007","First Quantum","FQVLF","Copper/Gold","Canada","Tier 2"),("MC008","Albemarle","ALB","Lithium","USA","Tier 1"),("MC009","Teck Resources","TECK","Copper/Zinc","Canada","Tier 2"),("MC010","Antofagasta","ANTO.L","Copper","Chile","Tier 2")]
df = spark.createDataFrame(md, mc).withColumn("ingestion_ts",current_timestamp())
df.writeTo(f"{CATALOG}.lakehouse_bronze.mining_companies").using("delta").createOrReplace()
print(f"mining_companies: {df.count()} rows")

ps = StructType([StructField("company_id",StringType(),False),StructField("year",IntegerType(),False),StructField("quarter",IntegerType(),False),StructField("commodity",StringType(),False),StructField("production_kt",DoubleType(),False),StructField("aisc_usd_per_t",DoubleType(),False)])
pd2 = [("MC001",2025,1,"Copper",420.5,4250.0),("MC001",2025,1,"Gold",185.2,980.0),("MC002",2025,1,"Cobalt",12.8,28500.0),("MC003",2025,1,"Iron Ore",62000.0,18.5),("MC003",2025,1,"Nickel",22.5,14500.0),("MC004",2025,1,"Iron Ore",58000.0,19.2),("MC004",2025,1,"Lithium",8.5,6200.0),("MC005",2025,1,"Copper",245.8,3950.0),("MC006",2025,1,"Iron Ore",78000.0,16.8),("MC006",2025,1,"Nickel",18.2,13200.0),("MC007",2025,1,"Copper",155.3,4100.0),("MC008",2025,1,"Lithium",22.5,7500.0),("MC009",2025,1,"Copper",105.2,4800.0),("MC009",2025,1,"Zinc",85.0,1850.0),("MC010",2025,1,"Copper",185.5,4050.0)]
df2 = spark.createDataFrame(pd2, ps).withColumn("ingestion_ts",current_timestamp())
df2.writeTo(f"{CATALOG}.lakehouse_bronze.production_records").using("delta").createOrReplace()
print(f"production_records: {df2.count()} rows")

fs = StructType([StructField("company_id",StringType(),False),StructField("year",IntegerType(),False),StructField("quarter",IntegerType(),False),StructField("revenue_m_usd",DoubleType(),False),StructField("ebitda_m_usd",DoubleType(),False),StructField("net_income_m_usd",DoubleType(),False),StructField("debt_to_equity",DoubleType(),False),StructField("roe_pct",DoubleType(),False)])
fd = [("MC001",2025,1,6800,2100,850,0.42,18.5),("MC002",2025,1,15200,3200,1100,0.58,14.2),("MC003",2025,1,28000,12500,5800,0.35,22.8),("MC004",2025,1,11500,5200,2800,0.31,20.5),("MC005",2025,1,2800,1200,550,0.28,21.2),("MC006",2025,1,12500,4800,2200,0.52,16.8),("MC007",2025,1,2200,950,420,0.38,17.5),("MC008",2025,1,3800,1100,480,0.45,12.5),("MC009",2025,1,3200,1400,620,0.33,19.8),("MC010",2025,1,1800,750,380,0.30,20.2)]
df3 = spark.createDataFrame(fd, fs).withColumn("ingestion_ts",current_timestamp())
df3.writeTo(f"{CATALOG}.lakehouse_bronze.financial_metrics").using("delta").createOrReplace()
print(f"financial_metrics: {df3.count()} rows")
print("Bronze layer complete: 3 tables")
