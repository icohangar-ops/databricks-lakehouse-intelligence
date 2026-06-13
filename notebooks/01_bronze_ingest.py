# Databricks notebook source
# Bronze Layer - Ingest Raw Data
# Creates 3 Delta tables: mining_companies, production_records, financial_metrics

import logging

from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from cubiczan_resilience import resilient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_ingest")

CATALOG = "workspace"
logger.info("Bronze Layer - Data Ingestion")


@resilient(timeout=300, max_attempts=3)
def upsert_delta(df, fqn: str, keys: list[str]) -> None:
    """Idempotent write: MERGE on natural keys instead of createOrReplace.

    createOrReplace discards all previous data on every run. Using MERGE keeps
    the table durable across the daily schedule and makes re-runs idempotent.
    The write is wrapped so a transient failure surfaces with context instead
    of silently failing the job.
    """
    try:
        if not spark.catalog.tableExists(fqn):
            # First run: materialize the table so subsequent MERGEs have a target.
            df.writeTo(fqn).using("delta").createOrReplace()
            logger.info("%s: created with %d rows", fqn, df.count())
            return

        target = DeltaTable.forName(spark, fqn)
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (
            target.alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("%s: merged %d rows", fqn, df.count())
    except Exception:
        logger.exception("Failed writing %s", fqn)
        raise


mc = StructType([StructField("company_id",StringType(),False),StructField("company_name",StringType(),False),StructField("ticker",StringType(),False),StructField("commodity_focus",StringType(),False),StructField("country",StringType(),False),StructField("tier",StringType(),False)])
md = [("MC001","Freeport-McMoRan","FCX","Copper/Gold","USA","Tier 1"),("MC002","Glencore","GLEN.L","Cobalt/Nickel","Switzerland","Tier 1"),("MC003","BHP Group","BHP","Iron Ore/Nickel","Australia","Tier 1"),("MC004","Rio Tinto","RIO","Iron Ore/Lithium","UK","Tier 1"),("MC005","Southern Copper","SCCO","Copper","USA","Tier 2"),("MC006","Vale SA","VALE","Iron Ore/Nickel","Brazil","Tier 1"),("MC007","First Quantum","FQVLF","Copper/Gold","Canada","Tier 2"),("MC008","Albemarle","ALB","Lithium","USA","Tier 1"),("MC009","Teck Resources","TECK","Copper/Zinc","Canada","Tier 2"),("MC010","Antofagasta","ANTO.L","Copper","Chile","Tier 2")]
df = spark.createDataFrame(md, mc).withColumn("ingestion_ts",current_timestamp())
upsert_delta(df, f"{CATALOG}.lakehouse_bronze.mining_companies", ["company_id"])

ps = StructType([StructField("company_id",StringType(),False),StructField("year",IntegerType(),False),StructField("quarter",IntegerType(),False),StructField("commodity",StringType(),False),StructField("production_kt",DoubleType(),False),StructField("aisc_usd_per_t",DoubleType(),False)])
pd2 = [("MC001",2025,1,"Copper",420.5,4250.0),("MC001",2025,1,"Gold",185.2,980.0),("MC002",2025,1,"Cobalt",12.8,28500.0),("MC003",2025,1,"Iron Ore",62000.0,18.5),("MC003",2025,1,"Nickel",22.5,14500.0),("MC004",2025,1,"Iron Ore",58000.0,19.2),("MC004",2025,1,"Lithium",8.5,6200.0),("MC005",2025,1,"Copper",245.8,3950.0),("MC006",2025,1,"Iron Ore",78000.0,16.8),("MC006",2025,1,"Nickel",18.2,13200.0),("MC007",2025,1,"Copper",155.3,4100.0),("MC008",2025,1,"Lithium",22.5,7500.0),("MC009",2025,1,"Copper",105.2,4800.0),("MC009",2025,1,"Zinc",85.0,1850.0),("MC010",2025,1,"Copper",185.5,4050.0)]
df2 = spark.createDataFrame(pd2, ps).withColumn("ingestion_ts",current_timestamp())
upsert_delta(df2, f"{CATALOG}.lakehouse_bronze.production_records", ["company_id","year","quarter","commodity"])

fs = StructType([StructField("company_id",StringType(),False),StructField("year",IntegerType(),False),StructField("quarter",IntegerType(),False),StructField("revenue_m_usd",DoubleType(),False),StructField("ebitda_m_usd",DoubleType(),False),StructField("net_income_m_usd",DoubleType(),False),StructField("debt_to_equity",DoubleType(),False),StructField("roe_pct",DoubleType(),False)])
fd = [("MC001",2025,1,6800,2100,850,0.42,18.5),("MC002",2025,1,15200,3200,1100,0.58,14.2),("MC003",2025,1,28000,12500,5800,0.35,22.8),("MC004",2025,1,11500,5200,2800,0.31,20.5),("MC005",2025,1,2800,1200,550,0.28,21.2),("MC006",2025,1,12500,4800,2200,0.52,16.8),("MC007",2025,1,2200,950,420,0.38,17.5),("MC008",2025,1,3800,1100,480,0.45,12.5),("MC009",2025,1,3200,1400,620,0.33,19.8),("MC010",2025,1,1800,750,380,0.30,20.2)]
df3 = spark.createDataFrame(fd, fs).withColumn("ingestion_ts",current_timestamp())
upsert_delta(df3, f"{CATALOG}.lakehouse_bronze.financial_metrics", ["company_id","year","quarter"])

logger.info("Bronze layer complete: 3 tables")
