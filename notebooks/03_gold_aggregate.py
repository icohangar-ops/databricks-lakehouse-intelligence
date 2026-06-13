# Databricks notebook source
# Gold Layer - Signal Scores (0-100) across 5 dimensions
import logging

from pyspark.sql.functions import *
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gold_aggregate")

CATALOG = "workspace"
logger.info("Gold Layer - Signal Score Aggregation")


def upsert_delta(df, fqn: str, keys: list[str]) -> None:
    """Idempotent MERGE upsert with structured logging and error context.

    Replaces mode("overwrite").saveAsTable (which discards all prior rows on
    every run) so the gold layer survives re-runs and transient failures
    surface with context rather than silently aborting the scheduled job.
    """
    try:
        if not spark.catalog.tableExists(fqn):
            df.write.mode("overwrite").saveAsTable(fqn)
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


df_c = spark.read.table(f"{CATALOG}.lakehouse_silver.mining_companies")
df_p = spark.read.table(f"{CATALOG}.lakehouse_silver.production_records")
df_f = spark.read.table(f"{CATALOG}.lakehouse_silver.financial_metrics")

# Grade and Cost scores
df_g = df_p.groupBy("company_id").agg(avg("aisc_usd_per_t").alias("avg_aisc"))
df_g = df_g.withColumn("grade_score", when(col("avg_aisc") < 3000, 95).when(col("avg_aisc") < 4000, 80).when(col("avg_aisc") < 5000, 65).when(col("avg_aisc") < 7000, 50).otherwise(35))
df_g = df_g.withColumn("cost_score", when(col("avg_aisc") < 3500, 90).when(col("avg_aisc") < 5000, 72).when(col("avg_aisc") < 8000, 55).otherwise(40))
df_g = df_g.select("company_id", "grade_score", "cost_score")

# Production score
df_pr = df_p.groupBy("company_id").agg(sum("production_kt").alias("total_prod"))
df_pr = df_pr.withColumn("production_score", when(col("total_prod") > 50000, 95).when(col("total_prod") > 10000, 80).when(col("total_prod") > 1000, 65).otherwise(50))
df_pr = df_pr.select("company_id", "production_score")

# Growth score
df_gr = df_f.withColumn("growth_score", col("ebitda_margin") * 1.5 + col("roe_pct") * 0.8)
df_gr = df_gr.groupBy("company_id").agg(avg("growth_score").alias("growth_score"))
df_gr = df_gr.withColumn("growth_score", least(lit(100), greatest(lit(0), col("growth_score"))))

# ESG score
df_e = df_f.withColumn("esg_score", when(col("debt_to_equity") < 0.30, 85).when(col("debt_to_equity") < 0.40, 75).when(col("debt_to_equity") < 0.50, 60).otherwise(45))
df_e = df_e.groupBy("company_id").agg(avg("esg_score").alias("esg_score"))

# Join all - one join at a time
df_s = df_c.select("company_id", "company_name", "ticker", "commodity_focus")
df_s = df_s.join(df_g, "company_id", "left")
df_s = df_s.join(df_pr, "company_id", "left")
df_s = df_s.join(df_gr, "company_id", "left")
df_s = df_s.join(df_e, "company_id", "left")
df_s = df_s.fillna(50, subset=["grade_score", "cost_score", "production_score", "growth_score", "esg_score"])
df_s = df_s.withColumn("composite_score", round(col("grade_score") * 0.20 + col("cost_score") * 0.25 + col("production_score") * 0.20 + col("growth_score") * 0.20 + col("esg_score") * 0.15, 1))
df_s = df_s.withColumn("signal_band", when(col("composite_score") >= 80, "Strong Buy").when(col("composite_score") >= 65, "Buy").when(col("composite_score") >= 50, "Hold").when(col("composite_score") >= 35, "Sell").otherwise("Strong Sell"))
df_s = df_s.withColumn("gold_ts", current_timestamp())

upsert_delta(df_s, f"{CATALOG}.lakehouse_gold.mining_signal_scores", ["company_id"])

# Cross-domain intelligence
df_cross = df_s.join(df_f.select("company_id", "revenue_m_usd", "ebitda_margin", "net_margin", "debt_to_equity"), "company_id", "left")
df_cross = df_cross.withColumn("gold_ts", current_timestamp())
upsert_delta(df_cross, f"{CATALOG}.lakehouse_gold.cross_domain_intelligence", ["company_id"])

logger.info("Gold layer complete")
