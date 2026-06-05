"""Configuration for the Lakehouse Intelligence Suite."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LakehouseConfig:
    """Central configuration for the Lakehouse Intelligence pipeline."""

    # Unity Catalog
    catalog: str = "workspace"
    bronze_schema: str = "mining_bronze"
    silver_schema: str = "mining_silver"
    gold_schema: str = "mining_gold"
    ml_schema: str = "ml_experiments"

    # Bronze tables
    bronze_companies_table: str = "mining_companies"
    bronze_production_table: str = "production_records"
    bronze_financials_table: str = "financial_metrics"

    # Silver tables
    silver_companies_table: str = "companies_cleaned"
    silver_production_table: str = "production_cleaned"
    silver_financials_table: str = "financials_cleaned"

    # Gold tables
    gold_signals_table: str = "signal_scores"
    gold_rankings_table: str = "company_rankings"

    # Workspace — set via DATABRICKS_WORKSPACE_URL and DATABRICKS_WORKSPACE_ID env vars
    workspace_url: str = ""
    workspace_id: str = ""

    # Signal scoring weights
    grade_weight: float = 0.25
    cost_weight: float = 0.25
    production_weight: float = 0.20
    growth_weight: float = 0.15
    esg_weight: float = 0.15

    # MLflow
    mlflow_experiment_name: str = "/lakehouse/signal_scores"
    mlflow_tracking_uri: str = "databricks"

    # Pipeline
    bronze_checkpoint: str = "/tmp/checkpoints/bronze"
    silver_checkpoint: str = "/tmp/checkpoints/silver"
    gold_checkpoint: str = "/tmp/checkpoints/gold"

    @property
    def bronze_companies_fqn(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_companies_table}"

    @property
    def bronze_production_fqn(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_production_table}"

    @property
    def bronze_financials_fqn(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_financials_table}"

    @property
    def silver_companies_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_companies_table}"

    @property
    def silver_production_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_production_table}"

    @property
    def silver_financials_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_financials_table}"

    @property
    def gold_signals_fqn(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.{self.gold_signals_table}"

    @property
    def gold_rankings_fqn(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.{self.gold_rankings_table}"

    @property
    def all_bronze_tables(self) -> list[str]:
        return [
            self.bronze_companies_fqn,
            self.bronze_production_fqn,
            self.bronze_financials_fqn,
        ]

    @property
    def all_silver_tables(self) -> list[str]:
        return [
            self.silver_companies_fqn,
            self.silver_production_fqn,
            self.silver_financials_fqn,
        ]

    @property
    def all_gold_tables(self) -> list[str]:
        return [self.gold_signals_fqn, self.gold_rankings_fqn]


# Default configuration instance
config = LakehouseConfig()
