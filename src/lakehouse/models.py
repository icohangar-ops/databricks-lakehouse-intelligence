"""Pydantic models for the Lakehouse Intelligence Suite."""

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class CommodityType(str, Enum):
    """Commodity types tracked in the mining pipeline."""

    IRON_ORE = "Iron Ore"
    COPPER = "Copper"
    NICKEL = "Nickel"
    GOLD = "Gold"
    ZINC = "Zinc"
    LITHIUM = "Lithium"
    MANGANESE = "Manganese"
    PLATINUM = "Platinum"
    PALLADIUM = "Palladium"
    ALUMINUM = "Aluminum"
    COBALT = "Cobalt"
    PGMS = "PGMs"


class MiningCompany(BaseModel):
    """Represents a mining company with operational and financial metadata."""

    company_id: str = Field(..., description="Unique company identifier")
    name: str = Field(..., description="Company name")
    ticker: str = Field(..., description="Stock ticker symbol")
    country: str = Field(..., description="Primary country of operations")
    commodity: CommodityType = Field(..., description="Primary commodity")
    grade_percent: float = Field(..., ge=0, le=100, description="Ore grade in %")
    market_cap_billions: float = Field(..., ge=0, description="Market cap in $B")
    annual_production_kt: Optional[float] = Field(None, description="Annual production in kilotonnes")
    employees: Optional[int] = Field(None, description="Number of employees")
    founded_year: Optional[int] = Field(None, description="Year founded")
    esg_score: Optional[float] = Field(None, ge=0, le=100, description="ESG rating 0-100")
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class ProductionRecord(BaseModel):
    """A single production record for a mining company."""

    record_id: str
    company_id: str
    reporting_date: date
    commodity: CommodityType
    volume_tonnes: float = Field(..., ge=0, description="Production volume in tonnes")
    grade_recovery_pct: float = Field(..., ge=0, le=100, description="Grade recovery %")
    cost_per_tonne_usd: float = Field(..., ge=0, description="All-in sustaining cost $/t")
    head_grade: float = Field(..., ge=0, description="Head grade percentage")
    recovery_rate_pct: float = Field(..., ge=0, le=100, description="Metallurgical recovery rate")
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class FinancialMetric(BaseModel):
    """Quarterly financial metrics for a mining company."""

    metric_id: str
    company_id: str
    quarter: str = Field(..., pattern=r"^20\d{2}Q[1-4]$", description="Fiscal quarter e.g. 2024Q1")
    revenue_millions: float = Field(..., ge=0, description="Revenue in $M")
    ebitda_millions: float = Field(..., description="EBITDA in $M")
    capex_millions: float = Field(..., description="Capital expenditure in $M")
    free_cash_flow_millions: float = Field(..., description="FCF in $M")
    net_debt_millions: float = Field(..., description="Net debt in $M")
    debt_to_equity: float = Field(..., description="Debt-to-equity ratio")
    roic_pct: float = Field(..., description="Return on invested capital %")
    dividend_yield_pct: Optional[float] = Field(None, ge=0, description="Dividend yield %")
    reported_at: datetime = Field(default_factory=datetime.utcnow)


class SignalScore(BaseModel):
    """Computed intelligence signal score across 5 dimensions."""

    company_id: str
    company_name: str
    period: str

    grade_score: float = Field(..., ge=0, le=100)
    cost_score: float = Field(..., ge=0, le=100)
    production_score: float = Field(..., ge=0, le=100)
    growth_score: float = Field(..., ge=0, le=100)
    esg_score: float = Field(..., ge=0, le=100)

    weighted_signal: float = Field(..., ge=0, le=100, description="Weighted composite signal 0-100")
    rank: Optional[int] = Field(None, description="Rank among peers")
    tier: Optional[str] = Field(None, description="Tier classification: A/B/C/D")
    computed_at: datetime = Field(default_factory=datetime.utcnow)
