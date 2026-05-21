"""Signal score computation engine.

Computes weighted intelligence signal scores across 5 dimensions:
  Grade, Cost, Production, Growth, ESG
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .config import LakehouseConfig
from .models import SignalScore


@dataclass
class SignalWeights:
    """Adjustable weights for signal computation."""

    grade: float = 0.25
    cost: float = 0.25
    production: float = 0.20
    growth: float = 0.15
    esg: float = 0.15

    def __post_init__(self):
        total = self.grade + self.cost + self.production + self.growth + self.esg
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"Weights must sum to 1.0, got {total}")


class SignalEngine:
    """Engine for computing mining intelligence signal scores."""

    def __init__(self, config: Optional[LakehouseConfig] = None, weights: Optional[SignalWeights] = None):
        self.config = config or LakehouseConfig()
        self.weights = weights or SignalWeights(
            grade=self.config.grade_weight,
            cost=self.config.cost_weight,
            production=self.config.production_weight,
            growth=self.config.growth_weight,
            esg=self.config.esg_weight,
        )

    def compute_signal(
        self,
        company_id: str,
        company_name: str,
        period: str,
        grade_score: float,
        cost_score: float,
        production_score: float,
        growth_score: float,
        esg_score: float,
    ) -> SignalScore:
        """Compute a weighted signal score.

        Args:
            company_id: Unique company identifier.
            company_name: Display name.
            period: Reporting period string (e.g. "2024Q1").
            grade_score: Ore quality score (0-100).
            cost_score: Cost efficiency score (0-100).
            production_score: Output volume score (0-100).
            growth_score: Growth trajectory score (0-100).
            esg_score: ESG rating score (0-100).

        Returns:
            SignalScore with computed weighted signal and tier.
        """
        # Clamp individual scores
        grade_score = self._clamp(grade_score)
        cost_score = self._clamp(cost_score)
        production_score = self._clamp(production_score)
        growth_score = self._clamp(growth_score)
        esg_score = self._clamp(esg_score)

        # Weighted composite
        weighted_signal = round(
            self.weights.grade * grade_score
            + self.weights.cost * cost_score
            + self.weights.production * production_score
            + self.weights.growth * growth_score
            + self.weights.esg * esg_score,
            2,
        )

        # Tier classification
        tier = self._classify_tier(weighted_signal)

        return SignalScore(
            company_id=company_id,
            company_name=company_name,
            period=period,
            grade_score=grade_score,
            cost_score=cost_score,
            production_score=production_score,
            growth_score=growth_score,
            esg_score=esg_score,
            weighted_signal=weighted_signal,
            tier=tier,
        )

    def compute_batch(
        self,
        records: list[dict],
    ) -> list[SignalScore]:
        """Compute signals for a batch of company records.

        Each dict must contain:
            company_id, company_name, period,
            grade_score, cost_score, production_score, growth_score, esg_score
        """
        results = []
        for rec in records:
            results.append(self.compute_signal(**rec))
        # Assign ranks
        ranked = sorted(results, key=lambda s: s.weighted_signal, reverse=True)
        for idx, signal in enumerate(ranked, 1):
            signal.rank = idx
        return ranked

    def grade_to_score(self, grade_percent: float, commodity: str) -> float:
        """Convert raw ore grade to a 0-100 score.

        Args:
            grade_percent: Raw ore grade percentage.
            commodity: Commodity type for benchmarking.

        Returns:
            Normalized score 0-100.
        """
        benchmarks = {
            "Iron Ore": (30.0, 70.0),
            "Copper": (0.2, 2.0),
            "Nickel": (0.5, 3.0),
            "Gold": (1.0, 10.0),
            "Zinc": (2.0, 12.0),
            "Lithium": (0.5, 3.0),
            "Manganese": (20.0, 50.0),
            "Platinum": (2.0, 8.0),
            "Palladium": (2.0, 8.0),
            "Cobalt": (0.05, 0.5),
            "PGMs": (3.0, 10.0),
            "Aluminum": (25.0, 55.0),
        }

        low, high = benchmarks.get(commodity, (0.0, 100.0))
        if grade_percent >= high:
            return 100.0
        if grade_percent <= low:
            return 0.0
        return round((grade_percent - low) / (high - low) * 100, 1)

    def cost_to_score(self, cost_per_tonne: float, commodity: str) -> float:
        """Convert cost per tonne to a 0-100 score (lower cost = higher score).

        Args:
            cost_per_tonne: All-in sustaining cost in USD/tonne.
            commodity: Commodity type.

        Returns:
            Cost efficiency score 0-100.
        """
        cost_benchmarks = {
            "Iron Ore": (15.0, 60.0),
            "Copper": (3000.0, 12000.0),
            "Nickel": (5000.0, 20000.0),
            "Gold": (600.0, 2500.0),
            "Zinc": (800.0, 3000.0),
            "Lithium": (4000.0, 15000.0),
            "Cobalt": (8000.0, 30000.0),
        }

        low, high = cost_benchmarks.get(commodity, (0.0, 100000.0))
        if cost_per_tonne <= low:
            return 100.0
        if cost_per_tonne >= high:
            return 0.0
        return round(100.0 - (cost_per_tonne - low) / (high - low) * 100, 1)

    @staticmethod
    def _clamp(value: float, min_val: float = 0.0, max_val: float = 100.0) -> float:
        return max(min_val, min(max_val, value))

    @staticmethod
    def _classify_tier(score: float) -> str:
        if score >= 80:
            return "A"
        elif score >= 65:
            return "B"
        elif score >= 50:
            return "C"
        else:
            return "D"
