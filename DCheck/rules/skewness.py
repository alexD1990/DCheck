from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult
from pyspark.sql import functions as F

class SkewnessRule(Rule):
    """
    Rask sjekk av numerisk fordeling.
    Beregner min, max, avg og stddev i én operasjon.
    Varsler hvis maks/min verdi er ekstremt langt unna snittet (default > 5 stddevs).
    """
    name = "extreme_values"

    def __init__(self, threshold_stddev=5.0):
        self.threshold = threshold_stddev

    def apply(self, df, context=None):
        numeric_cols = [c for c, t in df.dtypes if t in ("int", "bigint", "double", "float")]

        if not numeric_cols:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={},
                message="No numeric columns to check.",
            )

        # Bygg én stor aggregering for alle kolonner samtidig
        # Dette er mye raskere enn approxQuantile i loop
        exprs = []
        for c in numeric_cols:
            exprs.extend([
                F.min(c).alias(f"{c}_min"),
                F.max(c).alias(f"{c}_max"),
                F.avg(c).alias(f"{c}_avg"),
                F.stddev(c).alias(f"{c}_std")
            ])

        # Kjører 1 jobb for hele tabellen
        row = df.agg(*exprs).collect()[0].asDict()

        flagged_columns = {}
        total_extreme_cols = 0

        for c in numeric_cols:
            c_min = row[f"{c}_min"]
            c_max = row[f"{c}_max"]
            c_avg = row[f"{c}_avg"] or 0
            c_std = row[f"{c}_std"] or 0

            # Unngå deling på null hvis flat data
            if c_std == 0:
                continue

            # Sjekk din logikk: Er max/min langt unna?
            z_max = (c_max - c_avg) / c_std
            z_min = (c_avg - c_min) / c_std

            if z_max > self.threshold or z_min > self.threshold:
                flagged_columns[c] = {
                    "min": float(c_min),
                    "max": float(c_max),
                    "avg": float(c_avg),
                    "std": float(c_std),
                    "max_sigma": float(round(max(z_max, z_min), 1)) # Hvor mange ganger stddev
                }
                total_extreme_cols += 1

        status = "warning" if total_extreme_cols > 0 else "ok"
        message = (
            f"Extreme values detected in {total_extreme_cols} columns (>{self.threshold} stddev)"
            if total_extreme_cols > 0
            else "No extreme values detected"
        )

        return RuleResult(
            name=self.name,
            status=status,
            metrics={"flagged_columns": flagged_columns},
            message=message,
        )