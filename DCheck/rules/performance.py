from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult
from pyspark.sql import functions as F
import re

class SmallFileRule(Rule):
    name = "small_files"

    def __init__(self, table_name=None, min_avg_mb=64):
        self.table_name = table_name
        self.min_avg_mb = min_avg_mb

    def apply(self, df, context=None):
        if not self.table_name:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={
                    "num_files": 0.0,
                    "avg_file_size_mb": 0.0,
                },
                message="Small file check skipped (no table name provided)",
            )

        # Harden input: allow db.table or catalog.db.table with underscores/digits
        if not re.fullmatch(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){1,2}", self.table_name):
            return RuleResult(
                name=self.name,
                status="error",
                metrics={"num_files": 0.0, "avg_file_size_mb": 0.0},
                message="Invalid table_name format for DESCRIBE DETAIL",
            )

        detail = df.sparkSession.sql(f"DESCRIBE DETAIL {self.table_name}").collect()[0]

        num_files = detail["numFiles"]
        total_bytes = detail["sizeInBytes"]

        if num_files == 0:
            avg_file_size_mb = 0.0
        else:
            avg_file_size_mb = (total_bytes / num_files) / (1024 * 1024)

        if avg_file_size_mb < 16:
            status = "warning"
            message = "Severe small file problem detected"
        elif avg_file_size_mb < self.min_avg_mb:
            status = "warning"
            message = "Small file problem detected"
        else:
            status = "ok"
            message = "No small file problem detected"

        return RuleResult(
            name=self.name,
            status=status,
            metrics={
                "num_files": float(num_files),
                "avg_file_size_mb": float(avg_file_size_mb),
            },
            message=message,
        )


class IqrOutlierRule(Rule):
    name = "iqr_outliers"

    def apply(self, df, context=None):
        numeric_cols = [
            c for c, t in df.dtypes
            if t in ("int", "bigint", "double", "float")
        ]

        if not numeric_cols:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={"total_outliers": 0, "per_column": {}},
                message="No numeric columns to check",
            )

        bounds = {}
        for c in numeric_cols:
            q = df.approxQuantile(c, [0.25, 0.75], 0.01)
            if not q or len(q) != 2:
                continue
            q1, q3 = q
            if q1 is None or q3 is None:
                continue
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            bounds[c] = (lower, upper)

        if not bounds:
            return RuleResult(
                name=self.name,
                status="ok",
                metrics={"total_outliers": 0, "per_column": {}},
                message="Could not compute IQR bounds",
            )

        # Count outliers for all numeric columns in ONE pass
        agg_exprs = []
        for c, (lower, upper) in bounds.items():
            agg_exprs.append(
                F.sum(
                    F.when(
                        (F.col(c) < F.lit(lower)) | (F.col(c) > F.lit(upper)),
                        1
                    ).otherwise(0)
                ).alias(c)
            )

        row = df.agg(*agg_exprs).collect()[0].asDict()
        per_column = {c: {"outliers": int(v)} for c, v in row.items() if v and int(v) > 0}
        total_outliers = int(sum(int(v) for v in row.values() if v is not None))

        metrics = {
            "total_outliers": total_outliers,
            "per_column": per_column,
        }

        status = "warning" if total_outliers > 0 else "ok"
        message = "Outlier values detected (IQR)" if total_outliers > 0 else "No outliers detected (IQR)"

        return RuleResult(
            name=self.name,
            status=status,
            metrics=metrics,
            message=message,
        )
