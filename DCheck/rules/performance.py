from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult
from pyspark.sql.functions import col

class SmallFileRule(Rule):
    name = "small_files"

    def __init__(self, table_name=None, min_avg_mb=64):
        self.table_name = table_name
        self.min_avg_mb = min_avg_mb

    def apply(self, df):
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

        detail = df.sparkSession.sql(
            f"DESCRIBE DETAIL {self.table_name}"
        ).collect()[0]

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


class IqrOutlierRule:
    name = "iqr_outliers"

    def apply(self, df):
        numeric_cols = [
            c for c, t in df.dtypes
            if t in ("int", "bigint", "double", "float")
        ]

        total_outliers = 0
        per_column = {}

        for c in numeric_cols:
            q1, q3 = df.approxQuantile(c, [0.25, 0.75], 0.01)

            if q1 is None or q3 is None:
                continue

            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr

            cnt = df.filter((col(c) < lower) | (col(c) > upper)).count()

            if cnt > 0:
                per_column[c] = {"outliers": cnt}
                total_outliers += cnt

        metrics = {
            "total_outliers": total_outliers,
            "per_column": per_column,
        }

        status = "ok"
        message = "Outlier values detected (IQR)"

        return RuleResult(
            name=self.name,
            status=status,
            metrics=metrics,
            message=message,
        )

