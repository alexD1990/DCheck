from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult
from pyspark.sql import functions as F

class NullRatioRule:
    name = "null_ratio"

    def apply(self, df):
        rows = df.count()
        cols = len(df.columns)
        total_cells = rows * cols

        total_nulls = 0
        per_column = {}

        for c in df.columns:
            cnt = df.filter(F.col(c).isNull()).count()
            if cnt > 0:
                per_column[c] = {"nulls": cnt}
                total_nulls += cnt

        metrics = {
            "total_nulls": total_nulls,
            "per_column": per_column,
        }

        status = "ok"
        message = "Null values detected"

        return RuleResult(
            name=self.name,
            status=status,
            metrics=metrics,
            message=message,
        )

