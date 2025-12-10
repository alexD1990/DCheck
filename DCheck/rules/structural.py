from DCheck.rules.base import Rule
from DCheck.core.report import RuleResult

class DuplicateRowRule(Rule):
    name = "duplicate_rows"

    def apply(self, df):
        total_rows = df.count()
        unique_rows = df.dropDuplicates().count()
        duplicate_rows = total_rows - unique_rows

        status = "ok"
        message = "Duplicate rows detected" if duplicate_rows > 0 else "No duplicate rows"

        return RuleResult(
            name=self.name,
            status=status,
            metrics={
                "total_rows": float(total_rows),
                "unique_rows": float(unique_rows),
                "duplicate_rows": float(duplicate_rows),
            },
            message=message,
        )
