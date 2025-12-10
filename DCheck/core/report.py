from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class RuleResult:
    name: str
    status: str
    metrics: Dict[str, float]
    message: str

@dataclass
class ValidationReport:
    rows: int
    columns: int
    column_names: List[str]
    results: List[RuleResult] = field(default_factory=list)

    def summary(self):
        return {
            "rows": self.rows,
            "columns": self.columns,
            "rules_run": len(self.results),
            "warnings": sum(r.status == "warning" for r in self.results),
            "errors": sum(r.status == "error" for r in self.results),
        }

def render_report(report: ValidationReport):
    print("=" * 60)
    print("DCHECK REPORT")
    print(f"Rows    : {report.rows}")
    print(f"Columns : {report.columns}")
    print(f"Rules   : {len(report.results)}")
    print("=" * 60)
    print()

    status_count = {"ok": 0, "warning": 0, "error": 0}

    for result in report.results:
        status = result.status.lower()
        status_count[status] = status_count.get(status, 0) + 1

        print(f"[RULE] {result.name}")
        print(f"Status : {result.status}")
        print(f"Message: {result.message}")
        print()
        print("Total metrics:")

        metrics = result.metrics or {}

        for key, value in metrics.items():
            if key != "per_column":
                if isinstance(value, float):
                    print(f"  - {key} : {round(value, 6)}")
                else:
                    print(f"  - {key} : {value}")

        if "per_column" in metrics and isinstance(metrics["per_column"], dict):
            print()
            print("Per column:")
            for col, col_metrics in metrics["per_column"].items():
                nulls = col_metrics.get("nulls")
                ratio = col_metrics.get("ratio")

                if nulls is not None and ratio is not None:
                    print(f"  - {col} : {nulls} ({round(ratio * 100, 3)}%)")
                else:
                    print(f"  - {col} : {col_metrics}")

        print()
        print("-" * 60)
        print()

    print("=" * 60)
    print(
        f"Summary: ok={status_count['ok']} | "
        f"warning={status_count['warning']} | "
        f"error={status_count['error']}"
    )
    print("=" * 60)
