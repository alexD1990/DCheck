# dcheck/orchestrator/adapters.py
from __future__ import annotations

from typing import List, Optional

from dcheck.common.types import Report as NewReport, CheckResult as NewCheckResult
from dcheck.core.report import ValidationReport, RuleResult


def report_to_validation_report(r: NewReport) -> ValidationReport:
    """
    Flatten module-isolated results into the existing ValidationReport
    so render_report continues to work unchanged.
    """
    flat: List[NewCheckResult] = r.all_results_flat()

    # Determine rows from core.rowcount if present, else 0
    rows = 0
    for cr in flat:
        if cr.check_id == "core.rowcount":
            try:
                rows = int((cr.metrics or {}).get("rows", 0))
            except Exception:
                rows = 0
            break

    vr = ValidationReport(
        rows=rows,
        columns=r.columns,
        column_names=r.column_names,
        results=[],
    )

    for cr in flat:
        # Skip the pseudo-check from printing unless you want it visible
        if cr.check_id == "core.rowcount":
            continue

        # Convert check_id back to old "name" used by renderer
        # Example: "core.null_ratio" -> "null_ratio"
        name = cr.check_id.split(".", 1)[1] if "." in cr.check_id else cr.check_id

        vr.results.append(
            RuleResult(
                name=name,
                status=cr.status,
                metrics=cr.metrics or {},
                message=cr.message,
            )
        )

    return vr
