from DCheck.rules.structural import DuplicateRowRule
from DCheck.rules.quality import NullRatioRule
from DCheck.rules.performance import SmallFileRule
from DCheck.rules.skewness import SkewnessRule
from DCheck.core.report import ValidationReport, RuleResult


def run_engine(
    df,
    table_name=None,
    preflight_only: bool = False,
    abort_on_preflight_warning: bool = False,
    abort_on_preflight_error: bool = True,
    cache: bool = False,
):
    # Build a report without triggering Spark actions yet
    report = ValidationReport(
        rows=0,  # set after (optional) caching/materialization
        columns=len(df.columns),
        column_names=df.columns,
    )

    # Context gets enriched after we know rows
    context = {"table_name": table_name}

    # =========================================
    # PRE-FLIGHT: PERFORMANCE (metadata-based)
    # =========================================
    preflight_rules = [
        SmallFileRule(table_name=table_name),
    ]

    for rule in preflight_rules:
        result = rule.apply(df, context=context)
        report.results.append(result)

    # Optional early return after preflight
    if preflight_only:
        report.results.append(
            RuleResult(
                name="engine",
                status="ok",
                metrics={"phase": "preflight_only"},
                message="Preflight completed. Core checks skipped by preflight_only=True.",
            )
        )
        return report

    # Optional gating (default is OFF for warning)
    if abort_on_preflight_error and report.has_errors():
        report.results.append(
            RuleResult(
                name="engine",
                status="warning",
                metrics={"phase": "preflight_aborted", "reason": "error"},
                message="Core checks skipped due to preflight error (abort_on_preflight_error=True).",
            )
        )
        return report

    if abort_on_preflight_warning and report.has_warnings():
        report.results.append(
            RuleResult(
                name="engine",
                status="warning",
                metrics={"phase": "preflight_aborted", "reason": "warning"},
                message="Core checks skipped due to preflight warning (abort_on_preflight_warning=True).",
            )
        )
        return report

    # =========================================
    # CORE DATA CHECKS (scan-based)
    # =========================================
    persisted = False
    try:
        if cache:
            df = df.persist()
            persisted = True

        # Materialize once to avoid recompute across rules
        rows = int(df.count())
        report.rows = rows
        context["rows"] = rows

        core_rules = [
            DuplicateRowRule(),
            NullRatioRule(),
            # Erstatter IqrOutlierRule med den mye raskere SkewnessRule
            SkewnessRule(threshold_stddev=5.0),
        ]

        for rule in core_rules:
            result = rule.apply(df, context=context)
            report.results.append(result)

        return report

    finally:
        if persisted:
            df.unpersist()
