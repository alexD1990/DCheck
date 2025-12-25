from DCheck.rules.structural import DuplicateRowRule
from DCheck.rules.quality import NullRatioRule
from DCheck.rules.performance import SmallFileRule, IqrOutlierRule
from DCheck.core.report import ValidationReport, RuleResult


def run_engine(
    df,
    table_name=None,
    preflight_only: bool = False,
    abort_on_preflight_warning: bool = False,
    abort_on_preflight_error: bool = True,
):
    rows = df.count()
    report = ValidationReport(
        rows=rows,
        columns=len(df.columns),
        column_names=df.columns,
    )
    context = {"rows": rows, "table_name": table_name}

    # =========================================
    # PRE-FLIGHT: PERFORMANCE 
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
    # CORE DATA CHECKS 
    # =========================================
    core_rules = [
        DuplicateRowRule(),
        NullRatioRule(),
        IqrOutlierRule(),
    ]

    for rule in core_rules:
        result = rule.apply(df, context=context)
        report.results.append(result)

    return report