from DCheck.core.engine import run_engine

def validate_spark(
    df,
    table_name=None,
    preflight_only: bool = False,
    abort_on_preflight_warning: bool = False,
):
    """
    Public Spark API for validation.
    """
    return run_engine(
        df,
        table_name=table_name,
        preflight_only=preflight_only,
        abort_on_preflight_warning=abort_on_preflight_warning,
    )