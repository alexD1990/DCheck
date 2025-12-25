import io
import contextlib
from DCheck.api import validate_spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("DCheckTest").getOrCreate()

def test_validate_spark_runs_and_returns_report():
    df = spark.createDataFrame([(1, 2), (3, 4), (5, 6)], ["col1", "col2"])
    report = validate_spark(df)
    s = report.summary()
    assert s["rows"] == 3
    assert s["columns"] == 2
    assert s["rules_run"] >= 1
    assert isinstance(report.results, list)

def test_engine_has_no_print_side_effects():
    df = spark.createDataFrame([(1, 2), (3, 4)], ["col1", "col2"])

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _ = validate_spark(df)

    # Core must not print anything by default
    assert buf.getvalue() == ""

def test_preflight_only_returns_early():
    df = spark.createDataFrame([(1, 2), (3, 4)], ["col1", "col2"])

    report = validate_spark(df, table_name=None, preflight_only=True)

    # Should run small_files + engine note, but skip core checks
    names = [r.name for r in report.results]
    assert "small_files" in names
    assert "duplicate_rows" not in names
    assert "null_ratio" not in names
    assert "extreme_values" not in names  # Endret fra iqr_outliers

def test_status_semantics_warning_when_issues_found():
    # Duplicate rows: identical rows
    df_dupes = spark.createDataFrame([(1, 2), (1, 2), (3, 4)], ["a", "b"])
    report_dupes = validate_spark(df_dupes)
    dup = next(r for r in report_dupes.results if r.name == "duplicate_rows")
    assert dup.status.lower() == "warning"

    # Nulls: include nulls in a column
    df_nulls = spark.createDataFrame([(1, None), (2, 2), (3, None)], ["a", "b"])
    report_nulls = validate_spark(df_nulls)
    nullr = next(r for r in report_nulls.results if r.name == "null_ratio")
    assert nullr.status.lower() == "warning"

    # Extreme Values (Skewness): 
    # Lager en serie med normale verdier (10) og en ekstrem (1000)
    data = [(10.0,) for _ in range(50)] + [(1000.0,)]
    df_skew = spark.createDataFrame(data, ["x"])
    
    report_skew = validate_spark(df_skew)
    skew_rule = next(r for r in report_skew.results if r.name == "extreme_values")
    
    # Skal gi warning fordi 1000 er langt unna snittet
    assert skew_rule.status.lower() == "warning"
    assert isinstance(skew_rule.metrics, dict)