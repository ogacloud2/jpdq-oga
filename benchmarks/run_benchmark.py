"""Benchmark: jpdq-oga's Spark-native checks vs pandas-UDF baselines.

Every group applies the same check to the same synthetic column and compares

* ``native``               the jpdq-oga expression / DQX check (no Python UDF),
* ``pandas_udf_*``         pandas UDFs, the usual way to reuse Python code in Spark:
  - ``pandas_udf_stdnum``     ``Series.map`` over python-stdnum's ``jp.cn.is_valid`` (existing library),
  - ``pandas_udf_reference``  ``Series.map`` over this package's pure-Python reference validator,
  - ``pandas_udf_vectorized`` a hand-vectorised numpy implementation (the strongest pandas baseline),
  - ``pandas_udf_set``        a Python ``set`` lookup (reference-data existence).

Before timing, all implementations of a group must produce identical counts of true / false / NULL;
a disagreement aborts the run (a fast wrong answer is not a result).

Each implementation is timed over ``--repeats`` full passes (after one warm-up pass on a tenth of the
data); a pass reads the data, evaluates the check and discards the result (``noop`` sink).

    python benchmarks/run_benchmark.py --rows 10000000 --output-dir benchmark-output
    python benchmarks/run_benchmark.py --rows 200000 --skip-udf --data-source generated   # quick local dry run
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

from jpdq_oga import expressions as E
from jpdq_oga import validators as V
from jpdq_oga import wareki as W

POSTAL_REFERENCE_ROWS = 120_000  # about the size of Japan Post's KEN_ALL (120,877 distinct rows)


# --- synthetic data -----------------------------------------------------------------------------------------


def _wareki_samples() -> list[str]:
    """Valid era dates over the supported range, mixed notations, plus a few broken ones."""
    import datetime as dt

    out: list[str] = []
    d = dt.date(1873, 1, 1)
    while d <= dt.date(2060, 12, 31):
        era = next(e for e in reversed(W.ERAS) if d >= e.start)
        year = d.year - era.first_year + 1
        out.append(
            f"{era.name}{year}年{d.month}月{d.day}日" if len(out) % 2 else f"{era.letter}{year}.{d.month}.{d.day}"
        )
        d += dt.timedelta(days=97)
    broken = ["令和元年4月30日", "平成31年5月1日", "令和6年2月30日", "明治5年12月31日", "2024-04-01", "令和6年4月"]
    return out + broken * 20


def generate(spark: SparkSession, rows: int, seed: int = 20260101) -> DataFrame:
    """One DataFrame with a column per check, built with Spark expressions only (no Python workers)."""
    n_ref = POSTAL_REFERENCE_ROWS
    samples = _wareki_samples()
    r = lambda k: F.rand(seed + k)  # noqa: E731

    def digits(k: int, width: int) -> Column:
        return F.lpad(F.floor(r(k) * (10**width)).cast("long").cast("string"), width, "0")

    corporate = (
        F.when(r(1) < 0.01, F.lit(None).cast("string"))
        .when(r(2) < 0.05, digits(3, 12))  # twelve digits: a format violation
        .otherwise(digits(4, 13))
    )
    local_gov = F.when(r(5) < 0.01, F.lit(None).cast("string")).otherwise(
        F.concat(F.lpad((F.floor(r(6) * 48)).cast("string"), 2, "0"), digits(7, 4))
    )
    wareki = F.when(r(8) < 0.01, F.lit(None).cast("string")).otherwise(
        F.element_at(F.array(*[F.lit(s) for s in samples]), (F.floor(r(9) * len(samples)) + 1).cast("int"))
    )
    in_reference = F.lpad((F.floor(r(10) * n_ref) * 83).cast("long").cast("string"), 7, "0")
    postal = F.when(r(11) < 0.01, F.lit(None).cast("string")).otherwise(
        F.when(r(12) < 0.7, in_reference).otherwise(digits(13, 7))
    )
    return spark.range(rows).select(
        corporate.alias("corporate_number"),
        local_gov.alias("local_gov_code"),
        wareki.alias("wareki_date"),
        postal.alias("postal_code"),
    )


def postal_reference(spark: SparkSession) -> DataFrame:
    """Synthetic ``postal_codes`` table with validity periods (half of the rows retired on 2025-04-01)."""
    codes = F.lpad((F.col("id") * 83).cast("string"), 7, "0")
    retired = F.when(F.col("id") % 2 == 0, F.lit("2025-04-01").cast("date")).cast("date")
    return spark.range(POSTAL_REFERENCE_ROWS).select(
        codes.alias("postal_code"),
        F.lit("13101").alias("local_gov_code"),
        F.lit("東京都").alias("prefecture"),
        F.lit("千代田区").alias("city"),
        F.lit(None).cast("date").alias("valid_from"),
        retired.alias("valid_to"),
    )


# --- baselines ----------------------------------------------------------------------------------------------


def _bool_series(values: list[Any]) -> pd.Series:
    return pd.Series(values, dtype="boolean")


_BASE_WEIGHTS = np.array([1, 2] * 6, dtype=np.int64)[::-1]  # weights of the 12 base digits, left to right


def corporate_stdnum_py(s: pd.Series) -> pd.Series:
    """Existing library: python-stdnum's ``jp.cn.is_valid`` per value (``Series.map`` style)."""
    from stdnum.jp import cn

    return _bool_series([None if v is None else bool(cn.is_valid(v)) for v in s.tolist()])


def corporate_reference_py(s: pd.Series) -> pd.Series:
    return _bool_series([V.is_valid_corporate_number(v) for v in s.tolist()])


def corporate_vectorized_py(s: pd.Series) -> pd.Series:
    """Hand-vectorised numpy check digit: the strongest pandas baseline."""
    text = s.astype("string")
    ok = text.str.fullmatch(r"[0-9]{13}").fillna(False).to_numpy(dtype=bool)
    filled = text.where(pd.Series(ok, index=s.index), "0" * 13).to_numpy(dtype=str)
    digits = np.frombuffer("".join(filled).encode("ascii"), dtype=np.uint8).reshape(-1, 13).astype(np.int64) - 48
    check = 9 - ((digits[:, 1:] * _BASE_WEIGHTS).sum(axis=1) % 9)
    valid = pd.array(check == digits[:, 0], dtype="boolean")
    valid[~ok] = False
    valid[s.isna().to_numpy()] = pd.NA
    return pd.Series(valid, index=s.index)


def local_gov_reference_py(s: pd.Series) -> pd.Series:
    return _bool_series([V.is_valid_local_gov_code(v) for v in s.tolist()])


def wareki_reference_py(s: pd.Series) -> pd.Series:
    return _bool_series([W.is_valid_wareki(v) for v in s.tolist()])


def current_postal_codes() -> frozenset[str]:
    """Codes of :func:`postal_reference` with ``valid_to IS NULL`` (odd ids) -- in force on the as-of date."""
    return frozenset(f"{i * 83:07d}" for i in range(POSTAL_REFERENCE_ROWS) if i % 2 == 1)


def make_pandas_udfs() -> dict[str, Callable[..., Column]]:
    current = current_postal_codes()

    def postal_set(s: pd.Series) -> pd.Series:
        return _bool_series([None if v is None else v in current for v in s.tolist()])

    return {
        "corporate_stdnum": pandas_udf("boolean")(corporate_stdnum_py),
        "corporate_reference": pandas_udf("boolean")(corporate_reference_py),
        "corporate_vectorized": pandas_udf("boolean")(corporate_vectorized_py),
        "local_gov_reference": pandas_udf("boolean")(local_gov_reference_py),
        "wareki_reference": pandas_udf("boolean")(wareki_reference_py),
        "postal_set": pandas_udf("boolean")(postal_set),
    }


# --- harness ------------------------------------------------------------------------------------------------


def not_violation(problem: Column, source: Column) -> Column:
    """TRUE when the value passes, FALSE when it violates, NULL when it is NULL."""
    return F.when(source.isNotNull(), problem.isNull())


def tally(df: DataFrame, column: str) -> tuple[int, int, int]:
    row = df.agg(
        F.count(F.when(F.col(column), 1)).alias("t"),
        F.count(F.when(~F.col(column), 1)).alias("f"),
        F.count(F.when(F.col(column).isNull(), 1)).alias("n"),
    ).collect()[0]
    return int(row["t"]), int(row["f"]), int(row["n"])


def sink(df: DataFrame) -> None:
    df.write.format("noop").mode("overwrite").save()


def timed(run: Callable[[], None], repeats: int) -> list[float]:
    out = []
    for _ in range(repeats):
        start = time.perf_counter()
        run()
        out.append(time.perf_counter() - start)
    return out


def environment(spark: SparkSession, rows: int, args: argparse.Namespace) -> dict[str, Any]:
    import pyspark

    jvm = spark.sparkContext._jvm
    mem_gb = None
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            mem_gb = round(int(f.readline().split()[1]) / 1024 / 1024, 1)
    except OSError:
        pass
    return {
        "rows": rows,
        "repeats": args.repeats,
        "data_source": args.data_source,
        "os": platform.platform(),
        "cpu_count": os.cpu_count(),
        "memory_gb": mem_gb,
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "java": jvm.System.getProperty("java.version") if jvm else None,
        "spark_master": spark.sparkContext.master,
        "driver_memory": spark.conf.get("spark.driver.memory", "default"),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "arrow_enabled": spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"),
        "arrow_batch_size": spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch"),
        "jpdq_oga": _version("jpdq-oga"),
        "python_stdnum": _version("python-stdnum"),
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "databricks_labs_dqx": _version("databricks-labs-dqx"),
    }


def _version(distribution: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(distribution)
    except PackageNotFoundError:
        return None


def build_groups(
    spark: SparkSession, skip_udf: bool
) -> list[tuple[str, str, dict[str, Callable[[DataFrame], DataFrame]]]]:
    """``(group, input column, {implementation: DataFrame -> DataFrame with a boolean ``r`` column})``."""
    from databricks.labs.dqx.engine import DQEngineCore

    from jpdq_oga import get_japan_checks

    udfs = {} if skip_udf else make_pandas_udfs()
    col = F.col

    def native(problem: Callable[[Column], Column], name: str) -> Callable[[DataFrame], DataFrame]:
        return lambda df: df.select(not_violation(problem(col(name)), col(name)).alias("r"))

    def udf(fn_name: str, name: str) -> Callable[[DataFrame], DataFrame]:
        return lambda df: df.select(udfs[fn_name](col(name)).alias("r"))

    groups: list[tuple[str, str, dict[str, Callable[[DataFrame], DataFrame]]]] = [
        ("corporate_number", "corporate_number", {"native": native(E.corporate_number_problem, "corporate_number")}),
        ("local_gov_code", "local_gov_code", {"native": native(E.local_gov_code_problem, "local_gov_code")}),
        ("wareki_date", "wareki_date", {"native": native(E.wareki_problem, "wareki_date")}),
    ]
    if not skip_udf:
        groups[0][2].update(
            {
                "pandas_udf_stdnum": udf("corporate_stdnum", "corporate_number"),
                "pandas_udf_reference": udf("corporate_reference", "corporate_number"),
                "pandas_udf_vectorized": udf("corporate_vectorized", "corporate_number"),
            }
        )
        groups[1][2]["pandas_udf_reference"] = udf("local_gov_reference", "local_gov_code")
        groups[2][2]["pandas_udf_reference"] = udf("wareki_reference", "wareki_date")

    # reference-data existence: DQX dataset-level check (broadcast join) vs a pandas UDF with a Python set
    engine = DQEngineCore(workspace_client=MagicMock(), spark=spark)
    ref = postal_reference(spark)
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "is_existing_jp_postal_code",
                "arguments": {"column": "postal_code", "ref_df_name": "postal", "as_of": "2026-01-01"},
            },
        }
    ]

    def postal_native(df: DataFrame) -> DataFrame:
        out = engine.apply_checks_by_metadata(
            df.select("postal_code"), checks, custom_check_functions=get_japan_checks(), ref_dfs={"postal": ref}
        )
        return out.select(F.when(col("postal_code").isNotNull(), F.size(F.col("_errors")) == 0).alias("r"))

    postal_impls: dict[str, Callable[[DataFrame], DataFrame]] = {"native_dqx_broadcast_join": postal_native}
    if not skip_udf:
        postal_impls["pandas_udf_set"] = udf("postal_set", "postal_code")
    groups.append(("postal_code_exists", "postal_code", postal_impls))

    # DQX end to end: the four row-level rules in one pass
    row_checks = [
        {"criticality": "error", "check": {"function": f, "arguments": {"column": c}}}
        for f, c in (
            ("is_valid_jp_corporate_number", "corporate_number"),
            ("is_valid_jp_invoice_number", "corporate_number"),
            ("is_valid_jp_local_gov_code", "local_gov_code"),
            ("is_valid_jp_wareki_date", "wareki_date"),
        )
    ]

    def dqx_rows(df: DataFrame) -> DataFrame:
        out = engine.apply_checks_by_metadata(df, row_checks, custom_check_functions=get_japan_checks())
        return out.select(F.when(col("corporate_number").isNotNull(), F.size(F.col("_errors")) == 0).alias("r"))

    groups.append(("dqx_four_row_rules", "corporate_number", {"native_dqx_apply_checks": dqx_rows}))
    return groups


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--skip-udf", action="store_true", help="native implementations only (no Python workers)")
    parser.add_argument("--only", nargs="*", help="run only these groups")
    parser.add_argument("--data-source", choices=["parquet", "generated"], default="parquet")
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark-output"))
    parser.add_argument("--driver-memory", default=os.environ.get("BENCH_DRIVER_MEMORY", "6g"))
    args = parser.parse_args(argv)

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("jpdq-oga-benchmark")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.ansi.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    work = Path(tempfile.mkdtemp(prefix="jpdq_bench_"))
    path = str(work / "data.parquet")
    if args.data_source == "parquet":
        generate(spark, args.rows).write.mode("overwrite").parquet(path)

    def load(n: int) -> DataFrame:
        """The first ``n`` rows (the whole data when ``n`` is the full size)."""
        if args.data_source == "generated":
            return generate(spark, n)
        df = spark.read.parquet(path)
        return df.limit(n) if n < args.rows else df

    groups = [g for g in build_groups(spark, args.skip_udf) if not args.only or g[0] in args.only]
    results: list[dict[str, Any]] = []
    agreement: dict[str, Any] = {}

    for group, _column, impls in groups:
        counts = {}
        for name, make in impls.items():
            counts[name] = tally(make(load(args.rows)), "r")
        agreement[group] = {name: {"true": c[0], "false": c[1], "null": c[2]} for name, c in counts.items()}
        if len(set(counts.values())) != 1:
            print(f"DISAGREEMENT in group {group}: {counts}", file=sys.stderr)
            Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            (args.output_dir / "disagreement.json").write_text(json.dumps(agreement, indent=2), encoding="utf-8")
            return 3
        for name, make in impls.items():
            sink(make(load(max(args.rows // 10, 1))))  # warm-up
            seconds = timed(lambda make=make: sink(make(load(args.rows))), args.repeats)
            results.append(
                {
                    "group": group,
                    "implementation": name,
                    "seconds": [round(s, 3) for s in seconds],
                    "median_seconds": round(statistics.median(seconds), 3),
                    "rows_per_second": round(args.rows / statistics.median(seconds)),
                }
            )
            print(f"{group:22s} {name:28s} median {statistics.median(seconds):8.2f}s  {seconds}", flush=True)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {"environment": environment(spark, args.rows, args), "agreement": agreement, "results": results}
    (args.output_dir / "results.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    (args.output_dir / "results.md").write_text(render_markdown(report), encoding="utf-8")
    print((args.output_dir / "results.md").read_text(encoding="utf-8"))
    spark.stop()
    return 0


def render_markdown(report: dict[str, Any]) -> str:
    env = report["environment"]
    lines = [
        f"rows: {env['rows']:,}, repeats: {env['repeats']} (median shown), master: {env['spark_master']}",
        "",
        "| check | implementation | median (s) | rows/s | vs native |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    baseline: dict[str, float] = {}
    for r in report["results"]:
        if r["implementation"].startswith("native"):
            baseline[r["group"]] = r["median_seconds"]
    for r in report["results"]:
        base = baseline.get(r["group"])
        ratio = f"{r['median_seconds'] / base:.1f}x" if base else ""
        lines.append(
            f"| {r['group']} | {r['implementation']} | {r['median_seconds']:.2f} | {r['rows_per_second']:,} | {ratio} |"
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
