import os
import sys

import pytest


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    # make sure the workers use the interpreter running the tests
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("jpdq-oga-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    assert pyspark.__version__
    yield session
    session.stop()


def string_frame(spark, values, column="v"):
    """DataFrame ``(i int, <column> string)`` built entirely inside the JVM.

    Avoids ``createDataFrame`` on Python objects (which needs a Python worker process) and,
    unlike a plain literal table, is not constant-folded by the optimizer.
    """
    from pyspark.sql import functions as F

    literals = F.array(*[F.lit(v).cast("string") for v in values])
    return spark.range(len(values)).select(
        F.col("id").cast("int").alias("i"),
        F.element_at(literals, (F.col("id") + 1).cast("int")).alias(column),
    )


def rows_frame(spark, columns, rows, date_columns=()):
    """DataFrame of string columns (``None`` stays NULL) built inside the JVM; ``date_columns`` are cast to date."""
    from pyspark.sql import functions as F

    structs = [F.struct(*[F.lit(v).cast("string").alias(c) for c, v in zip(columns, row, strict=True)]) for row in rows]
    df = spark.range(1).select(F.explode(F.array(*structs)).alias("r")).select("r.*")
    for column in date_columns:
        df = df.withColumn(column, F.col(column).cast("date"))
    return df


@pytest.fixture(params=[False, True], ids=["ansi-off", "ansi-on"])
def ansi(spark, request):
    spark.conf.set("spark.sql.ansi.enabled", str(request.param).lower())
    yield request.param
    spark.conf.set("spark.sql.ansi.enabled", "false")
