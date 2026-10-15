"""Character-set profile: how many rows of each column hold half-width kana / platform-dependent / unencodable
characters, and what share of the non-null rows that is.

Unlike the row checks, this returns one summary row per column, for judging a source before deciding whether to
fail on it (e.g. before writing a Shift_JIS file).  One pass over the data, Spark built-ins only.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from . import charsets as _charsets
from . import expressions as E

PROFILE_COLUMNS = (
    "column",
    "non_null_rows",
    "halfwidth_kana_rows",
    "halfwidth_kana_rate",
    "platform_dependent_rows",
    "platform_dependent_rate",
    "unencodable_rows",
    "unencodable_rate",
    "any_rows",
    "any_rate",
)


def _count_when(condition: Column) -> Column:
    return F.count(F.when(condition, F.lit(1)))


def charset_profile(df: DataFrame, columns: Sequence[str], charset: str = _charsets.DEFAULT_CHARSET) -> DataFrame:
    """One row per column of ``columns`` with the counts and rates described in :data:`PROFILE_COLUMNS`.

    Rates are shares of the column's **non-null** rows (``NULL`` when the column has no non-null row).
    ``unencodable_*`` is judged against ``charset`` (``cp932`` or ``shift_jis``).  Rows are counted once per
    category, however many offending characters they hold; ``any_*`` counts rows with at least one of the three.
    """
    _charsets.validate_charset(charset)
    if not columns:
        raise ValueError("columns must not be empty")
    aggregates: list[Column] = []
    for index, name in enumerate(columns):
        s = F.col(f"`{name}`").cast("string")
        kana = E.halfwidth_kana_cases(s)[0][0]
        platform = E.platform_dependent_cases(s)[0][0]
        unencodable = E.unencodable_cases(s, charset)[0][0]
        aggregates += [
            F.count(s).alias(f"nn_{index}"),
            _count_when(kana).alias(f"kana_{index}"),
            _count_when(platform).alias(f"plat_{index}"),
            _count_when(unencodable).alias(f"unenc_{index}"),
            _count_when(kana | platform | unencodable).alias(f"any_{index}"),
        ]
    wide = df.agg(*aggregates)
    stack_args = ", ".join(
        "'{}', nn_{i}, kana_{i}, plat_{i}, unenc_{i}, any_{i}".format(name.replace("'", "\\'"), i=index)
        for index, name in enumerate(columns)
    )
    long_form = wide.selectExpr(
        f"stack({len(columns)}, {stack_args}) as (`column`, non_null_rows, halfwidth_kana_rows, "
        "platform_dependent_rows, unencodable_rows, any_rows)"
    )

    def rate(rows: str) -> Column:
        return F.when(F.col("non_null_rows") > 0, F.col(rows) / F.col("non_null_rows"))

    return long_form.select(
        "column",
        "non_null_rows",
        "halfwidth_kana_rows",
        rate("halfwidth_kana_rows").alias("halfwidth_kana_rate"),
        "platform_dependent_rows",
        rate("platform_dependent_rows").alias("platform_dependent_rate"),
        "unencodable_rows",
        rate("unencodable_rows").alias("unencodable_rate"),
        "any_rows",
        rate("any_rows").alias("any_rate"),
    )
