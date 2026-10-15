"""Spark-native column expressions (no Python UDFs, no pandas UDFs).

Every function takes a column name or a :class:`pyspark.sql.Column` and returns a ``Column``.
The ``*_problem`` expressions yield the same reason codes as :mod:`jpdq_oga.validators`
(``format`` / ``check_digit`` / ``prefecture``) and ``NULL`` when the value is fine or missing; they
are verified against the pure-Python validators in ``tests/test_expressions_parity.py`` with ANSI
mode both on and off.  Only built-in functions are used, so the plan runs as ordinary Catalyst
(and Photon) expressions.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column
from pyspark.sql import functions as F

from . import wareki as _wareki
from .validators import CHECK_DIGIT, ERA_BOUNDARY, FORMAT, NOT_EXIST, PRE_GREGORIAN, PREFECTURE

ColumnLike = str | Column

_WS_CLASS = r" \t\n\r\f\x0B"  # ASCII whitespace, spelled so that Python and Java regex agree


def _c(column: ColumnLike) -> Column:
    return F.col(column) if isinstance(column, str) else column


def _digit(s: Column, position: int) -> Column:
    return F.substring(s, position, 1).cast("int")


def _map_literal(mapping: dict[str, str]) -> Column:
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])


def _corporate_check_ok(s13: Column) -> Column:
    """The first of 13 digits equals ``9 - (S mod 9)`` of the following twelve (国税庁)."""
    total = F.lit(0)
    for j in range(2, 14):  # string positions 2..13 hold the 12 base digits
        position_from_right = 14 - j
        total = total + _digit(s13, j) * (1 if position_from_right % 2 else 2)
    return _digit(s13, 1) == (F.lit(9) - (total % 9))


def corporate_number_problem(column: ColumnLike) -> Column:
    """``format`` / ``check_digit`` / ``NULL`` for a 13-digit 法人番号."""
    s = _c(column).cast("string")
    return F.when(~s.rlike(r"\A[0-9]{13}\z"), F.lit(FORMAT)).when(~_corporate_check_ok(s), F.lit(CHECK_DIGIT))


def invoice_number_problem(column: ColumnLike) -> Column:
    """``format`` / ``check_digit`` / ``NULL`` for ``T`` + 法人番号 (corporate registration numbers)."""
    s = _c(column).cast("string")
    return F.when(~s.rlike(r"\AT[0-9]{13}\z"), F.lit(FORMAT)).when(
        ~_corporate_check_ok(F.substring(s, 2, 13)), F.lit(CHECK_DIGIT)
    )


def local_gov_code_problem(column: ColumnLike) -> Column:
    """``format`` / ``prefecture`` / ``check_digit`` / ``NULL`` for a 6-digit 全国地方公共団体コード."""
    s = _c(column).cast("string")
    total = F.lit(0)
    for position, weight in zip(range(1, 6), (6, 5, 4, 3, 2), strict=True):
        total = total + _digit(s, position) * weight
    expected = (F.lit(11) - (total % 11)) % 10
    return (
        F.when(~s.rlike(r"\A[0-9]{6}\z"), F.lit(FORMAT))
        .when(~F.substring(s, 1, 2).cast("int").between(1, 47), F.lit(PREFECTURE))
        .when(_digit(s, 6) != expected, F.lit(CHECK_DIGIT))
    )


# --- 和暦 --------------------------------------------------------------------------------------------


def _anchored(body: str) -> str:
    return rf"\A(?:{body})\z"


def wareki_problem(column: ColumnLike) -> Column:
    """Reason code for an invalid era date, ``NULL`` when it is a real date within its era.

    ``format`` / ``pre_gregorian`` / ``not_exist`` / ``era_boundary``, judged in this order as in
    :func:`jpdq_oga.wareki.wareki_problem` (same canonicalisation pipeline).
    """
    rx = _anchored(_wareki.PATTERN_CANONICAL)
    s = F.translate(_c(column).cast("string"), _wareki.WIDTH_FROM, _wareki.WIDTH_TO)
    s = F.regexp_replace(s, rf"[{_WS_CLASS}]+", "")
    s = F.translate(s, _wareki.KANSUJI_DIGITS_FROM, _wareki.KANSUJI_DIGITS_TO)

    def canonicalise(x: Column) -> Column:
        """The rewrite steps are the identity on strings that already have the canonical shape, so those
        (the common case) skip them; ``otherwise`` is only evaluated for the rest."""
        rewritten = x
        for pattern, replacement in _wareki.CANONICAL_STEPS:
            rewritten = F.regexp_replace(rewritten, pattern, replacement)
        return F.when(x.rlike(rx), x).otherwise(rewritten)

    s = F.transform(F.array(s), canonicalise)[0]

    # token -> era data, so that the expression does not grow with the number of eras
    first_years: dict[str, str] = {}
    starts: dict[str, str] = {}
    ends: dict[str, str] = {}
    for e in _wareki.ERAS:
        for token in (e.name, e.letter):
            first_years[token] = str(e.first_year)
            starts[token] = e.start.isoformat()
            ends[token] = (e.end or date(9999, 12, 31)).isoformat()
    first_year_map, start_map, end_map = _map_literal(first_years), _map_literal(starts), _map_literal(ends)
    meiji = _wareki.ERAS[0]

    def judge(p: Column) -> Column:
        """The verdict from the already-extracted parts (``p`` is a struct bound once, see below)."""
        era, year, month, day = p["era"], p["y"], p["m"], p["d"]
        first_year = F.try_element_at(first_year_map, era).cast("int")
        era_start = F.try_element_at(start_map, era).cast("date")
        era_end = F.try_element_at(end_map, era).cast("date")
        old_calendar = (first_year == meiji.first_year) & (year <= _wareki.LAST_PRE_GREGORIAN_MEIJI_YEAR)

        # evaluated only after year >= 1 is known (CASE WHEN is lazy), so make_date never sees bad input
        western = first_year + year - 1
        days_in_month = F.dayofmonth(F.last_day(F.make_date(western, month, F.lit(1))))
        exists = F.when(month.between(1, 12), (day >= 1) & (day <= days_in_month))
        candidate = F.when(exists, F.make_date(western, month, day))
        return (
            F.when(p["is_null"], F.lit(None).cast("string"))  # NULL is never a violation
            .when(~p["ok"], F.lit(FORMAT))
            .when(year < 1, F.lit(NOT_EXIST))
            .when(old_calendar, F.lit(PRE_GREGORIAN))
            .when(~F.coalesce(exists, F.lit(False)), F.lit(NOT_EXIST))
            .when(~candidate.between(era_start, era_end), F.lit(ERA_BOUNDARY))
        )

    def extract(x: Column) -> Column:
        """Split the canonical string once into (is_null, ok, era, y, m, d) and judge it."""

        def part(k: int) -> Column:
            g = F.regexp_extract(x, rx, k)
            return F.when(g != "", g)

        parts = F.struct(
            x.isNull().alias("is_null"),
            x.rlike(rx).alias("ok"),
            F.upper(part(1)).alias("era"),
            part(2).cast("int").alias("y"),
            part(3).cast("int").alias("m"),
            part(4).cast("int").alias("d"),
        )
        return F.transform(F.array(parts), judge)[0]

    # Spark has no ``let``: binding through a one-element array and a lambda evaluates the canonical
    # string (and then its parts) once instead of once per use, which is what keeps this expression fast.
    return F.transform(F.array(s), extract)[0]
