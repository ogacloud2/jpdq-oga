"""DQX check functions for Japan-specific data.

Row-level checks in DQX's custom-check form (``make_condition`` + ``register_rule("row")``).
Usable from Python (``DQRowRule(check_func=is_valid_jp_corporate_number, column="...")``) and from
YAML/JSON metadata with ``custom_check_functions=get_japan_checks()``.

Conventions: ``NULL`` passes (add ``is_not_null`` for mandatory columns); input is judged as given
(no trimming or width conversion); every check takes ``lang="ja"`` (default) or ``"en"`` for the
violation message.  Everything is built from Spark built-in functions, no Python UDFs.
"""

# NOTE: no `from __future__ import annotations` here: DQX resolves the parameter annotations of
# check functions at runtime (pydantic) and needs real types, not strings.

import uuid
from collections.abc import Callable

try:
    from databricks.labs.dqx.check_funcs import get_normalized_column_and_expr, make_condition
    from databricks.labs.dqx.rule import register_rule
except ImportError as exc:  # pragma: no cover - exercised only without the optional dependency
    raise ImportError(
        f"jpdq_oga.checks needs databricks-labs-dqx and its runtime dependencies ({exc}). "
        "Outside a Databricks runtime install them with: pip install 'jpdq-oga[dqx]'"
    ) from exc

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession

from . import expressions as E
from . import messages as M
from . import reference as R

MAX_SHOWN_VALUE_LENGTH = 50
"""Longest input value echoed in a violation message; longer values are cut and end with ``…``.

A wrong column (free text, JSON) would otherwise copy whole values into every ``_errors`` entry."""


def _shown(value: Column) -> Column:
    """The input value as shown in a message: cast to string, cut to ``MAX_SHOWN_VALUE_LENGTH`` characters."""
    text = value.cast("string")
    cut = F.concat(F.substring(text, 1, MAX_SHOWN_VALUE_LENGTH), F.lit("…"))
    return F.when(F.length(text) > MAX_SHOWN_VALUE_LENGTH, cut).otherwise(text)


def _problem_condition(
    column: str | Column,
    problem_expr: Callable[[Column], Column],
    kind: str,
    lang: str,
    alias_suffix: str,
    bind_once: bool = False,
    cases_expr: Callable[[Column], list[tuple[Column, str]]] | None = None,
) -> Column:
    """``make_condition`` that fails when ``problem_expr`` yields a reason code.

    ``bind_once`` evaluates the reason code a single time per use of the result column (see below); it is
    only worth it for expensive reason codes, because Spark runs higher-order functions interpreted
    instead of generating code for them, which made cheap expressions about 9x slower.
    """
    M.validate_language(lang)
    col_str_norm, _, col_expr = get_normalized_column_and_expr(column)
    templates = list(M.TEMPLATES[kind][lang].items())

    def message_of(problem: Column) -> Column:
        """The message for a reason code; NULL (no branch matches) when there is no violation."""
        branches = [
            (problem == code, F.concat(F.lit(prefix), _shown(col_expr), F.lit(suffix)))
            for code, (prefix, suffix) in templates
        ]
        message = F.when(*branches[0])
        for condition, text in branches[1:]:
            message = message.when(condition, text)
        return message

    if cases_expr is not None:
        # cheap checks: CASE WHEN <condition> THEN <message>; every condition is evaluated at most once
        by_code = dict(templates)
        branches = [
            (cond, F.concat(F.lit(by_code[code][0]), _shown(col_expr), F.lit(by_code[code][1])))
            for cond, code in cases_expr(col_expr)
        ]
        direct = F.when(*branches[0])
        for condition, text in branches[1:]:
            direct = direct.when(condition, text)
        return make_condition(F.lit(True), direct, f"{col_str_norm}_{alias_suffix}")
    problem = problem_expr(col_expr)
    if not bind_once:
        # cheap reason codes: plain expressions are code-generated and their repeats are eliminated
        return make_condition(problem.isNotNull(), message_of(problem), f"{col_str_norm}_{alias_suffix}")
    # Expensive reason code (和暦 parses the string).  Spark has no ``let``, so bind it through a
    # one-element array and a lambda: it is evaluated once per use of the result column instead of once for
    # the condition plus once per message branch.  ``condition=True`` because the message is already NULL
    # when there is no violation.
    message = F.transform(F.array(problem), message_of)[0]
    return make_condition(F.lit(True), message, f"{col_str_norm}_{alias_suffix}")


@register_rule("row")
def is_valid_jp_corporate_number(column: str | Column, lang: str = "ja") -> Column:
    """Checks for a valid 13-digit 法人番号 (corporate number) including its check digit."""
    return _problem_condition(
        column, E.corporate_number_problem, "corporate_number", lang, "is_not_valid_jp_corporate_number",
        cases_expr=E.corporate_number_cases,
    )  # fmt: skip


@register_rule("row")
def is_valid_jp_invoice_number(column: str | Column, lang: str = "ja") -> Column:
    """Checks for ``T`` + a valid 法人番号 (インボイス登録番号 of a corporation)."""
    return _problem_condition(
        column, E.invoice_number_problem, "invoice_number", lang, "is_not_valid_jp_invoice_number",
        cases_expr=E.invoice_number_cases,
    )  # fmt: skip


@register_rule("row")
def is_valid_jp_local_gov_code(column: str | Column, lang: str = "ja") -> Column:
    """Checks for a valid 6-digit 全国地方公共団体コード (prefecture 01-47 and check digit)."""
    return _problem_condition(
        column, E.local_gov_code_problem, "local_gov_code", lang, "is_not_valid_jp_local_gov_code",
        cases_expr=E.local_gov_code_cases,
    )  # fmt: skip


@register_rule("row")
def is_valid_jp_wareki_date(column: str | Column, lang: str = "ja") -> Column:
    """Checks that a Japanese-era date string is a real date within the stated era.

    Detects non-existent days (令和6年2月30日), era-boundary violations (平成31年5月1日 is 令和元年,
    令和元年4月30日 is still 平成) and 明治5年以前 (old calendar, outside the supported range).
    Accepts 元年, kanji / fullwidth numerals and the abbreviations R/H/S/T/M.
    """
    return _problem_condition(
        column, E.wareki_problem, "wareki_date", lang, "is_not_valid_jp_wareki_date", bind_once=True
    )


# --- reference-data checks (dataset level: broadcast join against a time-aware reference table) -------


def _message(templates: dict[str, tuple[str, ...]], reason: Column, values: list[Column], as_of_note: str) -> Column:
    """CASE WHEN reason = <code> THEN prefix || value || middle || value || suffix ..."""
    branches = []
    for code, parts in templates.items():
        pieces: list[Column] = []
        for index, part in enumerate(parts):
            pieces.append(F.lit(part.replace("{asof}", as_of_note)))
            if index < len(values) and index < len(parts) - 1:
                pieces.append(_shown(values[index]))
        branches.append((reason == code, F.concat(*pieces)))
    message = F.when(*branches[0])
    for condition, text in branches[1:]:
        message = message.when(condition, text)
    return message


def _reference_rule(
    *,
    kind: str,
    columns: list[str | Column],
    mode: str,
    entries_fields: tuple[str, ...],
    required_columns: tuple[str, ...],
    key_column: str,
    key_expr: Callable[[Column], Column],
    ref_df_name: str | None,
    ref_table: str | None,
    ref_path: str | None,
    ref_format: str,
    as_of: str | None,
    as_of_column: str | Column | None,
    lang: str,
    alias_suffix: str,
) -> tuple[Column, Callable]:
    """Shared body of the dataset-level reference checks.

    ``mode``: ``exists`` (the key must be in force), or ``match:<field>`` (the second column must equal
    ``<field>`` of an in-force entry; unknown values are not flagged).
    """
    M.validate_language(lang)
    R.validate_ref_source(ref_df_name, ref_table, ref_path)
    as_of_date = R.parse_as_of(as_of)
    col_str_norm, _, col_expr = get_normalized_column_and_expr(columns[0])
    other_expr = get_normalized_column_and_expr(columns[1])[2] if len(columns) > 1 else None
    as_of_col_expr = get_normalized_column_and_expr(as_of_column)[2] if as_of_column is not None else None

    uid = uuid.uuid4().hex
    row_key, ref_key, entries_col, reason_col = (f"__jpdq_{n}_{uid}" for n in ("key", "refkey", "entries", "reason"))

    def apply(df: DataFrame, spark: SparkSession, ref_dfs: dict[str, DataFrame]) -> DataFrame:
        ref = R.load_reference(spark, ref_dfs, ref_df_name, ref_table, ref_path, ref_format, required_columns)
        field = mode.split(":", 1)[1] if ":" in mode else None
        if as_of_col_expr is None:
            # One date for every row: keep only the rows in force on it, then a plain equi-join.
            in_force_ref = ref.filter(R.columns_valid_at(R.as_of_expression(as_of_date, None)))
            if field is None:
                by_key = in_force_ref.select(F.col(key_column).alias(ref_key)).distinct()
                found = F.col(ref_key).isNotNull()
            else:
                by_key = in_force_ref.groupBy(F.col(key_column).alias(ref_key)).agg(
                    F.collect_set(F.col(field)).alias(entries_col)
                )
                found = F.col(entries_col).isNotNull()
        else:
            # A date per row: join on the key and test each entry's validity period against the row's date.
            by_key = R.entries_by_key(ref, key_column, entries_fields, ref_key, entries_col)
            in_force = F.filter(
                F.col(entries_col), lambda x: R.valid_at(x, R.as_of_expression(as_of_date, as_of_col_expr))
            )
            found = F.coalesce(F.size(in_force) > 0, F.lit(False))
        key = F.col(row_key)
        if mode == "exists":
            reason = (
                F.when(col_expr.isNull(), F.lit(None).cast("string"))
                .when(key.isNull(), F.lit(R.FORMAT_REASON))
                .when(~found, F.lit(R.NOT_FOUND))
            )
        else:
            assert other_expr is not None and field is not None
            if as_of_col_expr is None:
                agrees = F.coalesce(F.array_contains(F.col(entries_col), other_expr), F.lit(False))
            else:
                agrees = F.coalesce(F.size(F.filter(in_force, lambda x: x[field] == other_expr)) > 0, F.lit(False))
            reason = (
                F.when(col_expr.isNull() | other_expr.isNull() | key.isNull(), F.lit(None).cast("string"))
                .when(~found, F.lit(None).cast("string"))  # unknown postal code: the existence check reports it
                .when(~agrees, F.lit(R.MISMATCH))
            )
        keyed = df.withColumn(row_key, key_expr(col_expr))
        joined = keyed.join(F.broadcast(by_key), F.col(row_key) == F.col(ref_key), "left")
        out = joined.withColumn(reason_col, reason)
        return out.drop(ref_key) if field is None and as_of_col_expr is None else out.drop(ref_key, entries_col)

    values = [col_expr] if other_expr is None else [col_expr, other_expr]
    templates = M.TEMPLATES[kind][lang]
    message = _message(templates, F.col(reason_col), values, M.as_of_note(lang, as_of_date))
    condition = make_condition(F.col(reason_col).isNotNull(), message, f"{col_str_norm}_{alias_suffix}")
    return condition, apply


@register_rule("dataset")
def is_existing_jp_postal_code(
    column: str | Column,
    ref_df_name: str | None = None,
    ref_table: str | None = None,
    ref_path: str | None = None,
    ref_format: str = "delta",
    as_of: str | None = None,
    as_of_column: str | Column | None = None,
    lang: str = "ja",
) -> tuple[Column, Callable]:
    """Checks that the postal code exists in the Japan Post data *as of* a date.

    ``1000001`` and ``100-0001`` are both accepted.  The reference table (``postal_codes``, see
    ``python -m jpdq_oga.cli``) comes from ``ref_df_name`` (a DataFrame in ``ref_dfs``), ``ref_table``
    (Unity Catalog / metastore) or ``ref_path`` (Delta/Parquet, e.g. a Volume).  ``as_of`` is an ISO
    date; ``as_of_column`` takes the date per row; default is the run date.  Large-business-site
    (大口事業所) postal codes are not in this data and are reported as not found.
    """
    return _reference_rule(
        kind="postal_code_exists", columns=[column], mode="exists", entries_fields=("prefecture", "city"),
        required_columns=R.POSTAL_COLUMNS, key_column="postal_code", key_expr=R.postal_key,
        ref_df_name=ref_df_name, ref_table=ref_table, ref_path=ref_path, ref_format=ref_format,
        as_of=as_of, as_of_column=as_of_column, lang=lang, alias_suffix="is_not_existing_jp_postal_code",
    )  # fmt: skip


@register_rule("dataset")
def is_consistent_jp_postal_code_and_prefecture(
    column: str | Column,
    prefecture_column: str | Column,
    ref_df_name: str | None = None,
    ref_table: str | None = None,
    ref_path: str | None = None,
    ref_format: str = "delta",
    as_of: str | None = None,
    as_of_column: str | Column | None = None,
    lang: str = "ja",
) -> tuple[Column, Callable]:
    """Checks that the postal code belongs to the prefecture named in ``prefecture_column``.

    The prefecture must be written as in the Japan Post data (``東京都``, not ``東京``): notation
    differences are reported as mismatches.  NULLs, malformed postal codes and codes that do not exist
    are not flagged here (use ``is_existing_jp_postal_code`` for those).
    """
    return _reference_rule(
        kind="postal_code_prefecture", columns=[column, prefecture_column], mode="match:prefecture",
        entries_fields=("prefecture", "city"), required_columns=R.POSTAL_COLUMNS, key_column="postal_code",
        key_expr=R.postal_key, ref_df_name=ref_df_name, ref_table=ref_table, ref_path=ref_path,
        ref_format=ref_format, as_of=as_of, as_of_column=as_of_column, lang=lang,
        alias_suffix="is_not_consistent_jp_postal_code_and_prefecture",
    )  # fmt: skip


@register_rule("dataset")
def is_consistent_jp_postal_code_and_city(
    column: str | Column,
    city_column: str | Column,
    ref_df_name: str | None = None,
    ref_table: str | None = None,
    ref_path: str | None = None,
    ref_format: str = "delta",
    as_of: str | None = None,
    as_of_column: str | Column | None = None,
    lang: str = "ja",
) -> tuple[Column, Callable]:
    """Checks that the postal code belongs to the municipality named in ``city_column``.

    The value must be written as in the Japan Post data (``千代田区``, ``札幌市中央区`` — designated-city
    wards include the city).  A postal code that spans several municipalities matches any of them.
    """
    return _reference_rule(
        kind="postal_code_city", columns=[column, city_column], mode="match:city",
        entries_fields=("prefecture", "city"), required_columns=R.POSTAL_COLUMNS, key_column="postal_code",
        key_expr=R.postal_key, ref_df_name=ref_df_name, ref_table=ref_table, ref_path=ref_path,
        ref_format=ref_format, as_of=as_of, as_of_column=as_of_column, lang=lang,
        alias_suffix="is_not_consistent_jp_postal_code_and_city",
    )  # fmt: skip


def _local_gov_key(column: Column) -> Column:
    s = column.cast("string")
    return F.when(s.rlike(r"\A[0-9]{6}\z"), s)


@register_rule("dataset")
def is_existing_jp_local_gov_code(
    column: str | Column,
    ref_df_name: str | None = None,
    ref_table: str | None = None,
    ref_path: str | None = None,
    ref_format: str = "delta",
    as_of: str | None = None,
    as_of_column: str | Column | None = None,
    lang: str = "ja",
) -> tuple[Column, Callable]:
    """Checks that the 全国地方公共団体コード exists *as of* a date (detects codes retired by mergers).

    Combine with ``is_valid_jp_local_gov_code`` for the check digit.  The reference table is
    ``local_gov_codes`` (columns ``code, prefecture, city, valid_from, valid_to``).
    """
    return _reference_rule(
        kind="local_gov_code_exists", columns=[column], mode="exists", entries_fields=("prefecture", "city"),
        required_columns=R.LOCAL_GOV_COLUMNS, key_column="code", key_expr=_local_gov_key,
        ref_df_name=ref_df_name, ref_table=ref_table, ref_path=ref_path, ref_format=ref_format,
        as_of=as_of, as_of_column=as_of_column, lang=lang, alias_suffix="is_not_existing_jp_local_gov_code",
    )  # fmt: skip


CHECK_FUNCTIONS: dict[str, Callable[..., Column | tuple[Column, Callable]]] = {
    f.__name__: f
    for f in (
        is_valid_jp_corporate_number,
        is_valid_jp_invoice_number,
        is_valid_jp_local_gov_code,
        is_valid_jp_wareki_date,
        is_existing_jp_postal_code,
        is_consistent_jp_postal_code_and_prefecture,
        is_consistent_jp_postal_code_and_city,
        is_existing_jp_local_gov_code,
    )
}
