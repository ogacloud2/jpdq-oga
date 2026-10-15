"""Time-aware reference data (郵便番号・地方公共団体コード) and the Spark logic that joins against it.

Reference tables carry a validity period per row:

* ``valid_from`` (inclusive; ``NULL`` = in effect since before the first snapshot we observed)
* ``valid_to``   (exclusive, the first day on which the row no longer applied; ``NULL`` = still current)

so a check can be run *as of* a date (``as_of``) instead of against whatever file happens to be
loaded.  Everything here is JVM-native Spark (joins, aggregates, higher-order functions on built-in
expressions); there are no Python UDFs.

Tables
------
``postal_codes``      ``postal_code`` (7 digits), ``local_gov_code`` (5 digits as in KEN_ALL),
                      ``prefecture``, ``city``, ``valid_from``, ``valid_to``
``local_gov_codes``   ``code`` (6 digits with check digit), ``prefecture``, ``city``,
                      ``valid_from``, ``valid_to``
"""

from __future__ import annotations

import re
import zipfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

VALID_FROM = "valid_from"
VALID_TO = "valid_to"

POSTAL_DATA_COLUMNS = ("postal_code", "local_gov_code", "prefecture", "city")
POSTAL_COLUMNS = (*POSTAL_DATA_COLUMNS, VALID_FROM, VALID_TO)
LOCAL_GOV_DATA_COLUMNS = ("code", "prefecture", "city")
LOCAL_GOV_COLUMNS = (*LOCAL_GOV_DATA_COLUMNS, VALID_FROM, VALID_TO)

FORMAT_REASON = "format"
NOT_FOUND = "not_found"
MISMATCH = "mismatch"

# --- the 15 columns of Japan Post's KEN_ALL (utf_ken_all.csv / ken_all.csv) -------------------------
KEN_ALL_COLUMNS = (
    "local_gov_code",  # 1 全国地方公共団体コード
    "old_postal_code",  # 2 (旧)郵便番号 5桁
    "postal_code",  # 3 郵便番号 7桁
    "prefecture_kana",  # 4
    "city_kana",  # 5
    "town_kana",  # 6
    "prefecture",  # 7 都道府県名 (漢字)
    "city",  # 8 市区町村名 (漢字)
    "town",  # 9 町域名 (漢字)
    "multiple_codes_per_town",  # 10 一町域が二以上の郵便番号で表される場合の表示
    "koaza_numbered",  # 11 小字毎に番地が起番されている町域の表示
    "has_chome",  # 12 丁目を有する町域の場合の表示
    "multiple_towns_per_code",  # 13 一つの郵便番号で二以上の町域を表す場合の表示
    "update_flag",  # 14 更新の表示
    "update_reason",  # 15 変更理由
)


class ReferenceDataError(ValueError):
    """The reference data is missing, malformed or inconsistent."""


# --- loading ----------------------------------------------------------------------------------------


def parse_as_of(as_of: str | date | None) -> date | None:
    """``"2026-04-01"`` -> ``date``; ``None`` stays ``None`` (= today at run time)."""
    if as_of is None or isinstance(as_of, date):
        return as_of
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", as_of):
        raise ReferenceDataError(f"as_of must be an ISO date (YYYY-MM-DD), got {as_of!r}")
    try:
        return date.fromisoformat(as_of)
    except ValueError as exc:
        raise ReferenceDataError(f"as_of is not a real date: {as_of!r}") from exc


def validate_ref_source(ref_df_name: str | None, ref_table: str | None, ref_path: str | None) -> None:
    given = [
        name
        for name, value in (("ref_df_name", ref_df_name), ("ref_table", ref_table), ("ref_path", ref_path))
        if value
    ]
    if len(given) != 1:
        raise ReferenceDataError(
            "exactly one of ref_df_name (a DataFrame passed as ref_dfs), ref_table (Unity Catalog / metastore table) "
            f"or ref_path (Delta/Parquet location, e.g. a Volume) is required; got {given or 'none'}"
        )


def load_reference(
    spark: SparkSession,
    ref_dfs: dict[str, DataFrame] | None,
    ref_df_name: str | None,
    ref_table: str | None,
    ref_path: str | None,
    ref_format: str,
    required_columns: tuple[str, ...],
) -> DataFrame:
    """The reference DataFrame from one of the three sources, with its columns verified."""
    if ref_df_name:
        if not ref_dfs or ref_df_name not in ref_dfs:
            raise ReferenceDataError(
                f"reference DataFrame {ref_df_name!r} was not provided; pass it in ref_dfs when applying the checks"
            )
        ref = ref_dfs[ref_df_name]
    elif ref_table:
        ref = spark.table(ref_table)
    else:
        assert ref_path
        ref = spark.read.format(ref_format).load(ref_path)
    missing = [c for c in required_columns if c not in ref.columns]
    if missing:
        raise ReferenceDataError(
            f"reference data lacks column(s) {missing}; expected {list(required_columns)} "
            "(build it with `python -m jpdq_oga.cli`)"
        )
    return ref


# --- validity ---------------------------------------------------------------------------------------


def as_of_expression(as_of: date | None, as_of_column: Column | None) -> Column:
    """The date each row is validated against: the column (falling back to today), a literal, or today."""
    if as_of_column is not None:
        return F.coalesce(as_of_column.cast("date"), F.lit(as_of) if as_of else F.current_date())
    return F.lit(as_of) if as_of else F.current_date()


def valid_at(entry: Column, as_of: Column) -> Column:
    """Is the reference entry (a struct with valid_from / valid_to) in force on ``as_of``?"""
    return (entry[VALID_FROM].isNull() | (entry[VALID_FROM] <= as_of)) & (
        entry[VALID_TO].isNull() | (as_of < entry[VALID_TO])
    )


def columns_valid_at(as_of: Column) -> Column:
    """Row filter for a reference table: ``valid_from`` / ``valid_to`` columns in force on ``as_of``."""
    valid_from, valid_to = F.col(VALID_FROM), F.col(VALID_TO)
    return (valid_from.isNull() | (valid_from <= as_of)) & (valid_to.isNull() | (as_of < valid_to))


def entries_by_key(ref: DataFrame, key: str, fields: tuple[str, ...], key_alias: str, entries_alias: str) -> DataFrame:
    """One row per key with an array of its (field..., valid_from, valid_to) entries.

    Collapsing to one row per key keeps the later join from multiplying input rows, whatever the
    number of versions or cities a key has.
    """
    entry = F.struct(*[F.col(f) for f in (*fields, VALID_FROM, VALID_TO)])
    return ref.groupBy(F.col(key).alias(key_alias)).agg(F.collect_set(entry).alias(entries_alias))


def postal_key(column: Column) -> Column:
    """7 digits from ``1000001`` or ``100-0001`` (ASCII only); ``NULL`` for anything else."""
    s = column.cast("string")
    return F.when(s.rlike(r"\A[0-9]{7}\z"), s).when(s.rlike(r"\A[0-9]{3}-[0-9]{4}\z"), F.regexp_replace(s, "-", ""))


# --- building reference tables ---------------------------------------------------------------------


def extract_single_csv(zip_path: str | Path, destination: str | Path) -> Path:
    """Extract the only CSV in a Japan Post zip into ``destination`` (an empty, dedicated directory)."""
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ReferenceDataError(f"expected exactly one CSV in {zip_path}, found {archive.namelist()}")
        target = destination / "ken_all.csv"
        with archive.open(names[0]) as source, open(target, "wb") as out:
            out.write(source.read())
    return target


# Spark 4 restricts the CSV reader to these charsets unless ``spark.sql.legacy.javaCharsets`` is true.
_SPARK_NATIVE_CHARSETS = frozenset({"iso-8859-1", "us-ascii", "utf-16", "utf-16be", "utf-16le", "utf-32", "utf-8"})


def read_ken_all(spark: SparkSession, csv_path: str | Path, encoding: str = "utf-8") -> DataFrame:
    """Distinct ``postal_code, local_gov_code, prefecture, city`` rows of a KEN_ALL CSV (JVM-native read).

    ``encoding`` other than UTF-8 (e.g. ``cp932`` for the legacy Shift_JIS file) needs
    ``spark.sql.legacy.javaCharsets`` on Spark 4; it is switched on for the session when the encoding is not
    one Spark 4 reads natively. If the platform refuses that, a :class:`ReferenceDataError` says to use the
    UTF-8 file (``utf_ken_all.zip``).
    """
    uses_legacy_charset = encoding.lower() not in _SPARK_NATIVE_CHARSETS
    if uses_legacy_charset:
        try:
            spark.conf.set("spark.sql.legacy.javaCharsets", "true")
        except Exception:  # noqa: BLE001 - a locked-down platform; the read below reports the problem
            pass
    schema = ", ".join(f"{name} string" for name in KEN_ALL_COLUMNS)
    raw = (
        spark.read.option("encoding", encoding)
        .option("header", "false")
        .option("quote", '"')
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(str(csv_path))
    )
    parsed = raw.select(*POSTAL_DATA_COLUMNS)
    bad = parsed.filter(
        ~F.col("postal_code").rlike(r"\A[0-9]{7}\z")
        | ~F.col("local_gov_code").rlike(r"\A[0-9]{5}\z")
        | F.col("prefecture").isNull()
        | F.col("city").isNull()
    )
    try:
        has_bad_rows = bool(bad.limit(1).count())
    except Exception as exc:
        if uses_legacy_charset and "charset" in str(exc).lower():
            raise ReferenceDataError(
                f"this Spark session cannot read the {encoding!r} encoding ({str(exc).splitlines()[0]}). "
                "Use the UTF-8 file (utf_ken_all.zip) with encoding 'utf-8' instead."
            ) from exc
        raise
    if has_bad_rows:
        sample = bad.limit(3).collect()
        raise ReferenceDataError(
            f"{csv_path} is not a KEN_ALL file (expected 15 columns: 5-digit local government code, "
            f"7-digit postal code, prefecture and city names). Offending rows: {sample}"
        )
    return parsed.distinct()


def frame_from_rows(
    spark: SparkSession,
    columns: tuple[str, ...],
    rows: Sequence[tuple[str | None, ...]],
    date_columns: tuple[str, ...] = (),
) -> DataFrame:
    """A DataFrame of string columns built inside the JVM (no Python worker needed); some cast to date."""
    if not rows:
        raise ReferenceDataError("no rows to build a table from")
    structs = [F.struct(*[F.lit(v).cast("string").alias(c) for c, v in zip(columns, row, strict=True)]) for row in rows]
    df = spark.range(1).select(F.explode(F.array(*structs)).alias("r")).select("r.*")
    for column in date_columns:
        df = df.withColumn(column, F.col(column).cast("date"))
    return df


def _same_fact(left: DataFrame, right: DataFrame, columns: tuple[str, ...]) -> Column:
    cond = F.lit(True)
    for c in columns:
        cond = cond & left[c].eqNullSafe(right[c])
    return cond


def apply_snapshot(
    existing: DataFrame | None,
    snapshot: DataFrame,
    data_columns: tuple[str, ...],
    snapshot_date: date,
) -> DataFrame:
    """Fold a full snapshot into a reference table with validity periods.

    * facts in the snapshot that are not currently open get ``valid_from = snapshot_date``
      (``NULL`` for the very first snapshot: we only know they existed *before* it);
    * open facts missing from the snapshot are closed with ``valid_to = snapshot_date``;
    * everything else is left untouched, so applying the same snapshot twice changes nothing.

    The history is only as fine as the snapshot cadence: a change is dated to the snapshot that first
    shows it, not to the day it took effect.
    """
    columns = list(data_columns)
    snap = snapshot.select(*columns).distinct()
    null_date = F.lit(None).cast("date")
    if existing is None:
        return snap.withColumn(VALID_FROM, null_date).withColumn(VALID_TO, null_date)

    latest = existing.agg(F.max(F.greatest(F.col(VALID_FROM), F.col(VALID_TO))).alias("d")).collect()[0]["d"]
    if latest is not None and snapshot_date < latest:
        raise ReferenceDataError(
            f"snapshot date {snapshot_date} is earlier than the latest change already recorded ({latest}); "
            "apply snapshots in chronological order"
        )

    ordered = [*columns, VALID_FROM, VALID_TO]
    is_open = F.col(VALID_TO).isNull()
    open_rows = existing.filter(is_open).select(*ordered)
    closed_rows = existing.filter(~is_open).select(*ordered)

    kept = open_rows.alias("o").join(
        snap.alias("s"), _same_fact(open_rows.alias("o"), snap.alias("s"), data_columns), "left_semi"
    )
    ended = (
        open_rows.alias("o")
        .join(snap.alias("s"), _same_fact(open_rows.alias("o"), snap.alias("s"), data_columns), "left_anti")
        .withColumn(VALID_TO, F.lit(snapshot_date))
    )
    opened = (
        snap.alias("s")
        .join(open_rows.alias("o"), _same_fact(snap.alias("s"), open_rows.alias("o"), data_columns), "left_anti")
        .withColumn(VALID_FROM, F.lit(snapshot_date))
        .withColumn(VALID_TO, null_date)
    )
    return (
        closed_rows.unionByName(kept.select(*ordered))
        .unionByName(ended.select(*ordered))
        .unionByName(opened.select(*ordered))
    )
