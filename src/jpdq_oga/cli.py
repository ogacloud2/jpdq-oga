"""Build the time-aware reference tables used by the reference-data checks.

::

    # 1. get the Japan Post data (zip) and fold it into a reference table
    python -m jpdq_oga.cli postal --zip utf_ken_all.zip --snapshot-date 2026-10-01 \\
        --output-path /Volumes/main/ref/postal_codes --format delta

    # 2. next month: fold the new snapshot into the existing table (history is kept)
    python -m jpdq_oga.cli postal --zip utf_ken_all.zip --snapshot-date 2026-11-01 \\
        --existing-path /Volumes/main/ref/postal_codes --output-path /Volumes/main/ref/postal_codes_v2

    # unity catalog tables instead of paths
    python -m jpdq_oga.cli postal --zip utf_ken_all.zip --snapshot-date 2026-11-01 \\
        --existing-table main.ref.postal_codes --output-table main.ref.postal_codes

    # local government codes with history, from the two 総務省 Excel files
    python -m jpdq_oga.cli local-gov-codes-xlsx --current codes.xlsx --revisions revisions.xlsx \\
        --output-table main.ref.local_gov_codes

    # local government codes from a tidy CSV (code,prefecture,city); later snapshots fold into a table
    python -m jpdq_oga.cli local-gov-codes --csv codes.csv --snapshot-date 2026-04-01 --output-table main.ref.codes

``--snapshot-date`` is the date the source data was published or obtained; changes are dated to the
snapshot that first shows them.  Pass ``--download`` instead of ``--zip`` to fetch the official file
(it can fail: the site may reject non-browser clients; then download it in a browser and use ``--zip``).
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import reference as R
from . import soumu

DEFAULT_POSTAL_URL = "https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_ken_all.zip"


def download(url: str, destination: Path) -> Path:
    """Fetch ``url`` into ``destination``; failures explain the manual fallback."""
    request = urllib.request.Request(url, headers={"User-Agent": "jpdq-oga reference-data builder"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response, open(destination, "wb") as out:  # noqa: S310
            out.write(response.read())
    except (urllib.error.URLError, OSError) as exc:
        raise R.ReferenceDataError(
            f"could not download {url} ({exc}). Download the file in a browser from "
            "https://www.post.japanpost.jp/zipcode/download.html and pass it with --zip"
        ) from exc
    return destination


# --- building (no writes: testable anywhere) ---------------------------------------------------------


def build_postal(
    spark: SparkSession,
    zip_path: Path,
    work_dir: Path,
    encoding: str,
    existing: DataFrame | None,
    snapshot_date: date,
) -> DataFrame:
    """``postal_codes`` table: the KEN_ALL snapshot folded into ``existing``.

    The result reads the CSV extracted into ``work_dir``; keep that directory until it is written.
    """
    csv_path = R.extract_single_csv(zip_path, work_dir / "csv")
    snapshot = R.read_ken_all(spark, csv_path, encoding)
    return R.apply_snapshot(existing, snapshot, R.POSTAL_DATA_COLUMNS, snapshot_date)


def build_local_gov_codes(
    spark: SparkSession, csv: str, encoding: str, existing: DataFrame | None, snapshot_date: date
) -> DataFrame:
    """``local_gov_codes`` table from a tidy CSV with the header ``code,prefecture,city``."""
    snapshot = spark.read.option("header", "true").option("encoding", encoding).option("mode", "FAILFAST").csv(csv)
    missing = [c for c in R.LOCAL_GOV_DATA_COLUMNS if c not in snapshot.columns]
    if missing:
        raise R.ReferenceDataError(f"{csv} lacks column(s) {missing}; expected header code,prefecture,city")
    snapshot = snapshot.select(*R.LOCAL_GOV_DATA_COLUMNS)
    if snapshot.filter(~F.col("code").rlike(r"\A[0-9]{6}\z")).limit(1).count():
        raise R.ReferenceDataError(f"{csv}: every code must be 6 ASCII digits (with the check digit)")
    return R.apply_snapshot(existing, snapshot, R.LOCAL_GOV_DATA_COLUMNS, snapshot_date)


def build_local_gov_codes_from_workbooks(
    spark: SparkSession, current: str, revisions: str
) -> tuple[DataFrame, list[str]]:
    """``local_gov_codes`` with history from the two 総務省 workbooks; returns the table and warnings."""
    codes, as_of = soumu.read_current_codes(current)
    parsed = soumu.read_revisions(revisions)
    rows = soumu.build_history(codes, parsed.events)
    table = R.frame_from_rows(spark, R.LOCAL_GOV_COLUMNS, rows, (R.VALID_FROM, R.VALID_TO))
    notes = list(parsed.skipped)
    if as_of is not None:
        notes.insert(0, f"current code list as of {as_of.isoformat()}; revisions from 2005-04-01")
    return table, notes


# --- command line -----------------------------------------------------------------------------------


def _read_existing(spark: SparkSession, args: argparse.Namespace, columns: tuple[str, ...]) -> DataFrame | None:
    if args.existing_table:
        existing = spark.table(args.existing_table)
    elif args.existing_path:
        existing = spark.read.format(args.format).load(args.existing_path)
    else:
        return None
    missing = [c for c in columns if c not in existing.columns]
    if missing:
        raise R.ReferenceDataError(f"existing reference data lacks column(s) {missing}")
    return existing.select(*columns)


def _write(df: DataFrame, args: argparse.Namespace) -> str:
    if args.output_table:
        df.write.mode("overwrite").saveAsTable(args.output_table)
        return f"table {args.output_table}"
    if args.existing_path and Path(args.output_path).resolve() == Path(args.existing_path).resolve():
        raise R.ReferenceDataError(
            "--output-path equals --existing-path: Spark cannot overwrite a location it is reading; "
            "write to a new location (or use tables)"
        )
    df.write.format(args.format).mode("overwrite").save(args.output_path)
    return f"{args.format} at {args.output_path}"


def _snapshot_date(args: argparse.Namespace) -> date:
    parsed = R.parse_as_of(args.snapshot_date)
    assert parsed is not None  # --snapshot-date is required
    return parsed


def run_postal(spark: SparkSession, args: argparse.Namespace) -> str:
    snapshot_date = _snapshot_date(args)
    existing = _read_existing(spark, args, R.POSTAL_COLUMNS)
    with tempfile.TemporaryDirectory(prefix="jpdq_postal_") as work:
        work_dir = Path(work)
        zip_path = Path(args.zip) if args.zip else download(DEFAULT_POSTAL_URL, work_dir / "utf_ken_all.zip")
        result = build_postal(spark, zip_path, work_dir, args.encoding, existing, snapshot_date)
        where = _write(result, args)  # the write materialises the table before the temp CSV is removed
    return f"postal_codes written to {where}"


def run_local_gov_codes(spark: SparkSession, args: argparse.Namespace) -> str:
    existing = _read_existing(spark, args, R.LOCAL_GOV_COLUMNS)
    result = build_local_gov_codes(spark, args.csv, args.encoding, existing, _snapshot_date(args))
    return f"local_gov_codes written to {_write(result, args)}"


def run_local_gov_codes_xlsx(spark: SparkSession, args: argparse.Namespace) -> str:
    table, notes = build_local_gov_codes_from_workbooks(spark, args.current, args.revisions)
    for note in notes:
        print(f"note: {note}", file=sys.stderr)
    return f"local_gov_codes written to {_write(table, args)}"


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--snapshot-date", required=True, help="date the source data was published/obtained (YYYY-MM-DD)"
    )
    parser.add_argument("--existing-path", help="existing reference table (Delta/Parquet location) to fold into")
    parser.add_argument("--existing-table", help="existing reference table name (Unity Catalog / metastore)")
    out = parser.add_mutually_exclusive_group(required=True)
    out.add_argument("--output-path", help="where to write the table (Delta/Parquet)")
    out.add_argument("--output-table", help="table name to write (Unity Catalog / metastore)")
    parser.add_argument(
        "--format", default="delta", choices=["delta", "parquet"], help="format for paths (default delta)"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m jpdq_oga.cli", description=(__doc__ or "").split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    postal = sub.add_parser("postal", help="postal_codes table from Japan Post utf_ken_all.zip / ken_all.zip")
    source = postal.add_mutually_exclusive_group(required=True)
    source.add_argument("--zip", help="path to utf_ken_all.zip (or ken_all.zip with --encoding cp932)")
    source.add_argument("--download", action="store_true", help=f"fetch {DEFAULT_POSTAL_URL}")
    postal.add_argument("--encoding", default="utf-8", help="CSV encoding (utf-8, or cp932 for ken_all.zip)")
    _add_common(postal)
    postal.set_defaults(run=run_postal)

    codes = sub.add_parser("local-gov-codes", help="local_gov_codes table from a tidy CSV (code,prefecture,city)")
    codes.add_argument("--csv", required=True)
    codes.add_argument("--encoding", default="utf-8")
    _add_common(codes)
    codes.set_defaults(run=run_local_gov_codes)

    xlsx = sub.add_parser(
        "local-gov-codes-xlsx",
        help="local_gov_codes table with history from the two 総務省 Excel files (needs openpyxl)",
    )
    xlsx.add_argument("--current", required=True, help="「都道府県コード及び市区町村コード」の Excel")
    xlsx.add_argument("--revisions", required=True, help="同「改正一覧表（平成17年4月1日以降）」の Excel")
    out = xlsx.add_mutually_exclusive_group(required=True)
    out.add_argument("--output-path", help="where to write the table (Delta/Parquet)")
    out.add_argument("--output-table", help="table name to write (Unity Catalog / metastore)")
    xlsx.add_argument("--format", default="delta", choices=["delta", "parquet"])
    xlsx.set_defaults(run=run_local_gov_codes_xlsx, existing_path=None, existing_table=None)
    return parser


def main(argv: list[str] | None = None, spark: SparkSession | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        session = spark or SparkSession.builder.appName("jpdq-oga-refdata").getOrCreate()
        print(args.run(session, args))
    except R.ReferenceDataError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
