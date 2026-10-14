"""The reference-data builder, on a synthetic KEN_ALL file and (optionally) the real Japan Post file.

The real file is not stored in the repository: set ``JPDQ_KEN_ALL_ZIP`` to a downloaded
``utf_ken_all.zip`` to run ``test_real_japan_post_file``.

Writing tables with Spark on Windows needs winutils (HADOOP_HOME); those end-to-end tests are skipped
there when it is missing.  Everything up to the write is covered on every platform.
"""

import os
import sys
import zipfile
from datetime import date
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

import jpdq_oga  # noqa: E402
from jpdq_oga import cli  # noqa: E402
from jpdq_oga import reference as R  # noqa: E402

pytestmark = pytest.mark.spark

can_write = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
    reason="Spark file writes on Windows need winutils (HADOOP_HOME)",
)


def ken_all_row(code, postal, pref, city, town="町域"):
    kana = ["ﾄｳｷｮｳﾄ", "ﾁﾖﾀﾞｸ", "ﾁﾖﾀﾞ"]
    return [code, postal[:5], postal, *kana, pref, city, town, "0", "0", "0", "0", "0", "0"]


def write_zip(path, rows, encoding="utf-8", name="utf_ken_all.csv"):
    text = "\r\n".join(",".join(f'"{c}"' for c in row) for row in rows) + "\r\n"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(name, text.encode(encoding))
    return path


ROWS_V1 = [
    ken_all_row("13101", "1000001", "東京都", "千代田区", "千代田"),
    ken_all_row("13101", "1000001", "東京都", "千代田区", "千代田（別の町域）"),  # duplicate fact, other town
    ken_all_row("01108", "0040000", "北海道", "札幌市厚別区", "以下に掲載がない場合"),
    ken_all_row("01110", "0040000", "北海道", "札幌市清田区", "以下に掲載がない場合"),
    ken_all_row("13101", "1000002", "東京都", "千代田区", "皇居外苑"),
]
ROWS_V2 = [r for r in ROWS_V1 if r[2] != "1000002"] + [
    ken_all_row("13101", "1000003", "東京都", "千代田区", "北の丸公園")
]


def facts(df):
    return {
        (r["postal_code"], r["city"], r["valid_from"], r["valid_to"])
        for r in df.select("postal_code", "city", "valid_from", "valid_to").collect()
    }


@pytest.mark.parametrize("encoding", ["utf-8", "cp932"])
def test_postal_build_and_fold(spark, tmp_path, encoding):
    v1 = write_zip(tmp_path / "v1.zip", ROWS_V1, encoding)
    v2 = write_zip(tmp_path / "v2.zip", ROWS_V2, encoding)

    first = cli.build_postal(spark, v1, tmp_path / "w1", encoding, None, date(2026, 4, 1))
    assert sorted(first.columns) == sorted(R.POSTAL_COLUMNS)
    assert facts(first) == {
        ("1000001", "千代田区", None, None),
        ("0040000", "札幌市厚別区", None, None),
        ("0040000", "札幌市清田区", None, None),
        ("1000002", "千代田区", None, None),
    }

    second = cli.build_postal(spark, v2, tmp_path / "w2", encoding, first.cache(), date(2026, 5, 1))
    assert facts(second) == {
        ("1000001", "千代田区", None, None),
        ("0040000", "札幌市厚別区", None, None),
        ("0040000", "札幌市清田区", None, None),
        ("1000002", "千代田区", None, date(2026, 5, 1)),
        ("1000003", "千代田区", date(2026, 5, 1), None),
    }


def test_not_a_ken_all_file_is_rejected(spark, tmp_path):
    bad = write_zip(tmp_path / "bad.zip", [["1", "2", "3"] + ["x"] * 12])
    with pytest.raises(R.ReferenceDataError, match="not a KEN_ALL file"):
        cli.build_postal(spark, bad, tmp_path / "w", "utf-8", None, date(2026, 4, 1))


def test_zip_with_two_csv_files_is_rejected(tmp_path):
    path = tmp_path / "two.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("a.csv", "x")
        archive.writestr("b.csv", "y")
    with pytest.raises(R.ReferenceDataError, match="exactly one CSV"):
        R.extract_single_csv(path, tmp_path / "out")


def test_download_failure_points_to_the_manual_route(tmp_path):
    with pytest.raises(R.ReferenceDataError, match="Download the file in a browser"):
        cli.download((tmp_path / "does-not-exist.zip").as_uri(), tmp_path / "x.zip")


def test_download_from_a_file_url(tmp_path):
    src = write_zip(tmp_path / "src.zip", ROWS_V1)
    assert cli.download(src.as_uri(), tmp_path / "copy.zip").read_bytes() == src.read_bytes()


def test_local_gov_codes_from_a_tidy_csv(spark, tmp_path):
    csv = tmp_path / "codes.csv"
    csv.write_text("code,prefecture,city\n011002,北海道,札幌市\n130001,東京都,\n", encoding="utf-8")
    result = cli.build_local_gov_codes(spark, str(csv), "utf-8", None, date(2026, 4, 1))
    assert {(r["code"], r["prefecture"]) for r in result.collect()} == {("011002", "北海道"), ("130001", "東京都")}


def test_local_gov_codes_csv_is_validated(spark, tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text("code,prefecture,city\n1100,北海道,札幌市\n", encoding="utf-8")
    with pytest.raises(R.ReferenceDataError, match="6 ASCII digits"):
        cli.build_local_gov_codes(spark, str(csv), "utf-8", None, date(2026, 4, 1))
    csv.write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(R.ReferenceDataError, match="lacks column"):
        cli.build_local_gov_codes(spark, str(csv), "utf-8", None, date(2026, 4, 1))


def test_parser_requires_a_source_and_a_destination():
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["postal", "--snapshot-date", "2026-04-01", "--output-path", "x"])
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["postal", "--zip", "a.zip", "--snapshot-date", "2026-04-01"])


def test_bad_snapshot_date_is_a_clean_error(spark, tmp_path, capsys):
    zip_path = write_zip(tmp_path / "v1.zip", ROWS_V1)
    code = cli.main(
        ["postal", "--zip", str(zip_path), "--snapshot-date", "yesterday", "--output-path", str(tmp_path / "o")],
        spark=spark,
    )
    assert code == 2 and "ISO date" in capsys.readouterr().err


@can_write
def test_end_to_end_with_files(spark, tmp_path, capsys):
    v1, v2 = write_zip(tmp_path / "v1.zip", ROWS_V1), write_zip(tmp_path / "v2.zip", ROWS_V2)
    out1, out2 = str(tmp_path / "postal_v1"), str(tmp_path / "postal_v2")
    args1 = ["postal", "--zip", str(v1), "--snapshot-date", "2026-04-01", "--output-path", out1, "--format", "parquet"]
    assert cli.main(args1, spark=spark) == 0
    args2 = ["postal", "--zip", str(v2), "--snapshot-date", "2026-05-01", "--existing-path", out1,
             "--output-path", out2, "--format", "parquet"]  # fmt: skip
    assert cli.main(args2, spark=spark) == 0
    assert ("1000002", "千代田区", None, date(2026, 5, 1)) in facts(spark.read.parquet(out2))
    # the table it reads cannot be overwritten in place
    same = [*args2[:-4], "--existing-path", out2, "--output-path", out2, "--format", "parquet"]
    assert cli.main(same, spark=spark) == 2 and "new location" in capsys.readouterr().err


# --- the real file ------------------------------------------------------------------------------------


@pytest.mark.skipif("JPDQ_KEN_ALL_ZIP" not in os.environ, reason="set JPDQ_KEN_ALL_ZIP to a downloaded utf_ken_all.zip")
def test_real_japan_post_file(spark, tmp_path):
    from conftest import rows_frame
    from databricks.labs.dqx.engine import DQEngineCore

    ref = cli.build_postal(spark, os.environ["JPDQ_KEN_ALL_ZIP"], tmp_path, "utf-8", None, date(2026, 10, 15)).cache()
    assert ref.count() > 100_000
    assert ref.select("prefecture").distinct().count() == 47

    engine = DQEngineCore(workspace_client=MagicMock(), spark=spark)
    rows = [
        ("0", "1000001", "東京都", "千代田区"),  # 千代田区千代田 (皇居)
        ("1", "100-0001", "東京都", "千代田区"),
        ("2", "1000001", "大阪府", "千代田区"),  # wrong prefecture
        ("3", "0600000", "北海道", "札幌市中央区"),  # first row of the file
        ("4", "0000000", "東京都", "千代田区"),  # no such postal code
        ("5", "0040000", "北海道", "札幌市清田区"),  # one postal code spanning several wards
    ]
    df = rows_frame(spark, ["i", "postal", "pref", "city"], rows)

    def flagged(function, **args):
        check = {"function": function, "arguments": {"column": "postal", "ref_df_name": "p", **args}}
        out = engine.apply_checks_by_metadata(
            df, [{"criticality": "error", "check": check}],
            custom_check_functions=jpdq_oga.get_japan_checks(), ref_dfs={"p": ref},
        )  # fmt: skip
        assert out.count() == len(rows)
        return {int(r["i"]): bool(r["_errors"]) for r in out.collect()}

    assert flagged("is_existing_jp_postal_code") == {0: False, 1: False, 2: False, 3: False, 4: True, 5: False}
    assert flagged("is_consistent_jp_postal_code_and_prefecture", prefecture_column="pref") == {
        0: False, 1: False, 2: True, 3: False, 4: False, 5: False,
    }  # fmt: skip
    assert flagged("is_consistent_jp_postal_code_and_city", city_column="city") == {
        0: False, 1: False, 2: False, 3: False, 4: False, 5: False,
    }  # fmt: skip
