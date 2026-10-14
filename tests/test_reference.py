"""Reference-data checks (Phase B): history building, ``as_of`` semantics and the DQX dataset-level rules."""

from datetime import date
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("databricks.labs.dqx")

from conftest import rows_frame  # noqa: E402

import jpdq_oga  # noqa: E402
from jpdq_oga import reference as R  # noqa: E402

pytestmark = [pytest.mark.spark, pytest.mark.dqx]

POSTAL_COLS = ["postal_code", "local_gov_code", "prefecture", "city", "valid_from", "valid_to"]
POSTAL_ROWS = [
    ("1000001", "13101", "東京都", "千代田区", None, None),
    ("0600000", "01101", "北海道", "札幌市中央区", None, None),
    ("0040000", "01108", "北海道", "札幌市厚別区", None, None),  # one postal code, two wards
    ("0040000", "01110", "北海道", "札幌市清田区", None, None),
    ("9999991", "13101", "東京都", "千代田区", "2025-04-01", None),  # introduced 2025-04-01
    ("9999992", "13101", "東京都", "千代田区", None, "2024-04-01"),  # retired on 2024-04-01
]
CODE_COLS = ["code", "prefecture", "city", "valid_from", "valid_to"]
CODE_ROWS = [
    ("011002", "北海道", "札幌市", None, None),
    ("012343", "北海道", "旧町", None, "2010-04-01"),  # retired by a merger on 2010-04-01
]


@pytest.fixture()
def engine(spark):
    from databricks.labs.dqx.engine import DQEngineCore

    return DQEngineCore(workspace_client=MagicMock(), spark=spark)


@pytest.fixture()
def postal_ref(spark):
    return rows_frame(spark, POSTAL_COLS, POSTAL_ROWS, ("valid_from", "valid_to"))


@pytest.fixture()
def code_ref(spark):
    return rows_frame(spark, CODE_COLS, CODE_ROWS, ("valid_from", "valid_to"))


def run(engine, df, function, ref, ref_name, **arguments):
    checks = [
        {"criticality": "error", "check": {"function": function, "arguments": {"ref_df_name": ref_name, **arguments}}}
    ]
    return engine.apply_checks_by_metadata(
        df, checks, custom_check_functions=jpdq_oga.get_japan_checks(), ref_dfs={ref_name: ref}
    )


def messages(df):
    rows = df.orderBy("i").collect()
    return {int(r["i"]): [e["message"] for e in (r["_errors"] or [])] for r in rows}


def inputs(spark, columns, rows):
    return rows_frame(spark, ["i", *columns], [(str(n), *row) for n, row in enumerate(rows)])


# --- 郵便番号の実在性 ------------------------------------------------------------------------------------


def test_postal_existence_and_notation(spark, engine, postal_ref):
    df = inputs(
        spark, ["postal"], [("1000001",), ("100-0001",), ("1000002",), ("100-000",), ("１０００００１",), (None,)]
    )
    out = messages(
        run(engine, df, "is_existing_jp_postal_code", postal_ref, "postal", column="postal", as_of="2026-01-01")
    )
    assert out[0] == [] and out[1] == []  # with and without hyphen
    assert out[2] == ["郵便番号が日本郵便のデータに存在しません（基準日: 2026-01-01）（入力値: 1000002）"]
    assert "7桁の半角数字" in out[3][0]
    assert "7桁の半角数字" in out[4][0]  # fullwidth digits are reported, not converted
    assert out[5] == []  # NULL passes


@pytest.mark.parametrize(
    "as_of,code,expected_missing",
    [
        ("2025-03-31", "9999991", True),  # not yet introduced
        ("2025-04-01", "9999991", False),  # valid_from is inclusive
        ("2024-03-31", "9999992", False),  # still in force the day before it was retired
        ("2024-04-01", "9999992", True),  # valid_to is exclusive
        ("1999-01-01", "1000001", False),  # NULL valid_from: in force since before the first snapshot
    ],
)
def test_postal_as_of_boundaries(spark, engine, postal_ref, as_of, code, expected_missing):
    out = messages(
        run(
            engine,
            inputs(spark, ["p"], [(code,)]),
            "is_existing_jp_postal_code",
            postal_ref,
            "postal",
            column="p",
            as_of=as_of,
        )
    )
    assert bool(out[0]) is expected_missing


def test_postal_default_is_the_run_date(spark, engine, postal_ref):
    out = messages(
        run(
            engine,
            inputs(spark, ["p"], [("9999991",), ("9999992",), ("1000001",)]),
            "is_existing_jp_postal_code",
            postal_ref,
            "postal",
            column="p",
        )
    )
    assert out == {0: [], 1: [out[1][0]], 2: []}
    assert "存在しません" in out[1][0] and "基準日" not in out[1][0]


def test_postal_as_of_from_a_column(spark, engine, postal_ref):
    df = inputs(
        spark,
        ["p", "d"],
        [("9999991", "2025-03-31"), ("9999991", "2025-04-01"), ("9999992", "2024-03-31"), ("9999992", "2024-04-01")],
    )
    out = messages(run(engine, df, "is_existing_jp_postal_code", postal_ref, "postal", column="p", as_of_column="d"))
    assert [bool(out[i]) for i in range(4)] == [True, False, False, True]


def test_english_messages(spark, engine, postal_ref):
    out = messages(
        run(
            engine,
            inputs(spark, ["p"], [("1000002",)]),
            "is_existing_jp_postal_code",
            postal_ref,
            "postal",
            column="p",
            as_of="2026-01-01",
            lang="en",
        )
    )
    assert out[0] == ["Postal code does not exist in the Japan Post data (as of 2026-01-01) (value: 1000002)"]


# --- 整合 --------------------------------------------------------------------------------------------------


def test_postal_prefecture_consistency(spark, engine, postal_ref):
    df = inputs(
        spark,
        ["p", "pref"],
        [
            ("1000001", "東京都"),
            ("1000001", "大阪府"),
            ("100-0001", "東京"),  # notation difference is a mismatch
            ("0040000", "北海道"),
            ("1000002", "大阪府"),  # postal code does not exist: not this check's finding
            ("bad", "東京都"),
            ("1000001", None),
            (None, "東京都"),
        ],
    )
    out = messages(
        run(
            engine,
            df,
            "is_consistent_jp_postal_code_and_prefecture",
            postal_ref,
            "postal",
            column="p",
            prefecture_column="pref",
            as_of="2026-01-01",
        )
    )
    assert out[0] == []
    assert out[1] == ["郵便番号と都道府県が一致しません（基準日: 2026-01-01）（郵便番号: 1000001、都道府県: 大阪府）"]
    assert len(out[2]) == 1
    assert [out[i] for i in (3, 4, 5, 6, 7)] == [[]] * 5


def test_postal_city_consistency_with_a_postal_code_spanning_several_wards(spark, engine, postal_ref):
    df = inputs(
        spark,
        ["p", "city"],
        [
            ("0040000", "札幌市厚別区"),
            ("0040000", "札幌市清田区"),
            ("0040000", "札幌市中央区"),
            ("0600000", "札幌市中央区"),
        ],
    )
    out = messages(
        run(engine, df, "is_consistent_jp_postal_code_and_city", postal_ref, "postal", column="p", city_column="city")
    )
    assert out[0] == [] and out[1] == [] and out[3] == []
    assert len(out[2]) == 1 and "札幌市中央区" in out[2][0]


def test_joining_never_changes_the_number_of_rows(spark, engine, postal_ref):
    rows = [("0040000",)] * 5 + [("1000001",)] * 3 + [("nope",)]
    out = run(engine, inputs(spark, ["p"], rows), "is_existing_jp_postal_code", postal_ref, "postal", column="p")
    assert out.count() == len(rows)
    assert not [c for c in out.columns if c.startswith("__jpdq")]


# --- 地方公共団体コードの実在性 --------------------------------------------------------------------------------


def test_local_gov_code_existence_detects_retired_codes(spark, engine, code_ref):
    df = inputs(spark, ["c"], [("011002",), ("012343",), ("999999",), ("01100",), (None,)])
    now = messages(run(engine, df, "is_existing_jp_local_gov_code", code_ref, "codes", column="c"))
    assert now[0] == [] and now[4] == []
    assert "合併などで廃止された" in now[1][0] and "012343" in now[1][0]
    assert "存在しません" in now[2][0]
    assert "6桁" in now[3][0]

    before_merger = messages(
        run(engine, df, "is_existing_jp_local_gov_code", code_ref, "codes", column="c", as_of="2010-03-31")
    )
    assert before_merger[1] == []
    on_merger_day = messages(
        run(engine, df, "is_existing_jp_local_gov_code", code_ref, "codes", column="c", as_of="2010-04-01")
    )
    assert on_merger_day[1] != []


# --- 引数・参照データの検証 ----------------------------------------------------------------------------------


def test_reference_source_must_be_exactly_one():
    from jpdq_oga.checks import is_existing_jp_postal_code

    with pytest.raises(R.ReferenceDataError, match="exactly one"):
        is_existing_jp_postal_code("p")
    with pytest.raises(R.ReferenceDataError, match="exactly one"):
        is_existing_jp_postal_code("p", ref_df_name="a", ref_table="b")
    with pytest.raises(R.ReferenceDataError, match="ISO date"):
        is_existing_jp_postal_code("p", ref_df_name="a", as_of="2026/01/01")
    with pytest.raises(R.ReferenceDataError, match="not a real date"):
        is_existing_jp_postal_code("p", ref_df_name="a", as_of="2026-02-30")
    with pytest.raises(ValueError, match="unsupported language"):
        is_existing_jp_postal_code("p", ref_df_name="a", lang="fr")


def test_missing_reference_frame_and_columns(spark, engine, postal_ref):
    df = inputs(spark, ["p"], [("1000001",)])
    with pytest.raises(R.ReferenceDataError, match="was not provided"):
        run(engine, df, "is_existing_jp_postal_code", postal_ref, "other", column="p", ref_df_name="missing")
    with pytest.raises(R.ReferenceDataError, match="lacks column"):
        run(engine, df, "is_existing_jp_postal_code", postal_ref.drop("valid_to"), "postal", column="p")


# --- サンプル YAML ------------------------------------------------------------------------------------------


def test_example_reference_yaml_passes_dqx_validation():
    from pathlib import Path

    import yaml
    from databricks.labs.dqx.engine import DQEngineCore

    path = Path(__file__).parent.parent / "examples" / "dqx_reference_checks.yaml"
    checks = yaml.safe_load(path.read_text(encoding="utf-8"))
    status = DQEngineCore.validate_checks(checks, custom_check_functions=jpdq_oga.get_japan_checks())
    assert not status.has_errors, status.to_string()
    assert {c["check"]["function"] for c in checks} == {
        "is_existing_jp_postal_code",
        "is_consistent_jp_postal_code_and_prefecture",
        "is_consistent_jp_postal_code_and_city",
        "is_existing_jp_local_gov_code",
    }


# --- Spark ネイティブ・broadcast ----------------------------------------------------------------------------


def test_plan_has_a_broadcast_join_and_no_python_udf(spark, engine, postal_ref):
    out = run(
        engine, inputs(spark, ["p", "pref"], [("1000001", "東京都")]), "is_consistent_jp_postal_code_and_prefecture",
        postal_ref, "postal", column="p", prefecture_column="pref",
    )  # fmt: skip
    plan = out._jdf.queryExecution().executedPlan().toString()
    assert "BroadcastHashJoin" in plan
    for marker in ("PythonUDF", "BatchEvalPython", "ArrowEvalPython", "MapInPandas"):
        assert marker not in plan


# --- 履歴の構築 -----------------------------------------------------------------------------------------------

DATA = ("postal_code", "local_gov_code", "prefecture", "city")


def snapshot(spark, rows):
    return rows_frame(spark, DATA, rows)


def table(df):
    return {
        (r["postal_code"], r["city"], r["valid_from"], r["valid_to"])
        for r in df.select("postal_code", "city", "valid_from", "valid_to").collect()
    }


A = ("1000001", "13101", "東京都", "千代田区")
B = ("1000002", "13101", "東京都", "千代田区")
C = ("1000003", "13101", "東京都", "千代田区")


def test_first_snapshot_has_unknown_start(spark):
    first = R.apply_snapshot(None, snapshot(spark, [A, B]), DATA, date(2026, 4, 1))
    assert table(first) == {("1000001", "千代田区", None, None), ("1000002", "千代田区", None, None)}


def test_later_snapshots_open_and_close_facts(spark):
    first = R.apply_snapshot(None, snapshot(spark, [A, B]), DATA, date(2026, 4, 1))
    second = R.apply_snapshot(first, snapshot(spark, [A, C]), DATA, date(2026, 5, 1))
    assert table(second) == {
        ("1000001", "千代田区", None, None),
        ("1000002", "千代田区", None, date(2026, 5, 1)),  # B disappeared
        ("1000003", "千代田区", date(2026, 5, 1), None),  # C appeared
    }
    # re-applying the same snapshot changes nothing
    assert table(R.apply_snapshot(second, snapshot(spark, [A, C]), DATA, date(2026, 5, 1))) == table(second)


def test_a_fact_that_comes_back_gets_a_new_row(spark):
    s1 = R.apply_snapshot(None, snapshot(spark, [A, B]), DATA, date(2026, 4, 1))
    s2 = R.apply_snapshot(s1, snapshot(spark, [A]), DATA, date(2026, 5, 1))
    s3 = R.apply_snapshot(s2, snapshot(spark, [A, B]), DATA, date(2026, 6, 1))
    assert table(s3) == {
        ("1000001", "千代田区", None, None),
        ("1000002", "千代田区", None, date(2026, 5, 1)),
        ("1000002", "千代田区", date(2026, 6, 1), None),
    }


def test_snapshots_must_be_applied_in_order(spark):
    s1 = R.apply_snapshot(None, snapshot(spark, [A]), DATA, date(2026, 5, 1))
    s2 = R.apply_snapshot(s1, snapshot(spark, [A, B]), DATA, date(2026, 6, 1))
    with pytest.raises(R.ReferenceDataError, match="chronological"):
        R.apply_snapshot(s2, snapshot(spark, [A]), DATA, date(2026, 5, 15))


def test_history_makes_as_of_checks_possible(spark, engine):
    """Snapshots in, as_of checks out: the retired postal code is found before its retirement date."""
    s1 = R.apply_snapshot(None, snapshot(spark, [A, B]), DATA, date(2026, 4, 1))
    s2 = R.apply_snapshot(s1, snapshot(spark, [A]), DATA, date(2026, 5, 1))
    df = inputs(spark, ["p"], [("1000002",)])
    before = messages(run(engine, df, "is_existing_jp_postal_code", s2, "postal", column="p", as_of="2026-04-30"))
    after = messages(run(engine, df, "is_existing_jp_postal_code", s2, "postal", column="p", as_of="2026-05-01"))
    assert before[0] == [] and after[0] != []
