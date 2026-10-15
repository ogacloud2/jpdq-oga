"""The check functions against the real DQX engine (local Spark, no Databricks workspace)."""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("databricks.labs.dqx")

from conftest import string_frame  # noqa: E402

import jpdq_oga  # noqa: E402
from jpdq_oga import validators as V  # noqa: E402
from jpdq_oga.checks import (  # noqa: E402
    is_valid_jp_corporate_number,
    is_valid_jp_invoice_number,
    is_valid_jp_local_gov_code,
    is_valid_jp_wareki_date,
)

pytestmark = [pytest.mark.spark, pytest.mark.dqx]

PYTHON_UDF_MARKERS = ("PythonUDF", "BatchEvalPython", "ArrowEvalPython", "FlatMapGroupsInPandas", "MapInPandas")


@pytest.fixture()
def engine(spark):
    from databricks.labs.dqx.engine import DQEngineCore

    return DQEngineCore(workspace_client=MagicMock(), spark=spark)


def messages_by_row(df):
    return {r["i"]: [e["message"] for e in (r["_errors"] or [])] for r in df.orderBy("i").collect()}


def rule(function, column="v", **arguments):
    return {"criticality": "error", "check": {"function": function, "arguments": {"column": column, **arguments}}}


# --- behaviour ---------------------------------------------------------------------------------


def test_python_api_and_japanese_messages(spark, engine):
    from databricks.labs.dqx.rule import DQRowRule

    values = ["8700110005901", "1234567890123", "870011000590", None]
    df = string_frame(spark, values)
    out = engine.apply_checks(df, [DQRowRule(check_func=is_valid_jp_corporate_number, column="v", criticality="error")])
    msgs = messages_by_row(out)
    assert msgs[0] == []
    # CLAUDE.md example message
    assert V.corporate_number_problem("1234567890123") == V.CHECK_DIGIT
    assert msgs[1] == ["法人番号のチェックディジットが一致しません（入力値: 1234567890123）"]
    assert msgs[2] == ["法人番号は半角数字13桁で指定してください（入力値: 870011000590）"]
    assert msgs[3] == []  # NULL passes


def test_english_messages_via_yaml_style_metadata(spark, engine):
    df = string_frame(spark, ["8700110005901", "1234567890123"])
    out = engine.apply_checks_by_metadata(
        df, [rule("is_valid_jp_corporate_number", lang="en")], custom_check_functions=jpdq_oga.get_japan_checks()
    )
    assert messages_by_row(out) == {0: [], 1: ["Corporate number check digit does not match (value: 1234567890123)"]}


def test_long_input_values_are_cut_in_messages(spark, engine):
    from jpdq_oga.checks import MAX_SHOWN_VALUE_LENGTH

    just_fits = "9" * MAX_SHOWN_VALUE_LENGTH
    too_long = "9" * 10_000
    df = string_frame(spark, [just_fits, too_long])
    out = engine.apply_checks_by_metadata(
        df, [rule("is_valid_jp_corporate_number")], custom_check_functions=jpdq_oga.get_japan_checks()
    )
    msgs = messages_by_row(out)
    assert msgs[0] == [f"法人番号は半角数字13桁で指定してください（入力値: {just_fits}）"]
    assert msgs[1] == [f"法人番号は半角数字13桁で指定してください（入力値: {'9' * MAX_SHOWN_VALUE_LENGTH}…）"]


def test_invoice_and_local_gov_code(spark, engine):
    inv = string_frame(spark, ["T8700110005901", "T1700110005901", "8700110005901"])
    out = engine.apply_checks_by_metadata(
        inv, [rule("is_valid_jp_invoice_number")], custom_check_functions=jpdq_oga.get_japan_checks()
    )
    msgs = messages_by_row(out)
    assert msgs[0] == []
    assert "チェックディジット" in msgs[1][0] and "T1700110005901" in msgs[1][0]
    assert "「T」と半角数字13桁" in msgs[2][0]

    lg = string_frame(spark, ["162019", "162010", "480008", "16201"])
    out = engine.apply_checks_by_metadata(
        lg, [rule("is_valid_jp_local_gov_code")], custom_check_functions=jpdq_oga.get_japan_checks()
    )
    msgs = messages_by_row(out)
    assert msgs[0] == []
    assert "検査数字" in msgs[1][0]
    assert "都道府県コード（01〜47）" in msgs[2][0]
    assert "6桁" in msgs[3][0]


def test_wareki_messages(spark, engine):
    values = [
        "令和6年4月1日",  # fine
        "平成31年5月1日",  # era boundary
        "令和元年4月30日",  # era boundary (spec example)
        "令和6年2月30日",  # does not exist
        "明治5年12月31日",  # old calendar
        "2024-04-01",  # not an era date
        None,
    ]
    out = engine.apply_checks_by_metadata(
        string_frame(spark, values),
        [rule("is_valid_jp_wareki_date")],
        custom_check_functions=jpdq_oga.get_japan_checks(),
    )
    msgs = messages_by_row(out)
    assert msgs[0] == [] and msgs[6] == []
    assert "元号の期間外" in msgs[1][0] and "平成31年5月1日" in msgs[1][0]
    assert "元号の期間外" in msgs[2][0]
    assert msgs[3] == ["存在しない日付です（入力値: 令和6年2月30日）"]
    assert "旧暦" in msgs[4][0]
    assert "和暦の日付として読み取れません" in msgs[5][0]

    en = engine.apply_checks_by_metadata(
        string_frame(spark, ["令和6年2月30日"]),
        [rule("is_valid_jp_wareki_date", lang="en")],
        custom_check_functions=jpdq_oga.get_japan_checks(),
    )
    assert messages_by_row(en) == {0: ["The date does not exist (value: 令和6年2月30日)"]}


def test_unsupported_language_is_rejected_when_the_rule_is_built():
    with pytest.raises(ValueError, match="unsupported language"):
        is_valid_jp_corporate_number("v", lang="fr")


def test_get_japan_checks_names_match_the_registry():
    from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY

    checks = jpdq_oga.get_japan_checks()
    assert set(checks) == {
        "is_valid_jp_corporate_number",
        "is_valid_jp_invoice_number",
        "is_valid_jp_local_gov_code",
        "is_valid_jp_wareki_date",
        "is_existing_jp_postal_code",
        "is_consistent_jp_postal_code_and_prefecture",
        "is_consistent_jp_postal_code_and_city",
        "is_existing_jp_local_gov_code",
    }
    dataset_level = {
        "is_existing_jp_postal_code",
        "is_consistent_jp_postal_code_and_prefecture",
        "is_consistent_jp_postal_code_and_city",
        "is_existing_jp_local_gov_code",
    }
    for name in checks:
        assert CHECK_FUNC_REGISTRY[name] == ("dataset" if name in dataset_level else "row")


def test_example_yaml_is_valid_and_runs(spark, engine):
    from pathlib import Path

    import yaml

    checks = yaml.safe_load((Path(__file__).parent.parent / "examples" / "dqx_checks.yaml").read_text(encoding="utf-8"))
    # the row-level checks; the reference checks are in examples/dqx_reference_checks.yaml
    row_level = {n for n in jpdq_oga.get_japan_checks() if "existing" not in n and "consistent" not in n}
    assert {c["check"]["function"] for c in checks} == row_level
    df = string_frame(spark, ["x"], "corporate_number")
    for column in ("invoice_registration_number", "municipality_code", "contract_date"):
        df = df.join(string_frame(spark, ["x"], column), "i")
    out = engine.apply_checks_by_metadata(df, checks, custom_check_functions=jpdq_oga.get_japan_checks())
    assert len(messages_by_row(out)[0]) == 4


# --- Spark-native guarantee ----------------------------------------------------------------------


def executed_plan(df) -> str:
    return df._jdf.queryExecution().executedPlan().toString()


def test_detector_notices_a_python_udf(spark):
    """Negative control: the plan check must be able to fail."""
    from pyspark.sql import functions as F

    udf = F.udf(lambda s: s, "string")
    plan = executed_plan(string_frame(spark, ["a"]).select(udf("v")))
    assert any(marker in plan for marker in PYTHON_UDF_MARKERS)


@pytest.mark.parametrize(
    "check",
    [is_valid_jp_corporate_number, is_valid_jp_invoice_number, is_valid_jp_local_gov_code, is_valid_jp_wareki_date],
)
@pytest.mark.parametrize("lang", ["ja", "en"])
def test_checks_run_without_python_udfs(spark, engine, check, lang):
    from databricks.labs.dqx.rule import DQRowRule

    df = string_frame(spark, ["8700110005901", "x", None])
    rule_ = DQRowRule(check_func=check, column="v", criticality="error", check_func_kwargs={"lang": lang})
    plan = executed_plan(engine.apply_checks(df, [rule_]))
    for marker in PYTHON_UDF_MARKERS:
        assert marker not in plan, f"{check.__name__} plan contains {marker}"
