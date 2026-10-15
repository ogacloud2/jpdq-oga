"""Great Expectations adapter: the pandas and Spark implementations behave the same and as documented."""

import pytest

pytest.importorskip("great_expectations")
pd = pytest.importorskip("pandas")

import great_expectations as gx  # noqa: E402

from jpdq_oga import gx_expectations as X  # noqa: E402


@pytest.fixture()
def context():
    return gx.get_context(mode="ephemeral")


def validate_pandas(context, values, expectation):
    asset = context.data_sources.add_pandas("pd").add_dataframe_asset("a")
    batch = asset.add_batch_definition_whole_dataframe("b").get_batch(
        batch_parameters={"dataframe": pd.DataFrame({"v": values})}
    )
    return batch.validate(expectation, result_format="COMPLETE")


def validate_spark(context, spark, values, expectation):
    rows = ", ".join("(NULL)" if v is None else "('" + v.replace("'", "''") + "')" for v in values)
    sdf = spark.sql(f"select cast(v as string) as v from values {rows} as t(v)")
    asset = context.data_sources.add_spark("sp").add_dataframe_asset("sa")
    batch = asset.add_batch_definition_whole_dataframe("sb").get_batch(batch_parameters={"dataframe": sdf})
    return batch.validate(expectation, result_format="COMPLETE")


CASES = [
    # (expectation class, kwargs, values, the values that must be reported as unexpected)
    (
        X.ExpectColumnValuesToBeValidJpCorporateNumber,
        {},
        ["8700110005901", "8700110005902", "870011000590", None],
        ["8700110005902", "870011000590"],
    ),
    (
        X.ExpectColumnValuesToBeValidJpInvoiceNumber,
        {},
        ["T8700110005901", "T1700110005901", "8700110005901", None],
        ["T1700110005901", "8700110005901"],
    ),
    (
        X.ExpectColumnValuesToBeValidJpLocalGovCode,
        {},
        ["162019", "162010", "480008", "16201", None],
        ["162010", "480008", "16201"],
    ),
    (
        X.ExpectColumnValuesToBeValidJpWarekiDate,
        {},
        ["令和元年5月1日", "令和元年4月30日", "令和6年2月30日", "R6.4.1", None],
        ["令和元年4月30日", "令和6年2月30日"],
    ),
    (
        X.ExpectColumnValuesToHaveNoJpHalfwidthKana,
        {},
        ["東京都", "ｱｲｳ", "ABC", None],
        ["ｱｲｳ"],
    ),
    (
        X.ExpectColumnValuesToHaveNoJpPlatformDependentChars,
        {},
        ["東京都", "山田①", "髙橋", "ｱｲｳ", None],
        ["山田①", "髙橋"],
    ),
    (
        X.ExpectColumnValuesToBeEncodableInJpCharset,
        {},
        ["東京都", "山田①", "😀", None],
        ["😀"],
    ),
    (
        X.ExpectColumnValuesToBeEncodableInJpCharset,
        {"charset": "shift_jis"},
        ["東京都", "山田①", "😀", None],
        ["山田①", "😀"],
    ),
]


@pytest.mark.parametrize(("expectation_class", "kwargs", "values", "unexpected"), CASES)
def test_pandas_engine(context, expectation_class, kwargs, values, unexpected):
    result = validate_pandas(context, values, expectation_class(column="v", **kwargs))
    assert result.success is False
    assert sorted(result.result["unexpected_list"]) == sorted(unexpected)
    # NULL is left out of the evaluation, not counted as a violation
    assert result.result["unexpected_count"] == len(unexpected)


@pytest.mark.spark
@pytest.mark.parametrize(("expectation_class", "kwargs", "values", "unexpected"), CASES)
def test_spark_engine_agrees_with_pandas(context, spark, expectation_class, kwargs, values, unexpected):
    result = validate_spark(context, spark, values, expectation_class(column="v", **kwargs))
    assert result.success is False
    assert sorted(result.result["unexpected_list"]) == sorted(unexpected)
    assert result.result["unexpected_count"] == len(unexpected)


def test_clean_data_and_mostly(context):
    clean = validate_pandas(
        context, ["8700110005901", None], X.ExpectColumnValuesToBeValidJpCorporateNumber(column="v")
    )
    assert clean.success is True

    values = ["8700110005901"] * 9 + ["1234567890123"]
    ctx2 = gx.get_context(mode="ephemeral")
    strict = validate_pandas(ctx2, values, X.ExpectColumnValuesToBeValidJpCorporateNumber(column="v"))
    assert strict.success is False
    ctx3 = gx.get_context(mode="ephemeral")
    lenient = validate_pandas(ctx3, values, X.ExpectColumnValuesToBeValidJpCorporateNumber(column="v", mostly=0.9))
    assert lenient.success is True


def test_numeric_values_are_read_as_text(context):
    asset = context.data_sources.add_pandas("pd").add_dataframe_asset("a")
    batch = asset.add_batch_definition_whole_dataframe("b").get_batch(
        batch_parameters={"dataframe": pd.DataFrame({"v": [8700110005901, 8700110005902]})}
    )
    result = batch.validate(X.ExpectColumnValuesToBeValidJpCorporateNumber(column="v"), result_format="COMPLETE")
    assert result.result["unexpected_list"] == [8700110005902]


def test_unsupported_charset_is_rejected_when_the_expectation_is_built():
    with pytest.raises(ValueError, match="unsupported charset"):
        X.ExpectColumnValuesToBeEncodableInJpCharset(column="v", charset="utf-8")


def test_all_expectations_are_listed_and_registered():
    names = {cls.__name__ for cls in X.EXPECTATIONS}
    assert names == set(X.__all__) - {"EXPECTATIONS"}
    for cls in X.EXPECTATIONS:
        assert cls.expectation_type.startswith("expect_column_values_to_")
