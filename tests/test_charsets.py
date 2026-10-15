"""Character checks: the Spark expressions agree with the codec tables for every character."""

import pytest

from jpdq_oga import charsets as C

# --- pure Python ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "kana", "platform", "cp932", "shift_jis"),
    [
        ("東京都千代田区", None, None, None, None),
        ("ｱｲｳ", C.HALFWIDTH_KANA, None, None, None),
        ("①②", None, C.PLATFORM_DEPENDENT, None, C.UNENCODABLE),
        ("㈱ジェーピー", None, C.PLATFORM_DEPENDENT, None, C.UNENCODABLE),
        ("髙橋", None, C.PLATFORM_DEPENDENT, None, C.UNENCODABLE),
        ("﨑山", None, C.PLATFORM_DEPENDENT, None, C.UNENCODABLE),
        ("😀", None, None, C.UNENCODABLE, C.UNENCODABLE),
        ("𠮷野家", None, None, C.UNENCODABLE, C.UNENCODABLE),
        ("", None, None, None, None),
        (None, None, None, None, None),
    ],
)
def test_reference_values(value, kana, platform, cp932, shift_jis):
    assert C.halfwidth_kana_problem(value) == kana
    assert C.platform_dependent_problem(value) == platform
    assert C.unencodable_problem(value, "cp932") == cp932
    assert C.unencodable_problem(value, "shift_jis") == shift_jis


def test_tables_have_the_expected_shape():
    def points(ranges):
        return {cp for lo, hi in ranges for cp in range(lo, hi + 1)}

    cp932 = points(C.encodable_ranges("cp932"))
    strict = points(C.encodable_ranges("shift_jis"))
    assert len(cp932) > len(strict) > 7000  # cp932 = JIS X 0208 + NEC / IBM extensions
    assert points(C.platform_dependent_ranges()) == cp932 - strict
    # the opposite direction is just the yen sign and the overline: shift_jis maps them to 0x5C / 0x7E, cp932 does not
    assert strict - cp932 == {0x00A5, 0x203E}
    assert C.HALFWIDTH_KANA_RANGE == (0xFF61, 0xFF9F)


def test_unsupported_charset_is_rejected():
    with pytest.raises(ValueError, match="unsupported charset"):
        C.unencodable_problem("a", "utf-8")
    with pytest.raises(ValueError, match="unsupported charset"):
        C.encodable_ranges("euc-jp")


# --- Spark: every character of the Basic Multilingual Plane, plus some outside it ----------------------


@pytest.fixture(scope="module")
def all_characters(spark):
    """One row per BMP character (surrogates left out) as a string column ``v``, built by the JVM only."""
    from pyspark.sql import functions as F

    bmp = (
        spark.range(0x10000)
        .filter(~F.col("id").between(0xD800, 0xDFFF))
        .select(F.expr("decode(unhex(lpad(hex(id), 4, '0')), 'UTF-16BE')").alias("v"), F.col("id").alias("cp"))
    )
    outside = spark.sql(
        "select decode(unhex(h), 'UTF-16BE') as v, cp from values "
        "('D842DFB7', 134583), ('D83DDE00', 128512), ('DBFFDFFF', 1114111) as t(h, cp)"
    )
    return bmp.unionByName(outside).cache()


def _problems(df, charset):
    from jpdq_oga import expressions as E

    return df.select(
        "cp",
        E.halfwidth_kana_problem("v").alias("kana"),
        E.platform_dependent_problem("v").alias("platform"),
        E.unencodable_problem("v", charset).alias("unencodable"),
    ).collect()


@pytest.mark.spark
@pytest.mark.parametrize("charset", ["cp932", "shift_jis"])
def test_spark_expressions_agree_with_the_codec_tables_for_every_character(all_characters, charset):
    mismatches = []
    for row in _problems(all_characters, charset):
        ch = chr(row["cp"])
        expected = (
            C.halfwidth_kana_problem(ch),
            C.platform_dependent_problem(ch),
            C.unencodable_problem(ch, charset),
        )
        if (row["kana"], row["platform"], row["unencodable"]) != expected:
            mismatches.append((hex(row["cp"]), (row["kana"], row["platform"], row["unencodable"]), expected))
    assert mismatches[:10] == []


@pytest.mark.spark
def test_spark_handles_whole_strings_nulls_and_empty(spark):
    from conftest import string_frame

    from jpdq_oga import expressions as E

    df = string_frame(spark, ["東京都", "ｱｲｳ", "山田①", "😀笑", "", None, "ABC 123 　ー"])
    out = {
        r["i"]: (r["kana"], r["platform"], r["unencodable"])
        for r in df.select(
            "i",
            E.halfwidth_kana_problem("v").alias("kana"),
            E.platform_dependent_problem("v").alias("platform"),
            E.unencodable_problem("v").alias("unencodable"),
        ).collect()
    }
    assert out[0] == (None, None, None)
    assert out[1] == (C.HALFWIDTH_KANA, None, None)
    assert out[2] == (None, C.PLATFORM_DEPENDENT, None)
    assert out[3] == (None, None, C.UNENCODABLE)
    assert out[4] == (None, None, None)  # empty string
    assert out[5] == (None, None, None)  # NULL passes
    assert out[6] == (None, None, None)
