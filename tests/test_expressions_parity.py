"""The Spark expressions must agree with the pure-Python reference on every input."""

import pytest

pytest.importorskip("pyspark")

from conftest import string_frame  # noqa: E402

from jpdq_oga import expressions as E  # noqa: E402
from jpdq_oga import validators as V  # noqa: E402
from jpdq_oga import wareki as W  # noqa: E402

pytestmark = pytest.mark.spark

COMMON = [None, "", " ", "　", "abc", "０", "\n", "\t", "T", "-"]

CORPORATE = COMMON + [
    "8700110005901", "7000012050002", "1700110005901", "0000000000000", "9999999999999",
    "870011000590", "87001100059011", "８７００１１０００５９０１", " 8700110005901", "8700110005901\n",
    "8700110005901 ", "8700-1100-05901", "T8700110005901", "87001100059O1",
]  # fmt: skip

INVOICE = COMMON + [
    "T8700110005901", "T7000012050002", "T1700110005901", "8700110005901", "t8700110005901",
    "Ｔ8700110005901", "T-8700110005901", "T870011000590", "TT8700110005901", " T8700110005901",
    "T8700110005901\n", "T８７００１１０００５９０１",
]  # fmt: skip

LOCAL_GOV = COMMON + [
    "162019", "162010", "010006", "130001", "130002", "470007", "480000", "000000", "990000",
    "16201", "1620190", "１６２０１９", "16201A", "162019\n", " 162019", "00031" + "1", "000401",
]  # fmt: skip

WAREKI = COMMON + [
    # era first day / the day before
    "明治6年1月1日", "明治5年12月31日", "明治元年1月1日", "明治45年7月29日", "明治45年7月30日",
    "大正元年7月30日", "大正元年7月29日", "大正15年12月24日", "大正15年12月25日",
    "昭和元年12月25日", "昭和元年12月24日", "昭和64年1月7日", "昭和64年1月8日",
    "平成元年1月8日", "平成元年1月7日", "平成31年4月30日", "平成31年5月1日",
    "令和元年5月1日", "令和元年4月30日", "令和6年4月1日",
    # notation
    "令和1年5月1日", "R6.4.1", "r6/4/1", "H31.04.30", "S64-1-7", "Ｒ６．４．１", "令和６年４月１日",
    "昭和六十四年一月七日", "令和二十年十二月三十一日", "令和十年十月十日", "令和二〇年一月一日",
    "令和 6 年 4 月 1 日", "令和6年4月1", "R060401", "R元.5.1", "令和元年五月一日", "平成元年一月八日",
    # existence / leap years
    "昭和55年2月29日", "昭和57年2月29日", "明治33年2月29日", "平成12年2月29日", "令和6年2月29日",
    "令和7年2月29日", "令和6年2月30日", "令和6年4月31日", "令和6年4月30日", "令和6年13月1日",
    "令和6年0月1日", "令和6年4月0日", "令和0年1月1日", "令和〇年1月1日",
    "昭和65年1月1日", "大正16年1月1日", "平成99年1月1日",
    # unreadable
    "令和", "R", "R6.4", "令和6年4月", "令和年4月1日", "令和6年元月1日", "令和百年1月1日",
    "令和十十年1月1日", "2024-04-01", "2024年4月1日", "6年4月1日", "5060401", "令和6年4月1日\n以降",
    "令和6年4月1日 以降", "令和6年4月1日 ", "令和6年4月1日日",
]  # fmt: skip


def evaluate(spark, values, make_expr):
    df = string_frame(spark, values)
    from pyspark.sql import functions as F

    rows = df.select("i", make_expr(F.col("v")).alias("r")).orderBy("i").collect()
    assert len(rows) == len(values)
    return [row["r"] for row in rows]


@pytest.mark.parametrize(
    "spark_expr,python_fn,values",
    [
        (E.corporate_number_problem, V.corporate_number_problem, CORPORATE),
        (E.invoice_number_problem, V.invoice_number_problem, INVOICE),
        (E.local_gov_code_problem, V.local_gov_code_problem, LOCAL_GOV),
        (E.wareki_problem, W.wareki_problem, WAREKI),
    ],
    ids=["corporate", "invoice", "local_gov", "wareki"],
)
def test_parity(spark, ansi, spark_expr, python_fn, values):
    got = evaluate(spark, values, spark_expr)
    for value, actual in zip(values, got, strict=True):
        assert actual == python_fn(value), f"input={value!r}: spark={actual!r} python={python_fn(value)!r}"


def _calendar_sample():
    """Every 17th day from 明治6年 to 2060 in the era-name notation, plus the day after each era change."""
    from datetime import date, timedelta

    days = []
    d = date(1873, 1, 1)
    while d <= date(2060, 12, 31):
        days.append(d)
        d += timedelta(days=17)
    days += [era.start for era in W.ERAS] + [era.end for era in W.ERAS if era.end]
    out = []
    for d in days:
        era = next(e for e in reversed(W.ERAS) if d >= e.start)
        out.append(f"{era.name}{d.year - era.first_year + 1}年{d.month}月{d.day}日")
    return out


def test_wareki_parity_over_the_calendar(spark, ansi):
    values = _calendar_sample()
    got = evaluate(spark, values, E.wareki_problem)
    assert got == [None] * len(values)
    assert [W.wareki_problem(v) for v in values] == [None] * len(values)


def test_every_reason_code_is_exercised(spark):
    assert {
        *evaluate(spark, CORPORATE, E.corporate_number_problem),
        *evaluate(spark, LOCAL_GOV, E.local_gov_code_problem),
        *evaluate(spark, WAREKI, E.wareki_problem),
    } >= {None, V.FORMAT, V.CHECK_DIGIT, V.PREFECTURE, V.NOT_EXIST, V.ERA_BOUNDARY, V.PRE_GREGORIAN}
