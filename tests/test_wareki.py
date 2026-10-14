"""和暦 (Phase C).  Boundaries are from 国立天文台 暦計算室「元号」 (see docs/sources.md):

* 大正: 明治45年(1912)07月30日 詔書 07月30日以後を大正元年とする
* 昭和: 大正15年(1926)12月25日 詔書 12月25日以後を昭和元年とする
* 平成: 昭和64年(1989)01月07日 政令第1号 01月08日以後を平成元年とする
* 令和: 平成31年(2019)04月01日 政令第143号 05月01日以後を令和元年とする

Policy for 明治5年以前: those dates are on the old (lunisolar) calendar and cannot be checked by year
arithmetic, so they are reported as ``pre_gregorian``; the supported range starts at 明治6年1月1日.
"""

from datetime import date, timedelta

import pytest

from jpdq_oga import validators as V
from jpdq_oga import wareki as w

OK = None
FORMAT, NOT_EXIST, ERA_BOUNDARY, PRE_GREGORIAN = V.FORMAT, V.NOT_EXIST, V.ERA_BOUNDARY, V.PRE_GREGORIAN


# --- 各元号の初日と前日 -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        # 明治6年1月1日 = 1873-01-01 (グレゴリオ暦採用) is the first supported day
        ("明治6年1月1日", OK),
        ("明治5年12月31日", PRE_GREGORIAN),
        ("明治元年1月1日", PRE_GREGORIAN),
        ("明治5年12月3日", PRE_GREGORIAN),
        # 大正 begins 1912-07-30
        ("明治45年7月29日", OK),
        ("明治45年7月30日", ERA_BOUNDARY),  # that day is already 大正元年
        ("大正元年7月30日", OK),
        ("大正元年7月29日", ERA_BOUNDARY),  # still 明治45年
        # 昭和 begins 1926-12-25
        ("大正15年12月24日", OK),
        ("大正15年12月25日", ERA_BOUNDARY),
        ("昭和元年12月25日", OK),
        ("昭和元年12月24日", ERA_BOUNDARY),
        # 平成 begins 1989-01-08
        ("昭和64年1月7日", OK),
        ("昭和64年1月8日", ERA_BOUNDARY),
        ("平成元年1月8日", OK),
        ("平成元年1月7日", ERA_BOUNDARY),
        # 令和 begins 2019-05-01
        ("平成31年4月30日", OK),
        ("平成31年5月1日", ERA_BOUNDARY),
        ("令和元年5月1日", OK),
        ("令和元年4月30日", ERA_BOUNDARY),
    ],
)
def test_era_first_day_and_the_day_before(value, expected):
    assert w.wareki_problem(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("大正元年7月30日", date(1912, 7, 30)),
        ("昭和元年12月25日", date(1926, 12, 25)),
        ("平成元年1月8日", date(1989, 1, 8)),
        ("令和元年5月1日", date(2019, 5, 1)),
        ("明治6年1月1日", date(1873, 1, 1)),
        ("明治45年7月29日", date(1912, 7, 29)),
        ("昭和64年1月7日", date(1989, 1, 7)),
        ("平成31年4月30日", date(2019, 4, 30)),
        ("令和6年4月1日", date(2024, 4, 1)),
    ],
)
def test_reference_parse_gives_the_gregorian_date(value, expected):
    assert w.parse_wareki(value) == expected


# --- 表記 (元年・漢数字・全角・略号) ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "令和元年5月1日",
        "令和1年5月1日",
        "令和６年４月１日",
        "令和六年四月一日",
        "令和二十年十二月三十一日",
        "令和十年十月十日",
        "昭和六十四年一月七日",
        "平成元年一月八日",
        "令和二〇年一月一日",
        "令和 6 年 4 月 1 日",
        "令和6年4月1",
        "R6.4.1",
        "r6/4/1",
        "H31.04.30",
        "S64-1-7",
        "Ｒ６．４．１",
        "R060401",
        "R元.5.1",
    ],
)
def test_accepted_notations(value):
    assert w.wareki_problem(value) is None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "令和",
        "R",
        "R6.4",
        "令和6年4月",
        "令和年4月1日",
        "令和6年元月1日",  # 元 is only a year
        "令和百年1月1日",
        "令和十十年1月1日",
        "2024-04-01",
        "2024年4月1日",
        "6年4月1日",
        "5060401",  # an era-digit code is not a notation
        "令和6年4月1日\n以降",
        "令和6年4月1日 以降",
    ],
)
def test_unreadable_values_are_format_violations(value):
    assert w.wareki_problem(value) == FORMAT


# --- 存在しない日付・閏年 ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("昭和55年2月29日", OK),  # 1980 leap year
        ("昭和57年2月29日", NOT_EXIST),  # 1982
        ("明治33年2月29日", NOT_EXIST),  # 1900 is not a leap year
        ("平成12年2月29日", OK),  # 2000 is a leap year
        ("令和6年2月29日", OK),
        ("令和7年2月29日", NOT_EXIST),
        ("令和6年2月30日", NOT_EXIST),
        ("令和6年4月31日", NOT_EXIST),
        ("令和6年4月30日", OK),
        ("令和6年13月1日", NOT_EXIST),
        ("令和6年0月1日", NOT_EXIST),
        ("令和6年4月0日", NOT_EXIST),
        ("令和0年1月1日", NOT_EXIST),
        ("令和〇年1月1日", NOT_EXIST),
        ("昭和65年1月1日", ERA_BOUNDARY),  # exists (1990-01-01) but not in 昭和
        ("大正16年1月1日", ERA_BOUNDARY),
        ("平成99年1月1日", ERA_BOUNDARY),
    ],
)
def test_nonexistent_dates_and_leap_years(value, expected):
    assert w.wareki_problem(value) == expected


def test_null_is_not_a_violation():
    assert w.wareki_problem(None) is None
    assert w.is_valid_wareki(None) is None
    assert w.parse_wareki(None) is None


def test_eras_are_contiguous_and_in_range():
    eras = w.ERAS
    assert [e.name for e in eras] == ["明治", "大正", "昭和", "平成", "令和"]
    for earlier, later in zip(eras, eras[1:], strict=False):
        assert earlier.end == later.start - timedelta(days=1)
    assert eras[-1].end is None
    assert eras[0].start == date(1873, 1, 1)


def _format(d: date) -> str:
    for era in reversed(w.ERAS):
        if d >= era.start:
            year = d.year - era.first_year + 1
            return f"{era.name}{year}年{d.month}月{d.day}日"
    raise AssertionError(d)


def test_every_supported_day_roundtrips_through_the_validator():
    d = date(1873, 1, 1)
    while d <= date(2060, 12, 31):
        text = _format(d)
        assert w.wareki_problem(text) is None, text
        assert w.parse_wareki(text) == d, text
        d += timedelta(days=1)
