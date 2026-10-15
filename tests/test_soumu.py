"""総務省 workbooks -> ``local_gov_codes`` with history.

The synthetic workbooks reproduce the layout of the published files (verified 2026-10-15): header rows,
blocks starting at a prefecture name, ``〃`` ditto cells, integer codes that lost their leading zero,
and the three date notations (``R6.1.1`` strings, date cells, Excel serial numbers).
Set ``JPDQ_SOUMU_CURRENT`` and ``JPDQ_SOUMU_REVISIONS`` to the downloaded files for the real-file test.
"""

import datetime as dt
import os
from unittest.mock import MagicMock

import pytest

openpyxl = pytest.importorskip("openpyxl")

from jpdq_oga import reference as R  # noqa: E402
from jpdq_oga import soumu as S  # noqa: E402

D = dt.date


def make_current(path, extra_sheet=True):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "R6.1.1現在の団体"
    ws.append(
        ["団体コード", "都道府県名\n（漢字）", "市区町村名\n（漢字）", "都道府県名\n（カナ）", "市区町村名\n（カナ）"]
    )
    ws.append(["010006", "北海道", None, "ﾎｯｶｲﾄﾞｳ", None])
    ws.append(["011002", "北海道", "札幌市", "ﾎｯｶｲﾄﾞｳ", "ｻｯﾎﾟﾛｼ"])
    ws.append(["221384", "静岡県", "浜松市中央区", None, None])
    ws.append(["221392", "静岡県", "浜松市浜名区", None, None])
    ws.append(["282219", "兵庫県", "丹波篠山市", None, None])
    ws.append(["042161", "宮城県", "富谷市", None, None])
    ws.append([None, None, None])  # blank row
    if extra_sheet:
        wards = wb.create_sheet("R6.1.1政令指定都市")
        wards.append(
            ["団体コード", "都道府県名\n（漢字）", "市区町村名\n（漢字）", "都道府県名\n（ｶﾅ）", "市区町村名\n（ｶﾅ）"]
        )
        wards.append(["011002", "北海道", "札幌市"])  # overlaps with the first sheet
        wards.append(["011011", "北海道", "札幌市中央区"])
    wb.save(path)


def blank_row():
    return [None] * 14


def row(
    prefecture=None, before_code=None, before_name=None, category=None, after_code=None, after_name=None, date=None
):
    r = blank_row()
    r[4], r[5], r[6], r[8], r[9], r[10], r[13] = (
        prefecture,
        before_code,
        before_name,
        category,
        after_code,
        after_name,
        date,
    )
    return r


def make_revisions(path, rows=None):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "改正一覧表"
    top = blank_row()
    top[4], top[5] = "【参考資料】", "(R5.4.1時点)"
    ws.append(top)
    head = blank_row()
    head[0], head[1], head[4], head[5], head[8], head[9], head[12], head[13] = (
        "旧並び順", "新並び順", "都道府県名", "改正前のコード表", "コードの改定区分",
        "改正後のコード表", "改正事由等", "施行年月日",
    )  # fmt: skip
    ws.append(head)
    sub1 = blank_row()
    sub1[5], sub1[6], sub1[9], sub1[10] = "コード", "市区町村名", "コード", "市区町村名"
    ws.append(sub1)
    sub2 = blank_row()
    sub2[6], sub2[7], sub2[10], sub2[11] = "名称", "ふりがな", "名称", "ふりがな"
    ws.append(sub2)
    for r in rows if rows is not None else DEFAULT_ROWS:
        ws.append(r)
    wb.save(path)


DEFAULT_ROWS = [
    # block 1: wards re-organised; the date sits on the first 新設 row, "〃" below it
    row("静岡県", 221317, "中区", "欠番", "削除", "削除"),  # integer code
    row(None, "221325", "東区", "〃", "〃", "〃"),
    row(None, None, None, "新設", "221384", "浜松市中央区", date="R6.1.1"),
    row(None, None, None, "〃", "221392", "浜松市浜名区", date="〃"),
    # block 2: rename only, the code stays
    row("兵庫県", "282219", "篠山市", None, "同左", "丹波篠山市", date="R1.5.1"),
    # block 3: Excel serial number (41734 = 2014-04-05), a code that lost its leading zero
    row("栃木県", "093670", "岩舟町", "欠番", "削除", "削除", date=41734),
    row("北海道", 14397, " 幌加内町", "欠番", "削除", " 削除", date=40269),  # 2010-04-01
    # block 4: a date cell; the receiving city keeps its code (編入合併)
    row("新潟県", "152021", "長岡市", None, "同左", "同左"),
    row(None, "152153", "栃尾市", "削除", "削除", "削除", date=dt.datetime(2006, 1, 1)),
    row(None, "154041", "和島村", "〃", "〃", "〃", date="〃"),
    # block 5: a city created and later (in another block) listed as current
    row("宮城県", "044237", "富谷町", "欠番", "削除", "削除"),
    row(None, None, None, "新設", "042161", "富谷市", date="H28.10.10"),
]


# --- small parsers ----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("R6.1.1", D(2024, 1, 1)),
        (" H30.10.1 ", D(2018, 10, 1)),
        (dt.datetime(2006, 1, 1, 0, 0), D(2006, 1, 1)),
        (41734, D(2014, 4, 5)),  # 岩舟町 -> 栃木市
        (40269.0, D(2010, 4, 1)),
        ("〃", None),
        (None, None),
        (True, None),
        ("来月", None),
    ],
)
def test_parse_effective_date(value, expected):
    assert S.parse_effective_date(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("011002", "011002"),
        (14397, "014397"),
        (14397.0, "014397"),
        (" 221317 ", "221317"),
        ("削除", None),
        ("1234567", None),
        (None, None),
        (True, None),
    ],
)
def test_code6(value, expected):
    assert S.code6(value) == expected


# --- workbooks --------------------------------------------------------------------------------------------


def test_current_codes_are_the_union_of_all_sheets(tmp_path):
    path = tmp_path / "current.xlsx"
    make_current(path)
    codes, as_of = S.read_current_codes(path)
    assert as_of == D(2024, 1, 1)
    assert {c.code for c in codes} == {"010006", "011002", "221384", "221392", "282219", "042161", "011011"}
    by_code = {c.code: c for c in codes}
    assert by_code["010006"].city is None  # the prefecture's own row
    assert by_code["011011"].city == "札幌市中央区"


def test_revisions_events_and_block_dates(tmp_path):
    path = tmp_path / "revisions.xlsx"
    make_revisions(path)
    result = S.read_revisions(path)
    events = {(e.kind, e.code): e for e in result.events}
    assert result.skipped == []
    assert set(events) == {
        ("retire", "221317"), ("retire", "221325"), ("new", "221384"), ("new", "221392"),
        ("retire", "093670"), ("retire", "014397"), ("retire", "152153"), ("retire", "154041"),
        ("retire", "044237"), ("new", "042161"),
    }  # fmt: skip
    # the retired rows precede the row that carries the date: the block date applies to all of them
    assert events[("retire", "221317")].effective == D(2024, 1, 1)
    assert events[("retire", "221325")].effective == D(2024, 1, 1)
    assert events[("new", "221392")].effective == D(2024, 1, 1)
    assert events[("retire", "093670")].effective == D(2014, 4, 5)  # Excel serial
    assert events[("retire", "014397")].effective == D(2010, 4, 1)  # leading zero restored
    assert events[("retire", "014397")].city == "幌加内町"  # stripped
    assert events[("retire", "154041")].effective == D(2006, 1, 1)  # date cell, then "〃"
    assert events[("new", "042161")].effective == D(2016, 10, 10)
    assert events[("retire", "044237")].effective == D(2016, 10, 10)  # block date written on the 新設 row
    assert events[("retire", "221317")].prefecture == "静岡県"
    # rename-only and unchanged rows create no events
    assert not [e for e in result.events if e.code in ("282219", "152021")]


def test_history_rows(tmp_path):
    make_current(tmp_path / "c.xlsx", extra_sheet=False)
    make_revisions(tmp_path / "r.xlsx")
    codes, _ = S.read_current_codes(tmp_path / "c.xlsx")
    rows = {r[0]: r for r in S.build_history(codes, S.read_revisions(tmp_path / "r.xlsx").events)}
    assert rows["042161"] == ("042161", "宮城県", "富谷市", "2016-10-10", None)  # created inside the window
    assert rows["044237"] == ("044237", "宮城県", "富谷町", None, "2016-10-10")  # retired the same day
    assert rows["011002"] == ("011002", "北海道", "札幌市", None, None)  # in force since before the window
    assert rows["221384"][3] == "2024-01-01" and rows["221384"][4] is None
    assert rows["221317"] == ("221317", "静岡県", "中区", None, "2024-01-01")
    assert rows["010006"][2] is None


def test_codes_both_retired_and_current_are_rejected(tmp_path):
    make_current(tmp_path / "c.xlsx")
    make_revisions(tmp_path / "r.xlsx", [row("北海道", "011002", "札幌市", "欠番", "削除", "削除", date="R1.5.1")])
    codes, _ = S.read_current_codes(tmp_path / "c.xlsx")
    with pytest.raises(R.ReferenceDataError, match="same release"):
        S.build_history(codes, S.read_revisions(tmp_path / "r.xlsx").events)


def test_block_without_a_date_is_reported_not_guessed(tmp_path):
    make_revisions(tmp_path / "r.xlsx", [row("北海道", "014397", "幌加内町", "欠番", "削除", "削除")])
    result = S.read_revisions(tmp_path / "r.xlsx")
    assert result.events == []
    assert len(result.skipped) == 1 and "014397" in result.skipped[0]


def test_unexpected_workbooks_are_refused(tmp_path):
    wb = openpyxl.Workbook()
    wb.active.append(["code", "name"])
    wb.save(tmp_path / "other.xlsx")
    with pytest.raises(R.ReferenceDataError, match="団体コード"):
        S.read_current_codes(tmp_path / "other.xlsx")
    with pytest.raises(R.ReferenceDataError, match="改正一覧表 layout"):
        S.read_revisions(tmp_path / "other.xlsx")


# --- end to end with the DQX check ---------------------------------------------------------------------


@pytest.mark.spark
@pytest.mark.dqx
def test_history_drives_the_existence_check(spark, tmp_path):
    pytest.importorskip("databricks.labs.dqx")
    from conftest import rows_frame
    from databricks.labs.dqx.engine import DQEngineCore

    import jpdq_oga
    from jpdq_oga import cli

    make_current(tmp_path / "c.xlsx")
    make_revisions(tmp_path / "r.xlsx")
    table, notes = cli.build_local_gov_codes_from_workbooks(
        spark, str(tmp_path / "c.xlsx"), str(tmp_path / "r.xlsx"), today=dt.date(2024, 6, 1)
    )
    assert notes == ["current code list as of 2024-01-01; revisions from 2005-04-01"]
    assert sorted(table.columns) == sorted(R.LOCAL_GOV_COLUMNS)

    engine = DQEngineCore(workspace_client=MagicMock(), spark=spark)
    df = rows_frame(spark, ["i", "code"], [("0", "044237"), ("1", "042161")])

    def flagged(as_of):
        check = {
            "function": "is_existing_jp_local_gov_code",
            "arguments": {"column": "code", "ref_df_name": "c", "as_of": as_of},
        }
        out = engine.apply_checks_by_metadata(
            df, [{"criticality": "error", "check": check}],
            custom_check_functions=jpdq_oga.get_japan_checks(), ref_dfs={"c": table},
        )  # fmt: skip
        return {r["code"]: bool(r["_errors"]) for r in out.collect()}

    assert flagged("2016-10-09") == {"044237": False, "042161": True}  # 富谷町 still exists, 富谷市 does not yet
    assert flagged("2016-10-10") == {"044237": True, "042161": False}  # the merger day: roles swap


@pytest.mark.spark
@pytest.mark.parametrize(
    ("today", "stale"),
    # 2024 is a leap year: 2024-01-01 -> 2024-12-31 is 365 days (not stale), -> 2025-01-01 is 366 days
    [(dt.date(2024, 12, 31), False), (dt.date(2025, 1, 1), True), (dt.date(2026, 10, 16), True)],
)
def test_an_old_code_list_is_flagged(spark, tmp_path, today, stale):
    from jpdq_oga import cli

    make_current(tmp_path / "c.xlsx")  # as of 2024-01-01
    make_revisions(tmp_path / "r.xlsx")
    _, notes = cli.build_local_gov_codes_from_workbooks(
        spark, str(tmp_path / "c.xlsx"), str(tmp_path / "r.xlsx"), today=today
    )
    warnings = [n for n in notes if n.startswith("WARNING")]
    assert bool(warnings) is stale
    if stale:
        assert "as of 2024-01-01" in warnings[0] and "https://www.soumu.go.jp/denshijiti/code.html" in warnings[0]


# --- the real files ---------------------------------------------------------------------------------------


@pytest.mark.skipif(
    "JPDQ_SOUMU_CURRENT" not in os.environ or "JPDQ_SOUMU_REVISIONS" not in os.environ,
    reason="set JPDQ_SOUMU_CURRENT and JPDQ_SOUMU_REVISIONS to the downloaded 総務省 workbooks",
)
def test_real_workbooks():
    codes, as_of = S.read_current_codes(os.environ["JPDQ_SOUMU_CURRENT"])
    revisions = S.read_revisions(os.environ["JPDQ_SOUMU_REVISIONS"])
    assert as_of is not None and len(codes) > 1900
    assert revisions.skipped == []
    assert len([e for e in revisions.events if e.kind == "retire"]) > 900
    assert min(e.effective for e in revisions.events) >= D(2005, 4, 1)

    rows = S.build_history(codes, revisions.events)  # raises if a retired code is still current
    by_code = {r[0]: r for r in rows}
    assert len(rows) == len(by_code)
    assert all(r[3] is None or r[4] is None or r[3] <= r[4] for r in rows)  # valid_from <= valid_to
    # facts visible in the published file: 富谷町 became 富谷市 on 2016-10-10, 那珂川町 became 那珂川市 on 2018-10-01
    assert by_code["044237"][4] == "2016-10-10" and by_code["042161"][3] == "2016-10-10"
    assert by_code["403059"][4] == "2018-10-01" and by_code["402311"][3] == "2018-10-01"
    assert by_code["011002"][3] is None and by_code["011002"][4] is None
