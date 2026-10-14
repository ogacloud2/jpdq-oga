"""和暦 (Japanese-era dates): validation by the era's real calendar range (pure Python reference).

A value is judged in four steps (the first failing step names the reason code):

1. ``format``        the text cannot be read as an era date
2. ``pre_gregorian`` 明治5年以前: old-calendar dates, outside the supported range (see below)
3. ``not_exist``     the day does not exist (2月30日, 13月, 0日, 令和0年, 非閏年の2月29日)
4. ``era_boundary``  the day exists but lies outside the stated era (平成31年5月1日 is 令和元年)

Parsing is a fixed pipeline of regular-expression rewrites that brings every accepted spelling to one
canonical shape (``R6.4.1``) which is then matched once.  The Spark implementation in
:mod:`jpdq_oga.expressions` runs the very same pipeline (:data:`CANONICAL_STEPS`).

Supported range: 明治6年1月1日 (1873-01-01, adoption of the Gregorian calendar) onwards.  Era start dates
are from 国立天文台 暦計算室「元号」; see docs/sources.md.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import NamedTuple

from .validators import ERA_BOUNDARY, FORMAT, NOT_EXIST, PRE_GREGORIAN


class Era(NamedTuple):
    name: str  # 令和
    letter: str  # R
    first_year: int  # Gregorian year of 元年 (令和元年 = 2019)
    start: date  # first supported day of the era
    end: date | None  # last day of the era (None = current era)


# 明治: 慶応4年を明治元年とする (1868) but dates before 明治6年1月1日 (1873-01-01) are old-calendar dates and are
# outside the supported range, so the supported start is 1873-01-01.
_STARTS = (
    ("明治", "M", 1868, date(1873, 1, 1)),
    ("大正", "T", 1912, date(1912, 7, 30)),  # 明治45年7月30日 詔書 07月30日以後を大正元年とする
    ("昭和", "S", 1926, date(1926, 12, 25)),  # 大正15年12月25日 詔書 12月25日以後を昭和元年とする
    ("平成", "H", 1989, date(1989, 1, 8)),  # 昭和64年 政令第1号 01月08日以後を平成元年とする
    ("令和", "R", 2019, date(2019, 5, 1)),  # 平成31年 政令第143号 05月01日以後を令和元年とする
)

ERAS: tuple[Era, ...] = tuple(
    Era(name, letter, first_year, start, (_STARTS[i + 1][3] - timedelta(days=1)) if i + 1 < len(_STARTS) else None)
    for i, (name, letter, first_year, start) in enumerate(_STARTS)
)

#: 明治 years up to this one are on the old calendar (明治5年12月2日 was followed by 明治6年1月1日).
LAST_PRE_GREGORIAN_MEIJI_YEAR = 5

# --- text pipeline -----------------------------------------------------------------------------------
# Fullwidth digits / Latin letters / separators -> ASCII; the ideographic space becomes a space.
WIDTH_FROM = "".join(chr(c) for c in range(0xFF10, 0xFF1A)) + "ＲＨＳＴＭｒｈｓｔｍ" + "．／－" + "\u3000"
WIDTH_TO = "0123456789" + "RHSTMrhstm" + "./-" + " "
assert len(WIDTH_FROM) == len(WIDTH_TO)

KANSUJI_DIGITS_FROM = "〇零一二三四五六七八九"
KANSUJI_DIGITS_TO = "00123456789"

_ERA_CHARS = "令和平成昭大正明治RHSTMrhstm"

# (pattern, replacement) applied in order to the whitespace-free string.  Replacement syntax is Java's
# (``$1``); the Python side converts it.  Patterns only use constructs shared by Python ``re`` and
# Java ``java.util.regex``.
CANONICAL_STEPS: tuple[tuple[str, str], ...] = (
    # kanji tens: 二十三 -> 2十3 -> 23, 十二 -> 12, 二十 -> 20, 十 -> 10
    (r"([0-9])十([0-9])", "$1$2"),
    (r"([0-9])十", "$10"),  # Java reads "$10" as group 1 + "0" (there is no group 10)
    (r"十([0-9])", "1$1"),
    (r"十", "10"),
    # gannen
    (f"(?<=[{_ERA_CHARS}])元", "1"),
    # compact letter form: R060401 -> R06.04.01
    (r"\A([RHSTMrhstm])([0-9]{2})([0-9]{2})([0-9]{2})\z", "$1$2.$3.$4"),
)

PATTERN_CANONICAL = r"(令和|平成|昭和|大正|明治|[RHSTMrhstm])([0-9]{1,3})[年./\-]([0-9]{1,3})[月./\-]([0-9]{1,3})日?"

_WS = re.compile(r"[ \t\n\r\f\x0B]+")
_STEPS = tuple(
    (re.compile(p.replace(r"\z", r"\Z")), re.sub(r"\$\{?(\d)\}?", r"\\g<\1>", r)) for p, r in CANONICAL_STEPS
)
_CANONICAL = re.compile(PATTERN_CANONICAL)
_WIDTH_TABLE = str.maketrans(WIDTH_FROM, WIDTH_TO)
_KANSUJI = str.maketrans(KANSUJI_DIGITS_FROM, KANSUJI_DIGITS_TO)


def canonical_form(value: str) -> str:
    """The canonical shape of an era-date string (``"令和六年四月一日"`` -> ``"令和6年4月1日"``)."""
    s = _WS.sub("", value.translate(_WIDTH_TABLE)).translate(_KANSUJI)
    for pattern, replacement in _STEPS:
        s = pattern.sub(replacement, s)
    return s


def _find_era(token: str) -> Era:
    token = token.upper()
    for era in ERAS:
        if token in (era.name, era.letter):
            return era
    raise AssertionError(token)  # PATTERN_CANONICAL admits only the names and letters above


def wareki_problem(value: str | None) -> str | None:
    """``None`` for a real date within its era (or ``None`` input), else the reason code."""
    if value is None:
        return None
    m = _CANONICAL.fullmatch(canonical_form(value))
    if m is None:
        return FORMAT
    era = _find_era(m.group(1))
    year, month, day = (int(m.group(i)) for i in (2, 3, 4))
    if year < 1:
        return NOT_EXIST
    if era.name == "明治" and year <= LAST_PRE_GREGORIAN_MEIJI_YEAR:
        return PRE_GREGORIAN
    try:
        result = date(era.first_year + year - 1, month, day)
    except ValueError:
        return NOT_EXIST
    if result < era.start or (era.end is not None and result > era.end):
        return ERA_BOUNDARY
    return None


def parse_wareki(value: str | None) -> date | None:
    """Reference conversion used by tests: the Gregorian date, or ``None`` if the value is a violation."""
    if value is None or wareki_problem(value) is not None:
        return None
    m = _CANONICAL.fullmatch(canonical_form(value))
    assert m is not None
    era = _find_era(m.group(1))
    return date(era.first_year + int(m.group(2)) - 1, int(m.group(3)), int(m.group(4)))


def is_valid_wareki(value: str | None) -> bool | None:
    return None if value is None else wareki_problem(value) is None
