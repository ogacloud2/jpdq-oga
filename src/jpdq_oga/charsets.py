"""Character-set facts and a pure-Python reference for the character checks (no dependencies).

Three questions about a string, all answered from Python's own codec tables so that the Spark expressions
(:mod:`jpdq_oga.expressions`, built from the ranges below) and this reference are the same by construction and
are compared character by character in ``tests/test_charsets.py``:

``halfwidth_kana``
    contains a half-width katakana (U+FF61-U+FF9F).
``platform_dependent``
    contains a character that ``cp932`` (Windows' Shift_JIS) can encode but ``shift_jis`` (JIS X 0208) cannot:
    NEC special characters (①, ㈱, ㌔ ...) and IBM extension kanji (髙, 﨑 ...), whose code points are not the same
    on every platform.
``unencodable``
    contains a character the chosen charset (``cp932`` by default, or ``shift_jis``) cannot encode at all
    (emoji, 𠮷 and other characters outside the Basic Multilingual Plane, ...).

Both codecs only contain Basic Multilingual Plane characters, so the tables are built over U+0000-U+FFFF.
"""

from __future__ import annotations

from functools import cache

HALFWIDTH_KANA = "halfwidth_kana"
PLATFORM_DEPENDENT = "platform_dependent"
UNENCODABLE = "unencodable"

CHARSETS = ("cp932", "shift_jis")
DEFAULT_CHARSET = "cp932"

HALFWIDTH_KANA_RANGE = (0xFF61, 0xFF9F)

Ranges = tuple[tuple[int, int], ...]


def validate_charset(charset: str) -> str:
    if charset not in CHARSETS:
        raise ValueError(f"unsupported charset {charset!r}; use one of {CHARSETS}")
    return charset


def _to_ranges(code_points: list[int]) -> Ranges:
    ranges: list[tuple[int, int]] = []
    for cp in code_points:
        if ranges and cp == ranges[-1][1] + 1:
            ranges[-1] = (ranges[-1][0], cp)
        else:
            ranges.append((cp, cp))
    return tuple(ranges)


@cache
def encodable_ranges(charset: str) -> Ranges:
    """Code points (as ranges) of the Basic Multilingual Plane that ``charset`` can encode."""
    validate_charset(charset)
    points = []
    for cp in range(0x10000):
        if 0xD800 <= cp <= 0xDFFF:  # surrogates are not characters
            continue
        try:
            chr(cp).encode(charset)
        except UnicodeEncodeError:
            continue
        points.append(cp)
    return _to_ranges(points)


@cache
def platform_dependent_ranges() -> Ranges:
    """Encodable in ``cp932`` but not in ``shift_jis``."""
    strict = _ranges_to_set(encodable_ranges("shift_jis"))
    return _to_ranges([cp for lo, hi in encodable_ranges("cp932") for cp in range(lo, hi + 1) if cp not in strict])


def _ranges_to_set(ranges: Ranges) -> frozenset[int]:
    return frozenset(cp for lo, hi in ranges for cp in range(lo, hi + 1))


def _in(ranges: Ranges, cp: int) -> bool:
    return any(lo <= cp <= hi for lo, hi in ranges)


def characters(ranges: Ranges) -> str:
    """Every character of ``ranges`` as one string (for Spark's ``translate``, a hash lookup per character)."""
    return "".join(chr(cp) for lo, hi in ranges for cp in range(lo, hi + 1))


def regex_class(ranges: Ranges, negate: bool = False) -> str:
    """A Java regular-expression character class (``\\x{...}`` escapes) for ``ranges``."""
    body = "".join(rf"\x{{{lo:X}}}" if lo == hi else rf"\x{{{lo:X}}}-\x{{{hi:X}}}" for lo, hi in ranges)
    return f"[{'^' if negate else ''}{body}]"


# --- pure-Python reference ---------------------------------------------------------------------------------


def halfwidth_kana_problem(value: str | None) -> str | None:
    if value is None:
        return None
    lo, hi = HALFWIDTH_KANA_RANGE
    return HALFWIDTH_KANA if any(lo <= ord(ch) <= hi for ch in value) else None


def platform_dependent_problem(value: str | None) -> str | None:
    if value is None:
        return None
    ranges = platform_dependent_ranges()
    return PLATFORM_DEPENDENT if any(_in(ranges, ord(ch)) for ch in value) else None


def unencodable_problem(value: str | None, charset: str = DEFAULT_CHARSET) -> str | None:
    """``unencodable`` when some character cannot be encoded in ``charset`` (checked with the codec itself)."""
    validate_charset(charset)
    if value is None:
        return None
    try:
        value.encode(charset)
    except UnicodeEncodeError:
        return UNENCODABLE
    return None
