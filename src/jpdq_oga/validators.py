"""Pure-Python reference validators for Japanese identifiers.

These scalar functions are the specification the Spark expressions in :mod:`jpdq_oga.expressions`
are tested against.  Algorithms and their sources are listed in ``docs/sources.md``.

Detection, not conversion: input is judged exactly as given.  Fullwidth digits, surrounding
whitespace, hyphens and lowercase ``t`` are reported as format violations rather than repaired.
``None`` is never a violation (combine with a not-null check if the column is mandatory).

Each ``*_problem`` function returns ``None`` when the value is fine, otherwise one of
:data:`FORMAT`, :data:`CHECK_DIGIT`, :data:`PREFECTURE` (and, for dates, :data:`NOT_EXIST`,
:data:`ERA_BOUNDARY`, :data:`PRE_GREGORIAN`).
"""

from __future__ import annotations

import re

FORMAT = "format"
CHECK_DIGIT = "check_digit"
PREFECTURE = "prefecture"
NOT_EXIST = "not_exist"
ERA_BOUNDARY = "era_boundary"
PRE_GREGORIAN = "pre_gregorian"

_CORPORATE_NUMBER = re.compile(r"[0-9]{13}")
_INVOICE_NUMBER = re.compile(r"T[0-9]{13}")
_LOCAL_GOV_CODE = re.compile(r"[0-9]{6}")


def corporate_number_check_digit(base12: str) -> int:
    """Check digit (検査用数字) of a 12-digit base number (基礎番号).

    ``9 - (S mod 9)`` where ``S = Σ(odd positions from the right) + 2 · Σ(even positions)``.
    Source: 国税庁「チェックデジットの計算」(see docs/sources.md).
    """
    total = 0
    for position, digit in enumerate(reversed(base12), start=1):
        total += int(digit) * (1 if position % 2 else 2)
    return 9 - (total % 9)


def corporate_number_problem(value: str | None) -> str | None:
    """13-digit 法人番号: the first digit is the check digit of the following 12 digits."""
    if value is None:
        return None
    if not _CORPORATE_NUMBER.fullmatch(value):
        return FORMAT
    return None if int(value[0]) == corporate_number_check_digit(value[1:]) else CHECK_DIGIT


def invoice_number_problem(value: str | None) -> str | None:
    """インボイス登録番号 of a corporation: ``T`` followed by a valid 13-digit 法人番号.

    Registration numbers of sole proprietors are also ``T`` + 13 digits but are not derived from a
    法人番号; no check-digit rule for them is published in the sources used here, so this
    validator is a validator for the corporate form.
    """
    if value is None:
        return None
    if not _INVOICE_NUMBER.fullmatch(value):
        return FORMAT
    return corporate_number_problem(value[1:])


def local_gov_code_check_digit(code5: str) -> int:
    """検査数字 of a 5-digit 全国地方公共団体コード.

    Multiply digits 1-5 by 6,5,4,3,2, sum, take the remainder ``r`` modulo 11; the check digit is
    the last digit of ``11 - r`` (so ``r = 0`` gives 1, ``r = 1`` gives 0, ``r = 10`` gives 1).
    Source: 総務省「全国地方公共団体コード仕様」11 検査数字.
    """
    total = sum(int(d) * w for d, w in zip(code5, (6, 5, 4, 3, 2), strict=True))
    return (11 - total % 11) % 10


def local_gov_code_problem(value: str | None) -> str | None:
    """6-digit 全国地方公共団体コード: prefecture 01-47 in digits 1-2, check digit in digit 6."""
    if value is None:
        return None
    if not _LOCAL_GOV_CODE.fullmatch(value):
        return FORMAT
    if not 1 <= int(value[:2]) <= 47:
        return PREFECTURE
    return None if int(value[5]) == local_gov_code_check_digit(value[:5]) else CHECK_DIGIT


def is_valid_corporate_number(value: str | None) -> bool | None:
    return None if value is None else corporate_number_problem(value) is None


def is_valid_invoice_number(value: str | None) -> bool | None:
    return None if value is None else invoice_number_problem(value) is None


def is_valid_local_gov_code(value: str | None) -> bool | None:
    return None if value is None else local_gov_code_problem(value) is None
