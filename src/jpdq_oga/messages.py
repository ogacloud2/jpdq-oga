"""Violation messages in Japanese (default) and English.

Every template is a ``(prefix, suffix)`` pair wrapped around the offending value, e.g.
``法人番号のチェックディジットが一致しません（入力値: 1234567890123）``.
"""

from __future__ import annotations

from datetime import date

from .reference import MISMATCH, NOT_FOUND
from .validators import CHECK_DIGIT, ERA_BOUNDARY, FORMAT, NOT_EXIST, PRE_GREGORIAN, PREFECTURE

LANGUAGES = ("ja", "en")
DEFAULT_LANGUAGE = "ja"

# (prefix, suffix) around one value, or (prefix, middle, suffix) around two values.  ``{asof}`` is replaced
# by :func:`as_of_note`.
Template = tuple[str, ...]

TEMPLATES: dict[str, dict[str, dict[str, Template]]] = {
    "corporate_number": {
        "ja": {
            FORMAT: ("法人番号は半角数字13桁で指定してください（入力値: ", "）"),
            CHECK_DIGIT: ("法人番号のチェックディジットが一致しません（入力値: ", "）"),
        },
        "en": {
            FORMAT: ("Corporate number must be 13 ASCII digits (value: ", ")"),
            CHECK_DIGIT: ("Corporate number check digit does not match (value: ", ")"),
        },
    },
    "invoice_number": {
        "ja": {
            FORMAT: ("インボイス登録番号は「T」と半角数字13桁で指定してください（入力値: ", "）"),
            CHECK_DIGIT: ("インボイス登録番号（法人番号部分）のチェックディジットが一致しません（入力値: ", "）"),
        },
        "en": {
            FORMAT: ("Invoice registration number must be 'T' followed by 13 ASCII digits (value: ", ")"),
            CHECK_DIGIT: ("Invoice registration number: corporate number check digit does not match (value: ", ")"),
        },
    },
    "local_gov_code": {
        "ja": {
            FORMAT: ("全国地方公共団体コードは半角数字6桁で指定してください（入力値: ", "）"),
            PREFECTURE: ("全国地方公共団体コードの先頭2桁が都道府県コード（01〜47）ではありません（入力値: ", "）"),
            CHECK_DIGIT: ("全国地方公共団体コードの検査数字が一致しません（入力値: ", "）"),
        },
        "en": {
            FORMAT: ("Local government code must be 6 ASCII digits (value: ", ")"),
            PREFECTURE: ("Local government code must start with a prefecture code 01-47 (value: ", ")"),
            CHECK_DIGIT: ("Local government code check digit does not match (value: ", ")"),
        },
    },
    "postal_code_exists": {
        "ja": {
            FORMAT: ("郵便番号は7桁の半角数字（ハイフンは任意）で指定してください（入力値: ", "）"),
            NOT_FOUND: ("郵便番号が日本郵便のデータに存在しません{asof}（入力値: ", "）"),
        },
        "en": {
            FORMAT: ("Postal code must be 7 ASCII digits, hyphen optional (value: ", ")"),
            NOT_FOUND: ("Postal code does not exist in the Japan Post data{asof} (value: ", ")"),
        },
    },
    "postal_code_prefecture": {
        "ja": {MISMATCH: ("郵便番号と都道府県が一致しません{asof}（郵便番号: ", "、都道府県: ", "）")},
        "en": {MISMATCH: ("Postal code does not belong to the prefecture{asof} (postal code: ", ", prefecture: ", ")")},
    },
    "postal_code_city": {
        "ja": {MISMATCH: ("郵便番号と市区町村が一致しません{asof}（郵便番号: ", "、市区町村: ", "）")},
        "en": {MISMATCH: ("Postal code does not belong to the city{asof} (postal code: ", ", city: ", ")")},
    },
    "local_gov_code_exists": {
        "ja": {
            FORMAT: ("全国地方公共団体コードは半角数字6桁で指定してください（入力値: ", "）"),
            NOT_FOUND: (
                "全国地方公共団体コードが存在しません{asof}。合併などで廃止されたコードの可能性があります（入力値: ",
                "）",
            ),
        },
        "en": {
            FORMAT: ("Local government code must be 6 ASCII digits (value: ", ")"),
            NOT_FOUND: (
                "Local government code does not exist{asof}; it may have been retired by a merger (value: ",
                ")",
            ),
        },
    },
    "wareki_date": {
        "ja": {
            FORMAT: ("和暦の日付として読み取れません（例: 令和6年4月1日、R6.4.1）（入力値: ", "）"),
            NOT_EXIST: ("存在しない日付です（入力値: ", "）"),
            ERA_BOUNDARY: (
                "元号の期間外の日付です。改元日（昭和64年1月7日／平成31年4月30日 など）を確認してください（入力値: ",
                "）",
            ),
            PRE_GREGORIAN: ("明治5年以前は旧暦のため検証できません（明治6年1月1日以降が対象です）（入力値: ", "）"),
        },
        "en": {
            FORMAT: ("Cannot be read as a Japanese-era date (e.g. 令和6年4月1日, R6.4.1) (value: ", ")"),
            NOT_EXIST: ("The date does not exist (value: ", ")"),
            ERA_BOUNDARY: ("The date lies outside the stated era; check the era change dates (value: ", ")"),
            PRE_GREGORIAN: ("Meiji 5 and earlier use the old calendar and cannot be validated (value: ", ")"),
        },
    },
}


def as_of_note(lang: str, as_of: date | None) -> str:
    """Suffix naming the reference date; empty when the date is the run date or comes from a column."""
    if as_of is None:
        return ""
    return f"（基準日: {as_of.isoformat()}）" if lang == "ja" else f" (as of {as_of.isoformat()})"


def validate_language(lang: str) -> str:
    if lang not in LANGUAGES:
        raise ValueError(f"unsupported language {lang!r}; use one of {LANGUAGES}")
    return lang
