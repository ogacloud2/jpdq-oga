"""Environment self-check: ``jpdq_oga.selfcheck()``.

Prints the versions that matter (Python, Spark, ANSI mode, DQX, this package), runs every check on a handful of
values whose answers are known, and says what to paste into a bug report if something differs. It reads no
tables and writes nothing.
"""

from __future__ import annotations

import os
import platform
import sys
from importlib import metadata
from typing import Any

from . import __version__

# (label, column expression module attribute, input value, expected reason code or None for "valid")
_CASES: list[tuple[str, str, str, str | None]] = [
    ("法人番号: 正しい番号", "corporate_number_problem", "8700110005901", None),
    ("法人番号: 検査用数字の誤り", "corporate_number_problem", "8700110005902", "check_digit"),
    ("法人番号: 桁数の誤り", "corporate_number_problem", "870011000590", "format"),
    ("地方公共団体コード: 正しい番号", "local_gov_code_problem", "162019", None),
    ("地方公共団体コード: 検査数字の誤り", "local_gov_code_problem", "162010", "check_digit"),
    ("和暦: 令和元年5月1日（実在）", "wareki_problem", "令和元年5月1日", None),
    ("和暦: 令和元年4月30日（元号の期間外）", "wareki_problem", "令和元年4月30日", "era_boundary"),
    ("和暦: 令和6年4月31日（存在しない日）", "wareki_problem", "令和6年4月31日", "not_exist"),
    ("文字種: 通常の漢字・かな", "unencodable_problem", "東京都千代田区", None),
    ("文字種: 半角カナ", "halfwidth_kana_problem", "ｱｲｳ", "halfwidth_kana"),
    ("文字種: 機種依存文字（丸数字）", "platform_dependent_problem", "①", "platform_dependent"),
    ("文字種: cp932 に変換できない文字（絵文字）", "unencodable_problem", "😀", "unencodable"),
]


def _version(distribution: str) -> str:
    try:
        return metadata.version(distribution)
    except metadata.PackageNotFoundError:
        return "未インストール"


def _active_spark() -> Any:
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


def selfcheck(spark: Any = None, *, quiet: bool = False) -> bool:
    """Run the environment self-check. Returns ``True`` when every case behaved as expected.

    ``spark`` defaults to the active session. ``quiet=True`` suppresses the report (the result is still returned).
    """
    lines: list[str] = []
    ok = True

    def say(text: str = "") -> None:
        lines.append(text)

    try:
        session = spark if spark is not None else _active_spark()
        spark_version = session.version
        try:
            ansi = str(session.conf.get("spark.sql.ansi.enabled"))
        except Exception:  # noqa: BLE001 - not every platform exposes the setting
            ansi = "取得できません"
    except Exception as exc:  # noqa: BLE001
        say(f"[NG] Spark のセッションを取得できません: {type(exc).__name__}: {str(exc).splitlines()[0]}")
        _emit(lines, quiet)
        return False

    say("== 環境 ==")
    say(f"jpdq-oga           : {__version__}")
    say(f"Python             : {platform.python_version()} ({sys.platform})")
    say(f"Spark              : {spark_version}（ANSI モード: {ansi}）")
    say(f"databricks-labs-dqx: {_version('databricks-labs-dqx')}")
    runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION")
    say(f"Databricks Runtime : {runtime if runtime else '（Databricks 以外、または取得できません）'}")

    say("")
    say("== 行ごとのチェック（Spark 式。参照データは使いません）==")
    from pyspark.sql import functions as F

    from . import expressions as E

    for label, function_name, value, expected in _CASES:
        try:
            frame = session.range(1).select(F.lit(value).alias("v"))
            problem = getattr(E, function_name)(F.col("v"))
            actual = frame.select(problem.alias("p")).collect()[0]["p"]
        except Exception as exc:  # noqa: BLE001
            ok = False
            say(f"[NG] {label}: 実行できません: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}")
            continue
        if actual == expected:
            say(f"[OK] {label}")
        else:
            ok = False
            say(f"[NG] {label}: 期待 {expected!r}、実際 {actual!r}")

    say("")
    say("== DQX（YAML から呼ぶ場合）==")
    try:
        from .checks import CHECK_FUNCTIONS

        say(f"[OK] DQX のカスタムチェックを読み込めました（{len(CHECK_FUNCTIONS)} 関数）")
    except Exception as exc:  # noqa: BLE001
        ok = False
        say(f"[NG] DQX のカスタムチェックを読み込めません: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}")

    say("")
    if ok:
        say("結果: すべて期待どおりです。")
    else:
        say("結果: 期待と異なる項目があります。上の「環境」と [NG] の行を、そのまま報告してください。")
    _emit(lines, quiet)
    return ok


def _emit(lines: list[str], quiet: bool) -> None:
    if not quiet:
        print("\n".join(lines))
