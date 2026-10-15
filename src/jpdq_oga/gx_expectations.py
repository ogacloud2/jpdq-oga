"""Great Expectations (GX 1.x) expectations for Japan-specific data.

The same checks as the DQX ones, as custom ``ColumnMapExpectation`` classes.  Each one has two implementations:

* **Spark** (``SparkDFExecutionEngine``): the Spark-native expressions of :mod:`jpdq_oga.expressions`
  (no Python UDFs), so the work stays inside Spark.
* **pandas** (``PandasExecutionEngine``): the pure-Python reference of :mod:`jpdq_oga.validators`,
  :mod:`jpdq_oga.wareki` and :mod:`jpdq_oga.charsets` (the very functions the Spark expressions are tested
  against).

Importing this module registers the expectations::

    from jpdq_oga.gx_expectations import ExpectColumnValuesToBeValidJpCorporateNumber

    batch.validate(ExpectColumnValuesToBeValidJpCorporateNumber(column="corporate_number"))

``NULL`` values are not evaluated (GX leaves them out of map expectations); use
``expect_column_values_to_not_be_null`` for mandatory columns.  The reference-data (dataset-level) checks of the
DQX flavour are not part of this adapter.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

try:
    from great_expectations.compatibility import pydantic
    from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
    from great_expectations.expectations.expectation import ColumnMapExpectation
    from great_expectations.expectations.metrics import ColumnMapMetricProvider, column_condition_partial
except ImportError as exc:  # pragma: no cover - exercised only without GX
    raise ImportError(
        f"jpdq_oga.gx_expectations needs Great Expectations 1.x ({exc}). Install it with: pip install 'jpdq-oga[gx]'"
    ) from exc

from . import charsets as _charsets
from . import expressions as E
from . import validators as V
from .wareki import wareki_problem

# --- shared helpers --------------------------------------------------------------------------------------


def _is_fine(problem: Callable[[str | None], str | None], value: Any) -> bool:
    """pandas side: ``True`` when the reference validator has no complaint (missing values pass)."""
    if value is None or value != value:  # None / NaN
        return True
    return problem(str(value)) is None


# --- 法人番号 ----------------------------------------------------------------------------------------------


class ColumnValuesAreValidJpCorporateNumber(ColumnMapMetricProvider):
    condition_metric_name = "column_values.are_valid_jp_corporate_number"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.map(lambda v: _is_fine(V.corporate_number_problem, v))

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return E.corporate_number_problem(column).isNull()


class ExpectColumnValuesToBeValidJpCorporateNumber(ColumnMapExpectation):
    """Expect each value to be a valid 13-digit 法人番号 (corporate number), check digit included."""

    map_metric = "column_values.are_valid_jp_corporate_number"
    success_keys = ("mostly",)
    default_kwarg_values = {"mostly": 1.0}


# --- インボイス登録番号 --------------------------------------------------------------------------------------


class ColumnValuesAreValidJpInvoiceNumber(ColumnMapMetricProvider):
    condition_metric_name = "column_values.are_valid_jp_invoice_number"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.map(lambda v: _is_fine(V.invoice_number_problem, v))

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return E.invoice_number_problem(column).isNull()


class ExpectColumnValuesToBeValidJpInvoiceNumber(ColumnMapExpectation):
    """Expect each value to be ``T`` + a valid 法人番号 (invoice registration number of a corporation)."""

    map_metric = "column_values.are_valid_jp_invoice_number"
    success_keys = ("mostly",)
    default_kwarg_values = {"mostly": 1.0}


# --- 全国地方公共団体コード ----------------------------------------------------------------------------------


class ColumnValuesAreValidJpLocalGovCode(ColumnMapMetricProvider):
    condition_metric_name = "column_values.are_valid_jp_local_gov_code"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.map(lambda v: _is_fine(V.local_gov_code_problem, v))

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return E.local_gov_code_problem(column).isNull()


class ExpectColumnValuesToBeValidJpLocalGovCode(ColumnMapExpectation):
    """Expect each value to be a valid 6-digit 全国地方公共団体コード (prefecture 01-47, check digit)."""

    map_metric = "column_values.are_valid_jp_local_gov_code"
    success_keys = ("mostly",)
    default_kwarg_values = {"mostly": 1.0}


# --- 和暦 ---------------------------------------------------------------------------------------------------


class ColumnValuesAreValidJpWarekiDate(ColumnMapMetricProvider):
    condition_metric_name = "column_values.are_valid_jp_wareki_date"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.map(lambda v: _is_fine(wareki_problem, v))

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return E.wareki_problem(column).isNull()


class ExpectColumnValuesToBeValidJpWarekiDate(ColumnMapExpectation):
    """Expect each value to be a real Japanese-era date within the stated era (e.g. 令和6年4月1日)."""

    map_metric = "column_values.are_valid_jp_wareki_date"
    success_keys = ("mostly",)
    default_kwarg_values = {"mostly": 1.0}


# --- 文字種 -------------------------------------------------------------------------------------------------


class ColumnValuesHaveNoJpHalfwidthKana(ColumnMapMetricProvider):
    condition_metric_name = "column_values.have_no_jp_halfwidth_kana"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.map(lambda v: _is_fine(_charsets.halfwidth_kana_problem, v))

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return E.halfwidth_kana_problem(column).isNull()


class ExpectColumnValuesToHaveNoJpHalfwidthKana(ColumnMapExpectation):
    """Expect no value to contain half-width katakana (ｱｲｳ)."""

    map_metric = "column_values.have_no_jp_halfwidth_kana"
    success_keys = ("mostly",)
    default_kwarg_values = {"mostly": 1.0}


class ColumnValuesHaveNoJpPlatformDependentChars(ColumnMapMetricProvider):
    condition_metric_name = "column_values.have_no_jp_platform_dependent_chars"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.map(lambda v: _is_fine(_charsets.platform_dependent_problem, v))

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return E.platform_dependent_problem(column).isNull()


class ExpectColumnValuesToHaveNoJpPlatformDependentChars(ColumnMapExpectation):
    """Expect no value to contain 機種依存文字: characters cp932 encodes but shift_jis (JIS X 0208) does not."""

    map_metric = "column_values.have_no_jp_platform_dependent_chars"
    success_keys = ("mostly",)
    default_kwarg_values = {"mostly": 1.0}


class ColumnValuesAreEncodableInJpCharset(ColumnMapMetricProvider):
    condition_metric_name = "column_values.are_encodable_in_jp_charset"
    condition_value_keys = ("charset",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, charset="cp932", **kwargs):
        _charsets.validate_charset(charset)
        return column.map(lambda v: _is_fine(lambda s: _charsets.unencodable_problem(s, charset), v))

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, charset="cp932", **kwargs):
        return E.unencodable_problem(column, _charsets.validate_charset(charset)).isNull()


class ExpectColumnValuesToBeEncodableInJpCharset(ColumnMapExpectation):
    """Expect every character of each value to be encodable in ``charset`` (``cp932`` or ``shift_jis``)."""

    charset: str = "cp932"

    @pydantic.validator("charset")
    def _supported_charset(cls, value: str) -> str:
        return _charsets.validate_charset(value)

    map_metric = "column_values.are_encodable_in_jp_charset"
    success_keys = ("charset", "mostly")
    default_kwarg_values = {"charset": "cp932", "mostly": 1.0}


EXPECTATIONS = (
    ExpectColumnValuesToBeValidJpCorporateNumber,
    ExpectColumnValuesToBeValidJpInvoiceNumber,
    ExpectColumnValuesToBeValidJpLocalGovCode,
    ExpectColumnValuesToBeValidJpWarekiDate,
    ExpectColumnValuesToHaveNoJpHalfwidthKana,
    ExpectColumnValuesToHaveNoJpPlatformDependentChars,
    ExpectColumnValuesToBeEncodableInJpCharset,
)

__all__ = [cls.__name__ for cls in EXPECTATIONS] + ["EXPECTATIONS"]
