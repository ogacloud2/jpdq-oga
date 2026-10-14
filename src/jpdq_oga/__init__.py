"""jpdq-oga: DQX checks for Japan-specific data.

``from jpdq_oga import get_japan_checks`` returns the DQX check functions for use with
``apply_checks_by_metadata(..., custom_check_functions=get_japan_checks())``.  The pure-Python
reference validators live in :mod:`jpdq_oga.validators` and need no dependencies; the Spark
expressions are in :mod:`jpdq_oga.expressions`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .validators import (
    corporate_number_check_digit,
    is_valid_corporate_number,
    is_valid_invoice_number,
    is_valid_local_gov_code,
    local_gov_code_check_digit,
)
from .wareki import is_valid_wareki

__version__ = "0.1.0"


def get_japan_checks() -> dict[str, Callable[..., Any]]:
    """DQX custom check functions by name (imports DQX and PySpark on first use)."""
    from .checks import CHECK_FUNCTIONS

    return dict(CHECK_FUNCTIONS)


__all__ = [
    "__version__",
    "corporate_number_check_digit",
    "get_japan_checks",
    "is_valid_corporate_number",
    "is_valid_invoice_number",
    "is_valid_local_gov_code",
    "is_valid_wareki",
    "local_gov_code_check_digit",
]
