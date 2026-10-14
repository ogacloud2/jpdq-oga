"""The benchmark's pandas baselines must compute the same answers as the package (no Spark involved).

A baseline that is fast because it is wrong would make the benchmark meaningless; the benchmark itself
also compares counts across implementations before timing.
"""

import importlib.util
import random
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("numpy")
pytest.importorskip("pyspark")
pytest.importorskip("stdnum")

from jpdq_oga import validators as V  # noqa: E402
from jpdq_oga import wareki as W  # noqa: E402

_PATH = Path(__file__).parent.parent / "benchmarks" / "run_benchmark.py"
_spec = importlib.util.spec_from_file_location("run_benchmark", _PATH)
bench = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(bench)


def corporate_values():
    rng = random.Random(7)
    values = ["8700110005901", "7000012050002", "1234567890123", "870011000590", "", "abc", None, "0000000000000"]
    values += ["".join(rng.choice("0123456789") for _ in range(13)) for _ in range(2000)]
    # valid ones, so that the True branch is exercised
    for _ in range(300):
        base = "".join(rng.choice("0123456789") for _ in range(12))
        values.append(f"{V.corporate_number_check_digit(base)}{base}")
    values += [None, None, "9" * 12]
    return values


def normalise(series):
    return [None if pd.isna(v) else bool(v) for v in series.tolist()]


def as_series(values, kind):
    """The three ways missing values reach a pandas UDF: None in object, NaN in object, <NA> in string dtype."""
    if kind == "string":
        return pd.Series(values, dtype="string")
    if kind == "nan":
        return pd.Series([float("nan") if v is None else v for v in values], dtype="object")
    return pd.Series(values, dtype="object")


@pytest.mark.parametrize("kind", ["none", "nan", "string"])
@pytest.mark.parametrize("function", ["corporate_stdnum_py", "corporate_reference_py", "corporate_vectorized_py"])
def test_corporate_baselines_agree_with_the_package(function, kind):
    values = corporate_values()
    got = normalise(getattr(bench, function)(as_series(values, kind)))
    expected = [V.is_valid_corporate_number(v) for v in values]
    assert got == expected
    assert True in expected and False in expected and None in expected


@pytest.mark.parametrize("kind", ["none", "nan", "string"])
def test_local_gov_and_wareki_baselines(kind):
    codes = ["162019", "162010", "010006", "480000", "16201", None, "abc", "130001"]
    assert normalise(bench.local_gov_reference_py(as_series(codes, kind))) == [
        V.is_valid_local_gov_code(c) for c in codes
    ]
    dates = ["令和6年4月1日", "平成31年5月1日", "R6.4.1", "明治5年12月31日", None, "2024-04-01", "令和6年2月30日"]
    assert normalise(bench.wareki_reference_py(as_series(dates, kind))) == [W.is_valid_wareki(d) for d in dates]


def test_postal_reference_and_set_agree():
    current = bench.current_postal_codes()
    assert len(current) == bench.POSTAL_REFERENCE_ROWS // 2
    assert "0000083" in current  # id 1 -> odd -> still in force
    assert "0000000" not in current  # id 0 -> even -> retired on 2025-04-01
    assert all(len(c) == 7 for c in current)


def test_generated_wareki_samples_cover_valid_and_invalid():
    samples = bench._wareki_samples()
    outcomes = {W.wareki_problem(s) for s in samples}
    assert None in outcomes and len(outcomes) > 3
