"""Reference validators.  Expected values come from official material (see docs/sources.md):

* NTA  "チェックデジットの計算" (houjin-bangou.nta.go.jp/documents/checkdigit.pdf)
* MIC  "全国地方公共団体コード仕様" (soumu.go.jp/main_content/000137948.pdf)
"""

import pytest

from jpdq_oga import validators as v

# --- 法人番号 -----------------------------------------------------------------------------------


def test_corporate_number_nta_worked_example():
    # NTA: base number 700110005901, odd-position sum 11, even-position sum 13 -> 13*2 + 11 = 37,
    # 37 mod 9 = 1, 9 - 1 = 8, so the corporate number is 8700110005901.
    assert v.corporate_number_check_digit("700110005901") == 8
    assert v.corporate_number_problem("8700110005901") is None


def test_corporate_number_nta_own_number():
    # printed on NTA publications as "国税庁 法人番号 7000012050002"
    assert v.corporate_number_problem("7000012050002") is None


@pytest.mark.parametrize("wrong_digit", [d for d in "012345679"])
def test_corporate_number_every_wrong_check_digit_is_detected(wrong_digit):
    assert v.corporate_number_problem(wrong_digit + "700110005901") == v.CHECK_DIGIT


@pytest.mark.parametrize(
    "value",
    [
        "870011000590",  # 12 digits
        "87001100059011",  # 14 digits
        "８７００１１０００５９０１",  # fullwidth digits are reported, not converted
        " 8700110005901",  # surrounding whitespace
        "8700110005901\n",
        "8700-1100-05901",
        "T8700110005901",
        "87001100059O1",  # letter O
        "",
    ],
)
def test_corporate_number_format_violations(value):
    assert v.corporate_number_problem(value) == v.FORMAT


def test_corporate_number_null_is_not_a_violation():
    assert v.corporate_number_problem(None) is None
    assert v.is_valid_corporate_number(None) is None


def test_corporate_number_check_digit_range_boundaries():
    # 9 - (sum mod 9): remainder 0 gives 9, remainder 8 gives 1; the digit is never 0
    assert v.corporate_number_check_digit("000000000000") == 9  # sum 0, remainder 0
    assert v.corporate_number_check_digit("000000000008") == 1  # sum 8, remainder 8
    assert v.corporate_number_check_digit("000000000009") == 9  # sum 9, remainder 0


# --- インボイス登録番号 ----------------------------------------------------------------------------


def test_invoice_number_is_t_plus_valid_corporate_number():
    assert v.invoice_number_problem("T8700110005901") is None
    assert v.invoice_number_problem("T7000012050002") is None


@pytest.mark.parametrize(
    "value",
    [
        "8700110005901",  # no T
        "t8700110005901",  # lowercase t
        "Ｔ8700110005901",  # fullwidth T
        "T-8700110005901",
        "T870011000590",  # 12 digits
        "TT8700110005901",
        " T8700110005901",
        "",
    ],
)
def test_invoice_number_format_violations(value):
    assert v.invoice_number_problem(value) == v.FORMAT


def test_invoice_number_check_digit_violation():
    assert v.invoice_number_problem("T1700110005901") == v.CHECK_DIGIT


def test_invoice_number_null():
    assert v.invoice_number_problem(None) is None


# --- 全国地方公共団体コード -------------------------------------------------------------------------


def test_local_gov_code_mic_worked_example():
    # MIC: 1 6 2 0 1 x 6 5 4 3 2 = 6 + 30 + 8 + 0 + 2 = 46, 46 mod 11 = 2, 11 - 2 = 9
    assert v.local_gov_code_check_digit("16201") == 9
    assert v.local_gov_code_problem("162019") is None


@pytest.mark.parametrize(
    "code5,remainder,expected",
    [
        ("00031", 0, 1),  # sum 11 -> remainder 0 -> check digit 1   (MIC note 1)
        ("00040", 1, 0),  # sum 12 -> remainder 1 -> check digit 0   (MIC note 2)
        ("00022", 10, 1),  # sum 10 -> remainder 10 -> check digit 1  (MIC note 3)
    ],
)
def test_local_gov_code_mic_notes(code5, remainder, expected):
    total = sum(int(c) * w for c, w in zip(code5, (6, 5, 4, 3, 2), strict=True))
    assert total % 11 == remainder
    assert v.local_gov_code_check_digit(code5) == expected


def test_local_gov_code_violations():
    assert v.local_gov_code_problem("162010") == v.CHECK_DIGIT
    assert v.local_gov_code_problem("16201") == v.FORMAT
    assert v.local_gov_code_problem("1620190") == v.FORMAT
    assert v.local_gov_code_problem("１６２０１９") == v.FORMAT
    assert v.local_gov_code_problem("16201A") == v.FORMAT
    assert v.local_gov_code_problem(None) is None


@pytest.mark.parametrize("prefix", ["00", "48", "99"])
def test_local_gov_code_prefecture_out_of_range(prefix):
    code5 = prefix + "000"
    code = code5 + str(v.local_gov_code_check_digit(code5))  # right check digit, wrong prefecture
    assert v.local_gov_code_problem(code) == v.PREFECTURE


def test_local_gov_code_prefecture_range_is_01_to_47():
    for n in range(1, 48):
        code5 = f"{n:02d}000"
        assert v.local_gov_code_problem(code5 + str(v.local_gov_code_check_digit(code5))) is None
