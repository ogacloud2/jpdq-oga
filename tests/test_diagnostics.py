import jpdq_oga
from jpdq_oga import diagnostics


def test_selfcheck_passes_and_reports_the_environment(spark, capsys):
    assert jpdq_oga.selfcheck(spark) is True
    report = capsys.readouterr().out
    assert "== 環境 ==" in report
    assert f"jpdq-oga           : {jpdq_oga.__version__}" in report
    assert f"Spark              : {spark.version}" in report
    assert "[OK] 法人番号: 正しい番号" in report
    assert "[OK] 和暦: 令和元年4月30日（元号の期間外）" in report
    assert "[NG]" not in report
    assert "すべて期待どおりです" in report


def test_selfcheck_is_quiet_on_request(spark, capsys):
    assert jpdq_oga.selfcheck(spark, quiet=True) is True
    assert capsys.readouterr().out == ""


def test_selfcheck_names_the_case_that_differs(spark, capsys, monkeypatch):
    monkeypatch.setattr(
        diagnostics, "_CASES", [("法人番号: わざと誤った期待値", "corporate_number_problem", "8700110005901", "format")]
    )
    assert jpdq_oga.selfcheck(spark) is False
    report = capsys.readouterr().out
    assert "[NG] 法人番号: わざと誤った期待値: 期待 'format'、実際 None" in report
    assert "報告してください" in report


def test_selfcheck_survives_a_missing_session(capsys):
    class Broken:
        @property
        def version(self):
            raise RuntimeError("no session")

    assert jpdq_oga.selfcheck(Broken()) is False
    assert "[NG] Spark のセッションを取得できません" in capsys.readouterr().out
