# Databricks notebook source
# MAGIC %md
# MAGIC # jpdq-oga を Databricks Free Edition で試す
# MAGIC
# MAGIC 手順書: `docs/databricks-free-edition.md`。上から順に、セルを **1つずつ** 実行してください（Shift+Enter）。
# MAGIC
# MAGIC | 部 | 内容 | 追加の準備 |
# MAGIC | --- | --- | --- |
# MAGIC | 0 | ライブラリのインストール | GitHub の ZIP を Volume に置く |
# MAGIC | A | 番号・日付のチェック（法人番号、インボイス、団体コード、和暦） | なし |
# MAGIC | B | 参照データのチェック（郵便番号、時点指定）。小さな表を自分で作る | なし |
# MAGIC | C | （任意）日本郵便の実データで郵便番号チェック | `utf_ken_all.zip` を Volume に置く |
# MAGIC | D | （任意）総務省の Excel から地方公共団体コードの履歴を作る | Excel 2 つを Volume に置く |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. インストール
# MAGIC
# MAGIC 下のセルの `/Volumes/workspace/default/jpdq/jpdq-oga-main.zip` は、手順書の「Volume に ZIP を置く」で作った場所です。
# MAGIC カタログ名・スキーマ名・Volume 名が違う場合は、**このパスを書き換えて**ください。

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx==0.16.0 openpyxl /Volumes/workspace/default/jpdq/jpdq-oga-main.zip

# COMMAND ----------

# インストールしたライブラリを読み込むために Python を再起動します（実行後、状態がリセットされます）
dbutils.library.restartPython()

# COMMAND ----------

import jpdq_oga

print("jpdq-oga", jpdq_oga.__version__)
print("使えるチェック:", sorted(jpdq_oga.get_japan_checks()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. 番号・日付のチェック
# MAGIC
# MAGIC 4 行のサンプルを作ります。1 行目はすべて正しい値、2・3 行目はわざと間違えた値、4 行目はすべて NULL です。
# MAGIC （NULL は違反になりません。必須項目は DQX の `is_not_null` と組み合わせます）

# COMMAND ----------

data = [
    ("1", "8700110005901", "T8700110005901", "162019", "令和6年4月1日"),  # すべて正しい（国税庁・総務省の公式の例）
    ("2", "1234567890123", "T1700110005901", "162010", "平成31年5月1日"),  # チェックディジット違反・元号の境界違反
    ("3", "870011000590", "8700110005901", "480008", "令和6年2月30日"),  # 桁数違反・Tなし・県コード違反・存在しない日
    ("4", None, None, None, None),  # NULL
]
df = spark.createDataFrame(
    data, "id string, corporate_number string, invoice_number string, municipality_code string, contract_date string"
)
display(df)

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

from jpdq_oga import get_japan_checks

# DQX のチェック定義（YAML）。lang を省略すると日本語、"en" で英語のメッセージになります
checks = yaml.safe_load(
    """
- name: corporate_number
  criticality: error
  check:
    function: is_valid_jp_corporate_number
    arguments: {column: corporate_number}
- name: invoice_number
  criticality: error
  check:
    function: is_valid_jp_invoice_number
    arguments: {column: invoice_number}
- name: municipality_code
  criticality: error
  check:
    function: is_valid_jp_local_gov_code
    arguments: {column: municipality_code}
- name: contract_date
  criticality: error
  check:
    function: is_valid_jp_wareki_date
    arguments: {column: contract_date}
"""
)

engine = DQEngine(WorkspaceClient())
good_df, bad_df = engine.apply_checks_by_metadata_and_split(df, checks, custom_check_functions=get_japan_checks())

print("問題のない行:")
display(good_df.select("id", "corporate_number", "invoice_number", "municipality_code", "contract_date"))

# COMMAND ----------

# 問題のある行と、何が悪いのかのメッセージ
messages = (
    bad_df.select("id", F.explode("_errors").alias("e"))
    .select("id", F.col("e.name").alias("ルール"), F.col("e.message").alias("メッセージ"))
    .orderBy("id", "ルール")
)
display(messages)

# COMMAND ----------

# MAGIC %md
# MAGIC **確認ポイント**
# MAGIC - 行 1 と行 4（NULL）が「問題のない行」、行 2 と行 3 が違反として出ます。
# MAGIC - 行 2 は `平成31年5月1日`（実際は令和元年）を元号の境界違反として検出します。正規表現では検出できない種類の間違いです。
# MAGIC - 行 3 の `令和6年2月30日` は「存在しない日付」です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. 参照データのチェック（郵便番号と時点指定）
# MAGIC
# MAGIC 実際のデータを取り込む前に、小さな参照表を自分で作って仕組みを確認します。参照表の各行には**有効期間**
# MAGIC （`valid_from` 以上、`valid_to` 未満）があります。

# COMMAND ----------

from datetime import date

postal_ref = spark.createDataFrame(
    [
        ("1000001", "13101", "東京都", "千代田区", None, None),  # ずっと有効
        ("0600000", "01101", "北海道", "札幌市中央区", None, None),
        ("9999991", "13101", "東京都", "千代田区", date(2025, 4, 1), None),  # 2025-04-01 に新設
        ("9999992", "13101", "東京都", "千代田区", None, date(2024, 4, 1)),  # 2024-04-01 に廃止
    ],
    "postal_code string, local_gov_code string, prefecture string, city string, valid_from date, valid_to date",
)

orders = spark.createDataFrame(
    [
        ("1", "100-0001", "東京都"),  # 正しい
        ("2", "1000001", "大阪府"),  # 都道府県が違う
        ("3", "9999991", "東京都"),  # 2025-04-01 以降だけ存在
        ("4", "9999992", "東京都"),  # 2024-04-01 に廃止
        ("5", "123-4567", "東京都"),  # 存在しない
    ],
    "id string, postal_code string, prefecture string",
)


def check_postal(as_of: str):
    """指定した日付時点で、郵便番号の実在性と都道府県との整合を調べる"""
    rules = yaml.safe_load(
        f"""
- name: postal_exists
  criticality: error
  check:
    function: is_existing_jp_postal_code
    arguments: {{column: postal_code, ref_df_name: postal, as_of: "{as_of}"}}
- name: postal_matches_prefecture
  criticality: error
  check:
    function: is_consistent_jp_postal_code_and_prefecture
    arguments: {{column: postal_code, prefecture_column: prefecture, ref_df_name: postal, as_of: "{as_of}"}}
"""
    )
    result = engine.apply_checks_by_metadata(
        orders, rules, custom_check_functions=get_japan_checks(), ref_dfs={"postal": postal_ref}
    )
    return (
        result.select("id", "postal_code", "prefecture", F.explode_outer("_errors").alias("e"))
        .select("id", "postal_code", "prefecture", F.col("e.message").alias("メッセージ"))
        .orderBy("id")
    )


print("2026-01-01 時点:")
display(check_postal("2026-01-01"))

# COMMAND ----------

print("2024-03-31 時点（9999992 はまだ存在し、9999991 はまだ存在しない）:")
display(check_postal("2024-03-31"))

# COMMAND ----------

# MAGIC %md
# MAGIC **確認ポイント**
# MAGIC - 同じデータでも、`as_of`（基準日）を変えると結果が変わります。廃止された郵便番号を「その日までは正しかった」と判定できます。
# MAGIC - 行 1 の `100-0001`（ハイフン付き）も正しいと判定されます。
# MAGIC - 行 2 は、郵便番号は存在するが都道府県が一致しない例です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.（任意）日本郵便の実データで郵便番号をチェックする
# MAGIC
# MAGIC 手順書の「実データで試す」で `utf_ken_all.zip` を Volume に置いてから実行します。置いていない場合はスキップしてください。

# COMMAND ----------

from datetime import date
from pathlib import Path

from jpdq_oga import cli

VOLUME = Path("/Volumes/workspace/default/jpdq")  # 手順書で作った Volume
ZIP = VOLUME / "utf_ken_all.zip"
TABLE = "workspace.default.postal_codes"  # 作成する表（カタログ.スキーマ.表名）

# ZIP を Volume の作業フォルダに展開して読み込み、有効期間付きの表にする（初回なので valid_from は NULL）
postal_codes = cli.build_postal(spark, ZIP, VOLUME / "work", "utf-8", None, date.today())
postal_codes.write.mode("overwrite").saveAsTable(TABLE)
print(TABLE, "に", spark.table(TABLE).count(), "行を書き込みました")

# COMMAND ----------

real_orders = spark.createDataFrame(
    [
        ("1", "100-0001", "東京都", "千代田区"),
        ("2", "100-0001", "大阪府", "千代田区"),
        ("3", "000-0000", "東京都", "千代田区"),
    ],
    "id string, postal_code string, prefecture string, city string",
)
real_checks = yaml.safe_load(
    f"""
- name: postal_exists
  criticality: error
  check:
    function: is_existing_jp_postal_code
    arguments: {{column: postal_code, ref_table: {TABLE}}}
- name: postal_matches_prefecture
  criticality: error
  check:
    function: is_consistent_jp_postal_code_and_prefecture
    arguments: {{column: postal_code, prefecture_column: prefecture, ref_table: {TABLE}}}
- name: postal_matches_city
  criticality: error
  check:
    function: is_consistent_jp_postal_code_and_city
    arguments: {{column: postal_code, city_column: city, ref_table: {TABLE}}}
"""
)
out = engine.apply_checks_by_metadata(real_orders, real_checks, custom_check_functions=get_japan_checks())
display(
    out.select("id", "postal_code", "prefecture", "city", F.explode_outer("_errors").alias("e")).select(
        "id", "postal_code", "prefecture", "city", F.col("e.message").alias("メッセージ")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.（任意）総務省の Excel から地方公共団体コードの履歴を作る
# MAGIC
# MAGIC 手順書の「総務省の Excel」で 2 つの Excel を Volume に置いてから実行します。

# COMMAND ----------

from pathlib import Path

from jpdq_oga import cli

VOLUME = Path("/Volumes/workspace/default/jpdq")
CURRENT = str(VOLUME / "codes_current.xlsx")  # 「都道府県コード及び市区町村コード」
REVISIONS = str(VOLUME / "codes_revisions.xlsx")  # 「…の改正一覧表（平成17年4月1日以降）」
CODES_TABLE = "workspace.default.local_gov_codes"

codes, notes = cli.build_local_gov_codes_from_workbooks(spark, CURRENT, REVISIONS)
codes.write.mode("overwrite").saveAsTable(CODES_TABLE)
print(notes)
display(spark.table(CODES_TABLE).filter("valid_to is not null").orderBy(F.desc("valid_to")).limit(10))
