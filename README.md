# jpdq-oga

[Databricks Labs DQX](https://github.com/databrickslabs/dqx) 向けの、**日本固有データの検証**を行うチェック集です。
正規表現では書けない（書くと間違える）検証 — チェックディジット、実在性、項目間の整合、存在しない日付 — を、
DQX の YAML から1行で呼べるようにします。

> DQX check functions for Japan-specific data: corporate-number / invoice-number / local-government-code
> check digits, Japanese-era (和暦) dates, and postal-code / local-government-code existence and consistency
> *as of a date* (time-aware reference data). Spark-native (no Python UDFs); Japanese or English violation messages.

**状態: alpha (v0.1.0 開発中)。実装済みは下の「実装済み」表のとおりで、他はロードマップです。**

## 設計の原則

1. **形式ではなく正しさを検証する。** 正規表現で済むチェックは提供しません。
2. **Spark ネイティブ。** Python UDF / pandas UDF を使わず、組み込み関数だけで実装します。テストで実行計画に
   `PythonUDF` / `BatchEvalPython` / `ArrowEvalPython` が含まれないことを確認しています。
3. **変換ではなく検出。** 値を直さず、違反を検出して DQX の警告・隔離に載せます。全角数字や前後の空白は
   「直して通す」のではなく、形式違反として報告します。
4. **業務担当者が読めるメッセージ。** 既定は日本語、`lang: en` で英語。
5. **出典を残す。** 計算式・境界日は公的資料で確認したものだけを実装し、[docs/sources.md](docs/sources.md) に記録します。

## 実装済み

| チェック関数 | 内容 | 出典 |
| --- | --- | --- |
| `is_valid_jp_corporate_number` | 法人番号（13桁）。先頭1桁が検査用数字 | [国税庁](https://www.houjin-bangou.nta.go.jp/documents/checkdigit.pdf) |
| `is_valid_jp_invoice_number` | インボイス登録番号（法人）: `T` + 有効な法人番号 | 同上 |
| `is_valid_jp_local_gov_code` | 全国地方公共団体コード（6桁）。都道府県 01–47、検査数字 | [総務省](https://www.soumu.go.jp/main_content/000137948.pdf) |
| `is_existing_jp_postal_code` | 郵便番号が日本郵便のデータに**指定時点で**実在するか（ハイフン有無の両対応） | [日本郵便](http://www.post.japanpost.jp/service/search/zipcode/download/readme.html) |
| `is_consistent_jp_postal_code_and_prefecture` | 郵便番号と都道府県の整合 | 同上 |
| `is_consistent_jp_postal_code_and_city` | 郵便番号と市区町村の整合（複数の市区町村にまたがる番号に対応） | 同上 |
| `is_existing_jp_local_gov_code` | 地方公共団体コードが指定時点で実在するか（合併等で廃止されたコードの検出） | [総務省](https://www.soumu.go.jp/denshijiti/code.html) |
| `is_valid_jp_wareki_date` | 和暦日付が実在し、元号の期間内か。元年・漢数字・全角・略号（R6.4.1）に対応 | [国立天文台](https://eco.mtk.nao.ac.jp/koyomi/wiki/CEF2BBCB2FB8B5B9E6.html)、[内閣府](https://www.cao.go.jp/others/soumu/gengou/) |

### 正規表現では通ってしまうが、検出するもの

| 入力 | `^\d{13}$` 等の正規表現 | 本パッケージ |
| --- | --- | --- |
| 法人番号 `1234567890123`（検査用数字が不一致） | 通る | 違反: 法人番号のチェックディジットが一致しません |
| インボイス `T1700110005901`（法人番号部分の検査用数字が不一致） | 通る | 違反: チェックディジットが一致しません |
| 団体コード `162010`（検査数字が不一致） | 通る | 違反: 検査数字が一致しません |
| 団体コード `480008`（先頭2桁が 47 を超える） | 通る | 違反: 都道府県コード（01〜47）ではありません |
| 郵便番号 `1000002` / `100-0001` を都道府県 `大阪府` と組み合わせ | 通る | 違反: 郵便番号と都道府県が一致しません |
| 郵便番号 `9999991`（指定時点では未開設） | 通る | 違反: 郵便番号が日本郵便のデータに存在しません（基準日: …） |
| 団体コード `012343`（合併で廃止） | 通る | 違反: 合併などで廃止されたコードの可能性があります |
| 和暦 `令和元年4月30日`（令和は5月1日から） | 通る | 違反: 元号の期間外の日付です |
| 和暦 `平成31年5月1日`（すでに令和元年） | 通る | 違反: 元号の期間外の日付です |
| 和暦 `令和6年2月30日` / `昭和57年2月29日`（平年） | 通る | 違反: 存在しない日付です |
| 和暦 `明治5年12月31日`（旧暦） | 通る | 違反: 明治5年以前は旧暦のため検証できません |

## 使い方

### YAML（DQX のメタデータ）

```python
import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from jpdq_oga import get_japan_checks

checks = yaml.safe_load(open("examples/dqx_checks.yaml", encoding="utf-8"))
engine = DQEngine(WorkspaceClient())
good, bad = engine.apply_checks_by_metadata_and_split(df, checks, custom_check_functions=get_japan_checks())
```

```yaml
- criticality: error
  check:
    function: is_valid_jp_corporate_number
    arguments: {column: corporate_number}      # 既定は日本語メッセージ
- criticality: warn
  check:
    function: is_valid_jp_invoice_number
    arguments: {column: invoice_number, lang: en}
```

### Python

```python
from databricks.labs.dqx.rule import DQRowRule
from jpdq_oga.checks import is_valid_jp_corporate_number

rule = DQRowRule(criticality="error", check_func=is_valid_jp_corporate_number, column="corporate_number")
```

メッセージ例: `法人番号のチェックディジットが一致しません（入力値: 1234567890123）`

### 参照データ（郵便番号・地方公共団体コード）と時点指定

参照データは**時点付きの表**（各行に `valid_from` / `valid_to`）として持ち、`as_of` で「いつ時点で検証するか」を
指定します。古い CSV を抱えたまま検証する問題を避けるため、データは同梱せず、取得 CLI で作ります。

```bash
# 日本郵便の utf_ken_all.zip（ブラウザで取得）を取り込み、時点付きの表を作る
python -m jpdq_oga.cli postal --zip utf_ken_all.zip --snapshot-date 2026-10-01 --output-table main.reference.postal_codes

# 翌月: 新しいスナップショットを既存の表に重ねる（履歴が残る）
python -m jpdq_oga.cli postal --zip utf_ken_all.zip --snapshot-date 2026-11-01 \
    --existing-table main.reference.postal_codes --output-table main.reference.postal_codes
```

```yaml
- criticality: error
  check:
    function: is_existing_jp_postal_code
    arguments:
      column: postal_code
      ref_table: main.reference.postal_codes   # Unity Catalog のテーブル。ref_path（Volume 等）、ref_df_name も可
      as_of: "2026-04-01"                       # 固定日。as_of_column: contract_date で行ごとの日付も指定可
```

```bash
# 地方公共団体コード: 総務省の2つの Excel（現行コード表と改正一覧表）から、廃止日付きの表を作る
pip install "jpdq-oga[excel]"
python -m jpdq_oga.cli local-gov-codes-xlsx --current codes.xlsx --revisions revisions.xlsx \
    --output-table main.reference.local_gov_codes
```

全体の例: [examples/dqx_reference_checks.yaml](examples/dqx_reference_checks.yaml)。結合は参照表をキーごとに1行へ
集約したうえでの broadcast join で、入力の行数は変わりません。

### 依存なしの検証関数・Spark 式

```python
from jpdq_oga import is_valid_corporate_number, is_valid_local_gov_code, is_valid_wareki

is_valid_corporate_number("8700110005901")  # True（国税庁の計算例）
is_valid_local_gov_code("162019")  # True（総務省の算出例）
is_valid_wareki("令和元年4月30日")  # False（令和は5月1日から）
```

Spark の列式は `jpdq_oga.expressions`（`corporate_number_problem` など。違反理由コード
`format` / `check_digit` / `prefecture` / `not_exist` / `era_boundary` / `pre_gregorian` または `NULL` を返す）。

## ベンチマーク

GitHub Actions（2 vCPU、`local[*]`）で 1,000万行の合成データを処理した時間です。詳細・環境・限界は
[BENCHMARK.md](BENCHMARK.md)、再現手順は [benchmarks/](benchmarks/README.md)。実装間の件数一致を確認したうえでの計測です。

| チェック | native（本パッケージ） | pandas UDF（python-stdnum） | pandas UDF（参照実装） | pandas UDF（numpy ベクトル化） |
| --- | ---: | ---: | ---: | ---: |
| 法人番号 | 6.9 秒 | 29.1 秒 | 12.6 秒 | **5.2 秒** |
| 地方公共団体コード | **2.6 秒** | — | 11.4 秒 | — |
| 和暦 | 37.7 秒 | — | 38.6 秒 | — |
| 郵便番号の実在性（参照12万行） | 12.0 秒 | — | **3.7 秒**（`set`） | — |

- **UDF なしで DQX の YAML から使える**ことが主な価値で、速度は用途次第です。素朴な pandas UDF より 2〜4 倍速い
  チェックもありますが、numpy で丁寧にベクトル化した pandas UDF（法人番号）や、`set` を引くだけの UDF（郵便番号）には
  負けます。和暦は同等です。
- DQX 経由は式の単体評価より遅くなります（DQX が行ルールの条件列を複数回評価するため）。4 ルール同時で約 5.1 万行/秒。
- ランナー間のばらつきが大きく（同じ実装で 1.6 倍の差）、**絶対値ではなく同一実行内の比率**として読んでください。
  Photon・クラスタでの計測はしていません。

## Databricks Free Edition で試す

初心者向けの手順書があります: **[docs/databricks-free-edition.md](docs/databricks-free-edition.md)**
（GitHub から ZIP を取得し、Volume に置いて、ノートブック [examples/databricks_free_edition.py](examples/databricks_free_edition.py)
を上から実行します）。Free Edition（サーバーレス）の実機で、部 0・A・B の動作を確認済みです（部 C・D は未確認）。

## 仕様と制限

- `NULL` は違反になりません。必須項目は DQX の `is_not_null` と組み合わせてください。
- 入力は**そのまま**判定します。全角数字・空白・ハイフン・小文字の `t` は形式違反です。
- **法人番号の実在確認はしません**（オフライン検証のみ。実在確認は国税庁の公表サイトで）。
- **インボイス番号は法人形式のみ**です。個人事業者の登録番号（`T` + 13桁）はチェックディジット規則を
  一次資料で確認できていないため、正しい番号でも違反と判定されることがあります。
- **和暦は明治6年1月1日以降が対象**です。明治5年以前は旧暦のため、`pre_gregorian` として違反扱いで報告します
  （検証できないことを識別できるよう、他の理由と別コードにしています）。判定順は `format` → `pre_gregorian` →
  `not_exist` → `era_boundary`。元号の開始日は国立天文台・内閣府の資料に基づきます（[docs/sources.md](docs/sources.md)）。
- 和暦の区切りは `年月日` または `. / -`、空白は無視します。`5060401` のような元号コード形式は受け付けません。
- **履歴の粒度は取り込み頻度に等しい**: 変更は実際の施行日ではなく、それを最初に観測したスナップショットの日付になります。
  最初のスナップショットの事実は `valid_from = NULL`（それ以前から有効）です。
- **郵便番号の実在性**は町域の郵便番号のみが対象です。**大口事業所の個別番号は含まれず、「存在しない」と報告されます**。
- **都道府県・市区町村は日本郵便データの表記どおり**（`東京都`、`札幌市中央区`）に一致させます。表記ゆれは不一致です。
  郵便番号が存在しない・形式が不正・値が NULL の行は、整合チェックでは違反にしません（実在性チェックが報告します）。
- **地方公共団体コードの履歴は総務省の Excel から作ります**（`local-gov-codes-xlsx`）。改正一覧表の対象は
  **平成17年4月1日以降**で、それ以前に廃止されたコードは「存在しない」と報告されます。現行コード表の基準日は
  令和6年1月1日です。以降の変更は、新しい版を取得するか、整形済み CSV（`code,prefecture,city`）を
  `local-gov-codes` で重ねてください（[docs/sources.md](docs/sources.md)）。
- 取得 CLI の `--download` は、公式サイトがブラウザ以外を拒否する場合に失敗します。その場合はブラウザで取得し `--zip` を使ってください。
- Windows でファイルへ書き込む場合、Spark が winutils（`HADOOP_HOME`）を必要とします（Linux / Databricks では不要）。
- DQX の内部関数（`make_condition` など）を使っています。**対応バージョンは databricks-labs-dqx 0.16.x** です。

## ロードマップ

| フェーズ | 内容 | 状態 |
| --- | --- | --- |
| A | 法人番号・インボイス番号・地方公共団体コード | **実装済み** |
| C | 和暦日付の実在性（元号境界、元年表記、漢数字・全角、閏年、明治6年以前の扱い） | **実装済み** |
| B | 参照データ（郵便番号の実在性・都道府県/市区町村との整合、地方公共団体コードの実在性）、`as_of` 指定、取得 CLI | **実装済み** |
| — | ベンチマーク（1,000万行、pandas UDF との比較） | **計測済み**（[BENCHMARK.md](BENCHMARK.md)） |
| v0.2 | Shift_JIS に戻せない文字・半角カナ・機種依存文字の混入率、Great Expectations アダプタ | 設計のみ |

## 開発

```bash
pip install -e ".[dev]"
ruff check . && ruff format --check .
mypy
python -m pytest        # JDK 17 が必要。カバレッジ 90% 未満で失敗します
# 実データでの確認（任意）: JPDQ_KEN_ALL_ZIP、JPDQ_SOUMU_CURRENT、JPDQ_SOUMU_REVISIONS に取得済みファイルのパスを設定
```

動作確認環境: Python 3.12、PySpark 3.5.3、databricks-labs-dqx 0.16.0、Temurin JDK 17。

## ライセンス

MIT License — [LICENSE](LICENSE)
