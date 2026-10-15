# 公開時のチェックリスト（リポジトリを Public にするとき）

リポジトリの所有者向けです。上から順に行います。

## 公開の直前

- [ ] 記事の「公開前チェックリスト」（HTML コメント）が、すべて確認済みか
  - 所属先・顧客の情報の公開規程に触れないか（はじめにの文）
  - 総務省の著作権表記の原文を、目で確認したか（[sources.md](sources.md)）
- [ ] README の「動作確認の範囲」が、実際に確認した内容と一致しているか
- [ ] `main` の CI（Python 3.10〜3.12、Spark 4.0）が成功しているか
- [ ] 履歴に、個人情報・認証情報が無いか（`git log --format='%an <%ae>'`、`git grep`）
- [ ] DQX の Databricks License の扱いに、問題が無いか（README の「ライセンスと DQX について」。GitHub Actions で DQX を使うことを含む）

## 公開する

1. GitHub の **Settings → General → Danger Zone → Change visibility → Make public**
2. リポジトリの説明（About）と Topics が設定されていること（設定済み: databricks / dqx / data-quality / japan / pyspark / japanese / data-validation）

## 公開した直後（Private では設定できなかったもの）

- [ ] **ブランチ保護**（`main` の強制 push と削除を禁止）。Private では、GitHub Pro が必要で、設定できませんでした

  ```bash
  echo '{"required_status_checks":null,"enforce_admins":false,"required_pull_request_reviews":null,"restrictions":null,"allow_force_pushes":false,"allow_deletions":false}' \
    | gh api -X PUT repos/ogacloud2/jpdq-oga/branches/main/protection --input -
  ```

- [ ] **Private vulnerability reporting** を有効にする（Settings → Code security → Private vulnerability reporting）。
  `SECURITY.md` の 1 つ目の連絡方法が、これに依存しています
- [ ] **Dependabot alerts / secret scanning** を有効にする（Settings → Code security）
- [ ] ログアウトした状態（または別のブラウザ）で、次を確認する
  - README の相対リンク（`docs/...`）が開くこと
  - リリース `v0.1.0` の ZIP（`.../archive/refs/tags/v0.1.0.zip`）が取得できること
  - Issues の新規作成で、テンプレート（不具合・要望）が出ること
- [ ] Actions の通知（メール）が、自分に届く設定になっていること。週次の `dqx-latest.yml` が失敗したときに気付くためです

## 公開後の運用

- 週次の `dqx-latest.yml` が赤くなったら、DQX の更新による影響を調べる（依存は `databricks-labs-dqx>=0.16,<0.17`）
- 総務省のコード表・日本郵便のデータの更新（README の「仕様と制限」、`WARNING` の仕組み）
- 新しい元号が決まったら、`jpdq_oga/wareki.py` の `ERAS` を更新する
- Issue への返信は「できる範囲で」の方針（README の「サポートと貢献」）
