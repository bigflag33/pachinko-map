# 🎰 パチンコホール 立地・商圏分析マップ

ゴーパチ・P-WORLDの店舗データを自動スクレイピングし、
国勢調査の人口データと組み合わせてマップ表示する分析ツール。

**GitHub Pages で公開 → 毎朝6時に自動更新**

---

## 🚀 セットアップ手順

### 1. リポジトリ作成

```bash
git init
git remote add origin https://github.com/YOUR_USERNAME/pachinko-map.git
```

### 2. GitHub Pages の設定

GitHubリポジトリの **Settings → Pages** を開き:
- Source: `GitHub Actions` を選択

### 3. 初回ローカル動作確認

```bash
pip install -r requirements.txt

# サンプルデータで動作確認 (スクレイピングなし)
python generate.py --dry-run

# 生成されたHTMLをブラウザで開く
open docs/index.html
```

### 4. 本番スクレイピングを試す

```bash
# 東京・大阪のみ (テスト)
python generate.py --prefs 東京 大阪 --go8-days 30

# 全主要都市
python generate.py --go8-days 90
```

### 5. GitHubにプッシュ → 自動デプロイ開始

```bash
git add .
git commit -m "initial commit"
git push -u origin main
```

Actions タブで実行状況を確認。完了後:
`https://YOUR_USERNAME.github.io/pachinko-map/` で公開。

---

## 📁 ファイル構成

```
pachinko-map/
├── scraper/
│   ├── go8_scraper.py      # ゴーパチ グランドオープン取得
│   ├── pworld_scraper.py   # P-WORLD 店舗・台数取得
│   └── geocoder.py         # 住所→緯度経度 (国土地理院API)
├── generate.py             # メイン: スクレイプ→HTML生成
├── template_v5.html        # マップUIテンプレート
├── geocache.json           # ジオコードキャッシュ (自動生成)
├── docs/
│   ├── index.html          # 公開HTML (自動生成)
│   └── data.json           # 店舗データJSON (自動生成)
├── .github/workflows/
│   └── update.yml          # 毎朝6:00 JST 自動更新
└── requirements.txt
```

---

## ⚙️ カスタマイズ

### 取得都道府県を変更

`.github/workflows/update.yml` の `--prefs` を編集:

```yaml
python generate.py \
  --prefs 東京 神奈川 大阪 愛知 福岡  # 必要な県を列挙
```

### 更新スケジュール変更

`update.yml` の cron を変更:

```yaml
- cron: "0 21 * * *"  # 毎日 6:00 JST
- cron: "0 21 * * 1"  # 毎週月曜 6:00 JST
```

### ゴーパチの取得期間変更

```yaml
--go8-days 90  # 直近90日分のGOホール
```

---

## 🔧 スクレイパーの調整について

各サイトのHTML構造はリニューアルで変わることがあります。
スクレイピングが取れなくなった場合は以下をチェック:

**ゴーパチ** (`scraper/go8_scraper.py`):
- `scrape_go8()` 内の `soup.select(...)` のCSSセレクターを実際のHTMLに合わせる

**P-WORLD** (`scraper/pworld_scraper.py`):
- `_scrape_pref()` 内の URL形式とセレクターを調整

ブラウザの開発者ツール (F12) でHTML構造を確認してセレクターを更新してください。

---

## 📊 機能

- 🗺 **マップ表示** — グランドオープン店 & P-WORLD競合店
- 📈 **人口トレンド** — 国勢調査2010/2015/2020年 + 2025/2030予測
- 👥 **年齢構成・男女比** — 0-14/15-39/40-64/65+歳、男女比
- 🏆 **競合分析** — 商圏内の競合台数・距離ランキング
- 🔎 **条件検索** — 若年層比率・人口変化・競合台数などで絞り込み
- 📱 **スマホ対応** — 下部タブナビ、タッチ最適化

---

## ⚠️ 注意事項

- スクレイピングは各サイトの利用規約を確認してください
- 国勢調査データはサンプルの概算値です (実際の分析には[e-Stat](https://www.e-stat.go.jp/)のデータ取得を推奨)
- ジオコーディングは国土地理院APIとNominatimを使用 (無料・商用可)
