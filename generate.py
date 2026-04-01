"""
パチンコホール分析マップ HTMLジェネレーター

スクレイピングデータを受け取り、pachinko_map_v5.html を生成して
docs/index.html として出力する。

使い方:
    python generate.py                  # スクレイピング + HTML生成
    python generate.py --dry-run        # データなしでサンプルHTML生成
    python generate.py --data data.json # 既存JSONからHTML生成
"""
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ================================================================
# サンプルデータ (--dry-run 用)
# ================================================================
SAMPLE_HALLS = [
    {"name":"グランドオープン 新宿エース","address":"東京都新宿区歌舞伎町1-1-1","open_date":"2026-03-20","source":"ゴーパチ","url":"#","lat":35.6938,"lng":139.7034,"is_grand_open":True,"machines":{"total":450,"pachinko":280,"pachinko_4en":230,"pachinko_1en":50,"slot":170,"slot_20en":130,"slot_5en":40}},
    {"name":"グランドオープン 渋谷パラダイス","address":"東京都渋谷区道玄坂1-2-3","open_date":"2026-03-15","source":"ゴーパチ","url":"#","lat":35.6605,"lng":139.6984,"is_grand_open":True,"machines":{"total":380,"pachinko":220,"pachinko_4en":180,"pachinko_1en":40,"slot":160,"slot_20en":120,"slot_5en":40}},
    {"name":"グランドオープン 梅田キング","address":"大阪府大阪市北区梅田2-1-1","open_date":"2026-03-25","source":"ゴーパチ","url":"#","lat":34.7024,"lng":135.4959,"is_grand_open":True,"machines":{"total":620,"pachinko":380,"pachinko_4en":320,"pachinko_1en":60,"slot":240,"slot_20en":190,"slot_5en":50}},
    {"name":"パチンコ大王 新宿店","address":"東京都新宿区西新宿1-1-1","open_date":"","source":"P-WORLD","url":"#","lat":35.6896,"lng":139.6917,"is_grand_open":False,"machines":{"total":520,"pachinko":300,"pachinko_4en":250,"pachinko_1en":50,"slot":220,"slot_20en":170,"slot_5en":50}},
    {"name":"梅田パチンコワールド","address":"大阪府大阪市北区梅田1-1-1","open_date":"","source":"P-WORLD","url":"#","lat":34.7005,"lng":135.4965,"is_grand_open":False,"machines":{"total":580,"pachinko":360,"pachinko_4en":300,"pachinko_1en":60,"slot":220,"slot_20en":175,"slot_5en":45}},
]


PWORLD_ALL_JSON = Path("docs/pworld_all.json")  # 全件スクレイプ済みJSON


def run_scraper(dry_run: bool = False, go8_days: int = 90, pworld_prefs: list[str] | None = None) -> list[dict]:
    """スクレイピング実行

    P-WORLD 全件JSON (docs/pworld_all.json) が存在する場合はそれを読み込む。
    存在しない場合は従来の一覧スクレイピング（都道府県指定・上限300件）にフォールバック。
    ゴーパチは毎回スクレイプ（新規GO情報のため）。
    """
    if dry_run:
        logger.info("Dry-run mode: using sample data")
        return SAMPLE_HALLS

    halls = []

    # ── ゴーパチ（毎回スクレイプ） ──
    try:
        from scraper.go8_scraper import scrape_go8
        logger.info(f"Scraping ゴーパチ (直近{go8_days}日)...")
        go_halls = scrape_go8(days_back=go8_days)
        halls.extend(go_halls)
        logger.info(f"  ゴーパチ: {len(go_halls)}件")
    except Exception as e:
        logger.error(f"ゴーパチ スクレイピング失敗: {e}")

    # ── P-WORLD（全件JSONがあればそれを優先） ──
    if PWORLD_ALL_JSON.exists():
        try:
            pw_halls = json.loads(PWORLD_ALL_JSON.read_text(encoding="utf-8"))
            # lat/lng がある店舗だけ使う
            pw_halls = [h for h in pw_halls if h.get("lat") and h.get("lng")]
            halls.extend(pw_halls)
            logger.info(f"  P-WORLD (全件JSON): {len(pw_halls)}件")
        except Exception as e:
            logger.error(f"P-WORLD 全件JSON読み込み失敗: {e}")
    else:
        # フォールバック: 一覧スクレイピング（従来の都道府県指定方式）
        logger.info("  pworld_all.json が未生成。一覧スクレイピングにフォールバック。")
        logger.info("  ヒント: python3 scraper/pworld_full_scraper.py を実行すると全件取得できます。")
        try:
            from scraper.pworld_scraper import scrape_pworld_by_prefs
            target_prefs = pworld_prefs or ["東京", "神奈川", "大阪", "愛知", "福岡", "埼玉", "千葉", "兵庫"]
            logger.info(f"  Scraping P-WORLD ({', '.join(target_prefs)})...")
            pw_halls = scrape_pworld_by_prefs(target_prefs=target_prefs, max_stores=300)
            halls.extend(pw_halls)
            logger.info(f"  P-WORLD (一覧): {len(pw_halls)}件")
        except Exception as e:
            logger.error(f"P-WORLD スクレイピング失敗: {e}")

    # ── 重複除去 (同名+同住所) ──
    seen = set()
    deduped = []
    for h in halls:
        key = (h.get("name", ""), h.get("address", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(h)
    logger.info(f"重複除去後: {len(deduped)}件")

    return deduped


def geocode_halls(halls: list[dict]) -> list[dict]:
    """ジオコーディング"""
    try:
        from scraper.geocoder import geocode_batch
        logger.info("ジオコーディング中...")
        halls = geocode_batch(halls, sleep_sec=0.5)
    except Exception as e:
        logger.error(f"ジオコーディング失敗: {e}")
    return halls


def generate_html(halls: list[dict], output_path: str = "docs/index.html") -> None:
    """v5 HTML を生成"""
    # 有効な座標を持つホールのみ
    valid_halls = [h for h in halls if h.get("lat") and h.get("lng")]
    logger.info(f"マップに表示: {len(valid_halls)}/{len(halls)}件")

    go_count = sum(1 for h in valid_halls if h.get("is_grand_open"))
    pw_count = sum(1 for h in valid_halls if not h.get("is_grand_open"))
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    halls_json = json.dumps(valid_halls, ensure_ascii=False, separators=(",", ":"))

    # v5テンプレートを読み込む
    template_path = Path(__file__).parent / "template_v5.html"
    if template_path.exists():
        template = template_path.read_text(encoding="utf-8")
    else:
        # テンプレートがなければ内蔵バージョンを使用
        template = _get_embedded_template()

    # プレースホルダーを置換
    html = (template
            .replace("{{HALLS_JSON}}", halls_json)
            .replace("{{GO_COUNT}}", str(go_count))
            .replace("{{PW_COUNT}}", str(pw_count))
            .replace("{{GENERATED_AT}}", now_str))

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    logger.info(f"HTML生成完了: {output_path} ({len(html):,} chars)")


def _get_embedded_template() -> str:
    """テンプレートファイルがない場合の内蔵テンプレート"""
    # docs/index.html のベースは template_v5.html から生成
    # ここでは最小限のフォールバック
    return """<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>パチンコホール分析マップ</title></head><body>
<p>テンプレートファイル (template_v5.html) が見つかりません。<br>
setup手順を確認してください。</p>
<script>const HALLS = {{HALLS_JSON}};</script>
</body></html>"""


def save_json(halls: list[dict], path: str = "docs/data.json") -> None:
    """データをJSONとして保存"""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(halls, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"JSON保存: {path}")


def main():
    parser = argparse.ArgumentParser(description="パチンコホール分析マップ生成")
    parser.add_argument("--dry-run", action="store_true", help="サンプルデータで生成")
    parser.add_argument("--data", type=str, help="既存JSONファイルパス")
    parser.add_argument("--output", type=str, default="docs/index.html", help="出力HTMLパス")
    parser.add_argument("--go8-days", type=int, default=90, help="ゴーパチ取得日数")
    parser.add_argument("--prefs", nargs="+", help="P-WORLD取得都道府県 (例: 東京 大阪)")
    args = parser.parse_args()

    # データ取得
    if args.data:
        logger.info(f"既存データ読み込み: {args.data}")
        halls = json.loads(Path(args.data).read_text(encoding="utf-8"))
    else:
        halls = run_scraper(dry_run=args.dry_run, go8_days=args.go8_days, pworld_prefs=args.prefs)
        halls = geocode_halls(halls)
        save_json(halls, "docs/data.json")

    # HTML生成
    generate_html(halls, output_path=args.output)
    logger.info("✅ 完了")


if __name__ == "__main__":
    main()
