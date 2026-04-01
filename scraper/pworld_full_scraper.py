"""
P-WORLD 全店舗スクレイパー（初回 & 月次手動実行用）

使い方:
    python3 scraper/pworld_full_scraper.py

    # 特定都道府県のみ
    python3 scraper/pworld_full_scraper.py --pref tokyo osaka

    # 途中から再開 (チェックポイントあり)
    python3 scraper/pworld_full_scraper.py --resume

出力: docs/pworld_all.json
実行時間: 全国で約40〜60分 (0.6秒/店舗)
"""

import re
import json
import time
import logging
import argparse
import sys
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pworld_full_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.p-world.co.jp"
CHECKPOINT_FILE = Path("pworld_checkpoint.json")
OUTPUT_FILE = Path("docs/pworld_all.json")
DELAY_LIST   = 0.6   # 一覧ページ間 (秒)
DELAY_DETAIL = 0.6   # 個別ページ間 (秒)

# 47都道府県 dir名
ALL_PREFS = [
    ("hokkaido",  "北海道"),
    ("aomori",    "青森"),
    ("iwate",     "岩手"),
    ("miyagi",    "宮城"),
    ("akita",     "秋田"),
    ("yamagata",  "山形"),
    ("fukushima", "福島"),
    ("ibaraki",   "茨城"),
    ("tochigi",   "栃木"),
    ("gunma",     "群馬"),
    ("saitama",   "埼玉"),
    ("chiba",     "千葉"),
    ("tokyo",     "東京"),
    ("kanagawa",  "神奈川"),
    ("niigata",   "新潟"),
    ("toyama",    "富山"),
    ("ishikawa",  "石川"),
    ("fukui",     "福井"),
    ("yamanashi", "山梨"),
    ("nagano",    "長野"),
    ("shizuoka",  "静岡"),
    ("aichi",     "愛知"),
    ("mie",       "三重"),
    ("shiga",     "滋賀"),
    ("kyoto",     "京都"),
    ("osaka",     "大阪"),
    ("hyogo",     "兵庫"),
    ("nara",      "奈良"),
    ("wakayama",  "和歌山"),
    ("tottori",   "鳥取"),
    ("shimane",   "島根"),
    ("okayama",   "岡山"),
    ("hiroshima", "広島"),
    ("yamaguchi", "山口"),
    ("tokushima", "徳島"),
    ("kagawa",    "香川"),
    ("ehime",     "愛媛"),
    ("kochi",     "高知"),
    ("fukuoka",   "福岡"),
    ("saga",      "佐賀"),
    ("nagasaki",  "長崎"),
    ("kumamoto",  "熊本"),
    ("oita",      "大分"),
    ("miyazaki",  "宮崎"),
    ("kagoshima", "鹿児島"),
    ("okinawa",   "沖縄"),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.9",
    "Referer": BASE_URL + "/",
}


# ================================================================
# セッション作成
# ================================================================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(BASE_URL + "/", timeout=15)
    except Exception:
        pass
    return s


# ================================================================
# 一覧ページから店舗URLを収集
# ================================================================
def collect_store_urls(session: requests.Session, dir_name: str, pref_name: str) -> list[str]:
    """一覧ページを全ページ走査して店舗URLリストを返す"""
    urls = []
    pattern = re.compile(rf"/{dir_name}/[^/]+\.htm$")
    page = 1

    while True:
        list_url = f"{BASE_URL}/_machine/kensaku.cgi?dir={dir_name}&is_new_ver=1&page={page}"
        try:
            resp = session.get(list_url, timeout=20)
            if resp.status_code != 200:
                logger.warning(f"  {pref_name} p{page}: HTTP {resp.status_code}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # 一覧コンテナを探す
            container = soup.select_one("div.hallList-body") or soup.select_one("div.hallList")
            if not container:
                # ページが存在しない or 最終ページ超過
                break

            page_urls = []
            seen = set()
            for a in container.find_all("a", href=True):
                href = a["href"]
                if pattern.search(href):
                    full = href if href.startswith("http") else BASE_URL + href
                    if full not in seen:
                        seen.add(full)
                        page_urls.append(full)

            if not page_urls:
                break

            urls.extend(page_urls)
            logger.info(f"  {pref_name} p{page}: {len(page_urls)}件 (累計 {len(urls)})")
            page += 1
            time.sleep(DELAY_LIST)

        except requests.RequestException as e:
            logger.warning(f"  {pref_name} p{page} エラー: {e}")
            time.sleep(3)
            break

    return urls


# ================================================================
# 個別ページから lat/lng・住所・台数を取得
# ================================================================
LAT_LNG_RE = re.compile(
    r"show_group_hall_page\s*\(\s*\{[^}]*?lng\s*:\s*['\"]([^'\"]+)['\"][^}]*?lat\s*:\s*['\"]([^'\"]+)['\"]",
    re.DOTALL,
)
LAT_LNG_RE2 = re.compile(
    r"show_group_hall_page\s*\(\s*\{[^}]*?lat\s*:\s*['\"]([^'\"]+)['\"][^}]*?lng\s*:\s*['\"]([^'\"]+)['\"]",
    re.DOTALL,
)
MACHINE_RE = re.compile(r"設置台数[^\d]*(\d+)\s*台")
ADDR_RE = re.compile(r"住\s*所[　\s]*([^\n\r<]{5,50})")


def scrape_detail(session: requests.Session, url: str) -> Optional[dict]:
    """個別店舗ページをスクレイプして dict を返す。失敗時は None"""
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None

        html = resp.text

        # ── lat/lng ──
        lat = lng = None
        m = LAT_LNG_RE.search(html)
        if m:
            lng, lat = float(m.group(1)), float(m.group(2))
        else:
            m2 = LAT_LNG_RE2.search(html)
            if m2:
                lat, lng = float(m2.group(1)), float(m2.group(2))

        if lat is None:
            return None  # 座標なし → スキップ

        soup = BeautifulSoup(html, "html.parser")

        # ── 店舗名 ──
        name = ""
        h1 = soup.select_one("h1.shopName") or soup.select_one("h1") or soup.select_one(".hall-name")
        if h1:
            name = h1.get_text(strip=True)
        if not name:
            title = soup.find("title")
            if title:
                name = title.get_text(strip=True).split("|")[0].strip()

        # ── 住所 ──
        address = ""
        addr_tag = soup.select_one(".hallData-address") or soup.select_one(".shopAddress")
        if addr_tag:
            address = addr_tag.get_text(strip=True)
        if not address:
            m_addr = ADDR_RE.search(html)
            if m_addr:
                address = m_addr.group(1).strip()

        # ── 台数 ──
        total = pachinko = slot = 0
        machine_section = soup.select_one(".machineInfo") or soup.select_one(".hallData-machine")
        if machine_section:
            text = machine_section.get_text()
            nums = re.findall(r"(\d+)\s*台", text)
            if nums:
                total = int(nums[0])
            # パチンコ / スロット
            for row in machine_section.find_all(["tr", "div", "li"]):
                row_text = row.get_text()
                if "パチンコ" in row_text and "スロ" not in row_text:
                    m_n = re.search(r"(\d+)\s*台", row_text)
                    if m_n:
                        pachinko = int(m_n.group(1))
                elif "スロ" in row_text:
                    m_n = re.search(r"(\d+)\s*台", row_text)
                    if m_n:
                        slot = int(m_n.group(1))

        if total == 0 and pachinko > 0:
            total = pachinko + slot

        return {
            "name": name,
            "url": url,
            "address": address,
            "lat": lat,
            "lng": lng,
            "source": "P-WORLD",
            "is_grand_open": False,
            "open_date": "",
            "machines": {
                "total": total,
                "pachinko": pachinko,
                "pachinko_4en": 0,
                "pachinko_1en": 0,
                "slot": slot,
                "slot_20en": 0,
                "slot_5en": 0,
            },
        }

    except Exception as e:
        logger.debug(f"detail error {url}: {e}")
        return None


# ================================================================
# チェックポイント 読み書き
# ================================================================
def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"done_prefs": [], "stores": []}


def save_checkpoint(cp: dict):
    CHECKPOINT_FILE.write_text(json.dumps(cp, ensure_ascii=False, indent=2), encoding="utf-8")


# ================================================================
# メイン
# ================================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pref", nargs="*", help="都道府県dirを指定 (例: tokyo osaka)")
    parser.add_argument("--resume", action="store_true", help="チェックポイントから再開")
    parser.add_argument("--list-only", action="store_true", help="URL収集のみ（個別ページ取得しない）")
    args = parser.parse_args()

    # 対象都道府県を決定
    if args.pref:
        target_prefs = [(d, n) for d, n in ALL_PREFS if d in args.pref]
    else:
        target_prefs = ALL_PREFS

    # チェックポイント
    cp = load_checkpoint() if args.resume else {"done_prefs": [], "stores": []}
    done_prefs = set(cp["done_prefs"])
    stores: list[dict] = cp["stores"]

    session = make_session()

    total_prefs = len(target_prefs)
    for i, (dir_name, pref_name) in enumerate(target_prefs, 1):
        if dir_name in done_prefs:
            logger.info(f"[{i}/{total_prefs}] {pref_name} スキップ（完了済み）")
            continue

        logger.info(f"[{i}/{total_prefs}] {pref_name} 一覧収集中...")
        urls = collect_store_urls(session, dir_name, pref_name)
        logger.info(f"  → {len(urls)}件のURL収集完了")

        if args.list_only:
            for u in urls:
                stores.append({"url": u, "pref": pref_name})
        else:
            pref_count = 0
            for j, url in enumerate(urls, 1):
                result = scrape_detail(session, url)
                if result:
                    result["pref"] = pref_name
                    stores.append(result)
                    pref_count += 1
                if j % 50 == 0:
                    logger.info(f"    {pref_name}: {j}/{len(urls)}件処理中... 取得成功 {pref_count}件")
                    save_checkpoint({"done_prefs": list(done_prefs), "stores": stores})
                time.sleep(DELAY_DETAIL)

            logger.info(f"  {pref_name} 完了: {pref_count}/{len(urls)}件取得")

        done_prefs.add(dir_name)
        save_checkpoint({"done_prefs": list(done_prefs), "stores": stores})

    # 出力
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(stores, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"✅ 完了: {len(stores)}件 → {OUTPUT_FILE}")

    # チェックポイント削除
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


if __name__ == "__main__":
    main()
