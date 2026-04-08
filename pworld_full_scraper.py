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

            # バイト列をそのまま渡してBeautifulSoupにエンコーディングを自動検出させる
            soup = BeautifulSoup(resp.content, "html.parser")

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
# lat/lng抽出: シンプルな個別パターンで確実にマッチ
LAT_RE = re.compile(r"lat\s*:\s*['\"]([0-9]+\.[0-9]+)['\"]")
LNG_RE = re.compile(r"lng\s*:\s*['\"]([0-9]+\.[0-9]+)['\"]")
# lat/lngの別パターン（Google Maps埋め込みなど）
LAT_RE2 = re.compile(r"[?&,](-?[0-9]{2}\.[0-9]{4,})")
LNG_RE2 = re.compile(r"[?&,](1[23][0-9]\.[0-9]{4,})")
MACHINE_RE = re.compile(r"設置台数[^\d]*(\d+)\s*台")
TOTAL_RE  = re.compile(r"総台数[^\d]*(\d+)\s*台")
# 住所抽出パターン（複数試行）
ADDR_RE   = re.compile(r"住\s*所[　\s:：]*([^\n\r<]{5,60})")
ADDR_RE2  = re.compile(r"〒\s*\d{3}[-－]\d{4}\s*([^\n\r<]{5,60})")


def _geocode_address(address: str) -> tuple[Optional[float], Optional[float]]:
    """国土地理院APIで住所→緯度経度変換。失敗時はNominatimを試みる"""
    import urllib.parse
    try:
        q = urllib.parse.quote(address)
        url = f"https://msearch.gsi.go.jp/address-search/AddressSearch?q={q}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data:
            coords = data[0]["geometry"]["coordinates"]
            return float(coords[1]), float(coords[0])
    except Exception:
        pass
    try:
        q = urllib.parse.quote(address)
        url = f"https://nominatim.openstreetmap.org/search?q={q}&format=json&limit=1&countrycodes=jp"
        resp = requests.get(url, timeout=10, headers={"User-Agent": "pachinko-map-scraper/1.0"})
        data = resp.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass
    return None, None


def scrape_detail(session: requests.Session, url: str) -> Optional[dict]:
    """個別店舗ページをスクレイプして dict を返す。失敗時は None"""
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None

        # エンコーディング検出: バイト列にregexを直接適用（BeautifulSoup経由より確実）
        # P-WORLDはほぼ全ページEUC-JPを使用
        raw_head = resp.content[:4000]
        charset_match = re.search(rb'charset\s*=\s*["\']?\s*([A-Za-z0-9\-_]+)', raw_head, re.I)
        if charset_match:
            encoding = charset_match.group(1).decode("ascii", errors="ignore").strip("\"'")
        else:
            encoding = "euc-jp"  # P-WORLD デフォルト
        try:
            html = resp.content.decode(encoding, errors="replace")
        except (LookupError, UnicodeDecodeError):
            html = resp.content.decode("euc-jp", errors="replace")

        # ── lat/lng ──
        lat = lng = None
        m_lat = LAT_RE.search(html)
        m_lng = LNG_RE.search(html)
        if m_lat and m_lng:
            lat = float(m_lat.group(1))
            lng = float(m_lng.group(1))
            # 日本国内の座標チェック (lat: 20-46, lng: 122-154)
            if not (20 < lat < 46 and 122 < lng < 154):
                lat = lng = None

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
        # 優先順位でCSSセレクタを試す
        addr_tag = (
            soup.select_one(".hallData-address")
            or soup.select_one(".shopAddress")
            or soup.select_one(".hall-address")
            or soup.select_one("[class*='address']")
            or soup.select_one("td.address")
        )
        if addr_tag:
            address = addr_tag.get_text(strip=True)
        # テーブル行から「住所」ラベルの次セルを探す
        if not address:
            for th in soup.find_all(["th", "td", "dt"]):
                if "住所" in th.get_text():
                    sib = th.find_next_sibling(["td", "dd"])
                    if sib:
                        address = sib.get_text(strip=True)
                        break
        # regex fallback
        if not address:
            m_addr = ADDR_RE.search(html)
            if m_addr:
                address = m_addr.group(1).strip()
        if not address:
            m_addr = ADDR_RE2.search(html)
            if m_addr:
                address = m_addr.group(1).strip()

        # ── 台数 ──
        total = pachinko = slot = 0

        # 候補セクションを優先順位で探す
        machine_section = (
            soup.select_one(".machineInfo")
            or soup.select_one(".hallData-machine")
            or soup.select_one(".hall-machine")
            or soup.select_one(".machineData")
            or soup.select_one("[class*='machine']")
            or soup.select_one("[class*='台数']")
        )

        def _extract_machines(text: str):
            """テキストからパチンコ/スロット台数を抽出して (total, pachinko, slot) を返す"""
            p = s = t = 0
            # 「総台数」「合計」「設置台数」から total を先に取得
            m = MACHINE_RE.search(text) or TOTAL_RE.search(text)
            if m:
                t = int(m.group(1))
            # パチンコ行
            for pat in [r"パチンコ[^\d]*(\d+)\s*台", r"CR[^\d]*(\d+)\s*台"]:
                mp = re.search(pat, text)
                if mp:
                    p = int(mp.group(1))
                    break
            # スロット行
            for pat in [r"スロット?[^\d]*(\d+)\s*台", r"スロ[^\d]*(\d+)\s*台"]:
                ms = re.search(pat, text)
                if ms:
                    s = int(ms.group(1))
                    break
            # totalが取れなかったらパチンコ+スロットの和で推定
            if t == 0 and (p > 0 or s > 0):
                t = p + s
            return t, p, s

        if machine_section:
            sec_text = machine_section.get_text()
            total, pachinko, slot = _extract_machines(sec_text)

            # セクション内で取れなかった場合、行単位でも試す
            if total == 0:
                for row in machine_section.find_all(["tr", "div", "li"]):
                    row_text = row.get_text()
                    nums = re.findall(r"(\d+)\s*台", row_text)
                    if nums:
                        row_total, row_p, row_s = _extract_machines(row_text)
                        if row_p > 0: pachinko = row_p
                        if row_s > 0: slot = row_s
                if pachinko > 0 or slot > 0:
                    total = pachinko + slot

        # セクションが見つからなかった場合、ページ全体から台数を抽出
        if total == 0:
            # テーブル行の「設置台数」「総台数」「合計台数」ラベルを探す
            for th in soup.find_all(["th", "td", "dt", "span", "div"]):
                th_text = th.get_text(strip=True)
                if re.search(r"(設置|総|合計)?台数", th_text) and len(th_text) < 15:
                    sib = th.find_next_sibling(["td", "dd", "span"])
                    if sib:
                        m_n = re.search(r"(\d+)", sib.get_text())
                        if m_n and int(m_n.group(1)) > 10:
                            total = int(m_n.group(1))
                            break
            # それでも0ならページ全体のテキストから「設置台数」を正規表現で
            if total == 0:
                m_total = MACHINE_RE.search(html) or TOTAL_RE.search(html)
                if m_total:
                    total = int(m_total.group(1))
            # パチンコ/スロット個別も試す
            if total == 0:
                full_total, full_p, full_s = _extract_machines(html)
                total, pachinko, slot = full_total, full_p, full_s

        # lat/lngが取れなかった場合は住所でジオコーディング
        if lat is None and address:
            lat, lng = _geocode_address(address)

        if lat is None:
            return None  # 座標なし → スキップ

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
    parser.add_argument("--update", action="store_true", help="既存のpworld_all.jsonに指定都道府県を上書きマージ")
    parser.add_argument("--list-only", action="store_true", help="URL収集のみ（個別ページ取得しない）")
    args = parser.parse_args()

    # 対象都道府県を決定
    if args.pref:
        target_prefs = [(d, n) for d, n in ALL_PREFS if d in args.pref]
    else:
        target_prefs = ALL_PREFS

    # --update: 既存JSONから対象都道府県のデータを除いてベースにする
    if args.update and args.pref and OUTPUT_FILE.exists():
        existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
        update_prefs = set(args.pref)
        # 対象都道府県の日本語名を取得
        pref_ja = {d: n for d, n in ALL_PREFS}
        update_ja = {pref_ja[d] for d in update_prefs if d in pref_ja}
        stores = [s for s in existing if s.get("pref") not in update_ja]
        logger.info(f"既存データ読み込み: {len(existing)}件 → {len(stores)}件（{update_ja} を除外）")
        cp = {"done_prefs": [], "stores": stores}
    else:
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
