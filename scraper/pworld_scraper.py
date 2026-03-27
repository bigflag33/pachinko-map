"""
P-WORLD (p-world.co.jp) 店舗情報スクレイパー

URL構造:
  一覧: GET https://www.p-world.co.jp/_machine/kensaku.cgi?dir={dir}&is_new_ver=1&page={n}
        dir の値: tokyo, kanagawa, osaka, aichi 等

ページ構造:
  div.hallList-item が各店舗のコンテナ
  内部の a[href$=".htm"] が店舗ページリンク（店名テキスト含む）
  テキストから「都道府県名〜周辺」パターンで住所を抽出
  1ページ50件、「全XXX件」でページ総数を判定
"""
import re
import time
import logging

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ブラウザに近い完全なヘッダーセット
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

BASE_URL = "https://www.p-world.co.jp"

PREF_DIRS = {
    "北海道": "hokkaido", "青森": "aomori",  "岩手": "iwate",
    "宮城":   "miyagi",   "秋田": "akita",    "山形": "yamagata",
    "福島":   "fukushima","茨城": "ibaraki",  "栃木": "tochigi",
    "群馬":   "gunma",    "埼玉": "saitama",  "千葉": "chiba",
    "東京":   "tokyo",    "神奈川":"kanagawa", "新潟": "niigata",
    "富山":   "toyama",   "石川": "ishikawa", "福井": "fukui",
    "山梨":   "yamanashi","長野": "nagano",   "岐阜": "gifu",
    "静岡":   "shizuoka", "愛知": "aichi",    "三重": "mie",
    "滋賀":   "shiga",    "京都": "kyoto",    "大阪": "osaka",
    "兵庫":   "hyogo",    "奈良": "nara",     "和歌山":"wakayama",
    "鳥取":   "tottori",  "島根": "shimane",  "岡山": "okayama",
    "広島":   "hiroshima","山口": "yamaguchi","徳島": "tokushima",
    "香川":   "kagawa",   "愛媛": "ehime",    "高知": "kochi",
    "福岡":   "fukuoka",  "佐賀": "saga",     "長崎": "nagasaki",
    "熊本":   "kumamoto", "大分": "oita",     "宮崎": "miyazaki",
    "鹿児島": "kagoshima","沖縄": "okinawa",
}


def _make_session() -> requests.Session:
    """セッションを初期化。トップページを訪問してCookieを取得する。"""
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        resp = session.get(BASE_URL + "/", timeout=15)
        logger.info(f"P-WORLD トップページ: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"P-WORLD トップページ取得失敗: {e}")
    return session


def fetch_page(url: str, retries: int = 3, session=None) -> BeautifulSoup | None:
    s = session or requests.Session()
    for i in range(retries):
        try:
            resp = s.get(url, timeout=20)
            logger.info(f"  HTTP {resp.status_code} | {len(resp.content)} bytes | {url}")
            resp.raise_for_status()

            # エンコーディング検出（Shift-JIS / EUC-JP 対策）
            if resp.encoding and resp.encoding.lower() in ("shift_jis", "shift-jis", "sjis", "cp932"):
                text = resp.content.decode("cp932", errors="replace")
            elif resp.encoding and resp.encoding.lower() in ("euc-jp", "euc_jp"):
                text = resp.content.decode("euc-jp", errors="replace")
            else:
                enc = resp.apparent_encoding or "utf-8"
                text = resp.content.decode(enc, errors="replace")

            soup = BeautifulSoup(text, "html.parser")

            # 診断ログ（最初のページのみ詳細出力）
            if i == 0:
                title = soup.find("title")
                logger.info(f"  ページタイトル: {title.get_text(strip=True) if title else '(なし)'}")
                items_found = len(soup.select("div.hallList-item"))
                htm_links   = len(soup.select("a[href$='.htm']"))
                all_divs    = [d.get("class", []) for d in soup.find_all("div", limit=20)]
                logger.info(f"  hallList-item: {items_found}件 | .htm リンク: {htm_links}件")
                logger.info(f"  先頭20divクラス: {all_divs}")
                # 先頭300文字をダンプ（文字化け検出）
                preview = text[:300].replace("\n", " ").replace("\r", "")
                logger.info(f"  HTML先頭: {preview}")
                # ★ 最初のhallList-itemの中身を詳細ダンプ
                first_items = soup.select("div.hallList-item")
                if first_items:
                    item_html = str(first_items[0])[:600].replace("\n", " ")
                    logger.info(f"  [DEBUG] 1件目のhallList-item HTML: {item_html}")
                    # その中のリンクをすべて列挙
                    links_in_item = first_items[0].find_all("a", href=True)
                    logger.info(f"  [DEBUG] 1件目のリンク一覧: {[(a.get('href'), a.get_text(strip=True)[:20]) for a in links_in_item]}")

            return soup
        except Exception as e:
            logger.warning(f"Fetch failed ({i+1}/{retries}): {url} - {e}")
            time.sleep(2 ** i)
    return None


def scrape_pworld_by_prefs(target_prefs=None, max_stores=500):
    """
    P-WORLDから指定都道府県の店舗情報を取得。

    Args:
        target_prefs: 取得する都道府県名リスト (None=全国)
        max_stores: 最大取得店舗数
    """
    prefs_to_scrape = target_prefs if target_prefs else list(PREF_DIRS.keys())
    results = []
    session = _make_session()

    for pref in prefs_to_scrape:
        if len(results) >= max_stores:
            break
        dir_name = PREF_DIRS.get(pref)
        if not dir_name:
            logger.warning(f"P-WORLD: 都道府県コード不明 '{pref}'")
            continue

        logger.info(f"P-WORLD取得: {pref} (dir={dir_name})")
        stores = _scrape_pref(dir_name, pref, session, max_stores - len(results))
        results.extend(stores)
        logger.info(f"  → {len(stores)}件")
        time.sleep(1)

    logger.info(f"p-world: {len(results)} halls scraped")
    return results


def _scrape_pref(dir_name: str, pref_name: str, session, limit: int) -> list[dict]:
    """都道府県別にP-WORLD店舗一覧を取得（ページネーション対応）"""
    stores = []
    page = 1
    total = None  # 全件数（初回取得後にセット）

    while len(stores) < limit:
        url = (
            f"{BASE_URL}/_machine/kensaku.cgi"
            f"?dir={dir_name}&is_new_ver=1&page={page}"
        )
        soup = fetch_page(url, session=session)
        if not soup:
            logger.warning(f"  {pref_name} p{page}: ページ取得失敗")
            break

        # 全件数を初回に取得
        if total is None:
            m = re.search(r"全(\d+)件", soup.get_text())
            total = int(m.group(1)) if m else 0
            logger.info(f"  {pref_name}: 全{total}件")

        # div.hallList-item が各店舗
        items = soup.select("div.hallList-item")
        logger.info(f"  {pref_name} p{page}: {len(items)}件の hallList-item")
        if not items:
            # セレクタが合わない場合、別のパターンを試す
            alt = soup.select("div[class*='hall']")
            logger.info(f"  hallクラスを含むdiv: {len(alt)}件 → {[str(d)[:80] for d in alt[:3]]}")
            break

        for item in items:
            if len(stores) >= limit:
                break
            store = _parse_item(item, dir_name, pref_name)
            if store:
                stores.append(store)

        # ページ終了判定
        if len(items) < 50:
            break
        if total and len(stores) >= min(total, limit):
            break

        page += 1
        time.sleep(0.8)

    return stores


def _parse_item(item, dir_name: str, pref_name: str) -> dict | None:
    """div.hallList-item から店舗情報を抽出"""
    # 店舗リンク: /tokyo/xxx.htm 形式
    link = item.select_one(f"a[href*='/{dir_name}/'][href$='.htm']")
    if not link:
        # 任意の .htm リンクでも試す
        link = item.select_one("a[href$='.htm']")
    if not link:
        # href に dir_name を含む任意リンク
        for a in item.find_all("a", href=True):
            href = a.get("href", "")
            if dir_name in href or ".htm" in href:
                link = a
                break
    if not link:
        return None

    href = link.get("href", "")
    store_url = href if href.startswith("http") else BASE_URL + href
    name = link.get_text(strip=True)
    if not name:
        return None

    # テキストから住所を抽出（「都道府県名〜周辺」パターン）
    text = item.get_text(" ", strip=True)
    address = _extract_address(text, pref_name)

    return {
        "name":         name,
        "address":      address,
        "open_date":    "",
        "url":          store_url,
        "source":       "P-WORLD",
        "is_grand_open": False,
        "machines":     {},
        "lat":          None,
        "lng":          None,
    }


def _extract_address(text: str, fallback: str) -> str:
    """テキストから住所部分を抽出"""
    m = re.search(
        r"((?:北海道|[^\s]{2,3}[都道府県]).{4,60}?)(?:周辺|$)",
        text
    )
    if m:
        addr = re.sub(r"\d+(?:時間|分|日)前.*", "", m.group(1)).strip()
        if len(addr) > 5:
            return addr
    return fallback
