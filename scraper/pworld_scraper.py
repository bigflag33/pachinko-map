"""
P-WORLD (p-world.co.jp) 店舗情報スクレイパー

URL構造:
  一覧: GET https://www.p-world.co.jp/_machine/kensaku.cgi?dir={dir}&is_new_ver=1&page={n}
        dir の値: tokyo, kanagawa, osaka, aichi, fukuoka, saitama, chiba,
                  hyogo, hokkaido, miyagi, hiroshima, etc.
  1ページ50件、「全XXX件」でページ数判定

ページのテキスト形式 (例):
  51 ＴＯＨＯ 要町店 http://69822.p-world.jp 1時間前 東京都豊島区要町1-2-17周辺 41パチ 1000円/46枚スロ
"""
import re
import time
import requests
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": "https://www.p-world.co.jp/",
}

BASE_URL = "https://www.p-world.co.jp"

# 都道府県名 → P-WORLD の dir パラメータ
PREF_DIRS = {
    "北海道": "hokkaido",
    "青森":   "aomori",
    "岩手":   "iwate",
    "宮城":   "miyagi",
    "秋田":   "akita",
    "山形":   "yamagata",
    "福島":   "fukushima",
    "茨城":   "ibaraki",
    "栃木":   "tochigi",
    "群馬":   "gunma",
    "埼玉":   "saitama",
    "千葉":   "chiba",
    "東京":   "tokyo",
    "神奈川": "kanagawa",
    "新潟":   "niigata",
    "富山":   "toyama",
    "石川":   "ishikawa",
    "福井":   "fukui",
    "山梨":   "yamanashi",
    "長野":   "nagano",
    "岐阜":   "gifu",
    "静岡":   "shizuoka",
    "愛知":   "aichi",
    "三重":   "mie",
    "滋賀":   "shiga",
    "京都":   "kyoto",
    "大阪":   "osaka",
    "兵庫":   "hyogo",
    "奈良":   "nara",
    "和歌山": "wakayama",
    "鳥取":   "tottori",
    "島根":   "shimane",
    "岡山":   "okayama",
    "広島":   "hiroshima",
    "山口":   "yamaguchi",
    "徳島":   "tokushima",
    "香川":   "kagawa",
    "愛媛":   "ehime",
    "高知":   "kochi",
    "福岡":   "fukuoka",
    "佐賀":   "saga",
    "長崎":   "nagasaki",
    "熊本":   "kumamoto",
    "大分":   "oita",
    "宮崎":   "miyazaki",
    "鹿児島": "kagoshima",
    "沖縄":   "okinawa",
}


def fetch_page(url: str, retries: int = 3, session=None):
    s = session or requests.Session()
    for i in range(retries):
        try:
            resp = s.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return BeautifulSoup(resp.text, "html.parser")
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

    Returns:
        list of dict with keys: name, address, url, source, machines, lat, lng
    """
    prefs_to_scrape = target_prefs if target_prefs else list(PREF_DIRS.keys())
    results = []
    session = requests.Session()

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

    logger.info(f"P-WORLD合計: {len(results)}件")
    return results


def _scrape_pref(dir_name: str, pref_name: str, session, limit: int):
    """都道府県別にP-WORLD店舗一覧を取得"""
    stores = []
    page = 1

    while len(stores) < limit:
        url = (
            f"{BASE_URL}/_machine/kensaku.cgi"
            f"?dir={dir_name}&is_new_ver=1&page={page}"
        )
        soup = fetch_page(url, session=session)
        if not soup:
            break

        # ストアリンク: href が http://{数字}.p-world.jp 形式
        store_links = soup.find_all(
            "a", href=re.compile(r"https?://[^/]+\.p-world\.jp/?$")
        )
        if not store_links:
            break

        for a in store_links:
            if len(stores) >= limit:
                break

            store_url = a.get("href", "").rstrip("/")
            if not store_url:
                continue

            # 店舗名
            name = a.get_text(strip=True)
            if not name:
                continue

            # 親要素のテキストから住所を抽出
            parent = a.find_parent(["tr", "li", "div", "p"]) or a
            text = parent.get_text(" ", strip=True)

            # 住所: 都道府県〜「周辺」の直前まで
            address = _extract_address(text, pref_name)

            stores.append({
                "name": name,
                "address": address,
                "open_date": "",
                "url": store_url,
                "source": "P-WORLD",
                "is_grand_open": False,
                "machines": {},   # 詳細ページ取得なし (速度優先)
                "lat": None,
                "lng": None,
            })

        # 次ページ判定: 50件未満なら最終ページ
        if len(store_links) < 50:
            break

        # ページネーションリンクで確認
        next_link = soup.find("a", string=re.compile(r"次|Next|>"))
        if not next_link:
            # URL内 page= を直接チェック
            page_links = soup.find_all(
                "a", href=re.compile(rf"dir={dir_name}.*page=\d+")
            )
            current_pages = {
                int(m.group(1))
                for lnk in page_links
                for m in [re.search(r"page=(\d+)", lnk.get("href", ""))]
                if m
            }
            if not current_pages or max(current_pages) <= page:
                break

        page += 1
        time.sleep(0.8)

    return stores


def _extract_address(text: str, fallback: str) -> str:
    """テキストから住所を抽出 (「周辺」の直前まで)"""
    # 「都道府県名〜周辺」パターン
    m = re.search(
        r"((?:北海道|[^\s]{2,3}[都道府県]).{4,60}?)(?:周辺|$)",
        text
    )
    if m:
        addr = m.group(1).strip()
        # 不要な時刻表現を除去
        addr = re.sub(r"\d+(?:時間|分|日)前.*", "", addr).strip()
        if len(addr) > 5:
            return addr
    return fallback
