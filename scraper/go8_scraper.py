"""
ゴーパチ (5pachi.com) グランドオープン情報スクレイパー

URL構造:
  都道府県別一覧: GET https://5pachi.com/pref/{romaji}
  例: https://5pachi.com/pref/tokyo

ページ構造:
  table.result_area の各 tr が1ホール
  td[0]=開店日, td[1]=タイプ, td[2]=店名(aタグ), td[3]=住所,
  td[4]=パチンコ台数, td[5]=スロット台数
"""
import re
import time
import logging
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": "https://5pachi.com/",
}

BASE_URL = "https://5pachi.com"

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

GRAND_OPEN_TYPES = {
    "グランドオープン", "グランドリニューアルオープン",
    "新装オープン", "リニューアルオープン", "移転オープン",
}


def fetch(url: str, retries: int = 3, session=None) -> BeautifulSoup | None:
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


def scrape_go8(days_back: int = 90, target_prefs: list[str] | None = None) -> list[dict]:
    """
    ゴーパチから都道府県別グランドオープン情報を取得。

    Args:
        days_back: 何日前までのデータを取得するか
        target_prefs: 取得する都道府県リスト (None=全国)
    """
    cutoff = datetime.now() - timedelta(days=days_back)
    prefs = target_prefs if target_prefs else list(PREF_DIRS.keys())
    results = []
    session = requests.Session()

    for pref in prefs:
        romaji = PREF_DIRS.get(pref)
        if not romaji:
            logger.warning(f"ゴーパチ: 都道府県コード不明 '{pref}'")
            continue

        url = f"{BASE_URL}/pref/{romaji}"
        logger.info(f"ゴーパチ取得: {pref}")
        soup = fetch(url, session=session)
        if not soup:
            logger.info(f"  → 取得失敗")
            continue

        halls = _parse_pref_page(soup, pref, cutoff)
        results.extend(halls)
        logger.info(f"  → {len(halls)}件")
        time.sleep(1)

    logger.info(f"go8: {len(results)} halls scraped")
    return results


def _parse_pref_page(soup: BeautifulSoup, pref_name: str, cutoff: datetime) -> list[dict]:
    """都道府県ページの table.result_area を解析"""
    halls = []
    table = soup.select_one("table.result_area")
    if not table:
        return halls

    for tr in table.select("tr"):
        tds = tr.select("td")
        if len(tds) < 4:
            continue  # ヘッダー行・不完全行をスキップ

        # td[0]: 開店日
        date_str = tds[0].get_text(strip=True)
        open_date = _parse_date(date_str)
        if open_date and open_date < cutoff:
            continue  # 古すぎるデータを除外

        # td[1]: タイプ
        hall_type = tds[1].get_text(strip=True)
        is_grand_open = any(t in hall_type for t in GRAND_OPEN_TYPES)

        # td[2]: 店名 + リンク
        name_td = tds[2]
        name = name_td.get_text(strip=True)
        if not name:
            continue
        link = name_td.select_one("a[href*='/hall/details/']")
        if link:
            href = link.get("href", "")
            hall_url = (BASE_URL + href) if href.startswith("/") else href
        else:
            hall_url = ""

        # td[3]: 住所
        address = tds[3].get_text(strip=True) if len(tds) > 3 else pref_name

        # td[4]: パチンコ台数, td[5]: スロット台数
        machines = {}
        if len(tds) > 4:
            p = _to_int(tds[4].get_text(strip=True))
            if p:
                machines["pachinko"] = p
        if len(tds) > 5:
            s = _to_int(tds[5].get_text(strip=True))
            if s:
                machines["slot"] = s
        if machines:
            machines["total"] = machines.get("pachinko", 0) + machines.get("slot", 0)

        halls.append({
            "name":         name,
            "address":      address,
            "open_date":    date_str,
            "url":          hall_url,
            "source":       "ゴーパチ",
            "is_grand_open": is_grand_open,
            "machines":     machines,
            "lat":          None,
            "lng":          None,
        })

    return halls


def _parse_date(s: str) -> datetime | None:
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            pass
    return None


def _to_int(s: str) -> int | None:
    m = re.search(r"\d+", s)
    return int(m.group()) if m else None
