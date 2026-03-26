"""
P-WORLD (p-world.or.jp) 店舗情報スクレイパー
対象: https://www.p-world.co.jp/

P-WORLDは全国のパチンコ店の台数情報が最も網羅的。
都道府県別に店舗一覧と台数(パチンコ/パチスロ)を取得する。
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
    "Accept-Language": "ja,en-US;q=0.9",
    "Referer": "https://www.p-world.co.jp/",
}

BASE_URL = "https://www.p-world.co.jp"

# 都道府県コード (p-world.co.jp のURL体系に合わせて調整)
PREFS = {
    "北海道": "01", "青森": "02", "岩手": "03", "宮城": "04", "秋田": "05",
    "山形": "06", "福島": "07", "茨城": "08", "栃木": "09", "群馬": "10",
    "埼玉": "11", "千葉": "12", "東京": "13", "神奈川": "14", "新潟": "15",
    "富山": "16", "石川": "17", "福井": "18", "山梨": "19", "長野": "20",
    "岐阜": "21", "静岡": "22", "愛知": "23", "三重": "24", "滋賀": "25",
    "京都": "26", "大阪": "27", "兵庫": "28", "奈良": "29", "和歌山": "30",
    "鳥取": "31", "島根": "32", "岡山": "33", "広島": "34", "山口": "35",
    "徳島": "36", "香川": "37", "愛媛": "38", "高知": "39", "福岡": "40",
    "佐賀": "41", "長崎": "42", "熊本": "43", "大分": "44", "宮崎": "45",
    "鹿児島": "46", "沖縄": "47",
}


def fetch_page(url: str, retries: int = 3, session=None) -> BeautifulSoup | None:
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


def scrape_pworld_by_prefs(target_prefs: list[str] | None = None, max_stores: int = 500) -> list[dict]:
    """
    P-WORLDから指定都道府県の店舗情報を取得。

    Args:
        target_prefs: 取得する都道府県名リスト (None=全国)
        max_stores: 最大取得店舗数

    Returns:
        list of dict with keys: name, address, url, source, machines, lat, lng
    """
    prefs_to_scrape = target_prefs if target_prefs else list(PREFS.keys())
    results = []
    session = requests.Session()

    for pref in prefs_to_scrape:
        if len(results) >= max_stores:
            break
        pref_code = PREFS.get(pref)
        if not pref_code:
            continue

        logger.info(f"Scraping P-WORLD: {pref}")
        stores = _scrape_pref(pref_code, pref, session, max_stores - len(results))
        results.extend(stores)
        time.sleep(1)

    logger.info(f"p-world: {len(results)} halls scraped")
    return results


def _scrape_pref(pref_code: str, pref_name: str, session, limit: int) -> list[dict]:
    """都道府県ページから店舗一覧を取得"""
    stores = []
    page = 1

    while len(stores) < limit:
        # P-WORLDの都道府県別店舗一覧URL (実際のURL構造に合わせて調整)
        url = f"{BASE_URL}/area/{pref_code}/?p={page}"
        soup = fetch_page(url, session=session)
        if not soup:
            break

        # 店舗リスト要素 (実際のHTML構造に合わせて調整が必要)
        items = (
            soup.select(".shop-list li") or
            soup.select("table.list tr[class*='shop']") or
            soup.select(".store-list .store-item") or
            soup.select("article.store") or
            []
        )

        if not items:
            break

        for item in items:
            if len(stores) >= limit:
                break
            store = _parse_pworld_item(item, pref_name)
            if store:
                stores.append(store)

        # 次ページ確認
        next_btn = soup.select_one("a.next") or soup.select_one(".pagination .next a")
        if not next_btn:
            break
        page += 1
        time.sleep(0.8)

    return stores


def _parse_pworld_item(item, pref_name: str) -> dict | None:
    """個別店舗の解析"""
    # 店舗名
    name_el = (item.select_one("h2") or item.select_one("h3") or
               item.select_one(".shop-name") or item.select_one("a"))
    if not name_el:
        return None
    name = name_el.get_text(strip=True)
    if not name:
        return None

    # 住所
    addr_el = (item.select_one(".address") or item.select_one(".addr") or
               item.select_one("[class*='address']"))
    address = addr_el.get_text(strip=True) if addr_el else f"{pref_name}"

    # URL
    link_el = item.select_one("a[href]")
    url = ""
    if link_el:
        href = link_el["href"]
        url = href if href.startswith("http") else BASE_URL + href

    # 台数情報 (一覧ページにある場合)
    text = item.get_text(" ", strip=True)
    machines = _parse_pworld_machines(text)

    # 台数が取れない場合は詳細ページから
    if url and not machines.get("total") and not machines.get("pachinko"):
        machines = _fetch_pworld_detail(url)

    return {
        "name": name,
        "address": address,
        "open_date": "",
        "url": url,
        "source": "P-WORLD",
        "is_grand_open": False,
        "machines": machines,
        "lat": None,
        "lng": None,
    }


def _parse_pworld_machines(text: str) -> dict:
    """P-WORLD形式の台数テキストを解析"""
    machines = {}

    # P-WORLDの一般的な台数表示パターン
    # 例: "パチンコ 320台 / スロット 180台"
    patterns = {
        "pachinko":     r"パチンコ\s*[：:]\s*(\d+)\s*台?",
        "slot":         r"スロット\s*[：:]\s*(\d+)\s*台?",
        "total":        r"総台数\s*[：:]\s*(\d+)\s*台?",
        "pachinko_4en": r"4円パチ[ンン]?\s*[：:]\s*(\d+)",
        "pachinko_1en": r"1円パチ[ンン]?\s*[：:]\s*(\d+)",
        "slot_20en":    r"20円スロット?\s*[：:]\s*(\d+)",
        "slot_5en":     r"5円スロット?\s*[：:]\s*(\d+)",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, text)
        if m:
            machines[key] = int(m.group(1))

    # 合計が取れていない場合は計算
    if not machines.get("total") and (machines.get("pachinko") or machines.get("slot")):
        machines["total"] = machines.get("pachinko", 0) + machines.get("slot", 0)

    return machines


def _fetch_pworld_detail(url: str) -> dict:
    """詳細ページから台数情報を取得"""
    try:
        time.sleep(0.5)
        soup = fetch_page(url)
        if not soup:
            return {}

        # 台数テーブルを探す
        text = soup.get_text(" ", strip=True)
        machines = _parse_pworld_machines(text)

        # テーブル形式の台数
        for row in soup.select("table tr"):
            cells = [td.get_text(strip=True) for td in row.select("th,td")]
            if len(cells) >= 2:
                label, val = cells[0], cells[1]
                num_m = re.search(r"(\d+)", val)
                if num_m:
                    n = int(num_m.group(1))
                    if "パチンコ" in label and "4円" in label:
                        machines["pachinko_4en"] = n
                    elif "パチンコ" in label and "1円" in label:
                        machines["pachinko_1en"] = n
                    elif "パチンコ" in label:
                        machines["pachinko"] = n
                    elif "スロット" in label and "20円" in label:
                        machines["slot_20en"] = n
                    elif "スロット" in label and "5円" in label:
                        machines["slot_5en"] = n
                    elif "スロット" in label:
                        machines["slot"] = n
                    elif "総台数" in label or "合計" in label:
                        machines["total"] = n

        if not machines.get("total") and (machines.get("pachinko") or machines.get("slot")):
            machines["total"] = machines.get("pachinko", 0) + machines.get("slot", 0)

        return machines
    except Exception:
        return {}
