"""
ゴーパチ (go8.jp) グランドオープン情報スクレイパー
対象: https://www.go8.jp/open/
"""
import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

BASE_URL = "https://www.go8.jp"


def fetch_page(url: str, retries: int = 3) -> BeautifulSoup | None:
    for i in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            logger.warning(f"Fetch failed ({i+1}/{retries}): {url} - {e}")
            time.sleep(2 ** i)
    return None


def parse_machines(text: str) -> dict:
    """テキストから台数情報を抽出"""
    machines = {}
    patterns = {
        "total":        r"総台数[:\s：]*(\d+)",
        "pachinko":     r"パチンコ[:\s：]*(\d+)",
        "pachinko_4en": r"4円[パP]チ[:\s：]*(\d+)",
        "pachinko_1en": r"1円[パP]チ[:\s：]*(\d+)",
        "slot":         r"スロット?[:\s：]*(\d+)",
        "slot_20en":    r"20円[スS]ロ[:\s：]*(\d+)",
        "slot_5en":     r"5円[スS]ロ[:\s：]*(\d+)",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, text)
        if m:
            machines[key] = int(m.group(1))
    return machines


def scrape_go8(days_back: int = 90) -> list[dict]:
    """
    ゴーパチのグランドオープン一覧をスクレイピング

    Returns:
        list of dict with keys:
            name, address, open_date, url, source, machines
    """
    results = []
    cutoff = datetime.now() - timedelta(days=days_back)

    # ページネーション対応
    page = 1
    while True:
        url = f"{BASE_URL}/open/" if page == 1 else f"{BASE_URL}/open/page/{page}/"
        logger.info(f"Fetching go8 page {page}: {url}")
        soup = fetch_page(url)
        if soup is None:
            break

        # 店舗リストを探す (実際のHTML構造に合わせて調整が必要)
        # ゴーパチのHTMLは変わる場合があるため、複数のセレクタを試みる
        items = (
            soup.select("article.open-item") or
            soup.select(".shop-list li") or
            soup.select(".entry-list article") or
            soup.select("article") or
            []
        )

        if not items:
            logger.warning(f"No items found on page {page}, stopping")
            break

        found_old = False
        for item in items:
            try:
                hall = _parse_go8_item(item)
                if not hall:
                    continue

                # 日付フィルタ
                if hall.get("open_date"):
                    dt = datetime.strptime(hall["open_date"], "%Y-%m-%d")
                    if dt < cutoff:
                        found_old = True
                        continue

                results.append(hall)
            except Exception as e:
                logger.debug(f"Parse error: {e}")

        # 次ページ確認
        next_btn = soup.select_one("a.next") or soup.select_one(".pagination .next a")
        if not next_btn or found_old:
            break

        page += 1
        time.sleep(1)  # サーバー負荷軽減

    logger.info(f"go8: {len(results)} halls scraped")
    return results


def _parse_go8_item(item) -> dict | None:
    """個別店舗の解析"""
    # 店舗名
    name_el = (item.select_one("h2") or item.select_one("h3") or
               item.select_one(".shop-name") or item.select_one(".title"))
    if not name_el:
        return None
    name = name_el.get_text(strip=True)
    if not name:
        return None

    # 住所
    addr_el = (item.select_one(".address") or item.select_one(".addr") or
               item.select_one("[class*='address']"))
    address = addr_el.get_text(strip=True) if addr_el else ""

    # 開店日
    date_el = (item.select_one("time") or item.select_one(".date") or
               item.select_one("[class*='date']"))
    open_date = ""
    if date_el:
        raw_date = date_el.get("datetime", "") or date_el.get_text(strip=True)
        open_date = _normalize_date(raw_date)

    # URL
    link_el = item.select_one("a")
    url = BASE_URL + link_el["href"] if link_el and link_el.get("href", "").startswith("/") else ""

    # 台数 (詳細ページから取得する場合)
    text = item.get_text(" ", strip=True)
    machines = parse_machines(text)

    # 詳細ページがある場合は取得
    if url and not machines.get("total"):
        machines = _fetch_detail_machines(url)

    return {
        "name": name,
        "address": address,
        "open_date": open_date,
        "url": url,
        "source": "ゴーパチ",
        "is_grand_open": True,
        "machines": machines,
        "lat": None,
        "lng": None,
    }


def _fetch_detail_machines(url: str) -> dict:
    """詳細ページから台数情報を取得"""
    try:
        time.sleep(0.5)
        soup = fetch_page(url)
        if not soup:
            return {}
        text = soup.get_text(" ", strip=True)
        return parse_machines(text)
    except Exception:
        return {}


def _normalize_date(raw: str) -> str:
    """日付文字列を YYYY-MM-DD に正規化"""
    raw = raw.strip()
    # ISO形式
    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", raw)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # 和暦 (令和)
    m = re.search(r"令和(\d+)年(\d{1,2})月(\d{1,2})日", raw)
    if m:
        year = 2018 + int(m.group(1))
        return f"{year}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return ""
