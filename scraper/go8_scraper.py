"""
ゴーパチ (5pachi.com) グランドオープン情報スクレイパー

URL構造:
  一覧: POST https://5pachi.com/hall
        ty=1 (グランドオープン等), pref[]=都道府県コード
  詳細: GET  https://5pachi.com/hall/details/1/{hall_id}
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
    "Referer": "https://5pachi.com/",
}

BASE_URL = "https://5pachi.com"

# 都道府県コード (ゴーパチ pref[] の値)
PREF_CODES = {
    "北海道":1,"青森":2,"岩手":3,"宮城":4,"秋田":5,"山形":6,"福島":7,
    "茨城":8,"栃木":9,"群馬":10,"埼玉":11,"千葉":12,"東京":13,"神奈川":14,
    "新潟":15,"富山":16,"石川":17,"福井":18,"山梨":19,"長野":20,
    "岐阜":21,"静岡":22,"愛知":23,"三重":24,"滋賀":25,"京都":26,
    "大阪":27,"兵庫":28,"奈良":29,"和歌山":30,"鳥取":31,"島根":32,
    "岡山":33,"広島":34,"山口":35,"徳島":36,"香川":37,"愛媛":38,"高知":39,
    "福岡":40,"佐賀":41,"長崎":42,"熊本":43,"大分":44,"宮崎":45,
    "鹿児島":46,"沖縄":47,
}


def fetch(url, method="get", data=None, retries=3):
    for i in range(retries):
        try:
            if method == "post":
                resp = requests.post(url, data=data, headers=HEADERS, timeout=20)
            else:
                resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            logger.warning(f"Fetch failed ({i+1}/{retries}): {url} - {e}")
            time.sleep(2 ** i)
    return None


def scrape_go8(days_back=90, target_prefs=None):
    """
    ゴーパチからグランドオープン店舗を取得。

    Args:
        days_back: 直近何日分取得するか
        target_prefs: 都道府県名リスト (None=主要都道府県)
    Returns:
        list of dict
    """
    if target_prefs is None:
        target_prefs = list(PREF_CODES.keys())

    cutoff = datetime.now() - timedelta(days=days_back)
    results = []

    for pref_name in target_prefs:
        pref_code = PREF_CODES.get(pref_name)
        if not pref_code:
            continue

        logger.info(f"ゴーパチ取得: {pref_name}")
        halls = _scrape_pref(pref_code, pref_name, cutoff)
        results.extend(halls)
        logger.info(f"  → {len(halls)}件")
        time.sleep(1)

    logger.info(f"ゴーパチ合計: {len(results)}件")
    return results


def _scrape_pref(pref_code, pref_name, cutoff):
    """都道府県ごとにグランドオープン一覧を取得"""
    halls = []
    page = 1

    while True:
        soup = fetch(
            f"{BASE_URL}/hall",
            method="post",
            data={"ty": "1", "pref[]": str(pref_code), "page": str(page)},
        )
        if not soup:
            break

        # 店舗リンクを探す: /hall/details/1/{id} または /hall/details/2/{id}
        items = soup.select("a[href*='/hall/details/']")
        if not items:
            break

        found_old = False
        seen_ids = set()

        for a in items:
            href = a.get("href", "")
            m = re.search(r"/hall/details/(\d+)/(\d+)", href)
            if not m:
                continue
            hall_id = m.group(2)
            if hall_id in seen_ids:
                continue
            seen_ids.add(hall_id)

            # 親要素からテキスト情報取得
            parent = a.find_parent(["li", "div", "article", "tr"]) or a
            text = parent.get_text(" ", strip=True)

            # 日付取得
            open_date = _extract_date(text)
            if open_date:
                try:
                    dt = datetime.strptime(open_date, "%Y-%m-%d")
                    if dt < cutoff:
                        found_old = True
                        continue
                except ValueError:
                    pass

            # 店舗名
            name = a.get_text(strip=True) or text[:30]

            # 詳細ページから住所・台数取得
            detail_url = BASE_URL + href
            detail = _fetch_detail(detail_url)
            time.sleep(0.5)

            halls.append({
                "name": detail.get("name") or name,
                "address": detail.get("address", pref_name),
                "open_date": detail.get("open_date") or open_date,
                "url": detail_url,
                "source": "ゴーパチ",
                "is_grand_open": True,
                "machines": detail.get("machines", {}),
                "lat": None,
                "lng": None,
            })

        # 次ページ確認
        next_btn = soup.select_one("a.next, .pagination a[rel='next'], a[href*='page=']:last-child")
        if not next_btn or found_old:
            break
        page += 1
        time.sleep(1)

    return halls


def _fetch_detail(url):
    """詳細ページから店舗情報を取得"""
    soup = fetch(url)
    if not soup:
        return {}

    result = {}

    # 店舗名
    h1 = soup.select_one("h1, h2, .hall-name, .shop-name")
    if h1:
        result["name"] = re.sub(r"[（(].*", "", h1.get_text(strip=True)).strip()

    # 全テキスト
    text = soup.get_text(" ", strip=True)

    # 住所
    addr_el = soup.select_one(".address, .addr, [class*='address'], [itemprop='address']")
    if addr_el:
        result["address"] = addr_el.get_text(strip=True)
    else:
        # テキストから住所パターン検索
        m = re.search(r"((?:北海道|[東西南北]?[都道府県]).{2,50}?(?:丁目|番地|号|\d+[-－]\d+))", text)
        if m:
            result["address"] = m.group(1)

    # 開店日
    date = _extract_date(text)
    if date:
        result["open_date"] = date

    # 台数
    result["machines"] = _parse_machines(text)

    return result


def _extract_date(text):
    """テキストから日付を抽出"""
    m = re.search(r"(\d{4})[年/\-](\d{1,2})[月/\-](\d{1,2})", text)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # 令和
    m = re.search(r"令和(\d+)年(\d{1,2})月(\d{1,2})日", text)
    if m:
        year = 2018 + int(m.group(1))
        return f"{year}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return ""


def _parse_machines(text):
    """テキストから台数情報を抽出"""
    machines = {}
    patterns = {
        "total":        r"総台数[:\s：]*(\d+)",
        "pachinko":     r"パチンコ[:\s：]*(\d+)\s*台",
        "pachinko_4en": r"4円[パP]チ[:\s：]*(\d+)",
        "pachinko_1en": r"1円[パP]チ[:\s：]*(\d+)",
        "slot":         r"スロット?[:\s：]*(\d+)\s*台",
        "slot_20en":    r"20円スロ[:\s：]*(\d+)",
        "slot_5en":     r"5円スロ[:\s：]*(\d+)",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, text)
        if m:
            machines[key] = int(m.group(1))

    # 総台数がなければ合計
    if not machines.get("total") and (machines.get("pachinko") or machines.get("slot")):
        machines["total"] = machines.get("pachinko", 0) + machines.get("slot", 0)

    return machines
