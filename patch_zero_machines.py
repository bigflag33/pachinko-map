"""
0台の店舗だけ再スクレイプするパッチスクリプト

使い方（pachinko-mapフォルダで実行）:
    python3 patch_zero_machines.py

    # 上限件数を指定（テスト用）
    python3 patch_zero_machines.py --limit 20

出力: docs/pworld_all.json を上書き更新
"""

import re
import json
import time
import argparse
import sys
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── 定数 ──────────────────────────────────────────
DATA_FILE = Path("docs/pworld_all.json")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en;q=0.9",
}
DELAY = 0.8  # 秒

# ── 正規表現 ───────────────────────────────────────
LAT_RE    = re.compile(r"lat\s*:\s*['\"]([0-9]+\.[0-9]+)['\"]")
LNG_RE    = re.compile(r"lng\s*:\s*['\"]([0-9]+\.[0-9]+)['\"]")
MACHINE_RE = re.compile(r"設置台数[^\d]*(\d+)\s*台")
TOTAL_RE   = re.compile(r"総台数[^\d]*(\d+)\s*台")
ADDR_RE    = re.compile(r"住\s*所[　\s:：]*([^\n\r<]{5,60})")
ADDR_RE2   = re.compile(r"〒\s*\d{3}[-－]\d{4}\s*([^\n\r<]{5,60})")


def _extract_machines(text: str):
    """テキストからパチンコ/スロット台数を抽出して (total, pachinko, slot) を返す"""
    p = s = t = 0
    m = MACHINE_RE.search(text) or TOTAL_RE.search(text)
    if m:
        t = int(m.group(1))
    for pat in [r"パチンコ[^\d]*(\d+)\s*台", r"CR[^\d]*(\d+)\s*台"]:
        mp = re.search(pat, text)
        if mp:
            p = int(mp.group(1))
            break
    for pat in [r"スロット?[^\d]*(\d+)\s*台", r"スロ[^\d]*(\d+)\s*台"]:
        ms = re.search(pat, text)
        if ms:
            s = int(ms.group(1))
            break
    if t == 0 and (p > 0 or s > 0):
        t = p + s
    return t, p, s


def scrape_machines(session: requests.Session, url: str) -> Optional[dict]:
    """URLから台数・住所を再取得して返す"""
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None

        raw_head = resp.content[:4000]
        charset_match = re.search(rb'charset\s*=\s*["\']?\s*([A-Za-z0-9\-_]+)', raw_head, re.I)
        encoding = charset_match.group(1).decode("ascii", errors="ignore").strip("\"'") \
            if charset_match else "euc-jp"
        try:
            html = resp.content.decode(encoding, errors="replace")
        except (LookupError, UnicodeDecodeError):
            html = resp.content.decode("euc-jp", errors="replace")

        soup = BeautifulSoup(html, "html.parser")

        # ── 住所（空の場合のみ更新） ──
        address = ""
        addr_tag = (
            soup.select_one(".hallData-address")
            or soup.select_one(".shopAddress")
            or soup.select_one(".hall-address")
            or soup.select_one("[class*='address']")
            or soup.select_one("td.address")
        )
        if addr_tag:
            address = addr_tag.get_text(strip=True)
        if not address:
            for th in soup.find_all(["th", "td", "dt"]):
                if "住所" in th.get_text():
                    sib = th.find_next_sibling(["td", "dd"])
                    if sib:
                        address = sib.get_text(strip=True)
                        break
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
        machine_section = (
            soup.select_one(".machineInfo")
            or soup.select_one(".hallData-machine")
            or soup.select_one(".hall-machine")
            or soup.select_one(".machineData")
            or soup.select_one("[class*='machine']")
        )

        if machine_section:
            total, pachinko, slot = _extract_machines(machine_section.get_text())
            if total == 0:
                for row in machine_section.find_all(["tr", "div", "li"]):
                    _, row_p, row_s = _extract_machines(row.get_text())
                    if row_p > 0: pachinko = row_p
                    if row_s > 0: slot = row_s
                if pachinko > 0 or slot > 0:
                    total = pachinko + slot

        if total == 0:
            for th in soup.find_all(["th", "td", "dt", "span", "div"]):
                th_text = th.get_text(strip=True)
                if re.search(r"(設置|総|合計)?台数", th_text) and len(th_text) < 15:
                    sib = th.find_next_sibling(["td", "dd", "span"])
                    if sib:
                        m_n = re.search(r"(\d+)", sib.get_text())
                        if m_n and int(m_n.group(1)) > 10:
                            total = int(m_n.group(1))
                            break
        if total == 0:
            m_total = MACHINE_RE.search(html) or TOTAL_RE.search(html)
            if m_total:
                total = int(m_total.group(1))
        if total == 0:
            total, pachinko, slot = _extract_machines(html)

        return {"total": total, "pachinko": pachinko, "slot": slot, "address": address}

    except Exception as e:
        print(f"  エラー: {e}")
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=9999, help="再スクレイプ上限件数")
    args = parser.parse_args()

    if not DATA_FILE.exists():
        print(f"エラー: {DATA_FILE} が見つかりません。pachinko-mapフォルダで実行してください。")
        sys.exit(1)

    stores = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    zero_stores = [s for s in stores if s.get("machines", {}).get("total", 0) == 0 and s.get("url")]
    print(f"総店舗数: {len(stores)} / 0台の店舗: {len(zero_stores)}")

    if not zero_stores:
        print("0台の店舗はありません。終了。")
        return

    target = zero_stores[:args.limit]
    print(f"再スクレイプ対象: {len(target)} 件\n")

    session = requests.Session()
    session.headers.update(HEADERS)

    updated = fixed = 0
    for i, store in enumerate(target):
        name = store.get("name", "?")
        url  = store.get("url", "")
        print(f"[{i+1}/{len(target)}] {name} ... ", end="", flush=True)

        result = scrape_machines(session, url)
        updated += 1

        if result and result["total"] > 0:
            store["machines"]["total"]    = result["total"]
            store["machines"]["pachinko"] = result["pachinko"]
            store["machines"]["slot"]     = result["slot"]
            if result["address"] and not store.get("address"):
                store["address"] = result["address"]
            print(f"✅ {result['total']}台 (パチンコ{result['pachinko']} スロット{result['slot']})")
            fixed += 1
        else:
            print("❌ 取得できず（0台のまま）")

        time.sleep(DELAY)

    # 保存
    DATA_FILE.write_text(json.dumps(stores, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n完了！ {fixed}/{updated} 件が更新されました → {DATA_FILE}")
    print("\n次のステップ:")
    print("  1. GitHub Desktop で docs/pworld_all.json をコミット & プッシュ")
    print("  2. GitHub Actions で Run workflow → index.html 更新")


if __name__ == "__main__":
    main()
