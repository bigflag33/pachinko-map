"""
住所 → 緯度経度 変換 (ジオコーディング)

優先順:
1. 国土地理院 (GSI) API  ... 無料・日本専用・高精度
2. Nominatim (OSM)       ... 無料・グローバル・レート制限あり

キャッシュファイル (geocache.json) で同じ住所の重複取得を防ぐ。
"""
import json
import time
import hashlib
import logging
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_PATH = Path(__file__).parent.parent / "geocache.json"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
GSI_URL = "https://msearch.gsi.go.jp/address-search/AddressSearch"

_cache: dict = {}


def load_cache():
    global _cache
    if CACHE_PATH.exists():
        try:
            _cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            _cache = {}


def save_cache():
    CACHE_PATH.write_text(json.dumps(_cache, ensure_ascii=False, indent=2), encoding="utf-8")


def geocode(address: str) -> tuple[float, float] | tuple[None, None]:
    """
    住所を緯度経度に変換。

    Returns:
        (lat, lng) or (None, None)
    """
    if not address:
        return None, None

    key = hashlib.md5(address.encode()).hexdigest()
    if key in _cache:
        return _cache[key]

    # 1. 国土地理院 API
    result = _geocode_gsi(address)
    if result[0] is not None:
        _cache[key] = result
        return result

    time.sleep(0.5)

    # 2. Nominatim (フォールバック)
    result = _geocode_nominatim(address)
    if result[0] is not None:
        _cache[key] = result
        return result

    _cache[key] = (None, None)
    return None, None


def _geocode_gsi(address: str) -> tuple[float, float] | tuple[None, None]:
    """国土地理院 住所検索API"""
    try:
        resp = requests.get(
            GSI_URL,
            params={"q": address},
            headers={"User-Agent": "PachinkoMap/1.0"},
            timeout=10,
        )
        data = resp.json()
        if data and isinstance(data, list) and len(data) > 0:
            coords = data[0].get("geometry", {}).get("coordinates", [])
            if len(coords) >= 2:
                lng, lat = float(coords[0]), float(coords[1])
                return lat, lng
    except Exception as e:
        logger.debug(f"GSI geocode failed for '{address}': {e}")
    return None, None


def _geocode_nominatim(address: str) -> tuple[float, float] | tuple[None, None]:
    """Nominatim (OpenStreetMap) ジオコーディング"""
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={
                "q": address + " 日本",
                "format": "json",
                "limit": 1,
                "countrycodes": "jp",
            },
            headers={"User-Agent": "PachinkoMap/1.0 (github.com)"},
            timeout=10,
        )
        data = resp.json()
        if data:
            lat = float(data[0]["lat"])
            lng = float(data[0]["lon"])
            return lat, lng
    except Exception as e:
        logger.debug(f"Nominatim geocode failed for '{address}': {e}")
    return None, None


def geocode_batch(halls: list[dict], sleep_sec: float = 0.3) -> list[dict]:
    """
    店舗リストの住所をまとめてジオコーディング。
    lat/lng が既に設定されている場合はスキップ。
    """
    load_cache()
    total = len(halls)
    geocoded = 0
    skipped = 0

    for i, hall in enumerate(halls):
        if hall.get("lat") and hall.get("lng"):
            skipped += 1
            continue

        address = hall.get("address", "")
        lat, lng = geocode(address)
        hall["lat"] = lat
        hall["lng"] = lng

        if lat:
            geocoded += 1
            logger.debug(f"[{i+1}/{total}] ✓ {hall['name']}: {lat:.4f}, {lng:.4f}")
        else:
            logger.warning(f"[{i+1}/{total}] ✗ ジオコード失敗: {hall['name']} / {address}")

        # レート制限対策
        if not _is_cached(address):
            time.sleep(sleep_sec)

    save_cache()
    logger.info(f"Geocoding done: {geocoded} success, {total - geocoded - skipped} failed, {skipped} skipped")
    return halls


def _is_cached(address: str) -> bool:
    key = hashlib.md5(address.encode()).hexdigest()
    return key in _cache
