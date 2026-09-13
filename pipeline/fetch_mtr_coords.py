#!/usr/bin/env python3
"""港鐵（重鐵）車站坐標補齊 —— 資料源 OpenStreetMap（Overpass API）。

為什麼需要這一步：
  港鐵官方開放數據四張 CSV（`mtr_lines_and_stations` / `mtr_lines_fares` /
  `airport_express_fares` / `light_rail_fares`）**逐表確認都沒有車站經緯度**（只有港鐵巴士站有）。
  「附近站點」要對港鐵站算距離排序，就必須另找來源 —— OSM 是唯一公開、可直接查的來源。

實測事實（踩過的坑，寫下來省得重摸）：
  * **必須同時查 node + way + relation**：香港的大型轉車站（香港／金鐘／中環…）多畫成 way（面），
    只查 node 只能命中 44/97，加上 way 之後 95/97。way 用 `out center` 取幾何中心。
  * 鏡像可用性不穩：`overpass-api.de` 回 504/HTML、`overpass.kumi.systems` 大查詢 504，
    `overpass.openstreetmap.fr` 實測可用 → 依序重試多個鏡像。
  * 剩下的（香港、鑽石山）在 OSM 裡標成 `public_transport=station` / `stop_position` 或只有名稱節點，
    要第二輪查詢（放寬標籤 + 名稱前綴）才拿得到。目標 97/97，沒拿到的一律如實列出來。

韌性要求（使用者明確要求）：
  **Overpass 全掛時不能讓整條資料管道失敗** —— 那會連線路資料都發不出去。
  所以：所有鏡像都失敗時，退回倉庫內隨版本保存的 `mtr_station_coords.json`（上次成功的結果），
  並把「本次用回退坐標」寫進 stdout 與 `--summary-file`，讓工作流摘要看得到。

許可證：OSM 資料是 **ODbL 1.0**，需標注來源（App 設定頁「關於」有標注，見 docs/DATA-SOURCES.md）。

用法：
    python3 pipeline/fetch_mtr_coords.py --mtr-dir work/mtr --out pipeline/mtr_station_coords.json
    python3 pipeline/fetch_mtr_coords.py --stations stations.json --out /tmp/coords.json
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# 香港全域（含離島）：南 22.15 / 西 113.83 / 北 22.57 / 東 114.45
BBOX = "22.15,113.83,22.57,114.45"

# 依序重試：實測 openstreetmap.fr 穩定；其餘為常見鏡像（大查詢容易 504，但小查詢可用）
MIRRORS = [
    "https://overpass.openstreetmap.fr/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.osm.jp/api/interpreter",
]

QUERY_MAIN = f"""
[out:json][timeout:180];
(
  node["railway"="station"]({BBOX});
  way["railway"="station"]({BBOX});
  relation["railway"="station"]({BBOX});
);
out center tags;
"""

# 第二輪：放寬到 public_transport（香港站、鑽石山站這類）+ 名稱節點
QUERY_SECOND = f"""
[out:json][timeout:180];
(
  node["public_transport"="station"]({BBOX});
  way["public_transport"="station"]({BBOX});
  relation["public_transport"="station"]({BBOX});
  node["railway"="stop"]({BBOX});
  node["railway"="halt"]({BBOX});
  node["station"]({BBOX});
  node["public_transport"="stop_position"]["name"]({BBOX});
);
out center tags;
"""

_CJK = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")


def cjk_run(name: str) -> str:
    """取名稱開頭的連續漢字段：OSM 的名字常是中英混寫（`金鐘 Admiralty`）。"""
    name = (name or "").strip()
    out = []
    for ch in name:
        if _CJK.match(ch) or ch in "（）()·-— ":
            out.append(ch)
        else:
            break
    return re.sub(r"\s+", "", "".join(out))


def norm_cjk(name: str) -> str:
    return re.sub(r"[（）()\s·]", "", cjk_run(name)).removesuffix("站").removesuffix("總站")


def norm_en(name: str) -> str:
    return re.sub(r"[^a-z]", "", (name or "").lower())


def read_stations(mtr_dir: str | None, stations_json: str | None) -> dict[str, dict]:
    """回傳 `{站碼: {"name_tc": …, "name_en": …}}`。"""
    if stations_json:
        with open(stations_json, encoding="utf-8") as f:
            raw = json.load(f)
        out = {}
        for code, st in (raw.get("stations") or raw).items():
            if isinstance(st, dict) and ("name_tc" in st or "nameEn" in st or "name_en" in st):
                out[code] = {
                    "name_tc": st.get("name_tc") or st.get("nameTc") or "",
                    "name_en": st.get("name_en") or st.get("nameEn") or "",
                }
        if out:
            return out
    path = os.path.join(mtr_dir or "", "mtr_lines_and_stations.csv")
    if not os.path.isfile(path):
        sys.exit(f"✗ 找不到港鐵站表：{path}（--mtr-dir 或 --stations 至少給一個）")
    out: dict[str, dict] = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            code = (r.get("Station Code") or "").strip()
            if not code:
                continue
            out[code] = {
                "name_tc": (r.get("Chinese Name") or "").strip(),
                "name_en": (r.get("English Name") or "").strip(),
            }
    return out


def overpass(query: str, mirrors: list[str]) -> tuple[dict, str, str, list[str]]:
    """依序打鏡像；全部失敗回 ([], "", "", 錯誤訊息)。"""
    errors: list[str] = []
    for url in mirrors:
        body = urllib.parse.urlencode({"data": query}).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={"User-Agent": "gocommute-data-pipeline/1.0 (station coords; ODbL)"},
        )
        try:
            with urllib.request.urlopen(req, timeout=240) as resp:
                raw = resp.read()
            data = json.loads(raw.decode("utf-8", "replace"))
            els = data.get("elements") or []
            if not els:
                errors.append(f"{url}: 回應沒有元素")
                continue
            ts = (data.get("osm3s") or {}).get("timestamp_osm_base") or ""
            print(f"    {url} → {len(els)} 個元素（OSM 鏡像資料時間 {ts or '未知'}）")
            return els, url, ts, errors
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, OSError) as e:
            errors.append(f"{url}: {e.__class__.__name__} {e}")
            print(f"    ✗ {url} 失敗：{e.__class__.__name__} {str(e)[:120]}")
            time.sleep(2)
    return [], "", "", errors


def candidates(elements: list[dict]) -> list[dict]:
    """把 Overpass 元素整理成可匹配的候選（含中心點、正規化名、標籤評分）。"""
    out = []
    for e in elements:
        tags = e.get("tags") or {}
        name = tags.get("name") or ""
        lat = e.get("lat") if e.get("lat") is not None else (e.get("center") or {}).get("lat")
        lon = e.get("lon") if e.get("lon") is not None else (e.get("center") or {}).get("lon")
        if lat is None or lon is None:
            continue
        railway = tags.get("railway", "")
        public = tags.get("public_transport", "")
        station = tags.get("station", "")
        score = 0
        if railway == "station":
            score += 10
        elif railway in ("halt", "stop"):
            score += 2
        if public == "station":
            score += 6
        elif public == "stop_position":
            score += 1
        if station in ("subway", "monorail", "light_rail", "train"):
            score += 3
        out.append(
            {
                "osm_type": e.get("type", ""),
                "osm_id": e.get("id"),
                "lat": round(float(lat), 6),
                "lng": round(float(lon), 6),
                "name_osm": name,
                "cjk": norm_cjk(name),
                "en": norm_en(tags.get("name:en") or ""),
                "raw_en": tags.get("name:en") or "",
                "tags": {"railway": railway, "public_transport": public, "station": station},
                "score": score,
            }
        )
    return out


def pick(station: dict, cands: list[dict], exact_only: bool) -> dict | None:
    """替一個港鐵站挑 OSM 元素。

    匹配優先序（先精確後寬鬆，任何一步都不接受「站名是別站的子串」的錯配）：
      1. 漢文名完全相同（去空白、去「站」）
      2. `name:en` 完全相同（去非字母、不分大小寫）
      3. 寬鬆輪：漢文前綴包含（如「九龍塘」vs「九龍塘站」）+ 英文包含
    同分時 `score` 高者勝（`railway=station` 優先），再同則 osm_id 小者（確定性）。
    """
    tc, en = norm_cjk(station["name_tc"]), norm_en(station["name_en"])

    def matched(c: dict, kind: str) -> bool:
        if kind == "exact":
            return (tc and c["cjk"] == tc) or (en and c["en"] and c["en"] == en)
        return (
            (tc and c["cjk"] and (c["cjk"].startswith(tc) or tc.startswith(c["cjk"]) and len(c["cjk"]) >= 2))
            or (en and c["en"] and (c["en"].startswith(en) or (len(c["en"]) >= 4 and en.startswith(c["en"]))))
        )

    for kind in ("exact",) if exact_only else ("exact", "loose"):
        hits = [c for c in cands if matched(c, kind)]
        if hits:
            best = max(hits, key=lambda c: (c["score"], -(c["osm_id"] or 0)))
            best = dict(best)
            best["match"] = kind
            return best
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mtr-dir", default=None, help="港鐵官方 CSV 目錄（讀 mtr_lines_and_stations.csv）")
    ap.add_argument("--stations", default=None, help="站表 JSON（{站碼:{name_tc,name_en}} 或建庫產物）")
    here = os.path.dirname(os.path.abspath(__file__))
    ap.add_argument("--out", default=os.path.join(here, "mtr_station_coords.json"), help="輸出檔（本次抓取的結果）")
    ap.add_argument("--fallback", default=os.path.join(here, "mtr_station_coords.json"),
                    help="Overpass 全掛時退回的檔（隨倉庫保存的上一份成功結果）")
    ap.add_argument("--mirrors", default=",".join(MIRRORS))
    ap.add_argument("--summary-file", default=None, help="把一行結果摘要寫到這裡（工作流摘要用）")
    args = ap.parse_args()

    mirrors = [m for m in args.mirrors.split(",") if m.strip()]

    def summary(line: str) -> None:
        print(line)
        if args.summary_file:
            with open(args.summary_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    stations = read_stations(args.mtr_dir, args.stations)
    print(f"==> 港鐵站表：{len(stations)} 站")

    print("==> 第 1 輪 Overpass（railway=station；node+way+relation）")
    els, mirror, osm_ts, errs = overpass(QUERY_MAIN, mirrors)
    cands = candidates(els) if els else []

    found: dict[str, dict] = {}
    for code, st in stations.items():
        hit = pick(st, cands, exact_only=True)
        if hit:
            found[code] = hit

    missing = [c for c in stations if c not in found]
    if missing:
        print(f"==> 第 2 輪 Overpass（public_transport=station / 名稱節點）；第 1 輪缺 {len(missing)} 站："
              + ", ".join(stations[c]["name_tc"] for c in missing))
        els2, mirror2, osm_ts2, errs2 = overpass(QUERY_SECOND, mirrors)
        errs += errs2
        if els2:
            mirror = mirror2 or mirror
            osm_ts = osm_ts or osm_ts2
            cands += candidates(els2)
        for code in missing:
            hit = pick(stations[code], cands, exact_only=False)
            if hit:
                found[code] = hit

    still = [c for c in stations if c not in found]

    if not found:
        # Overpass 全掛：回退到倉庫保存的那一份（管線繼續，不讓線路資料也發不出去）
        if os.path.isfile(args.fallback):
            with open(args.fallback, encoding="utf-8") as f:
                prev = json.load(f)
            n = len(prev.get("stations") or {})
            prev["fallback_used"] = True
            prev["fallback_reason"] = "; ".join(errs[:3])
            if os.path.abspath(args.fallback) != os.path.abspath(args.out):
                with open(args.out, "w", encoding="utf-8") as f:
                    json.dump(prev, f, ensure_ascii=False, indent=1)
                    f.write("\n")
            summary(f"⚠️ 本次用回退坐標：Overpass 全數失敗（{' | '.join(errs[:3])}）"
                    f"→ 退回 {os.path.basename(args.fallback)}（{n} 站，抓取於 {prev.get('fetched_at', '未知')}）")
            return 0
        summary("✗ Overpass 全數失敗，且沒有回退檔可用 → 本次沒有任何港鐵站坐標")
        return 0  # 刻意仍然回 0：管道不因座標缺失而中止（缺坐標會在校驗摘要裡如實列出）

    # 用 OSM 鏡像宣告的資料時間（`osm3s.timestamp_osm_base`）當「資料時間」
    out = {
        "schema": 1,
        "source": "OpenStreetMap（Overpass API）",
        "source_url": mirror,
        "license": "ODbL 1.0 — © OpenStreetMap contributors",
        "attribution": "港鐵車站坐標：© OpenStreetMap 貢獻者（ODbL）",
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "osm_data_timestamp": osm_ts,
        "query_bbox": BBOX,
        "matched": len(found),
        "total": len(stations),
        "unmatched": [{"code": c, "name_tc": stations[c]["name_tc"]} for c in still],
        "stations": {
            code: {
                "name_tc": stations[code]["name_tc"],
                "name_en": stations[code]["name_en"],
                "lat": hit["lat"],
                "lng": hit["lng"],
                "osm_type": hit["osm_type"],
                "osm_id": hit["osm_id"],
                "osm_name": hit["name_osm"],
                "match": hit["match"],
            }
            for code, hit in sorted(found.items())
        },
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
        f.write("\n")

    summary(f"港鐵站坐標：OSM 命中 {len(found)}/{len(stations)}"
            + (f"（未命中：{', '.join(stations[c]['name_tc'] for c in still)}）" if still else "（全部命中）")
            + f"；鏡像 {mirror}；OSM 資料時間 {out['osm_data_timestamp'] or '未知'}")
    if errs:
        print(f"    （期間失敗的鏡像：{' | '.join(errs[:3])}）")
    print(f"    已寫入 {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
