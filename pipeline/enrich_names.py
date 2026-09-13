#!/usr/bin/env python3
"""用运输署官方 XML 给自建静态库补英文名（站名 + 路线名）。

## 为什么需要这个脚本

自建库的骨架来自运输署 GTFS —— **官方 GTFS 只有中文**（`stop_name` / `route_long_name`
都不含英文），所以换库之后英文搜索退化了。运输署另有一套「路线与车费」XML
（`static.data.gov.hk/td/routes-fares-xml/`），字段是三语的：
`STOP_NAMEC/NAMES/NAMEE`、`LOC_START_NAMEE` / `LOC_END_NAMEE`，
而且**键就是本库的主键**（`ROUTE_ID` = 本库 route id、`STOP_ID` = 本库 stop id），所以可以直接补进去。

## 官方 XML 的形状（实测）

- `ROUTE_{BUS,GMB,FERRY,TRAM}.xml`：一条 `ROUTE` 一行，含 `COMPANY_CODE`、
  `LOC_START_NAMEC/S/E`、`LOC_END_NAMEC/S/E`。`ROUTE_NAMEC/S/E` 只是**路线号**
  （如 `1`、`619`），不是名字 —— 名字要从首末站拼。
- `RSTOP_{BUS,GMB,FERRY,TRAM}.xml`：英文名是**按 (路线, 方向, 站序)** 存的，
  同一物理站在不同路线上可能出现多次（甚至同一站在同一路线两个方向也各一行），
  所以必须**聚合成站级**。
- 官方把「简称 / 全称」塞在同一个字段里，用 `/<br>` 分隔，实测 5,001 行是这样的：
  `HIU TSUI STREET/<br>Hiu Tsui Street, Siu Sai Wan Road`。
  两段都是官方名，但全大写那段在界面上很丑，且 KMB / CTB 两家的风格不同
  （KMB 用「简称/<br>全称」、CTB 直接给全称）。本脚本归一化时取**可读的那段**
  （小写字母最多、其次最长）—— 实测 5,001 行里有 4,998 行的第二段是可读形式，
  另外 3 行（官方数据问题，如英文栏里塞了简体中文）由打分规则自动挑对。

## 落库字段

- 每个 `stop`：`name_en`（站级英文名）、`names_en`（**仅当**各营运商的英文名确实不同时才有，
  结构照抄 `names`：`{agency: english}`）。
- 每条 `route`：`name_en` = `<起点英文> - <终点英文>`。
- `manifest.json`：刷新 `built_at` —— app 端 `TransitStore.ensureSeeded` 靠
  `date|built_at` 判断内置快照换版了没有，不刷新的话装了新 APK 库还是旧的。

## 覆盖情况（实测，2026-09-13）

- 站点：9,447 里有 9,441 拿到英文名；缺的 6 个全是**山顶缆车**站（99801–99806）——
  运输署没有 `RSTOP_PTRAM.xml`（`curl -sI` 返回 403），无源可补。
- 路线：2,452 里有 2,451 拿到；缺的 1 条是山顶缆车 5001（同样没有 `ROUTE_PTRAM.xml`）。
- `RSTOP_LRTFEEDER/NLB/LWB/MTRBUS` 一律 403 —— 这些营运商的路线本身在 `ROUTE_BUS.xml` 里
  （已覆盖），但它们的**站名英文**只能靠同站的其它营运商那次出现来兜（实测未覆盖 0 站）。

用法：
    python3 tools/enrich_names.py                  # 缺 XML 才下载
    python3 tools/enrich_names.py --refresh        # 强制重下
    python3 tools/enrich_names.py --report-only    # 只报告不写盘
"""
from __future__ import annotations

import argparse
import collections
import gzip
import json
import os
import re
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET

BASE = "https://static.data.gov.hk/td/routes-fares-xml"

# 有英文名的四套 XML（实测存在且非空）。其余（PTRAM / LRTFEEDER / NLB / LWB / MTRBUS）
# 都是 403，拿不到 —— 由脚本如实报告缺多少。
ROUTE_FILES = ("ROUTE_BUS", "ROUTE_GMB", "ROUTE_FERRY", "ROUTE_TRAM")
RSTOP_FILES = ("RSTOP_BUS", "RSTOP_GMB", "RSTOP_FERRY", "RSTOP_TRAM")

_BR = re.compile(r"\s*/?\s*<br>\s*", re.I)
_WS = re.compile(r"\s+")


def normalize_en(raw: str) -> str:
    """官方英文名归一化：拆掉 `/<br>` 的「简称/全称」包装，取可读的那段。

    打分 = (小写字母个数, 长度)：全大写的那段得 0 分，自然被可读形式压过；
    两段都全大写（官方偶有这种）时取更长的那个，信息更多。
    """
    parts = [p for p in (_WS.sub(" ", x).strip() for x in _BR.split(raw)) if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return max(parts, key=lambda p: (sum(1 for c in p if c.islower()), len(p)))


def clean(raw: str) -> str:
    """不做「取一段」的简单清洗（路线首末站名用）。"""
    return _WS.sub(" ", _BR.sub(" ", raw or "")).strip().strip("/").strip()


def fetch(xml_dir: str, name: str, refresh: bool) -> str:
    path = os.path.join(xml_dir, f"{name}.xml")
    if os.path.isfile(path) and os.path.getsize(path) > 0 and not refresh:
        return path
    os.makedirs(xml_dir, exist_ok=True)
    url = f"{BASE}/{name}.xml"
    print(f"  下载 {url} …", flush=True)
    tmp = path + ".part"
    with urllib.request.urlopen(url, timeout=180) as r, open(tmp, "wb") as f:
        f.write(r.read())
    os.replace(tmp, path)
    return path


def iter_rows(path: str, tag: str, fields: tuple[str, ...]):
    """流式读 XML（RSTOP_BUS 有 20 MB，不能整棵建树）。"""
    want = set(fields)
    for _, el in ET.iterparse(path, events=("end",)):
        if el.tag != tag:
            continue
        row = {k: (el.findtext(k) or "").strip() for k in want}
        el.clear()
        yield row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--assets-dir", default="core/data/src/main/assets/transit")
    ap.add_argument("--xml-dir", default="tools/.tmp/tdnames")
    ap.add_argument("--refresh", action="store_true", help="强制重下 XML")
    ap.add_argument("--report-only", action="store_true", help="只打印覆盖情况，不写盘")
    args = ap.parse_args()

    static_path = os.path.join(args.assets_dir, "transit_static.json")
    manifest_path = os.path.join(args.assets_dir, "manifest.json")
    with open(static_path, encoding="utf-8") as f:
        static = json.load(f)
    stops = static["stops"]
    routes = static["routes"]

    before_raw = os.path.getsize(static_path)
    with open(static_path, "rb") as f:
        before_gz = len(gzip.compress(f.read(), 9))

    print("① 下载官方 XML（缺哪个下哪个）")
    route_paths = {n: fetch(args.xml_dir, n, args.refresh) for n in ROUTE_FILES}
    rstop_paths = {n: fetch(args.xml_dir, n, args.refresh) for n in RSTOP_FILES}
    for n, p in {**route_paths, **rstop_paths}.items():
        print(f"   {n:14s} {os.path.getsize(p)/1048576:6.2f} MB")

    # ---- 路线英文名：起点 - 终点 ----
    print("② 路线英文名（LOC_START_NAMEE - LOC_END_NAMEE）")
    route_en: dict[str, str] = {}
    for path in route_paths.values():
        for row in iter_rows(path, "ROUTE", ("ROUTE_ID", "ROUTE_NAMEE",
                                             "LOC_START_NAMEE", "LOC_END_NAMEE")):
            rid = row["ROUTE_ID"]
            if not rid:
                continue
            a, b = clean(row["LOC_START_NAMEE"]), clean(row["LOC_END_NAMEE"])
            name = f"{a} - {b}" if a and b else (a or b)
            if name:
                route_en.setdefault(rid, name)

    # ---- 站点英文名：按 (路线, 方向, 站序) 聚合到站级 ----
    print("③ 站点英文名（聚合成站级；各营运商不同时另存 names_en）")
    # 路线 → 营运商（用**本库**的 agency，联营线 KMB+CTB 展开成两家）——
    # 这样英文名按本库的营运商口径归类，与 stops.names 的键一致。
    route_ags = {rid: [a.strip() for a in r["agency"].split("+")] for rid, r in routes.items()}
    per_agency: dict[str, dict[str, collections.Counter]] = collections.defaultdict(
        lambda: collections.defaultdict(collections.Counter))
    overall: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    unknown_route = 0
    for path in rstop_paths.values():
        for row in iter_rows(path, "RSTOP", ("ROUTE_ID", "STOP_ID", "STOP_NAMEE")):
            sid, rid, raw = row["STOP_ID"], row["ROUTE_ID"], row["STOP_NAMEE"]
            if not sid or sid not in stops or not raw:
                continue
            if rid not in route_ags:
                unknown_route += 1
                continue
            value = normalize_en(raw)
            if not value:
                continue
            overall[sid][value] += 1
            for ag in route_ags[rid]:
                per_agency[sid][ag][value] += 1

    def top(counter: collections.Counter) -> str:
        # 出现次数最多优先；同票按字典序，保证可复现
        return sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

    stop_en: dict[str, str] = {}
    names_en: dict[str, dict[str, str]] = {}
    for sid, st in stops.items():
        if not overall.get(sid):
            continue
        names = st.get("names") or {}
        # 站级名优先跟随 name_tc 所代表的那个营运商（GTFS 复合站名的第一段就是它），
        # 这样 name_en 与 name_tc 说的是同一个叫法；没有对应就退回出现最多的那个。
        primary = next((a for a in names if names[a] == st.get("name_tc")), None)
        if primary is None:
            primary = next(iter(names), None)
        value = top(per_agency[sid][primary]) if primary and per_agency[sid].get(primary) \
            else top(overall[sid])
        stop_en[sid] = value
        pa = {a: top(per_agency[sid][a]) for a in names if per_agency[sid].get(a)}
        # names_en 只在**各家叫法确实不同**时才写（相同的话 name_en 就够了，别灌水）
        if len(set(pa.values())) > 1:
            names_en[sid] = pa

    # ---- 报告 ----
    missing_stops = [sid for sid in stops if sid not in stop_en]
    miss_by_agency = collections.Counter()
    for sid in missing_stops:
        keys = tuple(sorted((stops[sid].get("names") or {}).keys())) or ("<no names>",)
        miss_by_agency[keys] += 1
    missing_routes = [rid for rid in routes if rid not in route_en]
    miss_route_agency = collections.Counter(routes[rid]["agency"] for rid in missing_routes)

    print()
    print(f"站点：{len(stop_en)}/{len(stops)} 拿到英文名；缺 {len(missing_stops)}"
          f"{'（按 names 键：' + str(dict(miss_by_agency)) + '）' if missing_stops else ''}")
    if missing_stops:
        print("   缺英文名的站：", [(s, stops[s]["name_tc"]) for s in missing_stops[:10]],
              "…" if len(missing_stops) > 10 else "")
    print(f"路线：{len(route_en)}/{len(routes)} 拿到英文名；缺 {len(missing_routes)}"
          f"{'（按 agency：' + str(dict(miss_route_agency)) + '）' if missing_routes else ''}")
    if missing_routes:
        print("   缺英文名的路线：",
              [(r, routes[r]["agency"], routes[r]["code"]) for r in missing_routes[:10]])
    print(f"names_en（各家英文名不同）的站：{len(names_en)}")
    if unknown_route:
        print(f"   注意：{unknown_route} 行 RSTOP 的路线不在本库里，已跳过")
    print("   例：", json.dumps({sid: {"name_tc": stops[sid]["name_tc"], "name_en": stop_en[sid],
                                     "names_en": names_en.get(sid)} for sid in list(stop_en)[:2]},
                              ensure_ascii=False, indent=1))

    if args.report_only:
        print("\n--report-only：未写盘")
        return 0

    # ---- 写回静态库 ----
    for sid, value in stop_en.items():
        # 键顺序：id, lat, lng, name_tc, names, name_en, names_en（新键追加在后面）
        stops[sid]["name_en"] = value
    for sid, pa in names_en.items():
        stops[sid]["names_en"] = pa
    for rid, name in route_en.items():
        routes[rid]["name_en"] = name

    with open(static_path, "w", encoding="utf-8") as f:
        json.dump(static, f, ensure_ascii=False, separators=(",", ":"))

    # ---- manifest：刷 built_at，app 端据此整份覆盖旧库 ----
    if os.path.isfile(manifest_path):
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)
        manifest["built_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        manifest["source"] = "運輸署 GTFS（pt-headway-tc）+ 港鐵開放數據 + 運輸署路線車費 XML（英文名）"
        manifest["names_enriched_by"] = "tools/enrich_names.py"
        manifest["stop_name_en"] = len(stop_en)
        manifest["route_name_en"] = len(route_en)
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=1)

    after_raw = os.path.getsize(static_path)
    with open(static_path, "rb") as f:
        after_gz = len(gzip.compress(f.read(), 9))
    mb = 1048576
    print()
    print(f"transit_static.json 原始 {before_raw/mb:.2f} → {after_raw/mb:.2f} MB "
          f"（+{(after_raw-before_raw)/mb:.2f}）")
    print(f"transit_static.json gzip {before_gz/mb:.2f} → {after_gz/mb:.2f} MB "
          f"（+{(after_gz-before_gz)/mb:.2f}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
