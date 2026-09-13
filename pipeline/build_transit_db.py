#!/usr/bin/env python3
"""TD 官方 GTFS → gocommute 自建本地库。

设计原则（用户明确要求）：
  **九巴／城巴／小巴／港铁是四套不同的 id 空间与接口约定，绝不能混用。**
  所以本库只保存「政府侧的静态骨架」，一律用 TD 的 id：
    - 路线：TD 的 ROUTE_ID（GTFS route_id，如九巴 1 号 = 1001、小巴 1 号 = 2006408）
    - 站点：TD 的 STOP_ID（GTFS stop_id，如 4001、20014490）
    - 营运商：GTFS agency_id（KMB / CTB / GMB / LWB / NLB / FERRY / TRAM / PTRAM / XB / DB / PI / LRTFeeder）
  各营运商的「自己的站 id / 路线码 / 方向约定」**不在本库**，由运行期按营运商分别抓官方
  ETA 接口并写入 operator_* 映射表（四家互不影响、互不污染）。

输入：tools/.tmp/gtfs/（gtfs.zip 解包）
输出：tools/.tmp/out/transit_static.json.gz、transit_fares.json.gz、transit_headway.json.gz

用法：
    python3 tools/build_transit_db.py --gtfs tools/.tmp/gtfs --out tools/.tmp/out
"""
from __future__ import annotations

import argparse
import shutil
import csv
import gzip
import json
import os
import re
import sys
import time
from collections import defaultdict

# ---------------------------------------------------------------- 工具

_NAME_PART = re.compile(r"^\[([A-Za-z+]+)\]\s*(.*)$")


def parse_stop_name(raw: str) -> tuple[str, dict[str, str]]:
    """拆 GTFS 的复合站名。

    样例：`[CTB] 曉翠街, 小西灣道|[KMB+CTB] 曉翠街/<BR>曉翠街, 小西灣道|[KMB] 曉翠街`
    返回 (默认中文名, {营运商: 该营运商叫法})。
    同名多营运商（[KMB+CTB]）会展开成两家各一条。
    """
    by_op: dict[str, str] = {}
    fallback = ""
    for part in raw.split("|"):
        part = part.strip()
        if not part:
            continue
        m = _NAME_PART.match(part)
        name = part
        ops: list[str] = []
        if m:
            ops = [o.strip() for o in m.group(1).split("+") if o.strip()]
            name = m.group(2).strip()
        name = name.replace("<BR>", " ").replace("/", " ").strip()
        name = re.sub(r"\s+", " ", name)
        if not fallback:
            fallback = name
        for op in ops:
            by_op.setdefault(op.upper(), name)
    return fallback, by_op


def read_tsv(path: str, want: tuple[str, ...] | None = None):
    """流式读 GTFS 的 txt（逗号分隔）。want=None 表示全字段。"""
    with open(path, encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if want is None:
                yield row
            else:
                yield {k: row.get(k, "") for k in want}


def rows(path: str) -> int:
    with open(path, encoding="utf-8-sig") as f:
        return sum(1 for _ in f) - 1


# ---------------------------------------------------------------- 主流程

def _last_updated(args) -> str:
    """库的版本日期：优先命令行，其次读运输署的 DATA_LAST_UPDATED_DATE.csv，最后退回今天。"""
    if args.date:
        return args.date
    csv_path = os.path.join(os.path.dirname(args.gtfs.rstrip("/")), "td", "DATA_LAST_UPDATED_DATE.csv")
    for cand in (csv_path, os.path.join(args.gtfs, "..", "td", "DATA_LAST_UPDATED_DATE.csv")):
        if os.path.isfile(cand):
            with open(cand, encoding="utf-8-sig") as f:
                for line in f:
                    line = line.strip()
                    if re.match(r"^\d{4}-\d{2}-\d{2}$", line):
                        return line
    return time.strftime("%Y-%m-%d")


def build_mtr(mtr_dir: str):
    """港铁（重铁）线路 / 车站 / 车费。

    数据源：`opendata.mtr.com.hk` 的四张 CSV（官方开放数据）：
      - `mtr_lines_and_stations.csv`：线码 + 方向 + 站码/站 ID + 中英文名 + 站序号（97 站 / 11 线，
        方向除 DT/UT 外还有支线：东铁綫落馬洲 LMC-*、将军澳綫康城 TKS-*）；
      - `mtr_lines_fares.csv`：站对车费（9,216 行，含八达通/单程/小童/长者等 9 种票种）；
      - `airport_express_fares.csv`：机场快綫单独计价（14 行）；
      - `light_rail_fares.csv`：轻铁票价（4,624 行）——**本期不进库**（轻铁站不在重铁站表里，
        需要另一套站表；先如实不做，不做半截）。

    注意：**官方这四张表都不含车站坐标**（已逐表确认）。所以港铁站在库里没有经纬度，
    附近站点功能对港铁站要另想办法（这条写进文档，不拿假坐标糊过去）。
    """
    import csv

    # 线码 → 官方线路名（CSV 里没有线路名，这是稳定公开事实）
    names = {
        "AEL": ("機場快綫", "Airport Express"),
        "TCL": ("東涌綫", "Tung Chung Line"),
        "TML": ("屯馬綫", "Tuen Ma Line"),
        "TWL": ("荃灣綫", "Tsuen Wan Line"),
        "ISL": ("港島綫", "Island Line"),
        "KTL": ("觀塘綫", "Kwun Tong Line"),
        "TKL": ("將軍澳綫", "Tseung Kwan O Line"),
        "EAL": ("東鐵綫", "East Rail Line"),
        "SIL": ("南港島綫", "South Island Line"),
        "DRL": ("迪士尼綫", "Disneyland Resort Line"),
    }

    ls_path = os.path.join(mtr_dir, "mtr_lines_and_stations.csv")
    with open(ls_path, encoding="utf-8-sig", newline="") as f:
        raw = [r for r in csv.DictReader(f) if (r.get("Line Code") or "").strip()]

    stations: dict[str, dict] = {}
    id2code: dict[str, str] = {}
    code2ids: dict[str, list[str]] = {}
    lines: dict[str, dict] = {}

    for r in raw:
        code, sid = r["Station Code"].strip(), r["Station ID"].strip()
        id2code[sid] = code
        code2ids.setdefault(code, [])
        if sid not in code2ids[code]:
            code2ids[code].append(sid)
        st = stations.setdefault(code, {
            "id": sid, "name_tc": r["Chinese Name"].strip(),
            "name_en": r["English Name"].strip(), "lines": [],
        })
        lc = r["Line Code"].strip()
        if lc not in st["lines"]:
            st["lines"].append(lc)

    # 方向站序（按 Sequence 排序；DT/UT 与支线各自成序）
    for r in raw:
        lc = r["Line Code"].strip()
        d = r["Direction"].strip()
        ln = lines.setdefault(lc, {
            "name_tc": names.get(lc, (lc, lc))[0],
            "name_en": names.get(lc, (lc, lc))[1],
            "dirs": {},
        })
        seq = ln["dirs"].setdefault(d, [])
        seq.append((float(r["Sequence"]), r["Station Code"].strip()))
    for ln in lines.values():
        for d, seq in ln["dirs"].items():
            seq.sort()
            ln["dirs"][d] = [c for _, c in seq]

    # ---- 车费：按站对 ----
    rows_out: list[tuple] = []

    def fare_rows(filename: str, kind: str, cols: dict[str, str], src="SRC_STATION_ID", dst="DEST_STATION_ID"):
        path = os.path.join(mtr_dir, filename)
        if not os.path.isfile(path):
            return 0
        n = 0
        with open(path, encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                a = id2code.get((r.get(src) or "").strip())
                b = id2code.get((r.get(dst) or "").strip())
                if not a or not b:
                    continue
                vals = []
                for key in cols.values():   # 注意是 values()：这里要的是 CSV 里的列名
                    v = (r.get(key) or "").strip()
                    try:
                        vals.append(f"{float(v):.1f}")
                    except ValueError:
                        vals.append("")
                rows_out.append((kind, a, b, *vals))
                n += 1
        return n

    hr = fare_rows("mtr_lines_fares.csv", "hr", {
        "oct_adult": "OCT_ADT_FARE", "single_adult": "SINGLE_ADT_FARE",
        "oct_child": "OCT_CON_CHILD_FARE", "oct_elderly": "OCT_CON_ELDERLY_FARE",
    })
    ael = fare_rows("airport_express_fares.csv", "ael", {
        "oct_adult": "OCT_ADT_FARE", "single_adult": "SINGLE_ADT_FARE",
        "oct_child": "OCT_CHD_FARE", "oct_elderly": "",
    }, src="ST_FROM_ID", dst="ST_TO_ID")

    return lines, stations, rows_out, hr, ael


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gtfs", default="tools/.tmp/gtfs")
    ap.add_argument("--out", default="tools/.tmp/out")
    ap.add_argument("--mtr-dir", default="tools/.tmp/mtr",
                    help="港铁官方 CSV 所在目录（mtr_lines_and_stations.csv 等）")
    ap.add_argument("--assets-dir", default=None,
                    help="把内置资产（明文 JSON，供 app 打包与离线测试用）写到这里")
    ap.add_argument("--operator-map", default=None,
                    help="operator_stop_map.json.gz 路径；提供时会一并写入资产目录")
    ap.add_argument("--date", default=None, help="库版本日期（YYYY-MM-DD），默认读官方日期文件")
    args = ap.parse_args()

    t0 = time.time()
    os.makedirs(args.out, exist_ok=True)
    g = lambda n: os.path.join(args.gtfs, n)  # noqa: E731

    # ---- agency ----
    agencies = {a["agency_id"]: {
        "id": a["agency_id"],
        "name_tc": a["agency_name"],
        "type": None,          # 由路线推断
        "url": a.get("agency_url", ""),
    } for a in read_tsv(g("agency.txt"))}

    # ---- routes（按营运商隔离：agency_id 是分界线）----
    route_agency: dict[str, str] = {}
    routes: dict[str, dict] = {}
    for r in read_tsv(g("routes.txt")):
        rid, ag = r["route_id"], r["agency_id"]
        route_agency[rid] = ag
        long_name = r.get("route_long_name", "")
        routes[rid] = {
            "id": rid,
            "agency": ag,
            "code": r.get("route_short_name", ""),      # 路线号（如 "1"）
            "name_tc": long_name,                       # 如 "竹園邨 - 尖沙咀碼頭"
            "type": int(r.get("route_type") or 3),      # 3 巴士 / 4 渡轮 / 0 电车 / 7 缆车
            "url": r.get("route_url", ""),
        }
    for rid, ag in route_agency.items():
        rt = routes[rid]["type"]
        agencies.setdefault(ag, {"id": ag, "name_tc": ag, "type": None, "url": ""})
        agencies[ag]["type"] = rt

    # ---- stops（TD 的 stop_id 为物理站点主键；营运商叫法另存）----
    stops: dict[str, dict] = {}
    op_name_count: dict[str, int] = defaultdict(int)
    for s in read_tsv(g("stops.txt")):
        sid = s["stop_id"]
        name_tc, by_op = parse_stop_name(s.get("stop_name", ""))
        for op in by_op:
            op_name_count[op] += 1
        stops[sid] = {
            "id": sid,
            "lat": round(float(s["stop_lat"]), 6),
            "lng": round(float(s["stop_lon"]), 6),
            "name_tc": name_tc,
            "names": by_op,        # {营运商: 叫法}（严格按营运商分开，不合并）
        }

    # ---- trips：只取「结构」用的代表性 trip（trip_id = route-bound-service-time）----
    #      目标：每个 (route, bound) 一份站序；bound 从 trip_id 的第二段取（= TD 的 ROUTE_SEQ）
    trip_route: dict[str, str] = {}
    trip_bound: dict[str, str] = {}
    for t in read_tsv(g("trips.txt"), ("route_id", "service_id", "trip_id")):
        tid = t["trip_id"]
        parts = tid.split("-")
        bound = parts[1] if len(parts) >= 2 and parts[1] in ("1", "2") else "1"
        trip_route[tid] = t["route_id"]
        trip_bound[tid] = bound

    # 每个 (route, bound) 收集候选 trip 及其站序
    seq_of: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for st in read_tsv(g("stop_times.txt"),
                       ("trip_id", "stop_id", "stop_sequence", "pickup_type",
                        "drop_off_type", "arrival_time")):
        seq_of[st["trip_id"]].append(
            (int(st["stop_sequence"]), st["stop_id"], st.get("pickup_type", ""),
             st.get("drop_off_type", ""), st.get("arrival_time", "")))

    # 选代表站序：按「同一 (route, bound) 下出现次数最多的站序模式」定主班次。
    #   —— 一条线可能有常规班/特别班/绕经班等多种走法，出现次数最多的才是这条线的
    #      常规走法（站数最多 ≠ 常规，实测会挑到特别班次，导致与营运商数据对不上）。
    #   同样次数时取站数更多的；再相同则取 trip_id 最小的，保证可复现。
    pattern_stat: dict[tuple[str, str], dict[tuple[str, ...], list]] = defaultdict(dict)
    for tid, seq in seq_of.items():
        rid, b = trip_route.get(tid), trip_bound.get(tid)
        if not rid:
            continue
        sig = tuple(x[1] for x in sorted(seq))
        slot = pattern_stat[(rid, b)].setdefault(sig, [0, tid, seq])
        slot[0] += 1
        if tid < slot[1]:
            slot[1] = tid

    best: dict[tuple[str, str], tuple[int, str, list]] = {}
    for key, pats in pattern_stat.items():
        # (出现次数, 站数) 排序后取第一
        sig, slot = max(pats.items(), key=lambda kv: (kv[1][0], len(kv[0]), -1))
        best[key] = (slot[0], slot[1], slot[2])

    route_stop: dict[str, dict[str, list]] = defaultdict(dict)
    for (rid, b), (_, tid, seq) in sorted(best.items()):
        route_stop[rid][b] = [
            {
                "seq": i + 1,
                "stop": sid,
                "pick": pk,
                "drop": dp,
                "arr": arr,
            }
            for i, (_, sid, pk, dp, arr) in enumerate(sorted(seq))
        ]

    # ---- fare_attributes + fare_rules → 分段收费（按站对）----
    fare_price: dict[str, str] = {}
    for a in read_tsv(g("fare_attributes.txt"), ("fare_id", "price", "currency_type")):
        fare_price[a["fare_id"]] = a["price"]

    fares: dict[str, list] = defaultdict(list)   # route_id -> [{o,d,p}]
    for r in read_tsv(g("fare_rules.txt"), ("fare_id", "route_id", "origin_id", "destination_id")):
        p = fare_price.get(r["fare_id"])
        if p is None:
            continue
        fares[r["route_id"]].append([r.get("origin_id", ""), r.get("destination_id", ""), p])

    # ---- frequencies（班次间隔）----
    headway: dict[str, list] = defaultdict(list)
    trip_route_full = None
    for f in read_tsv(g("frequencies.txt"), ("trip_id", "start_time", "end_time", "headway_secs")):
        rid = trip_route.get(f["trip_id"])
        if not rid:
            continue
        headway[rid].append([f["start_time"], f["end_time"], int(f["headway_secs"])])

    # ---------------------------------------------------------------- 输出
    def dump(name: str, obj) -> tuple[str, int, float]:
        path = os.path.join(args.out, name)
        blob = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode()
        with gzip.open(path, "wb", compresslevel=9) as f:
            f.write(blob)
        return path, len(blob), os.path.getsize(path)

    simple_routes = {}
    for rid, r in routes.items():
        rs = route_stop.get(rid) or {}
        simple_routes[rid] = {**r, "bounds": {b: len(v) for b, v in rs.items()}}

    # ---- 港铁（独立命名空间：站码 SHW / 线码 ISL，绝不与运输署 id 混用）----
    mtr_lines, mtr_stations, mtr_fares, mtr_hr_rows, mtr_ael_rows = build_mtr(args.mtr_dir)
    # 车费按行写文件（与 TD 车费同样思路：别整份进内存）
    mtr_fares_path = os.path.join(args.out, "transit_mtr_fares.tsv")
    with open(mtr_fares_path, "w", encoding="utf-8") as f:
        for row in sorted(mtr_fares, key=lambda r: (r[0], r[1], r[2])):
            f.write("\t".join(row) + "\n")

    static = {
        "schema": 1,
        "agencies": agencies,
        "routes": simple_routes,
        "stops": stops,
        "route_stops": {rid: b for rid, b in route_stop.items()},
        "mtr": {"lines": mtr_lines, "stations": mtr_stations},
        "counts": {
            "agencies": len(agencies), "routes": len(routes),
            "stops": len(stops),
            "route_stops": sum(len(v) for b in route_stop.values() for v in b.values()),
            "mtr_lines": len(mtr_lines), "mtr_stations": len(mtr_stations),
            "mtr_fares": len(mtr_fares),
        },
    }
    p1, raw1, gz1 = dump("transit_static.json.gz", static)
    p3, raw3, gz3 = dump("transit_headway.json.gz", {k: v for k, v in headway.items()})

    # 车费单独用**按行**格式（route_id \t 上车 \t 下车 \t 票价），按 route_id 排序。
    #   原因：87.9 万条车费若整份解析进内存，手机上要上百 MB；按行后可以流式过滤，
    #   只把当前查看的那条线的票价读出来（配合 LRU 缓存），内存占用与线路数无关。
    p2 = os.path.join(args.out, "transit_fares.jsonl.gz")
    with gzip.open(p2, "wt", encoding="utf-8", compresslevel=9) as f:
        for rid in sorted(fares):
            for o, d, price in fares[rid]:
                f.write(f"{rid}\t{o}\t{d}\t{price}\n")
    raw2 = sum(len(v) * 24 for v in fares.values())
    gz2 = os.path.getsize(p2)

    # ---- 内置资产（app 用）----
    # 关键：AGP 打包时会**自动把 .gz 资产解压**并存明文（实测 asset 名会从
    # `x.json.gz` 变成 `x.json`），所以内置资产直接放明文，三者（AGP/app/离线测试）
    # 读到的才是同一份东西；明文 JSON 本身在 APK 里仍会被 deflate 压到 ~3.6 MB。
    if args.assets_dir:
        os.makedirs(args.assets_dir, exist_ok=True)
        plain = {
            "transit_static.json": p1,
            "transit_fares.jsonl": p2,
            "transit_headway.json": p3,
        }
        for name, src in plain.items():
            dst = os.path.join(args.assets_dir, name)
            with gzip.open(src, "rb") as fin, open(dst, "wb") as fout:
                fout.write(fin.read())
        # 港铁车费（明文 TSV，与 TD 车费同思路：按行流式读）
        shutil.copyfile(mtr_fares_path, os.path.join(args.assets_dir, "transit_mtr_fares.tsv"))
        # 版本信息（UI 上显示「資料版本」）
        manifest = json.dumps({
            "schema": 1,
            "date": _last_updated(args),
            # built_at：同一日期**重新生成**时靠它区分。app 端据此判断「内置快照换版本了」，
            # 否则装了新 APK、库还是第一次播种的旧版本（实测踩过：港铁加进去了却搜不到）。
            "built_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            # origin=bundle：这份库来自 APK 内置快照（更新管线写入的会是 download，
            # app 端不会用内置快照去覆盖它）。
            "origin": "bundle",
            "source": "運輸署 GTFS（pt-headway-tc）+ 港鐵開放數據",
            "built_by": "tools/build_transit_db.py",
            "mtr_lines": len(mtr_lines),
            "mtr_stations": len(mtr_stations),
            "mtr_fares": len(mtr_fares),
        }, ensure_ascii=False, indent=1)
        with open(os.path.join(args.assets_dir, "manifest.json"), "w", encoding="utf-8") as f:
            f.write(manifest)
        if args.operator_map and os.path.isfile(args.operator_map):
            with gzip.open(args.operator_map, "rb") as fin, \
                    open(os.path.join(args.assets_dir, "operator_stop_map.json"), "wb") as fout:
                fout.write(fin.read())
        print(f"内置资产已写入 {args.assets_dir}（明文，AGP 打包时会自行 deflate）")

    print(f"营运商 {len(agencies)}: " + ", ".join(
        f"{k}×{sum(1 for r in routes.values() if r['agency']==k)}" for k in sorted(agencies)))
    print(f"路线 {len(routes)} | 站点 {len(stops)} | "
          f"站序记录 {static['counts']['route_stops']} | "
          f"分段收费路线 {len(fares)} 条（{sum(len(v) for v in fares.values())} 条记录）")
    print(f"站名按营运商分开存的：{dict(sorted(op_name_count.items()))}")
    for p, raw, gz in ((p1, raw1, gz1), (p2, raw2, gz2), (p3, raw3, gz3)):
        print(f"  {os.path.basename(p):26s} 原始 {raw/1048576:6.2f} MB → gzip {gz/1048576:5.2f} MB")
    print(f"耗时 {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
