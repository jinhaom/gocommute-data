#!/usr/bin/env python3
"""建立 operator_stop 映射：TD 站 id ↔ 各营运商自己的站 id。

为什么必须分开建（用户明确要求）：
  九巴／城巴／小巴／港铁是四套 id 空间，混用必错。本工具**每家独立处理、独立输出**，
  互不推断、互不兜底。

各家的处理方式不同，这也是它们的差异本身：
  · 九巴 KMB / 龙运 LWB：官方提供**全量** `kmb/stop`（站+坐标）与 `kmb/route-stop`（全站序），
    可以完全离线对齐并用坐标逐站校验（≤120 m 判同一站）。
  · 城巴 CTB：**没有全量端点**，只能按 (路线, 方向) 逐个抓；对齐顺序为准，另抽样坐标校验。
  · 小巴 GMB：站 id 与路线 id **本来就是运输署的 id**（`data.etagmb.gov.hk/stop/20014490`），
    映射是恒等的，只需校验、不需推断。
  · 港铁 MTR：完全是另一套（线路码 + 站码，如 ISL/SHW），不在本工具范围，单独处理。

**每条路线的两个方向都对齐**（不再二选一）：
  之前为省请求，一条路线只留**最佳的那一个**方向 —— 后果是联营线（KMB+CTB，如 619/N619）
  在某个具体方向上，另一家的站 id 缺失，只能查到一家班次。
  现在两个方向都留，JSON 键改成 `"<td_route_id>|<td_bound>"`（如 `"1263|1"`、`"1263|2"`），
  每条记录里的 `bound` 就是它对应的 TD 方向，`fetched_at` 是这份数据的抓取时间。

省请求的做法（两个方向 ≠ 抓取量翻倍）：
  · 九巴/龙运：官方端点本来就是全量返回、返回里带 `bound=O/I` → 分两张表**零额外请求**；
  · 城巴：没有全量端点，必须按 (路线,方向) 抓，所以
      ① 复用 `tools/.tmp/api_cache` 的**按 (路线,方向)** 响应缓存（可续跑）；
      ② 站的经纬度按**站**去重缓存（同一站被多条线共用，一次请求服务全部路线）；
      ③ 只对**当前在服务时段**的路线发新请求（用运输署 GTFS 的日历 + 首末班窗口判断，
         夜间 N 线白天跳过）；已有缓存的照用不误 —— 省的是请求，不是丢数据。

用法：
    python3 tools/build_operator_map.py --db tools/.tmp/out \\
        --kmb-stop tools/.tmp/kmb_stop.json --kmb-route-stop tools/.tmp/kmb_routestop.json \\
        --cache tools/.tmp/api_cache --out tools/.tmp/out/operator_stop_map.json.gz
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

CTB_RS = "https://rt.data.gov.hk/v2/transport/citybus/route-stop/CTB/{route}/{direction}"
CTB_STOP = "https://rt.data.gov.hk/v2/transport/citybus/stop/{stop}"
UA = {"User-Agent": "gocommute/0.1 (personal HK commute app; contact: local)"}
NEAR = 120.0     # 米，判同一物理站
SCHEMA = 2       # 2 = 键为 "<route_id>|<td_bound>" 且每条记录带 fetched_at（1 = 一条路线只留一个方向）


def metres(a, b) -> float:
    return math.dist(a, b) * 111_320


def iso_local(ts: float) -> str:
    """本地时间 ISO 8601（带 +08:00 偏移），给 fetched_at 用。"""
    return dt.datetime.fromtimestamp(ts).astimezone().isoformat(timespec="seconds")


class Stats:
    """请求效率的账本：实际请求 / 缓存命中 / 失败 / 不在服务时段。"""

    def __init__(self) -> None:
        self.requests = 0
        self.cache_hits = 0
        self.failures = 0
        # 不在服务时段的 (路线,方向)：这是"本可以省掉"的量；其中无缓存的那部分才是本次真省下的请求。
        self.off_service = 0
        self.off_service_no_cache = 0
        self.off_service_cached = 0

    def merge(self, other: "Stats") -> None:
        for k in ("requests", "cache_hits", "failures",
                  "off_service", "off_service_no_cache", "off_service_cached"):
            setattr(self, k, getattr(self, k) + getattr(other, k))

    def line(self) -> str:
        return (f"网络请求 {self.requests} | 缓存命中 {self.cache_hits} | 失败 {self.failures}"
                f" | 不在服务时段 {self.off_service} 个 (路线,方向)"
                f"（本次靠缓存仍拿到 {self.off_service_cached} 个；若是冷缓存，"
                f"这些加上另 {self.off_service_no_cache} 个请求都会被省掉）")


def fetch(url: str, cache_dir: str, tag: str, stats: Stats | None = None,
          allow_network: bool = True) -> tuple[dict | None, str | None, bool]:
    """带磁盘缓存的 GET。

    返回 `(body, fetched_at, from_cache)`；失败返回 `(None, None, False)`。
    缓存命中时 `fetched_at` 取缓存文件的 mtime（= 当初真正抓到的时间），
    新抓时取当前时间 —— 这样每条映射记录的 fetched_at 都是**这份数据的抓取时间**，供 TTL 用。
    """
    path = os.path.join(cache_dir, f"{tag}.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                body = json.load(f)
            if stats:
                stats.cache_hits += 1
            return body, iso_local(os.path.getmtime(path)), True
        except Exception:
            pass
    if not allow_network:
        return None, None, False
    for attempt in range(3):
        if attempt == 0 and stats:
            stats.requests += 1
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=45) as r:
                body = json.loads(r.read().decode())
            os.makedirs(cache_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(body, f, ensure_ascii=False)
            return body, iso_local(time.time()), False
        except Exception:
            time.sleep(1.0 + attempt)
    if stats:
        stats.failures += 1
    return None, None, False


def align_by_order(ours: list[str], theirs: list[str]) -> list[dict]:
    """按顺序一一配对（长度不同时取较短的一侧，并标记长度差异）。"""
    out = []
    for i, (a, b) in enumerate(zip(ours, theirs)):
        out.append({"td": a, "op": b, "seq": i + 1})
    return out


def align_monotonic(ours: list[str], theirs: list[str],
                    coord_ours: dict, coord_theirs: dict, near: float = NEAR) -> dict:
    """保序对齐（允许跳站），按「坐标距离 ≤ near」判为同一站。

    为什么不能按下标硬配：一边可能多/少几个站（特别班次、绕经、加停），
    只要中间有一处错位，后面全部会跟着错（实测 113 条线因此从「全对」掉成「部分对」）。
    这里用 LCS 式 DP：能配就配（+1），配不上就允许任一侧跳过一个站（+0），
    既得到正确配对，也顺带指出「我方多几个站 / 对方多几个站」。

    返回 {pairs, matched, skipped_ours, skipped_theirs}。
    """
    n, mt = len(ours), len(theirs)
    ok = [[False] * (mt + 1) for _ in range(n + 1)]
    for i in range(1, n + 1):
        a = coord_ours.get(ours[i - 1])
        for j in range(1, mt + 1):
            b = coord_theirs.get(theirs[j - 1])
            if a and b and metres(a, b) <= near:
                ok[i][j] = True
    dp = [[0] * (mt + 1) for _ in range(n + 1)]
    for i in range(1, n + 1):
        for j in range(1, mt + 1):
            best = max(dp[i - 1][j], dp[i][j - 1])
            if ok[i][j]:
                best = max(best, dp[i - 1][j - 1] + 1)
            dp[i][j] = best
    pairs, i, j = [], n, mt
    while i > 0 and j > 0:
        if ok[i][j] and dp[i][j] == dp[i - 1][j - 1] + 1:
            pairs.append({"td": ours[i - 1], "op": theirs[j - 1], "seq": len(pairs) + 1})
            i -= 1
            j -= 1
        elif dp[i - 1][j] >= dp[i][j - 1]:
            i -= 1
        else:
            j -= 1
    pairs.reverse()
    for k, p in enumerate(pairs, 1):
        p["seq"] = k
    return {"pairs": pairs, "matched": dp[n][mt],
            "skipped_ours": n - dp[n][mt], "skipped_theirs": mt - dp[n][mt]}


def assign_bounds(candidates: dict[str, list[dict]], wanted: list[str],
                  key_of) -> tuple[dict[str, dict], int]:
    """给每个 TD 方向各挑一个营运商侧的对应物（两个方向都要，不再二选一）。

    [candidates] 是 `{TD 方向: [候选]}`，候选里必须有 `score`（越大越好）。
    做法：按分数从高到低贪心 —— **先保证最优的组合不被另一个方向抢走**
    （一条线的两个方向通常正好对营运商侧的 O/I 两个方向；若直接各取各的最佳，
    有可能两个 TD 方向都选中同一个营运商方向，真数据就白拿了）。
    剩下的方向（不存在对应的营运商方向）如实留空，不硬凑。
    返回 `({TD 方向: 候选}, 两个方向被迫重复的条数)`。
    """
    taken: set = set()
    out: dict[str, dict] = {}
    all_cands = [c for b in wanted for c in candidates.get(b, [])]
    for c in sorted(all_cands, key=lambda c: c["score"], reverse=True):
        b, k = c["bound"], key_of(c)
        if b in out or k in taken:
            continue
        out[b] = c
        taken.add(k)
    # 还剩没分到的方向：用它自己的最佳（允许与另一方向重复），如实记 duplicated。
    duplicated = 0
    for b in wanted:
        if b in out or not candidates.get(b):
            continue
        best = max(candidates[b], key=lambda c: c["score"])
        out[b] = best
        duplicated += 1
    return out, duplicated


def kmb_block(db, kmb_stops, kmb_rs, fetched_at: str, verbose=True):
    """九巴 + 龙运：全量离线对齐 + 坐标逐站校验，**TD 的两个方向各出一条**。

    官方 `kmb/route-stop` 是全量快照（返回里带 `bound` O/I），所以两个方向都留
    **不需要任何额外请求**：同一份数据按 (路线, bound, service_type) 分组即可。
    """
    routes, stops, route_stops = db["routes"], db["stops"], db["route_stops"]
    # (route_code, bound, service_type) -> [operator stop ids]
    by_combo: dict[tuple[str, str, str], list[tuple[int, str]]] = defaultdict(list)
    for r in kmb_rs:
        by_combo[(r["route"], r["bound"], r["service_type"])].append((int(r["seq"]), r["stop"]))
    for k in by_combo:
        by_combo[k].sort()

    result: dict[str, dict] = {}
    coords = {sid: (m["lat"], m["lng"]) for sid, m in stops.items()}
    kmb_coords = {}
    for sid, k in kmb_stops.items():
        try:
            kmb_coords[sid] = (float(k["lat"]), float(k["long"]))
        except (TypeError, ValueError, KeyError):
            pass
    duplicated = 0
    for rid, meta in routes.items():
        if meta["agency"] not in ("KMB", "LWB", "KMB+CTB", "LWB+CTB"):
            continue
        code = meta["code"]
        combos = [(b, s) for (c, b, s) in by_combo if c == code]
        if not combos:
            continue
        bounds = route_stops.get(rid) or {}
        # 每个 TD 方向 × 每个 (营运商方向, service_type) 打分
        cands: dict[str, list[dict]] = {}
        for b, seq in bounds.items():
            ours = [x["stop"] for x in seq]
            if not ours:
                continue
            for kb, ks in combos:
                theirs = [s for _, s in by_combo[(code, kb, ks)]]
                al = align_monotonic(ours, theirs, coords, kmb_coords)
                skips = al["skipped_ours"] + al["skipped_theirs"]
                # 评分顺序：先要「两侧站表刚好吻合（无跳站）」，再要配对多，
                # 最后优先常规班次 service_type=1。
                #   —— 一条线在营运商侧有多个 service_type（常规/特别/绕经），
                #      只有站表吻合的那个才是我方这个 route_id 的对应班次。
                cands.setdefault(b, []).append({
                    "bound": b,
                    "score": (skips == 0, al["matched"], -skips, ks == "1"),
                    "operator_bound": kb,
                    "service_type": ks,
                    "al": al,
                    "len_theirs": len(theirs),
                    "len_ours": len(ours),
                })
        picked, dup = assign_bounds(cands, list(bounds.keys()), lambda c: (c["operator_bound"], c["service_type"]))
        duplicated += dup
        for b, c in picked.items():
            al = c["al"]
            result[f"{rid}|{b}"] = {
                "operator": "KMB",
                "operator_route": code,
                "operator_bound": c["operator_bound"],
                "service_type": c["service_type"],
                "bound": b,
                "fetched_at": fetched_at,
                "pairs": al["pairs"],
                "verified": al["matched"],
                "total": len(al["pairs"]),
                "len_ours": c["len_ours"],
                "len_operator": c["len_theirs"],
                "skipped_ours": al["skipped_ours"],
                "skipped_operator": al["skipped_theirs"],
            }
    if verbose and duplicated:
        print(f"  （其中 {duplicated} 个方向在营运商侧找不到独立对应方向，沿用该方向自身最佳 —— 如实保留，不硬凑）")
    return result


def ctb_block(db, cache, jobs, service_windows, now_sec, stats: Stats, verbose=True):
    """城巴：按 (路线, 方向) 抓取 → 抓站坐标（按站去重）→ 保序坐标对齐，**两个方向都留**。

    省请求：按 (路线,方向) 的响应缓存 + 按站的坐标缓存 + 只对当前在服务时段的路线发新请求。
    """
    routes, stops, route_stops = db["routes"], db["stops"], db["route_stops"]

    rid_code: dict[str, str] = {}
    rid_bounds: dict[str, list[str]] = {}
    for rid, meta in routes.items():
        if meta["agency"] not in ("CTB", "KMB+CTB", "LWB+CTB"):
            continue
        bounds = route_stops.get(rid) or {}
        if not bounds:
            continue
        rid_code[rid] = meta["code"]
        rid_bounds[rid] = sorted(bounds.keys())

    # 城巴的 outbound/inbound 与运输署的 ROUTE_SEQ **不是固定对应** （实测 103 号：
    # 我方 bound=1 对应的是城巴 inbound），所以两个方向都必须抓。
    directions = ("outbound", "inbound")
    keys = sorted({(code, d) for code in set(rid_code.values()) for d in directions})

    # 只对**当前在服务时段**的路线发新请求：用 TD 的日历 + 首末班窗口判断
    # （夜间 N 线白天、周日停开的线都跳过）。窗口未知时保守照抓，不假装省。
    def in_service_now(code: str) -> bool:
        if service_windows is None:
            return True
        rids = [r for r, c in rid_code.items() if c == code]
        known = [service_windows.get((r, b)) for r in rids for b in rid_bounds.get(r, [])]
        known = [w for w in known if w]
        if not known:
            return True
        return any(in_window(now_sec, *w) for w in known)

    plan: dict[tuple[str, str], bool] = {}
    for k in keys:
        code, d = k
        cached = os.path.exists(os.path.join(cache, f"ctb_rs_{code}_{d}.json"))
        live = in_service_now(code)
        plan[k] = live
        if not live:
            stats.off_service += 1
            if cached:
                stats.off_service_cached += 1
            else:
                stats.off_service_no_cache += 1
    if verbose:
        print(f"  城巴：需要 {len(keys)} 个 (路线,方向) 组合；其中当前不在服务时段 "
              f"{stats.off_service} 个（已有缓存不必发请求 {stats.off_service_cached} 个）")

    def rs_one(item):
        key, allow = item
        code, direction = key
        body, fetched_at, from_cache = fetch(
            CTB_RS.format(route=urllib.parse.quote(code), direction=direction),
            cache, f"ctb_rs_{code}_{direction}", None, allow)
        if not body:
            return key, False, None, fetched_at, from_cache
        data = body.get("data") or []
        seq = sorted(data, key=lambda x: x.get("seq", 0))
        return key, allow, [x["stop"] for x in seq], fetched_at, from_cache

    # (code, direction) -> (站序, fetched_at)；同一 code 被多条 TD 路线共用时只抓一次
    per_key: dict[tuple[str, str], tuple[list[str], str]] = {}
    with ThreadPoolExecutor(max_workers=jobs) as ex:
        for i, (key, allow, theirs, fetched_at, from_cache) in enumerate(
                ex.map(rs_one, sorted(plan.items())), 1):
            if theirs:
                per_key[key] = (theirs, fetched_at or iso_local(time.time()))
            if from_cache:
                stats.cache_hits += 1
            elif theirs:
                stats.requests += 1
            else:
                stats.failures += 1
            if verbose and i % 200 == 0:
                print(f"    … 路线站序 {i}/{len(plan)}")

    need = sorted({s for code, d in per_key for s in per_key[(code, d)][0]} - set(stops))
    if verbose:
        print(f"  城巴：需要 {len(need)} 个站坐标（按站去重；有缓存则跳过）")

    def stop_one(sid):
        body, _, from_cache = fetch(CTB_STOP.format(stop=sid), cache, f"ctb_stop_{sid}", None, True)
        d = (body or {}).get("data") or {}
        try:
            return sid, (float(d["lat"]), float(d["long"])), from_cache, body is not None
        except (TypeError, ValueError, KeyError):
            return sid, None, from_cache, body is not None

    coord_theirs = {}
    with ThreadPoolExecutor(max_workers=jobs) as ex:
        for i, (sid, c, from_cache, got) in enumerate(ex.map(stop_one, need), 1):
            if c:
                coord_theirs[sid] = c
            if from_cache:
                stats.cache_hits += 1
            elif got:
                stats.requests += 1
            else:
                stats.failures += 1
            if verbose and i % 500 == 0:
                print(f"    … 站坐标 {i}/{len(need)}")

    coord_ours = {sid: (m["lat"], m["lng"]) for sid, m in stops.items()}
    result: dict[str, dict] = {}
    duplicated = 0
    for rid, bounds in rid_bounds.items():
        code = rid_code[rid]
        cands: dict[str, list[dict]] = {}
        for b in bounds:
            ours = [x["stop"] for x in (route_stops.get(rid) or {}).get(b, [])]
            if not ours:
                continue
            for direction in directions:
                got = per_key.get((code, direction))
                if not got:
                    continue
                theirs, fetched_at = got
                al = align_monotonic(ours, theirs, coord_ours, coord_theirs)
                skips = al["skipped_ours"] + al["skipped_theirs"]
                # 方向对应关系以坐标对齐结果为准；只有完全打平时才偏向「bound1↔outbound」
                natural = (b == "1") == (direction == "outbound")
                cands.setdefault(b, []).append({
                    "bound": b,
                    "score": (skips == 0, al["matched"], -skips, natural),
                    "operator_bound": "O" if direction == "outbound" else "I",
                    "direction": direction,
                    "fetched_at": fetched_at,
                    "al": al,
                    "len_ours": len(ours),
                    "len_theirs": len(theirs),
                })
        picked, dup = assign_bounds(cands, bounds, lambda c: c["direction"])
        duplicated += dup
        for b, c in picked.items():
            al = c["al"]
            result[f"{rid}|{b}"] = {
                "operator": "CTB",
                "operator_route": code,
                "operator_bound": c["operator_bound"],
                "bound": b,
                "fetched_at": c["fetched_at"],
                "pairs": al["pairs"],
                "verified": al["matched"],
                "total": len(al["pairs"]),
                "len_ours": c["len_ours"],
                "len_operator": c["len_theirs"],
                "skipped_ours": al["skipped_ours"],
                "skipped_operator": al["skipped_theirs"],
            }
    if verbose and duplicated:
        print(f"  （其中 {duplicated} 个方向在营运商侧找不到独立对应方向，沿用该方向自身最佳 —— 如实保留，不硬凑）")
    return result


def _hhmmss(raw: str) -> int | None:
    try:
        h, m, s = raw.split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)
    except Exception:
        return None


def in_window(now_sec: int, lo: int, hi: int, margin: int = 1800) -> bool:
    """当前时刻是否落在首末班窗口内（含 ±30 分钟余量；跨零点的线按跨天处理）。"""
    if hi >= 86400:                     # 末班在次日（GTFS 用 24:30 这种写法）
        return now_sec >= lo - margin or now_sec <= hi - 86400 + margin
    return lo - margin <= now_sec <= hi + margin


def service_windows(gtfs_dir: str, date: dt.date, now_sec: int,
                    route_ids: set[str]) -> dict[tuple[str, str], tuple[int, int]] | None:
    """今天在运营的班次，按 (route_id, bound) 的**首班/末班**窗口（秒）。

    数据源是运输署 GTFS 的 `calendar` / `calendar_dates` / `trips` / `stop_times`
    —— 这是"这条线的这个方向今天到底跑不跑、跑到几点"的权威说明。
    拿不到（缺文件 / 今天没有任何在运营的班次）时返回 None：**一律照抓**，
    宁可多发请求，也不拿"猜的运营时间"去砍数据。
    """
    cal = os.path.join(gtfs_dir, "calendar.txt")
    trips_f = os.path.join(gtfs_dir, "trips.txt")
    st_f = os.path.join(gtfs_dir, "stop_times.txt")
    if not (os.path.isfile(cal) and os.path.isfile(trips_f) and os.path.isfile(st_f)):
        return None
    weekday_col = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")[date.weekday()]
    day = date.strftime("%Y%m%d")
    active: set[str] = set()
    with open(cal, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if r.get(weekday_col) == "1" and r.get("start_date", "") <= day <= r.get("end_date", "99999999"):
                active.add(r["service_id"])
    cd = os.path.join(gtfs_dir, "calendar_dates.txt")
    if os.path.isfile(cd):
        with open(cd, encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                if r.get("date") != day:
                    continue
                if r.get("exception_type") == "1":
                    active.add(r["service_id"])
                else:
                    active.discard(r["service_id"])
    if not active:
        return None

    trips: dict[str, tuple[str, str]] = {}
    with open(trips_f, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            rid = r.get("route_id", "")
            if r.get("service_id") not in active or rid not in route_ids:
                continue
            parts = r.get("trip_id", "").split("-")
            b = parts[1] if len(parts) >= 2 and parts[1] in ("1", "2") else "1"
            trips[r.get("trip_id", "")] = (rid, b)
    if not trips:
        return None

    first: dict[str, tuple[int, int]] = {}
    last: dict[str, tuple[int, int]] = {}
    with open(st_f, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 5 or row[0] not in trips:
                continue
            t = _hhmmss(row[1] or row[2])
            if t is None:
                continue
            seq = int(row[4])
            cur = first.get(row[0])
            if cur is None or seq < cur[0]:
                first[row[0]] = (seq, t)
            cur = last.get(row[0])
            if cur is None or seq > cur[0]:
                last[row[0]] = (seq, t)
    win: dict[tuple[str, str], tuple[int, int]] = {}
    for tid, key in trips.items():
        a, z = first.get(tid), last.get(tid)
        if not a or not z:
            continue
        lo, hi = win.get(key, (a[1], z[1]))
        win[key] = (min(lo, a[1]), max(hi, z[1]))
    return win


GMB_STOP = "https://data.etagmb.gov.hk/stop/{stop}"
MTR_LINES_CSV = "https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv"


def fetch_text(url: str, cache_dir: str, tag: str, stats: Stats | None = None) -> str | None:
    """带磁盘缓存的文本 GET（CSV 用）。"""
    path = os.path.join(cache_dir, f"{tag}.csv")
    if os.path.exists(path):
        with open(path, encoding="utf-8-sig") as f:
            if stats:
                stats.cache_hits += 1
            return f.read()
    for attempt in range(3):
        if attempt == 0 and stats:
            stats.requests += 1
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=45) as r:
                text = r.read().decode("utf-8-sig")
            os.makedirs(cache_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            return text
        except Exception:
            time.sleep(1.0 + attempt)
    if stats:
        stats.failures += 1
    return None


def mtr_block(cache, stats: Stats) -> dict:
    """港铁：**不属于运输署 GTFS**，用港铁自己的开放数据（opendata.mtr.com.hk）。

    它是独立的一套命名空间：线路码（ISL/TCL…）+ 站码（SHW/TSH…），
    正是港铁 ETA 接口 `getSchedule.php?line=&sta=` 需要的参数。
    """
    text = fetch_text(MTR_LINES_CSV, cache, "mtr_lines_and_stations", stats)
    if not text:
        return {"error": "抓取失败"}
    import csv as _csv
    import io
    lines: dict[str, dict] = {}
    stations: dict[str, dict] = {}
    for row in _csv.DictReader(io.StringIO(text)):
        code = (row.get("Line Code") or "").strip()
        if not code:
            continue
        direction = (row.get("Direction") or "").strip()
        scode = (row.get("Station Code") or "").strip()
        stations.setdefault(scode, {
            "code": scode,
            "id": (row.get("Station ID") or "").strip(),
            "name_tc": (row.get("Chinese Name") or "").strip(),
            "name_en": (row.get("English Name") or "").strip(),
        })
        buckets = lines.setdefault(code, {})
        buckets.setdefault(direction, []).append({
            "seq": int(float(row.get("Sequence") or 0)),
            "station": scode,
        })
    for code in lines:
        for d in lines[code]:
            lines[code][d].sort(key=lambda x: x["seq"])
    return {
        "note": "港铁独立的线路码/站码空间，与运输署 id 无关；参数直接给 ETA 接口用",
        "lines": lines,
        "stations": stations,
        "counts": {"lines": len(lines), "stations": len(stations)},
    }


def gmb_check(db, cache, stats: Stats) -> dict:
    """小巴：站 id 就是运输署 id ⇒ 映射恒等。

    校验方式：拿同一个 id 直接问官方接口，比较它给的 WGS84 坐标与**我们自己库里**
    该站在 GTFS 里的坐标 —— 一致即证明两边指的是同一个物理站
    （接口不回显站 id，所以不能用「返回值里有没有这个 id」来校验）。
    """
    stops = db["stops"]
    sample = [s for s in stops if s.startswith("2001")][:8]
    hit, dists = 0, []
    for sid in sample:
        body, _, _ = fetch(GMB_STOP.format(stop=sid), cache, f"gmb_stop_{sid}", stats)
        d = (body or {}).get("data") or {}
        w = ((d.get("coordinates") or {}).get("wgs84") or {})
        ours = stops.get(sid)
        try:
            lat, lng = float(w["latitude"]), float(w["longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        if ours:
            dd = metres((ours["lat"], ours["lng"]), (lat, lng))
            dists.append(dd)
            if dd <= 50:
                hit += 1
    return {
        "identity": True,
        "sampled": sample,
        "sampled_ok": hit,
        "max_offset_m": round(max(dists), 1) if dists else None,
        "note": "小巴站 id／路线 id 本就是运输署 id，映射恒等；"
                "用「同一 id 在官方接口的坐标 vs 本库坐标」校验一致",
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="tools/.tmp/out")
    ap.add_argument("--kmb-stop", default="tools/.tmp/kmb_stop.json")
    ap.add_argument("--kmb-route-stop", default="tools/.tmp/kmb_routestop.json")
    ap.add_argument("--cache", default="tools/.tmp/api_cache")
    ap.add_argument("--out", default="tools/.tmp/out/operator_stop_map.json.gz")
    ap.add_argument("--gtfs", default="tools/.tmp/gtfs",
                    help="运输署 GTFS（只用来判断「今天这个方向在不在服务时段」，省城巴请求）")
    ap.add_argument("--jobs", type=int, default=6)
    ap.add_argument("--skip-ctb", action="store_true")
    ap.add_argument("--no-service-filter", action="store_true",
                    help="不做运营时段判断，城巴所有 (路线,方向) 都抓（慢，但适用于冷缓存全量建表）")
    args = ap.parse_args()

    t0 = time.time()
    db = json.loads(gzip.open(f"{args.db}/transit_static.json.gz", "rb").read())
    kmb_stop_blob = json.load(open(args.kmb_stop, encoding="utf-8"))
    kmb_rs_blob = json.load(open(args.kmb_route_stop, encoding="utf-8"))
    kmb_stops = {s["stop"]: s for s in kmb_stop_blob["data"]}
    kmb_rs = kmb_rs_blob["data"]
    kmb_fetched_at = (kmb_rs_blob.get("generated_timestamp")
                      or iso_local(os.path.getmtime(args.kmb_route_stop)))
    print(f"载入：自建库路线 {len(db['routes'])} | 九巴站 {len(kmb_stops)} | 九巴站序 {len(kmb_rs)}"
          f" | 九巴快照 {kmb_fetched_at}")

    print("① 九巴/龙运（全量离线 + 保序坐标对齐；两个方向各出一条，零额外请求）")
    kmb = kmb_block(db, kmb_stops, kmb_rs, kmb_fetched_at)
    kmb_routes = {k.split("|")[0] for k in kmb}
    kmb_both = sum(1 for r in kmb_routes if f"{r}|1" in kmb and f"{r}|2" in kmb)
    exact = [v for v in kmb.values() if v["total"] > 0 and not v["skipped_ours"] and not v["skipped_operator"]]
    part = [v for v in kmb.values() if v["skipped_ours"] or v["skipped_operator"]]
    print(f"  路线 {len(kmb_routes)} 条 → 方向记录 {len(kmb)} 条"
          f"（两个方向都有 {kmb_both} 条 / 单方向 {len(kmb_routes) - kmb_both} 条 —— 后者是官方本来就只有单向的环线）")
    print(f"  两侧站表完全对应 {len(exact)} 条 / 有跳站（多站或少站）{len(part)} 条")
    print(f"  配对总数 {sum(v['total'] for v in kmb.values())} 对；其中坐标校验通过 "
          f"{sum(v['verified'] for v in kmb.values())} 对")
    if part:
        print("  有跳站的样例（'我方多' = 官方站表里没有、可能是我们的特别班走法）:")
        for v in sorted(part, key=lambda v: v["skipped_ours"] + v["skipped_operator"], reverse=True)[:8]:
            print(f"     {v['operator_route']:>6s} {v['operator_bound']}/{v['service_type']} "
                  f"(TD {v['bound']}) 配对 {v['total']} 对；我方多 {v['skipped_ours']} 站，"
                  f"对方多 {v['skipped_operator']} 站（我方 {v['len_ours']} / 对方 {v['len_operator']}）")

    ctb_stats = Stats()
    print("② 城巴（按线抓取 + 顺序对齐；缓存 + 按站去重 + 只抓运营中方向）")
    ctb: dict[str, dict] = {}
    if args.skip_ctb:
        print("  已按 --skip-ctb 跳过")
    else:
        # 运营时段判断只针对城巴（它才需要按 (路线,方向) 逐个抓；九巴是全量快照，不受影响）
        ctb_rids = {rid for rid, m in db["routes"].items()
                    if m["agency"] in ("CTB", "KMB+CTB", "LWB+CTB") and (db["route_stops"].get(rid) or {})}
        now = dt.datetime.now()
        windows = None if args.no_service_filter else service_windows(
            args.gtfs, now.date(), now.hour * 3600 + now.minute * 60 + now.second, ctb_rids)
        if args.no_service_filter:
            print("  已按 --no-service-filter 关闭运营时段判断（全部照抓）")
        elif windows is None:
            print("  运营时段判断不可用（GTFS 缺失或今天无在运营班次）→ 保守起见全部照抓")
        else:
            print(f"  运营时段判断：{now.strftime('%Y-%m-%d %H:%M')}（周{'一二三四五六日'[now.weekday()]}），"
                  f"覆盖 {len(windows)} 个 (路线,方向)")
        ctb = ctb_block(db, args.cache, args.jobs, windows,
                        now.hour * 3600 + now.minute * 60 + now.second, ctb_stats)
    ctb_routes = {k.split("|")[0] for k in ctb}
    ctb_both = sum(1 for r in ctb_routes if f"{r}|1" in ctb and f"{r}|2" in ctb)
    same_len = sum(1 for v in ctb.values() if v["len_ours"] == v["len_operator"])
    print(f"  路线 {len(ctb_routes)} 条 → 方向记录 {len(ctb)} 条（两个方向都有的 {ctb_both} 条）")
    print(f"  站数一致 {same_len} 条；配对总数 {sum(v['total'] for v in ctb.values())} 对")
    print(f"  请求效率：{ctb_stats.line()}")

    print("③ 小巴（恒等映射，抽查）")
    gmb = gmb_check(db, args.cache, Stats())
    print(f"  {gmb['note']}（抽查 {len(gmb['sampled'])} 个站位，id 一致 {gmb.get('sampled_ok', 0)} 个）")

    print("④ 港铁（独立数据源，非运输署 GTFS）")
    mtr_stats = Stats()
    mtr = mtr_block(args.cache, mtr_stats)
    if mtr.get("lines"):
        print(f"  线路码 {mtr['counts']['lines']} 条 / 站码 {mtr['counts']['stations']} 个"
              f"（如 {sorted(mtr['lines'])[:5]}…）")
    else:
        print(f"  抓取失败：{mtr.get('error')}")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    kmb_pairs = sum(v["total"] for v in kmb.values())
    ctb_pairs = sum(v["total"] for v in ctb.values())
    payload = {
        "schema": SCHEMA,
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "note": "四家营运商分别建表，互不推断。key = \"<td_route_id>|<td_bound>\"，每条记录对应 TD 的一个方向 "
                "（bound 字段即该 TD 方向，fetched_at 为该条数据的抓取时间）。"
                "KMB/LWB 经坐标校验且两个方向零额外请求；CTB 按官方站序对齐并坐标校验"
                "（缓存 + 按站去重 + 只抓运营中方向）；GMB 为恒等映射；MTR 独立线路码/站码空间。",
        "kmb": kmb,
        "ctb": ctb,
        "gmb": gmb,
        "mtr": mtr,
        "counts": {"kmb": len(kmb), "ctb": len(ctb),
                   "pairs_kmb": kmb_pairs, "pairs_ctb": ctb_pairs,
                   "kmb_routes": len(kmb_routes), "ctb_routes": len(ctb_routes),
                   "kmb_routes_two_bounds": kmb_both, "ctb_routes_two_bounds": ctb_both,
                   "ctb_requests": ctb_stats.requests, "ctb_cache_hits": ctb_stats.cache_hits,
                   "ctb_off_service": ctb_stats.off_service,
                   "ctb_off_service_no_cache": ctb_stats.off_service_no_cache,
                   "mtr_lines": (mtr.get("counts") or {}).get("lines", 0),
                   "mtr_stations": (mtr.get("counts") or {}).get("stations", 0)},
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    with gzip.open(args.out, "wb", compresslevel=9) as f:
        f.write(raw)
    print(f"输出 {args.out}：原始 {len(raw)/1048576:.2f} MB → gzip "
          f"{os.path.getsize(args.out)/1048576:.2f} MB；耗时 {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
