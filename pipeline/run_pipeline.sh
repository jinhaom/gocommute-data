#!/usr/bin/env bash
# 跑完整條資料管道，產出可發佈的靜態庫資產。
#
# 用法：bash pipeline/run_pipeline.sh <work-dir>
#   產出：<work-dir>/assets/            庫檔案（明文 JSON，與 app 內置資產同格式）
#         <work-dir>/out/              中間產物
#         <work-dir>/DATA_DATE         官方資料截止日（YYYY-MM-DD）
#
# 設計約束：
#   * 只用標準庫 + curl/unzip（GitHub runner 都有，無需 pip install）
#   * 每一步失敗即中止（set -e），絕不產出半截資料
#   * 不依賴任何快取：這裡重跑的成本換取「映射一定對得上當日官方資料」
set -euo pipefail

WORK="${1:-work}"
PIPE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TD_HEADWAY="https://static.data.gov.hk/td/pt-headway-tc"
TD_XML="https://static.data.gov.hk/td/routes-fares-xml"
MTR_DATA="https://opendata.mtr.com.hk/data"

mkdir -p "$WORK"/{gtfs,mtr,out,assets,tdnames}

echo "==> 1/6 取官方資料截止日"
curl -fsS --retry 3 "$TD_HEADWAY/DATA_LAST_UPDATED_DATE.csv" -o "$WORK/date.csv"
DATE="$(tr -d '\r' < "$WORK/date.csv" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | head -1)"
if [ -z "$DATE" ]; then echo "✗ 無法從日期檔解析出日期" >&2; exit 1; fi
echo "    資料截止日 = $DATE"

echo "==> 2/6 下載運輸署 GTFS"
curl -fsSL --retry 3 -o "$WORK/gtfs.zip" "$TD_HEADWAY/gtfs.zip"
rm -rf "$WORK/gtfs"; mkdir -p "$WORK/gtfs"
unzip -q -o "$WORK/gtfs.zip" -d "$WORK/gtfs"
echo "    $(ls "$WORK/gtfs" | wc -l | tr -d ' ') 個檔案，$(du -m "$WORK/gtfs.zip" | cut -f1) MB"

echo "==> 3/6 下載港鐵開放數據 CSV"
for f in mtr_lines_and_stations mtr_lines_fares airport_express_fares; do
  curl -fsSL --retry 3 -o "$WORK/mtr/$f.csv" "$MTR_DATA/$f.csv"
done

echo "==> 4/6 建靜態庫（線路/站點/站序/車費/班次 + 港鐵）"
python3 "$PIPE_DIR/build_transit_db.py" \
  --gtfs "$WORK/gtfs" --out "$WORK/out" --mtr-dir "$WORK/mtr" \
  --assets-dir "$WORK/assets" --date "$DATE"

echo "==> 5/6 用運輸署 XML 補英文名"
python3 "$PIPE_DIR/enrich_names.py" --assets-dir "$WORK/assets" --xml-dir "$WORK/tdnames"

echo "==> 6/6 建四家营运商站 id 映射（九巴/龙运全量 + 城巴逐线 + 小巴/港铁）"
# 九巴/龙运有「全量端点」，2 个请求就能拿到全部站点与站序（其余三家没有全量端点，见 build_operator_map.py）
curl -fsSL --retry 3 -o "$WORK/kmb_stop.json"      "https://data.etabus.gov.hk/v1/transport/kmb/stop"
curl -fsSL --retry 3 -o "$WORK/kmb_routestop.json" "https://data.etabus.gov.hk/v1/transport/kmb/route-stop"
python3 "$PIPE_DIR/build_operator_map.py" \
  --db "$WORK/out" \
  --kmb-stop "$WORK/kmb_stop.json" --kmb-route-stop "$WORK/kmb_routestop.json" \
  --cache "$WORK/api_cache" --out "$WORK/out/operator_stop_map.json.gz"
gunzip -c "$WORK/out/operator_stop_map.json.gz" > "$WORK/assets/operator_stop_map.json"

echo "==> 合理性校驗（防上遊發半截資料）"
python3 - "$WORK/assets" <<'PY'
import json, os, sys
a = sys.argv[1]
d = json.load(open(os.path.join(a, "transit_static.json"), encoding="utf-8"))
c = d["counts"]
checks = [
    ("routes", c["routes"], 2000, 3200),
    ("stops", c["stops"], 8000, 11000),
    ("route_stops", c["route_stops"], 40000, 90000),
    ("mtr_stations", c["mtr_stations"], 90, 110),
]
bad = []
for name, got, lo, hi in checks:
    ok = lo <= got <= hi
    print(f"    {name:14} = {got:>7}  (期望 {lo}–{hi})  {'OK' if ok else '✗ 超出範圍'}")
    if not ok: bad.append(name)
fares = sum(1 for _ in open(os.path.join(a, "transit_fares.jsonl"), encoding="utf-8"))
print(f"    fares          = {fares:>7}  (期望 > 500000)")
if fares <= 500000: bad.append("fares")
for f in ("transit_static.json", "transit_fares.jsonl", "transit_headway.json",
          "transit_mtr_fares.tsv", "operator_stop_map.json", "manifest.json"):
    if not os.path.exists(os.path.join(a, f)):
        print(f"    ✗ 缺少 {f}"); bad.append(f)
if bad:
    print("✗ 校驗失敗：" + ", ".join(bad), file=sys.stderr); sys.exit(1)
print("    ✓ 全部通過")
PY

echo "$DATE" > "$WORK/DATA_DATE"
echo "==> 完成：$WORK/assets（$(du -sm "$WORK/assets" | cut -f1) MB）"
