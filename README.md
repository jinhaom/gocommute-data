# gocommute-data

[gocommute](https://github.com/jinhaom/gocommute)（香港通勤 App）的**資料建置倉庫**。

App 本身只負責**下載**成品資料；所有抓取與轉換都在這裡由 GitHub Actions 每天自動完成 ——
手機不需要下載 29 MB 原始資料、也不需要跑轉換（原本那樣既慢又耗記憶體）。

## 為什麼這樣設計

| | 做法 |
|---|---|
| 觸發 | 每天 03:17 UTC 檢查運輸署的「資料截止日」小檔案（**95 字節**）。日期沒變就**不下載任何東西**、約 1 分鐘結束 |
| 建置 | 日期變了才跑完整管道：GTFS（13 MB）＋運輸署 XML 英文名（26 MB）＋港鐵 CSV＋四家營運商站 id 映射 |
| 產出 | 一個約 **4 MB** 的 zip（gzip 壓縮後；原始 29 MB）＋ `version.json`（含每個檔案的 sha256） |
| 發佈 | GitHub Release，tag = `data-YYYY-MM-DD` —— **不可變、天然可回滾**（舊版本永遠還在） |
| 成本 | 公開倉庫的標準 runner **免費且不限分鐘**；不需要任何 token 或付費服務 |

## App 怎麼用

1. `GET https://raw.githubusercontent.com/jinhaom/gocommute-data/main/version.json`（幾百字節）
2. 與本機 manifest 比對 `data_date`；不同才下載 `version.json` 裡給的 zip 網址
3. 校驗 sha256 → 解壓到臨時目錄 → 校驗 counts 合理區間 → **原子替換**（保留上一版可回滾）
4. 任何一步失敗 → 丟棄本次更新，舊庫繼續用（絕不讓半截資料上線）

首啟仍用 APK 內置快照，零下載。

## 內容

| 檔案 | 說明 |
|---|---|
| `pipeline/run_pipeline.sh` | 一鍵管道（日期 → GTFS → 港鐵 CSV → 建庫 → 英文名 → 營運映射 → 合理性校驗） |
| `pipeline/build_transit_db.py` | 運輸署 GTFS + 港鐵開放數據 → 靜態庫（線路/站點/站序/車費/班次/港鐵站與車費） |
| `pipeline/enrich_names.py` | 運輸署路線車費 XML（三語）→ 補英文站名/路線名 |
| `pipeline/build_operator_map.py` | 四家營運商站 id 映射（九巴/龍運用全量端點、城巴逐線、小巴=運輸署 id 恆等、港鐵獨立線碼站碼） |
| `version.json` | App 輪詢用的版本檔（由工作流自動提交） |

## 資料來源（全部官方、公開）

- 運輸署 GTFS：`https://static.data.gov.hk/td/pt-headway-tc/gtfs.zip`
- 資料截止日：`https://static.data.gov.hk/td/pt-headway-tc/DATA_LAST_UPDATED_DATE.csv`
- 運輸署路線車費 XML（含英文名）：`https://static.data.gov.hk/td/routes-fares-xml/{ROUTE,RSTOP}_{BUS,GMB}.xml`
- 港鐵開放數據：`https://opendata.mtr.com.hk/data/*.csv`
- 九巴／龍運：`https://data.etabus.gov.hk/v1/transport/kmb/*`
- 城巴：`https://rt.data.gov.hk/v2/transport/citybus/*`
- 專線小巴：`https://data.etagmb.gov.hk/*`

## 手動觸發

Actions → update-data → Run workflow → 勾 `force` 可忽略日期比對、強制重建。

## 值得知道的取捨

- **發佈 Release 一定要斷言「已轉正 + 未認證可下載」**：`gh release create` 內部是「先建草稿 → 上傳資產 → 轉正」，
  若建立時 API 偶發 500（實測遇過）而用 `|| gh release upload` 兜底，就會留下一個**草稿**（未認證下載 404 = 手機永遠拿不到資料）
  而步驟仍報 success —— 典型的「綠色卻壞掉」。工作流現在會重試、明確 `--draft=false`、並用未認證 curl 斷言 HTTP 200。
- 管道**不使用快取**：城巴映射要抓約 3,400 個請求（冷啟動十幾分鐘）。因為只在官方資料變更時才跑（約每兩週一次），
  而快取會讓映射有機會對不上當日官方資料 —— 用時間換正確性。
- 定時任務可能被 GitHub 延遲（不是精確調度器）；資料本身約每兩週更新一次，無影響。
- 四家營運商 id 命名空間**互不混用**：九巴 16 位十六進制 / 城巴 6 位數字 / 小巴=運輸署 id / 港鐵站碼+線碼。
