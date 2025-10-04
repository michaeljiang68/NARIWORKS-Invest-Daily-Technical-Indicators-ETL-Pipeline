# NARIWORKS-Invest-Daily-Technical-Indicators-ETL-Pipeline

## 專案簡介
本專案為日頻技術指標 ETL 批次處理系統，結合 FastAPI、SQLAlchemy、pandas 與 PostgreSQL，實現自動化拉取 K線資料、計算多種技術指標、批次 upsert 寫入資料庫，並提供 API 介面供外部系統串接。

---

## 主要功能
- **K線資料自動拉取**：支援批次或單股日頻 K線資料取得。
- **技術指標計算**：內建多種常用技術指標（如 MA、RSI、MACD 等），可自動批次計算。
- **批次 upsert 寫入**：採用 ORM bulk upsert 實作，確保資料安全高效寫入。
- **API 介面**：以 FastAPI 提供分批 upsert、查詢等 RESTful API。
- **完整日誌記錄**：整合 loguru，所有 ETL/DB 操作皆有詳細日誌。
- **環境設定管理**：支援 .env 檔案，敏感資訊與參數集中管理。

---

## 專案結構
```
README.md
app/
    api/
        v1/
            endpoints/
                daily_features_etl.py   # API 端點，分批 upsert 技術指標
    core/
        logger.py                      # 日誌設定
        setups.py                      # 環境參數載入
    db/
        postgres.py                    # 資料庫連線、建表、Session 管理
    models/
        postgres_kline.py              # K線 ORM 與查詢
        postgres_daily_features_orm.py # 技術指標 ORM 與 bulk upsert
        postgres_daily_features_etl.py # 技術指標 ETL 計算流程
main.py                               # FastAPI 啟動、router 註冊
```

---

## 安裝與啟動
1. **安裝依賴**
   ```bash
   pip install -r requirements.txt
   ```
2. **設定環境參數**
   - 請於專案根目錄建立 `.env` 檔案，內容範例：
     ```env
     POSTGRES_HOST=localhost
     POSTGRES_PORT=5432
     POSTGRES_USER=your_user
     POSTGRES_PASSWORD=your_password
     POSTGRES_DB=your_db
     ```
3. **啟動 API 服務**
   ```bash
   uvicorn app.main:app --reload
   ```

---

## API 使用說明
- 主要端點：`/api/daily_features_etl/bulk_upsert`
- 支援批次技術指標 upsert，請參考 `app/api/v1/endpoints/daily_features_etl.py` 內註解與範例。

---

## 技術說明
- **FastAPI**：高效能 API 框架，支援 async、OpenAPI 文件。
- **SQLAlchemy ORM**：資料庫操作、bulk upsert、資料型別安全。
- **pandas**：技術指標計算、資料清理。
- **loguru**：日誌管理，方便除錯與追蹤。
- **pydantic / pydantic-settings**：環境參數、資料驗證。
- **psycopg2-binary**：PostgreSQL 驅動。
- **python-dotenv**：環境變數載入。

---

## 註解與文件
- 所有主要檔案、class、method 皆有詳細英文註解，便於維護與協作。
- 技術指標、API、ETL 流程均有完整說明。

---

## 常見問題排查
- 啟動失敗：請確認 .env 內容、依賴安裝、資料庫連線資訊。
- bulk upsert 失敗：請檢查 ORM 欄位型別、資料型態、NaN/null 處理。
- 查無資料：請確認 K線資料來源、API 輸入參數。
- 日誌無法輸出：請檢查 loguru 設定與 logger.py。

---

## 聯絡與協作
- 歡迎 issue、PR 或 email 討論技術指標擴充、API 需求、ETL 優化。

---
