# Flow: Streamlit -> Supabase JSON -> Web App Fetch

## Mục tiêu

- Dùng Streamlit để chạy scan.
- Sau scan, publish payload JSON lên Supabase Storage.
- Web App đọc trực tiếp JSON public URL để hiển thị.

---

## 1) Tạo Supabase Storage bucket

Trong Supabase project:

1. Vào **Storage**.
2. Tạo bucket mới tên `market-data`.
3. Đặt bucket ở chế độ **Public**.

Object mặc định sẽ dùng:

- `bizclaw/latest-scan.json`

---

## 2) Khai báo Secrets trên Streamlit Cloud

Trong app Streamlit, vào **Settings > Secrets** và thêm:

```toml
SUPABASE_URL = "https://<your-project-ref>.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "<your-service-role-key>"
SUPABASE_BUCKET = "market-data"
SUPABASE_OBJECT_PATH = "bizclaw/latest-scan.json"
SUPABASE_UPSERT = "true"

# Optional: tự publish sau mỗi lần bấm Run Market Scan
BIZCLAW_AUTO_PUBLISH_SUPABASE = "true"
```

> `SUPABASE_SERVICE_ROLE_KEY` là secret nhạy cảm, chỉ đặt ở backend/Streamlit secrets.

---

## 3) Publish JSON từ Streamlit

Sau khi deploy BizClaw mới:

1. Bấm `Run Market Scan`.
2. Nếu bật auto publish, JSON sẽ được upload ngay.
3. Hoặc bấm nút `Publish latest scan JSON to Supabase`.

Bạn sẽ nhận được public URL dạng:

`https://<project-ref>.supabase.co/storage/v1/object/public/market-data/bizclaw/latest-scan.json`

---

## 4) Cấu hình Web App đọc JSON public

Trong Vercel (hoặc `.env` local) của `APP_7Steps_OK`, set:

```env
REACT_APP_BIZCLAW_JSON_URL=https://<project-ref>.supabase.co/storage/v1/object/public/market-data/bizclaw/latest-scan.json

# Optional: khi bấm Refresh Scan trên web app sẽ trigger scan + publish mới
REACT_APP_BIZCLAW_TRIGGER_URL=https://<your-bizclaw-api-domain>/scan/publish?refresh=true
```

`/scan-markets` sẽ ưu tiên đọc URL này, không cần gọi API backend local.

Khi có `REACT_APP_BIZCLAW_TRIGGER_URL`, nút `Refresh Scan` sẽ:

1. Gọi endpoint trigger để chạy scan mới và publish JSON.
2. Tự fetch lại JSON public và cập nhật UI.

---

## 5) Kiểm tra nhanh

1. Mở JSON URL trên trình duyệt, phải thấy object có `results`, `overview`, `technical`, `top3`.
2. Mở trang Market Scan và bấm `Refresh Scan`.
3. Nếu chưa thấy dữ liệu mới, chạy lại scan + publish trên Streamlit rồi refresh web app.
