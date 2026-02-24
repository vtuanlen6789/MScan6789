# Deploy BizClaw lên Streamlit Community Cloud

## 0) Chuẩn bị trước khi deploy

- Đảm bảo code BizClaw đã push lên GitHub.
- Thư mục app gồm tối thiểu:
  - `BizClaw/dashboard.py`
  - `BizClaw/main.py`
  - `BizClaw/data_layer.py`
  - `BizClaw/engines/*`
  - `BizClaw/config.py`
  - `BizClaw/requirements.txt`

`requirements.txt` hiện tại đã đủ cho mode Yahoo:

- `pandas`
- `streamlit`
- `yfinance`

> Không cần `MetaTrader5` khi deploy cloud (không có terminal MT5 trên cloud).

---

## 1) Push code lên GitHub

Từ workspace root:

```bash
cd /Users/apple/Documents/FX_Dev/WEB_APP_Vercel
git add BizClaw
git commit -m "Prepare BizClaw for Streamlit Cloud deploy"
git push
```

---

## 2) Tạo app trên Streamlit Community Cloud

1. Truy cập: `https://share.streamlit.io/`
2. Đăng nhập bằng GitHub.
3. Chọn **New app**.
4. Chọn repo chứa project.
5. Cấu hình:
   - **Branch**: branch bạn vừa push
   - **Main file path**: `BizClaw/dashboard.py`
6. Bấm **Deploy**.

Sau vài phút sẽ có URL dạng:

`https://<app-name>.streamlit.app`

---

## 3) Test sau deploy

- Mở app URL.
- Bấm nút **Run Market Scan**.
- Kiểm tra 3 phần hiển thị:
  - `Market Overview`
  - `Pine V1_4 Technical Matrix`
  - `Top Analytical Focus`

Nếu scan không ra dữ liệu:

- Kiểm tra log app trong Streamlit Cloud.
- Xác nhận `config.py` đang là `DATA_SOURCE = "yahoo"`.

---

## 4) Nhúng vào Web APP_7Steps_OK (Option A)

Khi đã có URL Streamlit, nhúng vào page `SCAN MARKETS` bằng `iframe`:

```tsx
<iframe
  src="https://<app-name>.streamlit.app"
  title="BizClaw Scan Markets"
  style={{ width: '100%', height: '80vh', border: 'none' }}
/>
```

Và thêm link mở tab mới:

```tsx
<a href="https://<app-name>.streamlit.app" target="_blank" rel="noreferrer">
  Open full screen
</a>
```

---

## 5) Troubleshooting nhanh

1. **Module not found**
   - Kiểm tra `BizClaw/requirements.txt` đã commit.

2. **App deploy fail do đường dẫn file**
   - Đảm bảo `Main file path = BizClaw/dashboard.py` (đúng chữ hoa/thường).

3. **yfinance bị timeout tạm thời**
   - Thử rerun app trong Streamlit Cloud.

4. **Cảnh báo `use_container_width`**
   - Không chặn chạy app, chỉ là deprecation warning của Streamlit.

---

## 6) Chạy BizClaw ở chế độ JSON API (khuyến nghị cho Web App)

Khi muốn React render trực tiếp bảng dữ liệu (không iframe), chạy API backend:

```bash
cd BizClaw
source .venv/bin/activate
pip install -r requirements.txt
python api.py
```

API mặc định chạy ở:

- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/scan`

Query hữu ích:

- `/scan?refresh=true` để buộc scan mới
- `/scan?refresh=false` để dùng cache tạm

Biến môi trường tuỳ chọn:

- `BIZCLAW_CACHE_TTL` (giây, mặc định `180`)
- `BIZCLAW_CORS_ORIGINS` (vd: `https://your-vercel-app.vercel.app`)

Ví dụ:

```bash
BIZCLAW_CORS_ORIGINS="http://localhost:3000" BIZCLAW_CACHE_TTL=120 python api.py
```

---

## 7) Cấu hình Web App `APP_7Steps_OK`

Trong `APP_7Steps_OK/.env`:

```env
REACT_APP_BIZCLAW_API_URL=http://127.0.0.1:8000
REACT_APP_BIZCLAW_STREAMLIT_URL=https://mscan6789-n6lgffdmdbeyyfnfzyfnba.streamlit.app/
```

Sau đó chạy lại web app:

```bash
cd APP_7Steps_OK
npm start
```

Trang `/scan-markets` sẽ lấy data từ API và render trực tiếp.