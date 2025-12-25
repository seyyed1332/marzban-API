# Marzban Panel Hub (Web + Telegram)

این پروژه یک **پنل تحت وب** برای مدیریت چندین پنل **Marzban** است که می‌تواند:

- چندین پنل/‌API مرزبان را اضافه و مدیریت کند (Multi-panel)
- لیست یوزرها را بگیرد و لینک‌های هر یوزر را دریافت و «resolve» کند
- `revoke_sub` (ریست UUID/Subscription token) و `reset usage` را از طریق UI انجام دهد
- برای هر یوزر زمان‌بندی دقیق بگذارد (مثلاً هر 7 ساعت یا 20 ساعت) و در زمان مقرر ریست کند
- بعد از ریست، لینک‌های جدید + گزارش مصرف + زمان ریست بعدی را در تلگرام ارسال کند
- تنظیمات ربات تلگرام (توکن، admin user id ها، وبهوک) را از داخل پنل ذخیره و مدیریت کند

## نصب

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## تنظیمات

فایل `.env` بسازید (از `.env.example` کپی کنید) و حداقل این‌ها را تنظیم کنید:

- `APP_SECRET_KEY` (حتماً یک مقدار طولانی و تصادفی)
- `WEB_HOST` و `WEB_PORT` (اختیاری)
- `SIGNUP_CODE` (اختیاری؛ اگر بگذارید ثبت‌نام فقط با کد ممکن است)

نکته: تنظیمات پنل‌های مرزبان و ربات تلگرام **فقط داخل خود سایت** انجام می‌شود (در `.env` نگهداری نمی‌شود).

## اجرا (پنل وب)

```powershell
python web.py
```

سپس باز کنید: `http://127.0.0.1:8000`

### اولین بار

1) اگر اولین یوزر هستید، می‌توانید از `/signup` ثبت‌نام کنید (یا اگر `SIGNUP_CODE` دارید، با کد).
2) از صفحه `Panels` پنل مرزبان را اضافه کنید (Base URL + یوزر/پسورد ادمین).
3) از صفحه `Telegram` توکن ربات و admin user id ها را وارد کنید.
4) برای ارسال گزارش‌های زمان‌بندی، باید `chat_id` داشته باشید:
   - در صفحه `Panel Edit` مقدار `default_chat_id` را ست کنید، یا
   - برای هر یوزر داخل صفحه یوزر، `Bind chat_id` انجام دهید.

## وبهوک تلگرام

- endpoint وبهوک داخل پروژه: `/telegram/webhook`
- داخل صفحه `Telegram` یک `Webhook URL` مثل این بگذارید و `Set webhook` را بزنید:
  - `https://YOUR_DOMAIN/telegram/webhook`

برای لوکال‌هاست، وبهوک فقط وقتی کار می‌کند که آدرس شما از اینترنت قابل دسترسی و HTTPS باشد (مثلاً با reverse proxy).

### دستورات ساده وبهوک

- `/whoami` → ارسال `user_id` و `chat_id`
- `/start <panel_id>` → فقط برای ادمین‌ها؛ ذخیره `default_chat_id` همان پنل

## زمان‌بندی

زمان‌بندی‌ها از داخل پنل در صفحه یوزر انجام می‌شود:

- `Schedule (hours)` → هر N ساعت یک بار `revoke_sub` انجام می‌شود و لینک‌های جدید + گزارش مصرف به تلگرام ارسال می‌شود.

## فایل‌های مهم

- `marzban_bot/web_panel.py` (FastAPI web app + scheduler + webhook)
- `marzban_bot/db.py` (SQLite schema + CRUD)
- `marzban_bot/marzban_client.py` (Marzban API client)

---

## GitHub upload

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/<USER>/<REPO>.git
git push -u origin main
```

Notes:
- Do NOT commit `data/` or `.env` (already ignored by `.gitignore`).

## Ubuntu (auto install + run)

After pushing to GitHub, run this on your Ubuntu server:

```bash
wget -qO- https://raw.githubusercontent.com/seyyed1332/Marzban-Auto-repost/main/scripts/install_ubuntu.sh | \
  sudo bash -s -- \
    --repo-url https://github.com/seyyed1332/Marzban-Auto-repost.git \
    --host 0.0.0.0 \
    --port 8000
```

Service management:

```bash
sudo systemctl status marzban-panel-hub --no-pager
sudo journalctl -u marzban-panel-hub -f
sudo systemctl restart marzban-panel-hub
```

Update (pull latest + restart):

```bash
wget -qO- https://raw.githubusercontent.com/seyyed1332/Marzban-Auto-repost/main/scripts/update_ubuntu.sh | sudo bash -s --
```
