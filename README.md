# LARA STOCK

بوت تيليجرام + ميني أب + موقع خارجي (larastock.store)، كلهم على نفس قاعدة البيانات.

## الملفات المهمة
| الملف | وش هو |
|---|---|
| `bot.py` | البوت والسيرفر (API + الموقع + الميني أب) |
| `webapp/index.html` | واجهة الميني أب والموقع (ملف واحد) |
| `webapp/emoji`, `webapp/lib`, `logo.webp` | صور وإيموجي وأيقونات |
| `translation_repair.py` | الترجمة التلقائية للغات |
| `referral_auto_rewards.py` | مكافآت الإحالات |
| `wwc_platform.py` + `WWC-STORE-SETUP.sql` | منصة المتاجر (لازم ملف SQL يبقى جنبه) |
| `start_bot.py` + ملفات `.cmd` | تشغيل البوت على ويندوز |
| `verification/` | الاختبارات (`pytest`) |
| `docs/` | الشروحات وسجل التحديثات القديمة |

## التشغيل على Render
- Build: `pip install -r requirements.txt`
- Start: `python bot.py`
- المتغيرات (Environment): شوف `.env.example`

### الموقع والإيميل
- `PUBLIC_URL=https://larastock.store`
- `SMTP_HOST=mail.spacemail.com` · `SMTP_PORT=465` · `SMTP_USER` · `SMTP_PASS` · `MAIL_FROM` · `MAIL_FROM_NAME`
- `GOOGLE_CLIENT_ID` (اختياري: زر الدخول بحساب Google)

## الشروحات
- `docs/updates/` : شرح كل تحديث (آخرها UPDATE-42)
- `docs/guides/` : الأدلة (التشغيل، الإحالات، الشبكة، …)
- `docs/history/changes/` : فروقات التحديثات القديمة (أرشيف فقط)
