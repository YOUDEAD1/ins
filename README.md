# LARA STOCK — منصة المتجر الرقمي المتكاملة

منظومة موحّدة تجمع **بوت تيليجرام** + **تطبيق Mini App** + **موقع ويب مستقل (`larastock.store`)** + **واجهة API للموزّعين** على قاعدة بيانات واحدة وتصميم **Obsidian Royal** فائق الدقة.

## هيكلة المشروع (`Modular Architecture`)

| المسار | الوظيفة |
|---|---|
| `bot.py` | النواة الرئيسية للبوت وخادم الويب المدمج (`HTTP/API/Mini App/Website`) |
| `lara_core/ton_engine.py` | محرك الشبكات المتعدد (`TonAPI v2` + `TONCenter v3/v2` + `LTC` + `USDT`) والفحص السريع للمعاملات وتشخيص المحافظ (`/ton_status`) |
| `lara_core/ban_guard.py` | نظام الحظر الموحّد الشامل عبر البوت والموقع والبريد الإلكتروني وجلسات الدخول |
| `lara_core/security_vault.py` | خزنة التشفير (`AES-256-GCM`) وحماية `SSRF` وتأمين رموز التحقق `OTP` وفلترة البروكسيات المجانية |
| `lara_core/bot_theme.py` | الهوية البصرية للبوت وإيموجيات تيليجرام البريميوم التلقائية ومعالجة الصور عالية الدقة |
| `lara_core/translation_repair.py` | نظام الترجمة الفورية متعددة اللغات |
| `lara_core/referral_auto_rewards.py` | محرك مكافآت الإحالات التلقائية |
| `lara_core/wwc_platform.py` + `WWC-STORE-SETUP.sql` | طبقة قاعدة البيانات والعمليات الذرية (`PostgreSQL / Supabase` أو `MongoDB`) |
| `lara_core/runtime_handoff.py` | التبديل السلس بين نسخ التشغيل بدون توقف (`Zero-Downtime Handoff`) |
| `webapp/index.html` | واجهة المتجر والميني أب ولوحة الإدارة الفورية |
| `webapp/emoji/` | أصول الإيموجي المتحركة والملصقات الثابتة المحسّنة بدقة `128×128 RGBA WebP` |

## التشغيل السريع

### على Render / Linux
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `python bot.py`
- راجع ملف `.env.example` لضبط المتغيرات البيئية المطلوبة (`BOT_TOKEN`، `OWNER_ID`، `MONGO_URI` أو `SUPABASE_DB_URL`، `SESSION_ENCRYPTION_KEY`، `PUBLIC_URL`).

### على Windows
- شغّل `START-BOT.cmd` أو `python start_bot.py`.

## أوامر ومزايا الإدارة الجديدة
- **تشخيص المحافظ والشبكات المباشر:** عبر الأمر `/ton_status` أو زر **«🔬 فحص اتصال المحافظ»** في لوحة الإدارة.
- **الحظر الشامل الموحّد:** يدعم الحظر وفك الحظر عبر `User ID` أو `@username` أو `البريد الإلكتروني`، ويُطبّق فوراً على البوت والموقع والإيميل مع إنهاء الجلسات النشطة.
