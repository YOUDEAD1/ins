import sys
import os
import time
import datetime
import re
import logging
import requests
import threading
import asyncio
import html
import io
from bson.objectid import ObjectId
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv
load_dotenv()

try:
    import telebot
    from telebot import types
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice
except AttributeError:
    print("❌ خطأ: تأكد من حذف أي ملف اسمه telebot.py في مجلدك.")
    sys.exit(1)

try:
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
except ImportError:
    print("❌ خطأ: يرجى تثبيت مكتبة telethon (pip install telethon)")
    sys.exit(1)

from binance.client import Client as BinanceClient
from deep_translator import GoogleTranslator
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# 🔑 1. الإعدادات الأساسية
# ============================================================
TOKEN = os.getenv('TOKEN', '').strip()
try: OWNER_ID = int(os.getenv('OWNER_ID', '0').strip())
except ValueError: OWNER_ID = 0
OWNER_USER = os.getenv('OWNER_USER', '').strip()

BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', '').strip()
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET', '').strip()

MONGO_URI = os.getenv('MONGO_URI', '').strip()
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'shop_db').strip()

GITHUB_API_KEY = os.getenv('GITHUB_API_KEY', '').strip()
GITHUB_BASE_URL = os.getenv('GITHUB_BASE_URL', 'https://api.ahsanlabs.online').strip().rstrip('/')

try: STARS_RATE = int(os.getenv('STARS_RATE', '100').strip())
except ValueError: STARS_RATE = 120

# ============================================================
# 🎨 2. فئة الأزرار المخصصة (لدعم الألوان و Premium Emojis)
# ============================================================
class CustomInlineButton(InlineKeyboardButton):
    def __init__(self, text, style=None, icon_custom_emoji_id=None, **kwargs):
        super().__init__(text, **kwargs)
        self.style = style
        self.icon_custom_emoji_id = icon_custom_emoji_id

    def to_dict(self):
        d = super().to_dict()
        if self.style: d['style'] = self.style
        if self.icon_custom_emoji_id: d['icon_custom_emoji_id'] = str(self.icon_custom_emoji_id)
        return d

# ============================================================
# 🌐 3. السيرفر الوهمي وقاعدة البيانات
# ============================================================
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write("Bot is running! 🚀".encode('utf-8'))
    def log_message(self, format, *args): pass

def keep_alive():
    port = int(os.environ.get('PORT', 8080))
    HTTPServer(('0.0.0.0', port), DummyHandler).serve_forever()

threading.Thread(target=keep_alive, daemon=True).start()

bot = telebot.TeleBot(TOKEN)

logger.info("⏳ جاري الاتصال بقاعدة البيانات MongoDB...")
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.server_info()
    db = mongo_client[MONGO_DB_NAME] 
    logger.info("✅ تم الاتصال بقاعدة البيانات بنجاح!")
except Exception as e:
    logger.error(f"❌ خطأ حرج في MongoDB: {e}")
    sys.exit(1)

REFERRAL_REWARD = 0.10
temp_product = {}
temp_stock_edit = {}
temp_github_data = {} 
PROCESSING_TXS = set()
tx_lock = threading.Lock()

def get_setting(key, default="Not Set"):
    res = db.settings.find_one({'key': key})
    return res['value'] if res else default

# ============================================================
# 🤖 4. تهيئة اليوزربوت (Telethon) - للتفعيلات التلقائية
# ============================================================
client = None
USERBOT_LOOP = None
ACTIVE_GEMINI_SESSION = None
GEMINI_QUEUE = []

def start_dynamic_userbot():
    global client, USERBOT_LOOP
    session_string = get_setting("userbot_session", "")
    provider_bot = get_setting("provider_bot", "").replace("@", "")

    if not session_string or session_string == "Not Set" or not provider_bot or provider_bot == "Not Set":
        return

    if client:
        try: asyncio.run_coroutine_threadsafe(client.disconnect(), USERBOT_LOOP)
        except: pass

    USERBOT_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(USERBOT_LOOP)
    client = TelegramClient(StringSession(session_string), 6, "eb06d4abfb49dc3eeb1aeb98ae0f581e")

    @client.on(events.NewMessage(chats=provider_bot))
    @client.on(events.MessageEdited(chats=provider_bot))
    async def provider_msg_handler(event):
        global ACTIVE_GEMINI_SESSION
        if not ACTIVE_GEMINI_SESSION or not ACTIVE_GEMINI_SESSION.get('ready'): return
        
        text = event.raw_text or ""
        uid = ACTIVE_GEMINI_SESSION['uid']
        price = ACTIVE_GEMINI_SESSION['price']
        
        l = get_lang(uid)
        display_text = text
        if l == 'ar':
            try: display_text = GoogleTranslator(source='auto', target='ar').translate(text)
            except: pass

        formatted_text = f"📩 <b>{html.escape(display_text)}</b>"
        provider_msg_id = event.message.id

        if isinstance(event, events.MessageEdited.Event):
            if provider_msg_id in ACTIVE_GEMINI_SESSION.get('msg_map', {}):
                user_msg_id = ACTIVE_GEMINI_SESSION['msg_map'][provider_msg_id]
                try: bot.edit_message_text(formatted_text, chat_id=uid, message_id=user_msg_id, parse_mode="HTML")
                except: pass 
        else:
            try: 
                sent_msg = bot.send_message(uid, formatted_text, parse_mode="HTML")
                if 'msg_map' not in ACTIVE_GEMINI_SESSION: ACTIVE_GEMINI_SESSION['msg_map'] = {}
                ACTIVE_GEMINI_SESSION['msg_map'][provider_msg_id] = sent_msg.message_id
            except: pass

        if "✅ Status: SUCCEEDED" in text:
            db.orders.insert_one({'user_id': uid, 'product_id': 'Gemini_Activation', 'code_delivered': f"تم التفعيل بنجاح (Gemini)"})
            bot.send_message(uid, "🎉 <b>اكتمل التفعيل بنجاح!</b>\nتم خصم الرصيد وتوثيق الطلب.", parse_mode="HTML")
            ACTIVE_GEMINI_SESSION = None
            process_next_gemini()
            
        elif "❌ Status: FAILED" in text or "❌ Error" in text:
            db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
            bot.send_message(uid, "❌ <b>فشلت العملية وتم إرجاع رصيدك!</b>\nتأكد من البيانات.", parse_mode="HTML")
            ACTIVE_GEMINI_SESSION = None
            process_next_gemini()

    async def run_client():
        try:
            await client.connect()
            await client.run_until_disconnected()
        except Exception as e:
            logger.error(f"❌ خطأ حرج في تشغيل اليوزربوت: {e}")

    def run_it():
        asyncio.set_event_loop(USERBOT_LOOP)
        USERBOT_LOOP.run_until_complete(run_client())

    threading.Thread(target=run_it, daemon=True).start()

def start_gemini_session(uid, price):
    global ACTIVE_GEMINI_SESSION
    provider_bot = get_setting("provider_bot", "").replace("@", "")
    ACTIVE_GEMINI_SESSION = {'uid': uid, 'price': price, 'ready': False, 'msg_map': {}}
    bot.send_message(uid, "⏳ <b>جاري تحضير طلبك والاتصال بالنظام...</b>\nيرجى الانتظار قليلاً...", parse_mode="HTML")
    
    async def _init_chat():
        global ACTIVE_GEMINI_SESSION
        try:
            await client.send_message(provider_bot, "/start")
            await asyncio.sleep(3)
            ACTIVE_GEMINI_SESSION['ready'] = True 
            messages = await client.get_messages(provider_bot, limit=5)
            clicked = False
            for msg in messages:
                if msg.reply_markup and hasattr(msg.reply_markup, 'rows'):
                    for r_idx, row in enumerate(msg.reply_markup.rows):
                        for c_idx, button in enumerate(row.buttons):
                            if button.text and "Create verify" in button.text:
                                await msg.click(r_idx, c_idx)
                                clicked = True
                                break
                        if clicked: break
                if clicked: break
            if not clicked: await client.send_message(provider_bot, "✨ Create verify")
        except Exception as e:
            db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
            bot.send_message(uid, f"❌ <b>فشل الاتصال بمزود الخدمة. تم إرجاع رصيدك.</b>", parse_mode="HTML")
            ACTIVE_GEMINI_SESSION = None
            process_next_gemini()
            
    if client and USERBOT_LOOP: asyncio.run_coroutine_threadsafe(_init_chat(), USERBOT_LOOP)
    else:
        db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
        bot.send_message(uid, "❌ <b>النظام غير متصل.</b> تم إرجاع رصيدك.", parse_mode="HTML")
        ACTIVE_GEMINI_SESSION = None
        process_next_gemini()

def process_next_gemini():
    if GEMINI_QUEUE:
        next_user = GEMINI_QUEUE.pop(0)
        start_gemini_session(next_user['uid'], next_user['price'])

def add_to_gemini_queue(uid, price):
    global ACTIVE_GEMINI_SESSION
    if not ACTIVE_GEMINI_SESSION:
        start_gemini_session(uid, price)
    else:
        GEMINI_QUEUE.append({'uid': uid, 'price': price})
        bot.send_message(uid, f"⏳ <b>تم وضعك في طابور الانتظار!</b>\nدورك رقم: {len(GEMINI_QUEUE)}", parse_mode="HTML")

# ============================================================
# 🌍 5. قاموس اللغات (النصوص والأزرار الافتراضية)
# ============================================================
DEFAULT_BUTTONS = {
    'ar': {
        'btn_products': '🛒 المنتجات', 'btn_deposit': '💳 شحن الرصيد', 'btn_profile': '👤 الملف الشخصي',
        'btn_invite': '👥 الإحالات', 'btn_support': '👨‍💻 الدعم الفني', 'btn_lang': '🌐 English',
        'btn_admin': '👑 لوحة الإدارة', 'btn_stars': '⭐️ نجوم تيليجرام', 'btn_binance': '🟡 Binance Pay',
        'btn_usdt_trc20': '🟢 USDT (TRC-20)', 'btn_usdt_bep20': '🟡 USDT (BEP-20)', 'btn_ton': '💎 Toncoin (TON)',
        'btn_ltc': '🔵 Litecoin (LTC)', 'btn_buy_hist': '🛍 المشتريات', 'btn_dep_hist': '💳 الإيداعات',
        'btn_dl_buy': '📄 تحميل المشتريات', 'btn_gh': '🎓 تفعيل GitHub', 'btn_gemini': '✨ تفعيل Gemini',
        'btn_refresh': '🔄 تحديث', 'btn_main_menu': '🏠 القائمة الرئيسية', 'btn_back': '🔙 رجوع',
        'btn_buy_now': '✅ شراء الآن', 'btn_check_sub': '🔄 تحقق من الاشتراك'
    },
    'en': {
        'btn_products': '🛒 Products', 'btn_deposit': '💳 Deposit', 'btn_profile': '👤 Profile',
        'btn_invite': '👥 Referrals', 'btn_support': '👨‍💻 Support', 'btn_lang': '🌐 العربية',
        'btn_admin': '👑 Admin Panel', 'btn_stars': '⭐️ Telegram Stars', 'btn_binance': '🟡 Binance Pay',
        'btn_usdt_trc20': '🟢 USDT (TRC-20)', 'btn_usdt_bep20': '🟡 USDT (BEP-20)', 'btn_ton': '💎 Toncoin (TON)',
        'btn_ltc': '🔵 Litecoin (LTC)', 'btn_buy_hist': '🛍 Purchases', 'btn_dep_hist': '💳 Deposits',
        'btn_dl_buy': '📄 Download Purchases', 'btn_gh': '🎓 GitHub Pack', 'btn_gemini': '✨ Gemini Advanced',
        'btn_refresh': '🔄 Refresh', 'btn_main_menu': '🏠 Main Menu', 'btn_back': '🔙 Back',
        'btn_buy_now': '✅ Buy Now', 'btn_check_sub': '🔄 Verify Sub'
    }
}

LANG = {
    'ar': {
        'welcome': "👋 <b>أهلاً بك في المتجر الاحترافي!</b>\n\n🆔 الأيدي: <code>{}</code>\n👤 الاسم: <b>{}</b>\n👥 المستخدمين: <b>{}</b>\n💰 الرصيد: <b>${:.2f}</b>",
        'store_title': "🛒 <b>المنتجات المتوفرة:</b>",
        'new_stock': "🔔 <b>توفر ستوك جديد!</b>\n\n🛍 <b>المنتج:</b> {}\n📦 <b>المتوفر الآن:</b> {}\n\n<i>سارع بالشراء الآن من المتجر! 🛒</i>",
        'price_drop': "📉 <b>تخفيض مذهل!</b> 🔥\n\nالمنتج: <b>{}</b>\nالسعر القديم: <strike>${}</strike>\nالسعر الجديد: <b>${}</b> فقط!\n\nسارع بالشراء الآن من المتجر! 🛒",
        'profile_txt': "👤 <b>ملفك الشخصي</b>\n\n🆔 الأيدي: <code>{}</code>\n👤 الاسم: <b>{}</b>\n💰 الرصيد: <b>${:.2f}</b>\n✅ المشتريات: <b>{}</b>\n📦 إجمالي الشحن: <b>${:.2f}</b>",
        'invite_txt': "👥 <b>نظام الإحالات الذكي</b>\n\n🔗 <b>رابط الدعوة الخاص بك:</b>\n<code>https://t.me/{}?start={}</code>\n\n📊 <b>إحصائياتك:</b>\n👥 عدد المدعوين: <b>{}</b>\n💰 إجمالي أرباحك: <b>${:.2f}</b>\n\n🎁 <b>القوانين:</b> ستحصل على <b>$0.10</b> رصيد مجاني فور قيام صديقك بأول عملية شراء.",
        'dep_choose': "💳 <b>اختر طريقة الدفع المناسبة:</b>\n<i>جميع بواباتنا آمنة وتتم معالجتها تلقائياً ⚡️</i>",
        'dep_pay': "🟡 <b>Binance Pay</b>\n\nأرسل المبلغ إلى الـ ID التالي:\n🆔 Binance ID: <code>{}</code>\n\n⚠️ بعد التحويل، <b>أرسل رقم العملية (Order ID) كنص هنا.</b>",
        'dep_usdt': "🟢 <b>شحن عبر USDT (TRC-20)</b>\n\nأرسل المبلغ إلى المحفظة:\n<code>{}</code>\n\n⚠️ <b>الشبكة المقبولة: TRC-20 فقط.</b>\n⚠️ بعد التحويل، <b>أرسل الهاش (TxID) كنص هنا.</b>",
        'dep_ltc': "🔵 <b>شحن عبر Litecoin (LTC)</b>\n\nأرسل المبلغ إلى المحفظة:\n<code>{}</code>\n\n⚠️ <b>تأكد من الإرسال عبر شبكة اللايتكوين الأساسية (Litecoin Network).</b>\n⚠️ بعد التحويل، <b>أرسل الهاش (TxID) كنص هنا.</b>",
        'tx_used': "⚠️ <b>عذراً، هذا الرقم مستخدم مسبقاً!</b>",
        'crypto_checking': "⏳ <b>جاري فحص العملية بأمان... الرجاء الانتظار.</b>",
        'dep_success': "✅ <b>اكتمل الإيداع بنجاح!</b>\nتم إضافة <b>${:.2f}</b> إلى رصيدك. نشكر ثقتك بنا.",
        'dep_fail': "❌ <b>لم نجد العملية!</b> تأكد من صحة الرقم وأنه تم إرساله كنص (وليس صورة).",
        'dep_pending': "⏳ <b>قيد المعالجة!</b> لم يتم تأكيد الحوالة في البلوكتشين بعد، يرجى المحاولة بعد قليل.",
        'history_title': "📜 <b>سجلاتك المالية (أحدث 5 عمليات):</b>",
        'no_hist': "📭 لا توجد سجلات حتى الآن.",
        'buy_success': "✅ <b>تم الشراء بنجاح!</b>\n\nأكوادك جاهزة:\n{}\n\n<i>شكراً لاختيارك متجرنا 🛡️</i>",
        'no_balance': "❌ <b>رصيدك غير كافٍ!</b> يرجى شحن حسابك أولاً.", 'out_stock': "❌ <b>نفد المخزون!</b> يرجى الانتظار لحين التوفر.",
        'must_join': "🔒 <b>عذراً، يجب عليك الاشتراك في قنواتنا أولاً لتتمكن من استخدام البوت:</b>",
        'qty_prompt': "🔢 <b>أرسل الكمية التي تريد شراءها (أرقام فقط):</b>",
        'qty_invalid': "❌ <b>يرجى إرسال أرقام صحيحة أكبر من صفر!</b>",
        'qty_not_enough': "❌ <b>عذراً، المتوفر فقط {} قطعة!</b>",
        'banned': "❌ <b>عذراً، تم حظرك من استخدام هذا البوت نهائياً.</b>",
        
        # GitHub & Gemini
        'gh_desc': "🎓 <b>تفعيل اشتراك GitHub Student</b> 🚀\n\n💰 <b>السعر:</b> <b>${:.2f}</b>",
        'gh_prompt_user': "🎓 <b>الخطوة 1 من 3: (اسم المستخدم)</b>\n👇 الرجاء إرسال <b>اليوزر نيم</b> أو الإيميل:",
        'gh_prompt_pass': "🔑 <b>الخطوة 2 من 3: (كلمة المرور)</b>\n👇 الرجاء إرسال <b>الباسوورد</b>:",
        'gh_prompt_2fa': "🛡️ <b>الخطوة 3 من 3: (كود التحقق)</b>\n📱 أرسل <b>كود التحقق (الـ 6 أرقام)</b>:",
        'gh_deducted': "⏳ <b>تم استلام البيانات!</b> جاري التحقق...",
        'gh_submitted': "✅ <b>تم تقديم الطلب بنجاح!</b> التفعيل يتم الآن.",
        'gh_received': "🔄 <b>بدأت عملية التفعيل! (رقم: <code>{}</code>)</b>",
        'gh_success': "🎉 <b>اكتمل التفعيل بنجاح!</b> 🎓\n✅ تم تفعيل الحساب: <code>{}</code>",
        'gh_fail': "❌ <b>فشل التفعيل!</b>\nالسبب: <b>{}</b>\nتم إرجاع <b>${:.2f}</b> إلى رصيدك.",
        'gh_processing': "🔄 <b>الطلب قيد التنفيذ (رقم: <code>{}</code>)</b>\n⏳ <i>الخطوة: <b>{}</b> {} (فحص {}/35)</i>",
        'gh_timeout': "⚠️ <b>انتهى وقت الانتظار!</b> الطلب مستمر في الخلفية.",
        'gh_conn_err': "❌ <b>حدث خطأ في الاتصال:</b>\n<code>{}</code>\nتم إرجاع الرصيد.",
        'gemini_desc': "🤖 <b>تفعيل اشتراك Gemini Advanced</b>\n\n💰 <b>السعر:</b> <b>${:.2f}</b>"
    },
    'en': {
        'welcome': "👋 <b>Welcome to the Pro Shop!</b>\n\n🆔 ID: <code>{}</code>\n👤 Name: <b>{}</b>\n👥 Users: <b>{}</b>\n💰 Balance: <b>${:.2f}</b>",
        'store_title': "🛒 <b>Available Products:</b>",
        'new_stock': "🔔 <b>New Stock Available!</b>\n\n🛍 <b>Product:</b> {}\n📦 <b>Available Now:</b> {}\n\n<i>Hurry up and buy now! 🛒</i>",
        'price_drop': "📉 <b>Massive Price Drop!</b> 🔥\n\nProduct: <b>{}</b>\nOld Price: <strike>${}</strike>\nNew Price: Only <b>${}</b>!\n\nHurry up and buy now! 🛒",
        'profile_txt': "👤 <b>Your Profile</b>\n\n🆔 ID: <code>{}</code>\n👤 Name: <b>{}</b>\n💰 Balance: <b>${:.2f}</b>\n✅ Purchases: <b>{}</b>\n📦 Total Deposited: <b>${:.2f}</b>",
        'invite_txt': "👥 <b>Smart Referrals</b>\n\n🔗 <b>Your Link:</b>\n<code>https://t.me/{}?start={}</code>\n\n📊 <b>Stats:</b>\n👥 Invited: <b>{}</b>\n💰 Earned: <b>${:.2f}</b>",
        'dep_choose': "💳 <b>Choose payment method:</b>",
        'dep_pay': "🟡 <b>Binance Pay</b>\n\nSend amount to ID:\n🆔 Binance ID: <code>{}</code>\n\n⚠️ Send <b>Order ID</b> here as text.",
        'dep_usdt': "🟢 <b>USDT Deposit</b>\n\nSend to address:\n<code>{}</code>\n\n⚠️ Send <b>TxID (Hash)</b> here as text.",
        'dep_ltc': "🔵 <b>Litecoin (LTC) Deposit</b>\n\nSend to address:\n<code>{}</code>\n\n⚠️ Send <b>TxID (Hash)</b> here as text.",
        'tx_used': "⚠️ <b>ID already used!</b>",
        'crypto_checking': "⏳ <b>Verifying securely... Please wait.</b>",
        'dep_success': "✅ <b>Deposit Successful!</b>\n<b>${:.2f}</b> added to your balance.",
        'dep_fail': "❌ <b>Not found!</b> Check ID and send text, not an image.",
        'dep_pending': "⏳ <b>Pending!</b> Not confirmed on blockchain yet.",
        'history_title': "📜 <b>Your Financial Records:</b>",
        'no_hist': "📭 No records yet.",
        'buy_success': "✅ <b>Purchase Successful!</b>\n\nYour codes:\n{}\n",
        'no_balance': "❌ <b>Low balance!</b> Please deposit.", 'out_stock': "❌ <b>Out of stock!</b>",
        'must_join': "🔒 <b>You must join our channels first:</b>",
        'qty_prompt': "🔢 <b>Enter quantity:</b>",
        'qty_invalid': "❌ <b>Invalid number!</b>",
        'qty_not_enough': "❌ <b>Only {} pieces available!</b>",
        'banned': "❌ <b>You are banned.</b>",
        
        'gh_desc': "🎓 <b>GitHub Student Pack Activation</b> 🚀\n\n💰 <b>Price:</b> <b>${:.2f}</b>",
        'gh_prompt_user': "🎓 <b>Step 1 of 3:</b>\n👇 Send your GitHub <b>Username or Email</b>:",
        'gh_prompt_pass': "🔑 <b>Step 2 of 3:</b>\n👇 Send your GitHub <b>Password</b>:",
        'gh_prompt_2fa': "🛡️ <b>Step 3 of 3:</b>\n📱 Send <b>2FA Code</b>:",
        'gh_deducted': "⏳ <b>Data received!</b> Verifying...",
        'gh_submitted': "✅ <b>Request submitted!</b> Activation is processing.",
        'gh_received': "🔄 <b>Activation started! (ID: <code>{}</code>)</b>",
        'gh_success': "🎉 <b>Activation Completed!</b> 🎓\n✅ Account: <code>{}</code>",
        'gh_fail': "❌ <b>Failed!</b>\nReason: <b>{}</b>\nRefunded <b>${:.2f}</b>.",
        'gh_processing': "🔄 <b>Processing (ID: <code>{}</code>)</b>\n⏳ <i>Step: <b>{}</b> {} ({}/35)</i>",
        'gh_timeout': "⚠️ <b>Timeout!</b> Processing in background.",
        'gh_conn_err': "❌ <b>Connection error:</b>\n<code>{}</code>\nRefunded.",
        'gemini_desc': "🤖 <b>Gemini Advanced Activation</b>\n\n💰 <b>Price:</b> <b>${:.2f}</b>"
    }
}

# ============================================================
# 🛠️ 6. محرك الـ CMS (تنظيف الرموز، الترجمة الآمنة، وجلب النصوص)
# ============================================================

def clean_old_emojis(text):
    """دالة تقوم بمسح الرموز القديمة العادية لتجنب التكرار عند وضع رمز Premium جديد"""
    old_emojis = ['🛒', '💳', '👤', '👥', '👨‍💻', '🌐', '👑', '⭐️', '🟡', '🟢', '💎', '🔵', '🛍', '📄', '🎓', '✨', '🔄', '🏠', '🔙', '✅', '📦', '✏️', '🎛', '📝', '🚚', '💰', '📊', '📉', '🔔']
    for emj in old_emojis:
        text = text.replace(emj, '')
    return text.strip()

def safe_translate(text, target_lang='en'):
    try:
        emoji_pattern = r'<tg-emoji[^>]*>.*?</tg-emoji>'
        var_pattern = r'\{[^}]+\}'
        
        emojis = re.findall(emoji_pattern, text)
        for i, emj in enumerate(emojis):
            text = text.replace(emj, f' XEMOJIX{i}X ')
            
        vars_list = re.findall(var_pattern, text)
        for i, var in enumerate(vars_list):
            text = text.replace(var, f' XVARX{i}X ')
            
        translated = GoogleTranslator(source='auto', target=target_lang).translate(text)
        
        for i, emj in enumerate(emojis):
            translated = translated.replace(f' XEMOJIX{i}X ', emj).replace(f'XEMOJIX{i}X', emj)
            
        for i, var in enumerate(vars_list):
            translated = translated.replace(f' XVARX{i}X ', var).replace(f'XVARX{i}X', var)
            
        return translated.strip()
    except Exception as e:
        logger.error(f"Safe translation error: {e}")
        return text

def extract_custom_emojis_to_html(message):
    """تحويل الرموز التعبيرية التي أدخلتها في النصوص إلى كود HTML"""
    if not message.text: return ""
    if not message.entities: return message.text
        
    encoded_text = message.text.encode('utf-16-le')
    custom_emojis = [e for e in message.entities if e.type == 'custom_emoji']
    
    if not custom_emojis: return message.text
        
    custom_emojis.sort(key=lambda x: x.offset, reverse=True)
    
    for entity in custom_emojis:
        start = entity.offset * 2
        end = (entity.offset + entity.length) * 2
        emoji_char = encoded_text[start:end].decode('utf-16-le')
        replacement = f'<tg-emoji emoji-id="{entity.custom_emoji_id}">{emoji_char}</tg-emoji>'
        encoded_text = encoded_text[:start] + replacement.encode('utf-16-le') + encoded_text[end:]
        
    return encoded_text.decode('utf-16-le')

def parse_button_input(message):
    """استخراج اسم الزر و ID الإيموجي المميز، مع تنظيف الرموز القديمة"""
    text = message.text
    emoji_id = None
    if message.entities:
        for ent in message.entities:
            if ent.type == 'custom_emoji':
                emoji_id = ent.custom_emoji_id
                emoji_char = message.text[ent.offset:ent.offset+ent.length]
                text = text.replace(emoji_char, '')
                break
    text = clean_old_emojis(text)
    return text, emoji_id

def get_text(uid, key, *args):
    l = get_lang(uid)
    custom = db.custom_texts.find_one({'lang': l, 'key': key})
    base_text = custom['value'] if custom else LANG.get(l, LANG['en']).get(key, "")
    
    if args:
        try: return base_text.format(*args)
        except Exception as e:
            logger.error(f"Error formatting string for key {key}: {e}")
            return base_text
    return base_text

def get_btn_data(uid, key):
    l = get_lang(uid)
    custom = db.custom_buttons.find_one({'lang': l, 'key': key})
    if custom:
        return custom.get('text', ''), custom.get('emoji_id', None)
    default_text = DEFAULT_BUTTONS.get(l, DEFAULT_BUTTONS['en']).get(key, key)
    return default_text, None

def create_btn(uid, key, callback_data=None, url=None, style=None):
    text, emj_id = get_btn_data(uid, key)
    kwargs = {'text': text}
    if callback_data: kwargs['callback_data'] = callback_data
    if url: kwargs['url'] = url
    if style: kwargs['style'] = style
    if emj_id: kwargs['icon_custom_emoji_id'] = emj_id
    return CustomInlineButton(**kwargs)

# === دوال مساعدة عامة ===
def clean_name(text):
    if not text: return "بدون اسم"
    return html.escape(re.sub(r'<[^>]+>', '', str(text)).strip())

def obscure_text(text):
    if not text: return "***"
    if '@' in text:
        parts = text.split('@')
        if len(parts[0]) > 2: return parts[0][0] + "***" + parts[0][-1] + "@" + parts[1]
        else: return parts[0][0] + "***@" + parts[1]
    else:
        if len(text) > 2: return text[0] + "***" + text[-1]
        return text[0] + "***"

def find_product(pid):
    pid_str = str(pid)
    try:
        p = db.products.find_one({'id': pid_str})
        if p: return p
        if pid_str.isdigit():
            p = db.products.find_one({'id': int(pid_str)})
            if p: return p
        if len(pid_str) == 24:
            try:
                p = db.products.find_one({'_id': ObjectId(pid_str)})
                if p: return p
            except: pass
    except: pass
    return None

def get_product_stock_count(pid):
    try:
        pid_str = str(pid)
        queries = [{'product_id': pid_str}]
        if pid_str.isdigit(): queries.append({'product_id': int(pid_str)})
        return db.product_stock.count_documents({'$or': queries, 'is_sold': False})
    except: return 0

def get_user_data_full(uid): return db.users.find_one({'user_id': uid})
def get_lang(uid): u = get_user_data_full(uid); return u.get('lang', 'ar') if u else 'en'
def is_user_banned(uid): u = get_user_data_full(uid); return True if u and u.get('is_banned') == 1 else False
def check_forced_sub(uid):
    if uid == OWNER_ID: return True
    user_db = get_user_data_full(uid)
    if user_db and user_db.get('is_admin') == 1: return True
    chans = list(db.required_channels.find())
    if not chans: return True
    for c in chans:
        try:
            if bot.get_chat_member(c['channel_id'], uid).status in ['left', 'kicked']: return False
        except: return False
    return True

def notify_admins(message_text):
    if OWNER_ID:
        try: bot.send_message(OWNER_ID, message_text, parse_mode="HTML")
        except: pass
    for admin in list(db.users.find({'is_admin': 1})):
        if admin['user_id'] != OWNER_ID:
            try: bot.send_message(admin['user_id'], message_text, parse_mode="HTML")
            except: pass

# ============================================================
# 🏠 7. القائمة الرئيسية (Start)
# ============================================================
@bot.message_handler(commands=['start'])
def start_handler(message):
    is_callback = isinstance(message, types.CallbackQuery)
    chat_id = message.message.chat.id if is_callback else message.chat.id
    from_user = message.from_user
    uid = from_user.id
    uname = from_user.username.lower() if from_user.username else ""
    
    if is_user_banned(uid):
        bot.send_message(chat_id, get_text(uid, 'banned'), parse_mode="HTML")
        return

    user = get_user_data_full(uid)
    if not user:
        args = ("" if is_callback else (message.text or "")).split()
        ref = args[1] if len(args) > 1 and args[1].isdigit() else None
        db.users.insert_one({
            'user_id': uid, 'name': from_user.first_name, 'username': uname, 
            'referred_by': ref, 'balance': 0.0, 'lang_chosen': False, 'lang': 'ar', 'is_admin': 0, 'is_banned': 0
        })
        user = get_user_data_full(uid)
    else:
        db.users.update_one({'user_id': uid}, {'$set': {'username': uname}})

    if not user.get('lang_chosen'):
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(InlineKeyboardButton("🇸🇦 العربية", callback_data="init_lang_ar"), InlineKeyboardButton("🇬🇧 English", callback_data="init_lang_en"))
        bot.send_message(chat_id, "🌐 <b>الرجاء اختيار لغتك / Please choose your language:</b>", reply_markup=markup, parse_mode="HTML")
        return

    lang = user.get('lang', 'ar')
    if lang not in ['ar', 'en']: lang = 'en'
    
    if not check_forced_sub(uid):
        chans = list(db.required_channels.find())
        markup = InlineKeyboardMarkup(row_width=1)
        for c in chans: 
            markup.add(InlineKeyboardButton("📢 القناة", url=f"https://t.me/{c['channel_id'].replace('@','') }"))
        markup.add(create_btn(uid, 'btn_check_sub', callback_data="main_menu_refresh"))
        bot.send_message(chat_id, get_text(uid, 'must_join'), reply_markup=markup, parse_mode="HTML")
        return

    users_total = db.users.count_documents({})
    markup = InlineKeyboardMarkup(row_width=2)
    
    markup.add(create_btn(uid, 'btn_gh', callback_data="github_pack_info"))
    markup.add(create_btn(uid, 'btn_gemini', callback_data="gemini_pack_info"))
    
    # 🔵 الزر الأزرق للمنتجات في قائمة ستارت 🔵
    markup.add(create_btn(uid, 'btn_products', callback_data="open_shop", style="primary"),
               create_btn(uid, 'btn_deposit', callback_data="open_deposit"))
    markup.add(create_btn(uid, 'btn_profile', callback_data="open_profile"),
               create_btn(uid, 'btn_invite', callback_data="open_invite"))
    markup.add(create_btn(uid, 'btn_support', url=f"https://t.me/{OWNER_USER}"),
               create_btn(uid, 'btn_lang', callback_data="toggle_language"))
    
    if user.get('is_admin') == 1 or uid == OWNER_ID:
        markup.add(create_btn(uid, 'btn_admin', callback_data="admin_panel_main"))

    welcome_message = get_text(uid, 'welcome', uid, from_user.first_name, users_total, user.get('balance', 0.0))
    bot.send_message(chat_id, welcome_message, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("init_lang_"))
def init_lang_selection(call):
    bot.answer_callback_query(call.id)
    lang = call.data.split("_")[2]
    db.users.update_one({'user_id': call.from_user.id}, {'$set': {'lang': lang, 'lang_chosen': True}})
    try: bot.delete_message(call.message.chat.id, call.message.message_id)
    except: pass
    start_handler(call)

# ============================================================
# 👤 8. الملف الشخصي والمشتريات
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_profile")
def profile_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    if not check_forced_sub(uid): start_handler(call.message); return
    
    u = get_user_data_full(uid)
    buy_count = db.orders.count_documents({'user_id': uid})
    d_res = list(db.used_transactions.find({'user_id': uid}))
    dep_total = sum([float(d.get('amount', 0)) for d in d_res])

    prof_emoji_id = get_setting('emoji_profile', '')
    profile_text = get_text(uid, 'profile_txt', uid, clean_name(u.get('name','User')), u.get('balance', 0.0), buy_count, dep_total)
    if prof_emoji_id and prof_emoji_id != "Not Set": profile_text = f'<tg-emoji emoji-id="{prof_emoji_id}">✨</tg-emoji> ' + profile_text

    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(create_btn(uid, 'btn_buy_hist', callback_data="history_menu_callback"))
    markup.add(create_btn(uid, 'btn_deposit', callback_data="open_deposit"),
               create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh"))
    try: bot.edit_message_text(profile_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "history_menu_callback")
def history_menu_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(create_btn(uid, 'btn_buy_hist', callback_data="h_view_buy"),
               create_btn(uid, 'btn_dep_hist', callback_data="h_view_dep"))
    markup.add(create_btn(uid, 'btn_dl_buy', callback_data="h_dl_buy"))
    markup.add(create_btn(uid, 'btn_back', callback_data="open_profile"))
    try: bot.edit_message_text(get_text(uid, 'history_title'), call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "h_dl_buy")
def user_download_buy_hist(call):
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز الملف...")
    uid = call.from_user.id
    recs = list(db.orders.find({'user_id': uid}).sort('_id', -1))
    
    if not recs:
        bot.send_message(uid, get_text(uid, 'no_hist'))
        return
        
    content = "=== Your Purchase History ===\n\n"
    all_prods = {str(p.get('id', p.get('_id'))): p for p in db.products.find()}
    
    for i, r in enumerate(recs, 1):
        date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
        pid = str(r.get('product_id'))
        p = all_prods.get(pid)
        n = pid.replace('_', ' ') if pid in ['GitHub_Student', 'Gemini_Activation'] else clean_name(p.get('name_en') if p else "Unknown Product")
        code = r.get('code_delivered', '')
        content += f"{i}. Date: {date_str}\nProduct: {n}\nCode: {code}\n{'-'*30}\n"
        
    f = io.BytesIO(content.encode('utf-8'))
    f.name = f"My_Purchases_{uid}.txt"
    bot.send_document(call.message.chat.id, f)

@bot.callback_query_handler(func=lambda call: call.data.startswith("h_view_"))
def show_hist_detail(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    mode = call.data.replace("h_view_", "")
    out = ""
    try:
        if mode == "buy":
            recs = list(db.orders.find({'user_id': uid}).sort('_id', -1).limit(5))
            if not recs: out = get_text(uid, 'no_hist')
            for r in recs:
                date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
                pid = str(r.get('product_id'))
                n = pid.replace('_', ' ') if pid in ['GitHub_Student', 'Gemini_Activation'] else clean_name(find_product(pid).get('name_en') if find_product(pid) else "Product")
                out += f"🧾 <b>Receipt</b>\n📅 {date_str}\n📦 <b>{n}</b>\n🔑 <code>{r.get('code_delivered', '')}</code>\n────────────\n"
        else:
            recs = list(db.used_transactions.find({'user_id': uid}).sort('_id', -1).limit(5))
            if not recs: out = get_text(uid, 'no_hist')
            for r in recs: 
                date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
                out += f"💳 <b>Deposit</b>\n📅 {date_str}\n💰 <b>${r.get('amount', 0):.2f}</b>\n🆔 <code>{r.get('transaction_id', '')}</code>\n────────────\n"
    except Exception as e: out = f"❌ Error"
    
    markup = InlineKeyboardMarkup(); markup.add(create_btn(uid, 'btn_back', callback_data="history_menu_callback"))
    try: bot.edit_message_text(out, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "open_invite")
def invite_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    if not check_forced_sub(uid): start_handler(call.message); return
    
    inv_res = list(db.users.find({'referred_by': str(uid)}))
    inv_c = len(inv_res)
    actual_earned = len(db.orders.distinct('user_id', {'user_id': {'$in': [int(r['user_id']) for r in inv_res]}})) * REFERRAL_REWARD

    markup = InlineKeyboardMarkup()
    markup.add(create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh"))
    try: bot.edit_message_text(get_text(uid, 'invite_txt', bot.get_me().username, uid, inv_c, actual_earned), call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

# ============================================================
# 🛒 9. المتجر (الترتيب الأبجدي، ألوان أخضر وأحمر)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_shop")
def shop_list_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    if not check_forced_sub(uid): start_handler(call.message); return
    
    u = get_user_data_full(uid)
    is_admin = (u.get('is_admin') == 1 or uid == OWNER_ID)
    l = get_lang(uid)
    
    prods = list(db.products.find())
    prods.sort(key=lambda x: x.get('name_en' if l == 'en' else 'name_ar', '').lower())
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(create_btn(uid, 'btn_gh', callback_data="github_pack_info"))
    markup.add(create_btn(uid, 'btn_gemini', callback_data="gemini_pack_info"))
    
    for p in prods:
        if p.get('is_hidden', False) and not is_admin: continue
            
        st = get_product_stock_count(p['id'])
        
        # 🟢 أخضر (متوفر) | 🔴 أحمر (غير متوفر)
        btn_style = "success" if (p.get('is_manual') or st > 0) else "danger"
        custom_emoji_id = p.get('custom_emoji_id')
        
        hidden_icon = " 👻(مخفي)" if p.get('is_hidden', False) else ""
        n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar'))
        short_n = n[:25] + ".." if len(n) > 25 else n 
        
        st_text = "FW" if p.get('is_manual') else str(st)
        btn_text = f"{short_n} | ${p.get('price', 0):.2f} | 📦 {st_text}{hidden_icon}"
        
        btn_kwargs = {'text': btn_text, 'callback_data': f"vi_p_{p['id']}", 'style': btn_style}
        if custom_emoji_id: btn_kwargs['icon_custom_emoji_id'] = custom_emoji_id
            
        markup.add(CustomInlineButton(**btn_kwargs))
        
    markup.add(create_btn(uid, 'btn_refresh', callback_data="open_shop"))
    markup.add(create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh"))
    
    store_emoji_id = get_setting('emoji_store', '')
    store_text = get_text(uid, 'store_title')
    if store_emoji_id and store_emoji_id != "Not Set": store_text = f'<tg-emoji emoji-id="{store_emoji_id}">✨</tg-emoji> ' + store_text

    try: bot.edit_message_text(store_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("vi_p_"))
def shop_detail_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    l = get_lang(uid)
    pid = call.data.replace('vi_p_', '')
    p = find_product(pid)
    
    if not p or (p.get('is_hidden', False) and not (get_user_data_full(uid).get('is_admin') == 1 or uid == OWNER_ID)):
        bot.send_message(uid, "❌ عذراً، المنتج غير متوفر."); return

    is_manual = p.get('is_manual', False)
    st = get_product_stock_count(pid)
    
    delivery_type = ("يدوي 🤝" if l == 'ar' else "Manual 🤝") if is_manual else ("تلقائي ⚡" if l == 'ar' else "Auto ⚡")
    st_text = ("غير محدود" if l == 'ar' else "Unlimited") if is_manual else str(st)
        
    n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar'))
    d = clean_name(p.get('desc_en') if l == 'en' else p.get('desc_ar'))
    
    custom_emoji_id = p.get('custom_emoji_id')
    icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji>' if custom_emoji_id else '📦'
    
    text = f"{icon_html} <b>{n}</b>\n\n📝 {d}\n\n🚚 <b>Type:</b> {delivery_type}\n💰 <b>Price:</b> ${p.get('price', 0):.2f}\n📊 <b>Stock:</b> {st_text}"
    
    markup = InlineKeyboardMarkup()
    if is_manual or st > 0: 
        markup.add(create_btn(uid, 'btn_buy_now', callback_data=f"buy_qty_{pid}"))
    markup.add(create_btn(uid, 'btn_back', callback_data="open_shop"))
    try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_qty_"))
def prompt_quantity(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    l = get_lang(uid); pid = call.data.replace('buy_qty_', '')
    p = find_product(pid)
    
    if not p.get('is_manual', False) and get_product_stock_count(pid) == 0:
        bot.send_message(uid, get_text(uid, 'out_stock'), parse_mode="HTML"); return
        
    msg = bot.send_message(uid, get_text(uid, 'qty_prompt'), parse_mode="HTML")
    bot.register_next_step_handler(msg, execute_bulk_buy, pid, l)

def execute_bulk_buy(message, pid, lang):
    uid = message.from_user.id
    if not message.text or not message.text.isdigit() or int(message.text.strip()) <= 0:
        bot.send_message(uid, get_text(uid, 'qty_invalid'), parse_mode="HTML"); return
    
    qty = int(message.text.strip())
    u = get_user_data_full(uid)
    p = find_product(pid)

    if not p.get('is_manual', False):
        stk_items = list(db.product_stock.find({'product_id': {'$in': [str(pid), int(pid)]}, 'is_sold': False}).limit(qty))
        if len(stk_items) < qty:
            bot.send_message(uid, get_text(uid, 'qty_not_enough', len(stk_items)), parse_mode="HTML"); return
        
    total_price = float(p.get('price', 0)) * qty
    if float(u.get('balance', 0)) < total_price:
        bot.send_message(uid, get_text(uid, 'no_balance'), parse_mode="HTML"); return
        
    db.users.update_one({'user_id': uid}, {'$inc': {'balance': -total_price}})
    
    if p.get('is_manual', False):
        order_id = "M" + str(int(time.time()))[-6:] + str(uid)[-2:]
        db.orders.insert_one({'user_id': uid, 'product_id': str(pid), 'code_delivered': f"Order: {order_id}"})
        bot.send_message(uid, f"✅ <b>Order Placed! (${total_price:.2f})</b>\nID: <code>{order_id}</code>\nContact Admin.", parse_mode="HTML")
        notify_admins(f"🔐 <b>طلب تسليم يدوي</b>\nالعميل: <code>{uid}</code>\nالمنتج: {clean_name(p.get('name_ar'))}\nالكمية: {qty}\nرقم: <code>{order_id}</code>")
    else:
        delivered_codes = []
        for item in stk_items:
            db.product_stock.update_one({'_id': item['_id']}, {'$set': {'is_sold': True}})
            db.orders.insert_one({'user_id': uid, 'product_id': str(pid), 'code_delivered': item['code_line']})
            delivered_codes.append(item['code_line'])
            
        if qty > 3:
            f = io.BytesIO("\n".join(delivered_codes).encode('utf-8'))
            f.name = f"Codes_{pid}.txt"
            bot.send_document(uid, f, caption=f"✅ Purchase Successful! {qty} codes attached.")
        else:
            bot.send_message(uid, get_text(uid, 'buy_success', "\n".join([f"<code>{c}</code>" for c in delivered_codes])), parse_mode="HTML")
        notify_admins(f"🔐 <b>شراء تلقائي</b>\nالعميل: <code>{uid}</code>\nالمنتج: {clean_name(p.get('name_ar'))}\nالكمية: {qty}")

    # الإحالة
    if db.orders.count_documents({'user_id': uid}) == qty and u.get('referred_by'):
        ref_id = int(u['referred_by'])
        if db.users.find_one({'user_id': ref_id}):
            db.users.update_one({'user_id': ref_id}, {'$inc': {'balance': REFERRAL_REWARD}})

# ============================================================
# 💳 10. بوابات الدفع 
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_deposit")
def dep_init_ui(call):
    uid = call.from_user.id
    if not check_forced_sub(uid): start_handler(call.message); return
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(create_btn(uid, 'btn_stars', callback_data="dep_stars"))
    markup.add(create_btn(uid, 'btn_binance', callback_data="dep_binance"))
    markup.add(create_btn(uid, 'btn_usdt_trc20', callback_data="dep_crypto_USDT"))
    markup.add(create_btn(uid, 'btn_usdt_bep20', callback_data="dep_crypto_USDT_BEP20"))
    markup.add(create_btn(uid, 'btn_ton', callback_data="dep_crypto_TON"))
    markup.add(create_btn(uid, 'btn_ltc', callback_data="dep_crypto_LTC"))
    markup.add(create_btn(uid, 'btn_back', callback_data="open_profile"))
    
    try: bot.edit_message_text(get_text(uid, 'dep_choose'), call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "dep_stars")
def dep_stars_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    bot.clear_step_handler_by_chat_id(chat_id=uid)
    msg = bot.send_message(uid, f"⭐️ <b>Send USD amount:</b>\n<i>(1$ = {STARS_RATE} Stars)</i>", parse_mode="HTML")
    bot.register_next_step_handler(msg, lambda m: process_stars_amount(m, get_lang(uid)))

def process_stars_amount(message, lang):
    uid = message.from_user.id
    try:
        usd_amount = float(message.text.strip())
        if usd_amount < 0.1: return bot.send_message(uid, "❌ Minimum $0.1", parse_mode="HTML")
        prices = [LabeledPrice(label=f"Deposit ${usd_amount:.2f}", amount=int(usd_amount * STARS_RATE))]
        bot.send_invoice(uid, title="Shop Deposit", description=f"Deposit ${usd_amount:.2f}", invoice_payload=f"dep_{uid}_{usd_amount}", provider_token="", currency="XTR", prices=prices)
    except: bot.send_message(uid, "❌ Numbers only.", parse_mode="HTML")

@bot.pre_checkout_query_handler(func=lambda query: True)
def checkout(pre_checkout_query): bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@bot.message_handler(content_types=['successful_payment'])
def got_payment(message):
    payload = message.successful_payment.invoice_payload
    if payload.startswith("dep_"):
        credit_user(message.from_user.id, float(payload.split('_')[2]), message.successful_payment.telegram_payment_charge_id, get_lang(message.from_user.id), "Stars ⭐️")

@bot.callback_query_handler(func=lambda call: call.data == "dep_binance")
def dep_binance_ui(call):
    uid = call.from_user.id; bot.clear_step_handler_by_chat_id(uid)
    msg = bot.send_message(uid, get_text(uid, 'dep_pay', get_setting('wallet_address')), parse_mode="HTML")
    bot.register_next_step_handler(msg, verify_binance_pay, get_lang(uid))

def verify_binance_pay(message, lang):
    uid = message.from_user.id; tx_id = message.text.strip()
    if len(tx_id) < 15: return bot.send_message(uid, "❌ Invalid Order ID.", parse_mode="HTML")
    with tx_lock:
        if tx_id in PROCESSING_TXS or db.used_transactions.find_one({'transaction_id': tx_id}): return bot.send_message(uid, get_text(uid, 'tx_used'), parse_mode="HTML")
        PROCESSING_TXS.add(tx_id)
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)
        for d in client.get_pay_trade_history().get('data', []):
            if tx_id.lower() == str(d.get('orderId', '')).lower() and (int(time.time()*1000) - int(d.get('transactionTime', 0))) < 86400000:
                credit_user(uid, float(d.get('amount', 0.0)), tx_id.lower(), lang, "Binance Pay")
                PROCESSING_TXS.discard(tx_id); return
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
    except: bot.send_message(uid, "❌ Error.", parse_mode="HTML")
    finally: PROCESSING_TXS.discard(tx_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("dep_crypto_"))
def dep_crypto_ui(call):
    uid = call.from_user.id; coin = call.data.replace('dep_crypto_', '')
    wallet = get_setting({"USDT": "usdt_address", "USDT_BEP20": "usdt_bep20_address", "TON": "ton_address", "LTC": "ltc_address"}[coin])
    msg = bot.send_message(uid, get_text(uid, f'dep_{coin.lower().split("_")[0]}', wallet) if coin in ['USDT', 'LTC'] else f"<b>{coin} Deposit:</b>\n<code>{wallet}</code>\n\nSend TxID:", parse_mode="HTML")
    if coin == "LTC": bot.register_next_step_handler(msg, verify_ltc_public_blockchain, get_lang(uid), wallet)
    else: bot.register_next_step_handler(msg, verify_crypto_tx, get_lang(uid), coin.split('_')[0])

def verify_crypto_tx(message, lang, coin):
    uid = message.from_user.id; tx_id = message.text.strip().lower()
    if len(tx_id) < 20: return bot.send_message(uid, "❌ Invalid TxID.", parse_mode="HTML")
    with tx_lock:
        if tx_id in PROCESSING_TXS or db.used_transactions.find_one({'transaction_id': tx_id}): return bot.send_message(uid, get_text(uid, 'tx_used'), parse_mode="HTML")
        PROCESSING_TXS.add(tx_id)
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)
        for d in client.get_deposit_history(coin=coin):
            if tx_id in str(d.get('txId', '')).lower() and (int(time.time()*1000) - int(d.get('insertTime', 0))) < 86400000:
                if int(d.get('status', -1)) == 1: credit_user(uid, float(d.get('amount', 0.0)), tx_id, lang, f"Crypto {coin}")
                else: bot.send_message(uid, get_text(uid, 'dep_pending'), parse_mode="HTML")
                PROCESSING_TXS.discard(tx_id); return
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
    except: bot.send_message(uid, "❌ Error.", parse_mode="HTML")
    finally: PROCESSING_TXS.discard(tx_id)

def verify_ltc_public_blockchain(message, lang, wallet_address):
    uid = message.from_user.id; tx_id = message.text.strip().lower()
    with tx_lock:
        if tx_id in PROCESSING_TXS or db.used_transactions.find_one({'transaction_id': tx_id}): return bot.send_message(uid, get_text(uid, 'tx_used'), parse_mode="HTML")
        PROCESSING_TXS.add(tx_id)
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        res = requests.get(f"https://litecoinspace.org/api/tx/{tx_id}", timeout=10)
        if res.status_code == 200:
            data = res.json()
            received = sum([float(vout.get("value", 0))/100000000.0 for vout in data.get("vout", []) if vout.get("scriptpubkey_address") == wallet_address])
            if received > 0:
                client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)
                credit_user(uid, received * float(client.get_symbol_ticker(symbol="LTCUSDT")['price']), tx_id, lang, "Litecoin (LTC)")
            else: bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
    except: bot.send_message(uid, "❌ Network Error.", parse_mode="HTML")
    finally: PROCESSING_TXS.discard(tx_id)

def credit_user(uid, amt, tx_id, lang, method):
    db.users.update_one({'user_id': uid}, {'$inc': {'balance': amt}})
    db.used_transactions.insert_one({'transaction_id': tx_id, 'amount': amt, 'user_id': uid})
    bot.send_message(uid, get_text(uid, 'dep_success', amt), parse_mode="HTML")
    notify_admins(f"🔐 <b>إيداع جديد</b>\nعميل: <code>{uid}</code>\nالمبلغ: ${amt:.2f}\nالطريقة: {method}\nالعملية: {tx_id}")

# ============================================================
# 👑 11. لوحة الإدارة و CMS والبرودكاست
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "admin_panel_main")
def admin_main_ui(call):
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("➕ أضف منتج", callback_data="ad_p_add"), InlineKeyboardButton("📦 إدارة الستوك", callback_data="ad_s_list"))
    markup.add(InlineKeyboardButton("📝 تعديل منتج", callback_data="ad_p_edit"), InlineKeyboardButton("🗑 حذف منتج", callback_data="ad_p_del"))
    markup.add(InlineKeyboardButton("👥 العملاء", callback_data="ad_users_main"), InlineKeyboardButton("💰 شحن عضو", callback_data="ad_gift"))
    markup.add(InlineKeyboardButton("📢 برودكاست للأعضاء", callback_data="ad_bc"))
    markup.add(InlineKeyboardButton("✏️ تخصيص البوت والأزرار", callback_data="ad_texts_main"))
    markup.add(InlineKeyboardButton("⚙️ إعدادات المتجر", callback_data="ad_shop_settings"), InlineKeyboardButton("🎓 التفعيلات", callback_data="ad_api_main"))
    markup.add(InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="main_menu_refresh"))
    bot.edit_message_text("👑 <b>لوحة الإدارة:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

# ----------- نظام البرودكاست المطور بتقرير النجاح والفشل -----------
@bot.callback_query_handler(func=lambda call: call.data == "ad_bc")
def admin_bc_init(call):
    msg = bot.send_message(call.from_user.id, "📢 <b>أرسل رسالة البرودكاست الآن:</b>\n(أرسل النص مع الرموز المميزة، وسيتم تحويلها تلقائياً)", parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_bc_exe)

def admin_bc_exe(message):
    admin_id = message.chat.id
    bot.send_message(admin_id, "⏳ جاري الإرسال، يرجى الانتظار (سيصلك تقرير عند الانتهاء)...")
    
    final_html = extract_custom_emojis_to_html(message)
    
    def run_broadcast():
        users = list(db.users.find())
        success = 0; fail = 0
        for u in users:
            try:
                bot.send_message(u['user_id'], final_html, parse_mode="HTML")
                success += 1; time.sleep(0.05)
            except: fail += 1
        
        # إرسال التقرير الشامل للأدمن
        report = f"📢 <b>تقرير إرسال البرودكاست:</b>\n\n🟢 المستلمين بنجاح: <b>{success}</b> مستخدم\n🔴 فشل الإرسال (حظروا البوت): <b>{fail}</b> مستخدم"
        bot.send_message(admin_id, report, parse_mode="HTML")

    threading.Thread(target=run_broadcast, daemon=True).start()

# ----------- CMS النصوص والأزرار -----------
@bot.callback_query_handler(func=lambda call: call.data == "ad_texts_main")
def ad_texts_main_ui(call):
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("📝 نصوص الرسائل", callback_data="ad_cms_msgs"), InlineKeyboardButton("🎛 أزرار البوت", callback_data="ad_cms_btns_cats"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    bot.edit_message_text("✏️ <b>نظام التخصيص (CMS):</b>\nاختر ماذا تريد أن تخصص:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_cms_msgs")
def ad_cms_msgs_ui(call):
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("رسالة الترحيب (Start)", callback_data="edit_txt_welcome"))
    markup.add(InlineKeyboardButton("رسالة قسم الشحن", callback_data="edit_txt_dep_choose"))
    markup.add(InlineKeyboardButton("رسالة قسم الإحالات", callback_data="edit_txt_invite_txt"))
    markup.add(InlineKeyboardButton("إشعار توفر ستوك", callback_data="edit_txt_new_stock"))
    markup.add(InlineKeyboardButton("إشعار التخفيضات", callback_data="edit_txt_price_drop"))
    markup.add(InlineKeyboardButton("عنوان المتجر", callback_data="edit_txt_store_title"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="ad_texts_main"))
    bot.edit_message_text("📝 <b>تخصيص نصوص الرسائل:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_cms_btns_cats")
def ad_cms_btns_cats_ui(call):
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("🏠 أزرار القائمة الرئيسية", callback_data="ad_cms_b_start"))
    markup.add(InlineKeyboardButton("💳 أزرار الشحن والدفع", callback_data="ad_cms_b_dep"))
    markup.add(InlineKeyboardButton("👤 أزرار الملف والمشتريات", callback_data="ad_cms_b_prof"))
    markup.add(InlineKeyboardButton("🛒 أزرار المتجر والتنقل", callback_data="ad_cms_b_shop"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="ad_texts_main"))
    bot.edit_message_text("🎛 <b>تخصيص أزرار البوت:</b>\nاختر القسم:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cms_b_"))
def ad_cms_btns_list(call):
    cat = call.data.replace("ad_cms_b_", "")
    btn_categories = {
        'start': ['btn_products', 'btn_deposit', 'btn_profile', 'btn_invite', 'btn_support', 'btn_lang', 'btn_admin'],
        'dep': ['btn_stars', 'btn_binance', 'btn_usdt_trc20', 'btn_usdt_bep20', 'btn_ton', 'btn_ltc'],
        'prof': ['btn_buy_hist', 'btn_dep_hist', 'btn_dl_buy'],
        'shop': ['btn_gh', 'btn_gemini', 'btn_refresh', 'btn_main_menu', 'btn_buy_now', 'btn_back']
    }
    markup = InlineKeyboardMarkup(row_width=1)
    for key in btn_categories.get(cat, []):
        text, _ = get_btn_data(call.from_user.id, key)
        markup.add(InlineKeyboardButton(f"✏️ {text}", callback_data=f"edit_btn_{key}"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="ad_cms_btns_cats"))
    bot.edit_message_text("👇 <b>اختر الزر الذي تريد تغيير اسمه أو الإيموجي الخاص به:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_txt_"))
def ad_edit_txt_prompt(call):
    key = call.data.replace("edit_txt_", "")
    current_val = db.custom_texts.find_one({'lang': 'ar', 'key': key})
    current_text = current_val['value'] if current_val else LANG['ar'].get(key, "")
    msg = bot.send_message(call.message.chat.id, f"النص الحالي:\n\n<code>{html.escape(current_text)}</code>\n\n👇 <b>أرسل التعديل الآن (مع الرموز المميزة إن أردت):</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_save_custom_text, key)

def ad_save_custom_text(message, key):
    final_text_ar = extract_custom_emojis_to_html(message)
    final_text_en = safe_translate(final_text_ar, 'en')
    db.custom_texts.update_one({'lang': 'ar', 'key': key}, {'$set': {'value': final_text_ar}}, upsert=True)
    db.custom_texts.update_one({'lang': 'en', 'key': key}, {'$set': {'value': final_text_en}}, upsert=True)
    bot.send_message(message.chat.id, "✅ <b>تم الحفظ والترجمة التلقائية!</b>", parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_btn_"))
def ad_edit_btn_prompt(call):
    key = call.data.replace("edit_btn_", "")
    current_text, _ = get_btn_data(call.from_user.id, key)
    msg = bot.send_message(call.message.chat.id, f"الزر الحالي: <code>{html.escape(current_text)}</code>\n\n👇 <b>أرسل الاسم الجديد مع الرمز التعبيري المميز:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_save_custom_btn, key)

def ad_save_custom_btn(message, key):
    text_ar, emoji_id = parse_button_input(message)
    current_text, _ = get_btn_data(message.from_user.id, key)
    if not text_ar: text_ar = clean_old_emojis(current_text) or clean_old_emojis(DEFAULT_BUTTONS['ar'].get(key, key))
    text_en = safe_translate(text_ar, 'en')
    db.custom_buttons.update_one({'lang': 'ar', 'key': key}, {'$set': {'text': text_ar, 'emoji_id': emoji_id}}, upsert=True)
    db.custom_buttons.update_one({'lang': 'en', 'key': key}, {'$set': {'text': text_en, 'emoji_id': emoji_id}}, upsert=True)
    bot.send_message(message.chat.id, f"✅ <b>تم حفظ الزر! وتم تنظيف الرموز القديمة.</b>\n{text_ar} | {text_en}", parse_mode="HTML")

# ----------- المنتجات والمخزون والإشعارات (برودكاست مع تقرير) -----------
@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_add_"))
def admin_stock_input(call):
    pid = call.data.replace("stk_add_", "")
    msg = bot.send_message(call.from_user.id, "📥 <b>أرسل الأكواد (كل كود بسطر):</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_stock_save, pid)

def admin_stock_save(message, pid):
    lines = message.text.split('\n')
    count = sum(1 for l in lines if l.strip() and db.product_stock.insert_one({'product_id': str(pid), 'code_line': l.strip(), 'is_sold': False}))
    bot.send_message(message.chat.id, f"✅ <b>تم إضافة {count} كود! جاري إشعار العملاء...</b>", parse_mode="HTML")

    def broadcast_new_stock(pid_for_thread, admin_id):
        p = find_product(pid_for_thread); stk_total = get_product_stock_count(pid_for_thread)
        success = 0; fail = 0
        for u in list(db.users.find()):
            try:
                l = u.get('lang', 'en')
                c_emj = p.get('custom_emoji_id')
                i_html = f'<tg-emoji emoji-id="{c_emj}">✨</tg-emoji> ' if c_emj else '📦 '
                p_name = i_html + clean_name(p.get(f'name_{l}', p.get('name_en')))
                bot.send_message(u['user_id'], get_text(u['user_id'], 'new_stock', p_name, stk_total), parse_mode="HTML")
                success += 1; time.sleep(0.05)
            except: fail += 1
        bot.send_message(admin_id, f"📢 <b>تقرير إشعار المخزون ({p.get('name_ar')}):</b>\n🟢 نجاح: {success}\n🔴 فشل: {fail}", parse_mode="HTML")
    threading.Thread(target=broadcast_new_stock, args=(pid, message.chat.id), daemon=True).start()

def admin_save_edit(message, field, pid):
    p = find_product(pid)
    if field == "price":
        new_price = float(message.text); old_price = float(p.get('price', 0))
        db.products.update_one({'_id': p['_id']}, {'$set': {'price': new_price}})
        bot.send_message(message.chat.id, "✅ Updated.")
        if new_price < old_price:
            def broadcast_price_drop(admin_id):
                success = 0; fail = 0
                for u in list(db.users.find()):
                    try:
                        l = u.get('lang', 'en')
                        c_emj = p.get('custom_emoji_id')
                        i_html = f'<tg-emoji emoji-id="{c_emj}">✨</tg-emoji> ' if c_emj else '📦 '
                        p_name = i_html + clean_name(p.get(f'name_{l}', p.get('name_en')))
                        bot.send_message(u['user_id'], get_text(u['user_id'], 'price_drop', p_name, old_price, new_price), parse_mode="HTML")
                        success += 1; time.sleep(0.05)
                    except: fail += 1
                bot.send_message(admin_id, f"📢 <b>تقرير إشعار التخفيض ({p.get('name_ar')}):</b>\n🟢 نجاح: {success}\n🔴 فشل: {fail}", parse_mode="HTML")
            threading.Thread(target=broadcast_price_drop, args=(message.chat.id,), daemon=True).start()
    else:
        db.products.update_one({'_id': p['_id']}, {'$set': {field: message.text}})
        bot.send_message(message.chat.id, "✅ Updated.")

# ============================================================
# 🚀 12. تشغيل البوت
# ============================================================
def run_bot():
    try: bot.delete_webhook(drop_pending_updates=True); time.sleep(1)
    except: pass
    while True:
        try: bot.polling(non_stop=True, skip_pending=True)
        except Exception as e: logger.error(f"Polling Error: {e}"); time.sleep(5)

if __name__ == "__main__":
    run_bot()
