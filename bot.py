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
import random
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

try: 
    OWNER_ID = int(os.getenv('OWNER_ID', '0').strip())
except ValueError: 
    OWNER_ID = 0

OWNER_USER = os.getenv('OWNER_USER', '').strip()

BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', '').strip()
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET', '').strip()

MONGO_URI = os.getenv('MONGO_URI', '').strip()
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'shop_test_db').strip()

GITHUB_API_KEY = os.getenv('GITHUB_API_KEY', '').strip()
GITHUB_BASE_URL = os.getenv('GITHUB_BASE_URL', 'https://api.ahsanlabs.online').strip().rstrip('/')

try: 
    STARS_RATE = int(os.getenv('STARS_RATE', '120').strip())
except ValueError: 
    STARS_RATE = 120

# ============================================================
# 🛡️ 2. نظام جلب البروكسيات الذكي والمفحوص (Auto-Proxy)
# ============================================================
CACHED_PROXIES = []
LAST_PROXY_FETCH = 0

def test_proxy(proxy_url):
    """دالة لاختبار البروكسي قبل استخدامه لتجنب الحظر"""
    try:
        proxies_dict = {'http': proxy_url, 'https': proxy_url}
        res = requests.get("https://api.binance.com/api/v3/ping", proxies=proxies_dict, timeout=4)
        return res.status_code == 200
    except:
        return False

def get_free_proxies():
    """هذه الدالة تجلب بروكسيات مجانية من الإنترنت وتفحصها قبل الاستخدام"""
    global CACHED_PROXIES, LAST_PROXY_FETCH
    current_time = time.time()
    
    if not CACHED_PROXIES or (current_time - LAST_PROXY_FETCH > 3600):
        try:
            logger.info("🔄 جاري البحث عن بروكسيات جديدة وفحصها لتجنب حظر بينانس...")
            
            res = requests.get(
                "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", 
                timeout=10
            )
            
            if res.status_code == 200:
                proxies = res.text.strip().split('\n')
                random.shuffle(proxies)
                
                working_proxies = []
                for p in proxies[:60]: # تجربة مجموعة عشوائية
                    proxy_url = f"http://{p.strip()}"
                    if test_proxy(proxy_url):
                        working_proxies.append(proxy_url)
                    if len(working_proxies) >= 5: # نحتفظ بأفضل 5 بروكسيات شغالة
                        break
                        
                if working_proxies:
                    CACHED_PROXIES = working_proxies
                    LAST_PROXY_FETCH = current_time
                    logger.info(f"✅ تم العثور على {len(CACHED_PROXIES)} بروكسيات نشطة وقوية.")
                else:
                    logger.warning("⚠️ لم يتم العثور على بروكسي نشط، سيتم المحاولة لاحقاً.")
                 
        except Exception as e:
            logger.error(f"❌ فشل جلب البروكسيات المجانية: {e}")
            CACHED_PROXIES = []
            
    return CACHED_PROXIES

def get_binance_client():
    """دالة لإنشاء اتصال مع بينانس باستخدام بروكسي عشوائي مفحوص لتفادي الحظر"""
    proxies_list = get_free_proxies()
    
    if proxies_list:
        proxy = random.choice(proxies_list)
        proxies_dict = {'http': proxy, 'https': proxy}
        try:
            return BinanceClient(
                BINANCE_API_KEY, 
                BINANCE_API_SECRET, 
                requests_params={'proxies': proxies_dict, 'timeout': 10}
            )
        except Exception:
            pass 
            
    # الاتصال الافتراضي بدون بروكسي إذا لم تتوفر بروكسيات
    return BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)

# ============================================================
# 🎨 3. فئة الأزرار المخصصة (لدعم الألوان و Premium Emojis)
# ============================================================
class CustomInlineButton(InlineKeyboardButton):
    def __init__(self, text, style=None, icon_custom_emoji_id=None, **kwargs):
        super().__init__(text, **kwargs)
        self.style = style
        self.icon_custom_emoji_id = icon_custom_emoji_id

    def to_dict(self):
        d = super().to_dict()
        if self.style: 
            d['style'] = self.style
        if self.icon_custom_emoji_id: 
            d['icon_custom_emoji_id'] = str(self.icon_custom_emoji_id)
        return d

# ============================================================
# 🌐 4. السيرفر الوهمي وقاعدة البيانات
# ============================================================
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write("Bot is running! 🚀".encode('utf-8'))
        
    def log_message(self, format, *args):
        pass

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
    if res:
        return res['value']
    return default

# ============================================================
# 🤖 5. تهيئة اليوزربوت (Telethon) - للتفعيلات التلقائية
# ============================================================
client = None
USERBOT_LOOP = None
ACTIVE_GEMINI_SESSION = None
GEMINI_QUEUE = []

def start_dynamic_userbot():
    global client, USERBOT_LOOP
    
    session_string = get_setting("userbot_session", "")
    provider_bot = get_setting("provider_bot", "").replace("@", "")

    if not session_string or session_string == "Not Set":
        return
        
    if not provider_bot or provider_bot == "Not Set":
        return

    if client:
        try: 
            asyncio.run_coroutine_threadsafe(client.disconnect(), USERBOT_LOOP)
        except Exception: 
            pass

    USERBOT_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(USERBOT_LOOP)
  
    client = TelegramClient(StringSession(session_string), 6, "eb06d4abfb49dc3eeb1aeb98ae0f581e")

    @client.on(events.NewMessage(chats=provider_bot))
    @client.on(events.MessageEdited(chats=provider_bot))
    async def provider_msg_handler(event):
        global ACTIVE_GEMINI_SESSION
        
        if not ACTIVE_GEMINI_SESSION:
            return
            
        if not ACTIVE_GEMINI_SESSION.get('ready'):
            return
       
        text = event.raw_text or ""
        uid = ACTIVE_GEMINI_SESSION['uid']
        price = ACTIVE_GEMINI_SESSION['price']
        
        l = get_lang(uid)
        display_text = text
        
        if l == 'ar':
            try:
                display_text = GoogleTranslator(source='auto', target='ar').translate(text)
            except Exception:
                pass

        formatted_text = f"📩 <b>{html.escape(display_text)}</b>"
        provider_msg_id = event.message.id

        if isinstance(event, events.MessageEdited.Event):
            if provider_msg_id in ACTIVE_GEMINI_SESSION.get('msg_map', {}):
                user_msg_id = ACTIVE_GEMINI_SESSION['msg_map'][provider_msg_id]
                try:
                    bot.edit_message_text(
                        formatted_text, 
                        chat_id=uid, 
                        message_id=user_msg_id, 
                        parse_mode="HTML"
                    )
                except Exception:
                    pass 
        else:
            try: 
                sent_msg = bot.send_message(uid, formatted_text, parse_mode="HTML")
                if 'msg_map' not in ACTIVE_GEMINI_SESSION:
                    ACTIVE_GEMINI_SESSION['msg_map'] = {}
                ACTIVE_GEMINI_SESSION['msg_map'][provider_msg_id] = sent_msg.message_id
            except Exception:
                pass

        if "✅ Status: SUCCEEDED" in text:
            db.orders.insert_one({
                'user_id': uid, 
                'product_id': 'Gemini_Activation', 
                'code_delivered': f"تم التفعيل بنجاح (Gemini)"
            })
            
            bot.send_message(
                uid, 
                "🎉 <b>اكتمل التفعيل بنجاح!</b>\nتم خصم الرصيد وتوثيق الطلب. يمكنك رؤية الإيصال في المشتريات.", 
                parse_mode="HTML"
            )
            
            log_ch = get_setting('log_channel')
            u_data = db.users.find_one({'user_id': uid})
            obs_user = obscure_text(u_data.get('username') or str(uid))
          
            if log_ch and log_ch != "Not Set":
                try:
                    bot.send_message(
                        log_ch, 
                        f"✨ <b>New Gemini Advanced Activation!</b> 🚀\n\n👤 Account: <b>{obs_user}</b>\n✅ Status: <b>Successfully Activated</b>\n\n<i>Activated automatically via Bot ⚡</i>", 
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

            ACTIVE_GEMINI_SESSION = None
            process_next_gemini()
            
        elif "❌ Status: FAILED" in text or "❌ Error" in text:
            db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
            bot.send_message(
                uid, 
                "❌ <b>فشلت العملية وتم إرجاع رصيدك!</b>\nتأكد من تفعيل (التحقق بخطوتين) والبيانات الصحيحة.", 
                parse_mode="HTML"
            )
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
 
    ACTIVE_GEMINI_SESSION = {
        'uid': uid, 
        'price': price, 
        'ready': False, 
        'msg_map': {}
    }
    
    bot.send_message(
        uid, 
        "⏳ <b>جاري تحضير طلبك والاتصال بالنظام...</b>\nيرجى الانتظار قليلاً...", 
        parse_mode="HTML"
    )
  
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
                        if clicked:
                            break
            if clicked:
                break
                    
            if not clicked:
                await client.send_message(provider_bot, "✨ Create verify")
              
        except Exception as e:
            db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
            bot.send_message(
                uid, 
                f"❌ <b>فشل الاتصال بمزود الخدمة. تم إرجاع رصيدك.</b>\nالخطأ البرمجي: <code>{e}</code>", 
                parse_mode="HTML"
            )
            ACTIVE_GEMINI_SESSION = None
            process_next_gemini()
            
    if client and USERBOT_LOOP: 
        asyncio.run_coroutine_threadsafe(_init_chat(), USERBOT_LOOP)
    else:
        db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
        bot.send_message(
            uid, 
            "❌ <b>النظام غير متصل (اليوزربوت معطل).</b> تم إرجاع رصيدك.", 
            parse_mode="HTML"
        )
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
        bot.send_message(
            uid, 
            f"⏳ <b>تم وضعك في طابور الانتظار!</b>\nدورك رقم: {len(GEMINI_QUEUE)}\nسيتم بدء التفعيل تلقائياً عند وصول دورك.", 
            parse_mode="HTML"
        )

# ============================================================
# 🌍 6. القواميس الأساسية والنصوص الافتراضية
# ============================================================
DEFAULT_BUTTONS = {
    'ar': {
        'btn_products': 'المنتجات',
        'btn_deposit': '💳 شحن الرصيد',
        'btn_profile': '👤 الملف الشخصي',
        'btn_invite': '👥 الإحالات',
        'btn_support': '👨‍💻 الدعم الفني',
        'btn_lang': '🌐 English',
        'btn_admin': '👑 لوحة الإدارة',
        'btn_stars': '⭐️ نجوم تيليجرام',
        'btn_binance': '🟡 Binance Pay',
        'btn_usdt_trc20': '🟢 USDT (TRC-20)',
        'btn_usdt_bep20': '🟡 USDT (BEP-20)',
        'btn_ton': '💎 Toncoin (TON)',
        'btn_ltc': '🔵 Litecoin (LTC)',
        'btn_buy_hist': '🛍 المشتريات',
        'btn_dep_hist': '💳 الإيداعات',
        'btn_dl_buy': '📄 تحميل المشتريات',
        'btn_gh': '🎓 تفعيل GitHub',
        'btn_gemini': '✨ تفعيل Gemini',
        'btn_refresh': '🔄 تحديث',
        'btn_main_menu': '🏠 القائمة الرئيسية',
        'btn_back': '🔙 رجوع',
        'btn_buy_now': '✅ شراء الآن',
        'btn_check_sub': '🔄 تحقق من الاشتراك'
    },
    'en': {
        'btn_products': 'Products',
        'btn_deposit': '💳 Deposit',
        'btn_profile': '👤 Profile',
        'btn_invite': '👥 Referrals',
        'btn_support': '👨‍💻 Support',
        'btn_lang': '🌐 العربية',
        'btn_admin': '👑 Admin Panel',
        'btn_stars': '⭐️ Telegram Stars',
        'btn_binance': '🟡 Binance Pay',
        'btn_usdt_trc20': '🟢 USDT (TRC-20)',
        'btn_usdt_bep20': '🟡 USDT (BEP-20)',
        'btn_ton': '💎 Toncoin (TON)',
        'btn_ltc': '🔵 Litecoin (LTC)',
        'btn_buy_hist': '🛍 Purchases',
        'btn_dep_hist': '💳 Deposits',
        'btn_dl_buy': '📄 Download Purchases',
        'btn_gh': '🎓 GitHub Pack',
        'btn_gemini': '✨ Gemini Advanced',
        'btn_refresh': '🔄 Refresh',
        'btn_main_menu': '🏠 Main Menu',
        'btn_back': '🔙 Back',
        'btn_buy_now': '✅ Buy Now',
        'btn_check_sub': '🔄 Verify Sub'
    }
}

LANG = {
    'ar': {
        'welcome': "👋 <b>أهلاً بك في المتجر الاحترافي!</b>\n\n🆔 الأيدي: <code>{}</code>\n👤 الاسم: <b>{}</b>\n👥 المستخدمين: <b>{}</b>\n💰 الرصيد: <b>${:.2f}</b>",
        'store_title': "🛒 <b>المنتجات المتوفرة:</b>",
        'new_stock': "🔔 <b>توفر ستوك جديد!</b>\n\n🛍 <b>المنتج:</b> {}\n📦 <b>المتوفر الآن:</b> {}\n\n<i>سارع بالشراء الآن من المتجر!</i>",
        'price_drop': "📉 <b>تخفيض مذهل!</b> 🔥\n\nالمنتج: <b>{}</b>\nالسعر القديم: <strike>${}</strike>\nالسعر الجديد: <b>${}</b> فقط!\n\nسارع بالشراء الآن من المتجر!",
        'profile_txt': "👤 <b>ملفك الشخصي</b>\n\n🆔 الأيدي: <code>{}</code>\n👤 الاسم: <b>{}</b>\n💰 الرصيد: <b>${:.2f}</b>\n✅ المشتريات: <b>{}</b>\n📦 إجمالي الشحن: <b>${:.2f}</b>",
        'invite_txt': "👥 <b>نظام الإحالات الذكي</b>\n\n🔗 <b>الرابط الخاص بك:</b>\n<code>https://t.me/{}?start={}</code>\n\n📊 <b>إحصائياتك الفورية:</b>\n- 👥 إجمالي الزيارات: <b>{}</b>\n- ⏳ معلق (لم يشترك): <b>{}</b>\n- ✅ نشط (مشترك): <b>{}</b>\n- ❌ غادر القناة: <b>{}</b>\n\n💰 أرباحك الحالية: <b>${:.2f}</b>\n\n🎁 <b>قوانين النظام الجديد:</b>\nتحصل على <b>0.10$</b> مقابل كل <b>10 أشخاص</b> يكملون الاشتراك الإجباري بنجاح. وفي حال غادر أحدهم القناة سينقص العدد، وإذا قلّ عن مضاعفات العشرة سيتم خصم الرصيد.",
        'dep_choose': "💳 <b>اختر طريقة الدفع المناسبة:</b>",
        'dep_pay': "🟡 <b>Binance Pay</b>\n\nأرسل المبلغ إلى الـ ID التالي:\n🆔 Binance ID: <code>{}</code>\n\n⚠️ أرسل <b>رقم العملية (Order ID)</b> كنص هنا.",
        'dep_usdt': "🟢 <b>شحن عبر USDT (TRC-20)</b>\n\nالمحفظة:\n<code>{}</code>\n\n⚠️ أرسل <b>الهاش (TxID)</b> كنص هنا.",
        'dep_ltc': "🔵 <b>شحن عبر Litecoin (LTC)</b>\n\nالمحفظة:\n<code>{}</code>\n\n⚠️ أرسل <b>الهاش (TxID)</b> كنص هنا.",
        'tx_used': "⚠️ <b>عذراً، هذا الرقم مستخدم مسبقاً!</b>",
        'crypto_checking': "⏳ <b>جاري الفحص بأمان... الرجاء الانتظار.</b>",
        'dep_success': "✅ <b>اكتمل الإيداع بنجاح!</b>\nتم إضافة <b>${:.2f}</b> إلى رصيدك.",
        'dep_fail': "❌ <b>لم نجد العملية!</b> تأكد من صحة الرقم وأنه نص.",
        'dep_pending': "⏳ <b>قيد المعالجة!</b> لم يتم التأكيد في البلوكتشين بعد.",
        'history_title': "📜 <b>سجلاتك المالية (أحدث 5 عمليات):</b>",
        'no_hist': "📭 لا توجد سجلات حتى الآن.",
        'buy_success': "✅ <b>تم الشراء بنجاح!</b>\n\nأكوادك جاهزة:\n{}\n\n<i>شكراً لاختيارك متجرنا 🛡️</i>",
        'no_balance': "❌ <b>رصيدك غير كافٍ!</b> يرجى الشحن أولاً.", 
        'out_stock': "❌ <b>نفد المخزون!</b>",
        'must_join': "🔒 <b>يجب عليك الاشتراك في قنواتنا أولاً:</b>",
        'qty_prompt': "🔢 <b>أرسل الكمية (أرقام فقط):</b>",
        'qty_invalid': "❌ <b>رقم غير صحيح!</b>",
        'qty_not_enough': "❌ <b>المتوفر فقط {} قطعة!</b>",
        'banned': "❌ <b>تم حظرك من البوت.</b>",
        'log_buy': "🛒 <b>عملية شراء جديدة!</b> 🛍\n\n👤 العميل: <b>{user}</b>\n📦 المنتج: <b>{product}</b>\n🔢 الكمية: <b>{qty}</b>\n\n<i>تم التسليم بنجاح 🛡️</i>",
        'log_dep': "💳 <b>عملية إيداع جديدة!</b> 💵\n\n👤 العميل: <b>{user}</b>\n💰 المبلغ: <b>{amount}</b>\n🟢 الطريقة: <b>{method}</b>\n\n<i>تم الشحن تلقائياً ⚡</i>",
        
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
        'new_stock': "🔔 <b>New Stock Available!</b>\n\n🛍 <b>Product:</b> {}\n📦 <b>Available Now:</b> {}\n\n<i>Buy now!</i>",
        'price_drop': "📉 <b>Massive Price Drop!</b> 🔥\n\nProduct: <b>{}</b>\nOld Price: <strike>${}</strike>\nNew Price: <b>${}</b>!\n\n<i>Buy now!</i>",
        'profile_txt': "👤 <b>Your Profile</b>\n\n🆔 ID: <code>{}</code>\n👤 Name: <b>{}</b>\n💰 Balance: <b>${:.2f}</b>\n✅ Purchases: <b>{}</b>\n📦 Total Deposited: <b>${:.2f}</b>",
        'invite_txt': "👥 <b>Smart Referrals</b>\n\n🔗 <b>Your Link:</b>\n<code>https://t.me/{}?start={}</code>\n\n📊 <b>Real-time Stats:</b>\n- 👥 Total Clicks: <b>{}</b>\n- ⏳ Pending (No Sub): <b>{}</b>\n- ✅ Active (Joined): <b>{}</b>\n- ❌ Left Channel: <b>{}</b>\n\n💰 Current Earnings: <b>${:.2f}</b>\n\n🎁 <b>Rule:</b> Get $0.10 for every 10 active subs. If anyone leaves, the counter drops and balance may be deducted automatically.",
        'dep_choose': "💳 <b>Choose payment method:</b>",
        'dep_pay': "🟡 <b>Binance Pay</b>\n\nSend amount to ID:\n🆔 Binance ID: <code>{}</code>\n\n⚠️ Send <b>Order ID</b> here as text.",
        'dep_usdt': "🟢 <b>USDT Deposit</b>\n\nSend to address:\n<code>{}</code>\n\n⚠️ Send <b>TxID (Hash)</b> here as text.",
        'dep_ltc': "🔵 <b>Litecoin (LTC) Deposit</b>\n\nSend to address:\n<code>{}</code>\n\n⚠️ Send <b>TxID (Hash)</b> here as text.",
        'tx_used': "⚠️ <b>ID already used!</b>",
        'crypto_checking': "⏳ <b>Verifying securely...</b>",
        'dep_success': "✅ <b>Deposit Successful!</b>\n<b>${:.2f}</b> added to your balance.",
        'dep_fail': "❌ <b>Not found!</b> Check ID and send text, not an image.",
        'dep_pending': "⏳ <b>Pending!</b> Not confirmed on blockchain yet.",
        'history_title': "📜 <b>Your Financial Records:</b>",
        'no_hist': "📭 No records yet.",
        'buy_success': "✅ <b>Purchase Successful!</b>\n\nYour codes:\n{}\n",
        'no_balance': "❌ <b>Low balance!</b> Please deposit.", 
        'out_stock': "❌ <b>Out of stock!</b>",
        'must_join': "🔒 <b>You must join our channels first:</b>",
        'qty_prompt': "🔢 <b>Enter quantity:</b>",
        'qty_invalid': "❌ <b>Invalid number!</b>",
        'qty_not_enough': "❌ <b>Only {} pieces available!</b>",
        'banned': "❌ <b>You are banned.</b>",
        'log_buy': "🛒 <b>New Purchase!</b> 🛍\n\n👤 User: <b>{user}</b>\n📦 Product: <b>{product}</b>\n🔢 QTY: <b>{qty}</b>\n\n<i>Delivered successfully 🛡️</i>",
        'log_dep': "💳 <b>New Deposit!</b> 💵\n\n👤 User: <b>{user}</b>\n💰 Amount: <b>{amount}</b>\n🟢 Method: <b>{method}</b>\n\n<i>Processed automatically ⚡</i>",
        
        'gh_desc': "🎓 <b>GitHub Student Pack Activation</b>\n\n💰 <b>Price:</b> <b>${:.2f}</b>",
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
# 🛠️ 7. محرك الـ CMS (ترجمة آمنة مع حماية الرموز التعبيرية)
# ============================================================

def clean_html(text):
    """دالة مطورة تدعم التنسيقات (مثل المائل والغامق) في وصف المنتجات"""
    if not text: 
        return "بدون اسم"
    return str(text).strip()

def clean_old_emojis(text):
    old_emojis = [
        '🛒', '💳', '👤', '👥', '👨‍💻', '🌐', '👑', '⭐️', '🟡', '🟢', 
        '💎', '🔵', '🔴', '🛍', '📄', '🎓', '✨', '🔄', '🏠', '🔙', 
        '✅', '📦', '✏️', '🎛', '📝', '🚚', '💰', '📊', '📉', '🔔'
    ]
    for emj in old_emojis:
        text = text.replace(emj, '')
    return text.strip()

def safe_translate_for_cms(text, target_lang='en'):
    if not text or not text.strip(): 
        return text
        
    try:
        placeholders = []
        
        def replacer(match):
            placeholders.append(match.group(0))
            return f" XZQXZQ{len(placeholders)-1:04d}QZXQZX "
        
        temp_text = re.sub(r'<tg-emoji[^>]*>.*?</tg-emoji>', replacer, text)
        temp_text = re.sub(r'\{[^}]+\}', replacer, temp_text)
        temp_text = re.sub(r'<[^>]+>', replacer, temp_text)
        
        clean_check = re.sub(r'\s*XZQXZQ\d+QZXQZX\s*', '', temp_text).strip()
        
        if not clean_check: 
            return text
        
        translated = GoogleTranslator(source='auto', target=target_lang).translate(temp_text)
        
        if not translated: 
            return text
        
        arabic_to_eng = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')
        
        def clean_arabic_digits(match): 
            return match.group(0).translate(arabic_to_eng)
            
        translated = re.sub(r'XZQXZQ[^A-Za-z]*?QZXQZX', clean_arabic_digits, translated, flags=re.IGNORECASE)
        
        for i in range(len(placeholders) - 1, -1, -1):
            translated = re.sub(
                r'\s*XZQXZQ\s*0*' + str(i) + r'\s*QZXQZX\s*', 
                placeholders[i], 
                translated, 
                flags=re.IGNORECASE
            )
        
        if re.search(r'XZQXZQ', translated, re.IGNORECASE): 
            return text
            
        return translated.strip()
    except Exception as e:
        return text 

def extract_custom_emojis_to_html(message):
    if not message.text or not message.entities: 
        return message.text or ""
        
    text = message.text
    entities = sorted(
        [e for e in message.entities if e.type == 'custom_emoji'], 
        key=lambda x: x.offset, 
        reverse=True
    )
    
    encoded_text = text.encode('utf-16-le')
    
    for ent in entities:
        start = ent.offset * 2
        end = start + (ent.length * 2)
        emoji_char = encoded_text[start:end].decode('utf-16-le')
        html_emoji = f'<tg-emoji emoji-id="{ent.custom_emoji_id}">{emoji_char}</tg-emoji>'
        encoded_text = encoded_text[:start] + html_emoji.encode('utf-16-le') + encoded_text[end:]
        
    return encoded_text.decode('utf-16-le')

def parse_button_input(message):
    text = message.text
    emoji_id = None
    
    if message.entities:
        for ent in message.entities:
            if ent.type == 'custom_emoji':
                emoji_id = ent.custom_emoji_id
                emoji_char = message.text[ent.offset:ent.offset+ent.length]
                text = text.replace(emoji_char, '', 1) 
                break
                
    text = clean_old_emojis(text)
    return text.strip(), emoji_id

def get_text(uid, key, *args):
    l = get_lang(uid)
    
    if l not in ['ar', 'en']: 
        l = 'ar'
        
    try:
        custom = db.custom_texts.find_one({'lang': l, 'key': key})
        if custom and custom.get('value') and custom['value'].strip(): 
            base_text = custom['value']
        else: 
            base_text = LANG.get(l, LANG['ar']).get(key, "")
    except Exception as e:
        base_text = LANG.get(l, LANG['ar']).get(key, "")
        
    if not base_text: 
        base_text = LANG.get('ar', {}).get(key, "") or LANG.get('en', {}).get(key, "")
        
    if args:
        try: 
            return base_text.format(*args)
        except Exception as e: 
            return base_text
            
    return base_text

def get_btn_data(uid, key):
    l = get_lang(uid)
    
    if l not in ['ar', 'en']: 
        l = 'ar'
        
    try:
        custom = db.custom_buttons.find_one({'lang': l, 'key': key})
        if custom:
            text = custom.get('text', '').strip()
            emoji_id = custom.get('emoji_id', None)
            if not text: 
                return DEFAULT_BUTTONS.get(l, DEFAULT_BUTTONS['ar']).get(key, key), emoji_id
            return text, emoji_id
    except Exception as e: 
        pass
        
    return DEFAULT_BUTTONS.get(l, DEFAULT_BUTTONS['ar']).get(key, key), None

def create_btn(uid, key, callback_data=None, url=None, style=None):
    text, emj_id = get_btn_data(uid, key)
    kwargs = {'text': text}
    
    if callback_data: 
        kwargs['callback_data'] = callback_data
    if url: 
        kwargs['url'] = url
    if style: 
        kwargs['style'] = style
    if emj_id: 
        kwargs['icon_custom_emoji_id'] = emj_id
        
    return CustomInlineButton(**kwargs)

# ============================================================
# ⚙️ دوال المساعدة العامة
# ============================================================
def clean_name(text): 
    if not text: 
        return "بدون اسم"
    cleaned = re.sub(r'<[^>]+>', '', str(text)).strip()
    return html.escape(cleaned)

def obscure_text(text):
    if not text: 
        return "***"
        
    if '@' in text:
        parts = text.split('@')
        name = parts[0]
        domain = parts[1]
        if len(name) > 2: 
            return name[0] + "***" + name[-1] + "@" + domain
        else: 
            return name[0] + "***@" + domain
    else:
        if len(text) > 2: 
            return text[0] + "***" + text[-1]
        return text[0] + "***"

def find_product(pid):
    pid_str = str(pid)
    try:
        p = db.products.find_one({'id': pid_str})
        if p: 
            return p
            
        if pid_str.isdigit():
            p = db.products.find_one({'id': int(pid_str)})
            if p: 
                return p
                
        try:
            p = db.products.find_one({'id': float(pid_str)})
            if p: 
                return p
        except Exception: 
            pass
            
        if len(pid_str) == 24:
            try:
                p = db.products.find_one({'_id': ObjectId(pid_str)})
                if p: 
                    return p
            except Exception: 
                pass
    except Exception: 
        pass
        
    return None

def get_product_stock_count(pid):
    try:
        pid_str = str(pid)
        queries = [{'product_id': pid_str}]
        
        if pid_str.isdigit(): 
            queries.append({'product_id': int(pid_str)})
            
        try: 
            queries.append({'product_id': float(pid_str)})
        except Exception: 
            pass
            
        return db.product_stock.count_documents({'$or': queries, 'is_sold': False})
    except Exception: 
        return 0

def get_user_data_full(uid):
    return db.users.find_one({'user_id': uid})

def get_lang(uid):
    u = get_user_data_full(uid)
    if u:
        return u.get('lang', 'ar')
    return 'ar'

def is_user_banned(uid):
    u = get_user_data_full(uid)
    if u and u.get('is_banned') == 1:
        return True
    return False

def check_forced_sub(uid):
    if uid == OWNER_ID: 
        return True
        
    user_db = get_user_data_full(uid)
    if user_db and user_db.get('is_admin') == 1: 
        return True
        
    chans = list(db.required_channels.find())
    if not chans: 
        return True
        
    for c in chans:
        try:
            status = bot.get_chat_member(c['channel_id'], uid).status
            if status in ['left', 'kicked']: 
                return False
        except Exception: 
            return False
            
    return True

def notify_admins(message_text):
    if OWNER_ID:
        try: 
            bot.send_message(OWNER_ID, message_text, parse_mode="HTML")
        except Exception: 
            pass
            
    admins = list(db.users.find({'is_admin': 1}))
    for admin in admins:
        if admin['user_id'] != OWNER_ID:
            try: 
                bot.send_message(admin['user_id'], message_text, parse_mode="HTML")
            except Exception: 
                pass

# ============================================================
# 👥 8. نظام تحديث الإحالات الديناميكي السريع (في الخلفية)
# ============================================================
def update_referrer_balance(referrer_id):
    """دالة تقوم بحساب أرباح الشخص المحيل وتحديث رصيده بناءً على عدد الإحالات النشطة فقط"""
    try:
        active_count = db.users.count_documents({'referred_by': str(referrer_id), 'ref_status': 'active'})
        expected_earnings = (active_count // 10) * 0.10 # 0.10$ لكل 10 أشخاص

        referrer = db.users.find_one({'user_id': int(referrer_id)})
        if not referrer: return

        current_ref_earned = float(referrer.get('ref_earned', 0.0))

        if expected_earnings != current_ref_earned:
            diff = expected_earnings - current_ref_earned
            db.users.update_one(
                {'user_id': int(referrer_id)},
                {
                    '$inc': {'balance': diff},
                    '$set': {'ref_earned': expected_earnings}
                }
            )
    except Exception as e:
        logger.error(f"Error updating referrer balance: {e}")

def background_referral_checker():
    """هذا المحرك يعمل في الخلفية بصمت لفحص المشتركين وخصم الرصيد إذا غادروا لضمان سرعة فائقة للمستخدمين"""
    while True:
        try:
            referred_users = list(db.users.find({'referred_by': {'$ne': None}}))
            for ru in referred_users:
                inv_uid = ru['user_id']
                referrer_id = int(ru['referred_by'])
                current_status = ru.get('ref_status', 'pending')

                is_subbed = check_forced_sub(inv_uid)
                new_status = current_status

                if is_subbed:
                    if current_status != 'active':
                        new_status = 'active'
                else:
                    if current_status == 'active':
                        new_status = 'left'
                    elif current_status == 'pending':
                        new_status = 'pending'

                if new_status != current_status:
                    db.users.update_one({'user_id': inv_uid}, {'$set': {'ref_status': new_status}})
                    update_referrer_balance(referrer_id)

                time.sleep(0.1)
        except Exception as e:
            pass
        time.sleep(300)

threading.Thread(target=background_referral_checker, daemon=True).start()

# ============================================================
# 🏠 9. معالج البداية وتحديث الإحالة
# ============================================================
@bot.message_handler(commands=['start'])
def start_handler(message):
    is_callback = isinstance(message, types.CallbackQuery)
    
    if is_callback:
        chat_id = message.message.chat.id
    else:
        chat_id = message.chat.id
        
    from_user = message.from_user
    uid = from_user.id
    
    if from_user.username:
        uname = from_user.username.lower()
    else:
        uname = ""
    
    if is_user_banned(uid):
        bot.send_message(chat_id, get_text(uid, 'banned'), parse_mode="HTML")
        return

    user = get_user_data_full(uid)
    
    if not user:
        if is_callback:
            full_text = ""
        else:
            full_text = message.text or ""
            
        args = full_text.split()
        
        if len(args) > 1 and args[1].isdigit():
            ref = args[1]
        else:
            ref = None
        
        db.users.insert_one({
            'user_id': uid, 
            'name': from_user.first_name, 
            'username': uname, 
            'referred_by': ref, 
            'ref_status': 'pending', 
            'ref_earned': 0.0,
            'balance': 0.0, 
            'lang_chosen': False, 
            'lang': 'ar', 
            'is_admin': 0, 
            'is_banned': 0
        })
        user = get_user_data_full(uid)
    else:
        db.users.update_one({'user_id': uid}, {'$set': {'username': uname}})

    if not user.get('lang_chosen'):
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("🇸🇦 العربية", callback_data="init_lang_ar"),
            InlineKeyboardButton("🇬🇧 English", callback_data="init_lang_en")
        )
        bot.send_message(
            chat_id, 
            "🌐 <b>الرجاء اختيار لغتك / Please choose your language:</b>", 
            reply_markup=markup, 
            parse_mode="HTML"
        )
        return

    lang = user.get('lang', 'ar')
    if lang not in ['ar', 'en']: 
        lang = 'ar'
    
    # تحديث وتأكيد حالة الاشتراك الفورية للإحالات
    if not check_forced_sub(uid):
        if user.get('ref_status') == 'active':
            db.users.update_one({'user_id': uid}, {'$set': {'ref_status': 'left'}})
            if user.get('referred_by'): update_referrer_balance(int(user['referred_by']))
            
        chans = list(db.required_channels.find())
        markup = InlineKeyboardMarkup(row_width=1)
        for c in chans: 
            if lang == 'en':
                btn_txt = "📢 Channel"
            else:
                btn_txt = "📢 القناة"
            
            markup.add(InlineKeyboardButton(btn_txt, url=f"https://t.me/{c['channel_id'].replace('@','') }"))
            
        markup.add(create_btn(uid, 'btn_check_sub', callback_data="main_menu_refresh"))
        bot.send_message(chat_id, get_text(uid, 'must_join'), reply_markup=markup, parse_mode="HTML")
        return
    else:
        if user.get('ref_status') in ['pending', 'left']:
            db.users.update_one({'user_id': uid}, {'$set': {'ref_status': 'active'}})
            if user.get('referred_by'): update_referrer_balance(int(user['referred_by']))

    users_total = db.users.count_documents({})
    markup = InlineKeyboardMarkup(row_width=2)
    
    markup.add(
        create_btn(uid, 'btn_gh', callback_data="github_pack_info")
    )
    markup.add(
        create_btn(uid, 'btn_gemini', callback_data="gemini_pack_info")
    )
    
    markup.add(
        create_btn(uid, 'btn_products', callback_data="open_shop", style="primary"),
        create_btn(uid, 'btn_deposit', callback_data="open_deposit")
    )
               
    markup.add(
        create_btn(uid, 'btn_profile', callback_data="open_profile"),
        create_btn(uid, 'btn_invite', callback_data="open_invite")
    )
               
    markup.add(
        create_btn(uid, 'btn_support', url=f"https://t.me/{OWNER_USER}"),
        create_btn(uid, 'btn_lang', callback_data="toggle_language")
    )
    
    if user.get('is_admin') == 1 or uid == OWNER_ID:
        markup.add(
            create_btn(uid, 'btn_admin', callback_data="admin_panel_main")
        )

    welcome_message = get_text(
        uid, 
        'welcome', 
        uid, 
        clean_name(from_user.first_name), 
        users_total, 
        user.get('balance', 0.0)
    )
    
    bot.send_message(
        chat_id, 
        welcome_message, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("init_lang_"))
def init_lang_selection(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    new_lang = call.data.replace("init_lang_", "").strip()
    
    if new_lang not in ['ar', 'en']:
        new_lang = 'ar'
        
    db.users.update_one(
        {'user_id': call.from_user.id},
        {'$set': {'lang': new_lang, 'lang_chosen': True}}
    )
    
    try: 
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception: 
        pass
        
    call.message.from_user = call.from_user
    start_handler(call.message)

@bot.callback_query_handler(func=lambda call: call.data == "toggle_language")
def toggle_lang(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    
    if is_user_banned(uid): 
        return
        
    u = get_user_data_full(uid)
    if u.get('lang', 'ar') == 'ar':
        new_l = 'en'
    else:
        new_l = 'ar'
        
    db.users.update_one({'user_id': uid}, {'$set': {'lang': new_l}})
    
    try: 
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception: 
        pass
        
    call.message.from_user = call.from_user
    start_handler(call.message)

@bot.callback_query_handler(func=lambda call: call.data == "main_menu_refresh")
def refresh_main(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    
    try: 
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception: 
        pass
        
    call.message.from_user = call.from_user
    start_handler(call.message)

# ============================================================
# 👤 10. نظام الإحالات الصاروخي (بدون لود)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_invite")
def invite_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    
    if is_user_banned(uid): 
        return
        
    if not check_forced_sub(uid): 
        start_handler(call.message)
        return
    
    u = get_user_data_full(uid)
    if u:
        l = u.get('lang', 'ar')
    else:
        l = 'ar'
        
    b_n = bot.get_me().username
    
    # تحديث سريع للأرباح قبل العرض
    update_referrer_balance(uid)
    u = get_user_data_full(uid) 
    
    # جلب الإحصائيات الفورية من قاعدة البيانات فقط (سريع جداً)
    pending_count = db.users.count_documents({'referred_by': str(uid), 'ref_status': 'pending'})
    active_count = db.users.count_documents({'referred_by': str(uid), 'ref_status': 'active'})
    left_count = db.users.count_documents({'referred_by': str(uid), 'ref_status': 'left'})
    total_clicks = pending_count + active_count + left_count
    
    actual_earned = float(u.get('ref_earned', 0.0))

    markup = InlineKeyboardMarkup()
    markup.add(
         create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh")
    )
    
    try: 
        bot.edit_message_text(
            get_text(
                uid, 
                'invite_txt', 
                b_n, 
                uid, 
                total_clicks, 
                pending_count, 
                active_count, 
                left_count, 
                actual_earned
            ), 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

# ============================================================
# 🛒 11. المتجر والشراء والترتيب الأبجدي للمنتجات 
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_shop")
def shop_list_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    
    if is_user_banned(uid): 
        return
        
    if not check_forced_sub(uid): 
        start_handler(call.message)
        return
    
    u = get_user_data_full(uid)
    if u.get('is_admin') == 1 or uid == OWNER_ID:
        is_admin = True
    else:
        is_admin = False
     
    l = get_lang(uid)
    
    prods = list(db.products.find())
    
    def sort_key(x):
        if l == 'en':
            return str(x.get('name_en')).lower()
        else:
            return str(x.get('name_ar')).lower()
            
    prods.sort(key=sort_key)
    
    markup = InlineKeyboardMarkup(row_width=1)
 
    markup.add(
        create_btn(uid, 'btn_gh', callback_data="github_pack_info")
    )
    markup.add(
        create_btn(uid, 'btn_gemini', callback_data="gemini_pack_info")
    )
    
    for p in prods:
        is_hidden = p.get('is_hidden', False)
        if is_hidden and not is_admin:
            continue
            
        is_manual = p.get('is_manual', False)
        pid = p.get('id', str(p.get('_id', '')))
        st = get_product_stock_count(pid)
        
        if is_manual or st > 0:
            btn_style = "success"
        else:
            btn_style = "danger"
        
        if is_hidden:
            hidden_icon = " 👻(مخفي)"
        else:
            hidden_icon = ""
            
        if l == 'en':
            n = clean_html(p.get('name_en'))
        else:
            n = clean_html(p.get('name_ar'))
            
        if len(n) > 25:
            short_n = n[:25] + ".."
        else:
            short_n = n
        
        if is_manual:
            st_text = "FW"
        else:
            st_text = str(st)
            
        btn_text = f"{short_n} | ${p.get('price', 0):.2f} | 📦 {st_text}{hidden_icon}"
        
        btn_kwargs = {
            'text': btn_text,
            'callback_data': f"vi_p_{pid}",
            'style': btn_style
        }
        
        custom_emoji_id = p.get('custom_emoji_id')
        if custom_emoji_id:
            btn_kwargs['icon_custom_emoji_id'] = custom_emoji_id
            
        markup.add(CustomInlineButton(**btn_kwargs))
        
    markup.add(
        create_btn(uid, 'btn_refresh', callback_data="open_shop")
    )
    markup.add(
        create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh")
    )
    
    store_emoji_id = get_setting('emoji_store', '')
    store_text = get_text(uid, 'store_title')
    
    if store_emoji_id and store_emoji_id != "Not Set":
        store_text = f'<tg-emoji emoji-id="{store_emoji_id}">✨</tg-emoji> ' + store_text

    try: 
        bot.edit_message_text(
            store_text, 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("vi_p_"))
def shop_detail_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    
    if is_user_banned(uid): 
        return
        
    l = get_lang(uid)
    pid = call.data.replace('vi_p_', '')
    
    p = find_product(pid)
    if not p: 
        bot.send_message(uid, "❌ عذراً، المنتج غير متوفر.", parse_mode="HTML")
        return
    
    u = get_user_data_full(uid)
    
    if u.get('is_admin') == 1 or uid == OWNER_ID:
        is_admin = True
    else:
        is_admin = False
    
    if p.get('is_hidden', False) and not is_admin:
        bot.send_message(uid, "❌ عذراً، هذا المنتج غير متوفر حالياً.", parse_mode="HTML")
        return

    is_manual = p.get('is_manual', False)
    st = get_product_stock_count(pid)
    
    if l == 'ar':
        if is_manual:
            delivery_type = "يدوي 🤝"
            st_text = "غير محدود"
        else:
            delivery_type = "تلقائي ⚡"
            st_text = f"{st} قطعة"
    else:
        if is_manual:
            delivery_type = "Manual 🤝"
            st_text = "Unlimited"
        else:
            delivery_type = "Auto ⚡"
            st_text = f"{st} pcs"
        
    if l == 'en':
        n = str(p.get('name_en'))
        d = str(p.get('desc_en'))
    else:
        n = str(p.get('name_ar'))
        d = str(p.get('desc_ar'))
    
    custom_emoji_id = p.get('custom_emoji_id')
    if custom_emoji_id:
        icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji>'
    else:
        icon_html = '📦'
    
    if l == 'en':
        text = f"{icon_html} <b>{n}</b>\n\n📝 {d}\n\n🚚 <b>Delivery:</b> {delivery_type}\n💰 <b>Price:</b> ${p.get('price', 0):.2f}\n📊 <b>Stock:</b> {st_text}"
    else:
        text = f"{icon_html} <b>{n}</b>\n\n📝 {d}\n\n🚚 <b>نوع التسليم:</b> {delivery_type}\n💰 <b>السعر:</b> ${p.get('price', 0):.2f}\n📊 <b>المتوفر:</b> {st_text}"
    
    markup = InlineKeyboardMarkup()
    if is_manual or st > 0: 
        markup.add(
            create_btn(uid, 'btn_buy_now', callback_data=f"buy_qty_{pid}")
        )
        
    markup.add(
        create_btn(uid, 'btn_back', callback_data="open_shop")
    )
    
    try: 
        bot.edit_message_text(
            text, 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_qty_"))
def prompt_quantity(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    
    if is_user_banned(uid): 
        return
        
    l = get_lang(uid)
    pid = call.data.replace('buy_qty_', '')
    
    p = find_product(pid)
    if not p: 
        return
    
    is_manual = p.get('is_manual', False)
   
    if not is_manual and get_product_stock_count(pid) == 0:
        bot.send_message(uid, get_text(uid, 'out_stock'), parse_mode="HTML")
        return
        
    msg = bot.send_message(uid, get_text(uid, 'qty_prompt'), parse_mode="HTML")
    bot.register_next_step_handler(msg, execute_bulk_buy, pid, l)

def execute_bulk_buy(message, pid, lang):
    uid = message.from_user.id
    if is_user_banned(uid): 
        return
        
    if not message.text or not message.text.isdigit():
        bot.send_message(uid, get_text(uid, 'qty_invalid'), parse_mode="HTML")
        return
        
    qty = int(message.text.strip())
    if qty <= 0:
        bot.send_message(uid, get_text(uid, 'qty_invalid'), parse_mode="HTML")
        return

    u = get_user_data_full(uid)
    p = find_product(pid)
    if not p: 
        return

    is_manual = p.get('is_manual', False)
    
    if not is_manual:
        pid_str = str(pid)
        queries = [{'product_id': pid_str}]
        
        if pid_str.isdigit(): 
            queries.append({'product_id': int(pid_str)})
            
        try: 
            queries.append({'product_id': float(pid_str)})
        except Exception: 
            pass
        
        stk_items = list(db.product_stock.find({'$or': queries, 'is_sold': False}).limit(qty))
        
        if len(stk_items) < qty:
            bot.send_message(uid, get_text(uid, 'qty_not_enough', len(stk_items)), parse_mode="HTML")
            return
        
    total_price = float(p.get('price', 0)) * qty
    
    if float(u.get('balance', 0)) < total_price:
        bot.send_message(uid, get_text(uid, 'no_balance'), parse_mode="HTML")
        return
        
    db.users.update_one({'user_id': uid}, {'$inc': {'balance': -total_price}})
    
    if OWNER_USER:
        support_user = f"@{OWNER_USER}"
    else:
        support_user = "الإدارة"
        
    if u and u.get('username'):
        buyer_m = f"@{u['username']}"
    else:
        buyer_m = f"عضو جديد"
        
    log_ch = get_setting('log_channel')
    
    custom_emj = p.get('custom_emoji_id')
    if custom_emj:
        icon_html = f'<tg-emoji emoji-id="{custom_emj}">✨</tg-emoji> '
    else:
        icon_html = '📦 '
        
    if lang == 'en':
        p_name_html = str(p.get('name_en'))
    else:
        p_name_html = str(p.get('name_ar'))

    if is_manual:
        order_id = "M" + str(int(time.time()))[-6:] + str(uid)[-2:]
        db.orders.insert_one({
            'user_id': uid, 
            'product_id': str(pid), 
            'code_delivered': f"طلب يدوي: {order_id}"
        })
        
        if lang == 'ar':
            msg_txt = f"✅ <b>تم الطلب بنجاح! وتم خصم (${total_price:.2f})</b>\n\nهذا المنتج يتطلب (تسليم يدوي).\nرقم طلبك: <code>{order_id}</code>\n\nيرجى التواصل مع {support_user} لتنفيذ طلبك."
        else:
            msg_txt = f"✅ <b>Order Placed! (${total_price:.2f} deducted)</b>\n\nThis is a manual delivery product.\nOrder ID: <code>{order_id}</code>\n\nPlease contact {support_user}."
        
        bot.send_message(uid, msg_txt, parse_mode="HTML")
        
        admin_msg = f"🔐 <b>إشعار إدارة (طلب تسليم يدوي) 🤝</b>\n\n👤 العميل: {buyer_m} (<code>{uid}</code>)\n{icon_html} المنتج: {p_name_html}\n🔢 الكمية: {qty}\n💰 دفع: ${total_price:.2f}\n🔖 رقم الطلب: <code>{order_id}</code>\n\n⚠️ <b>تواصل مع العميل لتسليمه طلبه!</b>"
        notify_admins(admin_msg)
    else:
        delivered_codes = []
        for item in stk_items:
            db.product_stock.update_one({'_id': item['_id']}, {'$set': {'is_sold': True}})
            db.orders.insert_one({
                'user_id': uid, 
                'product_id': str(pid), 
                'code_delivered': item['code_line']
            })
            delivered_codes.append(item['code_line'])
            
        if qty > 3:
            file_content = ""
            for i, code in enumerate(delivered_codes, 1): 
                file_content += f"{i}. {code}\n"
                
            f = io.BytesIO(file_content.encode('utf-8'))
            f.name = f"Your_Codes_{pid}.txt"
            
            if lang == 'ar':
                success_msg = f"✅ <b>تم الشراء بنجاح!</b>\nتم إرفاق {qty} أكواد في هذا الملف 📄"
            else:
                success_msg = f"✅ <b>Purchase Successful!</b>\n{qty} codes attached 📄"
                
            bot.send_document(uid, f, caption=success_msg, parse_mode="HTML")
        else:
            codes_str = "\n".join([f"<code>{c}</code>" for c in delivered_codes])
            bot.send_message(uid, get_text(uid, 'buy_success', codes_str), parse_mode="HTML")
        
        admin_msg = f"🔐 <b>إشعار إدارة (شراء تلقائي) ⚡</b>\n\n👤 العميل: {buyer_m} (<code>{uid}</code>)\n{icon_html} المنتج: {p_name_html}\n🔢 الكمية: {qty}\n💰 دفع: ${total_price:.2f}"
        notify_admins(admin_msg)

    if log_ch and log_ch != "Not Set":
        try: 
            obs_user = obscure_text(u.get('username') or str(uid))
            custom_log = db.custom_texts.find_one({'lang': 'ar', 'key': 'log_buy'})
            
            if custom_log and custom_log.get('value'):
                pub_msg = custom_log['value'].replace('{user}', obs_user).replace('{product}', f"{icon_html} {p_name_html}").replace('{qty}', str(qty))
            else:
                pub_msg = f"🛒 <b>New Purchase!</b> 🛍\n\n👤 User: <b>{obs_user}</b>\n{icon_html} Product: <b>{p_name_html}</b>\n🔢 QTY: <b>{qty}</b>\n\n<i>Thank you for choosing us 🛡️</i>"
        
            bot.send_message(log_ch, pub_msg, parse_mode="HTML")
        except Exception: 
            pass

# ============================================================
# 🏦 12. بوابات الدفع 
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_deposit")
def dep_init_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    
    if is_user_banned(uid): 
        return
        
    if not check_forced_sub(uid): 
        start_handler(call.message)
        return
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        create_btn(uid, 'btn_stars', callback_data="dep_stars")
    )
    markup.add(
        create_btn(uid, 'btn_binance', callback_data="dep_binance")
    )
    markup.add(
        create_btn(uid, 'btn_usdt_trc20', callback_data="dep_crypto_USDT")
    )
    markup.add(
        create_btn(uid, 'btn_usdt_bep20', callback_data="dep_crypto_USDT_BEP20")
    )
    markup.add(
        create_btn(uid, 'btn_ton', callback_data="dep_crypto_TON")
    )
    markup.add(
        create_btn(uid, 'btn_ltc', callback_data="dep_crypto_LTC")
    )
    markup.add(
        create_btn(uid, 'btn_back', callback_data="open_profile")
    )
    
    try: 
        bot.edit_message_text(
            get_text(uid, 'dep_choose'), 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data == "dep_stars")
def dep_stars_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    l = get_lang(uid)
    
    bot.clear_step_handler_by_chat_id(chat_id=uid)
    
    if l == 'ar':
        prompt = f"⭐️ <b>أرسل المبلغ الذي تريد شحنه بالدولار ($):</b>\n<i>(سيتم تحويله تلقائياً، 1$ = {STARS_RATE} نجمة)</i>"
    else:
        prompt = f"⭐️ <b>Send the amount you want to deposit in USD ($):</b>\n<i>(Will be converted automatically, 1$ = {STARS_RATE} Stars)</i>"
    
    msg = bot.send_message(uid, prompt, parse_mode="HTML")
    bot.register_next_step_handler(msg, process_stars_amount, l)

def process_stars_amount(message, lang):
    uid = message.from_user.id
    try:
        usd_amount = float(message.text.strip())
        
        if usd_amount < 0.1:
            if lang == 'ar':
                err = "❌ الحد الأدنى للشحن هو 0.1$"
            else:
                err = "❌ Minimum deposit is $0.1"
                
            bot.send_message(uid, err, parse_mode="HTML")
            return
            
        stars_amount = int(usd_amount * STARS_RATE)
        
        if lang == 'ar':
            title = "شحن رصيد المتجر"
            desc = f"شحن حساب بمبلغ ${usd_amount:.2f}"
        else:
            title = "Shop Balance Deposit"
            desc = f"Deposit ${usd_amount:.2f} to your account"
            
        prices = [LabeledPrice(label=f"Deposit ${usd_amount:.2f}", amount=stars_amount)]
        
        bot.send_invoice(
            chat_id=uid, 
            title=title, 
            description=desc, 
            invoice_payload=f"dep_{uid}_{usd_amount}", 
            provider_token="", 
            currency="XTR", 
            prices=prices
        )
    except ValueError:
        if lang == 'ar':
            err = "❌ الرجاء إرسال أرقام فقط."
        else:
            err = "❌ Please send numbers only."
        bot.send_message(uid, err, parse_mode="HTML")

@bot.pre_checkout_query_handler(func=lambda query: True)
def checkout(pre_checkout_query):
    bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@bot.message_handler(content_types=['successful_payment'])
def got_payment(message):
    uid = message.from_user.id
    pay_info = message.successful_payment
    payload = pay_info.invoice_payload
    
    if payload.startswith("dep_"):
        parts = payload.split('_')
        usd_amount = float(parts[2])
        tx_id = pay_info.telegram_payment_charge_id
        l = get_lang(uid)
        credit_user(uid, usd_amount, tx_id, l, "Telegram Stars ⭐️")

@bot.callback_query_handler(func=lambda call: call.data == "dep_binance")
def dep_binance_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    
    if is_user_banned(uid): 
        return
        
    l = get_lang(uid)
    bot.clear_step_handler_by_chat_id(chat_id=uid)
    
    wallet = get_setting('wallet_address')
    msg = bot.send_message(uid, get_text(uid, 'dep_pay', wallet), parse_mode="HTML")
    bot.register_next_step_handler(msg, verify_binance_pay, l)

@bot.callback_query_handler(func=lambda call: call.data.startswith("dep_crypto_"))
def dep_crypto_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    
    if is_user_banned(uid): 
        return
        
    l = get_lang(uid)
    coin = call.data.replace('dep_crypto_', '')
    bot.clear_step_handler_by_chat_id(chat_id=uid)
    
    if coin == "USDT": 
        db_key = "usdt_address"
    elif coin == "USDT_BEP20": 
        db_key = "usdt_bep20_address"
    elif coin == "TON": 
        db_key = "ton_address"
    else: 
        db_key = "ltc_address"
        
    wallet = get_setting(db_key)
    
    if coin == "USDT": 
        msg_txt = get_text(uid, 'dep_usdt', wallet)
    elif coin == "USDT_BEP20": 
        if l == 'ar':
            msg_txt = f"🟡 <b>شحن عبر USDT (BEP-20)</b>\n\nأرسل المبلغ إلى المحفظة:\n<code>{wallet}</code>\n\n⚠️ <b>الشبكة المقبولة: BEP-20 (BSC) فقط.</b>\n⚠️ بعد التحويل، <b>أرسل الهاش (TxID) كنص هنا.</b>"
        else:
            msg_txt = f"🟡 <b>USDT (BEP-20) Deposit</b>\n\nSend to address:\n<code>{wallet}</code>\n\n⚠️ <b>Network: BEP-20 ONLY.</b>\n⚠️ Send <b>TxID (Hash)</b> here as text."
    elif coin == "TON": 
        if l == 'ar':
            msg_txt = f"💎 <b>شحن عبر Toncoin (TON)</b>\n\nأرسل المبلغ إلى المحفظة:\n<code>{wallet}</code>\n\n⚠️ <b>تأكد من وضع الـ Memo إذا كان مطلوباً!</b>\n⚠️ بعد التحويل، <b>أرسل الهاش (TxID) كنص هنا.</b>"
        else:
            msg_txt = f"💎 <b>TON Deposit</b>\n\nSend to address:\n<code>{wallet}</code>\n\n⚠️ <b>Don't forget the Memo if required!</b>\n⚠️ Send <b>TxID (Hash)</b> here as text."
    else: 
        msg_txt = get_text(uid, 'dep_ltc', wallet)
        
    msg = bot.send_message(uid, msg_txt, parse_mode="HTML")
    
    if coin == "LTC": 
        bot.register_next_step_handler(msg, verify_ltc_public_blockchain, l, wallet)
    elif coin == "TON": 
        bot.register_next_step_handler(msg, verify_crypto_tx, l, "TON")
    elif coin == "USDT_BEP20": 
        bot.register_next_step_handler(msg, verify_crypto_tx, l, "USDT")
    else: 
        bot.register_next_step_handler(msg, verify_crypto_tx, l, coin)

def verify_binance_pay(message, lang):
    uid = message.from_user.id
    if is_user_banned(uid): 
        return
        
    if not message.text: 
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
        return

    tx_id = message.text.strip()
    
    if len(tx_id) < 5: 
        bot.send_message(uid, "❌ <b>رقم العملية غير صحيح أو قصير جداً! الرجاء إرسال الـ Order ID بشكل صحيح.</b>", parse_mode="HTML")
        return
        
    with tx_lock:
        if tx_id in PROCESSING_TXS: 
            bot.send_message(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل، يرجى عدم التكرار.</b>", parse_mode="HTML")
            return
            
        if db.used_transactions.find_one({'transaction_id': tx_id}): 
            bot.reply_to(message, get_text(uid, 'tx_used'))
            return
            
        PROCESSING_TXS.add(tx_id)
        
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        
        success = False
        found = False
        amt = 0.0
        
        for attempt in range(4): 
            try:
                client = get_binance_client()
                pay_h = client.get_pay_trade_history().get('data', [])
                current_time_ms = int(time.time() * 1000)
                
                for d in pay_h:
                    if tx_id.lower() == str(d.get('orderId', '')).lower():
                        if (current_time_ms - int(d.get('transactionTime', 0))) > 24 * 60 * 60 * 1000:
                            bot.send_message(uid, "❌ <b>مرفوض:</b> الحوالة قديمة جداً.", parse_mode="HTML")
                            return
                            
                        found = True
                        amt = float(d.get('amount', 0.0))
                        break
                        
                success = True
                break 
            except Exception as e:
                logger.error(f"Binance Pay Proxy Attempt {attempt+1} Failed: {e}")
                time.sleep(1.5)
                
        if not success:
            bot.send_message(uid, "❌ السيرفر يواجه ضغطاً حالياً (تم تبديل البروكسي عدة مرات). يرجى إعادة إرسال الرقم بعد دقيقة.", parse_mode="HTML")
            return
            
        if found: 
            credit_user(uid, amt, tx_id.lower(), lang, "Binance Pay")
        else: 
            bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
            
    finally:
        PROCESSING_TXS.discard(tx_id)

def verify_crypto_tx(message, lang, coin):
    uid = message.from_user.id
    if is_user_banned(uid): 
        return
        
    if not message.text: 
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
        return

    tx_id = message.text.strip().lower()
    if len(tx_id) < 5: 
        bot.send_message(uid, "❌ <b>رقم الهاش (TxID) غير صحيح أو قصير جداً!</b>", parse_mode="HTML")
        return
        
    with tx_lock:
        if tx_id in PROCESSING_TXS: 
            bot.send_message(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل.</b>", parse_mode="HTML")
            return
            
        if db.used_transactions.find_one({'transaction_id': tx_id}): 
            bot.reply_to(message, get_text(uid, 'tx_used'))
            return
            
        PROCESSING_TXS.add(tx_id)
        
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        
        success = False
        found = False
        status = -1
        amt = 0.0
        
        for attempt in range(4): 
            try:
                client = get_binance_client()
                res = client.get_deposit_history(coin=coin)
                current_time_ms = int(time.time() * 1000)
                
                for d in res:
                    if tx_id in str(d.get('txId', '')).lower():
                        if (current_time_ms - int(d.get('insertTime', 0))) > 24 * 60 * 60 * 1000:
                            bot.send_message(uid, "❌ <b>مرفوض:</b> الحوالة قديمة جداً.", parse_mode="HTML")
                            return
                            
                        found = True
                        status = int(d.get('status', -1))
                        amt = float(d.get('amount', 0.0))
                        break
                        
                success = True
                break
            except Exception as e:
                logger.error(f"Crypto Proxy Attempt {attempt+1} Failed: {e}")
                time.sleep(1.5)
                
        if not success:
            bot.send_message(uid, "❌ السيرفر يواجه ضغطاً حالياً. يرجى إعادة إرسال الهاش بعد دقيقة.", parse_mode="HTML")
            return
            
        if found:
            if status == 1: 
                credit_user(uid, amt, tx_id, lang, f"Crypto {coin}")
            else: 
                bot.send_message(uid, get_text(uid, 'dep_pending'), parse_mode="HTML")
        else: 
            bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
            
    finally:
        PROCESSING_TXS.discard(tx_id)

def verify_ltc_public_blockchain(message, lang, wallet_address):
    uid = message.from_user.id
    if is_user_banned(uid): 
        return
        
    if not message.text: 
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
        return
        
    tx_id = message.text.strip().lower()
    if len(tx_id) < 5: 
        bot.send_message(uid, "❌ <b>رقم الهاش (TxID) غير صحيح أو قصير جداً!</b>", parse_mode="HTML")
        return
        
    if wallet_address == "Not Set" or len(wallet_address) < 10: 
        bot.send_message(uid, "❌ <b>خطأ:</b> عنوان المحفظة غير معين.", parse_mode="HTML")
        return
        
    with tx_lock:
        if tx_id in PROCESSING_TXS: 
            bot.send_message(uid, "⏳ <b>يتم معالجة العملية.</b>", parse_mode="HTML")
            return
            
        if db.used_transactions.find_one({'transaction_id': tx_id}): 
            bot.reply_to(message, get_text(uid, 'tx_used'))
            return
            
        PROCESSING_TXS.add(tx_id)
        
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        
        received_ltc = 0.0
        confirmations = 0
        is_sender = False
        is_old = False
        current_time = int(time.time())
        
        try:
            url = f"https://litecoinspace.org/api/tx/{tx_id}"
            res = requests.get(url, timeout=10)
            
            if res.status_code == 200:
                data = res.json()
                block_time = data.get("status", {}).get("block_time", 0)
                
                if block_time > 0 and (current_time - block_time) > 24 * 60 * 60: 
                    is_old = True
                    
                for vin in data.get("vin", []):
                    if vin.get("prevout", {}).get("scriptpubkey_address") == wallet_address:
                        is_sender = True
                        break
                        
                if data.get("status", {}).get("confirmed"): 
                    confirmations = 1
                    
                for vout in data.get("vout", []):
                    if vout.get("scriptpubkey_address") == wallet_address: 
                        received_ltc += float(vout.get("value", 0)) / 100000000.0
        except Exception: 
            pass

        if received_ltc == 0.0 and not is_sender and not is_old:
            try:
                url2 = f"https://api.blockcypher.com/v1/ltc/main/txs/{tx_id}"
                res2 = requests.get(url2, timeout=10)
                
                if res2.status_code == 200:
                    data2 = res2.json()
                    confirmed_str = data2.get("confirmed")
                    
                    if confirmed_str:
                        try:
                            tx_t = datetime.datetime.strptime(confirmed_str[:19], "%Y-%m-%dT%H:%M:%S").timestamp()
                            if (current_time - tx_t) > 24 * 60 * 60: 
                                is_old = True
                        except Exception: 
                            pass
                            
                    for inp in data2.get("inputs", []):
                        if wallet_address in inp.get("addresses", []):
                            is_sender = True
                            break
                            
                    confirmations = data2.get("confirmations", 0)
                    
                    for output in data2.get("outputs", []):
                        if wallet_address in output.get("addresses", []): 
                            received_ltc += float(output.get("value", 0)) / 100000000.0
            except Exception: 
                pass

        if is_old: 
            bot.send_message(uid, "❌ <b>مرفوض:</b> الحوالة قديمة جداً.", parse_mode="HTML")
            return
            
        if is_sender: 
            bot.send_message(uid, "❌ <b>مرفوض:</b> هذه الحوالة صادرة من محفظتنا وليست إيداعاً.", parse_mode="HTML")
            return

        if received_ltc > 0:
            if confirmations >= 1:
                ltc_price = 80.0
                for attempt in range(4): 
                    try:
                        client = get_binance_client()
                        ltc_price = float(client.get_symbol_ticker(symbol="LTCUSDT")['price'])
                        break
                    except Exception: 
                        time.sleep(1)
                        
                usd_amount = received_ltc * ltc_price
                credit_user(uid, usd_amount, tx_id, lang, "Litecoin (LTC)")
            else: 
                bot.send_message(uid, get_text(uid, 'dep_pending'), parse_mode="HTML")
        else: 
            bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
            
    except Exception as e:
        bot.send_message(uid, f"❌ حدث خطأ أثناء فحص الشبكة.", parse_mode="HTML")
    finally:
        PROCESSING_TXS.discard(tx_id)

def credit_user(uid, amt, tx_id, lang, method):
    db.users.update_one({'user_id': uid}, {'$inc': {'balance': amt}})
    db.used_transactions.insert_one({'transaction_id': tx_id, 'amount': amt, 'user_id': uid})
    
    bot.send_message(uid, get_text(uid, 'dep_success', amt), parse_mode="HTML")
    
    u = get_user_data_full(uid)
    if u and u.get('username'):
        buyer_m = f"@{u['username']}"
    else:
        buyer_m = f"مستخدم"
    
    notify_admins(f"🔐 <b>إشعار إدارة (إيداع)</b>\n\n👤 العميل: {buyer_m} (<code>{uid}</code>)\n💰 المبلغ: <b>${amt:.2f}</b>\n💳 الطريقة: {method}\n🆔 رقم العملية:\n<code>{tx_id}</code>")
    
    log_ch = get_setting('log_channel')
    if log_ch and log_ch != "Not Set":
        try: 
            obs_user = obscure_text(u.get('username') or str(uid))
            custom_log = db.custom_texts.find_one({'lang': 'ar', 'key': 'log_dep'})
            
            if custom_log and custom_log.get('value'):
                pub_msg = custom_log['value'].replace('{user}', obs_user).replace('{amount}', f"${amt:.2f}").replace('{method}', method)
            else:
                pub_msg = f"💳 <b>New Deposit!</b> 💵\n\n👤 User: <b>{obs_user}</b>\n💰 Amount: <b>${amt:.2f}</b>\n🟢 Method: <b>{method}</b>\n\n<i>Processed automatically ⚡</i>"
            
            bot.send_message(log_ch, pub_msg, parse_mode="HTML")
        except Exception: 
            pass

# ============================================================
# 👑 13. لوحة الإدارة 
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "admin_panel_main")
def admin_main_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    l = get_lang(call.from_user.id)
    markup = InlineKeyboardMarkup(row_width=2)
    
    if l == 'en':
        markup.add(
            InlineKeyboardButton("➕ Add Product", callback_data="ad_p_add"), 
            InlineKeyboardButton("📦 Manage Stock", callback_data="ad_s_list")
        )
        markup.add(
            InlineKeyboardButton("📝 Edit Product", callback_data="ad_p_edit"), 
            InlineKeyboardButton("🗑 Delete Product", callback_data="ad_p_del")
        )
        markup.add(
            InlineKeyboardButton("👥 Users & Balances", callback_data="ad_users_main"), 
            InlineKeyboardButton("🚫 Ban / Unban User", callback_data="ad_ban_user")
        )
        markup.add(
            InlineKeyboardButton("👑 Promote Admin", callback_data="ad_new_admin"), 
            InlineKeyboardButton("💰 Gift Balance", callback_data="ad_gift")
        )
        markup.add(
            InlineKeyboardButton("📜 Records", callback_data="ad_logs_all"), 
            InlineKeyboardButton("📢 Broadcast", callback_data="ad_bc")
        )
        markup.add(
            InlineKeyboardButton("🌟 Set Product Icon", callback_data="ad_prod_emoji_start")
        )
        markup.add(
            InlineKeyboardButton("✏️ Customize Bot (CMS)", callback_data="ad_texts_main")
        )
        markup.add(
            InlineKeyboardButton("⚙️ Settings", callback_data="ad_shop_settings"), 
            InlineKeyboardButton("📢 Forced Sub", callback_data="ad_fsub_list")
        )
        markup.add(
            InlineKeyboardButton("🎓 API Settings", callback_data="ad_api_main")
        )
        markup.add(
            InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu_refresh")
        )
        text = "👑 <b>Admin Dashboard:</b>"
    else:
        markup.add(
            InlineKeyboardButton("➕ أضف منتج", callback_data="ad_p_add"), 
            InlineKeyboardButton("📦 إدارة الستوك", callback_data="ad_s_list")
        )
        markup.add(
            InlineKeyboardButton("📝 تعديل منتج", callback_data="ad_p_edit"), 
            InlineKeyboardButton("🗑 حذف منتج", callback_data="ad_p_del")
        )
        markup.add(
            InlineKeyboardButton("👥 إدارة العملاء", callback_data="ad_users_main"), 
            InlineKeyboardButton("🚫 حظر / فك حظر", callback_data="ad_ban_user")
        )
        markup.add(
            InlineKeyboardButton("👑 ترقية مدير", callback_data="ad_new_admin"), 
            InlineKeyboardButton("💰 شحن رصيد", callback_data="ad_gift")
        )
        markup.add(
            InlineKeyboardButton("📜 السجلات", callback_data="ad_logs_all"), 
            InlineKeyboardButton("📢 برودكاست للأعضاء", callback_data="ad_bc")
        )
        markup.add(
            InlineKeyboardButton("🌟 تعيين أيقونة لمنتج", callback_data="ad_prod_emoji_start")
        )
        markup.add(
            InlineKeyboardButton("✏️ تخصيص البوت والأزرار", callback_data="ad_texts_main")
        )
        markup.add(
            InlineKeyboardButton("⚙️ إعدادات المتجر", callback_data="ad_shop_settings"), 
            InlineKeyboardButton("📢 الاشتراك الإجباري", callback_data="ad_fsub_list")
        )
        markup.add(
            InlineKeyboardButton("🎓 إعدادات التفعيلات", callback_data="ad_api_main")
        )
        markup.add(
            InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="main_menu_refresh")
        )
        text = "👑 <b>لوحة القيادة (الإدارة):</b>"
        
    try: 
        bot.edit_message_text(
            text, 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_texts_main")
def ad_texts_main_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("📝 نصوص الرسائل", callback_data="ad_cms_msgs"), 
        InlineKeyboardButton("🎛 أزرار البوت", callback_data="ad_cms_btns_cats")
    )
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main")
    )
    bot.edit_message_text(
        "✏️ <b>نظام التخصيص (CMS):</b>", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data == "ad_cms_msgs")
def ad_cms_msgs_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("رسالة الترحيب (Start)", callback_data="edit_txt_welcome"), 
        InlineKeyboardButton("رسالة قسم الشحن", callback_data="edit_txt_dep_choose")
    )
    markup.add(
        InlineKeyboardButton("رسالة قسم الإحالات", callback_data="edit_txt_invite_txt"), 
        InlineKeyboardButton("إشعار توفر ستوك", callback_data="edit_txt_new_stock")
    )
    markup.add(
        InlineKeyboardButton("إشعار التخفيضات", callback_data="edit_txt_price_drop"), 
        InlineKeyboardButton("عنوان المتجر", callback_data="edit_txt_store_title")
    )
    markup.add(
        InlineKeyboardButton("قناة اللوج (إشعار الشراء)", callback_data="edit_txt_log_buy"), 
        InlineKeyboardButton("قناة اللوج (إشعار الإيداع)", callback_data="edit_txt_log_dep")
    )
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="ad_texts_main")
    )
    bot.edit_message_text(
        "📝 <b>تخصيص نصوص الرسائل:</b>", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data == "ad_cms_btns_cats")
def ad_cms_btns_cats_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("🏠 أزرار القائمة الرئيسية", callback_data="ad_cms_b_start")
    )
    markup.add(
        InlineKeyboardButton("💳 أزرار الشحن والدفع", callback_data="ad_cms_b_dep")
    )
    markup.add(
        InlineKeyboardButton("👤 أزرار الملف والمشتريات", callback_data="ad_cms_b_prof")
    )
    markup.add(
        InlineKeyboardButton("🛒 أزرار المتجر والتنقل", callback_data="ad_cms_b_shop")
    )
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="ad_texts_main")
    )
    bot.edit_message_text(
        "🎛 <b>تخصيص أزرار البوت:</b>\nاختر القسم:", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cms_b_"))
def ad_cms_btns_list(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
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
        markup.add(
            InlineKeyboardButton(f"✏️ {text}", callback_data=f"edit_btn_{key}")
        )
        
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="ad_cms_btns_cats")
    )
    
    bot.edit_message_text(
        "👇 <b>اختر الزر الذي تريد تغيير اسمه أو الإيموجي الخاص به:</b>", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_txt_"))
def ad_edit_txt_prompt(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    key = call.data.replace("edit_txt_", "")
    current_val = db.custom_texts.find_one({'lang': 'ar', 'key': key})
    
    if current_val:
        current_text = current_val['value']
    else:
        current_text = LANG['ar'].get(key, "")
    
    hint = ""
    if key == "log_buy":
        hint = "\n\n💡 <b>المتغيرات المتاحة:</b>\n{user} = العميل\n{product} = المنتج\n{qty} = الكمية"
    elif key == "log_dep":
        hint = "\n\n💡 <b>المتغيرات المتاحة:</b>\n{user} = العميل\n{amount} = المبلغ\n{method} = طريقة الدفع"
        
    msg = bot.send_message(
        call.message.chat.id, 
        f"النص الحالي:\n\n<code>{html.escape(current_text)}</code>{hint}\n\n👇 <b>أرسل التعديل الآن بالرموز (سيتم ترجمته تلقائياً):</b>", 
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_save_custom_text, key)

def ad_save_custom_text(message, key):
    if not message.text or message.text.strip() == "الغاء": 
        bot.send_message(message.chat.id, "❌ تم الإلغاء.")
        return
        
    bot.send_message(message.chat.id, "⏳ جاري الحفظ والترجمة...")
    
    final_ar = extract_custom_emojis_to_html(message)
    final_en = safe_translate_for_cms(final_ar, 'en')
    
    db.custom_texts.update_one({'lang': 'ar', 'key': key}, {'$set': {'value': final_ar}}, upsert=True)
    db.custom_texts.update_one({'lang': 'en', 'key': key}, {'$set': {'value': final_en}}, upsert=True)
    
    if len(final_ar) > 100:
        preview = final_ar[:100] + "..."
    else:
        preview = final_ar
        
    bot.send_message(
        message.chat.id, 
        f"✅ <b>تم الحفظ!</b>\nالعربية:\n<code>{html.escape(preview)}</code>", 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_btn_"))
def ad_edit_btn_prompt(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    key = call.data.replace("edit_btn_", "")
    current_text, _ = get_btn_data(call.from_user.id, key)
    
    msg = bot.send_message(
        call.message.chat.id, 
        f"الزر الحالي: <code>{html.escape(current_text)}</code>\n\n👇 <b>أرسل الاسم الجديد مع الرمز المميز:</b>", 
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_save_custom_btn, key)

def ad_save_custom_btn(message, key):
    if not message.text or message.text.strip() == "الغاء": 
        bot.send_message(message.chat.id, "❌ تم الإلغاء.")
        return
        
    bot.send_message(message.chat.id, "⏳ جاري الحفظ...")
    
    text_ar, emoji_id = parse_button_input(message)
    
    if not text_ar or not text_ar.strip(): 
        text_ar = clean_old_emojis(get_btn_data(message.from_user.id, key)[0])
        
    text_en = safe_translate_for_cms(text_ar, 'en')
    
    db.custom_buttons.update_one({'lang': 'ar', 'key': key}, {'$set': {'text': text_ar, 'emoji_id': emoji_id}}, upsert=True)
    db.custom_buttons.update_one({'lang': 'en', 'key': key}, {'$set': {'text': text_en, 'emoji_id': emoji_id}}, upsert=True)
    
    bot.send_message(
        message.chat.id, 
        f"✅ <b>تم الحفظ!</b>\n{text_ar}", 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data == "ad_prod_emoji_start")
def ad_prod_emoji_start(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    prods = list(db.products.find())
    markup = InlineKeyboardMarkup(row_width=1)
    
    for p in prods: 
        if p.get('name_ar'):
            name = p.get('name_ar')
        else:
            name = p.get('name_en')
            
        markup.add(
            InlineKeyboardButton(
                f"📦 {clean_html(name)}", 
                callback_data=f"set_pemj_{p.get('id', str(p.get('_id', '')))}"
            )
        )
        
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main")
    )
    
    bot.edit_message_text(
        "👇 <b>اختر المنتج لتعيين أيقونة له:</b>", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("set_pemj_"))
def ad_prod_emoji_ask(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("set_pemj_", "")
    
    msg = bot.send_message(
        call.message.chat.id, 
        "🌟 <b>أرسل الآن الإيموجي المميز (Premium Emoji) لهذا المنتج:</b>", 
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_prod_emoji_save, pid)

def ad_prod_emoji_save(message, pid):
    if not message.text: 
        bot.send_message(message.chat.id, "❌ خطأ.")
        return
        
    emoji_id = None
    if message.entities:
        for ent in message.entities:
            if ent.type == 'custom_emoji':
                emoji_id = ent.custom_emoji_id
                break
                
    if not emoji_id: 
        bot.send_message(
            message.chat.id, 
            "❌ <b>لم يتم العثور على رمز Premium.</b>", 
            parse_mode="HTML"
        )
        return
        
    p = find_product(pid)
    if p:
        db.products.update_one({'_id': p['_id']}, {'$set': {'custom_emoji_id': emoji_id}})
        bot.send_message(
            message.chat.id, 
            f"✅ <b>تم تعيين الأيقونة بنجاح للمنتج.</b>", 
            parse_mode="HTML"
        )

@bot.callback_query_handler(func=lambda call: call.data == "ad_api_main")
def admin_api_main(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    
    gh_price = float(get_setting("github_price", 15.0))
    gem_price = float(get_setting("gemini_price", 5.0))
    
    markup = InlineKeyboardMarkup(row_width=1)
    
    markup.add(
        InlineKeyboardButton("💳 فحص رصيد API", callback_data="ad_gh_credits")
    )
    markup.add(
        InlineKeyboardButton(f"💰 تعديل سعر GitHub (${gh_price:.2f})", callback_data="ad_gh_price")
    )
    markup.add(
        InlineKeyboardButton(f"💰 تعديل سعر Gemini (${gem_price:.2f})", callback_data="ad_gem_price")
    )
    markup.add(
        InlineKeyboardButton("⚙️ إعداد Session", callback_data="ad_set_session")
    )
    markup.add(
        InlineKeyboardButton("🤖 إعداد المزود", callback_data="ad_set_provider")
    )
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main")
    )
    
    bot.edit_message_text(
        "⚙️ <b>إعدادات التفعيلات:</b>", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data in ["ad_set_session", "ad_set_provider"])
def admin_set_userbot_vars(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    
    if call.data == "ad_set_session":
        key = 'userbot_session'
    else:
        key = 'provider_bot'
        
    msg = bot.send_message(
        call.message.chat.id, 
        f"📝 <b>أرسل القيمة الجديدة لـ ({key}):</b>", 
        parse_mode="HTML"
    )
    
    def on_set(m):
        if db.settings.update_one({'key': key}, {'$set': {'value': m.text.strip()}}, upsert=True):
            bot.send_message(m.chat.id, "✅ تم حفظ الإعداد.")
            
    bot.register_next_step_handler(msg, on_set)

@bot.callback_query_handler(func=lambda call: call.data == "ad_gh_credits")
def admin_github_credits(call):
    try: bot.answer_callback_query(call.id, "⏳ جاري الاتصال...")
    except Exception: pass
    try:
        if not GITHUB_API_KEY:
            bot.send_message(call.message.chat.id, "❌ لم يتم العثور على GITHUB_API_KEY في ملف .env")
            return
            
        res = requests.get(
            f"{GITHUB_BASE_URL}/api/me", 
            headers={"X-API-Key": GITHUB_API_KEY, "User-Agent": "Mozilla/5.0"}, 
            timeout=15
        )
        
        if res.status_code == 200: 
            bot.send_message(
                call.message.chat.id, 
                f"💳 <b>رصيدك: {res.json().get('credits')}</b>", 
                parse_mode="HTML"
            )
    except Exception: 
        bot.send_message(
            call.message.chat.id, 
            f"❌ <b>فشل الاتصال.</b>", 
            parse_mode="HTML"
        )

@bot.callback_query_handler(func=lambda call: call.data in ["ad_gh_price", "ad_gem_price"])
def admin_set_price(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    
    if call.data == "ad_gh_price":
        key = 'github_price'
    else:
        key = 'gemini_price'
        
    msg = bot.send_message(
        call.message.chat.id, 
        "💰 <b>أرسل السعر الجديد بالدولار:</b>", 
        parse_mode="HTML"
    )
    
    def set_price(m):
        if db.settings.update_one({'key': key}, {'$set': {'value': float(m.text.strip())}}, upsert=True):
            bot.send_message(m.chat.id, "✅ تم التحديث.")
            
    bot.register_next_step_handler(msg, set_price)

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_add")
def ad_p_step1(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    msg = bot.send_message(
        call.from_user.id, 
        "📦 أرسل اسم المنتج (يمكنك وضع Premium Emoji):", 
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_p_step2)

def ad_p_step2(message):
    uid = message.from_user.id
    n_ar = extract_custom_emojis_to_html(message)
    n_en = safe_translate_for_cms(n_ar, 'en')
    temp_product[uid] = {'n_ar': n_ar, 'n_en': n_en}
    
    msg = bot.send_message(
        uid, 
        f"📝 أرسل وصف المنتج (يمكنك وضع Premium Emoji):", 
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_p_step3)

def ad_p_step3(message):
    uid = message.from_user.id
    d_ar = extract_custom_emojis_to_html(message)
    d_en = safe_translate_for_cms(d_ar, 'en')
    temp_product[uid].update({'d_ar': d_ar, 'd_en': d_en})
    
    msg = bot.send_message(uid, "💰 أرسل السعر بالدولار ($):")
    bot.register_next_step_handler(msg, ad_p_price)

def ad_p_price(message):
    uid = message.from_user.id
    try:
        temp_product[uid]['price'] = float(message.text)
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("⚡ تلقائي (أكواد)", callback_data="ad_ptype_auto"), 
            InlineKeyboardButton("🤝 يدوي", callback_data="ad_ptype_manual")
        )
        bot.send_message(
            uid, 
            "⚙️ <b>اختر نوع التسليم:</b>", 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        bot.send_message(uid, "❌ خطأ في السعر.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_ptype_"))
def ad_p_final(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    uid = call.from_user.id
    p = temp_product.get(uid)
    if not p: 
        return
        
    if call.data == "ad_ptype_manual":
        is_manual = True
    else:
        is_manual = False
        
    db.products.insert_one({
        'id': str(int(time.time())), 
        'name_ar': p['n_ar'], 
        'name_en': p['n_en'], 
        'desc_ar': p['d_ar'], 
        'desc_en': p['d_en'], 
        'price': p['price'], 
        'is_manual': is_manual, 
        'is_hidden': False
    })
    
    bot.edit_message_text(
        f"✅ <b>تم إضافة المنتج بنجاح!</b>", 
        call.message.chat.id, 
        call.message.message_id, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_edit")
def admin_edit_list(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=1)
    
    for p in list(db.products.find()): 
        p_name = p.get('name_ar')
        p_id = p.get('id', str(p.get('_id', '')))
        markup.add(
            InlineKeyboardButton(f"📝 {clean_html(p_name)}", callback_data=f"edit_p_{p_id}")
        )
        
    markup.add(
        InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main")
    )
    
    bot.edit_message_text(
        "👇 Select Product:", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_p_"))
def admin_edit_opts(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("edit_p_", "")
    p = find_product(pid)
    
    if not p: 
        return
        
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("💵 Price", callback_data=f"ep_price_{pid}")
    )
    markup.add(
        InlineKeyboardButton("📝 Desc (AR)", callback_data=f"ep_dar_{pid}"), 
        InlineKeyboardButton("📝 Desc (EN)", callback_data=f"ep_den_{pid}")
    )
    markup.add(
        InlineKeyboardButton("✏️ Name (AR)", callback_data=f"ep_nar_{pid}"), 
        InlineKeyboardButton("✏️ Name (EN)", callback_data=f"ep_nen_{pid}")
    )
    
    if p.get('is_hidden'):
        hide_text = "👁️ Show"
    else:
        hide_text = "🙈 Hide"
        
    markup.add(
        InlineKeyboardButton(hide_text, callback_data=f"toggle_hide_{pid}"), 
        InlineKeyboardButton("🔙 Back", callback_data="ad_p_edit")
    )
                  
    try: 
        bot.edit_message_text(
            "⚙️ Options:", 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("toggle_hide_"))
def admin_toggle_hide(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("toggle_hide_", "")
    p = find_product(pid)
    if p:
        new_status = not p.get('is_hidden', False)
        db.products.update_one({'_id': p['_id']}, {'$set': {'is_hidden': new_status}})
        admin_edit_opts(call)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ep_"))
def admin_edit_prompt(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    parts = call.data.split('_', 2)
    
    msg = bot.send_message(
        call.message.chat.id, 
        "✏️ أرسل القيمة الجديدة (يمكنك استخدام Premium Emojis):", 
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, admin_save_edit, parts[1], parts[2])

def admin_save_edit(message, field, pid):
    p = find_product(pid)
    if not p: 
        return
        
    if field == "price":
        try:
            new_price = float(message.text)
            db.products.update_one({'_id': p['_id']}, {'$set': {'price': new_price}})
            bot.send_message(message.chat.id, "✅ تم التحديث.", parse_mode="HTML")
        except Exception: 
            bot.send_message(message.chat.id, "❌ خطأ في السعر.", parse_mode="HTML")
    else:
        val_ar = extract_custom_emojis_to_html(message)
        
        if field in ['dar', 'nar']:
            val_en = safe_translate_for_cms(val_ar, 'en')
            if field == 'dar':
                db.products.update_one({'_id': p['_id']}, {'$set': {"desc_ar": val_ar, "desc_en": val_en}})
            else:
                db.products.update_one({'_id': p['_id']}, {'$set': {"name_ar": val_ar, "name_en": val_en}})
        else:
            if field == 'den':
                db.products.update_one({'_id': p['_id']}, {'$set': {"desc_en": val_ar}})
            else:
                db.products.update_one({'_id': p['_id']}, {'$set': {"name_en": val_ar}})
                
        bot.send_message(message.chat.id, "✅ تم التحديث بنجاح.", parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_del")
def admin_del_list(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=1)
    
    for p in list(db.products.find()): 
        p_name = p.get('name_ar')
        p_id = p.get('id', str(p.get('_id', '')))
        markup.add(
            InlineKeyboardButton(f"🗑 {clean_html(p_name)}", callback_data=f"del_p_{p_id}")
        )
        
    markup.add(
        InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main")
    )
    
    bot.edit_message_text(
        "👇 Select Product to Delete:", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_p_"))
def admin_del_exec(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("del_p_", "")
    p = find_product(pid)
    
    if p:
        db.product_stock.delete_many({'product_id': {'$in': [str(pid), int(pid)]}})
        db.orders.delete_many({'product_id': str(pid)})
        db.products.delete_one({'_id': p['_id']})
        
        try: bot.answer_callback_query(call.id, "✅ Deleted Successfully!", show_alert=True)
        except Exception: pass
        admin_main_ui(call)

@bot.callback_query_handler(func=lambda call: call.data == "ad_s_list")
def admin_stock_list_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=1)
    
    for p in list(db.products.find({'is_manual': {'$ne': True}})): 
        p_name = p.get('name_ar')
        p_id = p.get('id', str(p.get('_id', '')))
        stk = get_product_stock_count(p_id)
        
        markup.add(
            InlineKeyboardButton(f"📦 {clean_html(p_name)} ({stk})", callback_data=f"ad_s_opts_{p_id}")
        )
        
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main")
    )
    
    bot.edit_message_text(
        "📦 <b>اختر المنتج لإدارة الستوك الخاص به:</b>", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_s_opts_"))
def admin_stock_opts_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("ad_s_opts_", "")
    p = find_product(pid)
    if not p: return
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("➕ إضافة أكواد", callback_data=f"stk_add_{pid}")
    )
    markup.add(
        InlineKeyboardButton("👁️ عرض الأكواد (ملف txt)", callback_data=f"stk_view_{pid}")
    )
    markup.add(
        InlineKeyboardButton("✏️ تعديل كود", callback_data=f"stk_edit_{pid}"), 
        InlineKeyboardButton("🗑️ حذف كود", callback_data=f"stk_delcode_{pid}")
    )
    markup.add(
        InlineKeyboardButton("🧨 مسح كل الستوك", callback_data=f"stk_clear_{pid}")
    )
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="ad_s_list")
    )
    
    bot.edit_message_text(
        f"⚙️ <b>إدارة ستوك:</b> {clean_html(p.get('name_ar'))}\n📊 <b>المتوفر:</b> {get_product_stock_count(pid)}", 
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_add_"))
def admin_stock_input(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("stk_add_", "")
    
    msg = bot.send_message(
        call.from_user.id, 
        "📥 <b>أرسل الأكواد أو ارفع ملف .txt:</b>", 
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, admin_stock_save, pid)

def admin_stock_save(message, pid):
    lines = []
    if message.document:
        try:
            file_info = bot.get_file(message.document.file_id)
            lines = bot.download_file(file_info.file_path).decode('utf-8').split('\n')
        except Exception: 
            bot.send_message(message.chat.id, f"❌ خطأ.")
            return
    elif message.text: 
        lines = message.text.split('\n')
    else: 
        bot.send_message(message.chat.id, "❌ الرجاء إرسال نص أو ملف.")
        return

    count = 0
    for l in lines:
        if l.strip():
            db.product_stock.insert_one({'product_id': str(pid), 'code_line': l.strip(), 'is_sold': False})
            count += 1
            
    bot.send_message(message.chat.id, f"✅ <b>تم إضافة {count} كود!</b>", parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_view_"))
def admin_stock_view(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("stk_view_", "")
    
    queries = [{'product_id': str(pid)}]
    if str(pid).isdigit():
        queries.append({'product_id': int(pid)})
        
    items = list(db.product_stock.find({'$or': queries, 'is_sold': False}))
    
    if not items: 
        bot.send_message(call.message.chat.id, "📭 الستوك فارغ!")
        return
        
    content_str = "\n".join([item['code_line'] for item in items])
    bot.send_document(
        call.message.chat.id, 
        io.BytesIO(content_str.encode('utf-8')), 
        visible_file_name=f"Stock_{pid}.txt", 
        caption=f"📦 الأكواد المتوفرة: {len(items)}"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_delcode_"))
def admin_stock_delcode_prompt(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("stk_delcode_", "")
    
    msg = bot.send_message(
        call.message.chat.id, 
        "🗑️ <b>أرسل الكود للحذف:</b>", 
        parse_mode="HTML"
    )
    
    def remove_code(m):
        queries = [{'product_id': str(pid)}]
        if str(pid).isdigit():
            queries.append({'product_id': int(pid)})
            
        result = db.product_stock.delete_one({'$or': queries, 'code_line': m.text.strip(), 'is_sold': False})
        
        if result.deleted_count > 0:
            bot.send_message(m.chat.id, "✅ تم الحذف")
        else:
            bot.send_message(m.chat.id, "❌ لم يتم العثور على الكود.")
            
    bot.register_next_step_handler(msg, remove_code)

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_clear_"))
def admin_stock_clear_exec(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("stk_clear_", "")
    
    queries = [{'product_id': str(pid)}]
    if str(pid).isdigit():
        queries.append({'product_id': int(pid)})
        
    res = db.product_stock.delete_many({'$or': queries, 'is_sold': False})
    bot.send_message(call.message.chat.id, f"🧨 تم مسح {res.deleted_count} كود!")
    
    call.data = f"ad_s_opts_{pid}"
    admin_stock_opts_ui(call)

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_edit_"))
def admin_stock_edit_step1(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    pid = call.data.replace("stk_edit_", "")
    
    msg = bot.send_message(call.message.chat.id, "✏️ <b>أرسل الكود القديم:</b>", parse_mode="HTML")
    
    def step2(message, pid):
        queries = [{'product_id': str(pid)}]
        if str(pid).isdigit():
            queries.append({'product_id': int(pid)})
            
        item = db.product_stock.find_one({'$or': queries, 'code_line': message.text.strip(), 'is_sold': False})
        
        if not item: 
            bot.send_message(message.chat.id, "❌ <b>لم يتم العثور!</b>", parse_mode="HTML")
            return
            
        msg2 = bot.send_message(message.chat.id, "✅ تم العثور. أرسل الجديد:", parse_mode="HTML")
        
        def save_new(m):
            if db.product_stock.update_one({'_id': item['_id']}, {'$set': {'code_line': m.text.strip()}}):
                bot.send_message(m.chat.id, "✅ <b>تم التعديل!</b>", parse_mode="HTML")
            else:
                bot.send_message(m.chat.id, "❌ خطأ", parse_mode="HTML")
                 
        bot.register_next_step_handler(msg2, save_new)
        
    bot.register_next_step_handler(msg, step2, pid)

@bot.callback_query_handler(func=lambda call: call.data == "ad_ban_user")
def ad_ban_start(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    msg = bot.send_message(
        call.message.chat.id, 
        "🚫 <b>أرسل ID أو معرف المستخدم للحظر/فك الحظر:</b>", 
        parse_mode="HTML"
    )
    
    def ex(m):
        if m.text.strip().startswith('@'):
            u = db.users.find_one({'username': m.text.strip().replace('@', '').lower()})
        else:
            try:
                u = get_user_data_full(int(m.text.strip()))
            except Exception:
                u = None
            
        if u:
            if u['user_id'] == OWNER_ID: 
                bot.send_message(m.chat.id, "❌ لا يمكن حظر المالك!")
                return
                
            n = 1 if u.get('is_banned', 0) == 0 else 0
            db.users.update_one({'user_id': u['user_id']}, {'$set': {'is_banned': n}})
            
            status_text = 'تم حظر 🚫' if n==1 else 'فك حظر ✅'
            bot.send_message(m.chat.id, f"✅ {status_text} {u['user_id']}", parse_mode="HTML")
        else: 
            bot.send_message(m.chat.id, "❌ لم يتم العثور.")
            
    bot.register_next_step_handler(msg, ex)

@bot.callback_query_handler(func=lambda call: call.data == "ad_users_main")
def ad_users_main_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("🔍 بحث عن مستخدم", callback_data="ad_u_search"), 
        InlineKeyboardButton("🏆 أعلى 10 أرصدة", callback_data="ad_u_top")
    )
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main")
    )
    
    try: 
        bot.edit_message_text(
            "👥 <b>إدارة العملاء والأرصدة:</b>", 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_u_top")
def ad_u_top_ui(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=1)
    
    for tu in list(db.users.find().sort('balance', -1).limit(10)): 
        if tu.get('username'):
            u_name = f"@{tu['username']}"
        else:
            u_name = tu.get('name', 'User')
            
        btn_text = f"💰 ${tu.get('balance', 0):.2f} | 👤 {clean_name(u_name)[:15]}"
        markup.add(
            InlineKeyboardButton(btn_text, callback_data=f"ad_u_det_{tu['user_id']}")
        )
        
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="ad_users_main")
    )
    
    try: 
        bot.edit_message_text(
            "🏆 <b>أعلى 10 مستخدمين:</b>", 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_u_search")
def ad_u_search_prompt(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    msg = bot.send_message(call.message.chat.id, "🔍 <b>أرسل الأيدي أو المعرف:</b>", parse_mode="HTML")
    
    def handle_search(m):
        if m.text.strip().startswith('@'):
            res = db.users.find_one({'username': m.text.strip().replace('@', '').lower()})
            if res:
                show_user_admin_profile(m.chat.id, res['user_id'])
            else:
                bot.send_message(m.chat.id, "❌ لم يتم العثور.")
        else:
            try:
                show_user_admin_profile(m.chat.id, int(m.text.strip()))
            except Exception:
                bot.send_message(m.chat.id, "❌ الأيدي غير صحيح.")
            
    bot.register_next_step_handler(msg, handle_search)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_u_det_"))
def ad_u_det_router(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    target_uid = int(call.data.replace("ad_u_det_", ""))
    show_user_admin_profile(call.message.chat.id, target_uid, call.message.message_id)

def show_user_admin_profile(chat_id, target_uid, message_id=None):
    u = get_user_data_full(target_uid)
    if not u: 
        bot.send_message(chat_id, "❌ لم يتم العثور.")
        return
        
    buy_count = db.orders.count_documents({'user_id': target_uid})
    dep_total = sum([float(d.get('amount', 0)) for d in list(db.used_transactions.find({'user_id': target_uid}))])
    
    username_field = f"@{u['username']}" if u.get('username') else 'لا يوجد'
    status_field = 'محظور 🚫' if u.get('is_banned') == 1 else 'نشط ✅'
    
    text = f"📂 <b>ملف العميل</b>\n\n👤 الاسم: <b>{clean_name(u.get('name', 'بدون'))}</b>\n🔗 المعرف: {username_field}\n🆔 الأيدي: <code>{target_uid}</code>\n🛡️ الحالة: <b>{status_field}</b>\n\n💰 الرصيد الحالي: <b>${u.get('balance', 0):.2f}</b>\n✅ المشتريات: <b>{buy_count}</b>\n📦 إجمالي الإيداعات: <b>${dep_total:.2f}</b>"
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("🛍 المشتريات", callback_data=f"ad_uh_buy_{target_uid}"), 
        InlineKeyboardButton("💳 الإيداعات", callback_data=f"ad_uh_dep_{target_uid}")
    )
    markup.add(
        InlineKeyboardButton("📄 تحميل سجل (ملف)", callback_data=f"ad_dlbuy_{target_uid}")
    )
    markup.add(
        InlineKeyboardButton("💰 تعديل رصيده", callback_data=f"ad_ugift_{target_uid}")
    )
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="ad_users_main")
    )
    
    try:
        if message_id: 
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="HTML")
        else: 
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_dlbuy_"))
def admin_download_buy_hist(call):
    try: bot.answer_callback_query(call.id, "⏳ جاري التجهيز...")
    except Exception: pass
    target_uid = int(call.data.replace("ad_dlbuy_", ""))
    recs = list(db.orders.find({'user_id': target_uid}).sort('_id', -1))
    
    if not recs: 
        bot.send_message(call.message.chat.id, "📭 لا يوجد مشتريات.")
        return
        
    content = f"=== سجل العميل {target_uid} ===\n\n"
    for i, r in enumerate(recs, 1):
        p = find_product(r.get('product_id'))
        if r.get('product_id') in ['GitHub_Student', 'Gemini_Activation']:
            n = r.get('product_id').replace('_', ' ')
        else:
            if p:
                n = clean_html(p.get('name_ar', p.get('name_en')))
            else:
                n = "Unknown Product"
                
        content += f"{i}. {r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')} - {n} - {r.get('code_delivered', '')}\n"
        
    bot.send_document(
        call.message.chat.id, 
        io.BytesIO(content.encode('utf-8')), 
        visible_file_name=f"Purchases_{target_uid}.txt"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_uh_"))
def show_admin_hist_detail(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    parts = call.data.split('_', 3)
    mode = parts[2]
    target_uid = int(parts[3])
    out = f"📂 <b>سجلات العميل (<code>{target_uid}</code>) - أحدث 5:</b>\n\n"
    
    try:
        if mode == "buy":
            recs = list(db.orders.find({'user_id': target_uid}).sort('_id', -1).limit(5))
            if not recs: 
                out += "📭 لا يوجد مشتريات."
            for r in recs:
                d = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
                p = find_product(r['product_id'])
                if r.get('product_id') in ['GitHub_Student', 'Gemini_Activation']:
                    n = r.get('product_id').replace('_', ' ')
                else:
                    n = clean_html(p['name_ar'] if p else "Product")
                out += f"🛍 <b>{n}</b>\n📅 <code>{d}</code>\n🔑 <code>{r.get('code_delivered', '')}</code>\n---\n"
        else:
            recs = list(db.used_transactions.find({'user_id': target_uid}).sort('_id', -1).limit(5))
            if not recs: 
                out += "📭 لا يوجد إيداعات."
            for r in recs: 
                 d = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
                 out += f"💰 <b>${r.get('amount', 0):.2f}</b> | 📅 <code>{d}</code>\n🆔 <code>{r.get('transaction_id', '')}</code>\n"
    except Exception: 
        out = f"❌ Error"
        
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"ad_u_det_{target_uid}"))
    
    try: 
        bot.edit_message_text(
            out, 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_ugift_"))
def ad_ugift_prompt(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    target_uid = call.data.replace("ad_ugift_", "")
    
    msg = bot.send_message(call.message.chat.id, "💰 <b>أرسل المبلغ:</b>", parse_mode="HTML")
    
    def apply_gift(m):
        try:
            val = float(m.text)
            if db.users.update_one({'user_id': int(target_uid)}, {'$inc': {'balance': val}}):
                bot.send_message(m.chat.id, "✅ تم التعديل.")
            else:
                bot.send_message(m.chat.id, "❌ خطأ")
        except Exception:
            bot.send_message(m.chat.id, "❌ خطأ")
             
    bot.register_next_step_handler(msg, apply_gift)

@bot.callback_query_handler(func=lambda call: call.data == "ad_gift")
def ad_gift_start(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    msg = bot.send_message(call.from_user.id, "👤 <b>أرسل الأيدي أو المعرف:</b>", parse_mode="HTML")
    
    def step1(m):
        if m.text.strip().startswith('@'):
            u = db.users.find_one({'username': m.text.strip().replace('@', '').lower()})
            if not u:
                bot.send_message(m.chat.id, "❌ Not found")
                return
            uid_val = u['user_id']
        else:
            try:
                uid_val = int(m.text.strip())
            except Exception:
                bot.send_message(m.chat.id, "❌ Error")
                return
                
        msg2 = bot.send_message(m.chat.id, "💰 أرسل المبلغ:")
        
        def step2(m2):
            try:
                val = float(m2.text)
                if db.users.update_one({'user_id': uid_val}, {'$inc': {'balance': val}}):
                    bot.send_message(m2.chat.id, "✅ تم.")
                else:
                    bot.send_message(m2.chat.id, "❌ Error")
            except Exception:
                bot.send_message(m2.chat.id, "❌ Error")
                 
        bot.register_next_step_handler(msg2, step2)
        
    bot.register_next_step_handler(msg, step1)

@bot.callback_query_handler(func=lambda call: call.data == "ad_fsub_list")
def admin_fsub_list(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=1)
    
    for c in list(db.required_channels.find()): 
        markup.add(
            InlineKeyboardButton(f"❌ حذف {c['channel_id']}", callback_data=f"del_fsub_{c['channel_id']}")
        )
         
    markup.add(
        InlineKeyboardButton("➕ إضافة قناة", callback_data="ad_fsub_add"), 
        InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main")
    )
    
    try: 
        bot.edit_message_text("📢 <b>إدارة قنوات الاشتراك:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_fsub_add")
def admin_fsub_add(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    msg = bot.send_message(call.message.chat.id, "أرسل يوزر القناة:", parse_mode="HTML")
    
    def save_f(m):
        cid = m.text.strip()
        try: 
            bot.get_chat_member(cid, bot.get_me().id)
            db.required_channels.insert_one({'channel_id': cid})
            bot.send_message(m.chat.id, f"✅ تم.")
        except Exception: 
            bot.send_message(m.chat.id, f"❌ البوت ليس أدمن أو اليوزر خطأ.")
            
    bot.register_next_step_handler(msg, save_f)

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_fsub_"))
def del_fsub_btn(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    db.required_channels.delete_one({'channel_id': call.data.replace("del_fsub_", "")})
    try: bot.answer_callback_query(call.id, "✅ تم الحذف", show_alert=True)
    except Exception: pass
    admin_fsub_list(call)

@bot.callback_query_handler(func=lambda call: call.data == "ad_new_admin")
def admin_add_admin_start(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    msg = bot.send_message(call.from_user.id, "👑 <b>أرسل الأيدي للترقية:</b>", parse_mode="HTML")
    
    def exec_promote(m):
        if m.text.strip().startswith('@'):
            u = db.users.find_one({'username': m.text.strip().replace('@', '').lower()})
            if not u:
                bot.send_message(m.chat.id, "❌ لم يتم العثور.")
                return
            uid_val = u['user_id']
        else:
            try:
                uid_val = int(m.text.strip())
            except Exception:
                bot.send_message(m.chat.id, "❌ Error")
                return
                
        if db.users.update_one({'user_id': uid_val}, {'$set': {'is_admin': 1}}):
            bot.send_message(m.chat.id, "✅ تمت الترقية.")
        else:
            bot.send_message(m.chat.id, "❌ لم يتم العثور.")
            
    bot.register_next_step_handler(msg, exec_promote)

@bot.callback_query_handler(func=lambda call: call.data == "ad_logs_all")
def admin_all_logs(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    recs = list(db.used_transactions.find().sort('_id', -1).limit(10))
    txt = "📜 <b>آخر 10 إيداعات:</b>\n\n"
    
    for r in recs: 
        txt += f"👤 <code>{r.get('user_id')}</code> | 💰 <b>${r.get('amount')}</b> | 🆔 <code>{r.get('transaction_id')}</code>\n"
        
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    
    try: 
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_bc")
def admin_bc_init(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    msg = bot.send_message(call.from_user.id, "📢 أرسل رسالة البرودكاست (بكل الرموز):", parse_mode="HTML")
    
    def ex(m):
        for u in list(db.users.find()):
            try: 
                bot.copy_message(u['user_id'], m.chat.id, m.message_id)
                time.sleep(0.05)
            except Exception: 
                pass
        bot.send_message(m.chat.id, "✅ تم الإرسال.")
        
    bot.register_next_step_handler(msg, ex)

@bot.callback_query_handler(func=lambda call: call.data == "ad_shop_settings")
def admin_shop_settings(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    markup = InlineKeyboardMarkup(row_width=2)
    
    markup.add(
        InlineKeyboardButton("💳 Binance Pay", callback_data="set_v_wallet")
    )
    markup.add(
        InlineKeyboardButton("🟢 USDT TRC20", callback_data="set_v_usdt"), 
        InlineKeyboardButton("🟡 USDT BEP20", callback_data="set_v_usdt_bep20")
    )
    markup.add(
        InlineKeyboardButton("💎 TON Address", callback_data="set_v_ton"), 
        InlineKeyboardButton("🔵 LTC Address", callback_data="set_v_ltc")
    )
    markup.add(
        InlineKeyboardButton("📢 قناة الإثباتات", callback_data="set_v_log")
    )
    markup.add(
        InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main")
    )
    
    try: 
        bot.edit_message_text(
            "⚙️ <b>الإعدادات:</b>", 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("set_v_"))
def admin_set_inputs(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    msg = bot.send_message(call.from_user.id, "أرسل القيمة الجديدة:")
    
    def save_setting_value(m):
        key_map = {
            "set_v_log": "log_channel", 
            "set_v_usdt": "usdt_address", 
            "set_v_ltc": "ltc_address", 
            "set_v_wallet": "wallet_address", 
            "set_v_usdt_bep20": "usdt_bep20_address", 
            "set_v_ton": "ton_address"
        }
        
        if db.settings.update_one({'key': key_map[call.data]}, {'$set': {'value': m.text.strip()}}, upsert=True):
            bot.send_message(m.chat.id, "✅ تم.")
            
    bot.register_next_step_handler(msg, save_setting_value)

# ============================================================
# 🚀 15. التشغيل
# ============================================================
def run_bot():
    try: 
        bot.delete_webhook(drop_pending_updates=True)
        time.sleep(1)
    except Exception: 
        pass
        
    while True:
        try: 
            bot.polling(non_stop=True, skip_pending=True)
        except Exception as e: 
            logger.error(f"Polling Error Critical: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_bot()
