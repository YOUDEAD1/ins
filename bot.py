import sys
import os
import time
import datetime
import re
import logging
import requests
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    import telebot
    from telebot import types
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
except AttributeError:
    print("❌ خطأ: تأكد من حذف أي ملف اسمه telebot.py في مجلدك.")
    sys.exit(1)

from binance.client import Client
from deep_translator import GoogleTranslator
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# 🔑 1. الإعدادات (مع تنظيف المسافات والأسطر المخفية) 🧹
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

# ============================================================
# 🌐 2. السيرفر الوهمي (Render Keep-Alive)
# ============================================================
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write("Bot is alive! 🚀".encode('utf-8'))
    def log_message(self, format, *args):
        return

def keep_alive():
    port = int(os.environ.get('PORT', 8080))
    HTTPServer(('0.0.0.0', port), DummyHandler).serve_forever()

threading.Thread(target=keep_alive, daemon=True).start()

# ============================================================
# 🚀 3. تهيئة البوت وقواعد البيانات
# ============================================================
bot = telebot.TeleBot(TOKEN)

print("⏳ جاري الاتصال بقاعدة البيانات MongoDB...")
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.server_info()
    db = mongo_client['shop_db']
    print("✅ تم الاتصال بقاعدة البيانات MongoDB بنجاح!")
except Exception as e:
    print(f"❌ خطأ في MongoDB: {e}")
    sys.exit(1)

# ✅ تهيئة أداة باينانس بشكل آمن 
binance_client = None
if BINANCE_API_KEY and BINANCE_API_SECRET:
    try:
        binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        print("✅ تم الاتصال بـ Binance بنجاح!")
    except Exception as e:
        print(f"⚠️ تحذير: خطأ في اتصال Binance (تأكد من منطقة السيرفر). السبب: {e}")
else:
    print("⚠️ تحذير: مفاتيح Binance غير موجودة في إعدادات Render!")

REFERRAL_REWARD = 0.10
temp_product = {}

# ============================================================
# 🌟 4. القاموس الكامل والمطور
# ============================================================
LANG = {
    'ar': {
        'welcome': "👋 <b>أهلاً بك في المتجر الاحترافي!</b>\n\n🆔 الأيدي: <code>{}</code>\n👤 الاسم: <b>{}</b>\n👥 المستخدمين: <b>{}</b>\n💰 الرصيد: <b>${:.2f}</b>",
        'profile_txt': "👤 <b>ملفك الشخصي</b>\n\n🆔 الأيدي: <code>{}</code>\n👤 الاسم: <b>{}</b>\n💰 الرصيد: <b>${:.2f}</b>\n✅ المشتريات: <b>{}</b>\n📦 إجمالي الشحن: <b>${:.2f}</b>",
        'invite_txt': "👥 <b>نظام الإحالات الذكي</b>\n\n🔗 <b>رابط الدعوة الخاص بك:</b>\n<code>https://t.me/{}?start={}</code>\n\n📊 <b>إحصائياتك:</b>\n👥 عدد المدعوين: <b>{}</b>\n💰 إجمالي أرباحك: <b>${:.2f}</b>\n\n🎁 <b>القوانين:</b> ستحصل على <b>$0.10</b> رصيد مجاني فور قيام صديقك بأول عملية شراء.",
        'dep_choose': "💳 <b>اختر طريقة الدفع المناسبة:</b>\n<i>جميع بواباتنا آمنة وتتم معالجتها تلقائياً ⚡️</i>",
        'dep_pay': "🟡 <b>Binance Pay</b>\n\nأرسل المبلغ إلى الـ ID التالي:\n🆔 Binance ID: <code>{}</code>\n\n⚠️ بعد التحويل، <b>أرسل رقم العملية (Order ID) كنص هنا.</b>",
        'dep_crypto': "🟢 <b>شحن عبر {}</b>\n\nأرسل المبلغ إلى المحفظة:\n<code>{}</code>\n\n⚠️ بعد التحويل، <b>أرسل الهاش (TxID) كنص هنا.</b>",
        'tx_used': "⚠️ عذراً، هذا الرقم مستخدم مسبقاً!",
        'crypto_checking': "⏳ <b>جاري فحص العملية بأمان... الرجاء الانتظار.</b>",
        'dep_success': "✅ <b>اكتمل الإيداع بنجاح!</b>\nتم إضافة <b>${:.2f}</b> إلى رصيدك. نشكر ثقتك بنا.",
        'dep_fail': "❌ <b>لم نجد العملية!</b> تأكد من صحة الرقم وأنه تم إرساله كنص.",
        'dep_pending': "⏳ <b>قيد المعالجة!</b> لم يتم تأكيد الحوالة في البلوكتشين بعد، يرجى المحاولة بعد قليل.",
        'history_title': "📜 <b>سجلاتك المالية:</b>",
        'products': "🛒 المنتجات", 'deposit': "💳 شحن الرصيد", 'profile': "👤 الملف الشخصي", 
        'invite': "👥 الإحالات", 'support': "👨‍💻 الدعم الفني", 'lang_btn': "🌐 English", 
        'back': "🔙 رجوع", 'main_menu': "🏠 القائمة الرئيسية", 'buy_hist': "🛍 المشتريات", 
        'dep_hist': "💳 الإيداعات", 'no_hist': "📭 لا يوجد سجلات حتى الآن.",
        'store_title': "🛒 <b>المنتجات المتوفرة:</b>", 'buy_now': "✅ شراء الآن",
        'buy_success': "✅ <b>تم الشراء بنجاح!</b>\n\nأكوادك جاهزة:\n{}\n\n<i>شكراً لاختيارك متجرنا 🛡️</i>",
        'no_balance': "❌ رصيدك غير كافٍ! يرجى شحن حسابك.", 'out_stock': "❌ نفد المخزون! يرجى الانتظار لحين التوفر.",
        'must_join': "🔒 <b>عذراً، يجب عليك الاشتراك في قنواتنا أولاً لتتمكن من استخدام البوت:</b>", 'check_sub': "🔄 تحقق من الاشتراك",
        'qty_prompt': "🔢 <b>أرسل الكمية التي تريد شراءها (أرقام فقط):</b>",
        'qty_invalid': "❌ <b>يرجى إرسال أرقام صحيحة أكبر من صفر!</b>",
        'qty_not_enough': "❌ <b>عذراً، المتوفر فقط {} قطعة!</b>",
        'crypto_error': "❌ <b>نظام الدفع معطل حالياً (يوجد خلل في ربط باينانس من الإدارة).</b>"
    },
    'en': {
        'welcome': "👋 <b>Welcome to the Pro Shop!</b>\n\n🆔 ID: <code>{}</code>\n👤 Name: <b>{}</b>\n👥 Users: <b>{}</b>\n💰 Balance: <b>${:.2f}</b>",
        'profile_txt': "👤 <b>Your Profile</b>\n\n🆔 ID: <code>{}</code>\n👤 Name: <b>{}</b>\n💰 Balance: <b>${:.2f}</b>\n✅ Purchases: <b>{}</b>\n📦 Total Deposited: <b>${:.2f}</b>",
        'invite_txt': "👥 <b>Smart Referrals</b>\n\n🔗 <b>Your Link:</b>\n<code>https://t.me/{}?start={}</code>\n\n📊 <b>Stats:</b>\n👥 Invited: <b>{}</b>\n💰 Earned: <b>${:.2f}</b>\n\n🎁 <b>Rule:</b> Earn <b>$0.10</b> free balance after your friend's first purchase.",
        'dep_choose': "💳 <b>Choose payment method:</b>\n<i>All gateways are 100% secure and automated ⚡️</i>",
        'dep_pay': "🟡 <b>Binance Pay</b>\n\nSend amount to ID:\n🆔 Binance ID: <code>{}</code>\n\n⚠️ Send <b>Order ID</b> here as text.",
        'dep_crypto': "🟢 <b>{} Deposit</b>\n\nSend to address:\n<code>{}</code>\n\n⚠️ Send <b>TxID (Hash)</b> here as text.",
        'tx_used': "⚠️ ID already used!",
        'crypto_checking': "⏳ <b>Verifying securely... Please wait.</b>",
        'dep_success': "✅ <b>Deposit Successful!</b>\n<b>${:.2f}</b> added to your balance. Thank you!",
        'dep_fail': "❌ <b>Not found!</b> Check ID and send text, not an image.",
        'dep_pending': "⏳ <b>Pending!</b> Not confirmed on blockchain yet. Try again shortly.",
        'history_title': "📜 <b>Your Financial Records:</b>",
        'products': "🛒 Products", 'deposit': "💳 Deposit", 'profile': "👤 Profile", 
        'invite': "👥 Referrals", 'support': "👨‍💻 Support", 'lang_btn': "🌐 العربية", 
        'back': "🔙 Back", 'main_menu': "🏠 Main Menu", 'buy_hist': "🛍 Purchases", 
        'dep_hist': "💳 Deposits", 'no_hist': "📭 No records yet.",
        'store_title': "🛒 <b>Available Products:</b>", 'buy_now': "✅ Buy Now",
        'buy_success': "✅ <b>Purchase Successful!</b>\n\nYour codes:\n{}\n\n<i>Thank you for choosing us 🛡️</i>",
        'no_balance': "❌ Low balance! Please deposit.", 'out_stock': "❌ Out of stock!",
        'must_join': "🔒 <b>You must join our channels first to use the bot:</b>", 'check_sub': "🔄 Verify Subscription",
        'qty_prompt': "🔢 <b>Enter the quantity you want to buy (numbers only):</b>",
        'qty_invalid': "❌ <b>Please send valid numbers > 0!</b>",
        'qty_not_enough': "❌ <b>Only {} pieces available!</b>",
        'crypto_error': "❌ <b>Payment system is currently disabled (Binance connection issue).</b>"
    }
}

# ============================================================
# 🛠️ 5. دوال مساعدة
# ============================================================
def clean_name(text):
    return re.sub(r'<[^>]+>', '', text).strip() if text else ""

def get_setting(key, default="Not Set"):
    res = db.settings.find_one({'key': key})
    return res['value'] if res else default

def get_user_data_full(uid):
    return db.users.find_one({'user_id': uid})

def get_lang(uid):
    u = get_user_data_full(uid)
    return u.get('lang', 'ar') if u else 'ar'

def get_product_stock_count(pid):
    return db.product_stock.count_documents({'product_id': str(pid), 'is_sold': False})

def check_forced_sub(uid):
    if uid == OWNER_ID: return True
    user_db = get_user_data_full(uid)
    if user_db and user_db.get('is_admin') == 1: return True
    
    chans = list(db.required_channels.find())
    if not chans: return True

    for c in chans:
        try:
            status = bot.get_chat_member(c['channel_id'], uid).status
            if status in ['left', 'kicked']: return False
        except Exception: 
            return False
    return True

# ============================================================
# 🏠 6. معالج البداية واللغة والاشتراك
# ============================================================
@bot.message_handler(commands=['start'])
def start_handler(message):
    chat_id = message.chat.id if not isinstance(message, types.CallbackQuery) else message.message.chat.id
    from_user = message.from_user
    uid = from_user.id
    uname = from_user.username.lower() if from_user.username else ""
    
    user = get_user_data_full(uid)
    if not user:
        full_text = message.text if not isinstance(message, types.CallbackQuery) else (message.message.text or "")
        args = full_text.split()
        ref = args[1] if len(args) > 1 and args[1].isdigit() else None
        db.users.insert_one({
            'user_id': uid, 'name': from_user.first_name, 'username': uname, 
            'referred_by': ref, 'balance': 0.0, 'lang_chosen': False, 'lang': 'ar', 'is_admin': 0
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
        bot.send_message(chat_id, "🌐 <b>الرجاء اختيار لغتك / Please choose your language:</b>", reply_markup=markup, parse_mode="HTML")
        return

    lang = user.get('lang', 'ar')
    
    if not check_forced_sub(uid):
        chans = list(db.required_channels.find())
        markup = InlineKeyboardMarkup(row_width=1)
        for c in chans: 
            btn_txt = "📢 Channel" if lang=='en' else "📢 القناة"
            markup.add(InlineKeyboardButton(btn_txt, url=f"https://t.me/{c['channel_id'].replace('@','') }"))
        markup.add(InlineKeyboardButton(LANG[lang]['check_sub'], callback_data="main_menu_refresh"))
        bot.send_message(chat_id, LANG[lang]['must_join'], reply_markup=markup, parse_mode="HTML")
        return

    users_total = db.users.count_documents({})
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton(LANG[lang]['products'], callback_data="open_shop"),
               InlineKeyboardButton(LANG[lang]['deposit'], callback_data="open_deposit"))
    markup.add(InlineKeyboardButton(LANG[lang]['profile'], callback_data="open_profile"),
               InlineKeyboardButton(LANG[lang]['invite'], callback_data="open_invite"))
    markup.add(InlineKeyboardButton(LANG[lang]['support'], url=f"https://t.me/{OWNER_USER}"),
               InlineKeyboardButton(LANG[lang]['lang_btn'], callback_data="toggle_language"))
    
    if user.get('is_admin') == 1 or uid == OWNER_ID:
        admin_btn = "👑 Admin Panel" if lang=='en' else "👑 لوحة الإدارة"
        markup.add(InlineKeyboardButton(admin_btn, callback_data="admin_panel_main"))

    bot.send_message(chat_id, LANG[lang]['welcome'].format(uid, from_user.first_name, users_total, user.get('balance', 0.0)), reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("init_lang_"))
def init_lang_selection(call):
    lang = call.data.split("_")[2]
    db.users.update_one({'user_id': call.from_user.id}, {'$set': {'lang': lang, 'lang_chosen': True}})
    bot.delete_message(call.message.chat.id, call.message.message_id)
    start_handler(call)

# ============================================================
# 👤 7. الملف الشخصي والسجلات وإحالات
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_profile")
def profile_ui(call):
    if not check_forced_sub(call.from_user.id): start_handler(call); return
    uid = call.from_user.id; u = get_user_data_full(uid); l = u.get('lang', 'ar') if u else 'ar'
    buy_count = db.orders.count_documents({'user_id': uid})
    d_res = list(db.used_transactions.find({'user_id': uid}))
    dep_total = sum([float(d.get('amount', 0)) for d in d_res])

    markup = InlineKeyboardMarkup(row_width=2)
    history_btn = "🛍 Purchases" if l=='en' else "🛍 المشتريات"
    markup.add(InlineKeyboardButton(history_btn, callback_data="history_menu_callback"))
    markup.add(InlineKeyboardButton(LANG[l]['deposit'], callback_data="open_deposit"),
               InlineKeyboardButton(LANG[l]['main_menu'], callback_data="main_menu_refresh"))
    bot.edit_message_text(LANG[l]['profile_txt'].format(uid, u.get('name','User'), u.get('balance', 0.0), buy_count, dep_total), call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "history_menu_callback")
def history_menu_ui(call):
    l = get_lang(call.from_user.id)
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton(LANG[l]['buy_hist'], callback_data="h_view_buy"),
               InlineKeyboardButton(LANG[l]['dep_hist'], callback_data="h_view_dep"))
    markup.add(InlineKeyboardButton(LANG[l]['back'], callback_data="open_profile"))
    bot.edit_message_text(LANG[l]['history_title'], call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("h_view_"))
def show_hist_detail(call):
    uid = call.from_user.id; l = get_lang(uid); mode = call.data.split('_')[2]
    out = ""
    try:
        if mode == "buy":
            recs = list(db.orders.find({'user_id': uid}).sort('_id', -1).limit(5))
            if not recs: out = LANG[l]['no_hist']
            for r in recs:
                p = db.products.find_one({'id': str(r['product_id'])})
                n = clean_name(p['name_en'] if l == 'en' else p['name_ar']) if p else "Product"
                out += f"🧾 <b>فاتورة شراء | Receipt</b>\n📦 المنتج: <b>{n}</b>\n🔑 الكود: <code>{r.get('code_delivered', '')}</code>\n🛡️ حالة التسليم: آمن ومؤكد ✅\n────────────\n"
        else:
            recs = list(db.used_transactions.find({'user_id': uid}).sort('_id', -1).limit(5))
            if not recs: out = LANG[l]['no_hist']
            for r in recs: 
                out += f"💳 <b>إيصال إيداع | Deposit Receipt</b>\n💰 المبلغ: <b>${r.get('amount', 0):.2f}</b>\n🆔 العملية: <code>{r.get('transaction_id', '')}</code>\n🛡️ الحالة: مكتمل ✅\n────────────\n"
    except: out = "❌ Error"
    markup = InlineKeyboardMarkup(); markup.add(InlineKeyboardButton(LANG[l]['back'], callback_data="history_menu_callback"))
    bot.edit_message_text(out, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "open_invite")
def invite_ui(call):
    if not check_forced_sub(call.from_user.id): start_handler(call); return
    uid = call.from_user.id; u = get_user_data_full(uid); l = u.get('lang', 'ar') if u else 'ar'; b_n = bot.get_me().username
    inv_res = list(db.users.find({'referred_by': str(uid)}))
    inv_c = len(inv_res)
    actual_earned = 0.0
    for ref_user in inv_res:
        if db.orders.find_one({'user_id': ref_user['user_id']}):
            actual_earned += REFERRAL_REWARD

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(LANG[l]['main_menu'], callback_data="main_menu_refresh"))
    bot.edit_message_text(LANG[l]['invite_txt'].format(b_n, uid, inv_c, actual_earned), call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

# ============================================================
# 🛒 8. المتجر والشراء (مع تحديد الكمية)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_shop")
def shop_list_ui(call):
    if not check_forced_sub(call.from_user.id): start_handler(call); return
    l = get_lang(call.from_user.id)
    prods = list(db.products.find())
    markup = InlineKeyboardMarkup(row_width=1)
    
    for p in prods:
        st = get_product_stock_count(p['id'])
        icon = '✅' if st > 0 else '❌'
        n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar'))
        btn_text = f"{icon} {n} | ${p.get('price', 0):.2f} | 📦 {st}"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"vi_p_{p['id']}"))
        
    markup.add(InlineKeyboardButton("🔄 Refresh" if l=='en' else "🔄 تحديث", callback_data="open_shop"))
    markup.add(InlineKeyboardButton(LANG[l]['main_menu'], callback_data="main_menu_refresh"))
    try: bot.edit_message_text(LANG[l]['store_title'], call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("vi_p_"))
def shop_detail_ui(call):
    uid = call.from_user.id; l = get_lang(uid); pid = call.data.split('_')[2]
    p = db.products.find_one({'id': str(pid)})
    if not p: return
    stk = get_product_stock_count(pid)
    n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar'))
    d = clean_name(p.get('desc_en') if l == 'en' else p.get('desc_ar'))
    
    text = f"📦 <b>{n}</b>\n\n📝 {d}\n\n💰 <b>Price:</b> ${p.get('price', 0):.2f}\n📊 <b>Stock:</b> {stk}" if l=='en' else f"📦 <b>{n}</b>\n\n📝 {d}\n\n💰 <b>السعر:</b> ${p.get('price', 0):.2f}\n📊 <b>المتوفر:</b> {stk} قطعة"
    markup = InlineKeyboardMarkup()
    if stk > 0: markup.add(InlineKeyboardButton(LANG[l]['buy_now'], callback_data=f"buy_qty_{pid}"))
    markup.add(InlineKeyboardButton(LANG[l]['back'], callback_data="open_shop"))
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_qty_"))
def prompt_quantity(call):
    uid = call.from_user.id; l = get_lang(uid); pid = call.data.split('_')[2]
    if get_product_stock_count(pid) == 0:
        bot.answer_callback_query(call.id, LANG[l]['out_stock'], show_alert=True); return
        
    msg = bot.send_message(uid, LANG[l]['qty_prompt'], parse_mode="HTML")
    bot.register_next_step_handler(msg, execute_bulk_buy, pid, l)

def execute_bulk_buy(message, pid, lang):
    uid = message.from_user.id
    if not message.text or not message.text.isdigit():
        bot.send_message(uid, LANG[lang]['qty_invalid'], parse_mode="HTML"); return
        
    qty = int(message.text.strip())
    if qty <= 0:
        bot.send_message(uid, LANG[lang]['qty_invalid'], parse_mode="HTML"); return

    u = get_user_data_full(uid)
    p = db.products.find_one({'id': str(pid)})
    stk_items = list(db.product_stock.find({'product_id': str(pid), 'is_sold': False}).limit(qty))
    
    if len(stk_items) < qty:
        bot.send_message(uid, LANG[lang]['qty_not_enough'].format(len(stk_items)), parse_mode="HTML"); return
        
    total_price = float(p.get('price', 0)) * qty
    if float(u.get('balance', 0)) < total_price:
        bot.send_message(uid, LANG[lang]['no_balance'], parse_mode="HTML"); return
        
    db.users.update_one({'user_id': uid}, {'$inc': {'balance': -total_price}})
    
    delivered_codes = []
    for item in stk_items:
        db.product_stock.update_one({'_id': item['_id']}, {'$set': {'is_sold': True}})
        db.orders.insert_one({'user_id': uid, 'product_id': str(pid), 'code_delivered': item['code_line']})
        delivered_codes.append(item['code_line'])
        
    codes_str = "\n".join([f"<code>{c}</code>" for c in delivered_codes])
    bot.send_message(uid, LANG[lang]['buy_success'].format(codes_str), parse_mode="HTML")

    # إشعار الإدارة بالشراء
    buyer_m = f"@{u['username']}" if u and u.get('username') else f"ID: <code>{uid}</code>"
    log_ch = get_setting('log_channel')
    if log_ch and log_ch != "Not Set":
        try: 
            log_msg = f"🛒 <b>عملية شراء جديدة!</b>\n\n👤 العميل: {buyer_m}\n📦 المنتج: <b>{clean_name(p.get('name_ar'))}</b>\n🔢 الكمية: {qty}\n💰 القيمة الإجمالية: ${total_price:.2f}"
            bot.send_message(log_ch, log_msg, parse_mode="HTML")
        except: pass

    # نظام الإحالات
    buy_cnt = db.orders.count_documents({'user_id': uid})
    if buy_cnt == qty and u.get('referred_by'):
        ref_id = int(u['referred_by'])
        ref_u = get_user_data_full(ref_id)
        if ref_u:
            db.users.update_one({'user_id': ref_id}, {'$inc': {'balance': REFERRAL_REWARD}})
            if log_ch and log_ch != "Not Set":
                ref_m = f"@{ref_u['username']}" if ref_u.get('username') else f"ID: <code>{ref_id}</code>"
                try: bot.send_message(log_ch, f"🎁 <b>مكافأة إحالة!</b>\nالعميل {ref_m} ربح $0.10 بفضل شراء {buyer_m}", parse_mode="HTML")
                except: pass

# ============================================================
# 🏦 9. بوابات الدفع (آمنة من أخطاء الـ API)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_deposit")
def dep_init_ui(call):
    if not check_forced_sub(call.from_user.id): start_handler(call); return
    l = get_lang(call.from_user.id)
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("🟡 Binance Pay", callback_data="dep_binance"))
    markup.add(InlineKeyboardButton("🟢 USDT (TRC20 / BEP20)", callback_data="dep_crypto_USDT"))
    markup.add(InlineKeyboardButton("🔵 Litecoin (LTC)", callback_data="dep_crypto_LTC"))
    markup.add(InlineKeyboardButton(LANG[l]['back'], callback_data="open_profile"))
    bot.edit_message_text(LANG[l]['dep_choose'], call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "dep_binance")
def dep_binance_ui(call):
    uid = call.from_user.id; l = get_lang(uid)
    wallet = get_setting('wallet_address')
    msg = bot.send_message(uid, LANG[l]['dep_pay'].format(wallet), parse_mode="HTML")
    bot.register_next_step_handler(msg, verify_binance_pay, l)

@bot.callback_query_handler(func=lambda call: call.data.startswith("dep_crypto_"))
def dep_crypto_ui(call):
    uid = call.from_user.id; l = get_lang(uid); coin = call.data.split('_')[2]
    db_key = "usdt_address" if coin == "USDT" else "ltc_address"
    wallet = get_setting(db_key)
    c_name = "USDT (TRC20/BEP20)" if coin == "USDT" else "Litecoin (LTC)"
    msg = bot.send_message(uid, LANG[l]['dep_crypto'].format(c_name, wallet), parse_mode="HTML")
    if coin == "LTC": bot.register_next_step_handler(msg, verify_ltc_public_blockchain, l, wallet)
    else: bot.register_next_step_handler(msg, verify_crypto_tx, l, coin)

def verify_binance_pay(message, lang):
    uid = message.from_user.id
    if not message.text:
        bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML"); return
        
    if binance_client is None:
        bot.send_message(uid, LANG[lang]['crypto_error'], parse_mode="HTML"); return

    tx_id = message.text.strip()
    try:
        bot.send_message(uid, LANG[lang]['crypto_checking'], parse_mode="HTML")
        if db.used_transactions.find_one({'transaction_id': tx_id}):
            bot.reply_to(message, LANG[lang]['tx_used']); return
            
        try:
            pay_h = binance_client.get_pay_trade_history().get('data', [])
        except Exception as e:
            logger.error(f"Binance API Error: {e}")
            bot.send_message(uid, LANG[lang]['crypto_error'], parse_mode="HTML")
            return

        found = False; amt = 0.0
        for d in pay_h:
            if tx_id.lower() == str(d.get('orderId', '')).lower():
                found = True
                amt = float(d.get('amount', 0.0))
                break
                
        if found: credit_user(uid, amt, tx_id, lang, "Binance Pay")
        else: bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML")
    except: bot.send_message(uid, LANG[lang]['crypto_error'], parse_mode="HTML")

def verify_crypto_tx(message, lang, coin):
    uid = message.from_user.id
    if not message.text:
        bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML"); return

    if binance_client is None:
        bot.send_message(uid, LANG[lang]['crypto_error'], parse_mode="HTML"); return
        
    tx_id = message.text.strip()
    try:
        bot.send_message(uid, LANG[lang]['crypto_checking'], parse_mode="HTML")
        if db.used_transactions.find_one({'transaction_id': tx_id}):
            bot.reply_to(message, LANG[lang]['tx_used']); return
            
        try:
            res = binance_client.get_deposit_history(coin=coin)
        except Exception as e:
            logger.error(f"Crypto API Error: {e}")
            bot.send_message(uid, LANG[lang]['crypto_error'], parse_mode="HTML")
            return

        found = False; status = -1; amt = 0.0
        for d in res:
            api_txid = str(d.get('txId', '')).lower()
            if tx_id.lower() in api_txid:
                found = True
                status = int(d.get('status', -1))
                amt = float(d.get('amount', 0.0))
                break
                
        if found:
            if status == 1: credit_user(uid, amt, tx_id, lang, f"Crypto {coin}")
            else: bot.send_message(uid, LANG[lang]['dep_pending'], parse_mode="HTML")
        else: bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML")
    except: bot.send_message(uid, LANG[lang]['crypto_error'], parse_mode="HTML")

def verify_ltc_public_blockchain(message, lang, wallet_address):
    uid = message.from_user.id
    if not message.text:
        bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML"); return
        
    tx_id = message.text.strip()
    try:
        bot.send_message(uid, LANG[lang]['crypto_checking'], parse_mode="HTML")
        if db.used_transactions.find_one({'transaction_id': tx_id}):
            bot.reply_to(message, LANG[lang]['tx_used']); return
            
        url = f"https://api.blockcypher.com/v1/ltc/main/txs/{tx_id}"
        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()
            confirmations = data.get("confirmations", 0)
            received_ltc = 0.0
            for output in data.get("outputs", []):
                if wallet_address in output.get("addresses", []):
                    received_ltc += float(output.get("value", 0)) / 100000000.0
            
            if received_ltc > 0:
                if confirmations >= 1:
                    try:
                        ltc_price = float(binance_client.get_symbol_ticker(symbol="LTCUSDT")['price'])
                        usd_amount = received_ltc * ltc_price
                    except: usd_amount = received_ltc * 80.0
                    credit_user(uid, usd_amount, tx_id, lang, "Litecoin (LTC)")
                else: bot.send_message(uid, LANG[lang]['dep_pending'], parse_mode="HTML")
            else: bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML")
        else: bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML")
    except: bot.send_message(uid, LANG[lang]['crypto_error'], parse_mode="HTML")

def credit_user(uid, amt, tx_id, lang, method):
    db.users.update_one({'user_id': uid}, {'$inc': {'balance': amt}})
    db.used_transactions.insert_one({'transaction_id': tx_id, 'amount': amt, 'user_id': uid})
    bot.send_message(uid, LANG[lang]['dep_success'].format(amt), parse_mode="HTML")
    
    # إشعار الإدارة بالإيداع
    u = get_user_data_full(uid)
    buyer_m = f"@{u['username']}" if u and u.get('username') else f"ID: <code>{uid}</code>"
    log_ch = get_setting('log_channel')
    if log_ch and log_ch != "Not Set":
        try: 
            log_msg = f"💸 <b>إيداع جديد!</b>\n\n👤 العميل: {buyer_m}\n💰 المبلغ: <b>${amt:.2f}</b>\n💳 الطريقة: {method}\n🆔 رقم العملية: <code>{tx_id}</code>"
            bot.send_message(log_ch, log_msg, parse_mode="HTML")
        except: pass

# ============================================================
# 👑 10. لوحة الإدارة
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "admin_panel_main")
def admin_main_ui(call):
    l = get_lang(call.from_user.id)
    markup = InlineKeyboardMarkup(row_width=2)
    if l == 'en':
        markup.add(InlineKeyboardButton("➕ Add Product", callback_data="ad_p_add"),
                   InlineKeyboardButton("📦 Refill Stock", callback_data="ad_s_fill"))
        markup.add(InlineKeyboardButton("📝 Edit Product", callback_data="ad_p_edit"),
                   InlineKeyboardButton("🗑 Delete Product", callback_data="ad_p_del"))
        markup.add(InlineKeyboardButton("👑 Promote Admin", callback_data="ad_new_admin"),
                   InlineKeyboardButton("💰 Gift Balance", callback_data="ad_gift"))
        markup.add(InlineKeyboardButton("📜 Records", callback_data="ad_logs_all"),
                   InlineKeyboardButton("📢 Broadcast", callback_data="ad_bc"))
        markup.add(InlineKeyboardButton("⚙️ Settings", callback_data="ad_shop_settings"),
                   InlineKeyboardButton("📢 Forced Sub", callback_data="ad_fsub_list"))
        markup.add(InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu_refresh"))
        text = "👑 <b>Admin Dashboard:</b>"
    else:
        markup.add(InlineKeyboardButton("➕ أضف منتج", callback_data="ad_p_add"),
                   InlineKeyboardButton("📦 شحن ستوك", callback_data="ad_s_fill"))
        markup.add(InlineKeyboardButton("📝 تعديل منتج", callback_data="ad_p_edit"),
                   InlineKeyboardButton("🗑 حذف منتج", callback_data="ad_p_del"))
        markup.add(InlineKeyboardButton("👑 ترقية مدير", callback_data="ad_new_admin"),
                   InlineKeyboardButton("💰 شحن رصيد", callback_data="ad_gift"))
        markup.add(InlineKeyboardButton("📜 السجلات", callback_data="ad_logs_all"),
                   InlineKeyboardButton("📢 برودكاست", callback_data="ad_bc"))
        markup.add(InlineKeyboardButton("⚙️ إعدادات المتجر", callback_data="ad_shop_settings"),
                   InlineKeyboardButton("📢 الاشتراك الإجباري", callback_data="ad_fsub_list"))
        markup.add(InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="main_menu_refresh"))
        text = "👑 <b>لوحة القيادة (الإدارة):</b>"
        
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_fsub_list")
def admin_fsub_list(call):
    chans = list(db.required_channels.find())
    markup = InlineKeyboardMarkup(row_width=1)
    if chans:
        for c in chans: markup.add(InlineKeyboardButton(f"❌ حذف {c['channel_id']}", callback_data=f"del_fsub_{c['channel_id']}"))
    markup.add(InlineKeyboardButton("➕ إضافة قناة باليوزر (@)", callback_data="ad_fsub_add"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    bot.edit_message_text("📢 <b>إدارة قنوات الاشتراك الإجباري:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_fsub_add")
def admin_fsub_add(call):
    msg = bot.send_message(call.message.chat.id, "أرسل يوزر القناة (مثال: @ninto_dev):\n\n⚠️ <b>تنبيه:</b> ارفع البوت مشرف أولاً!", parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_fsub_save)

def admin_fsub_save(message):
    cid = message.text.strip()
    if not cid.startswith('@') and not cid.startswith('-100'):
        bot.send_message(message.chat.id, "❌ خطأ! يجب أن يبدأ اليوزر بـ @")
        return
    try:
        bot.get_chat_member(cid, bot.get_me().id)
        db.required_channels.insert_one({'channel_id': cid})
        bot.send_message(message.chat.id, f"✅ تم إضافة القناة {cid} بنجاح.")
    except Exception:
        bot.send_message(message.chat.id, f"❌ البوت ليس أدمن في القناة، أو أن اليوزر غير صحيح!")

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_fsub_"))
def del_fsub_btn(call):
    ch = call.data.replace("del_fsub_", "")
    db.required_channels.delete_one({'channel_id': ch})
    bot.answer_callback_query(call.id, "✅ تم حذف القناة بنجاح!", show_alert=True)
    admin_fsub_list(call)

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_add")
def ad_p_step1(call):
    msg = bot.send_message(call.from_user.id, "📦 أرسل اسم المنتج (بالعربية فقط):")
    bot.register_next_step_handler(msg, ad_p_step2)

def ad_p_step2(message):
    uid = message.from_user.id; n_ar = message.text
    try: n_en = GoogleTranslator(source='auto', target='en').translate(n_ar)
    except: n_en = n_ar
    temp_product[uid] = {'n_ar': n_ar, 'n_en': n_en}
    msg = bot.send_message(uid, f"✅ تم الحفظ (الترجمة: {n_en})\n📝 أرسل وصف المنتج (بالعربية فقط):")
    bot.register_next_step_handler(msg, ad_p_step3)

def ad_p_step3(message):
    uid = message.from_user.id; d_ar = message.text
    try: d_en = GoogleTranslator(source='auto', target='en').translate(d_ar)
    except: d_en = d_ar
    temp_product[uid].update({'d_ar': d_ar, 'd_en': d_en})
    msg = bot.send_message(uid, "💰 أرسل السعر بالدولار ($):")
    bot.register_next_step_handler(msg, ad_p_final)

def ad_p_final(message):
    uid = message.from_user.id
    try:
        price = float(message.text); p = temp_product[uid]
        pid = str(int(time.time()))
        db.products.insert_one({'id': pid, 'name_ar': p['n_ar'], 'name_en': p['n_en'], 'desc_ar': p['d_ar'], 'desc_en': p['d_en'], 'price': price})
        bot.send_message(uid, "✅ تم إضافة المنتج بنجاح!")
    except: bot.send_message(uid, "❌ خطأ في السعر.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_new_admin")
def admin_add_admin_start(call):
    msg = bot.send_message(call.from_user.id, "👑 Send <b>ID</b> or <b>@username</b>:")
    bot.register_next_step_handler(msg, admin_add_admin_save)

def admin_add_admin_save(message):
    target = message.text.strip()
    if target.startswith('@') or not target.replace('-', '').isdigit():
        u = db.users.find_one({'username': target.replace('@', '').lower()})
    else: u = get_user_data_full(int(target))
    if u:
        db.users.update_one({'user_id': u['user_id']}, {'$set': {'is_admin': 1}})
        bot.send_message(message.chat.id, "✅ User promoted.")
    else: bot.send_message(message.chat.id, "❌ Not found.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_gift")
def ad_gift_start(call):
    msg = bot.send_message(call.from_user.id, "👤 <b>Send User ID or @username:</b>")
    bot.register_next_step_handler(msg, ad_gift_val)

def ad_gift_val(message):
    target = message.text.strip()
    if target.startswith('@') or not target.replace('-', '').isdigit():
        u = db.users.find_one({'username': target.replace('@', '').lower()})
    else: u = get_user_data_full(int(target))
    if u:
        msg = bot.send_message(message.from_user.id, f"💰 Amount for {u.get('name')}:")
        bot.register_next_step_handler(msg, ad_gift_finish, u['user_id'])
    else: bot.send_message(message.chat.id, "❌ Not found.")

def ad_gift_finish(message, tid):
    try:
        val = float(message.text)
        db.users.update_one({'user_id': tid}, {'$inc': {'balance': val}})
        bot.send_message(message.from_user.id, "✅ Done.")
    except: bot.send_message(message.from_user.id, "❌ Error.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_edit")
def admin_edit_list(call):
    prods = list(db.products.find())
    markup = InlineKeyboardMarkup(row_width=1)
    for p in prods: markup.add(InlineKeyboardButton(f"📝 {clean_name(p.get('name_en'))}", callback_data=f"edit_p_{p['id']}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    bot.edit_message_text("👇 Select Product:", call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_p_"))
def admin_edit_opts(call):
    pid = call.data.split('_')[2]
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("💵 Price", callback_data=f"ep_price_{pid}"),
               InlineKeyboardButton("📝 Desc (AR)", callback_data=f"ep_dar_{pid}"),
               InlineKeyboardButton("✏️ Name (AR)", callback_data=f"ep_nar_{pid}"),
               InlineKeyboardButton("✏️ Name (EN)", callback_data=f"ep_nen_{pid}"),
               InlineKeyboardButton("🔙 Back", callback_data="ad_p_edit"))
    bot.edit_message_text("⚙️ Options:", call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ep_"))
def admin_edit_prompt(call):
    field = call.data.split('_')[1]; pid = call.data.split('_')[2]
    msg = bot.send_message(call.message.chat.id, "Send new value:")
    bot.register_next_step_handler(msg, admin_save_edit, field, pid)

def admin_save_edit(message, field, pid):
    val = message.text
    keys = {"price": "price", "dar": "desc_ar", "nar": "name_ar", "nen": "name_en"}
    
    if field == "price":
        try:
            new_price = float(val)
            p = db.products.find_one({'id': str(pid)})
            old_price = float(p.get('price', 0))
            
            db.products.update_one({'id': str(pid)}, {'$set': {'price': new_price}})
            bot.send_message(message.chat.id, "✅ Updated.")
            
            if new_price < old_price: 
                alert_msg = f"📉 <b>تخفيض مذهل! / Price Drop!</b> 🔥\n\nالمنتج: <b>{p['name_ar']}</b>\nالسعر القديم: <strike>${old_price}</strike>\nالسعر الجديد: <b>${new_price}</b> فقط!\n\nسارع بالشراء الآن من المتجر! 🛒"
                users = list(db.users.find())
                for u in users:
                    try: bot.send_message(u['user_id'], alert_msg, parse_mode="HTML"); time.sleep(0.05)
                    except: continue
        except:
            bot.send_message(message.chat.id, "❌ خطأ في السعر.")
    else:
        db.products.update_one({'id': str(pid)}, {'$set': {keys[field]: val}})
        bot.send_message(message.chat.id, "✅ Updated.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_del")
def admin_del_list(call):
    prods = list(db.products.find())
    markup = InlineKeyboardMarkup(row_width=1)
    for p in prods: markup.add(InlineKeyboardButton(f"🗑 {clean_name(p.get('name_en'))}", callback_data=f"del_p_{p['id']}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    bot.edit_message_text("👇 Select Product to Delete:", call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_p_"))
def admin_del_exec(call):
    pid = call.data.split('_')[2]
    try:
        db.product_stock.delete_many({'product_id': str(pid)})
        db.orders.delete_many({'product_id': str(pid)})
        db.products.delete_one({'id': str(pid)})
        bot.answer_callback_query(call.id, "✅ Deleted Successfully!", show_alert=True)
        admin_main_ui(call)
    except: bot.answer_callback_query(call.id, "❌ Error occurred.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == "ad_s_fill")
def admin_stock_list_ui(call):
    prods = list(db.products.find())
    markup = InlineKeyboardMarkup(row_width=1)
    for p in prods: markup.add(InlineKeyboardButton(f"📦 {clean_name(p.get('name_en'))}", callback_data=f"stk_add_{p['id']}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    bot.edit_message_text("👇 Select Product:", call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_add_"))
def admin_stock_input(call):
    pid = call.data.split('_')[2]
    msg = bot.send_message(call.from_user.id, "📥 <b>Send Codes (one per line):</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_stock_save, pid)

def admin_stock_save(message, pid):
    lines = message.text.split('\n')
    count = 0
    for l in lines:
        if l.strip():
            db.product_stock.insert_one({'product_id': str(pid), 'code_line': l.strip(), 'is_sold': False})
            count += 1
            
    p = db.products.find_one({'id': str(pid)})
    if p:
        p_name = clean_name(p.get('name_en', 'Product'))
        stk_total = get_product_stock_count(pid)
        alert_msg = f"🔔 <b>New Stock Added!</b>\n\n🛍 <b>Product:</b> {p_name}\n📦 <b>Available Now:</b> {stk_total}\n\n<i>Hurry up and grab yours!</i>"
        users = list(db.users.find())
        for u in users:
            try: bot.send_message(u['user_id'], alert_msg, parse_mode="HTML"); time.sleep(0.05)
            except: continue

    bot.send_message(message.chat.id, f"✅ <b>{count} Codes added! Broadcast sent.</b>", parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_logs_all")
def admin_all_logs(call):
    recs = list(db.used_transactions.find().sort('_id', -1).limit(10))
    txt = "📜 <b>Last 10 Deposits:</b>\n\n"
    if not recs: txt = "📭 No records."
    for r in recs: txt += f"👤 <code>{r.get('user_id')}</code> | 💰 <b>${r.get('amount')}</b> | 🆔 <code>{r.get('transaction_id')}</code>\n"
    markup = InlineKeyboardMarkup(); markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_bc")
def admin_bc_init(call):
    msg = bot.send_message(call.from_user.id, "📢 Send Broadcast Message:")
    bot.register_next_step_handler(msg, admin_bc_exe)

def admin_bc_exe(message):
    users = list(db.users.find())
    for u in users:
        try: bot.copy_message(u['user_id'], message.chat.id, message.message_id); time.sleep(0.05)
        except: continue
    bot.send_message(message.chat.id, "✅ Broadcast Sent.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_shop_settings")
def admin_shop_settings(call):
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("💳 Binance Pay ID", callback_data="set_v_wallet"))
    markup.add(InlineKeyboardButton("🟢 USDT Address", callback_data="set_v_usdt"))
    markup.add(InlineKeyboardButton("🔵 LTC Address", callback_data="set_v_ltc"))
    markup.add(InlineKeyboardButton("📢 Logs Channel (@)", callback_data="set_v_log"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    bot.edit_message_text("⚙️ <b>Settings:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("set_v_"))
def admin_set_inputs(call):
    mode = call.data
    msg = bot.send_message(call.from_user.id, "Send new value:")
    bot.register_next_step_handler(msg, admin_save_setting, mode)

def admin_save_setting(message, mode):
    val = message.text.strip()
    keys = {"set_v_log": "log_channel", "set_v_usdt": "usdt_address", "set_v_ltc": "ltc_address", "set_v_wallet": "wallet_address"}
    db.settings.update_one({'key': keys[mode]}, {'$set': {'value': val}}, upsert=True)
    bot.send_message(message.chat.id, "✅ Updated.")

@bot.callback_query_handler(func=lambda call: call.data == "toggle_language")
def toggle_lang(call):
    uid = call.from_user.id; u = get_user_data_full(uid)
    new_l = 'en' if u.get('lang', 'ar') == 'ar' else 'ar'
    db.users.update_one({'user_id': uid}, {'$set': {'lang': new_l}})
    bot.delete_message(call.message.chat.id, call.message.message_id)
    start_handler(call.message)

@bot.callback_query_handler(func=lambda call: call.data == "main_menu_refresh")
def refresh_main(call):
    try: bot.delete_message(call.message.chat.id, call.message.message_id)
    except: pass
    start_handler(call.message)

# ============================================================
# 🚀 11. تشغيل البوت 
# ============================================================
def run_bot():
    try:
        bot.delete_webhook(drop_pending_updates=True)
        time.sleep(1)
    except: pass

    while True:
        try:
            bot.polling(non_stop=True, skip_pending=True)
        except Exception as e:
            logger.error(f"Polling Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_bot()
