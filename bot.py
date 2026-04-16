import sys
import os
import time
import datetime
import re
import logging
import requests
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# ============================================================
# ⚙️ 1. نظام الحماية الصارم (منع تعليق المكتبة)
# ============================================================
try:
    import telebot
    from telebot import types
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    from telebot.apihelper import ApiTelegramException
except AttributeError:
    print("❌ خطأ: تأكد من عدم وجود ملف telebot.py في مجلدك.")
    sys.exit(1)

from binance.client import Client
from deep_translator import GoogleTranslator
from supabase import create_client, Client as SupabaseClient

# ضبط تسجيل الأخطاء يدوياً
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# 🔑 2. الإعدادات ومتغيرات البيئة (Render Environment Variables)
# ============================================================
TOKEN = os.getenv('TOKEN', 'YOUR_BOT_TOKEN_HERE') 
try:
    OWNER_ID = int(os.getenv('OWNER_ID', '8286529656'))
except ValueError:
    OWNER_ID = 8286529656
OWNER_USER = os.getenv('OWNER_USER', 'lara_v2') 

BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', 'YOUR_BINANCE_KEY_HERE')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET', 'YOUR_BINANCE_SECRET_HERE')

SUPABASE_URL = os.getenv('SUPABASE_URL', 'YOUR_SUPABASE_URL_HERE')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'YOUR_SUPABASE_KEY_HERE')

# ============================================================
# 🌐 السيرفر الوهمي لمنع Render من إيقاف البوت (Keep-Alive)
# ============================================================
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Bot is alive and running on Render! 🚀")
        
def keep_alive():
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), DummyHandler)
    logger.info(f"🌐 Dummy web server running on port {port} for Render...")
    server.serve_forever()

# تشغيل السيرفر في خلفية البوت
threading.Thread(target=keep_alive, daemon=True).start()

# ============================================================
# 🚀 تهيئة البوت وقاعدة البيانات
# ============================================================
bot = telebot.TeleBot(TOKEN, num_threads=100)
supabase: SupabaseClient = create_client(SUPABASE_URL, SUPABASE_KEY)

try:
    binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    print("✅ Binance API Connected")
except Exception as e:
    print(f"❌ Binance API Error: {e}")

REFERRAL_REWARD = 0.10
user_states = {}
temp_product = {}

# ============================================================
# 🌟 3. القاموس الكامل (عربي + إنجليزي)
# ============================================================
LANG = {
    'ar': {
        'welcome': "👋 <b>أهلاً بك في المتجر الاحترافي!</b>\n\n🆔 الأيدي: <code>{}</code>\n👤 الاسم: <b>{}</b>\n👥 المستخدمين: <b>{}</b>\n💰 الرصيد: <b>${:.2f}</b>",
        'profile_txt': "👤 <b>ملفك الشخصي</b>\n\n🆔 الأيدي: <code>{}</code>\n👤 الاسم: <b>{}</b>\n💰 الرصيد: <b>${:.2f}</b>\n✅ المشتريات: <b>{}</b>\n📦 إجمالي الشحن: <b>${:.2f}</b>",
        'invite_txt': "👥 <b>نظام الإحالات</b>\n\n🔗 رابط الدعوة الخاص بك:\n<code>https://t.me/{}?start={}</code>\n\n🎁 <b>طريقة الربح:</b>\nستحصل على <b>$0.10</b> رصيد مجاني عندما يقوم صديقك بـ <b>أول عملية شراء ناجحة</b>.",
        'dep_choose': "💳 <b>اختر طريقة الدفع المناسبة:</b>",
        'dep_pay': "🟡 <b>Binance Pay</b>\n\nأرسل المبلغ إلى الـ ID التالي:\n🆔 Binance ID: <code>{}</code>\n\n⚠️ بعد التحويل، أرسل رقم العملية <b>(Order ID)</b> هنا.",
        'dep_crypto': "🟢 <b>شحن عبر {}</b>\n\nأرسل المبلغ إلى المحفظة:\n<code>{}</code>\n\n⚠️ بعد التحويل، أرسل الهاش <b>(TxID)</b> هنا.",
        'tx_used': "⚠️ عذراً، هذا الرقم مستخدم مسبقاً!",
        'crypto_checking': "⏳ <b>جاري فحص البلوكتشين...</b>",
        'dep_success': "✅ <b>تم التحقق!</b> تم إضافة <b>${:.2f}</b> إلى رصيدك.",
        'dep_fail': "❌ <b>لم نجد العملية!</b> تأكد من الرقم ومرور 5 دقائق.",
        'dep_pending': "⏳ <b>قيد المعالجة!</b> لم تكتمل في الشبكة بعد.",
        'time_error': "❌ <b>مرفوض!</b> الحوالة قديمة جداً.",
        'history_title': "📜 <b>سجلاتك السابقة:</b>\nاختر السجل الذي تريد عرضه:",
        'products': "🛒 المنتجات", 'deposit': "💳 شحن الرصيد", 'profile': "👤 الملف الشخصي", 
        'invite': "👥 الإحالات", 'support': "👨‍💻 الدعم الفني", 'lang_btn': "🌐 English", 
        'back': "🔙 رجوع", 'main_menu': "🏠 القائمة الرئيسية", 'buy_hist': "🛍 المشتريات", 
        'dep_hist': "💳 الإيداعات", 'no_hist': "📭 لا يوجد سجلات.",
        'store_title': "🛒 <b>المنتجات المتوفرة:</b>", 'buy_now': "✅ شراء الآن",
        'buy_success': "✅ <b>تم الشراء بنجاح!</b>\n\nأكوادك هي:\n{}",
        'no_balance': "❌ رصيدك غير كافٍ!", 'out_stock': "❌ نفد المخزون!",
        'must_join': "❌ <b>يجب الاشتراك في القنوات:</b>", 'check_sub': "🔄 تحقق",
        'crypto_error': "❌ خطأ في الاتصال بالشبكة.",
        'qty_prompt': "🔢 <b>أرسل الكمية التي تريد شراءها (أرقام فقط):</b>",
        'qty_invalid': "❌ <b>يرجى إرسال أرقام صحيحة أكبر من صفر!</b>",
        'qty_not_enough': "❌ <b>المتوفر فقط {} قطعة!</b>"
    },
    'en': {
        'welcome': "👋 <b>Welcome to the Pro Shop!</b>\n\n🆔 ID: <code>{}</code>\n👤 Name: <b>{}</b>\n👥 Users: <b>{}</b>\n💰 Balance: <b>${:.2f}</b>",
        'profile_txt': "👤 <b>Your Profile</b>\n\n🆔 ID: <code>{}</code>\n👤 Name: <b>{}</b>\n💰 Balance: <b>${:.2f}</b>\n✅ Purchases: <b>{}</b>\n📦 Total Deposited: <b>${:.2f}</b>",
        'invite_txt': "👥 <b>Referral System</b>\n\n🔗 Your Link:\n<code>https://t.me/{}?start={}</code>\n\n🎁 <b>Rule:</b> Earn <b>$0.10</b> after your friend's <b>first purchase</b>.",
        'dep_choose': "💳 <b>Choose payment method:</b>",
        'dep_pay': "🟡 <b>Binance Pay</b>\n\nSend amount to ID:\n🆔 Binance ID: <code>{}</code>\n\n⚠️ Send <b>Order ID</b> here.",
        'dep_crypto': "🟢 <b>{} Deposit</b>\n\nSend amount to address:\n<code>{}</code>\n\n⚠️ Send <b>TxID (Hash)</b> here.",
        'tx_used': "⚠️ ID already used!",
        'crypto_checking': "⏳ <b>Checking network...</b>",
        'dep_success': "✅ <b>Verified!</b> <b>${:.2f}</b> added.",
        'dep_fail': "❌ <b>Not found!</b> Check ID and wait 5 min.",
        'dep_pending': "⏳ <b>Pending!</b> Not confirmed on blockchain yet.",
        'time_error': "❌ <b>Rejected!</b> Transaction over 24h old.",
        'history_title': "📜 <b>Your Records:</b>",
        'products': "🛒 Products", 'deposit': "💳 Deposit", 'profile': "👤 Profile", 
        'invite': "👥 Referrals", 'support': "👨‍💻 Support", 'lang_btn': "🌐 العربية", 
        'back': "🔙 Back", 'main_menu': "🏠 Main Menu", 'buy_hist': "🛍 Purchases", 
        'dep_hist': "💳 Deposits", 'no_hist': "📭 No records.",
        'store_title': "🛒 <b>Available Products:</b>", 'buy_now': "✅ Buy Now",
        'buy_success': "✅ <b>Purchase Successful!</b>\n\nYour codes:\n{}",
        'no_balance': "❌ Low balance!", 'out_stock': "❌ Out of stock!",
        'must_join': "❌ <b>Join channels first:</b>", 'check_sub': "🔄 Verify",
        'crypto_error': "❌ Network error.",
        'qty_prompt': "🔢 <b>Enter the quantity you want to buy (numbers only):</b>",
        'qty_invalid': "❌ <b>Please send valid numbers greater than zero!</b>",
        'qty_not_enough': "❌ <b>Only {} pieces available!</b>"
    }
}

# ============================================================
# 🛠️ 4. الدوال المساعدة وقاعدة البيانات
# ============================================================
def clean_name(text):
    if not text: return ""
    clean = re.sub(r'<[^>]+>', '', text)
    clean = re.sub(r'custom:\d+', '', clean)
    clean = re.sub(r'standard:', '', clean)
    return clean.strip()

def get_setting(key, default="Not Set"):
    try:
        res = supabase.table('settings').select('value').eq('key', key).execute().data
        return res[0]['value'] if res else default
    except: return default

def get_user_data_full(uid):
    try:
        res = supabase.table('users').select('*').eq('user_id', uid).execute()
        return res.data[0] if res.data else None
    except: return None

def get_product_stock_count(pid):
    try:
        res = supabase.table('product_stock').select('id').eq('product_id', pid).eq('is_sold', False).execute()
        return len(res.data)
    except: return 0

def check_forced_sub(uid):
    if uid == OWNER_ID: return True
    try:
        chans = supabase.table('required_channels').select('channel_id').execute().data
        for c in chans:
            status = bot.get_chat_member(c['channel_id'], uid).status
            if status in ['left', 'kicked']: return False
        return True
    except: return True

# ============================================================
# 🏠 5. معالج البداية (Start)
# ============================================================
@bot.message_handler(commands=['start'])
def start_handler(message):
    chat_id = message.chat.id if not isinstance(message, types.CallbackQuery) else message.message.chat.id
    from_user = message.from_user
    full_text = message.text if not isinstance(message, types.CallbackQuery) else (message.message.text or "")
    uid = from_user.id
    uname = from_user.username.lower() if from_user.username else ""
    
    user = get_user_data_full(uid)
    if not user:
        args = full_text.split()
        ref = args[1] if len(args) > 1 and args[1].isdigit() else None
        supabase.table('users').insert({
            'user_id': uid, 'name': from_user.first_name, 'username': uname, 
            'referred_by': ref, 'balance': 0.0, 'lang': 'ar', 'is_admin': 0
        }).execute()
        user = get_user_data_full(uid)
    else:
        supabase.table('users').update({'username': uname}).eq('user_id', uid).execute()

    lang = user['lang'] if user['lang'] in LANG else 'ar'
    if not check_forced_sub(uid):
        chans = supabase.table('required_channels').select('channel_id').execute().data
        markup = InlineKeyboardMarkup(row_width=1)
        for c in chans: 
            btn_txt = "📢 Channel" if lang=='en' else "📢 القناة"
            markup.add(InlineKeyboardButton(btn_txt, url=f"https://t.me/{c['channel_id'].replace('@','') }"))
        markup.add(InlineKeyboardButton(LANG[lang]['check_sub'], callback_data="main_menu_refresh"))
        bot.send_message(chat_id, LANG[lang]['must_join'], reply_markup=markup, parse_mode="HTML")
        return

    users_total = len(supabase.table('users').select('user_id').execute().data)
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

    bot.send_message(chat_id, LANG[lang]['welcome'].format(uid, from_user.first_name, users_total, user['balance']), reply_markup=markup, parse_mode="HTML")

# ============================================================
# 👤 6. الملف الشخصي والسجلات
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_profile")
def profile_ui(call):
    uid = call.from_user.id; u = get_user_data_full(uid); l = u['lang']
    buy_count = len(supabase.table('orders').select('id').eq('user_id', uid).execute().data)
    d_res = supabase.table('used_transactions').select('amount').eq('user_id', uid).execute().data
    dep_total = sum([d['amount'] for d in d_res]) if d_res else 0.0

    markup = InlineKeyboardMarkup(row_width=2)
    history_btn = "🛍 Purchases" if l=='en' else "🛍 المشتريات"
    markup.add(InlineKeyboardButton(history_btn, callback_data="history_menu_callback"))
    markup.add(InlineKeyboardButton(LANG[l]['deposit'], callback_data="open_deposit"),
               InlineKeyboardButton(LANG[l]['main_menu'], callback_data="main_menu_refresh"))
    bot.edit_message_text(LANG[l]['profile_txt'].format(uid, u['name'], u['balance'], buy_count, dep_total), call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "history_menu_callback")
def history_menu_ui(call):
    l = get_user_data_full(call.from_user.id)['lang']
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton(LANG[l]['buy_hist'], callback_data="h_view_buy"),
               InlineKeyboardButton(LANG[l]['dep_hist'], callback_data="h_view_dep"))
    markup.add(InlineKeyboardButton(LANG[l]['back'], callback_data="open_profile"))
    bot.edit_message_text(LANG[l]['history_title'], call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("h_view_"))
def show_hist_detail(call):
    uid = call.from_user.id; l = get_user_data_full(uid)['lang']; mode = call.data.split('_')[2]
    out = ""
    try:
        if mode == "buy":
            recs = supabase.table('orders').select('*').eq('user_id', uid).order('id', desc=True).limit(5).execute().data
            if not recs: out = LANG[l]['no_hist']
            for r in recs:
                p = supabase.table('products').select('name_ar, name_en').eq('id', r['product_id']).execute().data[0]
                n = clean_name(p['name_en'] if l == 'en' else p['name_ar'])
                out += f"🛍 <b>{n}</b>\n🔑 <code>{r['code_delivered']}</code>\n---\n"
        else:
            recs = supabase.table('used_transactions').select('*').eq('user_id', uid).order('id', desc=True).limit(5).execute().data
            if not recs: out = LANG[l]['no_hist']
            for r in recs: out += f"💰 <b>${r['amount']}</b> | 🆔 <code>{r['transaction_id']}</code>\n"
    except: out = "❌ Error"
    markup = InlineKeyboardMarkup(); markup.add(InlineKeyboardButton(LANG[l]['back'], callback_data="history_menu_callback"))
    bot.edit_message_text(out, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

# ============================================================
# 🤝 7. نظام الإحالات
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_invite")
def invite_ui(call):
    uid = call.from_user.id; u = get_user_data_full(uid); l = u['lang']; b_n = bot.get_me().username
    inv_res = supabase.table('users').select('user_id').eq('referred_by', str(uid)).execute().data
    inv_c = len(inv_res) if inv_res else 0
    actual_earned = 0.0
    if inv_res:
        for ref_user in inv_res:
            if supabase.table('orders').select('id').eq('user_id', ref_user['user_id']).limit(1).execute().data:
                actual_earned += REFERRAL_REWARD

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(LANG[l]['main_menu'], callback_data="main_menu_refresh"))
    bot.edit_message_text(LANG[l]['invite_txt'].format(b_n, uid, inv_c, actual_earned), call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

# ============================================================
# 🛒 8. المتجر والشراء (نظام تحديد الكميات)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_shop")
def shop_list_ui(call):
    l = get_user_data_full(call.from_user.id)['lang']
    prods = supabase.table('products').select('*').execute().data
    markup = InlineKeyboardMarkup(row_width=1)
    
    for p in prods:
        st = get_product_stock_count(p['id'])
        icon = '✅' if st > 0 else '❌'
        n = clean_name(p['name_en'] if l == 'en' else p['name_ar'])
        btn_text = f"{icon} {n} | ${p['price']:.2f} | 📦 {st}"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"vi_p_{p['id']}"))
        
    markup.add(InlineKeyboardButton("🔄 Refresh" if l=='en' else "🔄 تحديث", callback_data="open_shop"))
    markup.add(InlineKeyboardButton(LANG[l]['main_menu'], callback_data="main_menu_refresh"))
    
    current_time = datetime.datetime.now().strftime("%I:%M:%S %p")
    title = f"{LANG[l]['store_title']}\n⏳ {current_time}"
    try: bot.edit_message_text(title, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except ApiTelegramException: bot.answer_callback_query(call.id, "✅ Updated!", show_alert=False)

@bot.callback_query_handler(func=lambda call: call.data.startswith("vi_p_"))
def shop_detail_ui(call):
    uid = call.from_user.id; l = get_user_data_full(uid)['lang']; pid = call.data.split('_')[2]
    p = supabase.table('products').select('*').eq('id', pid).execute().data[0]
    stk = get_product_stock_count(pid)
    n = clean_name(p['name_en'] if l == 'en' else p['name_ar'])
    d = clean_name(p['desc_en'] if l == 'en' else p['desc_ar'])
    
    text = f"📦 <b>{n}</b>\n\n📝 {d}\n\n💰 <b>Price:</b> ${p['price']:.2f}\n📊 <b>Stock:</b> {stk}" if l=='en' else f"📦 <b>{n}</b>\n\n📝 {d}\n\n💰 <b>السعر:</b> ${p['price']:.2f}\n📊 <b>المتوفر:</b> {stk} قطعة"
    markup = InlineKeyboardMarkup()
    if stk > 0: 
        markup.add(InlineKeyboardButton(LANG[l]['buy_now'], callback_data=f"buy_qty_{pid}"))
    markup.add(InlineKeyboardButton(LANG[l]['back'], callback_data="open_shop"))
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_qty_"))
def prompt_quantity(call):
    uid = call.from_user.id; l = get_user_data_full(uid)['lang']; pid = call.data.split('_')[2]
    stk = get_product_stock_count(pid)
    
    if stk == 0:
        bot.answer_callback_query(call.id, LANG[l]['out_stock'], show_alert=True); return
        
    msg = bot.send_message(uid, LANG[l]['qty_prompt'], parse_mode="HTML")
    bot.register_next_step_handler(msg, execute_bulk_buy, pid, l)

def execute_bulk_buy(message, pid, lang):
    uid = message.from_user.id
    try:
        qty = int(message.text.strip())
        if qty <= 0: raise ValueError
    except:
        bot.send_message(uid, LANG[lang]['qty_invalid'], parse_mode="HTML"); return

    u = get_user_data_full(uid)
    p = supabase.table('products').select('*').eq('id', pid).execute().data[0]
    
    stk_items = supabase.table('product_stock').select('*').eq('product_id', pid).eq('is_sold', False).limit(qty).execute().data
    if len(stk_items) < qty:
        bot.send_message(uid, LANG[lang]['qty_not_enough'].format(len(stk_items)), parse_mode="HTML"); return
        
    total_price = p['price'] * qty
    if u['balance'] < total_price:
        bot.send_message(uid, LANG[lang]['no_balance'], parse_mode="HTML"); return
        
    supabase.table('users').update({'balance': u['balance'] - total_price}).eq('user_id', uid).execute()
    
    delivered_codes = []
    for item in stk_items:
        supabase.table('product_stock').update({'is_sold': True}).eq('id', item['id']).execute()
        supabase.table('orders').insert({'user_id': uid, 'product_id': pid, 'code_delivered': item['code_line']}).execute()
        delivered_codes.append(item['code_line'])
        
    codes_str = "\n".join([f"<code>{c}</code>" for c in delivered_codes])
    bot.send_message(uid, LANG[lang]['buy_success'].format(codes_str), parse_mode="HTML")

    buyer_m = f"@{u['username']}" if u.get('username') else f"ID: {uid}"
    log_ch = get_setting('log_channel')
    if log_ch and log_ch != "Not Set":
        try: bot.send_message(log_ch, f"🛍 <b>New Purchase!</b>\nUser: {buyer_m}\nProduct: <b>{clean_name(p['name_en'])}</b>\nQty: {qty}\nTotal Price: ${total_price:.2f}", parse_mode="HTML")
        except: pass

    buy_cnt = len(supabase.table('orders').select('id').eq('user_id', uid).execute().data)
    if buy_cnt == qty and u['referred_by']:
        ref_id = int(u['referred_by'])
        ref_u = get_user_data_full(ref_id)
        if ref_u:
            supabase.table('users').update({'balance': ref_u['balance'] + REFERRAL_REWARD}).eq('user_id', ref_id).execute()
            if log_ch and log_ch != "Not Set":
                ref_m = f"@{ref_u['username']}" if ref_u.get('username') else f"ID: {ref_id}"
                try: bot.send_message(log_ch, f"🎁 <b>Referral Reward!</b>\nUser {ref_m} earned $0.10 from {buyer_m}", parse_mode="HTML")
                except: pass

# ============================================================
# 🏦 9. بوابات الدفع (LTC Blockchain + Binance)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_deposit")
def dep_init_ui(call):
    l = get_user_data_full(call.from_user.id)['lang']
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("🟡 Binance Pay", callback_data="dep_binance"))
    markup.add(InlineKeyboardButton("🟢 USDT (TRC20/BEP20)", callback_data="dep_crypto_USDT"))
    markup.add(InlineKeyboardButton("🔵 Litecoin (LTC)", callback_data="dep_crypto_LTC"))
    markup.add(InlineKeyboardButton(LANG[l]['back'], callback_data="open_profile"))
    bot.edit_message_text(LANG[l]['dep_choose'], call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "dep_binance")
def dep_binance_ui(call):
    uid = call.from_user.id; l = get_user_data_full(uid)['lang']
    wallet = get_setting('wallet_address')
    msg = bot.send_message(uid, LANG[l]['dep_pay'].format(wallet), parse_mode="HTML")
    bot.register_next_step_handler(msg, verify_binance_pay, l)

@bot.callback_query_handler(func=lambda call: call.data.startswith("dep_crypto_"))
def dep_crypto_ui(call):
    uid = call.from_user.id; l = get_user_data_full(uid)['lang']; coin = call.data.split('_')[2]
    db_key = "usdt_address" if coin == "USDT" else "ltc_address"
    wallet = get_setting(db_key)
    msg = bot.send_message(uid, LANG[l]['dep_crypto'].format(coin, wallet), parse_mode="HTML")
    
    if coin == "LTC":
        bot.register_next_step_handler(msg, verify_ltc_public_blockchain, l, wallet)
    else:
        bot.register_next_step_handler(msg, verify_crypto_tx, l, coin)

def verify_binance_pay(message, lang):
    uid = message.from_user.id; tx_id = message.text.strip().lower()
    try:
        if supabase.table('used_transactions').select('*').eq('transaction_id', tx_id).execute().data:
            bot.reply_to(message, LANG[lang]['tx_used']); return
        found = False; amt = 0.0; now = int(time.time() * 1000)
        pay_h = binance_client.get_pay_trade_history().get('data', [])
        for d in pay_h:
            api_txid = str(d.get('orderId', '')).lower()
            if tx_id == api_txid:
                if (now - int(d.get('transactionTime', 0))) > (24 * 60 * 60 * 1000):
                    bot.send_message(uid, LANG[lang]['time_error'], parse_mode="HTML"); return
                found = True; amt = float(d.get('amount', 0.0)); break
        if found: credit_user(uid, amt, tx_id, lang)
        else: bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML")
    except: pass

def verify_crypto_tx(message, lang, coin):
    uid = message.from_user.id; tx_id = message.text.strip().lower()
    try:
        if supabase.table('used_transactions').select('*').eq('transaction_id', tx_id).execute().data:
            bot.reply_to(message, LANG[lang]['tx_used']); return
        bot.send_message(uid, LANG[lang]['crypto_checking'], parse_mode="HTML")
        res = binance_client.get_deposit_history(coin=coin)
        
        found = False; status = -1; amt = 0.0; now = int(time.time() * 1000)
        for d in res:
            api_txid = str(d.get('txId', '')).lower()
            if tx_id == api_txid or tx_id in api_txid:
                insert_time = int(d.get('insertTime', 0))
                if (now - insert_time) > (24 * 60 * 60 * 1000):
                    bot.send_message(uid, LANG[lang]['time_error'], parse_mode="HTML"); return
                found = True; status = int(d.get('status', -1)); amt = float(d.get('amount', 0.0)); break
                
        if found:
            if status == 1: credit_user(uid, amt, tx_id, lang)
            else: bot.send_message(uid, LANG[lang]['dep_pending'], parse_mode="HTML")
        else: bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML")
    except: bot.send_message(uid, LANG[lang]['crypto_error'], parse_mode="HTML")

def verify_ltc_public_blockchain(message, lang, wallet_address):
    uid = message.from_user.id; tx_id = message.text.strip()
    try:
        if supabase.table('used_transactions').select('*').eq('transaction_id', tx_id).execute().data:
            bot.reply_to(message, LANG[lang]['tx_used']); return
            
        bot.send_message(uid, LANG[lang]['crypto_checking'], parse_mode="HTML")
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
                        
                    credit_user(uid, usd_amount, tx_id, lang)
                else: bot.send_message(uid, LANG[lang]['dep_pending'], parse_mode="HTML")
            else: bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML")
        else: bot.send_message(uid, LANG[lang]['dep_fail'], parse_mode="HTML")
            
    except Exception as e:
        logger.error(f"LTC Error: {e}")
        bot.send_message(uid, LANG[lang]['crypto_error'], parse_mode="HTML")

def credit_user(uid, amt, tx_id, lang):
    db_user = get_user_data_full(uid)
    supabase.table('users').update({'balance': db_user['balance'] + amt}).eq('user_id', uid).execute()
    supabase.table('used_transactions').insert({'transaction_id': tx_id, 'amount': amt, 'user_id': uid}).execute()
    bot.send_message(uid, LANG[lang]['dep_success'].format(amt), parse_mode="HTML")

# ============================================================
# 👑 10. لوحة الإدارة (إضافة منتج بترجمة تلقائية)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "admin_panel_main")
def admin_main_ui(call):
    u = get_user_data_full(call.from_user.id)
    l = u['lang'] if u and 'lang' in u else 'ar'
    
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
        markup.add(InlineKeyboardButton("⚙️ Settings", callback_data="ad_shop_settings"))
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
        markup.add(InlineKeyboardButton("⚙️ إعدادات المتجر", callback_data="ad_shop_settings"))
        markup.add(InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="main_menu_refresh"))
        text = "👑 <b>لوحة القيادة (الإدارة):</b>"
        
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

# --- الترجمة التلقائية عند إضافة منتج ---
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
        supabase.table('products').insert({'name_ar': p['n_ar'], 'name_en': p['n_en'], 'desc_ar': p['d_ar'], 'desc_en': p['d_en'], 'price': price}).execute()
        bot.send_message(uid, "✅ تم إضافة المنتج بنجاح!")
    except: bot.send_message(uid, "❌ خطأ في السعر.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_new_admin")
def admin_add_admin_start(call):
    msg = bot.send_message(call.from_user.id, "👑 Send <b>ID</b> or <b>@username</b>:")
    bot.register_next_step_handler(msg, admin_add_admin_save)

def admin_add_admin_save(message):
    target = message.text.strip()
    u = None
    if target.startswith('@') or not target.replace('-', '').isdigit():
        res = supabase.table('users').select('*').eq('username', target.replace('@', '').lower()).execute()
        if res.data: u = res.data[0]
    else: u = get_user_data_full(target)
    if u:
        supabase.table('users').update({'is_admin': 1}).eq('user_id', u['user_id']).execute()
        bot.send_message(message.chat.id, "✅ User promoted.")
    else: bot.send_message(message.chat.id, "❌ Not found.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_gift")
def ad_gift_start(call):
    msg = bot.send_message(call.from_user.id, "👤 <b>Send User ID or @username:</b>")
    bot.register_next_step_handler(msg, ad_gift_val)

def ad_gift_val(message):
    target = message.text.strip()
    u = None
    if target.startswith('@') or not target.replace('-', '').isdigit():
        res = supabase.table('users').select('*').eq('username', target.replace('@', '').lower()).execute()
        if res.data: u = res.data[0]
    else: u = get_user_data_full(target)
    if u:
        msg = bot.send_message(message.from_user.id, f"💰 Amount for {u['name']}:")
        bot.register_next_step_handler(msg, ad_gift_finish, u['user_id'])
    else: bot.send_message(message.chat.id, "❌ Not found.")

def ad_gift_finish(message, tid):
    try:
        val = float(message.text); u = get_user_data_full(tid)
        supabase.table('users').update({'balance': u['balance'] + val}).eq('user_id', tid).execute()
        bot.send_message(message.from_user.id, "✅ Done.")
    except: bot.send_message(message.from_user.id, "❌ Error.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_edit")
def admin_edit_list(call):
    prods = supabase.table('products').select('*').execute().data
    markup = InlineKeyboardMarkup(row_width=1)
    for p in prods: markup.add(InlineKeyboardButton(f"📝 {clean_name(p['name_en'])}", callback_data=f"edit_p_{p['id']}"))
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
    if field == "price": val = float(val)
    supabase.table('products').update({keys[field]: val}).eq('id', pid).execute()
    bot.send_message(message.chat.id, "✅ Updated.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_del")
def admin_del_list(call):
    prods = supabase.table('products').select('*').execute().data
    markup = InlineKeyboardMarkup(row_width=1)
    for p in prods: markup.add(InlineKeyboardButton(f"🗑 {clean_name(p['name_en'])}", callback_data=f"del_p_{p['id']}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    bot.edit_message_text("👇 Select Product to Delete:", call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_p_"))
def admin_del_exec(call):
    pid = call.data.split('_')[2]
    try:
        supabase.table('product_stock').delete().eq('product_id', pid).execute()
        supabase.table('orders').delete().eq('product_id', pid).execute()
        supabase.table('products').delete().eq('id', pid).execute()
        bot.answer_callback_query(call.id, "✅ Deleted Successfully!", show_alert=True)
        admin_main_ui(call)
    except Exception as e:
        bot.answer_callback_query(call.id, "❌ Error occurred.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == "ad_s_fill")
def admin_stock_list_ui(call):
    prods = supabase.table('products').select('*').execute().data
    markup = InlineKeyboardMarkup(row_width=1)
    for p in prods: markup.add(InlineKeyboardButton(f"📦 {clean_name(p['name_en'])}", callback_data=f"stk_add_{p['id']}"))
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
            supabase.table('product_stock').insert({'product_id': pid, 'code_line': l.strip()}).execute()
            count += 1
            
    p_res = supabase.table('products').select('name_en').eq('id', pid).execute().data
    if p_res:
        p_name = clean_name(p_res[0]['name_en'])
        stk_total = get_product_stock_count(pid)
        alert_msg = f"🔔 <b>New Stock Added!</b>\n\n🛍 <b>Product:</b> {p_name}\n📦 <b>Available Now:</b> {stk_total}\n\n<i>Hurry up and grab yours!</i>"
        users = supabase.table('users').select('user_id').execute().data
        for u in users:
            try: bot.send_message(u['user_id'], alert_msg, parse_mode="HTML"); time.sleep(0.05)
            except: continue

    bot.send_message(message.chat.id, f"✅ <b>{count} Codes added! Broadcast sent.</b>", parse_mode="HTML")

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
    supabase.table('settings').upsert({'key': keys[mode], 'value': val}).execute()
    bot.send_message(message.chat.id, "✅ Updated.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_logs_all")
def admin_all_logs(call):
    recs = supabase.table('used_transactions').select('*').order('id', desc=True).limit(10).execute().data
    txt = "📜 <b>Last 10 Deposits:</b>\n\n"
    if not recs: txt = "📭 No records."
    for r in recs: txt += f"👤 <code>{r['user_id']}</code> | 💰 <b>${r['amount']}</b> | 🆔 <code>{r['transaction_id']}</code>\n"
    markup = InlineKeyboardMarkup(); markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_bc")
def admin_bc_init(call):
    msg = bot.send_message(call.from_user.id, "📢 Send Broadcast Message:")
    bot.register_next_step_handler(msg, admin_bc_exe)

def admin_bc_exe(message):
    users = supabase.table('users').select('user_id').execute().data
    for u in users:
        try: bot.copy_message(u['user_id'], message.chat.id, message.message_id); time.sleep(0.05)
        except: continue
    bot.send_message(message.chat.id, "✅ Broadcast Sent.")

@bot.callback_query_handler(func=lambda call: call.data == "toggle_language")
def toggle_lang(call):
    uid = call.from_user.id; u = get_user_data_full(uid)
    new_l = 'en' if u['lang'] == 'ar' else 'ar'
    supabase.table('users').update({'lang': new_l}).eq('user_id', uid).execute()
    bot.delete_message(call.message.chat.id, call.message.message_id)
    start_handler(call)

@bot.callback_query_handler(func=lambda call: call.data == "main_menu_refresh")
def refresh_main(call):
    bot.delete_message(call.message.chat.id, call.message.message_id)
    start_handler(call)

# ============================================================
# 🚀 تشغيل البوت
# ============================================================
def run_bot():
    while True:
        try:
            bot.infinity_polling(timeout=90, long_polling_timeout=5)
        except Exception as e:
            logger.error(f"Polling Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_bot()
