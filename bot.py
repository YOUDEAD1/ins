import telebot
from telebot import types
from instagrapi import Client
import time
import os
import threading
from datetime import datetime
from pymongo import MongoClient
from flask import Flask

# ---------------- 1. إعدادات السيرفر الوهمي (لـ Render) ----------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is alive!"

def run_web_server():
    # ريندر يعطينا البورت في متغير بيئة، وإذا لم نكن في ريندر نستخدم 8080
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

# تشغيل السيرفر في خيط منفصل فوراً عند تشغيل الملف
t_server = threading.Thread(target=run_web_server)
t_server.start()

# ---------------- 2. إعدادات البوت وقاعدة البيانات ----------------

# جلب المعلومات الحساسة من متغيرات البيئة (سنضيفها في موقع ريندر)
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

# التأكد من وجود البيانات
if not BOT_TOKEN or not MONGO_URL:
    print("Error: BOT_TOKEN or MONGO_URL not found in environment variables!")

# الاتصال بـ MongoDB
cluster = MongoClient(MONGO_URL)
db = cluster["telegram_bot_db"]
collection = db["users_data"]

bot = telebot.TeleBot(BOT_TOKEN)

# متغيرات التشغيل (في الذاكرة المؤقتة)
active_stats = {} 
stop_flags = {}

# ---------------- 3. دوال التعامل مع MongoDB ----------------

def get_user_data(chat_id):
    """جلب بيانات المستخدم من المونغو"""
    user = collection.find_one({"_id": str(chat_id)})
    if user:
        return user
    else:
        return {}

def update_user_data(chat_id, key, value):
    """تحديث أو إنشاء بيانات المستخدم"""
    str_id = str(chat_id)
    # نستخدم upsert=True لإنشاء الوثيقة إذا لم تكن موجودة
    collection.update_one(
        {"_id": str_id},
        {"$set": {key: value}},
        upsert=True
    )

def clear_user_data(chat_id):
    """حذف بيانات السيزن والجروبات (تسجيل خروج)"""
    # لا نحذف المستند بالكامل، بل نفرغ الحقول فقط
    collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {"session_id": None, "groups": [], "selected_ids": []}}
    )

# ---------------- 4. القوائم والمنطق (نفس منطق البوت السابق) ----------------

def get_main_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    btn_login = types.InlineKeyboardButton("🔑 تسجيل دخول", callback_data="main_login")
    btn_groups = types.InlineKeyboardButton("📂 إدارة الجروبات", callback_data="main_groups")
    btn_post = types.InlineKeyboardButton("🚀 بدء النشر", callback_data="main_post")
    btn_stats = types.InlineKeyboardButton("📊 الإحصائيات", callback_data="main_stats")
    btn_stop = types.InlineKeyboardButton("⛔ إيقاف النشر", callback_data="main_stop")
    markup.add(btn_login, btn_groups)
    markup.add(btn_post, btn_stats)
    markup.add(btn_stop)
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "جاري الاتصال بالسيرفر...", reply_markup=types.ReplyKeyboardRemove())
    bot.send_message(
        message.chat.id, 
        "👋 **بوت النشر التلقائي (MongoDB + Render)**\n\nكل مستخدم له حساب خاص وبيانات منفصلة.", 
        reply_markup=get_main_menu(),
        parse_mode="Markdown"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("main_"))
def handle_main_menu(call):
    chat_id = call.message.chat.id
    action = call.data
    user_data = get_user_data(chat_id)
    
    if action == "main_login":
        if user_data.get("session_id"):
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("نعم، خروج", callback_data="confirm_logout"),
                       types.InlineKeyboardButton("إلغاء", callback_data="cancel_action"))
            bot.send_message(chat_id, "⚠️ أنت مسجل بالفعل! هل تريد تغيير الحساب؟", reply_markup=markup)
        else:
            msg = bot.send_message(chat_id, "📥 **أرسل كود السيزن (Session ID):**")
            bot.register_next_step_handler(msg, process_login)
        
    elif action == "main_groups":
        show_groups_menu(chat_id, call.message.message_id)
        
    elif action == "main_post":
        if active_stats.get(chat_id, {}).get('status') == 'Running':
            bot.answer_callback_query(call.id, "البوت يعمل بالفعل!")
        else:
            start_post_process(chat_id)
            
    elif action == "main_stats":
        show_stats(chat_id)
        
    elif action == "main_stop":
        stop_flags[chat_id] = True
        bot.answer_callback_query(call.id, "تم طلب الإيقاف")
        if chat_id in active_stats: active_stats[chat_id]['status'] = "Stopping..."

@bot.callback_query_handler(func=lambda call: call.data == "confirm_logout")
def confirm_logout(call):
    chat_id = call.message.chat.id
    clear_user_data(chat_id)
    bot.answer_callback_query(call.id, "تم الخروج")
    msg = bot.send_message(chat_id, "📥 **أرسل كود السيزن الجديد:**")
    bot.register_next_step_handler(msg, process_login)

@bot.callback_query_handler(func=lambda call: call.data == "cancel_action")
def cancel_action(call):
    bot.delete_message(call.message.chat.id, call.message.message_id)

def process_login(message):
    if message.text.startswith("/"): return
    session_id = message.text
    chat_id = message.chat.id
    msg_wait = bot.send_message(chat_id, "⏳ جاري الفحص...")
    
    try:
        cl = Client()
        cl.login_by_sessionid(session_id)
        threads = cl.direct_threads(amount=50)
        groups_list = []
        for thread in threads:
            if thread.is_group: 
                t_name = thread.thread_title if thread.thread_title else "بدون اسم"
                groups_list.append({"id": thread.id, "name": t_name})
            
        update_user_data(chat_id, "session_id", session_id)
        update_user_data(chat_id, "groups", groups_list)
        update_user_data(chat_id, "selected_ids", []) # تصفير الاختيارات
        
        bot.delete_message(chat_id, msg_wait.message_id)
        bot.send_message(chat_id, f"✅ تم الحفظ! لديك {len(groups_list)} جروب.", reply_markup=get_main_menu())
    except Exception as e:
        bot.send_message(chat_id, f"❌ خطأ: {e}", reply_markup=get_main_menu())

def show_groups_menu(chat_id, message_id=None):
    user_data = get_user_data(chat_id)
    groups = user_data.get("groups", [])
    selected_ids = user_data.get("selected_ids", [])
    
    if not groups:
        bot.send_message(chat_id, "⚠️ لا توجد جروبات.")
        return

    markup = types.InlineKeyboardMarkup(row_width=1)
    for group in groups:
        is_selected = group['id'] in selected_ids
        icon = "✅" if is_selected else "⬜"
        markup.add(types.InlineKeyboardButton(f"{icon} {group['name']}", callback_data=f"toggle|{group['id']}"))
    
    markup.row(types.InlineKeyboardButton("الكل", callback_data="cmd|all"), types.InlineKeyboardButton("لا شيء", callback_data="cmd|none"))
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data="cmd|back"))

    text = f"👇 لديك {len(groups)} جروب. اختر:"
    if message_id:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
    else:
        bot.send_message(chat_id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("toggle|") or call.data.startswith("cmd|"))
def handle_group_clicks(call):
    chat_id = call.message.chat.id
    data = call.data
    user_data = get_user_data(chat_id)
    selected_ids = user_data.get("selected_ids", [])
    groups = user_data.get("groups", [])
    
    need_refresh = False
    if data.startswith("toggle|"):
        gid = data.split("|")[1]
        if gid in selected_ids: selected_ids.remove(gid)
        else: selected_ids.append(gid)
        need_refresh = True
    elif data == "cmd|all":
        selected_ids = [g['id'] for g in groups]
        need_refresh = True
    elif data == "cmd|none":
        selected_ids = []
        need_refresh = True
    elif data == "cmd|back":
        update_user_data(chat_id, "selected_ids", selected_ids)
        bot.edit_message_text("🏠 القائمة الرئيسية:", chat_id, call.message.message_id, reply_markup=get_main_menu())
        return

    if need_refresh:
        update_user_data(chat_id, "selected_ids", selected_ids)
        try:
            markup = types.InlineKeyboardMarkup(row_width=1)
            for group in groups:
                is_selected = group['id'] in selected_ids
                icon = "✅" if is_selected else "⬜"
                markup.add(types.InlineKeyboardButton(f"{icon} {group['name']}", callback_data=f"toggle|{group['id']}"))
            markup.row(types.InlineKeyboardButton("الكل", callback_data="cmd|all"), types.InlineKeyboardButton("لا شيء", callback_data="cmd|none"))
            markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data="cmd|back"))
            bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
        except: pass

def start_post_process(chat_id):
    user_data = get_user_data(chat_id)
    if not user_data.get("session_id") or not user_data.get("selected_ids"):
        bot.send_message(chat_id, "⚠️ تأكد من تسجيل الدخول واختيار الجروبات.")
        return
    msg = bot.send_message(chat_id, "📝 **أرسل الرسالة:**")
    bot.register_next_step_handler(msg, ask_time)

def ask_time(message):
    msg_text = message.text
    msg = bot.reply_to(message, "⏱ **كم دقيقة الانتظار؟**")
    bot.register_next_step_handler(msg, run_loop, msg_text)

def run_loop(message, msg_text):
    try:
        minutes = float(message.text)
        delay = minutes * 60
    except:
        bot.reply_to(message, "❌ رقم خطأ.")
        return
        
    chat_id = message.chat.id
    stop_flags[chat_id] = False
    active_stats[chat_id] = {'status': 'Running', 'round': 0, 'msg': msg_text, 'interval': minutes, 'start': datetime.now().strftime("%H:%M")}
    
    bot.send_message(chat_id, "🚀 تم التشغيل!", reply_markup=get_main_menu())
    t = threading.Thread(target=background_sender, args=(chat_id, msg_text, delay))
    t.start()

def background_sender(chat_id, text, wait_time):
    user_data = get_user_data(chat_id)
    all_groups = user_data.get("groups", [])
    selected_ids = user_data.get("selected_ids", [])
    session = user_data.get("session_id")
    
    targets = [g for g in all_groups if g['id'] in selected_ids]
    
    try:
        cl = Client()
        cl.login_by_sessionid(session)
    except:
        active_stats[chat_id]['status'] = 'Login Error'
        return

    round_num = 1
    while not stop_flags.get(chat_id, False):
        active_stats[chat_id]['round'] = round_num
        active_stats[chat_id]['status'] = 'Sending... 📤'
        
        for g in targets:
            if stop_flags.get(chat_id, False): break
            try:
                cl.direct_send(text, thread_ids=[g['id']])
                time.sleep(2)
            except: pass
            
        if stop_flags.get(chat_id, False): break
        
        active_stats[chat_id]['status'] = 'Sleeping 💤'
        slept = 0
        while slept < wait_time:
            if stop_flags.get(chat_id, False): break
            time.sleep(5)
            slept += 5
        round_num += 1
    
    active_stats[chat_id]['status'] = 'Stopped 🛑'

def show_stats(chat_id):
    stats = active_stats.get(chat_id)
    if not stats:
        bot.send_message(chat_id, "📭 لا يوجد نشر.")
        return
    text = f"📊 الحالة: {stats['status']}\n🔢 جولة: {stats['round']}\n⏱ كل: {stats['interval']}د"
    bot.send_message(chat_id, text)

print("Bot is ready for Render...")
bot.infinity_polling()
