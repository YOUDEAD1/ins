import telebot
from telebot import types
from instagrapi import Client
from instagrapi.exceptions import (
    FeedbackRequired, ChallengeRequired, 
    PleaseWaitFewMinutes, RateLimitError,
    LoginRequired
)
import time
import os
import threading
import shutil
import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from flask import Flask

# ==========================================
# 1. إعدادات السيرفر والاتصال (Render + MongoDB)
# ==========================================

app = Flask(__name__)

@app.route('/')
def home():
    return "🔥 The Ultimate Bot is Running (Smart Login/Logout)!"

def run_web_server():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

t_server = threading.Thread(target=run_web_server)
t_server.start()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

if not BOT_TOKEN or not MONGO_URL:
    print("❌ تنبيه: بيانات الاتصال ناقصة!")

cluster = MongoClient(MONGO_URL)
db = cluster["telegram_bot_db"]
users_collection = db["users_data"]        
follows_collection = db["follows_history"] 

bot = telebot.TeleBot(BOT_TOKEN)

active_stats = {}  
stop_flags = {}    
auto_reply_active = {} 

# ==========================================
# 2. دوال التعامل مع قاعدة البيانات
# ==========================================

def get_user_data(chat_id):
    user = users_collection.find_one({"_id": str(chat_id)})
    return user if user else {}

def update_user_data(chat_id, key, value):
    users_collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {key: value}},
        upsert=True
    )

def logout_user(chat_id):
    """حذف بيانات الجلسة تماماً"""
    users_collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {"session_id": None, "groups": [], "selected_ids": []}}
    )

def log_follow(chat_id, target_user_id):
    follows_collection.insert_one({
        "chat_id": str(chat_id),
        "target_id": str(target_user_id),
        "date": datetime.now()
    })

def get_old_follows(chat_id):
    cutoff = datetime.now() - timedelta(hours=48)
    return list(follows_collection.find({
        "chat_id": str(chat_id),
        "date": {"$lt": cutoff}
    }))

def remove_follow_log(chat_id, target_user_id):
    follows_collection.delete_one({"chat_id": str(chat_id), "target_id": str(target_user_id)})

# ==========================================
# 3. القوائم ولوحة التحكم
# ==========================================

def get_main_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    # === أزرار الحساب (تسجيل دخول / خروج) ===
    btn_login = types.InlineKeyboardButton("🔑 تسجيل دخول", callback_data="main_login")
    btn_logout = types.InlineKeyboardButton("🔴 تسجيل خروج", callback_data="main_logout")
    
    btn_groups = types.InlineKeyboardButton("📂 إدارة الجروبات", callback_data="main_groups")
    btn_post = types.InlineKeyboardButton("📨 نشر خاص", callback_data="main_post_dm")
    btn_stats = types.InlineKeyboardButton("📊 الإحصائيات", callback_data="main_stats")
    
    btn_story = types.InlineKeyboardButton("📸 نشر ستوري", callback_data="main_story")
    btn_reply = types.InlineKeyboardButton("🗣 تفعيل الرد التلقائي", callback_data="main_auto_reply")
    btn_stop_reply = types.InlineKeyboardButton("🔕 إيقاف الرد", callback_data="main_stop_reply")
    
    btn_follow = types.InlineKeyboardButton("➕ متابعة (0.5s)", callback_data="main_follow")
    btn_mass_unfollow = types.InlineKeyboardButton("🔥 حذف غير المتابعين", callback_data="main_mass_unfollow")
    btn_stop = types.InlineKeyboardButton("⛔ إيقاف الكل", callback_data="main_stop")
    
    markup.add(btn_login, btn_logout) # وضعناهم بجانب بعض
    markup.add(btn_groups)
    markup.add(btn_post, btn_stats)
    markup.add(btn_reply, btn_stop_reply)
    markup.add(btn_follow, btn_mass_unfollow)
    markup.add(btn_stop)
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(
        message.chat.id, 
        "👋 **مرحباً بك في البوت الشامل (Smart Login)**\n"
        "الآن يمكنك تسجيل الخروج أو تغيير السيزن التالف بسهولة.", 
        reply_markup=get_main_menu()
    )

# ==========================================
# 4. معالج الأزرار الرئيسي
# ==========================================

@bot.callback_query_handler(func=lambda call: call.data.startswith("main_"))
def handle_main_menu(call):
    chat_id = call.message.chat.id
    action = call.data
    user_data = get_user_data(chat_id)
    session = user_data.get("session_id")
    
    # 1. تسجيل الدخول الذكي (Smart Login)
    if action == "main_login":
        if session:
            # إذا وجدنا سيزن، نفحصه أولاً
            bot.answer_callback_query(call.id, "جاري فحص السيزن الحالي...")
            try:
                cl = Client()
                cl.login_by_sessionid(session)
                # إذا نجح الاتصال، يعني السيزن صالح
                bot.send_message(chat_id, "✅ **أنت مسجل بالفعل والسيزن يعمل!**\nإذا أردت تغييره، اضغط على 'تسجيل خروج' أولاً.")
            except Exception as e:
                # إذا فشل الاتصال، يعني السيزن تالف
                bot.send_message(chat_id, "⚠️ **السيزن القديم تالف أو منتهي الصلاحية.**\n📥 أرسل كود السيزن الجديد الآن:")
                logout_user(chat_id) # نحذف القديم تلقائياً
                bot.register_next_step_handler(call.message, process_login)
        else:
            # لا يوجد سيزن أصلاً
            msg = bot.send_message(chat_id, "📥 **أرسل كود السيزن (Session ID):**")
            bot.register_next_step_handler(msg, process_login)

    # 2. تسجيل الخروج (Logout)
    elif action == "main_logout":
        if session:
            logout_user(chat_id)
            bot.answer_callback_query(call.id, "تم الخروج بنجاح")
            bot.send_message(chat_id, "✅ **تم تسجيل الخروج وحذف البيانات.**\nيمكنك تسجيل الدخول بحساب جديد الآن.", reply_markup=get_main_menu())
        else:
            bot.answer_callback_query(call.id, "أنت غير مسجل أصلاً!")

    # 3. الجروبات والنشر
    elif action == "main_groups":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        show_groups_menu(chat_id, call.message.message_id)

    elif action == "main_post_dm":
        if not session or not user_data.get("selected_ids"):
            return bot.answer_callback_query(call.id, "سجل دخول واختر الجروبات!")
        msg = bot.send_message(chat_id, "📝 **أرسل الرسالة التي تريد نشرها:**")
        bot.register_next_step_handler(msg, ask_time_for_dm)

    elif action == "main_stats":
        show_stats(chat_id)

    # 4. الميزات
    elif action == "main_story":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        os.makedirs(f"downloads/{chat_id}", exist_ok=True)
        msg = bot.send_message(chat_id, "📸 **أرسل الصور الآن.**\nعند الانتهاء اكتب 'تم' أو اضغط /done")
        bot.register_next_step_handler(msg, collect_photos)

    elif action == "main_auto_reply":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "✍️ **أرسل نص الرد:**\n(سيرد البوت فقط على من يرد عليك)")
        bot.register_next_step_handler(msg, start_auto_reply_thread)

    elif action == "main_stop_reply":
        if chat_id in auto_reply_active and auto_reply_active[chat_id]:
            auto_reply_active[chat_id] = False
            bot.send_message(chat_id, "🔕 **تم إيقاف الرد التلقائي.**")
        else:
            bot.answer_callback_query(call.id, "متوقف بالفعل.")

    elif action == "main_follow":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        start_smart_follow_thread(chat_id, session)
        bot.answer_callback_query(call.id, "تم بدء المتابعة")

    elif action == "main_mass_unfollow":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "🔢 **كم شخص تريد حذفه؟** (اكتب الرقم):")
        bot.register_next_step_handler(msg, ask_unfollow_count)

    elif action == "main_stop":
        stop_flags[chat_id] = True
        auto_reply_active[chat_id] = False
        bot.answer_callback_query(call.id, "🛑 تم الإيقاف")
        if chat_id in active_stats: active_stats[chat_id]['status'] = "Stopping..."

# ==========================================
# 5. منطق الميزات (Functions)
# ==========================================

# --- تسجيل الدخول والجروبات ---
def process_login(message):
    session_id = message.text
    chat_id = message.chat.id
    msg_wait = bot.send_message(chat_id, "⏳ جاري التحقق من السيزن...")
    try:
        cl = Client()
        cl.login_by_sessionid(session_id)
        threads = cl.direct_threads(amount=40)
        groups_list = [{"id": t.id, "name": t.thread_title or "بدون اسم"} for t in threads if t.is_group]
        
        update_user_data(chat_id, "session_id", session_id)
        update_user_data(chat_id, "groups", groups_list)
        update_user_data(chat_id, "selected_ids", [])
        
        bot.delete_message(chat_id, msg_wait.message_id)
        bot.send_message(chat_id, f"✅ **تم تسجيل الدخول بنجاح!**\nوجدنا {len(groups_list)} جروب.", reply_markup=get_main_menu())
    except Exception as e:
        bot.delete_message(chat_id, msg_wait.message_id)
        bot.send_message(chat_id, f"❌ **فشل الدخول!**\nالسيزن غير صالح، تأكد منه وحاول مجدداً.\nالخطأ: {e}")

def show_groups_menu(chat_id, message_id=None):
    user_data = get_user_data(chat_id)
    groups = user_data.get("groups", [])
    selected_ids = user_data.get("selected_ids", [])
    markup = types.InlineKeyboardMarkup(row_width=1)
    for group in groups:
        is_selected = group['id'] in selected_ids
        icon = "✅" if is_selected else "⬜"
        markup.add(types.InlineKeyboardButton(f"{icon} {group['name']}", callback_data=f"toggle|{group['id']}"))
    markup.row(types.InlineKeyboardButton("الكل", callback_data="cmd|all"), types.InlineKeyboardButton("لا شيء", callback_data="cmd|none"))
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data="cmd|back"))
    text = f"👇 اختر الجروبات ({len(selected_ids)}/{len(groups)}):"
    if message_id: bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
    else: bot.send_message(chat_id, text, reply_markup=markup)

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
        try: show_groups_menu(chat_id, call.message.message_id)
        except: pass

def ask_time_for_dm(message):
    msg_text = message.text
    msg = bot.reply_to(message, "⏱ **كم دقيقة الانتظار بين كل جولة؟**")
    bot.register_next_step_handler(msg, start_dm_loop, msg_text)

def start_dm_loop(message, msg_text):
    try: minutes = float(message.text); delay = minutes * 60
    except: return bot.reply_to(message, "❌ أرقام فقط.")
    chat_id = message.chat.id
    stop_flags[chat_id] = False
    active_stats[chat_id] = {'status': 'Running', 'round': 0, 'interval': minutes}
    bot.send_message(chat_id, "🚀 تم التشغيل!", reply_markup=get_main_menu())
    threading.Thread(target=background_dm_sender, args=(chat_id, msg_text, delay)).start()

def background_dm_sender(chat_id, text, wait_time):
    user_data = get_user_data(chat_id)
    session = user_data.get("session_id")
    selected = user_data.get("selected_ids", [])
    try: cl = Client(); cl.login_by_sessionid(session)
    except: active_stats[chat_id]['status'] = 'Login Failed'; return
    round_num = 1
    while not stop_flags.get(chat_id, False):
        active_stats[chat_id]['round'] = round_num
        active_stats[chat_id]['status'] = 'Sending... 📤'
        for gid in selected:
            if stop_flags.get(chat_id, False): break
            try: cl.direct_send(text, thread_ids=[gid]); time.sleep(3)
            except: pass
        if stop_flags.get(chat_id, False): break
        active_stats[chat_id]['status'] = 'Sleeping 💤'
        time.sleep(wait_time)
        round_num += 1
    active_stats[chat_id]['status'] = 'Stopped 🛑'

def show_stats(chat_id):
    stats = active_stats.get(chat_id)
    if not stats: bot.send_message(chat_id, "📭 لا يوجد نشر.")
    else: bot.send_message(chat_id, f"📊 الحالة: {stats['status']}\n🔢 الجولة: {stats.get('round', 0)}")

# --- الستوري ---
def collect_photos(message):
    chat_id = message.chat.id
    if message.text in ['/done', 'تم']:
        bot.send_message(chat_id, "🚀 جاري الرفع...")
        session = get_user_data(chat_id).get("session_id")
        threading.Thread(target=run_story_uploader, args=(chat_id, session)).start()
        return
    if message.photo:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        path = f"downloads/{chat_id}/{datetime.now().timestamp()}.jpg"
        with open(path, 'wb') as f: f.write(downloaded_file)
        bot.reply_to(message, "✅ تم الحفظ. التالي...")
        bot.register_next_step_handler(message, collect_photos)

def run_story_uploader(chat_id, session):
    folder = f"downloads/{chat_id}"
    try:
        cl = Client(); cl.login_by_sessionid(session)
        for img in os.listdir(folder):
            cl.photo_upload_to_story(os.path.join(folder, img))
            time.sleep(5)
        bot.send_message(chat_id, "🎉 تم النشر!")
    except Exception as e: bot.send_message(chat_id, f"❌ خطأ: {e}")
    finally: shutil.rmtree(folder, ignore_errors=True)

# --- الرد التلقائي ---
def start_auto_reply_thread(message):
    text = message.text; chat_id = message.chat.id
    session = get_user_data(chat_id).get("session_id")
    stop_flags[chat_id] = False
    auto_reply_active[chat_id] = True
    threading.Thread(target=run_auto_reply, args=(chat_id, session, text)).start()
    bot.send_message(chat_id, "✅ تم التفعيل.", reply_markup=get_main_menu())

def run_auto_reply(chat_id, session, text):
    try:
        cl = Client(); cl.login_by_sessionid(session)
        my_id = cl.user_id
        replied_cache = [] 
        print(f"✅ Auto Reply Started for {chat_id}")
        while not stop_flags.get(chat_id, False) and auto_reply_active.get(chat_id, False):
            try:
                threads = cl.direct_threads(amount=20)
                for t in threads:
                    if t.is_group:
                        last_msg = t.messages[0]
                        if last_msg.id in replied_cache: continue
                        if last_msg.user_id == my_id: continue
                        
                        is_reply_to_me = False
                        try:
                            if last_msg.reply_to_message and last_msg.reply_to_message.user_id == my_id:
                                is_reply_to_me = True
                            elif hasattr(last_msg, 'replied_to_message') and last_msg.replied_to_message:
                                if last_msg.replied_to_message.user_id == my_id:
                                    is_reply_to_me = True
                        except: pass
                        
                        if is_reply_to_me:
                            cl.direct_send(text, thread_ids=[t.id])
                            replied_cache.append(last_msg.id)
                            if len(replied_cache) > 100: replied_cache.pop(0)
                            time.sleep(2)
            except: time.sleep(5)
            time.sleep(15)
    except: pass

# --- المتابعة والحذف ---
def start_smart_follow_thread(chat_id, session):
    stop_flags[chat_id] = False
    threading.Thread(target=run_smart_follow, args=(chat_id, session)).start()

def run_smart_follow(chat_id, session):
    try:
        cl = Client(); cl.login_by_sessionid(session)
        target_id = "460563723" 
        users = cl.user_followers(target_id, amount=200)
        for uid in users:
            if stop_flags.get(chat_id, False): break
            try:
                cl.user_follow(uid); log_follow(chat_id, uid); time.sleep(0.5)
            except (ChallengeRequired, FeedbackRequired, PleaseWaitFewMinutes):
                bot.send_message(chat_id, "⚠️ حظر مؤقت..."); time.sleep(1800); cl.login_by_sessionid(session)
            except: pass
        bot.send_message(chat_id, "✅ انتهى.")
    except Exception as e: bot.send_message(chat_id, f"خطأ: {e}")

def ask_unfollow_count(message):
    try:
        count = int(message.text)
        chat_id = message.chat.id
        session = get_user_data(chat_id).get("session_id")
        bot.send_message(chat_id, f"⚙️ جاري الحذف...")
        stop_flags[chat_id] = False
        threading.Thread(target=run_mass_unfollow_logic, args=(chat_id, session, count)).start()
    except: bot.send_message(message.chat.id, "❌ رقم فقط.")

def run_mass_unfollow_logic(chat_id, session, count):
    try:
        cl = Client(); cl.login_by_sessionid(session)
        my_id = cl.user_id
        following = cl.user_following(my_id)
        followers = cl.user_followers(my_id)
        non_followers = [uid for uid in following if uid not in followers]
        targets = non_followers[:count]
        
        bot.send_message(chat_id, f"🔎 سيتم حذف {len(targets)} شخص.")
        removed = 0
        for uid in targets:
            if stop_flags.get(chat_id, False): break
            try:
                cl.user_unfollow(uid); removed += 1; time.sleep(0.5)
            except (ChallengeRequired, FeedbackRequired, PleaseWaitFewMinutes):
                bot.send_message(chat_id, "🛑 حظر مؤقت..."); time.sleep(1800); cl.login_by_sessionid(session)
            except: pass
        bot.send_message(chat_id, f"🏁 تم حذف {removed}.")
    except Exception as e: bot.send_message(chat_id, f"خطأ: {e}")

# ==========================================
# تشغيل البوت (Anti-Crash)
# ==========================================
print("Bot Started...")
while True:
    try:
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
    except Exception as e:
        print(f"⚠️ Error: {e}... Restarting.")
        time.sleep(5)
