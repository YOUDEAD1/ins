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
from datetime import datetime, timedelta
from pymongo import MongoClient
from flask import Flask

# ==========================================
# 1. إعدادات السيرفر والاتصال (Configuration)
# ==========================================

app = Flask(__name__)

@app.route('/')
def home():
    return "🔥 Bot is Running (Full Version - No Shortcuts)!"

def run_web_server():
    # تشغيل سيرفر وهمي لكي يبقى البوت يعمل على Render
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

# تشغيل السيرفر في خيط منفصل
t_server = threading.Thread(target=run_web_server)
t_server.start()

# جلب المتغيرات من Render
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

if not BOT_TOKEN or not MONGO_URL:
    print("❌ خطأ: البيانات ناقصة! تأكد من إضافة BOT_TOKEN و MONGO_URL في Render.")

# الاتصال بقاعدة البيانات MongoDB
cluster = MongoClient(MONGO_URL)
db = cluster["telegram_bot_db"]

# تعريف الجداول (Collections)
users_collection = db["users_data"]        # بيانات المستخدمين
follows_collection = db["follows_history"] # سجل المتابعات
stories_collection = db["recurring_stories"] # الستوريات المتكررة

# تشغيل البوت
bot = telebot.TeleBot(BOT_TOKEN)

# متغيرات التحكم (Flags)
active_stats = {}      # حالة النشر
stop_flags = {}        # لإيقاف العمليات
auto_reply_active = {} # لتفعيل/إيقاف الرد التلقائي

# ==========================================
# 2. دوال التعامل مع قاعدة البيانات (Database Helpers)
# ==========================================

def get_user_data(chat_id):
    """جلب بيانات المستخدم من قاعدة البيانات"""
    user = users_collection.find_one({"_id": str(chat_id)})
    if user:
        return user
    else:
        return {}

def update_user_data(chat_id, key, value):
    """تحديث بيانات المستخدم (مثل السيزن، الجروبات المختارة)"""
    users_collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {key: value}},
        upsert=True
    )

def logout_user(chat_id):
    """تسجيل خروج (حذف السيزن والبيانات المؤقتة)"""
    users_collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {"session_id": None, "groups": [], "selected_ids": []}}
    )

def log_follow(chat_id, target_user_id):
    """تسجيل عملية متابعة مع الوقت"""
    follows_collection.insert_one({
        "chat_id": str(chat_id),
        "target_id": str(target_user_id),
        "date": datetime.now()
    })

def add_recurring_story(chat_id, file_id):
    """إضافة ستوري جديد للتكرار"""
    stories_collection.insert_one({
        "chat_id": str(chat_id),
        "file_id": file_id,
        "last_posted": datetime.now() - timedelta(days=2), # لكي ينشر فوراً
        "created_at": datetime.now()
    })

def get_my_stories(chat_id):
    """جلب الستوريات الخاصة بالمستخدم"""
    return list(stories_collection.find({"chat_id": str(chat_id)}))

def delete_story(story_id):
    """حذف ستوري من قاعدة البيانات"""
    from bson.objectid import ObjectId
    stories_collection.delete_one({"_id": ObjectId(story_id)})

# ==========================================
# 3. القوائم والأزرار (Menus & Keyboards)
# ==========================================

def get_main_menu():
    """القائمة الرئيسية للبوت"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    # الصف الأول: الحساب
    btn_login = types.InlineKeyboardButton("🔑 تسجيل دخول", callback_data="main_login")
    btn_logout = types.InlineKeyboardButton("🔴 خروج", callback_data="main_logout")
    
    # الصف الثاني: الميزة الجديدة (رابط -> جروبات)
    btn_link_share = types.InlineKeyboardButton("🔗 نشر ستوري من رابط للجروبات", callback_data="main_share_link")

    # الصف الثالث: الستوري والبرودكاست
    btn_recur_story = types.InlineKeyboardButton("🔄 ستوري متكرر (24h)", callback_data="menu_recur_story")
    btn_broadcast = types.InlineKeyboardButton("📢 برودكاست خاص", callback_data="main_broadcast")

    # الصف الرابع: الجروبات والنشر
    btn_groups = types.InlineKeyboardButton("📂 اختر الجروبات", callback_data="main_groups")
    btn_post = types.InlineKeyboardButton("📨 نشر نص للجروبات", callback_data="main_post_dm")
    
    # الصف الخامس: التفاعل والرد
    btn_reply = types.InlineKeyboardButton("🗣 الرد التلقائي", callback_data="main_auto_reply")
    btn_stop_reply = types.InlineKeyboardButton("🔕 إيقاف الرد", callback_data="main_stop_reply")
    
    # الصف السادس: المتابعة والحذف
    btn_follow = types.InlineKeyboardButton("➕ متابعة", callback_data="main_follow")
    btn_mass = types.InlineKeyboardButton("🔥 حذف متابعين", callback_data="main_mass_unfollow")
    
    # زر الإيقاف العام
    btn_stop = types.InlineKeyboardButton("⛔ إيقاف الكل", callback_data="main_stop")
    
    markup.add(btn_login, btn_logout)
    markup.add(btn_link_share)
    markup.add(btn_recur_story, btn_broadcast)
    markup.add(btn_groups, btn_post)
    markup.add(btn_reply, btn_stop_reply)
    markup.add(btn_follow, btn_mass)
    markup.add(btn_stop)
    return markup

def get_stories_menu():
    """قائمة إدارة الستوريات"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    btn_add = types.InlineKeyboardButton("➕ إضافة جديد", callback_data="story_add")
    btn_view = types.InlineKeyboardButton("🗑 حذف الستوريات", callback_data="story_view")
    btn_back = types.InlineKeyboardButton("🔙 رجوع", callback_data="cmd_back_main")
    markup.add(btn_add, btn_view)
    markup.add(btn_back)
    return markup

def get_groups_menu(chat_id, user_data):
    """قائمة اختيار الجروبات"""
    groups = user_data.get("groups", [])
    selected_ids = user_data.get("selected_ids", [])
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    for group in groups:
        is_selected = group['id'] in selected_ids
        icon = "✅" if is_selected else "⬜"
        markup.add(types.InlineKeyboardButton(f"{icon} {group['name']}", callback_data=f"toggle|{group['id']}"))
    
    markup.row(types.InlineKeyboardButton("الكل", callback_data="cmd|all"), types.InlineKeyboardButton("لا شيء", callback_data="cmd|none"))
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data="cmd|back"))
    return markup

# ==========================================
# 4. معالجة الرسائل والأزرار (Handlers)
# ==========================================

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(
        message.chat.id, 
        "👋 **البوت الشامل (Full Version)**\n"
        "جميع الميزات تعمل الآن بكفاءة وبدون اختصارات.", 
        reply_markup=get_main_menu()
    )

@bot.callback_query_handler(func=lambda call: True)
def handle_all_callbacks(call):
    chat_id = call.message.chat.id
    action = call.data
    user_data = get_user_data(chat_id)
    session = user_data.get("session_id")
    
    # --- التنقل (Navigation) ---
    if action == "cmd_back_main" or action == "cmd|back":
        bot.edit_message_text("🏠 القائمة الرئيسية:", chat_id, call.message.message_id, reply_markup=get_main_menu())
        return

    # --- الحساب (Account) ---
    if action == "main_login":
        if session:
            bot.answer_callback_query(call.id, "أنت مسجل بالفعل")
        else:
            msg = bot.send_message(chat_id, "📥 **أرسل كود السيزن (Session ID):**")
            bot.register_next_step_handler(msg, process_login)
            
    elif action == "main_logout":
        logout_user(chat_id)
        bot.answer_callback_query(call.id, "تم الخروج")
        bot.send_message(chat_id, "✅ تم تسجيل الخروج بنجاح.", reply_markup=get_main_menu())

    # --- الجروبات (Groups) ---
    elif action == "main_groups":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        if not user_data.get("groups"):
            bot.answer_callback_query(call.id, "لا يوجد جروبات محفوظة! سجل دخول مجدداً.")
            return
        bot.edit_message_text("👇 اختر الجروبات:", chat_id, call.message.message_id, reply_markup=get_groups_menu(chat_id, user_data))

    elif action.startswith("toggle|") or action.startswith("cmd|"):
        handle_group_selection(call)

    # --- الميزة الجديدة: نشر رابط ستوري ---
    elif action == "main_share_link":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        selected = user_data.get("selected_ids", [])
        if not selected:
            bot.send_message(chat_id, "⚠️ **تنبيه:** لم تختر أي جروبات! اذهب لزر 'اختر الجروبات' أولاً.")
            return
        
        msg = bot.send_message(chat_id, "🔗 **أرسل رابط الستوري الآن:**\n(سأقوم بتحميله وإرساله لـ {} جروب)".format(len(selected)))
        bot.register_next_step_handler(msg, start_link_share_thread)

    # --- الستوري المتكرر (Recurring Story) ---
    elif action == "menu_recur_story":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        bot.edit_message_text("🔄 **إدارة الستوري المتكرر:**", chat_id, call.message.message_id, reply_markup=get_stories_menu())
        
    elif action == "story_add":
        msg = bot.send_message(chat_id, "📸 **أرسل الصورة للجدولة (كل 24 ساعة):**")
        bot.register_next_step_handler(msg, process_add_story)
        
    elif action == "story_view":
        show_active_stories(chat_id)
        
    elif action.startswith("del_story|"):
        story_id = action.split("|")[1]
        delete_story(story_id)
        bot.answer_callback_query(call.id, "تم الحذف")
        show_active_stories(chat_id)

    # --- البرودكاست والنشر ---
    elif action == "main_broadcast":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "📢 **اكتب رسالة البرودكاست (للخاص):**")
        bot.register_next_step_handler(msg, start_broadcast_thread)

    elif action == "main_post_dm":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "📝 **أرسل النص للنشر في الجروبات:**")
        bot.register_next_step_handler(msg, ask_time_for_dm)

    # --- الرد التلقائي والمتابعة ---
    elif action == "main_auto_reply":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "✍️ **أرسل نص الرد التلقائي:**\n(سيرد البوت فقط على من يرد عليك)")
        bot.register_next_step_handler(msg, start_auto_reply_thread)
        
    elif action == "main_stop_reply":
        auto_reply_active[chat_id] = False
        bot.send_message(chat_id, "🔕 تم إيقاف الرد التلقائي.")
        
    elif action == "main_follow":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        start_smart_follow_thread(chat_id, session)
        bot.answer_callback_query(call.id, "تم بدء المتابعة")
        
    elif action == "main_mass_unfollow":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "🔢 **كم شخص تريد حذفه؟** (اكتب الرقم):")
        bot.register_next_step_handler(msg, ask_unfollow_count)

    # --- الإيقاف العام ---
    elif action == "main_stop":
        stop_flags[chat_id] = True
        auto_reply_active[chat_id] = False
        bot.answer_callback_query(call.id, "🛑 تم الإيقاف الشامل")

# ==========================================
# 5. منطق الميزات بالتفصيل (Logic Functions)
# ==========================================

# --------------------------
# أ. ميزة نشر رابط ستوري للجروبات
# --------------------------
def start_link_share_thread(message):
    url = message.text
    chat_id = message.chat.id
    session = get_user_data(chat_id).get("session_id")
    
    if "instagram.com/stories" not in url:
        bot.send_message(chat_id, "❌ هذا ليس رابط ستوري صحيح.")
        return

    bot.send_message(chat_id, "⏳ **جاري التحميل والنشر...**")
    threading.Thread(target=run_link_share, args=(chat_id, session, url)).start()

def run_link_share(chat_id, session, url):
    temp_path = f"temp_story_{chat_id}"
    try:
        cl = Client()
        cl.login_by_sessionid(session)
        
        # 1. جلب معلومات الستوري (PK)
        try:
            pk = cl.story_pk_from_url(url)
            media_info = cl.media_info(pk)
        except Exception as e:
            bot.send_message(chat_id, f"❌ لم أستطع قراءة الرابط: {e}")
            return

        # 2. تحميل الملف
        path = cl.story_download(pk, filename=temp_path)
        
        # 3. تحديد النوع
        is_video = media_info.media_type == 2
        
        # 4. النشر
        user_data = get_user_data(chat_id)
        selected_ids = user_data.get("selected_ids", [])
        
        count = 0
        for gid in selected_ids:
            if stop_flags.get(chat_id, False): break
            try:
                if is_video:
                    cl.direct_send_video(path, thread_ids=[gid])
                else:
                    cl.direct_send_photo(path, thread_ids=[gid])
                
                count += 1
                time.sleep(5) # انتظار 5 ثواني
            except Exception as e:
                print(f"Failed group {gid}: {e}")
                time.sleep(2)

        bot.send_message(chat_id, f"✅ **تمت المهمة!**\nتم النشر في {count} جروب.")

    except Exception as e:
        bot.send_message(chat_id, f"❌ حدث خطأ: {e}")
    finally:
        if os.path.exists(path): os.remove(path)

# --------------------------
# ب. الستوري المتكرر (الخلفية)
# --------------------------
def process_add_story(message):
    if message.photo:
        add_recurring_story(message.chat.id, message.photo[-1].file_id)
        bot.send_message(message.chat.id, "✅ تمت الجدولة (كل 24 ساعة).", reply_markup=get_stories_menu())
        # تنشيط الفاحص
        threading.Thread(target=check_and_post_stories).start()
    else:
        bot.send_message(message.chat.id, "❌ صورة فقط.")

def show_active_stories(chat_id):
    stories = get_my_stories(chat_id)
    if not stories:
        return bot.send_message(chat_id, "📭 لا يوجد ستوريات مجدولة.", reply_markup=get_stories_menu())
    
    markup = types.InlineKeyboardMarkup()
    for i, s in enumerate(stories):
        markup.add(types.InlineKeyboardButton(f"🗑 حذف الستوري رقم {i+1}", callback_data=f"del_story|{s['_id']}"))
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data="menu_recur_story"))
    bot.send_message(chat_id, f"👇 لديك {len(stories)} ستوري نشط:", reply_markup=markup)

def check_and_post_stories():
    """دالة تعمل في الخلفية لفحص الستوريات ونشرها"""
    while True:
        try:
            # البحث عن جميع الستوريات في قاعدة البيانات
            all_stories = list(stories_collection.find({}))
            for story in all_stories:
                # هل مرت 24 ساعة؟
                if datetime.now() - story.get('last_posted') > timedelta(hours=24):
                    try:
                        u = get_user_data(story['chat_id'])
                        if not u.get('session_id'): continue
                        
                        cl = Client()
                        cl.login_by_sessionid(u['session_id'])
                        
                        # تحميل من تيليجرام
                        fi = bot.get_file(story['file_id'])
                        d = bot.download_file(fi.file_path)
                        tp = f"s_{story['_id']}.jpg"
                        with open(tp, 'wb') as f: f.write(d)
                        
                        # رفع لانستقرام
                        cl.photo_upload_to_story(tp)
                        
                        # تحديث الوقت
                        from bson.objectid import ObjectId
                        stories_collection.update_one(
                            {"_id": ObjectId(story['_id'])}, 
                            {"$set": {"last_posted": datetime.now()}}
                        )
                        os.remove(tp)
                    except Exception as e:
                        print(f"Story Error: {e}")
            time.sleep(60) # فحص كل دقيقة
        except:
            time.sleep(60)

# تشغيل الفاحص عند بدء البوت
threading.Thread(target=check_and_post_stories).start()

# --------------------------
# ج. الرد التلقائي العميق
# --------------------------
def start_auto_reply_thread(message):
    auto_reply_active[message.chat.id] = True
    stop_flags[message.chat.id] = False
    threading.Thread(target=run_auto_reply, args=(message.chat.id, get_user_data(message.chat.id).get("session_id"), message.text)).start()
    bot.send_message(message.chat.id, "✅ تم تفعيل الرد التلقائي (فحص 10 رسائل).")

def run_auto_reply(chat_id, session, text):
    try:
        cl = Client()
        cl.login_by_sessionid(session)
        my_id = str(cl.user_id)
        replied_cache = [] 

        print(f"✅ Auto Reply Running for {chat_id}")
        
        while not stop_flags.get(chat_id, False) and auto_reply_active.get(chat_id, False):
            try:
                threads = cl.direct_threads(amount=20)
                for t in threads:
                    if t.is_group:
                        # فحص آخر 10 رسائل
                        for m in t.messages[:10]:
                            if m.id in replied_cache: continue
                            if str(m.user_id) == my_id: continue
                            
                            # هل هو رد علي؟
                            is_reply_to_me = False
                            try:
                                if m.reply_to_message and str(m.reply_to_message.user_id) == my_id:
                                    is_reply_to_me = True
                            except: pass
                            
                            if is_reply_to_me:
                                cl.direct_send(text, thread_ids=[t.id])
                                replied_cache.append(m.id)
                                if len(replied_cache) > 100: replied_cache.pop(0)
                                time.sleep(3)
            except: 
                time.sleep(5)
            time.sleep(15) # دورة كل 15 ثانية
    except Exception as e:
        print(f"AutoReply Error: {e}")

# --------------------------
# د. تسجيل الدخول والجروبات
# --------------------------
def process_login(message):
    try:
        cl = Client()
        cl.login_by_sessionid(message.text)
        ts = cl.direct_threads(amount=50)
        gs = [{"id": t.id, "name": t.thread_title or "NoName"} for t in ts if t.is_group]
        
        update_user_data(message.chat.id, "session_id", message.text)
        update_user_data(message.chat.id, "groups", gs)
        bot.send_message(message.chat.id, f"✅ تم! وجدت {len(gs)} جروب.", reply_markup=get_main_menu())
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ خطأ: {e}")

def handle_group_selection(call):
    chat_id = call.message.chat.id
    data = call.data
    user_data = get_user_data(chat_id)
    selected_ids = user_data.get("selected_ids", [])
    groups = user_data.get("groups", [])
    
    if data.startswith("toggle|"):
        gid = data.split("|")[1]
        if gid in selected_ids: selected_ids.remove(gid)
        else: selected_ids.append(gid)
    elif data == "cmd|all":
        selected_ids = [g['id'] for g in groups]
    elif data == "cmd|none":
        selected_ids = []

    update_user_data(chat_id, "selected_ids", selected_ids)
    try:
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=get_groups_menu(chat_id, get_user_data(chat_id)))
    except: pass

# --------------------------
# هـ. البرودكاست والنشر
# --------------------------
def start_broadcast_thread(message):
    threading.Thread(target=run_broadcast, args=(message.chat.id, get_user_data(message.chat.id).get("session_id"), message.text)).start()
    bot.send_message(message.chat.id, "🚀 بدأ البرودكاست للخاص.")

def run_broadcast(cid, sid, txt):
    try:
        cl = Client()
        cl.login_by_sessionid(sid)
        for t in cl.direct_threads(amount=100):
            if stop_flags.get(cid, False): break
            try:
                cl.direct_send(txt, thread_ids=[t.id])
                time.sleep(10)
            except: pass
        bot.send_message(cid, "🏁 انتهى البرودكاست.")
    except: pass

def ask_time_for_dm(m):
    msg = bot.reply_to(m, "⏱ كم دقيقة الانتظار؟")
    bot.register_next_step_handler(msg, lambda mm: threading.Thread(target=run_dm_post, args=(m.chat.id, m.text, float(mm.text)*60)).start())

def run_dm_post(cid, txt, dlay):
    u = get_user_data(cid)
    s = u.get("selected_ids", [])
    cl = Client()
    cl.login_by_sessionid(u['session_id'])
    
    bot.send_message(cid, "🚀 بدأ النشر للجروبات.")
    while not stop_flags.get(cid, False):
        for g in s:
            if stop_flags.get(cid, False): break
            try:
                cl.direct_send(txt, thread_ids=[g])
                time.sleep(3)
            except: pass
        time.sleep(dlay)
    bot.send_message(cid, "🛑 توقف النشر.")

# --------------------------
# و. المتابعة والحذف
# --------------------------
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
                bot.send_message(chat_id, "⚠️ حظر مؤقت (30 دقيقة)..."); time.sleep(1800); cl.login_by_sessionid(session)
            except: pass
        bot.send_message(chat_id, "✅ انتهت المتابعة.")
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
                cl.user_unfollow(uid)
                removed += 1
                time.sleep(0.5)
            except (ChallengeRequired, FeedbackRequired, PleaseWaitFewMinutes):
                bot.send_message(chat_id, "🛑 حظر مؤقت..."); time.sleep(1800); cl.login_by_sessionid(session)
            except: pass
        bot.send_message(chat_id, f"🏁 تم حذف {removed}.")
    except Exception as e: bot.send_message(chat_id, f"خطأ: {e}")

# ==========================================
# 6. التشغيل النهائي (Anti-Crash Loop)
# ==========================================
# ==========================================
# 6. التشغيل النهائي (Anti-Crash Loop)
# ==========================================
print("Bot Started...")

# ⚠️ هذا السطر الجديد يحل مشكلة عدم الاستجابة
try:
    bot.remove_webhook()
    time.sleep(1)
except:
    pass

while True:
    try:
        # إضافة print للتأكد في السجلات أن البوت يحاول الاتصال
        print("Checking for updates...") 
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
    except Exception as e:
        print(f"⚠️ Connection Error: {e}... Restarting in 5s")
        time.sleep(5)
