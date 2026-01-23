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
from bson.objectid import ObjectId
from flask import Flask

# ==========================================
# 1. إعدادات السيرفر والاتصال
# ==========================================

app = Flask(__name__)

@app.route('/')
def home():
    return "🔥 Bot is Running (Full Expanded Version)!"

def run_web_server():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

# تشغيل السيرفر في خيط مستقل
t_server = threading.Thread(target=run_web_server)
t_server.start()

# جلب المتغيرات البيئية
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

if not BOT_TOKEN or not MONGO_URL:
    print("❌ خطأ: البيانات ناقصة! تأكد من إضافة BOT_TOKEN و MONGO_URL.")

# الاتصال بقاعدة البيانات
try:
    cluster = MongoClient(MONGO_URL)
    db = cluster["telegram_bot_db"]
    users_collection = db["users_data"]
    follows_collection = db["follows_history"]
    stories_collection = db["recurring_stories"]
    print("✅ تم الاتصال بقاعدة البيانات بنجاح.")
except Exception as e:
    print(f"❌ فشل الاتصال بقاعدة البيانات: {e}")

bot = telebot.TeleBot(BOT_TOKEN)

# متغيرات التحكم العامة
stop_flags = {}
auto_reply_active = {}

# ==========================================
# 2. دوال التعامل مع قاعدة البيانات
# ==========================================

def get_user_data(chat_id):
    """جلب بيانات المستخدم"""
    user = users_collection.find_one({"_id": str(chat_id)})
    if user:
        return user
    else:
        return {}

def update_user_data(chat_id, key, value):
    """تحديث حقل معين للمستخدم"""
    users_collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {key: value}},
        upsert=True
    )

def logout_user(chat_id):
    """حذف بيانات الجلسة"""
    users_collection.update_one(
        {"_id": str(chat_id)},
        {"$set": {"session_id": None, "groups": [], "selected_ids": []}}
    )

def log_follow(chat_id, target_user_id):
    """تسجيل عملية متابعة"""
    follows_collection.insert_one({
        "chat_id": str(chat_id),
        "target_id": str(target_user_id),
        "date": datetime.now()
    })

def add_recurring_story(chat_id, file_id):
    """إضافة ستوري مجدول"""
    stories_collection.insert_one({
        "chat_id": str(chat_id),
        "file_id": file_id,
        "last_posted": datetime.now() - timedelta(days=2),
        "created_at": datetime.now()
    })

def get_my_stories(chat_id):
    """جلب قائمة الستوريات"""
    return list(stories_collection.find({"chat_id": str(chat_id)}))

def delete_story(story_id):
    """حذف ستوري معين"""
    stories_collection.delete_one({"_id": ObjectId(story_id)})

# ==========================================
# 3. القوائم ولوحات التحكم (Keyboards)
# ==========================================

def get_main_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    btn_login = types.InlineKeyboardButton("🔑 تسجيل دخول", callback_data="main_login")
    btn_logout = types.InlineKeyboardButton("🔴 خروج", callback_data="main_logout")
    
    btn_link_share = types.InlineKeyboardButton("🔗 نشر ستوري من رابط", callback_data="main_share_link")
    
    btn_recur_story = types.InlineKeyboardButton("🔄 ستوري متكرر", callback_data="menu_recur_story")
    btn_broadcast = types.InlineKeyboardButton("📢 برودكاست للخاص", callback_data="main_broadcast")
    
    btn_groups = types.InlineKeyboardButton("📂 الجروبات", callback_data="main_groups")
    btn_post = types.InlineKeyboardButton("📨 نشر نص للجروبات", callback_data="main_post_dm")
    
    btn_reply = types.InlineKeyboardButton("🗣 رد تلقائي", callback_data="main_auto_reply")
    btn_stop_reply = types.InlineKeyboardButton("🔕 إيقاف الرد", callback_data="main_stop_reply")
    
    btn_follow = types.InlineKeyboardButton("➕ متابعة", callback_data="main_follow")
    btn_mass = types.InlineKeyboardButton("🔥 حذف متابعين", callback_data="main_mass_unfollow")
    
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
    markup = types.InlineKeyboardMarkup(row_width=2)
    btn_add = types.InlineKeyboardButton("➕ إضافة جديد", callback_data="story_add")
    btn_view = types.InlineKeyboardButton("🗑 حذف الستوريات", callback_data="story_view")
    btn_back = types.InlineKeyboardButton("🔙 رجوع", callback_data="cmd_back_main")
    markup.add(btn_add, btn_view)
    markup.add(btn_back)
    return markup

def get_groups_menu(chat_id, user_data):
    groups = user_data.get("groups", [])
    selected_ids = user_data.get("selected_ids", [])
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    for group in groups:
        is_selected = group['id'] in selected_ids
        icon = "✅" if is_selected else "⬜"
        callback = f"toggle|{group['id']}"
        markup.add(types.InlineKeyboardButton(f"{icon} {group['name']}", callback_data=callback))
    
    markup.row(types.InlineKeyboardButton("الكل", callback_data="cmd|all"), types.InlineKeyboardButton("لا شيء", callback_data="cmd|none"))
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data="cmd|back"))
    return markup

# ==========================================
# 4. معالجة الرسائل والأزرار (Handlers)
# ==========================================

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "👋 **أهلاً بك في البوت (نسخة كاملة)**", reply_markup=get_main_menu())

@bot.callback_query_handler(func=lambda call: True)
def handle_all_callbacks(call):
    chat_id = call.message.chat.id
    action = call.data
    user_data = get_user_data(chat_id)
    session = user_data.get("session_id")
    
    # التنقل
    if action == "cmd_back_main" or action == "cmd|back":
        bot.edit_message_text("🏠 القائمة الرئيسية:", chat_id, call.message.message_id, reply_markup=get_main_menu())

    # تسجيل الدخول والخروج
    elif action == "main_login":
        if session:
            bot.answer_callback_query(call.id, "أنت مسجل بالفعل")
        else:
            msg = bot.send_message(chat_id, "📥 **أرسل كود السيزن (Session ID):**")
            bot.register_next_step_handler(msg, process_login)
            
    elif action == "main_logout":
        logout_user(chat_id)
        bot.edit_message_text("✅ تم تسجيل الخروج.", chat_id, call.message.message_id, reply_markup=get_main_menu())

    # إدارة الجروبات
    elif action == "main_groups":
        if not session:
            bot.answer_callback_query(call.id, "سجل دخول أولاً")
            return
        if not user_data.get("groups"):
            bot.answer_callback_query(call.id, "لا يوجد جروبات محفوظة!")
            return
        bot.edit_message_text("👇 اختر الجروبات:", chat_id, call.message.message_id, reply_markup=get_groups_menu(chat_id, user_data))

    elif action.startswith("toggle|") or action.startswith("cmd|"):
        handle_group_selection(call)

    # نشر رابط ستوري
    elif action == "main_share_link":
        if not session:
            bot.answer_callback_query(call.id, "سجل دخول أولاً")
            return
        selected = user_data.get("selected_ids", [])
        if not selected:
            bot.send_message(chat_id, "⚠️ اختر الجروبات أولاً.")
            return
        msg = bot.send_message(chat_id, "🔗 **أرسل رابط الستوري:**")
        bot.register_next_step_handler(msg, start_link_share_thread)

    # الستوري المتكرر
    elif action == "menu_recur_story":
        if not session:
            bot.answer_callback_query(call.id, "سجل دخول أولاً")
            return
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

    # البرودكاست والنشر
    elif action == "main_broadcast":
        if not session:
            bot.answer_callback_query(call.id, "سجل دخول أولاً")
            return
        msg = bot.send_message(chat_id, "📢 **اكتب رسالة البرودكاست (للخاص):**")
        bot.register_next_step_handler(msg, start_broadcast_thread)

    elif action == "main_post_dm":
        if not session:
            bot.answer_callback_query(call.id, "سجل دخول أولاً")
            return
        msg = bot.send_message(chat_id, "📝 **أرسل النص للنشر:**")
        bot.register_next_step_handler(msg, ask_time_for_dm)

    # الرد التلقائي
    elif action == "main_auto_reply":
        if not session:
            bot.answer_callback_query(call.id, "سجل دخول أولاً")
            return
        msg = bot.send_message(chat_id, "✍️ **أرسل نص الرد التلقائي:**")
        bot.register_next_step_handler(msg, start_auto_reply_thread)
        
    elif action == "main_stop_reply":
        auto_reply_active[chat_id] = False
        bot.send_message(chat_id, "🔕 تم إيقاف الرد التلقائي.")
        
    # المتابعة والحذف
    elif action == "main_follow":
        if not session:
            bot.answer_callback_query(call.id, "سجل دخول أولاً")
            return
        start_smart_follow_thread(chat_id, session)
        bot.answer_callback_query(call.id, "تم البدء")
        
    elif action == "main_mass_unfollow":
        if not session:
            bot.answer_callback_query(call.id, "سجل دخول أولاً")
            return
        msg = bot.send_message(chat_id, "🔢 **اكتب عدد الأشخاص للحذف:**")
        bot.register_next_step_handler(msg, ask_unfollow_count)

    # الإيقاف
    elif action == "main_stop":
        stop_flags[chat_id] = True
        auto_reply_active[chat_id] = False
        bot.answer_callback_query(call.id, "🛑 تم الإيقاف")

# ==========================================
# 5. الوظائف المنطقية (Logic Functions)
# ==========================================

def process_login(message):
    """دالة تسجيل الدخول المحسنة مع رسائل الانتظار"""
    wait_msg = bot.send_message(message.chat.id, "⏳ **جاري الاتصال بالسيرفر، يرجى الانتظار...**")
    
    try:
        cl = Client()
        cl.login_by_sessionid(message.text)
        
        # تحديث الرسالة ليعرف المستخدم أن السيزن صحيح
        bot.edit_message_text(
            "✅ **تم تسجيل الدخول بنجاح!**\n🔄 جاري الآن فحص الرسائل لجلب الجروبات (قد يستغرق وقتاً)...", 
            message.chat.id, 
            wait_msg.message_id
        )
        
        # محاولة جلب الجروبات بشكل منفصل لتجنب تعطل البوت
        gs = []
        try:
            ts = cl.direct_threads(amount=50)
            for t in ts:
                if t.is_group:
                    gs.append({"id": t.id, "name": t.thread_title or "NoName"})
        except Exception as e:
            print(f"Groups Error: {e}")
            bot.send_message(message.chat.id, "⚠️ تحذير: تم الدخول ولكن حدث خطأ في جلب الجروبات.")

        # حفظ البيانات
        update_user_data(message.chat.id, "session_id", message.text)
        update_user_data(message.chat.id, "groups", gs)
        
        # حذف رسالة الانتظار وإرسال الرسالة النهائية
        bot.delete_message(message.chat.id, wait_msg.message_id)
        bot.send_message(message.chat.id, f"🎉 **تم الانتهاء!**\n✅ تم العثور على {len(gs)} جروب.", reply_markup=get_main_menu())
        
    except Exception as e:
        bot.edit_message_text(f"❌ **فشل تسجيل الدخول:**\n{e}", message.chat.id, wait_msg.message_id)

def handle_group_selection(call):
    chat_id = call.message.chat.id
    data = call.data
    user_data = get_user_data(chat_id)
    selected_ids = user_data.get("selected_ids", [])
    groups = user_data.get("groups", [])
    
    if data.startswith("toggle|"):
        gid = data.split("|")[1]
        if gid in selected_ids:
            selected_ids.remove(gid)
        else:
            selected_ids.append(gid)
    elif data == "cmd|all":
        selected_ids = [g['id'] for g in groups]
    elif data == "cmd|none":
        selected_ids = []

    update_user_data(chat_id, "selected_ids", selected_ids)
    try:
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=get_groups_menu(chat_id, get_user_data(chat_id)))
    except:
        pass

# --- وظيفة نشر رابط ستوري ---
def start_link_share_thread(message):
    chat_id = message.chat.id
    session = get_user_data(chat_id).get("session_id")
    threading.Thread(target=run_link_share, args=(chat_id, session, message.text)).start()
    bot.send_message(chat_id, "⏳ جاري المعالجة...")

def run_link_share(chat_id, session, url):
    temp_path = f"temp_{chat_id}"
    path = None
    try:
        cl = Client()
        cl.login_by_sessionid(session)
        
        pk = cl.story_pk_from_url(url)
        media_info = cl.media_info(pk)
        path = cl.story_download(pk, filename=temp_path)
        is_video = media_info.media_type == 2
        
        selected = get_user_data(chat_id).get("selected_ids", [])
        count = 0
        
        for gid in selected:
            if stop_flags.get(chat_id):
                break
            try:
                if is_video:
                    cl.direct_send_video(path, thread_ids=[gid])
                else:
                    cl.direct_send_photo(path, thread_ids=[gid])
                count += 1
                time.sleep(5)
            except Exception as e:
                print(f"Error in group {gid}: {e}")
                time.sleep(2)
                
        bot.send_message(chat_id, f"✅ تم النشر في {count} جروب.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ خطأ: {e}")
    finally:
        if path and os.path.exists(path):
            os.remove(path)

# --- وظيفة الستوري المتكرر ---
def process_add_story(message):
    if message.photo:
        add_recurring_story(message.chat.id, message.photo[-1].file_id)
        bot.send_message(message.chat.id, "✅ تمت الجدولة.", reply_markup=get_stories_menu())
        # تشغيل الفاحص في خيط منفصل إذا لم يكن يعمل
        if threading.active_count() < 10: 
             threading.Thread(target=check_and_post_stories).start()
    else:
        bot.send_message(message.chat.id, "❌ صورة فقط.")

def show_active_stories(chat_id):
    stories = get_my_stories(chat_id)
    if not stories:
        bot.send_message(chat_id, "📭 القائمة فارغة.", reply_markup=get_stories_menu())
        return
        
    markup = types.InlineKeyboardMarkup()
    for i, s in enumerate(stories):
        callback = f"del_story|{s['_id']}"
        markup.add(types.InlineKeyboardButton(f"🗑 حذف {i+1}", callback_data=callback))
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data="menu_recur_story"))
    bot.send_message(chat_id, "الستوريات النشطة:", reply_markup=markup)

def check_and_post_stories():
    """دالة تعمل في الخلفية بشكل دائم"""
    while True:
        try:
            stories_list = list(stories_collection.find({}))
            for story in stories_list:
                time_diff = datetime.now() - story.get('last_posted')
                if time_diff > timedelta(hours=24):
                    try:
                        u = get_user_data(story['chat_id'])
                        if not u.get('session_id'):
                            continue
                            
                        cl = Client()
                        cl.login_by_sessionid(u['session_id'])
                        
                        fi = bot.get_file(story['file_id'])
                        d = bot.download_file(fi.file_path)
                        tp = f"s_{story['_id']}.jpg"
                        
                        with open(tp, 'wb') as f:
                            f.write(d)
                            
                        cl.photo_upload_to_story(tp)
                        
                        stories_collection.update_one(
                            {"_id": ObjectId(story['_id'])}, 
                            {"$set": {"last_posted": datetime.now()}}
                        )
                        
                        if os.path.exists(tp):
                            os.remove(tp)
                            
                    except Exception as e:
                        print(f"Story Error: {e}")
                        
            time.sleep(60)
        except Exception as e:
            print(f"Loop Error: {e}")
            time.sleep(60)

# تشغيل الفاحص عند بدء البوت
threading.Thread(target=check_and_post_stories).start()

# --- وظيفة الرد التلقائي ---
def start_auto_reply_thread(message):
    auto_reply_active[message.chat.id] = True
    stop_flags[message.chat.id] = False
    
    chat_id = message.chat.id
    session = get_user_data(chat_id).get("session_id")
    text = message.text
    
    threading.Thread(target=run_auto_reply, args=(chat_id, session, text)).start()
    bot.send_message(message.chat.id, "✅ بدأ الرد التلقائي.")

def run_auto_reply(chat_id, session, text):
    try:
        cl = Client()
        cl.login_by_sessionid(session)
        my_id = str(cl.user_id)
        replied_cache = []
        
        while not stop_flags.get(chat_id) and auto_reply_active.get(chat_id):
            try:
                threads = cl.direct_threads(amount=20)
                for t in threads:
                    if stop_flags.get(chat_id):
                        break
                        
                    if t.is_group:
                        # فحص آخر 10 رسائل
                        for m in t.messages[:10]:
                            if m.id in replied_cache:
                                continue
                            if str(m.user_id) == my_id:
                                continue
                                
                            is_reply_to_me = False
                            try: 
                                if m.reply_to_message and str(m.reply_to_message.user_id) == my_id:
                                    is_reply_to_me = True
                            except:
                                pass
                                
                            if is_reply_to_me:
                                cl.direct_send(text, thread_ids=[t.id])
                                replied_cache.append(m.id)
                                time.sleep(3)
            except Exception as e:
                print(f"AutoReply Loop Error: {e}")
                time.sleep(5)
                
            time.sleep(15)
    except Exception as e:
        print(f"AutoReply Critical Error: {e}")

# --- وظيفة البرودكاست والنشر ---
def start_broadcast_thread(message):
    chat_id = message.chat.id
    session = get_user_data(chat_id).get("session_id")
    text = message.text
    threading.Thread(target=run_broadcast, args=(chat_id, session, text)).start()
    bot.send_message(message.chat.id, "🚀 بدأ الإرسال.")

def run_broadcast(cid, sid, txt):
    try:
        cl = Client()
        cl.login_by_sessionid(sid)
        threads = cl.direct_threads(amount=100)
        
        for t in threads:
            if stop_flags.get(cid):
                break
            try:
                cl.direct_send(txt, thread_ids=[t.id])
                time.sleep(10)
            except Exception as e:
                print(f"Broadcast Error: {e}")
                
        bot.send_message(cid, "🏁 تم الانتهاء من البرودكاست.")
    except Exception as e:
        print(f"Broadcast Main Error: {e}")

def ask_time_for_dm(m):
    msg = bot.reply_to(m, "⏱ كم دقيقة الانتظار بين كل جروب؟")
    bot.register_next_step_handler(msg, lambda mm: start_dm_post_thread(m, m.text, mm.text))

def start_dm_post_thread(original_msg, text, delay_text):
    try:
        delay = float(delay_text) * 60
        threading.Thread(target=run_dm_post, args=(original_msg.chat.id, text, delay)).start()
    except ValueError:
        bot.send_message(original_msg.chat.id, "❌ الرجاء إدخال رقم صحيح.")

def run_dm_post(cid, txt, dlay):
    u = get_user_data(cid)
    try:
        cl = Client()
        cl.login_by_sessionid(u['session_id'])
        
        bot.send_message(cid, "🚀 بدأ النشر.")
        selected_groups = u.get("selected_ids", [])
        
        for g in selected_groups:
            if stop_flags.get(cid):
                break
            try:
                cl.direct_send(txt, thread_ids=[g])
                time.sleep(3)
            except Exception as e:
                print(f"DM Post Error: {e}")
            
            time.sleep(dlay)
            
        bot.send_message(cid, "🛑 انتهى النشر.")
    except Exception as e:
        bot.send_message(cid, f"❌ خطأ: {e}")

# --- وظيفة المتابعة الذكية ---
def start_smart_follow_thread(chat_id, session):
    stop_flags[chat_id] = False
    threading.Thread(target=run_smart_follow, args=(chat_id, session)).start()

def run_smart_follow(chat_id, session):
    try:
        cl = Client()
        cl.login_by_sessionid(session)
        target_id = "460563723"
        followers = cl.user_followers(target_id, amount=200)
        
        for uid in followers:
            if stop_flags.get(chat_id):
                break
            try:
                cl.user_follow(uid)
                log_follow(chat_id, uid)
                time.sleep(0.5)
            except Exception as e:
                print(f"Follow Error: {e}")
                time.sleep(60)
                
        bot.send_message(chat_id, "✅ تمت عملية المتابعة.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ خطأ: {e}")

# --- وظيفة الحذف الجماعي ---
def ask_unfollow_count(message):
    try:
        count = int(message.text)
        chat_id = message.chat.id
        session = get_user_data(chat_id).get("session_id")
        
        threading.Thread(target=run_mass_unfollow, args=(chat_id, session, count)).start()
        bot.send_message(chat_id, "جاري الحذف...")
    except ValueError:
        bot.send_message(message.chat.id, "رقم فقط من فضلك.")

def run_mass_unfollow(chat_id, session, count):
    try:
        cl = Client()
        cl.login_by_sessionid(session)
        my = cl.user_id
        
        following = cl.user_following(my)
        followers = cl.user_followers(my)
        
        targets = []
        for u in following:
            if u not in followers:
                targets.append(u)
                
        # قص القائمة حسب العدد المطلوب
        targets = targets[:count]
        
        for uid in targets:
            if stop_flags.get(chat_id):
                break
            try:
                cl.user_unfollow(uid)
                time.sleep(0.5)
            except Exception as e:
                print(f"Unfollow Error: {e}")
                time.sleep(60)
                
        bot.send_message(chat_id, "🏁 تم الانتهاء من الحذف.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ خطأ: {e}")

# ==========================================
# 6. التشغيل النهائي
# ==========================================

print("Bot Started...")

# حذف الويب هوك لتجنب التعارض (Anti-Crash)
try:
    bot.remove_webhook()
    time.sleep(1)
except Exception as e:
    print(f"Webhook Error: {e}")

# حلقة التشغيل الرئيسية
while True:
    try:
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
    except Exception as e:
        print(f"Polling Error: {e}")
        time.sleep(5)
