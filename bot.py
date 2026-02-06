import telebot
from telebot import types
from instagrapi import Client
from instagrapi.exceptions import (
    FeedbackRequired, ChallengeRequired, 
    PleaseWaitFewMinutes, RateLimitError, 
    LoginRequired, BadPassword
)
import time
import os
import threading
import json
import shutil
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId
from flask import Flask
from urllib.parse import unquote

# ==========================================
# 1. إعدادات السيرفر
# ==========================================

app = Flask(__name__)

@app.route('/')
def home():
    return "🔥 Bot is Running (Samsung Galaxy Mode)!"

def run_web_server():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

t_server = threading.Thread(target=run_web_server)
t_server.start()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

if not BOT_TOKEN or not MONGO_URL:
    print("❌ نقص في البيانات البيئية!")

try:
    cluster = MongoClient(MONGO_URL)
    db = cluster["telegram_bot_db"]
    users_collection = db["users_data"]
    follows_collection = db["follows_history"]
    stories_collection = db["recurring_stories"]
    print("✅ تم الاتصال بقاعدة البيانات.")
except Exception as e:
    print(f"❌ فشل الاتصال بالقاعدة: {e}")

bot = telebot.TeleBot(BOT_TOKEN)

stop_flags = {}
auto_reply_active = {}

# ==========================================
# 2. دوال التعامل مع البيانات
# ==========================================

def get_user_data(chat_id):
    user = users_collection.find_one({"_id": str(chat_id)})
    return user if user else {}

def update_user_data(chat_id, key, value):
    users_collection.update_one({"_id": str(chat_id)}, {"$set": {key: value}}, upsert=True)

def logout_user(chat_id):
    users_collection.update_one({"_id": str(chat_id)}, {"$set": {"session_id": None, "groups": [], "selected_ids": []}})

def log_follow(chat_id, target_user_id):
    follows_collection.insert_one({"chat_id": str(chat_id), "target_id": str(target_user_id), "date": datetime.now()})

def add_recurring_story(chat_id, file_id):
    stories_collection.insert_one({
        "chat_id": str(chat_id), 
        "file_id": file_id, 
        "last_posted": datetime.now() - timedelta(days=2), 
        "created_at": datetime.now()
    })

def get_my_stories(chat_id):
    return list(stories_collection.find({"chat_id": str(chat_id)}))

def delete_story(story_id):
    stories_collection.delete_one({"_id": ObjectId(story_id)})

# ==========================================
# 3. الدوال السحرية
# ==========================================

def get_safe_threads(cl):
    """جلب الجروبات وتجاهل أخطاء الرسائل"""
    try:
        # تقليل العدد لتسريع العملية وتجنب التايم أوت
        params = {"visual_message_return_type": "unseen", "thread_message_limit": "5", "persistent_badging": "true", "limit": "15"}
        response = cl.private_request("direct_v2/inbox/", params=params)
        threads = response.get('inbox', {}).get('threads', [])
        safe_groups = []
        for t in threads:
            if t.get('is_group'):
                safe_groups.append({"id": t.get('thread_id'), "name": t.get('thread_title', "NoName")})
        return safe_groups
    except Exception as e:
        print(f"Error fetching threads: {e}")
        return []

def force_story_upload(cl, file_path):
    try:
        print(f"🚀 Attempting force upload for {file_path}")
        with open(file_path, 'rb') as f:
            data = f.read()
        
        upload_id = str(int(time.time() * 1000))
        
        try:
             cl.photo_rupload(file_path, upload_id)
        except Exception as e:
            print(f"Upload step warning: {e}")

        try:
            cl.private_request(
                "media/configure_to_story/",
                data={
                    "upload_id": upload_id,
                    "source_type": "3",
                    "configure_mode": "1",
                    "client_shared_at": str(int(time.time())),
                    "edits": {
                        "crop_zoom": [0.0, 0.0],
                        "sf_scale": 1.0
                    },
                    "_uid": cl.user_id,
                    "_uuid": cl.uuid
                }
            )
            return True, "Published via Force Method"
        except Exception as e:
            print(f"Manual configure failed: {e}. Trying standard...")
            cl.photo_upload_to_story(file_path)
            return True, "Published via Standard Method"
            
    except Exception as e:
        if "pinned_channels" in str(e) or "ReplyMessage" in str(e):
            return True, "Published (Ignored Error)"
        return False, str(e)

# ==========================================
# 4. القوائم
# ==========================================

def get_main_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    btn_login = types.InlineKeyboardButton("🔑 تسجيل دخول", callback_data="main_login")
    btn_logout = types.InlineKeyboardButton("🔴 خروج", callback_data="main_logout")
    btn_link_share = types.InlineKeyboardButton("🔗 نشر رابط للجروبات", callback_data="main_share_link")
    btn_recur_story = types.InlineKeyboardButton("🔄 ستوري متكرر", callback_data="menu_recur_story")
    btn_broadcast = types.InlineKeyboardButton("📢 برودكاست", callback_data="main_broadcast")
    btn_groups = types.InlineKeyboardButton("📂 الجروبات", callback_data="main_groups")
    btn_post = types.InlineKeyboardButton("📨 نشر نص", callback_data="main_post_dm")
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
    if not groups:
        markup.add(types.InlineKeyboardButton("⚠️ تحديث الجروبات", callback_data="main_login"))
    for group in groups:
        is_selected = group['id'] in selected_ids
        icon = "✅" if is_selected else "⬜"
        markup.add(types.InlineKeyboardButton(f"{icon} {group['name']}", callback_data=f"toggle|{group['id']}"))
    markup.row(types.InlineKeyboardButton("الكل", callback_data="cmd|all"), types.InlineKeyboardButton("لا شيء", callback_data="cmd|none"))
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data="cmd|back"))
    return markup

# ==========================================
# 5. المعالجة (Handlers)
# ==========================================

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "👋 **البوت يعمل (Samsung Emulation Mode)**", reply_markup=get_main_menu())

@bot.callback_query_handler(func=lambda call: True)
def handle_all_callbacks(call):
    chat_id = call.message.chat.id
    action = call.data
    user_data = get_user_data(chat_id)
    session = user_data.get("session_id")
    
    if action == "cmd_back_main" or action == "cmd|back":
        bot.edit_message_text("🏠 القائمة الرئيسية:", chat_id, call.message.message_id, reply_markup=get_main_menu())

    elif action == "main_login":
        if session:
            msg = bot.send_message(chat_id, "🔄 **جاري تحديث الجروبات...**")
            threading.Thread(target=refresh_groups_only, args=(chat_id, session, msg)).start()
        else:
            msg = bot.send_message(chat_id, "📥 **أرسل كود السيزن (Session ID):**")
            bot.register_next_step_handler(msg, process_login)
            
    elif action == "main_logout":
        logout_user(chat_id)
        bot.edit_message_text("✅ تم تسجيل الخروج.", chat_id, call.message.message_id, reply_markup=get_main_menu())

    elif action == "main_groups":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        bot.edit_message_text("👇 اختر الجروبات:", chat_id, call.message.message_id, reply_markup=get_groups_menu(chat_id, user_data))

    elif action.startswith("toggle|") or action.startswith("cmd|"):
        handle_group_selection(call)

    elif action == "main_share_link":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        selected = user_data.get("selected_ids", [])
        if not selected: return bot.send_message(chat_id, "⚠️ اختر الجروبات أولاً.")
        msg = bot.send_message(chat_id, "🔗 **أرسل رابط الستوري:**")
        bot.register_next_step_handler(msg, start_link_share_thread)

    elif action == "menu_recur_story":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        bot.edit_message_text("🔄 **إدارة الستوري المتكرر:**", chat_id, call.message.message_id, reply_markup=get_stories_menu())
        
    elif action == "story_add":
        msg = bot.send_message(chat_id, "📸 **أرسل الصورة للجدولة:**")
        bot.register_next_step_handler(msg, process_add_story)
        
    elif action == "story_view":
        show_active_stories(chat_id)
        
    elif action.startswith("del_story|"):
        delete_story(action.split("|")[1])
        bot.answer_callback_query(call.id, "تم الحذف")
        show_active_stories(chat_id)

    elif action == "main_broadcast":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "📢 **اكتب رسالة البرودكاست (للخاص):**")
        bot.register_next_step_handler(msg, start_broadcast_thread)

    elif action == "main_post_dm":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "📝 **أرسل النص للنشر:**")
        bot.register_next_step_handler(msg, ask_time_for_dm)

    elif action == "main_auto_reply":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "✍️ **أرسل نص الرد التلقائي:**")
        bot.register_next_step_handler(msg, start_auto_reply_thread)
        
    elif action == "main_stop_reply":
        auto_reply_active[chat_id] = False
        bot.send_message(chat_id, "🔕 تم الإيقاف.")
        
    elif action == "main_follow":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        start_smart_follow_thread(chat_id, session)
        bot.answer_callback_query(call.id, "تم البدء")
        
    elif action == "main_mass_unfollow":
        if not session: return bot.answer_callback_query(call.id, "سجل دخول أولاً")
        msg = bot.send_message(chat_id, "🔢 **العدد:**")
        bot.register_next_step_handler(msg, ask_unfollow_count)

    elif action == "main_stop":
        stop_flags[chat_id] = True
        auto_reply_active[chat_id] = False
        bot.answer_callback_query(call.id, "🛑 تم الإيقاف")

# ==========================================
# 6. التنفيذ (Logic)
# ==========================================

def process_login(message):
    wait_msg = bot.send_message(message.chat.id, "⏳ **جاري محاكاة جهاز Samsung والاتصال...**")
    try:
        raw_text = message.text.strip().replace('"', '').replace("'", "").replace(" ", "")
        clean_session = unquote(raw_text)
        
        cl = Client()
        cl.request_timeout = 30
        
        # ========================================================
        # 🔥 الحل السحري: إعدادات جهاز وهمي (Samsung Galaxy S9)
        # ========================================================
        # هذا يجعل إنستجرام يظن أن الطلب قادم من هاتف أندرويد حقيقي
        # وليس من سيرفر بايثون.
        
        cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
        
        device_settings = {
            "app_version": "269.0.0.18.75",
            "android_version": 26,
            "android_release": "8.0.0",
            "dpi": "480dpi",
            "resolution": "1080x1920",
            "manufacturer": "Samsung",
            "device": "SM-G960F",
            "model": "Galaxy S9",
            "cpu": "samsungexynos9810",
            "version_code": "314665256"
        }
        cl.set_settings(device_settings)
        # ========================================================

        try:
            cl.login_by_sessionid(clean_session)
        except KeyError as e:
            if 'data' in str(e):
                bot.edit_message_text(
                    "❌ **فشل الدخول رغم المحاكاة**\n"
                    "للأسف، السيرفر الذي تستخدمه (IP) محظور تماماً من إنستجرام.\n"
                    "الحل الوحيد المتبقي هو استخدام بروكسي (Proxy).", 
                    message.chat.id, wait_msg.message_id
                )
                return
            else:
                raise e
            
        bot.edit_message_text("🔄 **تم قبول الجهاز! جاري سحب الجروبات...**", message.chat.id, wait_msg.message_id)
        
        gs = get_safe_threads(cl)
        
        update_user_data(message.chat.id, "session_id", clean_session)
        update_user_data(message.chat.id, "groups", gs)
        
        bot.delete_message(message.chat.id, wait_msg.message_id)
        bot.send_message(message.chat.id, f"✅ **تم الدخول بنجاح!**\n📱 الجهاز المحاكى: Samsung Galaxy S9\n📂 الجروبات: {len(gs)}", reply_markup=get_main_menu())
        
    except Exception as e:
        bot.edit_message_text(f"❌ خطأ غير متوقع: {str(e)}", message.chat.id, wait_msg.message_id)

def refresh_groups_only(chat_id, session, msg_obj):
    try:
        cl = Client()
        # محاكاة الجهاز عند التحديث أيضاً
        cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
        cl.login_by_sessionid(session)
        gs = get_safe_threads(cl)
        update_user_data(chat_id, "groups", gs)
        bot.edit_message_text(f"✅ تم التحديث.\nالجروبات: {len(gs)}", chat_id, msg_obj.message_id)
    except Exception as e:
         bot.edit_message_text(f"❌ فشل التحديث: {e}", chat_id, msg_obj.message_id)

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
    elif data == "cmd|all": selected_ids = [g['id'] for g in groups]
    elif data == "cmd|none": selected_ids = []

    update_user_data(chat_id, "selected_ids", selected_ids)
    try: bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=get_groups_menu(chat_id, get_user_data(chat_id)))
    except: pass

# --- Story Logic ---
def process_add_story(message):
    if message.photo:
        add_recurring_story(message.chat.id, message.photo[-1].file_id)
        bot.send_message(message.chat.id, "✅ **تمت الجدولة!**\nجاري محاولة النشر فوراً للتجربة...", reply_markup=get_stories_menu())
        threading.Thread(target=check_and_post_stories, args=(True,)).start()
    else: bot.send_message(message.chat.id, "❌ صورة فقط")

def show_active_stories(chat_id):
    stories = get_my_stories(chat_id)
    if not stories: return bot.send_message(chat_id, "📭 فارغ", reply_markup=get_stories_menu())
    markup = types.InlineKeyboardMarkup()
    for i, s in enumerate(stories):
        markup.add(types.InlineKeyboardButton(f"🗑 حذف {i+1}", callback_data=f"del_story|{s['_id']}"))
    markup.add(types.InlineKeyboardButton("🔙 رجوع", callback_data="menu_recur_story"))
    bot.send_message(chat_id, "الستوريات:", reply_markup=markup)

def check_and_post_stories(force_run=False):
    if force_run:
        try:
            stories_list = list(stories_collection.find({}))
            for story in stories_list:
                try:
                    u = get_user_data(story['chat_id'])
                    if not u.get('session_id'): continue
                    
                    bot.send_message(story['chat_id'], "⏳ جاري محاولة رفع الستوري...")
                    
                    cl = Client()
                    # إضافة المحاكاة هنا أيضاً
                    cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
                    cl.login_by_sessionid(u['session_id'])
                    
                    fi = bot.get_file(story['file_id'])
                    d = bot.download_file(fi.file_path)
                    tp = f"s_{story['_id']}.jpg"
                    with open(tp, 'wb') as f: f.write(d)
                    
                    success, msg = force_story_upload(cl, tp)
                    
                    if success:
                        stories_collection.update_one({"_id": ObjectId(story['_id'])}, {"$set": {"last_posted": datetime.now()}})
                        bot.send_message(story['chat_id'], f"✅ تم نشر الستوري بنجاح!\n(الحالة: {msg})")
                    else:
                        bot.send_message(story['chat_id'], f"❌ فشل نشر الستوري: {msg}")
                    
                    os.remove(tp)
                except Exception as e:
                    print(f"Force Story Error: {e}")
        except: pass
        return

    while True:
        try:
            for story in list(stories_collection.find({})):
                if datetime.now() - story.get('last_posted') > timedelta(hours=24):
                    try:
                        u = get_user_data(story['chat_id'])
                        if not u.get('session_id'): continue
                        cl = Client()
                        cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
                        cl.login_by_sessionid(u['session_id'])
                        
                        fi = bot.get_file(story['file_id'])
                        d = bot.download_file(fi.file_path)
                        tp = f"s_{story['_id']}.jpg"
                        with open(tp, 'wb') as f: f.write(d)
                        
                        success, _ = force_story_upload(cl, tp)
                        if success:
                            stories_collection.update_one({"_id": ObjectId(story['_id'])}, {"$set": {"last_posted": datetime.now()}})
                        os.remove(tp)
                    except: pass
            time.sleep(60)
        except: time.sleep(60)

threading.Thread(target=check_and_post_stories).start()

# --- Link Share Logic ---
def start_link_share_thread(message):
    threading.Thread(target=run_link_share, args=(message.chat.id, get_user_data(message.chat.id).get("session_id"), message.text)).start()
    bot.send_message(message.chat.id, "⏳ جاري المعالجة...")

def run_link_share(chat_id, session, url):
    temp_path = f"temp_{chat_id}"
    path = None
    try:
        cl = Client()
        cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
        cl.login_by_sessionid(session)
        pk = cl.story_pk_from_url(url)
        path = cl.story_download(pk, filename=temp_path)
        
        selected = get_user_data(chat_id).get("selected_ids", [])
        count = 0
        for gid in selected:
            if stop_flags.get(chat_id): break
            try:
                cl.direct_send_photo(path, thread_ids=[gid])
                count += 1
            except Exception as e:
                if "pinned" in str(e) or "ReplyMessage" in str(e):
                    count += 1
                else:
                    print(f"Error {gid}: {e}")
            time.sleep(5)
            
        bot.send_message(chat_id, f"✅ تم النشر في {count} جروب.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ حدث خطأ: {e}")
    finally:
        if path and os.path.exists(path): os.remove(path)

# --- Other Threads ---
def start_auto_reply_thread(message):
    auto_reply_active[message.chat.id] = True; stop_flags[message.chat.id] = False
    threading.Thread(target=run_auto_reply, args=(message.chat.id, get_user_data(message.chat.id).get("session_id"), message.text)).start()
    bot.send_message(message.chat.id, "✅ بدأ الرد.")

def run_auto_reply(chat_id, session, text):
    try:
        cl = Client()
        cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
        cl.login_by_sessionid(session)
        my_id = str(cl.user_id)
        replied_cache = []
        while not stop_flags.get(chat_id) and auto_reply_active.get(chat_id):
            try:
                ts = cl.direct_threads(amount=20) 
                for t in ts:
                    if stop_flags.get(chat_id): break
                    if t.is_group:
                        for m in t.messages[:5]:
                            if m.id in replied_cache or str(m.user_id) == my_id: continue
                            is_rep = False
                            try: 
                                if m.reply_to_message and str(m.reply_to_message.user_id) == my_id: is_rep = True
                            except: pass
                            if is_rep:
                                cl.direct_send(text, thread_ids=[t.id])
                                replied_cache.append(m.id)
                                time.sleep(3)
            except: time.sleep(5)
            time.sleep(15)
    except: pass

def start_broadcast_thread(message):
    threading.Thread(target=run_broadcast, args=(message.chat.id, get_user_data(message.chat.id).get("session_id"), message.text)).start()
    bot.send_message(message.chat.id, "🚀 بدأ.")

def run_broadcast(cid, sid, txt):
    try:
        cl = Client()
        cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
        cl.login_by_sessionid(sid)
        threads = get_safe_threads(cl)
        for t in threads:
            if stop_flags.get(cid): break
            try: cl.direct_send(txt, thread_ids=[t['id']]); time.sleep(10)
            except: pass
        bot.send_message(cid, "🏁 تم.")
    except: pass

def ask_time_for_dm(m):
    msg = bot.reply_to(m, "⏱ الانتظار بالدقيقة؟")
    bot.register_next_step_handler(msg, lambda mm: threading.Thread(target=run_dm_post, args=(m.chat.id, m.text, float(mm.text)*60)).start())

def run_dm_post(cid, txt, dlay):
    u = get_user_data(cid)
    cl = Client()
    cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
    cl.login_by_sessionid(u['session_id'])
    bot.send_message(cid, "🚀 بدأ.")
    for g in u.get("selected_ids", []):
        if stop_flags.get(cid): break
        try: cl.direct_send(txt, thread_ids=[g]); time.sleep(3)
        except: pass
        time.sleep(dlay)
    bot.send_message(cid, "🛑 تم.")

def start_smart_follow_thread(chat_id, session):
    stop_flags[chat_id] = False
    threading.Thread(target=run_smart_follow, args=(chat_id, session)).start()

def run_smart_follow(chat_id, session):
    try:
        cl = Client()
        cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
        cl.login_by_sessionid(session)
        for uid in cl.user_followers("460563723", amount=200):
            if stop_flags.get(chat_id): break
            try: cl.user_follow(uid); log_follow(chat_id, uid); time.sleep(0.5)
            except: time.sleep(60)
        bot.send_message(chat_id, "✅ تم.")
    except: pass

def ask_unfollow_count(message):
    try:
        threading.Thread(target=run_mass_unfollow, args=(message.chat.id, get_user_data(message.chat.id).get("session_id"), int(message.text))).start()
        bot.send_message(message.chat.id, "جاري...")
    except: bot.send_message(message.chat.id, "رقم فقط")

def run_mass_unfollow(chat_id, session, count):
    try:
        cl = Client()
        cl.set_user_agent("Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; Samsung; SM-G960F; star2lte; samsungexynos9810; en_US; 314665256)")
        cl.login_by_sessionid(session)
        my = cl.user_id
        targets = [u for u in cl.user_following(my) if u not in cl.user_followers(my)][:count]
        for uid in targets:
            if stop_flags.get(chat_id): break
            try: cl.user_unfollow(uid); time.sleep(0.5)
            except: time.sleep(60)
        bot.send_message(chat_id, "🏁 تم.")
    except: pass

print("Bot Started...")
try: bot.remove_webhook(); time.sleep(1)
except: pass
while True:
    try: bot.infinity_polling(timeout=10, long_polling_timeout=5)
    except: time.sleep(5)
