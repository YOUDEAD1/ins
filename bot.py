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
try: OWNER_ID = int(os.getenv('OWNER_ID', '0').strip())
except ValueError: OWNER_ID = 0
OWNER_USER = os.getenv('OWNER_USER', '').strip()

BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', '').strip()
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET', '').strip()

MONGO_URI = os.getenv('MONGO_URI', '').strip()
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'shop_db').strip()

GITHUB_API_KEY = os.getenv('GITHUB_API_KEY', '').strip()
GITHUB_BASE_URL = os.getenv('GITHUB_BASE_URL', 'https://api.ahsanlabs.online').strip().rstrip('/')

try: STARS_RATE = int(os.getenv('STARS_RATE', '120').strip())
except ValueError: STARS_RATE = 120

# ============================================================
# 🛡️ 2. نظام البروكسي البسيط والخفيف (يوفر RAM)
# ============================================================
# نظام بسيط: نجلب بروكسيات، نحط واحد ونجربه، لو فشل نجرب التالي.
# لا threads خلفية ضخمة، لا فحص متوازي بـ 100 worker، لا قوائم كبيرة.
# ============================================================

PROXY_SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000",
]

VERIFIED_PROXIES = []           # قائمة بسيطة من البروكسيات
PROXY_REFRESH_LOCK = threading.Lock()
LAST_PROXY_REFRESH = 0
PROXY_REFRESH_INTERVAL = 3600   # ساعة كاملة بين كل refresh (توفير RAM)
MAX_PROXIES_IN_POOL = 10        # نحتفظ بـ 10 فقط (بدل 30)


def _fetch_proxies_simple():
    """يجلب بروكسيات من المصدر الأول الذي يستجيب"""
    for source in PROXY_SOURCES:
        try:
            res = requests.get(source, timeout=8)
            if res.status_code == 200:
                lines = res.text.strip().split('\n')
                proxies = []
                for line in lines[:200]:  # نأخذ 200 فقط لتوفير RAM
                    line = line.strip()
                    if line and ':' in line and not line.startswith('#'):
                        if not line.startswith('http'):
                            line = f"http://{line}"
                        proxies.append(line)
                if proxies:
                    random.shuffle(proxies)
                    return proxies
        except Exception:
            continue
    return []


def refresh_proxies(force=False):
    """يحدث قائمة البروكسيات بشكل بسيط (بدون فحص متوازي ضخم)"""
    global VERIFIED_PROXIES, LAST_PROXY_REFRESH
    
    # ما نسوي refresh كثير
    if not PROXY_REFRESH_LOCK.acquire(blocking=False):
        return  # في refresh شغال بالفعل
    
    try:
        current_time = time.time()
        if not force and VERIFIED_PROXIES and (current_time - LAST_PROXY_REFRESH < PROXY_REFRESH_INTERVAL):
            return
        
        logger.info("🔄 جاري جلب البروكسيات...")
        new_proxies = _fetch_proxies_simple()
        
        if new_proxies:
            # نحط كل البروكسيات في الـ pool (بدون فحص مسبق)
            # سيتم اختبارها أثناء الاستخدام الفعلي
            VERIFIED_PROXIES = new_proxies[:MAX_PROXIES_IN_POOL * 5]  # 50 بروكسي كاحتياطي
            LAST_PROXY_REFRESH = current_time
            logger.info(f"✅ جلب {len(VERIFIED_PROXIES)} بروكسي.")
    except Exception as e:
        logger.error(f"❌ خطأ في جلب البروكسيات: {e}")
    finally:
        PROXY_REFRESH_LOCK.release()


def _remove_dead_proxy(proxy_url):
    """يحذف بروكسي ميت من القائمة"""
    global VERIFIED_PROXIES
    try:
        if proxy_url in VERIFIED_PROXIES:
            VERIFIED_PROXIES.remove(proxy_url)
        # لو نزل العدد كثير، نطلب تحديث (بدون thread - يتم في الـ call التالي)
        if len(VERIFIED_PROXIES) < 5:
            threading.Thread(target=refresh_proxies, args=(True,), daemon=True).start()
    except Exception:
        pass


def get_binance_client():
    """ينشئ Binance client مع بروكسي عشوائي"""
    if not VERIFIED_PROXIES:
        refresh_proxies(force=True)
    
    if VERIFIED_PROXIES:
        proxy = random.choice(VERIFIED_PROXIES)
        try:
            client = BinanceClient(
                BINANCE_API_KEY,
                BINANCE_API_SECRET,
                requests_params={
                    'proxies': {'http': proxy, 'https': proxy},
                    'timeout': 8
                }
            )
            client._used_proxy = proxy
            return client
        except Exception:
            _remove_dead_proxy(proxy)
    
    # آخر محاولة بدون بروكسي
    return BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params={'timeout': 10})


# جلب البروكسيات أول مرة عند بدء البوت (في الخلفية)
threading.Thread(target=refresh_proxies, args=(True,), daemon=True).start()


def execute_binance_call(call_fn, max_retries=10, fast_mode=False, total_timeout=8):
    """
    يحاول تنفيذ استدعاء Binance API بتجربة بروكسي تلو الآخر.
    
    fast_mode متجاهل (نستخدم نظام sequential بسيط لتوفير RAM).
    max_retries = عدد المحاولات (كل وحدة ببروكسي مختلف)
    """
    for attempt in range(max_retries):
        client = None
        try:
            client = get_binance_client()
            result = call_fn(client)
            return result
        except Exception as e:
            error_msg = str(e).lower()
            
            # لو الخطأ من البروكسي/الحظر، نحذف ونجرب التالي
            if any(kw in error_msg for kw in ['restricted', 'eligibility', 'service unavailable',
                                                'timeout', 'connection', 'proxy', 'unreachable',
                                                'max retries', 'remote disconnected']):
                if client and hasattr(client, '_used_proxy'):
                    _remove_dead_proxy(client._used_proxy)
                continue
            else:
                # خطأ مختلف (API key غلط، إلخ) - ما نعيد
                break
    
    return None

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
        if self.style: d['style'] = self.style
        if self.icon_custom_emoji_id: d['icon_custom_emoji_id'] = str(self.icon_custom_emoji_id)
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
REFERRAL_MIN_PURCHASE = 2.0  # الحد الأدنى لقيمة الشراء عشان يربح المُحيل المكافأة
temp_product = {}
temp_stock_edit = {}
temp_github_data = {} 
PROCESSING_TXS = set()
tx_lock = threading.Lock()

# ============================================================
# 🛡 حماية #3: تشفير بيانات GitHub الحساسة (AES-GCM)
# ============================================================
# نشفّر بيانات GitHub (user/pass/2FA) في الذاكرة عشان لو سُرّب الـ memory dump
# أو السيرفر، لا تكون البيانات بنص واضح.
# المفتاح يُولَّد عند بدء البوت ولا يُحفظ في أي مكان دائم.
# مرة البوت يقفل، المفتاح يضيع وكل البيانات اللي بقت ما تنفك تشفيرها (آمن).

import secrets
import base64
from hashlib import sha256

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    _CRYPTO_AVAILABLE = True
    # مفتاح عشوائي 256-bit يُولَّد عند تشغيل البوت
    _GH_ENCRYPTION_KEY = AESGCM.generate_key(bit_length=256)
    _gh_cipher = AESGCM(_GH_ENCRYPTION_KEY)
    logger.info("🔐 تم تفعيل تشفير AES-256-GCM لبيانات GitHub")
except ImportError:
    _CRYPTO_AVAILABLE = False
    _gh_cipher = None
    logger.warning("⚠️ مكتبة cryptography غير مثبتة. تشغيل: pip install cryptography")
    logger.warning("⚠️ سيتم استخدام XOR بسيط كحل بديل (أقل أمان من AES-GCM)")
    # حل بديل بسيط (أقل أمان) لو cryptography مو مثبتة
    _GH_XOR_KEY = secrets.token_bytes(64)


def encrypt_sensitive(plaintext: str) -> str:
    """تشفير نص حساس - يرجع string base64 جاهز للتخزين"""
    if not plaintext:
        return ""
    try:
        if _CRYPTO_AVAILABLE:
            # AES-GCM مع nonce عشوائي
            nonce = secrets.token_bytes(12)
            ciphertext = _gh_cipher.encrypt(nonce, plaintext.encode('utf-8'), None)
            # نخزّن nonce + ciphertext
            combined = nonce + ciphertext
            return base64.b64encode(combined).decode('ascii')
        else:
            # XOR بسيط (احتياطي)
            data = plaintext.encode('utf-8')
            encrypted = bytes(b ^ _GH_XOR_KEY[i % len(_GH_XOR_KEY)] for i, b in enumerate(data))
            return base64.b64encode(encrypted).decode('ascii')
    except Exception as e:
        logger.error(f"Encryption error: {e}")
        return ""


def decrypt_sensitive(encrypted_b64: str) -> str:
    """فك تشفير نص حساس"""
    if not encrypted_b64:
        return ""
    try:
        combined = base64.b64decode(encrypted_b64)
        if _CRYPTO_AVAILABLE:
            nonce = combined[:12]
            ciphertext = combined[12:]
            plaintext = _gh_cipher.decrypt(nonce, ciphertext, None)
            return plaintext.decode('utf-8')
        else:
            # XOR العكسي
            decrypted = bytes(b ^ _GH_XOR_KEY[i % len(_GH_XOR_KEY)] for i, b in enumerate(combined))
            return decrypted.decode('utf-8')
    except Exception as e:
        logger.error(f"Decryption error: {e}")
        return ""


def secure_wipe(data_dict: dict):
    """يمسح القيم الحساسة من dict (يستبدلها بنص عشوائي قبل الحذف)"""
    if not data_dict:
        return
    try:
        for k in list(data_dict.keys()):
            if k in ('user', 'pass', 'totp', '2fa', 'password'):
                # نستبدل القيمة بقمامة عشوائية قبل الحذف (يصعّب استرجاعها من memory)
                data_dict[k] = secrets.token_hex(32)
        data_dict.clear()
    except Exception:
        pass


def get_setting(key, default="Not Set"):
    res = db.settings.find_one({'key': key})
    return res['value'] if res else default

# ============================================================
# 👥 4.5. نظام الإحالات الجديد V2 (Real-time + جدول مستقل)
# ============================================================
# 📌 ملاحظات النظام الجديد:
# - جدول مستقل اسمه referrals_v2 (لا علاقة له بالقديم)
# - يبدأ من الصفر للجميع (لا يحسب أي إحالة قديمة)
# - المستخدم الموجود في البوت مسبقاً لا يُحسب كإحالة جديدة
# - تحديث فوري كل ثانية في الخلفية
# - 0.10$ مقابل كل 10 احالات نشطة (مكتملة الاشتراك الإجباري)
# - إذا غادر شخص ونقص العدد عن مضاعفات العشرة، يُخصم تلقائياً
# ============================================================
REF_V2_INIT_KEY = 'referrals_v2_initialized_v6'

def initialize_referrals_v2():
    """يُنفّذ مرة واحدة فقط: يمسح كل ما يخص النظام القديم ويهيّئ الجدول الجديد."""
    try:
        already_init = db.settings.find_one({'key': REF_V2_INIT_KEY})
        if already_init and already_init.get('value') == 'done':
            logger.info("✅ نظام الإحالات V2 مُهيأ مسبقاً.")
            return

        logger.info("🔄 جاري تهيئة نظام الإحالات V2 (لأول مرة)...")

        # 0. مسح المفاتيح القديمة لنسخ سابقة من التهيئة
        try:
            db.settings.delete_many({'key': {'$in': ['referrals_v2_initialized', 'referrals_v2_initialized_v2', 'referrals_v2_initialized_v3', 'referrals_v2_initialized_v4', 'referrals_v2_initialized_v5']}})
        except Exception:
            pass

        # 1. مسح كامل لجدول الإحالات الجديد (لو موجود من تجارب سابقة)
        try:
            db.referrals_v2.delete_many({})
        except Exception:
            pass

        # 2. مسح الحقول القديمة من جدول المستخدمين (referred_by القديم)
        try:
            db.users.update_many(
                {},
                {'$unset': {
                    'referred_by': "",
                    'ref_status': "",
                    'ref_earned': "",
                    'ref_v2_earned': ""
                }}
            )
        except Exception as e:
            logger.error(f"Error clearing old referral fields: {e}")
        
        # 2.5. مسح نص invite_txt المخصص القديم من custom_texts
        # عشان يستخدم النص الجديد من الكود (اللي يحتوي على 6 متغيرات بدل 2)
        try:
            db.custom_texts.delete_many({'key': 'invite_txt'})
            logger.info("✅ تم مسح النص القديم لرسالة الإحالات من custom_texts")
        except Exception as e:
            logger.error(f"Error clearing old invite_txt: {e}")

        # 3. إنشاء فهارس (Indexes) لتسريع الاستعلامات
        try:
            db.referrals_v2.create_index('referrer_id')
            db.referrals_v2.create_index('invited_id', unique=True)
            db.referrals_v2.create_index([('referrer_id', 1), ('status', 1)])
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")

        # 4. وضع علامة أن النظام تم تهيئته
        db.settings.update_one(
            {'key': REF_V2_INIT_KEY},
            {'$set': {'value': 'done', 'init_time': int(time.time())}},
            upsert=True
        )

        logger.info("✅ تم تهيئة نظام الإحالات V2 بنجاح. (كل شي يبدأ من الصفر)")
    except Exception as e:
        logger.error(f"❌ فشل تهيئة نظام الإحالات V2: {e}")

# نُفّذها فوراً عند بدء البوت
initialize_referrals_v2()


def register_new_referral(invited_id, referrer_id):
    """
    تسجيل إحالة جديدة في الجدول الجديد - محمي من race conditions.
    شروط القبول:
    - المستخدم المدعو (invited) ما يكون مسجلاً من قبل
    - الـ referrer_id ما يكون نفسه invited_id
    - ما يكون مسجلاً في جدول الإحالات من قبل
    
    🛡 الحماية:
    - يستخدم unique index على invited_id لمنع التكرار
    - DuplicateKeyError = الإحالة مسجلة بالفعل (آمن)
    """
    try:
        invited_id = int(invited_id)
        referrer_id = int(referrer_id)

        if invited_id == referrer_id:
            return False

        if not db.users.find_one({'user_id': referrer_id}):
            return False

        # 🛡 محاولة الإدراج مباشرة — الـ unique index بيمنع التكرار
        # هذا أسرع وأأمن من find_one ثم insert (ما فيه race condition)
        try:
            db.referrals_v2.insert_one({
                'invited_id': invited_id,
                'referrer_id': referrer_id,
                'status': 'pending',
                'created_at': int(time.time()),
                'updated_at': int(time.time())
            })
            return True
        except Exception as dup_err:
            # DuplicateKeyError - الإحالة مسجلة من قبل (طبيعي، نتجاهل)
            error_msg = str(dup_err).lower()
            if 'duplicate' in error_msg or 'e11000' in error_msg:
                return False
            # خطأ آخر — نسجله
            logger.error(f"Insert referral error: {dup_err}")
            return False
    except Exception as e:
        logger.error(f"Error registering referral: {e}")
        return False


def mark_referral_status(invited_id, new_status):
    """
    يحدّث حالة المُدعَى وبعدها يحدّث رصيد المُحيل تلقائياً.
    🛡 محمي من race conditions:
    - يستخدم find_one_and_update atomic
    - يحدّث فقط لو الحالة الحالية فعلاً تختلف عن الجديدة
    """
    try:
        invited_id = int(invited_id)
        
        # 🛡 atomic update — يحدّث فقط لو الحالة تختلف
        # هذا يمنع تحديثات مكررة من threads متعددة
        result = db.referrals_v2.find_one_and_update(
            {
                'invited_id': invited_id,
                'status': {'$ne': new_status}  # فقط لو الحالة الحالية مختلفة
            },
            {
                '$set': {
                    'status': new_status,
                    'updated_at': int(time.time())
                }
            },
            return_document=False  # نريد القيمة القديمة
        )
        
        # result = None يعني إما الإحالة غير موجودة أو الحالة نفسها (طبيعي)
        if result is None:
            return
        
        # تحديث رصيد المُحيل (آمن - update_referrer_balance بحد ذاته atomic)
        update_referrer_balance(result['referrer_id'])
    except Exception as e:
        logger.error(f"Error marking referral status: {e}")


def update_referrer_balance(referrer_id):
    """
    تحديث رصيد المُحيل بناءً على النشطين الحاليين.
    🛡 محمي من race conditions باستخدام lock لكل referrer_id
    🔔 يرسل إشعار للوق عند وصول إنجاز جديد (10، 20، 30 إحالة...)
    """
    try:
        rid = int(referrer_id)
        
        # 🛡 lock على هذا الـ referrer لمنع تحديثين بنفس الوقت
        with _get_referrer_lock(rid):
            active_count = db.referrals_v2.count_documents({'referrer_id': rid, 'status': 'active'})
            expected = round((active_count // 10) * REFERRAL_REWARD, 2)

            referrer = db.users.find_one({'user_id': rid})
            if not referrer:
                return

            current_earned = round(float(referrer.get('ref_v2_earned', 0.0)), 2)

            if expected != current_earned:
                diff = round(expected - current_earned, 2)
                # 🛡 atomic update مع شرط ref_v2_earned القديم
                # لو حد ثاني عدّل في نفس الوقت، الـ update هذا ما ينفّذ
                update_result = db.users.find_one_and_update(
                    {
                        'user_id': rid,
                        'ref_v2_earned': current_earned  # شرط optimistic locking
                    },
                    {
                        '$inc': {'balance': diff},
                        '$set': {'ref_v2_earned': expected}
                    },
                    return_document=True
                )
                
                # 🔔 إذا التحديث نجح + هذا إنجاز جديد (وصول لمضاعف 10) → إشعار اللوق
                if update_result is not None and diff > 0:
                    # diff > 0 يعني إن المُحيل ربح مكافأة جديدة (مو خسارة)
                    try:
                        log_ch = get_setting('log_channel')
                        if log_ch and log_ch != "Not Set":
                            referrer_display = obscure_text(referrer.get('username') or str(rid))
                            
                            # 🆕 النص الافتراضي (3 متغيرات فقط - بدون إجمالي الأرباح)
                            log_text = LANG['en']['log_ref_milestone'].format(
                                referrer_display,
                                active_count,
                                f"{diff:.2f}"
                            )
                            
                            # نشيك على النص المخصص لو الأدمن عدّله من CMS
                            custom_milestone = db.custom_texts.find_one({'lang': 'en', 'key': 'log_ref_milestone'})
                            if custom_milestone and custom_milestone.get('value'):
                                try:
                                    log_text = custom_milestone['value'].format(
                                        referrer_display,
                                        active_count,
                                        f"{diff:.2f}"
                                    )
                                except:
                                    pass  # لو في خطأ format، نستخدم الافتراضي
                            
                            bot.send_message(log_ch, log_text, parse_mode="HTML")
                    except Exception as log_err:
                        logger.debug(f"Milestone log error: {log_err}")
    except Exception as e:
        logger.error(f"Error updating referrer balance: {e}")


def award_purchase_referral_reward(buyer_uid, product_name="", purchase_amount=0):
    """
    🎁 مكافأة إحالة عند الشراء:
    لو هذا المستخدم (buyer_uid) جاي من إحالة شخص، نعطي ذاك الشخص 0.10$
    تشتغل في كل عملية شراء (متراكمة).
    
    ⚠️ الشرط: قيمة عملية الشراء لازم تكون > 2$ (REFERRAL_MIN_PURCHASE)
    
    🛡 محمي من race conditions بـ:
    - find_one_and_update atomic للرصيد
    - تسجيل المكافأة في جدول purchase_rewards لتتبعها (وكدليل)
    """
    try:
        buyer_uid = int(buyer_uid)
        purchase_amount = round(float(purchase_amount), 2)
        
        # ⚠️ شرط الحد الأدنى: قيمة الشراء لازم تكون أكثر من 2$
        if purchase_amount <= REFERRAL_MIN_PURCHASE:
            return False  # الشراء أقل من الحد الأدنى - لا مكافأة
        
        # نشوف هل المشتري عنده مُحيل في النظام الجديد
        ref_record = db.referrals_v2.find_one({'invited_id': buyer_uid})
        if not ref_record:
            return False  # المشتري ما له مُحيل
        
        referrer_id = int(ref_record.get('referrer_id', 0))
        if referrer_id <= 0:
            return False
        
        # تأكد إن المُحيل لازال موجود وما هو محظور
        referrer = db.users.find_one({'user_id': referrer_id})
        if not referrer or referrer.get('is_banned') == 1:
            return False
        
        # 🛡 منح المكافأة atomic
        result = db.users.find_one_and_update(
            {'user_id': referrer_id},
            {
                '$inc': {
                    'balance': REFERRAL_REWARD,
                    'ref_v2_purchase_earned': REFERRAL_REWARD  # مجموع أرباح المشتريات
                }
            },
            return_document=True
        )
        
        if result is None:
            return False
        
        # تسجيل المكافأة كسجل دائم (للأرشيف والتتبع)
        try:
            db.purchase_rewards.insert_one({
                'referrer_id': referrer_id,
                'buyer_id': buyer_uid,
                'amount': REFERRAL_REWARD,
                'product': product_name,
                'purchase_amount': purchase_amount,
                'timestamp': int(time.time())
            })
        except Exception:
            pass
        
        # 🎉 إشعار خاص للمُحيل في الشات (بالإنجليزي + قابل للتعديل من CMS)
        try:
            new_balance = float(result.get('balance', 0))
            buyer_data = db.users.find_one({'user_id': buyer_uid})
            buyer_display = obscure_text(buyer_data.get('username') or str(buyer_uid)) if buyer_data else "***"
            
            # 🆕 نستخدم نص CMS قابل للتعديل (إنجليزي افتراضياً)
            celebration = LANG['en']['ref_purchase_dm'].format(
                buyer_display,
                f"{REFERRAL_REWARD:.2f}",
                f"{new_balance:.2f}"
            )
            
            # نشيك على النص المخصص لو الأدمن عدّله من CMS
            custom_dm = db.custom_texts.find_one({'lang': 'en', 'key': 'ref_purchase_dm'})
            if custom_dm and custom_dm.get('value'):
                try:
                    celebration = custom_dm['value'].format(
                        buyer_display,
                        f"{REFERRAL_REWARD:.2f}",
                        f"{new_balance:.2f}"
                    )
                except:
                    pass  # لو في خطأ format، نستخدم الافتراضي
            
            bot.send_message(referrer_id, celebration, parse_mode="HTML")
        except Exception as notify_err:
            logger.debug(f"Couldn't notify referrer {referrer_id}: {notify_err}")
        
        logger.info(f"🎁 مكافأة شراء: {referrer_id} ربح ${REFERRAL_REWARD} من شراء {buyer_uid}")
        
        # 🔔 إشعار قناة اللوق (مع تخفي الأسماء بنجمات)
        # 🔔 إشعار قناة اللوق (مختصر + بالإنجليزي)
        try:
            log_ch = get_setting('log_channel')
            if log_ch and log_ch != "Not Set":
                # نخفي يوزر المُحيل بنجمات
                referrer_display = obscure_text(referrer.get('username') or str(referrer_id))
                
                # 🆕 نستخدم النص الإنجليزي المختصر (بدون ذكر السبب أو المُدعى)
                log_text = LANG['en']['log_ref_purchase'].format(
                    referrer_display,
                    f"{REFERRAL_REWARD:.2f}"
                )
                
                # نشيك على النص المخصص لو الأدمن عدّله من CMS
                custom_log = db.custom_texts.find_one({'lang': 'en', 'key': 'log_ref_purchase'})
                if custom_log and custom_log.get('value'):
                    try:
                        log_text = custom_log['value'].format(
                            referrer_display,
                            f"{REFERRAL_REWARD:.2f}"
                        )
                    except:
                        pass  # لو في خطأ format، نستخدم الافتراضي
                
                bot.send_message(log_ch, log_text, parse_mode="HTML")
        except Exception as log_err:
            logger.debug(f"Log channel notification error: {log_err}")
        
        return True
    except Exception as e:
        logger.error(f"Error in award_purchase_referral_reward: {e}")
        return False


# 🛡 قفل لكل referrer_id (للحماية الإضافية)
_referrer_locks = {}
_referrer_locks_master = threading.Lock()

def _get_referrer_lock(referrer_id):
    """يرجع lock مخصص لكل referrer لتفادي race conditions"""
    with _referrer_locks_master:
        if referrer_id not in _referrer_locks:
            _referrer_locks[referrer_id] = threading.Lock()
        return _referrer_locks[referrer_id]


def get_ref_counts(referrer_id):
    """يرجع (pending, active, left, total) للمُحيل."""
    try:
        rid = int(referrer_id)
        pending = db.referrals_v2.count_documents({'referrer_id': rid, 'status': 'pending'})
        active = db.referrals_v2.count_documents({'referrer_id': rid, 'status': 'active'})
        left = db.referrals_v2.count_documents({'referrer_id': rid, 'status': 'left'})
        total = pending + active + left
        return pending, active, left, total
    except Exception:
        return 0, 0, 0, 0


def background_referral_checker_v2():
    """
    فاحص خلفي ذكي - يتجنب Telegram Rate Limit
    
    🛡 الاستراتيجية الجديدة:
    - يفحص الإحالات على دفعات صغيرة (10 إحالات كل دورة)
    - فاصل 100ms بين كل فحص (آمن من Rate Limit)
    - دورة كاملة كل ~30 ثانية بدلاً من ثانية واحدة
    - يفحص بالأولوية: pending أولاً، ثم active (للتحقق من المغادرة)
    - يتجاهل left (محسوم - ما يرجعون)
    """
    BATCH_SIZE = 10          # عدد الإحالات في كل دفعة
    DELAY_BETWEEN_CHECKS = 0.1   # 100ms بين كل فحص (آمن جداً)
    DELAY_BETWEEN_CYCLES = 30    # 30 ثانية بين الدورات الكاملة
    
    while True:
        try:
            # 🛡 نفحص فقط pending و active (نتجاهل left - حسموا)
            # pending: نريد ترقيتهم لـ active لو اشتركوا
            # active: نريد تنزيلهم لـ left لو غادروا
            cursor = db.referrals_v2.find({
                'status': {'$in': ['pending', 'active']}
            })
            
            batch_count = 0
            for r in cursor:
                inv_uid = r.get('invited_id')
                if not inv_uid:
                    continue
                
                current_status = r.get('status', 'pending')
                
                try:
                    is_subbed = check_forced_sub(int(inv_uid))
                except Exception:
                    # خطأ في الفحص - نتركه للدورة التالية
                    time.sleep(DELAY_BETWEEN_CHECKS)
                    continue
                
                # تحديد الحالة الجديدة
                if is_subbed:
                    new_status = 'active'
                else:
                    new_status = 'left' if current_status == 'active' else 'pending'
                
                # تحديث فقط لو تغيرت الحالة
                if new_status != current_status:
                    mark_referral_status(inv_uid, new_status)
                
                # فاصل صغير بين الفحوصات (يحمي من Rate Limit)
                time.sleep(DELAY_BETWEEN_CHECKS)
                
                batch_count += 1
                # كل 10 إحالات، نعمل وقفة أطول
                if batch_count >= BATCH_SIZE:
                    batch_count = 0
                    time.sleep(1)  # ثانية وقفة بعد كل 10
        except Exception as e:
            logger.error(f"Background ref checker error: {e}")
        
        # دورة كاملة كل 30 ثانية (آمن وكافي)
        time.sleep(DELAY_BETWEEN_CYCLES)


# 🆕 فاحص فوري للإحالات الجديدة عند الـ /start
# هذا الـ thread الرئيسي يدور بهدوء، والتحديث الفوري يحصل في start_handler
threading.Thread(target=background_referral_checker_v2, daemon=True).start()

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
            bot.send_message(uid, "🎉 <b>اكتمل التفعيل بنجاح!</b>\nتم خصم الرصيد وتوثيق الطلب. يمكنك رؤية الإيصال في المشتريات.", parse_mode="HTML")
            
            log_ch = get_setting('log_channel')
            u_data = db.users.find_one({'user_id': uid})
            obs_user = obscure_text(u_data.get('username') or str(uid))
            if log_ch and log_ch != "Not Set":
                try: 
                    # 🆕 النص من CMS (قابل للتعديل)
                    gemini_msg = LANG['en']['log_gemini'].format(obs_user)
                    custom_g = db.custom_texts.find_one({'lang': 'en', 'key': 'log_gemini'})
                    if custom_g and custom_g.get('value'):
                        try:
                            gemini_msg = custom_g['value'].format(obs_user)
                        except: pass
                    bot.send_message(log_ch, gemini_msg, parse_mode="HTML")
                except: pass
            
            # 🎁 منح مكافأة الإحالة لو هذا المستخدم جاي من إحالة
            try:
                award_purchase_referral_reward(uid, "Gemini Advanced", price)
            except Exception as ref_err:
                logger.error(f"Error awarding referral on Gemini purchase: {ref_err}")

            ACTIVE_GEMINI_SESSION = None
            process_next_gemini()
            
        elif "❌ Status: FAILED" in text or "❌ Error" in text:
            db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
            bot.send_message(uid, "❌ <b>فشلت العملية وتم إرجاع رصيدك!</b>\nتأكد من تفعيل (التحقق بخطوتين) والبيانات الصحيحة.", parse_mode="HTML")
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
            bot.send_message(uid, f"❌ <b>فشل الاتصال بمزود الخدمة. تم إرجاع رصيدك.</b>\nالخطأ البرمجي: <code>{e}</code>", parse_mode="HTML")
            ACTIVE_GEMINI_SESSION = None
            process_next_gemini()
            
    if client and USERBOT_LOOP: asyncio.run_coroutine_threadsafe(_init_chat(), USERBOT_LOOP)
    else:
        db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
        bot.send_message(uid, "❌ <b>النظام غير متصل (اليوزربوت معطل).</b> تم إرجاع رصيدك.", parse_mode="HTML")
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
        bot.send_message(uid, f"⏳ <b>تم وضعك في طابور الانتظار!</b>\nدورك رقم: {len(GEMINI_QUEUE)}\nسيتم بدء التفعيل تلقائياً عند وصول دورك.", parse_mode="HTML")

# ============================================================
# 🌍 6. القواميس الأساسية والنصوص الافتراضية
# ============================================================
DEFAULT_BUTTONS = {
    'ar': {
        'btn_products': '🔵 المنتجات',
        'btn_deposit': '💳 شحن الرصيد',
        'btn_profile': '👤 الملف الشخصي',
        'btn_invite': '👥 الإحالات',
        'btn_support': '👨‍💻 الدعم الفني',
        'btn_lang': '🌐 English',
        'btn_admin': '👑 لوحة الإدارة',
        'btn_terms': '📜 شروط الاستخدام',
        'terms_content': "📜 <b>شروط استخدام المتجر</b>\n\n━━━━━━━━━━━━━━\n\n<i>لم يتم إضافة شروط الاستخدام بعد.</i>\n\n<i>سيتم تحديث هذه الصفحة قريباً من قبل الإدارة.</i>\n\n━━━━━━━━━━━━━━\n\n💬 <i>لأي استفسار، تواصل مع الإدارة.</i>",
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
        'btn_products': '🔵 Products',
        'btn_deposit': '💳 Deposit',
        'btn_profile': '👤 Profile',
        'btn_invite': '👥 Referrals',
        'btn_support': '👨‍💻 Support',
        'btn_lang': '🌐 العربية',
        'btn_admin': '👑 Admin Panel',
        'btn_terms': '📜 Terms of Use',
        'terms_content': "📜 <b>Store Terms of Use</b>\n\n━━━━━━━━━━━━━━\n\n<i>Terms of use have not been added yet.</i>\n\n<i>This page will be updated soon by the administration.</i>\n\n━━━━━━━━━━━━━━\n\n💬 <i>For any inquiries, contact support.</i>",
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
        'new_product': "🎉 <b>منتج جديد متاح في المتجر!</b> 🚀\n\n🛍 <b>المنتج:</b> {}\n💰 <b>السعر:</b> <b>${}</b>\n🚚 <b>نوع التسليم:</b> {}\n\n📝 <b>الوصف:</b>\n{}\n\n<i>سارع بزيارة المتجر والاستفادة من المنتج الجديد! 🛡️</i>",
        
        # 🆕 إشعارات قناة اللوق لنظام الإحالات (دائماً بالإنجليزي - مختصرة - تشجيعية)
        'log_ref_purchase': "💰 <b>Referral Bonus!</b> 🎉\n\n👤 <b>User:</b> {}\n💵 <b>Earned:</b> <code>+${}</code>\n\n<i>Keep inviting friends to earn more! 🚀</i>",
        
        'log_ref_milestone': "🏆 <b>Referral Achievement Unlocked!</b> 🎊\n\n👤 <b>User:</b> {}\n👥 <b>Active Invites:</b> <b>{}</b>\n💰 <b>Reward:</b> <code>+${}</code>\n\n<i>Invite your friends and earn rewards too! 🔥</i>",
        
        # 🆕 إشعار خاص للمُحيل لما يشتري شخص من دعواته (في خاص البوت)
        'ref_purchase_dm': "🎉 <b>Great News!</b> 💰\n\n🛒 One of your invited friends (<b>{}</b>) just made a purchase!\n\n💵 <b>You earned:</b> <code>+${}</code>\n💼 <b>Your new balance:</b> <b>${}</b>\n\n🔥 <i>Keep inviting friends — every purchase they make earns you more money!</i>\n🚀 <i>Share your link and watch your balance grow!</i>",
        
        # 🆕 إشعارات قناة اللوق العامة (قابلة للتعديل من CMS)
        'log_purchase': "🛒 <b>New Purchase!</b> 🛍\n\n👤 <b>User:</b> {}\n📦 <b>Product:</b> {}\n🔢 <b>QTY:</b> {}\n\n<i>Thank you for choosing us 🛡️</i>",
        
        'log_deposit': "💳 <b>New Deposit!</b> 💵\n\n👤 <b>User:</b> {}\n💰 <b>Amount:</b> <b>${}</b>\n🟢 <b>Method:</b> {}\n\n<i>Processed automatically ⚡</i>",
        
        # 🆕 إشعارات تفعيل المنتجات الخاصة
        'log_gemini': "✨ <b>New Gemini Advanced Activation!</b> 🚀\n\n👤 <b>Account:</b> {}\n✅ <b>Status:</b> Successfully Activated\n\n<i>Activated automatically via Bot ⚡</i>",
        
        'log_github': "🎓 <b>New GitHub Student Activation!</b> 🚀\n\n👤 <b>Account:</b> {}\n✅ <b>Status:</b> Successfully Activated\n\n<i>Activated automatically via Bot ⚡</i>",
        'price_drop': "📉 <b>تخفيض مذهل!</b> 🔥\n\nالمنتج: <b>{}</b>\nالسعر القديم: <strike>${}</strike>\nالسعر الجديد: <b>${}</b> فقط!\n\nسارع بالشراء الآن من المتجر!",
        'profile_txt': "👤 <b>ملفك الشخصي</b>\n\n🆔 الأيدي: <code>{}</code>\n👤 الاسم: <b>{}</b>\n💰 الرصيد: <b>${:.2f}</b>\n✅ المشتريات: <b>{}</b>\n📦 إجمالي الشحن: <b>${:.2f}</b>",
        'invite_txt': "💎 <b>نظام الإحالات</b>\n\n━━━━━━━━━━━━━━\n📊 <b>إحصائياتك المباشرة</b>\n━━━━━━━━━━━━━━\n\n👥 <b>الزيارات:</b>  <b>{}</b>\n⏳ <b>المعلق:</b>  <b>{}</b>\n✅ <b>النشط:</b>  <b>{}</b>\n❌ <b>غادر:</b>  <b>{}</b>\n\n💰 <b>أرباحك:</b>  <code>${:.2f}</code>\n\n━━━━━━━━━━━━━━\n🔗 <b>رابطك:</b>\n<code>https://t.me/{}?start={}</code>\n\n━━━━━━━━━━━━━━\n🎁 <b>طريقتين للربح:</b>\n\n🔥 كل <b>10</b> اشتراكات نشطة = <b>$0.10</b>\n💸 شراء صديقك > <b>$2</b> = <b>$0.10</b>\n\n⚡ <i>التحديث فوري</i>",
        'dep_choose': "💳 <b>اختر طريقة الدفع المناسبة:</b>",
        'dep_pay': "🟡 <b>Binance Pay</b>\n\nأرسل المبلغ إلى الـ ID التالي:\n🆔 Binance ID: <code>{}</code>\n\n⚠️ أرسل <b>رقم العملية (Order ID)</b> كنص هنا.",
        'dep_usdt': "🟢 <b>شحن عبر USDT (TRC-20)</b>\n\nالمحفظة:\n<code>{}</code>\n\n⚠️ أرسل <b>الهاش (TxID)</b> كنص هنا.",
        'dep_ltc': "🔵 <b>شحن عبر Litecoin (LTC)</b>\n\nالمحفظة:\n<code>{}</code>\n\n⚠️ أرسل <b>الهاش (TxID)</b> كنص هنا.",
        'tx_used': "⚠️ <b>عذراً، هذا الرقم مستخدم مسبقاً!</b>",
        'crypto_checking': "⏳ <b>جاري الفحص بأمان...</b>",
        'dep_success': "✅ <b>اكتمل الإيداع بنجاح!</b>\nتم إضافة <b>${:.2f}</b> إلى رصيدك.",
        'dep_fail': "❌ <b>لم نجد العملية!</b> تأكد من صحة الرقم وأنه نص.",
        'dep_pending': "⏳ <b>قيد المعالجة!</b> لم يتم التأكيد في البلوكتشين بعد.",
        'history_title': "📜 <b>سجلاتك المالية (أحدث 5 عمليات):</b>",
        'no_hist': "📭 لا توجد سجلات حتى الآن.",
        'buy_success': "✅ <b>تم الشراء بنجاح!</b>\n\nأكوادك جاهزة:\n{}\n\n<i>شكراً لاختيارك متجرنا 🛡️</i>",
        'no_balance': "❌ <b>رصيدك غير كافٍ!</b> يرجى الشحن أولاً.", 'out_stock': "❌ <b>نفد المخزون!</b>",
        'must_join': "🔒 <b>يجب عليك الاشتراك في قنواتنا أولاً:</b>",
        'qty_prompt': "🔢 <b>أرسل الكمية (أرقام فقط):</b>",
        'qty_invalid': "❌ <b>رقم غير صحيح!</b>",
        'qty_not_enough': "❌ <b>المتوفر فقط {} قطعة!</b>",
        'banned': "❌ <b>تم حظرك من البوت.</b>",
        
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
        'new_product': "🎉 <b>New Product Available!</b> 🚀\n\n🛍 <b>Product:</b> {}\n💰 <b>Price:</b> <b>${}</b>\n🚚 <b>Delivery:</b> {}\n\n📝 <b>Description:</b>\n{}\n\n<i>Visit the shop now and check out the new product! 🛡️</i>",
        
        # 🆕 Log channel notifications for referral system (always English, short, motivational)
        'log_ref_purchase': "💰 <b>Referral Bonus!</b> 🎉\n\n👤 <b>User:</b> {}\n💵 <b>Earned:</b> <code>+${}</code>\n\n<i>Keep inviting friends to earn more! 🚀</i>",
        
        'log_ref_milestone': "🏆 <b>Referral Achievement Unlocked!</b> 🎊\n\n👤 <b>User:</b> {}\n👥 <b>Active Invites:</b> <b>{}</b>\n💰 <b>Reward:</b> <code>+${}</code>\n\n<i>Invite your friends and earn rewards too! 🔥</i>",
        
        # 🆕 Private DM to inviter when their invitee makes a purchase
        'ref_purchase_dm': "🎉 <b>Great News!</b> 💰\n\n🛒 One of your invited friends (<b>{}</b>) just made a purchase!\n\n💵 <b>You earned:</b> <code>+${}</code>\n💼 <b>Your new balance:</b> <b>${}</b>\n\n🔥 <i>Keep inviting friends — every purchase they make earns you more money!</i>\n🚀 <i>Share your link and watch your balance grow!</i>",
        
        # 🆕 General log channel notifications (editable via CMS)
        'log_purchase': "🛒 <b>New Purchase!</b> 🛍\n\n👤 <b>User:</b> {}\n📦 <b>Product:</b> {}\n🔢 <b>QTY:</b> {}\n\n<i>Thank you for choosing us 🛡️</i>",
        
        'log_deposit': "💳 <b>New Deposit!</b> 💵\n\n👤 <b>User:</b> {}\n💰 <b>Amount:</b> <b>${}</b>\n🟢 <b>Method:</b> {}\n\n<i>Processed automatically ⚡</i>",
        
        # 🆕 Special product activation logs
        'log_gemini': "✨ <b>New Gemini Advanced Activation!</b> 🚀\n\n👤 <b>Account:</b> {}\n✅ <b>Status:</b> Successfully Activated\n\n<i>Activated automatically via Bot ⚡</i>",
        
        'log_github': "🎓 <b>New GitHub Student Activation!</b> 🚀\n\n👤 <b>Account:</b> {}\n✅ <b>Status:</b> Successfully Activated\n\n<i>Activated automatically via Bot ⚡</i>",
        'price_drop': "📉 <b>Massive Price Drop!</b> 🔥\n\nProduct: <b>{}</b>\nOld Price: <strike>${}</strike>\nNew Price: <b>${}</b>!\n\n<i>Buy now!</i>",
        'profile_txt': "👤 <b>Your Profile</b>\n\n🆔 ID: <code>{}</code>\n👤 Name: <b>{}</b>\n💰 Balance: <b>${:.2f}</b>\n✅ Purchases: <b>{}</b>\n📦 Total Deposited: <b>${:.2f}</b>",
        'invite_txt': "💎 <b>Referral System</b>\n\n━━━━━━━━━━━━━━\n📊 <b>Your Live Stats</b>\n━━━━━━━━━━━━━━\n\n👥 <b>Clicks:</b>  <b>{}</b>\n⏳ <b>Pending:</b>  <b>{}</b>\n✅ <b>Active:</b>  <b>{}</b>\n❌ <b>Left:</b>  <b>{}</b>\n\n💰 <b>Earnings:</b>  <code>${:.2f}</code>\n\n━━━━━━━━━━━━━━\n🔗 <b>Your Link:</b>\n<code>https://t.me/{}?start={}</code>\n\n━━━━━━━━━━━━━━\n🎁 <b>Two Ways to Earn:</b>\n\n🔥 Every <b>10</b> active joins = <b>$0.10</b>\n💸 Friend buys > <b>$2</b> = <b>$0.10</b>\n\n⚡ <i>Real-time updates</i>",
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
        'no_balance': "❌ <b>Low balance!</b> Please deposit.", 'out_stock': "❌ <b>Out of stock!</b>",
        'must_join': "🔒 <b>You must join our channels first:</b>",
        'qty_prompt': "🔢 <b>Enter quantity:</b>",
        'qty_invalid': "❌ <b>Invalid number!</b>",
        'qty_not_enough': "❌ <b>Only {} pieces available!</b>",
        'banned': "❌ <b>You are banned.</b>",
        
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

def clean_old_emojis(text):
    old_emojis = ['🛒', '💳', '👤', '👥', '👨‍💻', '🌐', '👑', '⭐️', '🟡', '🟢', '💎', '🔵', '🔴', '🛍', '📄', '🎓', '✨', '🔄', '🏠', '🔙', '✅', '📦', '✏️', '🎛', '📝', '🚚', '💰', '📊', '📉', '🔔']
    for emj in old_emojis:
        text = text.replace(emj, '')
    return text.strip()

def safe_translate_for_cms(text, target_lang='en'):
    """
    ترجمة آمنة تحافظ على:
    - Premium Emojis (<tg-emoji>)
    - HTML tags (<b>, <i>, <code>, <blockquote>, etc.)
    - Placeholders {} و {name}
    - URLs
    - الإيموجيات العادية
    - 🆕 الأسطر الجديدة والمسافات الفارغة (التنسيق البصري)
    """
    if not text or not text.strip():
        return text
    
    try:
        # 🆕 الاستراتيجية الجديدة: نترجم سطر بسطر
        # هذا يحافظ على كل الفواصل والأسطر الفارغة بالضبط
        lines = text.split('\n')
        translated_lines = []
        
        for line in lines:
            # 🛡 الأسطر الفارغة نتركها كما هي
            if not line.strip():
                translated_lines.append(line)  # سطر فارغ يبقى فارغ
                continue
            
            # 🛡 الأسطر اللي فيها بس فواصل بصرية (━━━ أو ─── أو ═══) نتركها
            if re.match(r'^[━─═\s]+$', line):
                translated_lines.append(line)
                continue
            
            # 🛡 لو السطر بدون أي نص قابل للترجمة (بس HTML tags + رموز)
            # نتحقق إن فيه أي حرف عربي أو إنجليزي حقيقي
            text_only = re.sub(r'<[^>]+>', '', line)  # احذف HTML
            text_only = re.sub(r'<tg-emoji[^>]*>.*?</tg-emoji>', '', text_only, flags=re.DOTALL)
            text_only = re.sub(r'\{[^}]*\}', '', text_only)  # احذف placeholders
            text_only = re.sub(r'https?://\S+', '', text_only)  # احذف URLs
            text_only = re.sub(r'[━─═]+', '', text_only)  # احذف فواصل
            
            # لو ما بقي حروف أو أرقام عربية/إنجليزية، نتركها كما هي
            if not re.search(r'[a-zA-Z\u0600-\u06FF\u0750-\u077F]', text_only):
                translated_lines.append(line)
                continue
            
            # 🌐 نترجم السطر مع حماية كل العناصر الحساسة
            translated_line = _translate_single_line(line, target_lang)
            translated_lines.append(translated_line)
        
        # 🛡 نجمع الأسطر كما كانت بالضبط (مع كل الـ \n)
        result = '\n'.join(translated_lines)
        
        # 🛡 فحص نهائي للسلامة
        # 1. عدد {} placeholders (هذا الفحص صارم - مهم)
        original_placeholders = len(re.findall(r'\{[^}]*\}', text))
        translated_placeholders = len(re.findall(r'\{[^}]*\}', result))
        if original_placeholders != translated_placeholders:
            logger.warning(
                f"⚠️ Translation lost placeholders! "
                f"Original: {original_placeholders}, Translated: {translated_placeholders}. "
                f"Returning original text."
            )
            return text
        
        # 2. عدد <tg-emoji> (هذا الفحص صارم - مهم للإيموجي المميز)
        original_emojis = len(re.findall(r'<tg-emoji', text))
        translated_emojis = len(re.findall(r'<tg-emoji', result))
        if original_emojis != translated_emojis:
            logger.warning(
                f"⚠️ Translation lost premium emojis! "
                f"Original: {original_emojis}, Translated: {translated_emojis}. "
                f"Trying to fix by appending missing emojis..."
            )
            # 🆕 لو فقدنا إيموجيات، نحاول نضيفها في نهاية النص بدل ما نرفض الترجمة كاملاً
            try:
                # نجلب كل tg-emoji tags من النص الأصلي
                original_emoji_tags = re.findall(r'<tg-emoji[^>]*>.*?</tg-emoji>', text, re.DOTALL)
                translated_emoji_set = set(re.findall(r'<tg-emoji[^>]*>.*?</tg-emoji>', result, re.DOTALL))
                
                # نضيف اللي مفقود في نهاية النص
                missing_emojis = []
                for emoji_tag in original_emoji_tags:
                    if emoji_tag not in translated_emoji_set:
                        missing_emojis.append(emoji_tag)
                
                if missing_emojis:
                    result = result + ' ' + ' '.join(missing_emojis)
                    logger.info(f"✅ Recovered {len(missing_emojis)} missing emojis")
            except Exception as fix_err:
                logger.error(f"Failed to recover emojis: {fix_err}")
                return text
        
        # 3. HTML tags متوازنة (هذا الفحص أصبح تحذيري فقط - مو يرفض الترجمة)
        unbalanced_tags = []
        for tag in ['b', 'i', 'u', 's', 'code', 'blockquote', 'pre']:
            open_count = len(re.findall(rf'<{tag}[^>]*>', result, re.IGNORECASE))
            close_count = len(re.findall(rf'</{tag}>', result, re.IGNORECASE))
            if open_count != close_count:
                unbalanced_tags.append(f"<{tag}>")
                # نحاول نصلح: نضيف tags إغلاق ناقصة في النهاية
                if open_count > close_count:
                    diff = open_count - close_count
                    result = result + (f'</{tag}>' * diff)
                elif close_count > open_count:
                    diff = close_count - open_count
                    result = (f'<{tag}>' * diff) + result
        
        if unbalanced_tags:
            logger.warning(f"⚠️ Fixed unbalanced tags: {unbalanced_tags}")
        
        return result
    except Exception as e:
        logger.error(f"Safe translation error: {e}")
        return text


def _translate_single_line(line, target_lang='en'):
    """
    يترجم سطر واحد مع حماية HTML والـ Premium Emojis والـ Placeholders.
    
    🆕 الطريقة الذكية الجديدة:
    - بدل ما نحمي <b> و </b> منفصلين (فيحط بينهم PROTECT يكسر الجملة)
    - نحمي الـ HTML attributes فقط، ونخلي الكلمات داخلها تترجم
    - مثلاً: <b>الزيارات</b> → <b>Clicks</b> (تترجم الكلمة وتبقي الـ tag)
    """
    try:
        protected_items = []
        
        def protect(match):
            protected_items.append(match.group(0))
            idx = len(protected_items) - 1
            # نستخدم marker قصير وبسيط ما يكسر السياق
            return f"@@{idx}@@"
        
        temp_line = line
        
        # 🛡 ترتيب الحماية (الأخص أولاً):
        # 1. Premium Emojis (كتلة كاملة)
        temp_line = re.sub(r'<tg-emoji\s+emoji-id="[^"]*">.*?</tg-emoji>', protect, temp_line)
        # 2. Code/Pre blocks (محتواها ما يترجم)
        temp_line = re.sub(r'<code>.*?</code>', protect, temp_line, flags=re.DOTALL)
        temp_line = re.sub(r'<pre>.*?</pre>', protect, temp_line, flags=re.DOTALL)
        # 3. URLs
        temp_line = re.sub(r'https?://[^\s<>]+', protect, temp_line)
        # 4. Placeholders {} و {name} و {:.2f}
        temp_line = re.sub(r'\{[^}]*\}', protect, temp_line)
        # 5. الفواصل البصرية
        temp_line = re.sub(r'[━─═]{2,}', protect, temp_line)
        
        # 6. ⭐ HTML tags بدون محتوى (نحميها كـ tokens ولكن نخلي محتواها قابل للترجمة)
        # مثلاً: <b>الزيارات:</b> → @@5@@الزيارات:@@6@@
        # هكذا الترجمة تشوف "الزيارات:" بشكل واضح وتترجمها لـ "Clicks:"
        temp_line = re.sub(r'</?[a-zA-Z][^>]*>', protect, temp_line)
        
        # 7. حماية الإيموجي العادي
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"
            "\U0001F300-\U0001F5FF"
            "\U0001F680-\U0001F6FF"
            "\U0001F1E0-\U0001F1FF"
            "\U00002500-\U00002BEF"
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "\U0001f926-\U0001f937"
            "\u2640-\u2642"
            "\u2600-\u2B55"
            "\u200d"
            "\u23cf"
            "\u23e9"
            "\u231a"
            "\ufe0f"
            "\u3030"
            "]+",
            flags=re.UNICODE
        )
        temp_line = emoji_pattern.sub(protect, temp_line)
        
        # تحقق: لو ما بقي نص حقيقي للترجمة، نرجع السطر كما هو
        clean_check = re.sub(r'@@\d+@@', ' ', temp_line).strip()
        if not clean_check or len(clean_check) < 2:
            return line
        
        # 🌐 الترجمة - بهذا الشكل، Google يشوف النص العربي واضح مع تكنات بسيطة
        translated = GoogleTranslator(source='auto', target=target_lang).translate(temp_line)
        
        if not translated:
            return line
        
        # ترميم markers اللي تغيرت في الترجمة
        # 1. ترميم المسافات في الـ markers (مثل @@ 5 @@)
        translated = re.sub(r'@\s*@\s*(\d+)\s*@\s*@', r'@@\1@@', translated)
        # 2. ترميم الأرقام العربية (لو ترجمت الأرقام)
        arabic_to_eng = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')
        def fix_marker_digits(match):
            return match.group(0).translate(arabic_to_eng)
        translated = re.sub(r'@@[\d٠-٩]+@@', fix_marker_digits, translated)
        
        # 🔁 استرجاع العناصر المحمية (من الآخر للأول لتجنب تداخل الأرقام)
        for i in range(len(protected_items) - 1, -1, -1):
            original = protected_items[i]
            # نسوي replace مرن: يقبل أي مسافات حول الأرقام
            translated = re.sub(
                r'@@\s*' + str(i) + r'\s*@@',
                lambda m: original,
                translated
            )
        
        # 🛡 لو بقي markers ما اتفك تشفيرها، نرجع السطر الأصلي
        if re.search(r'@@\d*@@', translated):
            logger.warning(f"Marker leak in translation - returning original line")
            return line
        
        return translated.strip()
    except Exception as e:
        logger.debug(f"Single line translation error: {e}")
        return line


def extract_custom_emojis_to_html(message):
    if not message.text or not message.entities:
        return message.text or ""
    
    text = message.text
    entities = sorted([e for e in message.entities if e.type == 'custom_emoji'], key=lambda x: x.offset, reverse=True)
    
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
    
    # نحفظ النص الافتراضي من الكود كـ backup
    default_text = LANG.get(l, LANG['ar']).get(key, "")
    
    base_text = ""
    custom_text_used = False
    
    try:
        custom = db.custom_texts.find_one({'lang': l, 'key': key})
        if custom and custom.get('value') and custom['value'].strip():
            base_text = custom['value']
            custom_text_used = True
        else:
            base_text = default_text
    except Exception as e:
        logger.error(f"get_text DB error: {e}")
        base_text = default_text
    
    if not base_text:
        base_text = LANG.get('ar', {}).get(key, "") or LANG.get('en', {}).get(key, "")
    
    if args:
        # 🛡 محاولة الـ format على النص الحالي
        try:
            return base_text.format(*args)
        except Exception as e:
            # ❌ الـ format فشلت — غالباً النص المخصص فقد بعض الـ {} placeholders
            # 🔄 fallback: نستخدم النص الافتراضي من الكود
            logger.warning(
                f"⚠️ Custom text for key '{key}' (lang={l}) has broken placeholders. "
                f"Falling back to default text. Error: {e}"
            )
            
            if custom_text_used and default_text:
                try:
                    # 🛡 محاولة بالنص الافتراضي
                    return default_text.format(*args)
                except Exception as e2:
                    logger.error(f"Even default text failed for key '{key}': {e2}")
                    return default_text
            
            # آخر شي: نرجع النص بدون format (أحسن من رسالة فاضية)
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
                default_text = DEFAULT_BUTTONS.get(l, DEFAULT_BUTTONS['ar']).get(key, key)
                return default_text, emoji_id
            return text, emoji_id
    except Exception as e:
        logger.error(f"get_btn_data DB error: {e}")
    
    default_text = DEFAULT_BUTTONS.get(l, DEFAULT_BUTTONS['ar']).get(key, key)
    return default_text, None

def create_btn(uid, key, callback_data=None, url=None, style=None):
    text, emj_id = get_btn_data(uid, key)
    kwargs = {'text': text}
    if callback_data: kwargs['callback_data'] = callback_data
    if url: kwargs['url'] = url
    if style: kwargs['style'] = style
    if emj_id: kwargs['icon_custom_emoji_id'] = emj_id
    return CustomInlineButton(**kwargs)

def clean_name(text):
    if not text: return "بدون اسم"
    cleaned = re.sub(r'<[^>]+>', '', str(text)).strip()
    return html.escape(cleaned)

def obscure_text(text):
    if not text: return "***"
    if '@' in text:
        parts = text.split('@')
        name = parts[0]; domain = parts[1]
        if len(name) > 2: return name[0] + "***" + name[-1] + "@" + domain
        else: return name[0] + "***@" + domain
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
        try:
            p = db.products.find_one({'id': float(pid_str)})
            if p: return p
        except: pass
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
        try: queries.append({'product_id': float(pid_str)})
        except: pass
        return db.product_stock.count_documents({'$or': queries, 'is_sold': False})
    except: return 0

def get_user_data_full(uid):
    return db.users.find_one({'user_id': uid})

def get_lang(uid):
    u = get_user_data_full(uid)
    return u.get('lang', 'ar') if u else 'ar' 

def is_user_banned(uid):
    u = get_user_data_full(uid)
    return True if u and u.get('is_banned') == 1 else False

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
        except: return False
    return True

def notify_admins(message_text):
    if OWNER_ID:
        try: bot.send_message(OWNER_ID, message_text, parse_mode="HTML")
        except: pass
    admins = list(db.users.find({'is_admin': 1}))
    for admin in admins:
        if admin['user_id'] != OWNER_ID:
            try: bot.send_message(admin['user_id'], message_text, parse_mode="HTML")
            except: pass

# ============================================================
# 🏠 8. معالج البداية 
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
    user_was_new = (user is None)
    
    if not user:
        full_text = "" if is_callback else (message.text or "")
        args = full_text.split()
        ref_candidate = args[1] if len(args) > 1 and args[1].isdigit() else None
        
        db.users.insert_one({
            'user_id': uid, 'name': from_user.first_name, 'username': uname, 
            'balance': 0.0, 'ref_v2_earned': 0.0,
            'lang_chosen': False, 'lang': 'ar', 'is_admin': 0, 'is_banned': 0
        })
        user = get_user_data_full(uid)
        
        # 🎯 تسجيل الإحالة فقط لو المستخدم جديد كلياً
        # المستخدم الموجود مسبقاً لا يُحسب أبداً
        if ref_candidate:
            try:
                referrer_id = int(ref_candidate)
                if referrer_id != uid:
                    register_new_referral(uid, referrer_id)
            except Exception as e:
                logger.error(f"Error registering new referral: {e}")
    else:
        # ⚠️ المستخدم موجود من قبل — لا تُسجَّل أي إحالة جديدة له
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
    if lang not in ['ar', 'en']: lang = 'ar'
    
    # ============================================================
    # ✅ تحديث حالة الإحالة (لو هذا المستخدم مُدعى من شخص آخر)
    # ============================================================
    ref_record = db.referrals_v2.find_one({'invited_id': uid})
    
    if not check_forced_sub(uid):
        if ref_record:
            current = ref_record.get('status', 'pending')
            if current == 'active':
                mark_referral_status(uid, 'left')
        
        chans = list(db.required_channels.find())
        markup = InlineKeyboardMarkup(row_width=1)
        for c in chans: 
            btn_txt = "📢 Channel" if lang=='en' else "📢 القناة"
            markup.add(InlineKeyboardButton(btn_txt, url=f"https://t.me/{c['channel_id'].replace('@','') }"))
        markup.add(create_btn(uid, 'btn_check_sub', callback_data="main_menu_refresh"))
        bot.send_message(chat_id, get_text(uid, 'must_join'), reply_markup=markup, parse_mode="HTML")
        return
    else:
        if ref_record:
            current = ref_record.get('status', 'pending')
            if current in ['pending', 'left']:
                mark_referral_status(uid, 'active')

    users_total = db.users.count_documents({})
    markup = InlineKeyboardMarkup(row_width=2)
    
    markup.add(create_btn(uid, 'btn_gh', callback_data="github_pack_info"))
    markup.add(create_btn(uid, 'btn_gemini', callback_data="gemini_pack_info"))
    
    markup.add(create_btn(uid, 'btn_products', callback_data="open_shop", style="primary"),
               create_btn(uid, 'btn_deposit', callback_data="open_deposit"))
    markup.add(create_btn(uid, 'btn_profile', callback_data="open_profile"),
               create_btn(uid, 'btn_invite', callback_data="open_invite"))
    markup.add(create_btn(uid, 'btn_support', url=f"https://t.me/{OWNER_USER}"),
               create_btn(uid, 'btn_lang', callback_data="toggle_language"))
    
    # 🆕 زر شروط الاستخدام
    markup.add(create_btn(uid, 'btn_terms', callback_data="open_terms"))
    
    if user.get('is_admin') == 1 or uid == OWNER_ID:
        markup.add(create_btn(uid, 'btn_admin', callback_data="admin_panel_main"))

    welcome_message = get_text(uid, 'welcome', uid, from_user.first_name, users_total, user.get('balance', 0.0))
    bot.send_message(chat_id, welcome_message, reply_markup=markup, parse_mode="HTML")


# ============================================================
# 📜 معالج زر شروط الاستخدام
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_terms")
def show_terms(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    if is_user_banned(uid): return
    
    # نجيب النص (يستخدم CMS تلقائياً لو الأدمن عدّله)
    terms_text = get_text(uid, 'terms_content')
    
    markup = InlineKeyboardMarkup()
    markup.add(create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh"))
    
    try:
        bot.edit_message_text(
            terms_text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
            parse_mode="HTML"
        )
    except Exception as e:
        error_str = str(e).lower()
        if 'message is not modified' in error_str:
            pass  # الرسالة نفسها، طبيعي
        else:
            # محاولة إرسال جديدة
            try:
                bot.send_message(call.message.chat.id, terms_text, reply_markup=markup, parse_mode="HTML")
            except:
                pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("init_lang_"))
def init_lang_selection(call):
    bot.answer_callback_query(call.id)
    new_lang = call.data.replace("init_lang_", "").strip()
    if new_lang not in ['ar', 'en']:
        new_lang = 'ar'
    db.users.update_one(
        {'user_id': call.from_user.id},
        {'$set': {'lang': new_lang, 'lang_chosen': True}}
    )
    try: bot.delete_message(call.message.chat.id, call.message.message_id)
    except: pass
    call.message.from_user = call.from_user
    start_handler(call.message)

@bot.callback_query_handler(func=lambda call: call.data == "toggle_language")
def toggle_lang(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    u = get_user_data_full(uid)
    new_l = 'en' if u.get('lang', 'ar') == 'ar' else 'ar'
    db.users.update_one({'user_id': uid}, {'$set': {'lang': new_l}})
    try: bot.delete_message(call.message.chat.id, call.message.message_id)
    except: pass
    call.message.from_user = call.from_user
    start_handler(call.message)

@bot.callback_query_handler(func=lambda call: call.data == "main_menu_refresh")
def refresh_main(call):
    bot.answer_callback_query(call.id)
    try: bot.delete_message(call.message.chat.id, call.message.message_id)
    except: pass
    call.message.from_user = call.from_user
    start_handler(call.message)

# ============================================================
# ✨ 9. وحدة تفعيل Gemini 
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "gemini_pack_info")
def gemini_info_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    l = get_lang(uid)
    gemini_price = float(get_setting("gemini_price", 5.0))
    
    text = get_text(uid, 'gemini_desc', gemini_price)
    
    markup = InlineKeyboardMarkup()
    if not client or not get_setting("userbot_session", "") or not get_setting("provider_bot", ""):
        text += "\n\n⚠️ <i>(الخدمة مغلقة حالياً من الإدارة للترقية)</i>"
    else:
        markup.add(create_btn(uid, 'btn_buy_now', callback_data="gemini_buy_prompt"))
    markup.add(create_btn(uid, 'btn_back', callback_data="main_menu_refresh"))
    
    try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "gemini_buy_prompt")
def gemini_buy_prompt(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    l = get_lang(uid)
    
    # 🛡 Rate Limiting
    if not _acquire_purchase_lock(uid):
        if l == 'ar':
            bot.send_message(uid, "⏳ <b>يوجد عملية شراء قيد المعالجة!</b>\nانتظر انتهاءها أولاً.", parse_mode="HTML")
        else:
            bot.send_message(uid, "⏳ <b>You have a purchase in progress!</b>", parse_mode="HTML")
        return
    
    try:
        gemini_price = round(float(get_setting("gemini_price", 5.0)), 2)
        
        # 🛡 خصم atomic مع فحص الرصيد - يمنع double-spending
        updated_user = db.users.find_one_and_update(
            {
                'user_id': uid,
                'balance': {'$gte': gemini_price}
            },
            {'$inc': {'balance': -gemini_price}},
            return_document=True
        )
        
        if updated_user is None:
            bot.send_message(uid, get_text(uid, 'no_balance'), parse_mode="HTML")
            return
        
        # ✅ نجح الخصم - أضفه للقائمة
        add_to_gemini_queue(uid, gemini_price)
    finally:
        # نحرر القفل بعد ما يدخل القائمة (لأنه ممكن يستنى طويل)
        _release_purchase_lock(uid)

@bot.message_handler(func=lambda m: ACTIVE_GEMINI_SESSION and m.from_user.id == ACTIVE_GEMINI_SESSION['uid'])
def relay_to_provider(message):
    text = message.text
    provider_bot = get_setting("provider_bot", "").replace("@", "")
    if text and client and USERBOT_LOOP and provider_bot:
        async def _send():
            try:
                await client.send_message(provider_bot, text)
            except Exception as e:
                logger.error(f"Error relaying message: {e}")
                bot.send_message(message.chat.id, f"❌ خطأ في الإرسال: <code>{e}</code>", parse_mode="HTML")
        asyncio.run_coroutine_threadsafe(_send(), USERBOT_LOOP)

# ============================================================
# 🎓 10. وحدة تفعيل GitHub 
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "github_pack_info")
def github_info_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    l = get_lang(uid)
    gh_price = float(get_setting("github_price", 15.0))
    
    text = get_text(uid, 'gh_desc', gh_price)
    
    markup = InlineKeyboardMarkup()
    markup.add(create_btn(uid, 'btn_buy_now', callback_data="github_buy_prompt"))
    markup.add(create_btn(uid, 'btn_back', callback_data="main_menu_refresh"))
    
    try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "github_buy_prompt")
def github_buy_prompt(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    l = get_lang(uid)
    
    # 🛡 Rate Limiting - منع طلبين بنفس الوقت
    if not _acquire_purchase_lock(uid):
        if l == 'ar':
            bot.send_message(uid, "⏳ <b>يوجد عملية شراء أو تفعيل قيد المعالجة!</b>\nانتظر انتهاءها أولاً.", parse_mode="HTML")
        else:
            bot.send_message(uid, "⏳ <b>You have a purchase/activation in progress!</b>", parse_mode="HTML")
        return
    
    gh_price = float(get_setting("github_price", 15.0))
    u = get_user_data_full(uid)
    
    if float(u.get('balance', 0)) < gh_price:
        _release_purchase_lock(uid)  # حرّر القفل
        bot.send_message(uid, get_text(uid, 'no_balance'), parse_mode="HTML")
        return
        
    temp_github_data[uid] = {'price': gh_price, 'lang': l}
    
    msg = bot.send_message(uid, get_text(uid, 'gh_prompt_user'), parse_mode="HTML")
    bot.register_next_step_handler(msg, process_gh_step_user)

def process_gh_step_user(message):
    uid = message.from_user.id
    if uid not in temp_github_data: return
    
    # 🛡 تشفير اسم المستخدم
    temp_github_data[uid]['user'] = encrypt_sensitive(message.text.strip())
    l = temp_github_data[uid]['lang']
    
    # حذف الرسالة من المحادثة (الأمان البصري)
    try: bot.delete_message(uid, message.message_id)
    except: pass
    
    msg = bot.send_message(uid, get_text(uid, 'gh_prompt_pass'), parse_mode="HTML")
    bot.register_next_step_handler(msg, process_gh_step_pass)

def process_gh_step_pass(message):
    uid = message.from_user.id
    if uid not in temp_github_data: return
    
    # 🛡 تشفير كلمة المرور
    temp_github_data[uid]['pass'] = encrypt_sensitive(message.text.strip())
    l = temp_github_data[uid]['lang']
    
    # حذف رسالة كلمة المرور فوراً من المحادثة (الأمان البصري)
    try: bot.delete_message(uid, message.message_id)
    except: pass
    
    msg = bot.send_message(uid, get_text(uid, 'gh_prompt_2fa'), parse_mode="HTML")
    bot.register_next_step_handler(msg, process_gh_step_2fa)

def process_gh_step_2fa(message):
    uid = message.from_user.id
    if uid not in temp_github_data: return
    
    # 🛡 تشفير كود 2FA
    two_factor_encrypted = encrypt_sensitive(message.text.strip())
    
    # حذف رسالة 2FA فوراً من المحادثة
    try: bot.delete_message(uid, message.message_id)
    except: pass
    
    data = temp_github_data.pop(uid)
    
    price = data['price']
    lang = data['lang']
    
    # 🛡 فك التشفير فقط عند الإرسال للـ API (وقت قصير جداً في الذاكرة)
    g_user = decrypt_sensitive(data['user'])
    g_pass = decrypt_sensitive(data['pass'])
    g_totp = decrypt_sensitive(two_factor_encrypted)
    
    # مسح المرجع المشفّر بعد فك التشفير
    secure_wipe(data)
    del two_factor_encrypted
    
    db.users.update_one({'user_id': uid}, {'$inc': {'balance': -price}})
    status_msg = bot.send_message(uid, get_text(uid, 'gh_deducted'), parse_mode="HTML")
    
    def api_worker():
        try:
            if not GITHUB_API_KEY:
                raise Exception("API Key not found in .env")
                
            headers = {
                "X-API-Key": GITHUB_API_KEY,
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0"
            }
            
            payload = {
                "github_username": g_user,
                "github_password": g_pass,
                "totp_secret": g_totp
            }
                
            api_url = f"{GITHUB_BASE_URL}/api/run"
            
            res = requests.post(api_url, headers=headers, json=payload, timeout=30)
            
            if res.status_code in [200, 201, 202]:
                res_data = res.json()
                job_id = res_data.get("job_id")
                
                if not job_id:
                    bot.edit_message_text(get_text(uid, 'gh_submitted'), chat_id=uid, message_id=status_msg.message_id, parse_mode="HTML")
                    return

                bot.edit_message_text(get_text(uid, 'gh_received', job_id), chat_id=uid, message_id=status_msg.message_id, parse_mode="HTML")
                
                for i in range(1, 35):
                    time.sleep(5) 
                    status_url = f"{GITHUB_BASE_URL}/api/job/{job_id}"
                    
                    try:
                        status_res = requests.get(status_url, headers=headers, timeout=15)
                        
                        if status_res.status_code == 200:
                            s_data = status_res.json()
                            status = s_data.get("status", "").lower()
                            
                            if status == "submitted":
                                app_id = s_data.get("app_id", "N/A")
                                
                                db.orders.insert_one({'user_id': uid, 'product_id': 'GitHub_Student', 'code_delivered': f"Account: {g_user} | AppID: {app_id}"})
                                bot.edit_message_text(get_text(uid, 'gh_success', g_user), chat_id=uid, message_id=status_msg.message_id, parse_mode="HTML")
                                notify_admins(f"🔐 <b>إشعار إدارة (تفعيل GitHub) ⚡</b>\n\n👤 العميل: <code>{uid}</code>\n📦 الحساب: {g_user}\n🔖 رقم الطلب: <code>{job_id}</code>\n✅ الحالة: تم التفعيل بنجاح!")
                                
                                log_ch = get_setting('log_channel')
                                u_data = db.users.find_one({'user_id': uid})
                                obs_user = obscure_text(u_data.get('username') or str(uid))
                                
                                if log_ch and log_ch != "Not Set":
                                    try: 
                                        # 🆕 النص من CMS (قابل للتعديل)
                                        github_msg = LANG['en']['log_github'].format(obs_user)
                                        custom_gh = db.custom_texts.find_one({'lang': 'en', 'key': 'log_github'})
                                        if custom_gh and custom_gh.get('value'):
                                            try:
                                                github_msg = custom_gh['value'].format(obs_user)
                                            except: pass
                                        bot.send_message(log_ch, github_msg, parse_mode="HTML")
                                    except: pass
                                
                                # 🎁 منح مكافأة الإحالة لو هذا المستخدم جاي من إحالة
                                try:
                                    award_purchase_referral_reward(uid, "GitHub Student Pack", price)
                                except Exception as ref_err:
                                    logger.error(f"Error awarding referral on GitHub: {ref_err}")

                                return 
                                
                            elif status in ["failed", "error"]:
                                err_reason = s_data.get("error", s_data.get("refund_reason", "بيانات تسجيل الدخول أو الـ 2FA غير صحيحة"))
                                db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
                                bot.edit_message_text(get_text(uid, 'gh_fail', err_reason, price), chat_id=uid, message_id=status_msg.message_id, parse_mode="HTML")
                                return
                                
                            else:
                                step = s_data.get("step", "processing")
                                dots = "." * (i % 3 + 1)
                                step_ar = step if lang == 'en' else step.replace("login", "تسجيل الدخول").replace("2fa", "التحقق الثنائي").replace("identity", "الهوية").replace("submit", "تقديم الطلب")
                                progress_text = get_text(uid, 'gh_processing', job_id, step_ar, dots, i)
                                try: bot.edit_message_text(progress_text, chat_id=uid, message_id=status_msg.message_id, parse_mode="HTML")
                                except: pass
                                
                    except requests.exceptions.Timeout:
                        continue
                        
                bot.edit_message_text(get_text(uid, 'gh_timeout'), chat_id=uid, message_id=status_msg.message_id, parse_mode="HTML")

            else:
                try: error_msg = res.json().get("error", "Unknown Error")
                except: error_msg = f"HTTP {res.status_code}"
                
                db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
                bot.edit_message_text(get_text(uid, 'gh_fail', error_msg, price), chat_id=uid, message_id=status_msg.message_id, parse_mode="HTML")
                logger.error(f"GitHub API Fast Error: {res.status_code} - {res.text}")
                
        except Exception as e:
            db.users.update_one({'user_id': uid}, {'$inc': {'balance': price}})
            try: bot.edit_message_text(get_text(uid, 'gh_conn_err', e), chat_id=uid, message_id=status_msg.message_id, parse_mode="HTML")
            except: pass
            logger.error(f"GitHub Connection Error: {e}")
        finally:
            # 🛡 تحرير قفل الشراء (سواء نجح أو فشل)
            _release_purchase_lock(uid)
            # 🛡 مسح بيانات GitHub الحساسة من الذاكرة بعد الانتهاء
            try:
                nonlocal_vars = {'g_user': g_user, 'g_pass': g_pass, 'g_totp': g_totp}
                secure_wipe(nonlocal_vars)
            except Exception:
                pass

    threading.Thread(target=api_worker, daemon=True).start()

# ============================================================
# 👤 11. الملف الشخصي وتاريخ العمليات 
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_profile")
def profile_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    if not check_forced_sub(uid): start_handler(call.message); return
    
    u = get_user_data_full(uid); l = u.get('lang', 'ar') if u else 'ar'
    buy_count = db.orders.count_documents({'user_id': uid})
    d_res = list(db.used_transactions.find({'user_id': uid}))
    dep_total = sum([float(d.get('amount', 0)) for d in d_res])

    prof_emoji_id = get_setting('emoji_profile', '')
    profile_text = get_text(uid, 'profile_txt', uid, clean_name(u.get('name','User')), u.get('balance', 0.0), buy_count, dep_total)
    
    if prof_emoji_id and prof_emoji_id != "Not Set":
        profile_text = f'<tg-emoji emoji-id="{prof_emoji_id}">✨</tg-emoji> ' + profile_text

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
    if is_user_banned(uid): return
    l = get_lang(uid)
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
    l = get_lang(uid)
    
    if not recs:
        empty_msg = "📭 No purchases found." if l == 'en' else "📭 لا يوجد سجل مشتريات."
        bot.send_message(uid, empty_msg)
        return
        
    content = "=== Your Purchase History ===\n\n" if l == 'en' else "=== سجل المشتريات الخاص بك ===\n\n"
    all_prods = {str(p.get('id', p.get('_id'))): p for p in db.products.find()}
    
    for i, r in enumerate(recs, 1):
        date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
        pid = str(r.get('product_id'))
        p = all_prods.get(pid)
        
        if pid in ['GitHub_Student', 'Gemini_Activation']:
            n = pid.replace('_', ' ')
        else:
            n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar')) if p else "Unknown Product"
            
        code = r.get('code_delivered', '')
        
        if l == 'en':
            content += f"{i}. Date: {date_str}\nProduct: {n}\nCode/Details: {code}\n{'-'*30}\n"
        else:
            content += f"{i}. التاريخ: {date_str}\nالمنتج: {n}\nالكود/التفاصيل: {code}\n{'-'*30}\n"
        
    f = io.BytesIO(content.encode('utf-8'))
    f.name = f"My_Purchases_{uid}.txt"
    caption = "📄 Here are all your purchases." if l == 'en' else "📄 ملف يحتوي على جميع مشترياتك وتواريخها."
    bot.send_document(call.message.chat.id, f, caption=caption)

@bot.callback_query_handler(func=lambda call: call.data.startswith("h_view_"))
def show_hist_detail(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)
    mode = call.data.replace("h_view_", "")
    
    try:
        if mode == "buy":
            # 🆕 نجمع المشتريات حسب المنتج
            all_orders = list(db.orders.find({'user_id': uid}).sort('_id', -1))
            
            if not all_orders:
                out = get_text(uid, 'no_hist')
                markup = InlineKeyboardMarkup()
                markup.add(create_btn(uid, 'btn_back', callback_data="history_menu_callback"))
                try: bot.edit_message_text(out, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
                except: pass
                return
            
            # تجميع حسب product_id
            grouped = {}
            for r in all_orders:
                pid = str(r.get('product_id', ''))
                if pid not in grouped:
                    grouped[pid] = []
                grouped[pid].append(r)
            
            # نبني الرسالة + الأزرار
            if l == 'en':
                out = f"🛍 <b>Your Purchases</b>\n\n"
                out += f"📦 You have purchased <b>{len(grouped)}</b> different product(s).\n"
                out += f"💼 Total orders: <b>{len(all_orders)}</b>\n\n"
                out += "👇 <b>Click any product below to download its codes:</b>\n"
            else:
                out = f"🛍 <b>مشترياتك</b>\n\n"
                out += f"📦 لقد اشتريت <b>{len(grouped)}</b> منتج مختلف.\n"
                out += f"💼 إجمالي الطلبات: <b>{len(all_orders)}</b>\n\n"
                out += "👇 <b>اضغط على أي منتج لتحميل أكواده:</b>\n"
            
            markup = InlineKeyboardMarkup(row_width=1)
            
            for pid, orders in grouped.items():
                # نجيب اسم المنتج
                if pid in ['GitHub_Student', 'Gemini_Activation']:
                    p_name = pid.replace('_', ' ')
                    custom_emoji_id = None
                else:
                    p = find_product(pid)
                    if p:
                        p_name = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar', p.get('name_en', 'Product')))
                        custom_emoji_id = p.get('custom_emoji_id')
                    else:
                        p_name = "منتج محذوف" if l == 'ar' else "Deleted Product"
                        custom_emoji_id = None
                
                count = len(orders)
                btn_text = f"📦 {p_name} ({count})"
                
                btn_kwargs = {
                    'text': btn_text,
                    'callback_data': f"dlhist_{pid}"
                }
                if custom_emoji_id:
                    btn_kwargs['icon_custom_emoji_id'] = custom_emoji_id
                
                markup.add(CustomInlineButton(**btn_kwargs))
            
            # زر تحميل كل المشتريات
            if l == 'en':
                markup.add(InlineKeyboardButton("📥 Download All Purchases", callback_data="h_dl_buy"))
            else:
                markup.add(InlineKeyboardButton("📥 تحميل كل المشتريات", callback_data="h_dl_buy"))
            
            markup.add(create_btn(uid, 'btn_back', callback_data="history_menu_callback"))
            
        else:  # mode == "dep" - الإيداعات
            recs = list(db.used_transactions.find({'user_id': uid}).sort('_id', -1).limit(10))
            out = ""
            if not recs: 
                out = get_text(uid, 'no_hist')
            else:
                if l == 'en':
                    out = "💳 <b>Your Last 10 Deposits</b>\n\n"
                else:
                    out = "💳 <b>آخر 10 إيداعات لك</b>\n\n"
                for r in recs: 
                    date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
                    out += f"💳 <b>{'Deposit' if l=='en' else 'إيداع'}</b>\n📅 <code>{date_str}</code>\n💰 <b>${r.get('amount', 0):.2f}</b>\n🆔 <code>{r.get('transaction_id', '')[:30]}...</code>\n────────────\n"
            
            markup = InlineKeyboardMarkup()
            markup.add(create_btn(uid, 'btn_back', callback_data="history_menu_callback"))
        
        try: bot.edit_message_text(out, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        except: pass
    except Exception as e:
        logger.error(f"Error in show_hist_detail: {e}")
        try: bot.send_message(uid, "❌ حدث خطأ، حاول مرة ثانية.", parse_mode="HTML")
        except: pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("dlhist_"))
def download_product_history(call):
    """🆕 تحميل أكواد منتج معين من سجل المشتريات"""
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز الملف...")
    uid = call.from_user.id
    if is_user_banned(uid): return
    
    pid = call.data.replace("dlhist_", "")
    l = get_lang(uid)
    
    # نجيب كل الطلبات لهذا المنتج
    orders = list(db.orders.find({'user_id': uid, 'product_id': pid}).sort('_id', -1))
    if not orders:
        # ممكن pid يكون رقم أو string، نجرب الأنواع
        try:
            orders = list(db.orders.find({'user_id': uid, 'product_id': int(pid)}).sort('_id', -1))
        except: pass
    
    if not orders:
        msg = "❌ No purchases found for this product." if l == 'en' else "❌ لا توجد مشتريات لهذا المنتج."
        bot.send_message(uid, msg)
        return
    
    # نجيب اسم المنتج
    if pid in ['GitHub_Student', 'Gemini_Activation']:
        p_name = pid.replace('_', ' ')
    else:
        p = find_product(pid)
        if p:
            p_name = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar', p.get('name_en', 'Product')))
        else:
            p_name = "Product"
    
    # نبني الملف
    if l == 'en':
        content = f"=== Your Purchases: {p_name} ===\n"
    else:
        content = f"=== مشترياتك: {p_name} ===\n"
    
    content += f"Total: {len(orders)} order(s)\n"
    content += f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    content += "=" * 50 + "\n\n"
    
    for i, r in enumerate(orders, 1):
        date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
        code = r.get('code_delivered', '')
        if l == 'en':
            content += f"#{i} | Date: {date_str}\nCode: {code}\n{'-'*50}\n"
        else:
            content += f"#{i} | التاريخ: {date_str}\nالكود: {code}\n{'-'*50}\n"
    
    # إرسال الملف
    f = io.BytesIO(content.encode('utf-8'))
    safe_name = re.sub(r'[^\w\-]', '_', p_name)[:30]
    f.name = f"My_{safe_name}_Codes.txt"
    
    if l == 'en':
        caption = f"📄 <b>{p_name}</b>\n\nAll your codes for this product ({len(orders)} total).\n\n<i>Keep this file safe!</i>"
    else:
        caption = f"📄 <b>{p_name}</b>\n\nكل أكوادك لهذا المنتج ({len(orders)} كود).\n\n<i>احتفظ بالملف بأمان!</i>"
    
    try:
        bot.send_document(uid, f, caption=caption, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to send product history file: {e}")
        bot.send_message(uid, "❌ فشل إرسال الملف، حاول مرة ثانية.")

@bot.callback_query_handler(func=lambda call: call.data == "open_invite")
def invite_ui(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    
    uid = call.from_user.id
    if is_user_banned(uid): return
    if not check_forced_sub(uid): start_handler(call.message); return
    
    u = get_user_data_full(uid)
    l = u.get('lang', 'ar') if u else 'ar'
    b_n = bot.get_me().username
    
    # تحديث فوري للرصيد قبل العرض
    try:
        update_referrer_balance(uid)
        u = get_user_data_full(uid)
    except Exception as e:
        logger.error(f"Error updating referrer balance in invite_ui: {e}")
    
    # جلب الإحصائيات من الجدول الجديد
    pending_count, active_count, left_count, total_clicks = get_ref_counts(uid)
    actual_earned = round(float(u.get('ref_v2_earned', 0.0)), 2)

    markup = InlineKeyboardMarkup()
    markup.add(create_btn(uid, 'btn_refresh', callback_data="open_invite"))
    markup.add(create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh"))
    
    # 🛡 جلب النص بشكل آمن مع 3 مستويات من الـ fallback
    final_text = None
    
    # المستوى 1: محاولة النص المخصص + format
    try:
        final_text = get_text(uid, 'invite_txt', total_clicks, pending_count, active_count, left_count, actual_earned, b_n, uid)
    except Exception as e:
        logger.error(f"Error getting invite_txt (level 1): {e}")
    
    # المستوى 2: لو فشل، نستخدم النص الافتراضي مباشرة من الكود
    if not final_text:
        try:
            default_text = LANG.get(l, LANG['ar']).get('invite_txt', '')
            final_text = default_text.format(total_clicks, pending_count, active_count, left_count, actual_earned, b_n, uid)
            logger.warning(f"⚠️ Fell back to default invite_txt for user {uid} (lang={l})")
        except Exception as e:
            logger.error(f"Error formatting default invite_txt (level 2): {e}")
    
    # المستوى 3: لو حتى الافتراضي فشل، نسوي رسالة بسيطة
    if not final_text:
        if l == 'en':
            final_text = (
                f"👥 <b>Referrals</b>\n\n"
                f"🔗 <code>https://t.me/{b_n}?start={uid}</code>\n\n"
                f"👥 Clicks: <b>{total_clicks}</b>\n"
                f"⏳ Pending: <b>{pending_count}</b>\n"
                f"✅ Active: <b>{active_count}</b>\n"
                f"❌ Left: <b>{left_count}</b>\n\n"
                f"💰 Balance: <b>${actual_earned:.2f}</b>"
            )
        else:
            final_text = (
                f"👥 <b>الإحالات</b>\n\n"
                f"🔗 <code>https://t.me/{b_n}?start={uid}</code>\n\n"
                f"👥 الزيارات: <b>{total_clicks}</b>\n"
                f"⏳ معلق: <b>{pending_count}</b>\n"
                f"✅ نشط: <b>{active_count}</b>\n"
                f"❌ غادر: <b>{left_count}</b>\n\n"
                f"💰 الرصيد: <b>${actual_earned:.2f}</b>"
            )
    
    # 🛡 محاولة الإرسال - مع 3 محاولات لو فشل بسبب HTML
    sent_successfully = False
    
    # المحاولة 1: HTML كامل
    try: 
        bot.edit_message_text(
            final_text, 
            call.message.chat.id, call.message.message_id, 
            reply_markup=markup, parse_mode="HTML"
        )
        sent_successfully = True
    except Exception as e:
        error_str = str(e).lower()
        # 🛡 "message is not modified" مو خطأ - الرسالة نفسها زي ما هي (المستخدم ضغط تحديث ولا فيه تغيير)
        if 'message is not modified' in error_str or 'not modified' in error_str:
            sent_successfully = True
            # نظهر للمستخدم تأكيد إن البيانات محدثة
            try:
                # نسوي callback answer بدل ما نزعجه بإشعار
                bot.answer_callback_query(
                    call.id, 
                    "✅ البيانات محدّثة بالفعل" if l == 'ar' else "✅ Already up to date",
                    show_alert=False
                )
            except: pass
        else:
            # خطأ حقيقي - نسجله ونحاول fallback
            logger.error(f"Failed to send invite message with HTML: {e}")
    
    # المحاولة 2: بدون parse_mode (نص عادي)
    if not sent_successfully:
        try:
            # تنظيف HTML tags وعرض النص العادي
            clean_text = re.sub(r'<[^>]+>', '', final_text)
            bot.edit_message_text(
                clean_text,
                call.message.chat.id, call.message.message_id,
                reply_markup=markup
            )
            sent_successfully = True
            logger.warning(f"Sent invite as plain text (HTML failed) for user {uid}")
        except Exception as e:
            error_str = str(e).lower()
            if 'message is not modified' in error_str or 'not modified' in error_str:
                sent_successfully = True
            else:
                logger.error(f"Failed to send invite as plain text: {e}")
    
    # المحاولة 3: إرسال رسالة جديدة بدلاً من edit
    if not sent_successfully:
        try:
            bot.send_message(
                call.message.chat.id,
                final_text,
                reply_markup=markup,
                parse_mode="HTML"
            )
            sent_successfully = True
        except Exception as e:
            logger.error(f"Failed to send new invite message: {e}")
            # آخر شي - رسالة بسيطة جداً
            try:
                bot.send_message(
                    call.message.chat.id,
                    f"Your link: https://t.me/{b_n}?start={uid}\nBalance: ${actual_earned:.2f}",
                    reply_markup=markup
                )
            except: pass

# ============================================================
# 🛒 12. المتجر والشراء والترتيب الأبجدي للمنتجات 
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
        is_hidden = p.get('is_hidden', False)
        if is_hidden and not is_admin:
            continue
            
        is_manual = p.get('is_manual', False)
        pid = p.get('id', str(p.get('_id', '')))
        st = get_product_stock_count(pid)
        
        btn_style = "success" if (is_manual or st > 0) else "danger"
        
        hidden_icon = " 👻(مخفي)" if is_hidden else ""
        n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar'))
        short_n = n[:25] + ".." if len(n) > 25 else n 
        
        st_text = "FW" if is_manual else str(st)
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
        
    markup.add(create_btn(uid, 'btn_refresh', callback_data="open_shop"))
    markup.add(create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh"))
    
    store_emoji_id = get_setting('emoji_store', '')
    store_text = get_text(uid, 'store_title')
    if store_emoji_id and store_emoji_id != "Not Set":
        store_text = f'<tg-emoji emoji-id="{store_emoji_id}">✨</tg-emoji> ' + store_text

    try: bot.edit_message_text(store_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("vi_p_"))
def shop_detail_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)
    pid = call.data.replace('vi_p_', '')
    
    p = find_product(pid)
    if not p: 
        bot.send_message(uid, "❌ عذراً، المنتج غير متوفر.", parse_mode="HTML"); return
    
    u = get_user_data_full(uid)
    is_admin = (u.get('is_admin') == 1 or uid == OWNER_ID)
    if p.get('is_hidden', False) and not is_admin:
        bot.send_message(uid, "❌ عذراً، هذا المنتج غير متوفر حالياً.", parse_mode="HTML"); return

    is_manual = p.get('is_manual', False)
    st = get_product_stock_count(pid)
    
    if l == 'ar':
        delivery_type = "يدوي 🤝 (تواصل مع الإدارة بعد الدفع)" if is_manual else "تلقائي ⚡ (تسليم فوري)"
        st_text = "غير محدود" if is_manual else f"{st} قطعة"
    else:
        delivery_type = "Manual 🤝 (Contact admin after payment)" if is_manual else "Auto ⚡ (Instant delivery)"
        st_text = "Unlimited" if is_manual else f"{st} pcs"
        
    n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar'))
    d = clean_name(p.get('desc_en') if l == 'en' else p.get('desc_ar'))
    
    custom_emoji_id = p.get('custom_emoji_id')
    icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji>' if custom_emoji_id else '📦'
    
    if l == 'en':
        text = f"{icon_html} <b>{n}</b>\n\n📝 {d}\n\n🚚 <b>Delivery:</b> {delivery_type}\n💰 <b>Price:</b> ${p.get('price', 0):.2f}\n📊 <b>Stock:</b> {st_text}"
    else:
        text = f"{icon_html} <b>{n}</b>\n\n📝 {d}\n\n🚚 <b>نوع التسليم:</b> {delivery_type}\n💰 <b>السعر:</b> ${p.get('price', 0):.2f}\n📊 <b>المتوفر:</b> {st_text}"
    
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
    if is_user_banned(uid): return
    l = get_lang(uid); pid = call.data.replace('buy_qty_', '')
    
    p = find_product(pid)
    if not p: return
    
    is_manual = p.get('is_manual', False)
    if not is_manual and get_product_stock_count(pid) == 0:
        bot.send_message(uid, get_text(uid, 'out_stock'), parse_mode="HTML"); return
        
    msg = bot.send_message(uid, get_text(uid, 'qty_prompt'), parse_mode="HTML")
    bot.register_next_step_handler(msg, execute_bulk_buy, pid, l)

def execute_bulk_buy(message, pid, lang):
    uid = message.from_user.id
    if is_user_banned(uid): return
    if not message.text or not message.text.isdigit():
        bot.send_message(uid, get_text(uid, 'qty_invalid'), parse_mode="HTML"); return
        
    qty = int(message.text.strip())
    if qty <= 0:
        bot.send_message(uid, get_text(uid, 'qty_invalid'), parse_mode="HTML"); return
    
    # 🛡 حماية #4: Rate Limiting - منع المستخدم من شراء مرتين بنفس اللحظة
    if not _acquire_purchase_lock(uid):
        if lang == 'ar':
            bot.send_message(uid, "⏳ <b>يوجد طلب شراء قيد المعالجة لك بالفعل!</b>\nانتظر انتهاءه قبل بدء طلب جديد.", parse_mode="HTML")
        else:
            bot.send_message(uid, "⏳ <b>You have a purchase in progress!</b>\nWait until it finishes before starting another.", parse_mode="HTML")
        return

    try:
        u = get_user_data_full(uid)
        p = find_product(pid)
        if not p:
            bot.send_message(uid, "❌ المنتج غير موجود.", parse_mode="HTML")
            return

        is_manual = p.get('is_manual', False)
        total_price = round(float(p.get('price', 0)) * qty, 2)
        
        # 🛡 حماية #2.1: حجز الأكواد بشكل atomic قبل خصم الرصيد
        # نحجزها بـ "is_sold: True" + "reserved_by: uid + timestamp"
        # لو فشل الدفع نرجعها
        reserved_items = []
        reservation_id = f"{uid}_{int(time.time() * 1000)}"
        
        if not is_manual:
            pid_str = str(pid)
            queries = [{'product_id': pid_str}]
            if pid_str.isdigit(): queries.append({'product_id': int(pid_str)})
            try: queries.append({'product_id': float(pid_str)})
            except: pass
            
            # 🛡 حجز ذري: نحجز كود واحد بكل مرة باستخدام find_one_and_update
            # هذا يضمن إن نفس الكود ما ينباع لشخصين
            for _ in range(qty):
                reserved = db.product_stock.find_one_and_update(
                    {'$or': queries, 'is_sold': False},
                    {'$set': {
                        'is_sold': True,
                        'reservation_id': reservation_id,
                        'reserved_at': int(time.time())
                    }},
                    return_document=True
                )
                if reserved is None:
                    # لم نجد كود متاح - نُرجع كل المحجوزات
                    _release_reservation(reservation_id)
                    bot.send_message(uid, get_text(uid, 'qty_not_enough', len(reserved_items)), parse_mode="HTML")
                    return
                reserved_items.append(reserved)
        
        # 🛡 حماية #2.2: خصم الرصيد بشكل atomic - فقط لو الرصيد كافي
        # find_one_and_update مع شرط balance >= total_price يمنع double-spend
        updated_user = db.users.find_one_and_update(
            {
                'user_id': uid,
                'balance': {'$gte': total_price}  # شرط الرصيد الكافي
            },
            {'$inc': {'balance': -total_price}},
            return_document=True  # نريد القيمة الجديدة
        )
        
        if updated_user is None:
            # الرصيد غير كافي - نُرجع الأكواد المحجوزة
            _release_reservation(reservation_id)
            bot.send_message(uid, get_text(uid, 'no_balance'), parse_mode="HTML")
            return
        
        # ✅ نجح الخصم - نكمل العملية
        u = updated_user  # استخدم الإصدار المحدث
        
        support_user = f"@{OWNER_USER}" if OWNER_USER else "الإدارة"
        buyer_m = f"@{u['username']}" if u and u.get('username') else f"عضو جديد"
        log_ch = get_setting('log_channel')

        if is_manual:
            order_id = "M" + str(int(time.time()))[-6:] + str(uid)[-2:]
            try:
                db.orders.insert_one({
                    'user_id': uid, 
                    'product_id': str(pid), 
                    'code_delivered': f"طلب يدوي: {order_id}",
                    'qty': qty,
                    'total_price': total_price
                })
            except Exception as e:
                # فشل تسجيل الطلب - نرجع الرصيد
                logger.error(f"Failed to insert manual order: {e}")
                db.users.update_one({'user_id': uid}, {'$inc': {'balance': total_price}})
                bot.send_message(uid, "❌ حدث خطأ في معالجة الطلب. تم إرجاع رصيدك.", parse_mode="HTML")
                return
            
            if lang == 'ar':
                msg_txt = f"✅ <b>تم الطلب بنجاح! وتم خصم (${total_price:.2f})</b>\n\nهذا المنتج يتطلب (تسليم يدوي).\nرقم طلبك: <code>{order_id}</code>\n\nيرجى التواصل مع {support_user} لتنفيذ طلبك."
            else:
                msg_txt = f"✅ <b>Order Placed! (${total_price:.2f} deducted)</b>\n\nThis is a manual delivery product.\nOrder ID: <code>{order_id}</code>\n\nPlease contact {support_user}."
            bot.send_message(uid, msg_txt, parse_mode="HTML")
            
            admin_msg = f"🔐 <b>إشعار إدارة (طلب تسليم يدوي) 🤝</b>\n\n👤 العميل: {buyer_m} (<code>{uid}</code>)\n📦 المنتج: {clean_name(p.get('name_ar'))}\n🔢 الكمية: {qty}\n💰 دفع: ${total_price:.2f}\n🔖 رقم الطلب: <code>{order_id}</code>\n\n⚠️ <b>تواصل مع العميل لتسليمه طلبه!</b>"
            notify_admins(admin_msg)
        else:
            # تثبيت الأكواد المحجوزة (إزالة reservation_id - أصبحت مباعة فعلياً)
            delivered_codes = []
            try:
                for item in reserved_items:
                    db.product_stock.update_one(
                        {'_id': item['_id']},
                        {'$unset': {'reservation_id': "", 'reserved_at': ""}}
                    )
                    db.orders.insert_one({
                        'user_id': uid,
                        'product_id': str(pid),
                        'code_delivered': item['code_line'],
                        'qty': 1,
                        'price': float(p.get('price', 0))
                    })
                    delivered_codes.append(item['code_line'])
            except Exception as e:
                # فشل في تسليم بعض الأكواد - حذف نظيف
                logger.error(f"Critical: Failed during code delivery: {e}")
                # ما نقدر نرجع الكل بأمان هنا، نسجل الحادثة
                notify_admins(f"⚠️ <b>تنبيه أمني!</b>\nفشل في تسليم أكواد للمستخدم <code>{uid}</code>\nالخطأ: {e}\n\n<b>راجع يدوياً!</b>")
                
            # 📦 منطق إرسال موحّد:
            # دائماً نرسل ملف (حتى لو كود واحد) - حسب طلب المستخدم
            # - أوضح للعميل (الكود في ملف منظم)
            # - ما يصير في مشاكل HTML أو طول رسالة
            
            sent_successfully = False
            
            # إرسال ملف لكل عملية شراء (الطريقة الموحدة)
            try:
                file_content = ""
                p_name_for_file = p.get(f'name_{lang}', p.get('name_en', p.get('name_ar', 'product')))
                file_content += f"=== {p_name_for_file} ===\n"
                file_content += f"Quantity: {qty}\n"
                file_content += f"Total Paid: ${total_price:.2f}\n"
                file_content += f"Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                file_content += "=" * 40 + "\n\n"
                
                for i, code in enumerate(delivered_codes, 1):
                    file_content += f"{i}. {code}\n"
                
                f = io.BytesIO(file_content.encode('utf-8'))
                safe_name = re.sub(r'[^\w\-]', '_', str(pid))[:20]
                f.name = f"Your_Codes_{safe_name}.txt"
                
                if lang == 'ar':
                    if qty == 1:
                        success_msg = f"✅ <b>تم الشراء بنجاح!</b>\n\n📦 المنتج: <b>{clean_name(p.get('name_ar'))}</b>\n💰 المبلغ: <b>${total_price:.2f}</b>\n\n📄 الكود مرفق في الملف.\n\n<i>شكراً لاختيارك متجرنا 🛡️</i>"
                    else:
                        success_msg = f"✅ <b>تم الشراء بنجاح!</b>\n\n📦 الكمية: <b>{qty}</b> كود\n💰 المبلغ: <b>${total_price:.2f}</b>\n\n📄 الأكواد مرفقة في الملف لسهولة النسخ.\n\n<i>شكراً لاختيارك متجرنا 🛡️</i>"
                else:
                    if qty == 1:
                        success_msg = f"✅ <b>Purchase Successful!</b>\n\n📦 Product: <b>{clean_name(p.get('name_en', p.get('name_ar')))}</b>\n💰 Total: <b>${total_price:.2f}</b>\n\n📄 Code attached as file.\n\n<i>Thank you for choosing us 🛡️</i>"
                    else:
                        success_msg = f"✅ <b>Purchase Successful!</b>\n\n📦 Quantity: <b>{qty}</b> codes\n💰 Total: <b>${total_price:.2f}</b>\n\n📄 Codes attached as file for easy copying.\n\n<i>Thank you for choosing us 🛡️</i>"
                    
                bot.send_document(uid, f, caption=success_msg, parse_mode="HTML")
                sent_successfully = True
            except Exception as file_err:
                logger.error(f"Failed to send file for user {uid}: {file_err}")
                # Fallback 1: رسالة شات بسيطة بدون HTML
                try:
                    simple_msg = f"✅ تم الشراء!\n\nالأكواد:\n\n" + "\n".join(delivered_codes)
                    bot.send_message(uid, simple_msg[:4000])
                    sent_successfully = True
                except Exception as e2:
                    logger.error(f"Critical failure delivering codes: {e2}")
                    notify_admins(f"🚨 <b>عاجل!</b>\nفشل تسليم {qty} كود للمستخدم <code>{uid}</code>!\nالأكواد محجوزة وتم خصم ${total_price:.2f}.\n<b>راجع وسلّمها يدوياً!</b>")
            
            admin_msg = f"🔐 <b>إشعار إدارة (شراء تلقائي) ⚡</b>\n\n👤 العميل: {buyer_m} (<code>{uid}</code>)\n📦 المنتج: {clean_name(p.get('name_ar'))}\n🔢 الكمية: {qty}\n💰 دفع: ${total_price:.2f}"
            notify_admins(admin_msg)

        # 🔔 إشعار قناة اللوق (قابل للتعديل من CMS)
        if log_ch and log_ch != "Not Set":
            try: 
                obs_user = obscure_text(u.get('username') or str(uid))
                
                # 🆕 إضافة Premium Emoji للمنتج (لو موجود)
                product_name_clean = clean_name(p.get('name_en', p.get('name_ar', 'Product')))
                custom_emoji_id = p.get('custom_emoji_id')
                
                if custom_emoji_id:
                    # نستخدم Premium Emoji بدل 📦 العادي
                    product_name_log = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> <b>{product_name_clean}</b>'
                else:
                    product_name_log = f'📦 <b>{product_name_clean}</b>'
                
                # النص الافتراضي
                pub_msg = LANG['en']['log_purchase'].format(obs_user, product_name_log, qty)
                
                # شيك على النص المخصص من CMS
                custom_pub = db.custom_texts.find_one({'lang': 'en', 'key': 'log_purchase'})
                if custom_pub and custom_pub.get('value'):
                    try:
                        pub_msg = custom_pub['value'].format(obs_user, product_name_log, qty)
                    except:
                        pass
                
                bot.send_message(log_ch, pub_msg, parse_mode="HTML")
            except Exception as log_err: 
                logger.debug(f"Log channel error: {log_err}")

        # 🎁 منح مكافأة الإحالة للشخص اللي دعا هذا المشتري (لو موجود)
        # تشتغل في كل عملية شراء (ليست مرة واحدة)
        try:
            product_display = clean_name(p.get('name_ar') or p.get('name_en') or 'منتج')
            award_purchase_referral_reward(uid, product_display, total_price)
        except Exception as ref_err:
            logger.error(f"Error awarding referral on purchase: {ref_err}")
    
    finally:
        # 🛡 تحرير قفل الشراء دائماً (حتى لو حصل خطأ)
        _release_purchase_lock(uid)


def _release_reservation(reservation_id):
    """إرجاع الأكواد المحجوزة لو فشل الدفع"""
    try:
        db.product_stock.update_many(
            {'reservation_id': reservation_id},
            {
                '$set': {'is_sold': False},
                '$unset': {'reservation_id': "", 'reserved_at': ""}
            }
        )
    except Exception as e:
        logger.error(f"Error releasing reservation {reservation_id}: {e}")


# ============================================================
# 🛡 حماية #4: Rate Limiting لكل مستخدم
# ============================================================
_purchase_locks = {}              # {uid: lock_acquired_timestamp}
_purchase_locks_master = threading.Lock()
PURCHASE_LOCK_TIMEOUT = 60  # ثانية - أقصى وقت لعملية شراء واحدة

def _acquire_purchase_lock(uid):
    """
    يحاول الحصول على قفل لمنع المستخدم من فتح أكثر من عملية شراء.
    يرجع True لو نجح، False لو فيه عملية قائمة.
    """
    with _purchase_locks_master:
        now = int(time.time())
        existing = _purchase_locks.get(uid)
        
        # لو فيه قفل قديم وما انتهى timeout
        if existing and (now - existing) < PURCHASE_LOCK_TIMEOUT:
            return False
        
        # نأخذ القفل
        _purchase_locks[uid] = now
        return True

def _release_purchase_lock(uid):
    """تحرير قفل الشراء للمستخدم"""
    with _purchase_locks_master:
        _purchase_locks.pop(uid, None)


def _cleanup_stale_purchase_locks():
    """تنظيف الأقفال المعلقة (تشتغل في الخلفية)"""
    while True:
        try:
            time.sleep(120)  # كل دقيقتين
            with _purchase_locks_master:
                now = int(time.time())
                stale = [uid for uid, ts in _purchase_locks.items() if (now - ts) > PURCHASE_LOCK_TIMEOUT]
                for uid in stale:
                    _purchase_locks.pop(uid, None)
                if stale:
                    logger.info(f"🧹 تنظيف {len(stale)} قفل شراء معلق")
        except Exception as e:
            logger.error(f"Error in cleanup_stale_purchase_locks: {e}")

threading.Thread(target=_cleanup_stale_purchase_locks, daemon=True).start()

# ============================================================
# 🏦 13. بوابات الدفع (تحديث لقبول الهاش القصير)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "open_deposit")
def dep_init_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
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
    l = get_lang(uid)
    
    bot.clear_step_handler_by_chat_id(chat_id=uid)
    
    prompt = f"⭐️ <b>أرسل المبلغ الذي تريد شحنه بالدولار ($):</b>\n<i>(سيتم تحويله تلقائياً، 1$ = {STARS_RATE} نجمة)</i>" if l == 'ar' else f"⭐️ <b>Send the amount you want to deposit in USD ($):</b>\n<i>(Will be converted automatically, 1$ = {STARS_RATE} Stars)</i>"
    
    msg = bot.send_message(uid, prompt, parse_mode="HTML")
    bot.register_next_step_handler(msg, process_stars_amount, l)

def process_stars_amount(message, lang):
    uid = message.from_user.id
    try:
        usd_amount = float(message.text.strip())
        if usd_amount < 0.1:
            err = "❌ الحد الأدنى للشحن هو 0.1$" if lang == 'ar' else "❌ Minimum deposit is $0.1"
            bot.send_message(uid, err, parse_mode="HTML")
            return
            
        stars_amount = int(usd_amount * STARS_RATE)
        title = "شحن رصيد المتجر" if lang == 'ar' else "Shop Balance Deposit"
        desc = f"شحن حساب بمبلغ ${usd_amount:.2f}" if lang == 'ar' else f"Deposit ${usd_amount:.2f} to your account"
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
        err = "❌ الرجاء إرسال أرقام فقط." if lang == 'ar' else "❌ Please send numbers only."
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
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)
    bot.clear_step_handler_by_chat_id(chat_id=uid)
    
    wallet = get_setting('wallet_address')
    msg = bot.send_message(uid, get_text(uid, 'dep_pay', wallet), parse_mode="HTML")
    bot.register_next_step_handler(msg, verify_binance_pay, l)

@bot.callback_query_handler(func=lambda call: call.data.startswith("dep_crypto_"))
def dep_crypto_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid); coin = call.data.replace('dep_crypto_', '')
    bot.clear_step_handler_by_chat_id(chat_id=uid)
    
    if coin == "USDT": db_key = "usdt_address"
    elif coin == "USDT_BEP20": db_key = "usdt_bep20_address"
    elif coin == "TON": db_key = "ton_address"
    else: db_key = "ltc_address"
    
    wallet = get_setting(db_key)
    
    if coin == "USDT": 
        msg_txt = get_text(uid, 'dep_usdt', wallet)
    elif coin == "USDT_BEP20":
        msg_txt = f"🟡 <b>شحن عبر USDT (BEP-20)</b>\n\nأرسل المبلغ إلى المحفظة:\n<code>{wallet}</code>\n\n⚠️ <b>الشبكة المقبولة: BEP-20 (BSC) فقط.</b>\n⚠️ بعد التحويل، <b>أرسل الهاش (TxID) كنص هنا.</b>" if l=='ar' else f"🟡 <b>USDT (BEP-20) Deposit</b>\n\nSend to address:\n<code>{wallet}</code>\n\n⚠️ <b>Network: BEP-20 ONLY.</b>\n⚠️ Send <b>TxID (Hash)</b> here as text."
    elif coin == "TON":
        msg_txt = f"💎 <b>شحن عبر Toncoin (TON)</b>\n\nأرسل المبلغ إلى المحفظة:\n<code>{wallet}</code>\n\n⚠️ <b>تأكد من وضع الـ Memo إذا كان مطلوباً!</b>\n⚠️ بعد التحويل، <b>أرسل الهاش (TxID) كنص هنا.</b>" if l=='ar' else f"💎 <b>TON Deposit</b>\n\nSend to address:\n<code>{wallet}</code>\n\n⚠️ <b>Don't forget the Memo if required!</b>\n⚠️ Send <b>TxID (Hash)</b> here as text."
    else:
        msg_txt = get_text(uid, 'dep_ltc', wallet)
        
    msg = bot.send_message(uid, msg_txt, parse_mode="HTML")
    
    if coin == "LTC": bot.register_next_step_handler(msg, verify_ltc_public_blockchain, l, wallet)
    elif coin == "TON": bot.register_next_step_handler(msg, verify_crypto_tx, l, "TON")
    elif coin == "USDT_BEP20": bot.register_next_step_handler(msg, verify_crypto_tx, l, "USDT")
    else: bot.register_next_step_handler(msg, verify_crypto_tx, l, coin)

def verify_binance_pay(message, lang):
    uid = message.from_user.id
    if is_user_banned(uid): return
    if not message.text:
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML"); return

    tx_id = message.text.strip()
    
    if len(tx_id) < 5:
        bot.send_message(uid, "❌ <b>رقم العملية غير صحيح! الرجاء إرسال الـ Order ID بشكل صحيح.</b>", parse_mode="HTML")
        return
        
    with tx_lock:
        if tx_id in PROCESSING_TXS:
            bot.send_message(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل، يرجى عدم التكرار.</b>", parse_mode="HTML")
            return
        if db.used_transactions.find_one({'transaction_id': tx_id}):
            bot.reply_to(message, get_text(uid, 'tx_used')); return
        PROCESSING_TXS.add(tx_id)
        
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        
        pay_response = None
        attempt_num = 0
        max_total_time = 90  # 90 ثانية كحد أقصى (يحاول طوال هالمدة)
        start_time = time.time()
        
        # 🔥 حلقة محاولة مستمرة - ما توقف إلا لما تنجح
        while pay_response is None and (time.time() - start_time) < max_total_time:
            attempt_num += 1
            pay_response = execute_binance_call(
                lambda c: c.get_pay_trade_history(),
                max_retries=10,  # 10 بروكسيات في كل جولة
            )
            
            if pay_response is not None:
                break  # نجحنا!
            
            # كل 3 محاولات، نجلب بروكسيات جديدة بصمت
            if attempt_num % 3 == 0:
                try:
                    refresh_proxies(force=True)
                except: pass
            
            # محاولة بدون بروكسي بين المحاولات
            if attempt_num % 2 == 0:
                try:
                    direct_client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params={'timeout': 8})
                    pay_response = direct_client.get_pay_trade_history()
                    if pay_response is not None:
                        break
                except: pass
            
            time.sleep(0.3)  # وقفة صغيرة بين المحاولات
        
        # ✅ لو وصلنا هنا ولسا None بعد 90 ثانية، شي غريب جداً صار
        # لكن ما نطلع رسالة خطأ - نحاول مرة أخيرة بدون بروكسي
        if pay_response is None:
            try:
                direct_client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params={'timeout': 15})
                pay_response = direct_client.get_pay_trade_history()
            except:
                pass
        
        # ⚠️ في الحالة شبه المستحيلة - نعتبر الحوالة غير موجودة (مو خطأ سيرفر)
        if pay_response is None:
            bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
            return
        
        pay_h = pay_response.get('data', [])

        found = False; amt = 0.0
        current_time_ms = int(time.time() * 1000)
        
        for d in pay_h:
            if tx_id.lower() == str(d.get('orderId', '')).lower():
                tx_time = int(d.get('transactionTime', 0))
                if (current_time_ms - tx_time) > 24 * 60 * 60 * 1000:
                    bot.send_message(uid, "❌ <b>مرفوض:</b> الحوالة قديمة جداً.", parse_mode="HTML")
                    return
                found = True
                amt = float(d.get('amount', 0.0))
                break
                
        if found: credit_user(uid, amt, tx_id.lower(), lang, "Binance Pay")
        else: bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
    except Exception as e:
        logger.debug(f"Unexpected error in verify_binance_tx: {e}")
        bot.send_message(
            uid, 
            "⚠️ <b>تعذر الاتصال بسيرفر التحقق حالياً.</b>\n\n"
            "💡 انتظر دقيقة وحاول مرة ثانية، أو تواصل مع الإدارة.", 
            parse_mode="HTML"
        )
    finally:
        PROCESSING_TXS.discard(tx_id)

def verify_crypto_tx(message, lang, coin):
    """
    🛡 توجيه ذكي: TON يفحص من TONCenter (الـ blockchain العام)
    باقي العملات تفحص من Binance
    """
    # 🛡 TON يفحص من شبكة TON مباشرة (لا يحتاج بروكسي ولا Binance)
    if coin == "TON":
        ton_wallet = get_setting("ton_address", "Not Set")
        verify_ton_public_blockchain(message, lang, ton_wallet)
        return
    
    uid = message.from_user.id
    if is_user_banned(uid): return
    if not message.text:
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML"); return

    tx_id = message.text.strip().lower()
    
    if len(tx_id) < 5:
        bot.send_message(uid, "❌ <b>رقم الهاش (TxID) غير صحيح أو قصير جداً!</b>", parse_mode="HTML")
        return
        
    with tx_lock:
        if tx_id in PROCESSING_TXS:
            bot.send_message(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل، يرجى عدم التكرار.</b>", parse_mode="HTML")
            return
        if db.used_transactions.find_one({'transaction_id': tx_id}):
            bot.reply_to(message, get_text(uid, 'tx_used')); return
        PROCESSING_TXS.add(tx_id)
        
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        
        res = None
        attempt_num = 0
        max_total_time = 90  # 90 ثانية حد أقصى
        start_time = time.time()
        
        # 🔥 حلقة محاولة مستمرة - ما توقف إلا لما تنجح
        while res is None and (time.time() - start_time) < max_total_time:
            attempt_num += 1
            res = execute_binance_call(
                lambda c: c.get_deposit_history(coin=coin),
                max_retries=10,
            )
            
            if res is not None:
                break
            
            # كل 3 محاولات نجلب بروكسيات جديدة بصمت
            if attempt_num % 3 == 0:
                try:
                    refresh_proxies(force=True)
                except: pass
            
            # محاولة بدون بروكسي بين المحاولات
            if attempt_num % 2 == 0:
                try:
                    direct_client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params={'timeout': 8})
                    res = direct_client.get_deposit_history(coin=coin)
                    if res is not None:
                        break
                except: pass
            
            time.sleep(0.3)
        
        # محاولة أخيرة بدون بروكسي
        if res is None:
            try:
                direct_client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params={'timeout': 15})
                res = direct_client.get_deposit_history(coin=coin)
            except:
                pass
        
        # لو ما حصلنا نتيجة - نعتبر الحوالة غير موجودة (مو خطأ سيرفر)
        if res is None:
            bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
            return

        found = False; status = -1; amt = 0.0
        current_time_ms = int(time.time() * 1000)
        
        for d in res:
            api_txid = str(d.get('txId', '')).lower()
            if tx_id in api_txid:
                tx_time = int(d.get('insertTime', 0))
                if (current_time_ms - tx_time) > 24 * 60 * 60 * 1000:
                    bot.send_message(uid, "❌ <b>مرفوض:</b> الحوالة قديمة جداً.", parse_mode="HTML")
                    return
                found = True
                status = int(d.get('status', -1))
                amt = float(d.get('amount', 0.0))
                break
                
        if found:
            if status == 1: credit_user(uid, amt, tx_id, lang, f"Crypto {coin}")
            else: bot.send_message(uid, get_text(uid, 'dep_pending'), parse_mode="HTML")
        else: bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
    except Exception as e:
        logger.debug(f"Unexpected error in verify_crypto_tx: {e}")
        # ⚠️ ما نطلع رسالة خطأ سيرفر - نقول حوالة غير موجودة
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
    finally:
        PROCESSING_TXS.discard(tx_id)


def get_ltc_price_usd():
    """يجلب سعر LTC بالدولار من 3 مصادر مع تحقق منطقية (10$ - 1000$)"""
    # المصدر 1: Binance (عبر بروكسي - الأسرع)
    try:
        ticker = execute_binance_call(
            lambda c: c.get_symbol_ticker(symbol="LTCUSDT"),
            fast_mode=True,
            total_timeout=5
        )
        if ticker:
            price = float(ticker.get('price', 0))
            if 10 <= price <= 1000:
                logger.info(f"💱 سعر LTC من Binance: ${price:.4f}")
                return price
    except Exception as e:
        logger.debug(f"Binance LTC price error: {e}")
    
    # المصدر 2: CoinGecko
    try:
        cg_res = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=litecoin&vs_currencies=usd",
            timeout=10
        )
        if cg_res.status_code == 200:
            price = float(cg_res.json().get('litecoin', {}).get('usd', 0))
            if 10 <= price <= 1000:
                logger.info(f"💱 سعر LTC من CoinGecko: ${price:.4f}")
                return price
    except Exception as e:
        logger.debug(f"CoinGecko LTC price error: {e}")
    
    # المصدر 3: Coinbase
    try:
        cb_res = requests.get(
            "https://api.coinbase.com/v2/prices/LTC-USD/spot",
            timeout=10
        )
        if cb_res.status_code == 200:
            price = float(cb_res.json().get('data', {}).get('amount', 0))
            if 10 <= price <= 1000:
                logger.info(f"💱 سعر LTC من Coinbase: ${price:.4f}")
                return price
    except Exception as e:
        logger.debug(f"Coinbase LTC price error: {e}")
    
    return None


def get_ton_price_usd():
    """يجلب سعر TON بالدولار من 3 مصادر مع تحقق منطقية"""
    # المصدر 1: CoinGecko
    try:
        cg_res = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd",
            timeout=10
        )
        if cg_res.status_code == 200:
            price = float(cg_res.json().get('the-open-network', {}).get('usd', 0))
            if 0.5 <= price <= 50:  # نطاق منطقي لـ TON
                logger.info(f"💱 سعر TON من CoinGecko: ${price:.4f}")
                return price
    except Exception as e:
        logger.debug(f"CoinGecko TON price error: {e}")
    
    # المصدر 2: Coinbase
    try:
        cb_res = requests.get(
            "https://api.coinbase.com/v2/prices/TON-USD/spot",
            timeout=10
        )
        if cb_res.status_code == 200:
            price = float(cb_res.json().get('data', {}).get('amount', 0))
            if 0.5 <= price <= 50:
                logger.info(f"💱 سعر TON من Coinbase: ${price:.4f}")
                return price
    except Exception as e:
        logger.debug(f"Coinbase TON price error: {e}")
    
    # المصدر 3: Binance (احتياطي بالبروكسي)
    try:
        ticker = execute_binance_call(
            lambda c: c.get_symbol_ticker(symbol="TONUSDT"),
            fast_mode=True,
            total_timeout=5
        )
        if ticker:
            price = float(ticker.get('price', 0))
            if 0.5 <= price <= 50:
                logger.info(f"💱 سعر TON من Binance: ${price:.4f}")
                return price
    except Exception as e:
        logger.debug(f"Binance TON price error: {e}")
    
    return None


def verify_ton_public_blockchain(message, lang, wallet_address):
    """
    🛡 يفحص حوالات TON مباشرة من TON blockchain (بدون Binance، بدون بروكسي)
    يستخدم TONCenter API + TON Whales API + Tonviewer API
    """
    uid = message.from_user.id
    if is_user_banned(uid): return
    if not message.text:
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML"); return
    
    tx_id = message.text.strip()
    # 🛡 TON hashes case-sensitive - ما نسوي lower!
    tx_id_clean = tx_id.replace(' ', '').replace('\n', '')
    
    if len(tx_id_clean) < 20:
        bot.send_message(
            uid,
            "❌ <b>رقم الهاش (TxID) غير صحيح!</b>\n\n"
            "💡 <i>هاش TON يكون على شكل: <code>abc123...XYZ=</code> أو base64</i>\n"
            "تأكد من نسخه بالكامل من محفظتك.",
            parse_mode="HTML"
        )
        return
    
    if wallet_address == "Not Set" or len(wallet_address) < 10:
        bot.send_message(uid, "❌ <b>خطأ:</b> عنوان محفظة TON غير معين في البوت.", parse_mode="HTML")
        return
    
    # نستخدم lowercase للـ tracking في PROCESSING_TXS فقط
    tx_track_key = tx_id_clean.lower()
    
    with tx_lock:
        if tx_track_key in PROCESSING_TXS:
            bot.send_message(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل، يرجى عدم التكرار.</b>", parse_mode="HTML")
            return
        if db.used_transactions.find_one({'transaction_id': tx_track_key}):
            bot.reply_to(message, get_text(uid, 'tx_used')); return
        PROCESSING_TXS.add(tx_track_key)
    
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        
        received_ton = 0.0
        is_sender = False
        is_old = False
        is_confirmed = False
        data_source = None
        current_time = int(time.time())
        
        # 🛡 المصدر 1: TONCenter API (الأكثر موثوقية)
        try:
            # TONCenter يقبل hash بصيغ مختلفة
            # نجرب نبحث في الـ transactions الخاصة بعنواننا
            url = f"https://toncenter.com/api/v2/getTransactions"
            params = {
                "address": wallet_address,
                "limit": 50,  # آخر 50 transaction
                "archival": "true"
            }
            
            res = requests.get(url, params=params, timeout=15)
            if res.status_code == 200:
                data = res.json()
                if data.get("ok"):
                    transactions = data.get("result", [])
                    
                    for tx in transactions:
                        # هاش الـ tx
                        tx_hash = tx.get("transaction_id", {}).get("hash", "")
                        
                        # نشيك على الـ hash بطرق مختلفة (TON يستخدم صيغ متعددة)
                        if (tx_id_clean in tx_hash or 
                            tx_hash in tx_id_clean or 
                            tx_id_clean == tx_hash or
                            tx_id_clean.lower() == tx_hash.lower()):
                            
                            # تحقق من الوقت
                            tx_time = int(tx.get("utime", 0))
                            if tx_time > 0 and (current_time - tx_time) > 24 * 60 * 60:
                                is_old = True
                                break
                            
                            # تحقق هل هي إيداع (in_msg موجود)
                            in_msg = tx.get("in_msg", {})
                            if in_msg:
                                source_addr = in_msg.get("source", "")
                                # لو المصدر هو عنواننا، فهي حوالة صادرة
                                if source_addr == wallet_address:
                                    is_sender = True
                                    break
                                
                                # المبلغ بالـ nanoTON (1 TON = 1_000_000_000 nanoTON)
                                value_nano = int(in_msg.get("value", 0))
                                if value_nano > 0:
                                    received_ton = value_nano / 1_000_000_000
                                    is_confirmed = True
                                    data_source = "toncenter"
                                    logger.info(f"💎 TON من TONCenter: {received_ton:.6f} TON")
                                    break
        except Exception as e:
            logger.error(f"TONCenter API error: {e}")
        
        # 🛡 المصدر 2: TonAPI (احتياطي)
        if data_source is None and not is_sender and not is_old:
            try:
                # TonAPI v2
                url2 = f"https://tonapi.io/v2/blockchain/accounts/{wallet_address}/transactions"
                params2 = {"limit": 50}
                
                res2 = requests.get(url2, params=params2, timeout=15)
                if res2.status_code == 200:
                    data2 = res2.json()
                    transactions2 = data2.get("transactions", [])
                    
                    for tx in transactions2:
                        tx_hash = tx.get("hash", "")
                        
                        if (tx_id_clean in tx_hash or 
                            tx_hash in tx_id_clean or 
                            tx_id_clean.lower() == tx_hash.lower()):
                            
                            tx_time = int(tx.get("utime", 0))
                            if tx_time > 0 and (current_time - tx_time) > 24 * 60 * 60:
                                is_old = True
                                break
                            
                            in_msg = tx.get("in_msg", {})
                            if in_msg:
                                source = in_msg.get("source", {})
                                source_addr = source.get("address", "") if isinstance(source, dict) else str(source)
                                
                                if source_addr == wallet_address:
                                    is_sender = True
                                    break
                                
                                value_nano = int(in_msg.get("value", 0))
                                if value_nano > 0:
                                    received_ton = value_nano / 1_000_000_000
                                    is_confirmed = True
                                    data_source = "tonapi"
                                    logger.info(f"💎 TON من TonAPI: {received_ton:.6f} TON")
                                    break
            except Exception as e:
                logger.error(f"TonAPI error: {e}")
        
        # 🛡 المصدر 3: Tonviewer/Tonscan (احتياطي ثاني)
        if data_source is None and not is_sender and not is_old:
            try:
                # ندوّر الـ tx بشكل عام (بدون عنوان محدد)
                url3 = f"https://toncenter.com/api/v2/getTransactionByHash"
                params3 = {"hash": tx_id_clean}
                
                res3 = requests.get(url3, params=params3, timeout=15)
                if res3.status_code == 200:
                    data3 = res3.json()
                    if data3.get("ok"):
                        tx = data3.get("result", {})
                        tx_time = int(tx.get("utime", 0))
                        if tx_time > 0 and (current_time - tx_time) > 24 * 60 * 60:
                            is_old = True
                        else:
                            in_msg = tx.get("in_msg", {})
                            destination = in_msg.get("destination", "")
                            source_addr = in_msg.get("source", "")
                            
                            # تحقق إن الوجهة فعلاً عنواننا
                            if destination == wallet_address:
                                if source_addr == wallet_address:
                                    is_sender = True
                                else:
                                    value_nano = int(in_msg.get("value", 0))
                                    if value_nano > 0:
                                        received_ton = value_nano / 1_000_000_000
                                        is_confirmed = True
                                        data_source = "toncenter_direct"
                                        logger.info(f"💎 TON بحث مباشر: {received_ton:.6f} TON")
            except Exception as e:
                logger.error(f"TONCenter direct lookup error: {e}")
        
        # 🛡 الاستجابات
        if is_old:
            bot.send_message(
                uid,
                "❌ <b>مرفوض:</b> الحوالة قديمة جداً (أكثر من 24 ساعة).\n\n"
                "💡 يرجى التواصل مع الإدارة للمساعدة.",
                parse_mode="HTML"
            )
            return
        
        if is_sender:
            bot.send_message(
                uid,
                "❌ <b>مرفوض:</b> هذه الحوالة صادرة من محفظتنا وليست إيداعاً.",
                parse_mode="HTML"
            )
            return
        
        if received_ton == 0.0 or not is_confirmed:
            bot.send_message(
                uid,
                "❌ <b>لم نتمكن من العثور على الحوالة!</b>\n\n"
                "💡 <b>تأكد من:</b>\n"
                "• نسخ الهاش (TxID) بشكل صحيح بدون مسافات\n"
                "• أن الحوالة مكتملة على الشبكة (≥1 تأكيد)\n"
                "• أنك أرسلت إلى المحفظة الصحيحة\n"
                "• مرور دقيقة على الأقل بعد إرسال الحوالة\n\n"
                "🔄 جرب مرة ثانية بعد دقيقة، أو تواصل مع الإدارة.",
                parse_mode="HTML"
            )
            return
        
        # 🛡 جلب سعر TON والتحقق من منطقيته
        ton_price = get_ton_price_usd()
        if ton_price is None:
            bot.send_message(
                uid,
                "❌ <b>تعذر الحصول على سعر TON الحالي.</b>\n\n"
                "يرجى المحاولة بعد دقائق قليلة. لن يُخصم منك شيء.",
                parse_mode="HTML"
            )
            logger.error(f"⚠️ فشل جلب سعر TON للحوالة {tx_id_clean[:16]}")
            return
        
        # حساب المبلغ بالدولار
        usd_amount = round(received_ton * ton_price, 2)
        
        logger.info(
            f"✅ حساب TON نهائي:\n"
            f"   tx_id: {tx_id_clean[:30]}...\n"
            f"   received_ton: {received_ton:.6f} TON\n"
            f"   ton_price: ${ton_price:.4f}\n"
            f"   usd_amount: ${usd_amount:.2f}\n"
            f"   data_source: {data_source}"
        )
        
        credit_user(uid, usd_amount, tx_track_key, lang, "Toncoin (TON)")
    
    except Exception as e:
        logger.error(f"Unexpected error in verify_ton_public_blockchain: {e}")
        bot.send_message(
            uid,
            "❌ <b>حدث خطأ أثناء فحص الشبكة.</b>\n\nيرجى المحاولة بعد قليل.",
            parse_mode="HTML"
        )
    finally:
        PROCESSING_TXS.discard(tx_track_key)

def verify_ltc_public_blockchain(message, lang, wallet_address):
    uid = message.from_user.id
    if is_user_banned(uid): return
    if not message.text:
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML"); return
        
    tx_id = message.text.strip().lower()
    
    if len(tx_id) < 5:
        bot.send_message(uid, "❌ <b>رقم الهاش (TxID) غير صحيح أو قصير جداً! تأكد من نسخه بالكامل.</b>", parse_mode="HTML")
        return
        
    if wallet_address == "Not Set" or len(wallet_address) < 10:
        bot.send_message(uid, "❌ <b>خطأ:</b> عنوان المحفظة غير معين.", parse_mode="HTML")
        return
        
    with tx_lock:
        if tx_id in PROCESSING_TXS:
            bot.send_message(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل، يرجى عدم التكرار.</b>", parse_mode="HTML")
            return
        if db.used_transactions.find_one({'transaction_id': tx_id}):
            bot.reply_to(message, get_text(uid, 'tx_used')); return
        PROCESSING_TXS.add(tx_id)
        
    try:
        bot.send_message(uid, get_text(uid, 'crypto_checking'), parse_mode="HTML")
        received_ltc = 0.0
        confirmations = 0
        is_sender = False
        is_old = False
        current_time = int(time.time())
        data_source = None  # 🛡 لتتبع من أين جلبنا البيانات (يمنع الجمع المضاعف)
        
        # 🛡 المصدر الأول: litecoinspace.org
        try:
            url = f"https://litecoinspace.org/api/tx/{tx_id}"
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                data = res.json()
                block_time = data.get("status", {}).get("block_time", 0)
                if block_time > 0 and (current_time - block_time) > 24 * 60 * 60:
                    is_old = True
                
                # فحص هل المرسل من محفظتنا (إيداع ضد سحب)
                for vin in data.get("vin", []):
                    if vin.get("prevout", {}).get("scriptpubkey_address") == wallet_address:
                        is_sender = True
                        break
                
                if data.get("status", {}).get("confirmed"): 
                    confirmations = 1
                
                # 🛡 حساب المبلغ المستلم في عنواننا فقط (بدون تكرار)
                # نستخدم vout index كمفتاح فريد لتفادي outputs مكررة
                seen_vouts = set()
                temp_received = 0.0
                for idx, vout in enumerate(data.get("vout", [])):
                    if vout.get("scriptpubkey_address") == wallet_address:
                        vout_key = f"litecoinspace_{idx}_{vout.get('value', 0)}"
                        if vout_key not in seen_vouts:
                            seen_vouts.add(vout_key)
                            temp_received += float(vout.get("value", 0)) / 100000000.0
                
                if temp_received > 0:
                    received_ltc = temp_received  # 🛡 = بدل +=
                    data_source = "litecoinspace"
                    logger.info(f"💰 LTC من litecoinspace: {received_ltc:.8f} LTC للحوالة {tx_id[:16]}")
        except Exception as e:
            logger.error(f"LTC API 1 error: {e}")

        # 🛡 المصدر الثاني (احتياطي): blockcypher.com
        # فقط إذا لم نحصل على بيانات من المصدر الأول
        if data_source is None and not is_sender and not is_old:
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
                        except: pass
                    
                    for inp in data2.get("inputs", []):
                        if wallet_address in inp.get("addresses", []):
                            is_sender = True
                            break
                    
                    confirmations = data2.get("confirmations", 0)
                    
                    # 🛡 حساب المبلغ بدون تكرار
                    seen_outputs = set()
                    temp_received2 = 0.0
                    for idx, output in enumerate(data2.get("outputs", [])):
                        if wallet_address in output.get("addresses", []):
                            output_key = f"blockcypher_{idx}_{output.get('value', 0)}"
                            if output_key not in seen_outputs:
                                seen_outputs.add(output_key)
                                temp_received2 += float(output.get("value", 0)) / 100000000.0
                    
                    if temp_received2 > 0:
                        received_ltc = temp_received2  # 🛡 = بدل +=
                        data_source = "blockcypher"
                        logger.info(f"💰 LTC من blockcypher: {received_ltc:.8f} LTC للحوالة {tx_id[:16]}")
            except Exception as e:
                logger.error(f"LTC API 2 error: {e}")

        if is_old:
            bot.send_message(uid, "❌ <b>مرفوض:</b> الحوالة قديمة جداً.", parse_mode="HTML")
            return
        if is_sender:
            bot.send_message(uid, "❌ <b>مرفوض:</b> هذه الحوالة صادرة من محفظتنا وليست إيداعاً.", parse_mode="HTML")
            return

        if received_ltc > 0:
            if confirmations >= 1:
                # 🛡 جلب سعر LTC تلقائياً من 3 مصادر
                ltc_price = get_ltc_price_usd()
                
                # 🛡 إذا فشلت كل المصادر، نرفض العملية بدل ما نحط سعر تخميني
                if ltc_price is None:
                    bot.send_message(
                        uid, 
                        "❌ <b>تعذر الحصول على سعر LTC الحالي.</b>\n\n"
                        "يرجى المحاولة بعد دقائق قليلة. لن يُخصم منك شيء.", 
                        parse_mode="HTML"
                    )
                    logger.error(f"⚠️ فشل جلب سعر LTC للحوالة {tx_id} - رُفضت لحماية المستخدم")
                    return
                
                # 🛡 حساب نهائي مع تقريب آمن إلى منزلتين عشريتين
                usd_amount = round(received_ltc * ltc_price, 2)
                
                # 🛡 تسجيل مفصل لكل عملية حساب (للمراجعة لو في مشكلة)
                logger.info(
                    f"✅ حساب LTC نهائي:\n"
                    f"   tx_id: {tx_id[:16]}...\n"
                    f"   received_ltc: {received_ltc:.8f}\n"
                    f"   ltc_price: ${ltc_price:.4f}\n"
                    f"   usd_amount: ${usd_amount:.2f}\n"
                    f"   data_source: {data_source}"
                )
                
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
    buyer_m = f"@{u['username']}" if u and u.get('username') else f"مستخدم"
    
    admin_msg = f"🔐 <b>إشعار إدارة (إيداع)</b>\n\n👤 العميل: {buyer_m} (<code>{uid}</code>)\n💰 المبلغ: <b>${amt:.2f}</b>\n💳 الطريقة: {method}\n🆔 رقم العملية:\n<code>{tx_id}</code>"
    notify_admins(admin_msg)
    
    log_ch = get_setting('log_channel')
    if log_ch and log_ch != "Not Set":
        obs_user = obscure_text(u.get('username') or str(uid))
        try: 
            # 🔔 النص الافتراضي للإيداع
            pub_msg = LANG['en']['log_deposit'].format(obs_user, f"{amt:.2f}", method)
            
            # شيك على النص المخصص من CMS
            custom_dep = db.custom_texts.find_one({'lang': 'en', 'key': 'log_deposit'})
            if custom_dep and custom_dep.get('value'):
                try:
                    pub_msg = custom_dep['value'].format(obs_user, f"{amt:.2f}", method)
                except:
                    pass
            
            bot.send_message(log_ch, pub_msg, parse_mode="HTML")
        except Exception as log_err: 
            logger.debug(f"Log channel deposit error: {log_err}")

# ============================================================
# 👑 14. لوحة الإدارة ونظام التقارير 
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "admin_panel_main")
def admin_main_ui(call):
    bot.answer_callback_query(call.id)
    l = get_lang(call.from_user.id)
    markup = InlineKeyboardMarkup(row_width=2)
    if l == 'en':
        markup.add(InlineKeyboardButton("➕ Add Product", callback_data="ad_p_add"),
                   InlineKeyboardButton("📦 Manage Stock", callback_data="ad_s_list"))
        markup.add(InlineKeyboardButton("📝 Edit Product", callback_data="ad_p_edit"),
                   InlineKeyboardButton("🗑 Delete Product", callback_data="ad_p_del"))
        markup.add(InlineKeyboardButton("👥 Users & Balances", callback_data="ad_users_main"),
                   InlineKeyboardButton("🚫 Ban / Unban User", callback_data="ad_ban_user"))
        markup.add(InlineKeyboardButton("👑 Promote Admin", callback_data="ad_new_admin"),
                   InlineKeyboardButton("💰 Gift Balance", callback_data="ad_gift"))
        markup.add(InlineKeyboardButton("📜 Records", callback_data="ad_logs_all"),
                   InlineKeyboardButton("📢 Broadcast", callback_data="ad_bc"))
        markup.add(InlineKeyboardButton("🌟 Set Product Icon", callback_data="ad_prod_emoji_start"))
        markup.add(InlineKeyboardButton("✏️ Customize Bot (CMS)", callback_data="ad_texts_main"))
        markup.add(InlineKeyboardButton("⚙️ Settings", callback_data="ad_shop_settings"),
                   InlineKeyboardButton("📢 Forced Sub", callback_data="ad_fsub_list"))
        markup.add(InlineKeyboardButton("🎓 API Settings", callback_data="ad_api_main"))
        markup.add(InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu_refresh"))
        text = "👑 <b>Admin Dashboard:</b>"
    else:
        markup.add(InlineKeyboardButton("➕ أضف منتج", callback_data="ad_p_add"),
                   InlineKeyboardButton("📦 إدارة الستوك", callback_data="ad_s_list"))
        markup.add(InlineKeyboardButton("📝 تعديل منتج", callback_data="ad_p_edit"),
                   InlineKeyboardButton("🗑 حذف منتج", callback_data="ad_p_del"))
        markup.add(InlineKeyboardButton("👥 إدارة العملاء", callback_data="ad_users_main"),
                   InlineKeyboardButton("🚫 حظر / فك حظر", callback_data="ad_ban_user"))
        markup.add(InlineKeyboardButton("👑 ترقية مدير", callback_data="ad_new_admin"),
                   InlineKeyboardButton("💰 شحن رصيد", callback_data="ad_gift"))
        markup.add(InlineKeyboardButton("📜 السجلات", callback_data="ad_logs_all"),
                   InlineKeyboardButton("📢 برودكاست للأعضاء", callback_data="ad_bc"))
        markup.add(InlineKeyboardButton("🌟 تعيين أيقونة لمنتج", callback_data="ad_prod_emoji_start"))
        markup.add(InlineKeyboardButton("✏️ تخصيص البوت والأزرار", callback_data="ad_texts_main"))
        markup.add(InlineKeyboardButton("⚙️ إعدادات المتجر", callback_data="ad_shop_settings"),
                   InlineKeyboardButton("📢 الاشتراك الإجباري", callback_data="ad_fsub_list"))
        markup.add(InlineKeyboardButton("🎓 إعدادات التفعيلات", callback_data="ad_api_main"))
        markup.add(InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="main_menu_refresh"))
        text = "👑 <b>لوحة القيادة (الإدارة):</b>"
        
    try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

# ============================================================
# ✏️ نظام تخصيص نصوص البوت والأزرار المتقدم (CMS)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "ad_texts_main")
def ad_texts_main_ui(call):
    bot.answer_callback_query(call.id)
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("📝 نصوص الرسائل", callback_data="ad_cms_msgs"),
               InlineKeyboardButton("🎛 أزرار البوت", callback_data="ad_cms_btns_cats"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    bot.edit_message_text("✏️ <b>نظام التخصيص (CMS):</b>\nاختر ماذا تريد أن تخصص:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_cms_msgs")
def ad_cms_msgs_ui(call):
    bot.answer_callback_query(call.id)
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📋 رسالة الترحيب (Start)", callback_data="edit_txt_welcome"))
    markup.add(InlineKeyboardButton("💳 رسالة قسم الشحن", callback_data="edit_txt_dep_choose"))
    markup.add(InlineKeyboardButton("👥 رسالة قسم الإحالات", callback_data="edit_txt_invite_txt"))
    markup.add(InlineKeyboardButton("🔔 إشعار توفر ستوك", callback_data="edit_txt_new_stock"))
    markup.add(InlineKeyboardButton("📉 إشعار التخفيضات", callback_data="edit_txt_price_drop"))
    markup.add(InlineKeyboardButton("🏪 عنوان المتجر", callback_data="edit_txt_store_title"))
    # 🆕 إشعارات اللوق لنظام الإحالات
    markup.add(InlineKeyboardButton("🎁 لوق: مكافأة شراء إحالة", callback_data="edit_txt_log_ref_purchase"))
    markup.add(InlineKeyboardButton("🏆 لوق: إنجاز 10 إحالات", callback_data="edit_txt_log_ref_milestone"))
    markup.add(InlineKeyboardButton("💌 رسالة المُحيل (شراء صديقه)", callback_data="edit_txt_ref_purchase_dm"))
    # 🆕 إشعارات اللوق العامة
    markup.add(InlineKeyboardButton("🛒 لوق: شراء بنجاح", callback_data="edit_txt_log_purchase"))
    markup.add(InlineKeyboardButton("💳 لوق: إيداع بنجاح", callback_data="edit_txt_log_deposit"))
    markup.add(InlineKeyboardButton("✨ لوق: تفعيل Gemini", callback_data="edit_txt_log_gemini"))
    markup.add(InlineKeyboardButton("🎓 لوق: تفعيل GitHub", callback_data="edit_txt_log_github"))
    # 🆕 شروط الاستخدام
    markup.add(InlineKeyboardButton("📜 محتوى شروط الاستخدام", callback_data="edit_txt_terms_content"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="ad_texts_main"))
    bot.edit_message_text("📝 <b>تخصيص نصوص الرسائل:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_cms_btns_cats")
def ad_cms_btns_cats_ui(call):
    bot.answer_callback_query(call.id)
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("🏠 أزرار القائمة الرئيسية", callback_data="ad_cms_b_start"))
    markup.add(InlineKeyboardButton("💳 أزرار الشحن والدفع", callback_data="ad_cms_b_dep"))
    markup.add(InlineKeyboardButton("👤 أزرار الملف والمشتريات", callback_data="ad_cms_b_prof"))
    markup.add(InlineKeyboardButton("🛒 أزرار المتجر والتنقل", callback_data="ad_cms_b_shop"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="ad_texts_main"))
    bot.edit_message_text("🎛 <b>تخصيص أزرار البوت:</b>\nاختر القسم:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cms_b_"))
def ad_cms_btns_list(call):
    bot.answer_callback_query(call.id)
    cat = call.data.replace("ad_cms_b_", "")
    
    btn_categories = {
        'start': ['btn_products', 'btn_deposit', 'btn_profile', 'btn_invite', 'btn_support', 'btn_lang', 'btn_terms', 'btn_admin'],
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

# ----------- دوال تعديل النصوص (الرسائل) -----------
@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_txt_"))
def ad_edit_txt_prompt(call):
    bot.answer_callback_query(call.id)
    key = call.data.replace("edit_txt_", "")
    
    current_val = db.custom_texts.find_one({'lang': 'ar', 'key': key})
    current_text = current_val['value'] if current_val else LANG['ar'].get(key, "")
    
    # 🆕 شرح المتغيرات لكل نص (يساعد الأدمن)
    placeholders_info = {
        'invite_txt': (
            "💡 <b>المتغيرات في هذا النص (بالترتيب):</b>\n"
            "<code>{}</code> 1 = إجمالي الزيارات\n"
            "<code>{}</code> 2 = عدد المعلقين\n"
            "<code>{}</code> 3 = عدد النشطين\n"
            "<code>{}</code> 4 = عدد اللي غادروا\n"
            "<code>{}</code> 5 = الأرباح (الرصيد)\n"
            "<code>{}</code> 6 = اسم البوت\n"
            "<code>{}</code> 7 = معرف المستخدم"
        ),
        'log_ref_purchase': (
            "💡 <b>المتغيرات (2 فقط):</b>\n"
            "<code>{}</code> 1 = اسم المُحيل (مخفي)\n"
            "<code>{}</code> 2 = قيمة المكافأة (مثل 0.10)\n\n"
            "⚠️ <i>تم إلغاء ذكر اسم المُدعَى وقيمة الشراء في اللوق - تشجيعي فقط</i>"
        ),
        'log_ref_milestone': (
            "💡 <b>المتغيرات (3 فقط):</b>\n"
            "<code>{}</code> 1 = اسم المُحيل (مخفي)\n"
            "<code>{}</code> 2 = عدد الإحالات النشطة\n"
            "<code>{}</code> 3 = قيمة المكافأة (مثل 0.10)\n\n"
            "⚠️ <i>تم إلغاء ذكر إجمالي الأرباح - تشجيعي فقط</i>"
        ),
        'ref_purchase_dm': (
            "💡 <b>المتغيرات في رسالة المُحيل (3 متغيرات):</b>\n"
            "<code>{}</code> 1 = اسم الصديق المُدعَى (مخفي)\n"
            "<code>{}</code> 2 = قيمة المكافأة الجديدة (مثل 0.10)\n"
            "<code>{}</code> 3 = الرصيد الجديد للمُحيل\n\n"
            "💡 <i>هذي رسالة خاصة تُرسل للمُحيل لما يشتري شخص من إحالاته</i>"
        ),
        'log_purchase': (
            "💡 <b>المتغيرات (3 متغيرات):</b>\n"
            "<code>{}</code> 1 = اسم المستخدم (مخفي)\n"
            "<code>{}</code> 2 = اسم المنتج\n"
            "<code>{}</code> 3 = الكمية المشتراة"
        ),
        'log_deposit': (
            "💡 <b>المتغيرات (3 متغيرات):</b>\n"
            "<code>{}</code> 1 = اسم المستخدم (مخفي)\n"
            "<code>{}</code> 2 = المبلغ (مثل 5.00)\n"
            "<code>{}</code> 3 = طريقة الدفع (Binance Pay / USDT / TON ...)"
        ),
        'log_gemini': (
            "💡 <b>المتغيرات (1 متغير):</b>\n"
            "<code>{}</code> 1 = اسم حساب Gemini (مخفي)\n\n"
            "💡 <i>هذا الإشعار يُرسل في اللوق عند كل تفعيل Gemini Advanced ناجح</i>"
        ),
        'log_github': (
            "💡 <b>المتغيرات (1 متغير):</b>\n"
            "<code>{}</code> 1 = اسم حساب GitHub (مخفي)\n\n"
            "💡 <i>هذا الإشعار يُرسل في اللوق عند كل تفعيل GitHub Student ناجح</i>"
        ),
        'terms_content': (
            "💡 <b>محتوى شروط الاستخدام</b>\n\n"
            "🎨 <b>يدعم:</b>\n"
            "• Premium Emojis ✨\n"
            "• تنسيقات HTML (<b>, <i>, <code>, <u>)\n"
            "• الفواصل البصرية ━━━━\n"
            "• الإيموجي العادي 📜🎉\n"
            "• الأسطر الفارغة (تُحفظ كما هي)\n\n"
            "💡 <i>لا توجد متغيرات في هذا النص - اكتبه بحرية!</i>"
        ),
        'new_stock': (
            "💡 <b>المتغيرات في هذا النص (بالترتيب):</b>\n"
            "<code>{}</code> 1 = اسم المنتج\n"
            "<code>{}</code> 2 = عدد الأكواد المتوفرة"
        ),
        'price_drop': (
            "💡 <b>المتغيرات في هذا النص (بالترتيب):</b>\n"
            "<code>{}</code> 1 = اسم المنتج\n"
            "<code>{}</code> 2 = السعر القديم\n"
            "<code>{}</code> 3 = السعر الجديد"
        ),
        'welcome': (
            "💡 <b>المتغيرات في هذا النص (بالترتيب):</b>\n"
            "<code>{}</code> 1 = معرف المستخدم\n"
            "<code>{}</code> 2 = اسم المستخدم\n"
            "<code>{}</code> 3 = عدد مستخدمي البوت\n"
            "<code>{}</code> 4 = رصيد المستخدم"
        ),
    }
    
    info_section = placeholders_info.get(key, "")
    info_block = f"\n\n{info_section}\n\n<i>⚠️ تأكد من نسخ كل المتغيرات {{}}  بالعدد الصحيح والترتيب الصحيح!</i>" if info_section else ""

    # 🛡 نعرض النص الحالي كنص بمعالجة شاملة لـ HTML
    # المشكلة: لو النص يحتوي على <u> أو tag غير متوازن، Telegram يرفضه
    # الحل: نرسله كرسالة plain text (بدون parse_mode أصلاً)
    try:
        bot.send_message(
            call.message.chat.id, 
            f"📝 النص الحالي (انسخه وعدّل عليه):\n\n{current_text}"
            # ⚠️ بدون parse_mode! النص يظهر خام بدون تفسير HTML
        )
    except Exception as send_err:
        logger.error(f"Failed to send current text: {send_err}")
        bot.send_message(call.message.chat.id, "📝 النص الحالي غير قابل للعرض. عدّله بإرسال نص جديد.")
    
    # ثانياً: رسالة التعليمات (هذي آمنة لأن الـ HTML تحت سيطرتنا)
    msg_text = (
        f"━━━━━━━━━━━━━━━\n"
        f"👇 <b>انسخ النص فوق، عدّل عليه، وأرسله لي الآن.</b>"
        f"{info_block}\n\n"
        f"<i>سيتم ترجمته للإنجليزي تلقائياً مع حماية الرموز والتنسيقات والأسطر!</i>\n\n"
        f"💡 <i>لإلغاء العملية أرسل: <b>الغاء</b></i>"
    )
    try:
        msg = bot.send_message(call.message.chat.id, msg_text, parse_mode="HTML")
    except Exception:
        # fallback بدون parse_mode
        msg = bot.send_message(call.message.chat.id, "👇 أرسل النص الجديد. أرسل 'الغاء' للإلغاء.")
    
    bot.register_next_step_handler(msg, ad_save_custom_text, key)

def ad_save_custom_text(message, key):
    if message.text and message.text.strip() == "الغاء":
        bot.send_message(message.chat.id, "❌ تم إلغاء عملية التعديل.")
        return
        
    if not message.text:
        bot.send_message(message.chat.id, "❌ يجب إرسال نص.")
        return
        
    bot.send_message(message.chat.id, "⏳ جاري حفظ النص...")
    
    # 🆕 نستخرج النص مع الـ Premium Emojis
    final_text = extract_custom_emojis_to_html(message)
    
    # 🆕 اكتشاف اللغة: نشيك على النص بعد إزالة HTML والإيموجيات
    text_only = re.sub(r'<[^>]+>', '', final_text)
    text_only = re.sub(r'<tg-emoji[^>]*>.*?</tg-emoji>', '', text_only, flags=re.DOTALL)
    text_only = re.sub(r'\{[^}]*\}', '', text_only)
    text_only = re.sub(r'[━─═]+', '', text_only)
    # نشيل الإيموجي العادي
    emoji_pattern = re.compile(
        "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF\U00002500-\U00002BEF\U00002702-\U000027B0"
        "\u2640-\u2642\u2600-\u2B55\u200d\u23cf\u23e9\u231a\ufe0f\u3030]+",
        flags=re.UNICODE
    )
    text_only = emoji_pattern.sub('', text_only).strip()
    
    # نعد أحرف عربية vs إنجليزية
    arabic_chars = len(re.findall(r'[\u0600-\u06FF]', text_only))
    english_chars = len(re.findall(r'[a-zA-Z]', text_only))
    
    # نحدد لغة النص
    if english_chars > arabic_chars * 1.5:
        # 🆕 النص إنجليزي → نحفظه كما هو في النسختين (لا ترجمة)
        source_lang = 'en'
        final_text_en = final_text
        # نترجمه للعربي
        final_text_ar = safe_translate_for_cms(final_text, 'ar')
    elif arabic_chars > english_chars * 1.5:
        # النص عربي → نحفظه كما هو + نترجم للإنجليزي
        source_lang = 'ar'
        final_text_ar = final_text
        final_text_en = safe_translate_for_cms(final_text, 'en')
    else:
        # نص مختلط أو غير واضح → نحفظ في النسختين بدون ترجمة
        source_lang = 'mixed'
        final_text_ar = final_text
        final_text_en = final_text
    
    # 🛡 فحص: عدد placeholders في النسختين لازم يكون متطابق
    ar_count = len(re.findall(r'\{[^}]*\}', final_text_ar))
    en_count = len(re.findall(r'\{[^}]*\}', final_text_en))
    
    if ar_count != en_count:
        logger.warning(f"Translation lost placeholders for '{key}': ar={ar_count}, en={en_count}")
        # نخلي النسختين متطابقتين
        if source_lang == 'en':
            final_text_ar = final_text_en
        else:
            final_text_en = final_text_ar
        bot.send_message(
            message.chat.id,
            "⚠️ <b>ملاحظة:</b> تم حفظ النص بدون ترجمة لحماية المتغيرات والتنسيقات.",
            parse_mode="HTML"
        )
    
    # 🛡 فحص: عدد Premium Emojis في النسختين
    ar_emojis = len(re.findall(r'<tg-emoji', final_text_ar))
    en_emojis = len(re.findall(r'<tg-emoji', final_text_en))
    
    if ar_emojis != en_emojis:
        logger.warning(f"Translation lost premium emojis: ar={ar_emojis}, en={en_emojis}")
        # نخلي النسختين متطابقتين
        if source_lang == 'en':
            final_text_ar = final_text_en
        else:
            final_text_en = final_text_ar
            
    # حفظ النصين
    db.custom_texts.update_one({'lang': 'ar', 'key': key}, {'$set': {'value': final_text_ar}}, upsert=True)
    db.custom_texts.update_one({'lang': 'en', 'key': key}, {'$set': {'value': final_text_en}}, upsert=True)
    
    # رسالة تأكيد بسيطة (بدون كود escape عشان يبان شكل النص الحقيقي)
    lang_label = "🇸🇦 عربي" if source_lang == 'ar' else ("🇺🇸 إنجليزي" if source_lang == 'en' else "🌐 مختلط")
    
    bot.send_message(
        message.chat.id, 
        f"✅ <b>تم الحفظ بنجاح!</b>\n\n"
        f"📝 <b>لغة النص:</b> {lang_label}\n"
        f"💾 <b>المحفوظ:</b> عربي + إنجليزي\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"📌 <b>معاينة النص المحفوظ:</b>",
        parse_mode="HTML"
    )
    
    # نرسل النص نفسه كما هو (يبان الإيموجيات والتنسيقات)
    try:
        bot.send_message(message.chat.id, final_text_en, parse_mode="HTML")
    except Exception as preview_err:
        logger.debug(f"Preview error: {preview_err}")
        bot.send_message(message.chat.id, "⚠️ المعاينة تعذرت لكن النص محفوظ.")

# ----------- دوال تعديل الأزرار -----------
@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_btn_"))
def ad_edit_btn_prompt(call):
    bot.answer_callback_query(call.id)
    key = call.data.replace("edit_btn_", "")
    
    current_text, current_emoji = get_btn_data(call.from_user.id, key)
    
    msg_text = f"الزر الحالي: <code>{html.escape(current_text)}</code>\n\n👇 <b>أرسل الاسم الجديد للزر الآن. يمكنك إضافة (Premium Emoji) في رسالتك وسأقوم بالتقاطه وجعله أيقونة رسمية للزر!</b>\n(لإلغاء العملية أرسل: الغاء)"
    msg = bot.send_message(call.message.chat.id, msg_text, parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_save_custom_btn, key)

def ad_save_custom_btn(message, key):
    if message.text and message.text.strip() == "الغاء":
        bot.send_message(message.chat.id, "❌ تم إلغاء عملية التعديل.")
        return
        
    if not message.text:
        bot.send_message(message.chat.id, "❌ يجب إرسال نص للزر.")
        return
        
    bot.send_message(message.chat.id, "⏳ جاري الحفظ والترجمة التلقائية للزر...")
    
    text_ar, emoji_id = parse_button_input(message)
    
    if not text_ar or not text_ar.strip():
         current_text, _ = get_btn_data(message.from_user.id, key)
         text_ar = clean_old_emojis(current_text)
         if not text_ar or not text_ar.strip(): 
             text_ar = clean_old_emojis(DEFAULT_BUTTONS['ar'].get(key, key))
    
    text_en = safe_translate_for_cms(text_ar, 'en')
    
    if text_en == text_ar and text_ar.strip():
        try:
            simple = GoogleTranslator(source='ar', target='en').translate(text_ar)
            if simple and simple != text_ar:
                text_en = simple
        except:
            text_en = text_ar 
            
    db.custom_buttons.update_one({'lang': 'ar', 'key': key}, {'$set': {'text': text_ar, 'emoji_id': emoji_id}}, upsert=True)
    db.custom_buttons.update_one({'lang': 'en', 'key': key}, {'$set': {'text': text_en, 'emoji_id': emoji_id}}, upsert=True)
    
    emoji_status = f"<code>{emoji_id}</code>" if emoji_id else "لا يوجد"
    bot.send_message(message.chat.id, f"✅ <b>تم الحفظ! وتم تنظيف الرموز القديمة.</b>\n\n🇸🇦 العربية: <b>{html.escape(text_ar)}</b>\n🇺🇸 الإنجليزية: <b>{html.escape(text_en)}</b>\n🌟 الأيدي للرمز: {emoji_status}", parse_mode="HTML")

# ----------- تعيين أيقونة لمنتج -----------
@bot.callback_query_handler(func=lambda call: call.data == "ad_prod_emoji_start")
def ad_prod_emoji_start(call):
    bot.answer_callback_query(call.id) 
    l = get_lang(call.from_user.id)
    prods = list(db.products.find())
    
    # ترتيب أبجدي
    def sort_key(x):
        if l == 'en':
            return str(x.get('name_en', x.get('name_ar', ''))).lower()
        return str(x.get('name_ar', x.get('name_en', ''))).lower()
    prods.sort(key=sort_key)
    
    markup = InlineKeyboardMarkup(row_width=1)
    
    for p in prods: 
        p_name = p.get('name_en') if l == 'en' else p.get('name_ar')
        if not p_name:
            p_name = p.get('name_ar') or p.get('name_en') or 'بدون اسم'
        p_id = p.get('id', str(p.get('_id', '')))
        
        # علامة لو الإيموجي مُعيّن
        has_emoji_mark = " ✅" if p.get('custom_emoji_id') else " ⚪"
        btn_text = f"📦 {clean_name(p_name)}{has_emoji_mark}"
        btn_kwargs = {'text': btn_text, 'callback_data': f"set_pemj_{p_id}"}
        
        # عرض الإيموجي الحالي على الزر (لو موجود)
        custom_emoji_id = p.get('custom_emoji_id')
        if custom_emoji_id:
            btn_kwargs['icon_custom_emoji_id'] = custom_emoji_id
        
        markup.add(CustomInlineButton(**btn_kwargs))
        
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    bot.edit_message_text("👇 <b>اختر المنتج الذي تريد تعيين أيقونة له:</b>\n\n✅ = يحتوي على أيقونة | ⚪ = بدون أيقونة", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("set_pemj_"))
def ad_prod_emoji_ask(call):
    bot.answer_callback_query(call.id) 
    pid = call.data.replace("set_pemj_", "")
    msg = bot.send_message(call.message.chat.id, "🌟 <b>أرسل الآن الإيموجي المميز (Premium Emoji) لهذا المنتج:</b>\n(أرسله كرسالة عادية وسأقوم بالتقاطه)", parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_prod_emoji_save, pid)

def ad_prod_emoji_save(message, pid):
    if not message.text:
        bot.send_message(message.chat.id, "❌ الرجاء إرسال إيموجي.")
        return
    
    # دعم الإلغاء
    if message.text.strip().lower() in ['الغاء', 'cancel', '/cancel']:
        bot.send_message(message.chat.id, "❌ تم إلغاء العملية.")
        return
        
    emoji_id = None
    if message.entities:
        for ent in message.entities:
            if ent.type == 'custom_emoji':
                emoji_id = ent.custom_emoji_id
                break
                
    if not emoji_id: 
        bot.send_message(message.chat.id, "❌ <b>لم يتم العثور على رمز Premium</b>\n(تأكد أنك تستخدم إيموجي مخصص من تيليجرام Premium وليس إيموجي عادي).", parse_mode="HTML")
        return
        
    p = find_product(pid)
    if not p:
        bot.send_message(message.chat.id, "❌ عذراً، لم يتم العثور على المنتج في قاعدة البيانات.")
        return
    
    # 🆕 جلب الإيموجي القديم (لعرضه في رسالة التأكيد)
    old_emoji_id = p.get('custom_emoji_id')
    
    # 🆕 الحذف ثم الإدراج (atomic) - يضمن إن القديم ينحذف بشكل نظيف قبل الجديد
    # 1) أولاً نحذف الحقل القديم بالكامل
    db.products.update_one(
        {'_id': p['_id']}, 
        {'$unset': {'custom_emoji_id': ''}}
    )
    
    # 2) ثم نضيف الإيموجي الجديد
    db.products.update_one(
        {'_id': p['_id']}, 
        {'$set': {'custom_emoji_id': emoji_id}}
    )
    
    p_name = p.get('name_ar') or p.get('name_en') or 'المنتج'
    
    # رسالة تأكيد مع عرض الإيموجي الجديد فعلياً
    if old_emoji_id:
        confirm_msg = (
            f"✅ <b>تم تحديث الأيقونة بنجاح!</b>\n\n"
            f"📦 المنتج: <b>{clean_name(p_name)}</b>\n\n"
            f"🆕 الأيقونة الجديدة: <tg-emoji emoji-id=\"{emoji_id}\">✨</tg-emoji>\n"
            f"🗑 تم حذف الأيقونة القديمة تلقائياً\n\n"
            f"<i>ستظهر الأيقونة الجديدة في كل القوائم والإشعارات.</i>"
        )
    else:
        confirm_msg = (
            f"✅ <b>تم تعيين الأيقونة بنجاح!</b>\n\n"
            f"📦 المنتج: <b>{clean_name(p_name)}</b>\n\n"
            f"🆕 الأيقونة: <tg-emoji emoji-id=\"{emoji_id}\">✨</tg-emoji>\n\n"
            f"<i>ستظهر الآن في كل القوائم والإشعارات.</i>"
        )
    
    try:
        bot.send_message(message.chat.id, confirm_msg, parse_mode="HTML")
    except Exception:
        # لو فشل عرض الإيموجي (Premium مش متاح)، نرسل رسالة بسيطة
        bot.send_message(message.chat.id, f"✅ تم تحديث الأيقونة لمنتج [{p_name}]!")

@bot.callback_query_handler(func=lambda call: call.data == "ad_api_main")
def admin_api_main(call):
    bot.answer_callback_query(call.id)
    gh_price = float(get_setting("github_price", 15.0))
    gem_price = float(get_setting("gemini_price", 5.0))
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("💳 فحص رصيد API (AhsanLabs)", callback_data="ad_gh_credits"))
    markup.add(InlineKeyboardButton(f"💰 تعديل سعر GitHub (الحالي: ${gh_price:.2f})", callback_data="ad_gh_price"))
    markup.add(InlineKeyboardButton(f"💰 تعديل سعر Gemini (الحالي: ${gem_price:.2f})", callback_data="ad_gem_price"))
    markup.add(InlineKeyboardButton("⚙️ إعداد جلسة اليوزربوت (Session)", callback_data="ad_set_session"))
    markup.add(InlineKeyboardButton("🤖 إعداد يوزر بوت المزود", callback_data="ad_set_provider"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    bot.edit_message_text("⚙️ <b>إعدادات التفعيلات التلقائية:</b>\nمن هنا تتحكم بالأسعار وتفاصيل الربط مع اليوزربوت.", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data in ["ad_set_session", "ad_set_provider"])
def admin_set_userbot_vars(call):
    bot.answer_callback_query(call.id)
    key = 'userbot_session' if call.data == "ad_set_session" else 'provider_bot'
    msg = bot.send_message(call.message.chat.id, f"📝 <b>أرسل القيمة الجديدة لـ ({key}):</b>", parse_mode="HTML")
    
    def save_and_restart(message):
        db.settings.update_one({'key': key}, {'$set': {'value': message.text.strip()}}, upsert=True)
        bot.send_message(message.chat.id, "✅ تم حفظ الإعداد بنجاح. جاري إعادة تشغيل اليوزربوت في الخلفية...")
        start_dynamic_userbot()
        
    bot.register_next_step_handler(msg, save_and_restart)

@bot.callback_query_handler(func=lambda call: call.data == "ad_gh_credits")
def admin_github_credits(call):
    bot.answer_callback_query(call.id, "⏳ جاري الاتصال بـ AhsanLabs...")
    try:
        if not GITHUB_API_KEY:
            bot.send_message(call.message.chat.id, "❌ لم يتم العثور على GITHUB_API_KEY في ملف .env")
            return
            
        headers = {
            "X-API-Key": GITHUB_API_KEY,
            "User-Agent": "Mozilla/5.0"
        }
        
        api_url = f"{GITHUB_BASE_URL}/api/me" 
        
        res = requests.get(api_url, headers=headers, timeout=30)
        
        if res.status_code == 200:
            try:
                data = res.json()
                bal = data.get("credits", "Unknown") 
                api_cost = data.get("api_cost", 1)
                bot.send_message(call.message.chat.id, f"💳 <b>معلومات حسابك في AhsanLabs:</b>\n💰 الرصيد المتوفر: <b>{bal}</b> Credits\n⚡ تكلفة التفعيل: <b>{api_cost}</b> Credit", parse_mode="HTML")
            except Exception as e:
                bot.send_message(call.message.chat.id, "❌ فشل تحليل الرد من السيرفر.")
                logger.error(f"Parse error: {e}")
        else:
            bot.send_message(call.message.chat.id, f"❌ <b>فشل الاتصال.</b>\nالرابط: <code>{api_url}</code>\nكود الخطأ: {res.status_code}\n<i>تأكد من صحة الـ API Key.</i>", parse_mode="HTML")
            logger.error(f"API Failed: Status {res.status_code} | Response: {res.text}")
            
    except Exception as e:
        err_str = html.escape(str(e))
        bot.send_message(call.message.chat.id, f"❌ خطأ داخلي: <code>{err_str}</code>", parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data in ["ad_gh_price", "ad_gem_price"])
def admin_set_price(call):
    bot.answer_callback_query(call.id)
    key = 'github_price' if call.data == "ad_gh_price" else 'gemini_price'
    msg = bot.send_message(call.message.chat.id, "💰 <b>أرسل السعر الجديد بالدولار ($):</b>", parse_mode="HTML")
    
    def save_price(message):
        try:
            new_price = float(message.text.strip())
            db.settings.update_one({'key': key}, {'$set': {'value': new_price}}, upsert=True)
            bot.send_message(message.chat.id, f"✅ تم تحديث السعر بنجاح إلى <b>${new_price:.2f}</b>.", parse_mode="HTML")
            
            if key == 'gemini_price':
                def broadcast_gemini(admin_id):
                    p_name_ar = "✨ Gemini Advanced"
                    p_name_en = "✨ Gemini Advanced"
                    
                    users = list(db.users.find())
                    success = 0
                    fail = 0
                    for u in users:
                        try:
                            u_lang = u.get('lang', 'ar') if u.get('lang_chosen') else 'en'
                            if u_lang not in ['ar', 'en']: u_lang = 'en'
                            
                            p_name = p_name_ar if u_lang == 'ar' else p_name_en
                            alert_msg = get_text(u['user_id'], 'price_drop', p_name, "السعر السابق", new_price)
                            bot.send_message(u['user_id'], alert_msg, parse_mode="HTML")
                            success += 1
                            time.sleep(0.05)
                        except: 
                            fail += 1
                            continue
                    bot.send_message(admin_id, f"📢 <b>تقرير إشعار التخفيض (Gemini):</b>\n🟢 نجاح: {success}\n🔴 فشل: {fail}", parse_mode="HTML")
                
                threading.Thread(target=broadcast_gemini, args=(message.chat.id,), daemon=True).start()
                bot.send_message(message.chat.id, "📢 تم بدء إرسال إشعار التخفيض لجميع الأعضاء وسيصلك التقرير قريباً.")
        except Exception as e:
            bot.send_message(message.chat.id, "❌ خطأ في إدخال الرقم.")
            
    bot.register_next_step_handler(msg, save_price)

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_add")
def ad_p_step1(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.from_user.id, "📦 أرسل اسم المنتج (بالعربية فقط):")
    bot.register_next_step_handler(msg, ad_p_step2)

def ad_p_step2(message):
    uid = message.from_user.id
    # 🆕 نستخرج الـ Premium Emojis ونحولها لـ HTML
    n_ar = extract_custom_emojis_to_html(message)
    
    # 🆕 نستخدم safe_translate_for_cms اللي يحمي التنسيقات والإيموجيات
    n_en = safe_translate_for_cms(n_ar, 'en')
    
    temp_product[uid] = {'n_ar': n_ar, 'n_en': n_en}
    # نعرض المعاينة بدون escape عشان يبان الإيموجي
    bot.send_message(uid, f"✅ تم حفظ الاسم!\n\n🇸🇦 العربي: {n_ar}\n🇺🇸 الإنجليزي: {n_en}", parse_mode="HTML")
    msg = bot.send_message(uid, "📝 أرسل وصف المنتج (بالعربية):\n💡 <i>يمكنك استخدام Premium Emojis، تنسيقات (Bold، Italic)، وأي رموز تعبيرية</i>", parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_p_step3)

def ad_p_step3(message):
    uid = message.from_user.id
    # 🆕 نستخرج الـ Premium Emojis ونحولها لـ HTML
    d_ar = extract_custom_emojis_to_html(message)
    
    # 🆕 نستخدم safe_translate_for_cms للحفاظ على التنسيقات
    d_en = safe_translate_for_cms(d_ar, 'en')
    
    # تحقق إن الترجمة نجحت فعلاً (مو نفس النص العربي)
    is_translated = (d_en != d_ar)
    
    temp_product[uid].update({'d_ar': d_ar, 'd_en': d_en})
    
    # 🆕 عرض معاينة كاملة بالنسختين
    preview_msg = (
        f"✅ <b>تم حفظ الوصف!</b>\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🇸🇦 <b>العربي:</b>\n{d_ar}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🇺🇸 <b>الإنجليزي:</b>\n{d_en}\n"
        f"━━━━━━━━━━━━━━"
    )
    
    if not is_translated:
        preview_msg += "\n\n⚠️ <i>ملاحظة: الترجمة فشلت في الحفاظ على بعض التنسيقات. تم حفظ النسخة العربية في الإنجليزي.</i>"
    
    try:
        bot.send_message(uid, preview_msg, parse_mode="HTML")
    except Exception as preview_err:
        # لو الـ HTML معطّل بسبب الترجمة، نرسل بدون parse_mode
        logger.warning(f"Preview HTML error: {preview_err}")
        bot.send_message(uid, "✅ تم حفظ الوصف (المعاينة تعذرت بسبب HTML).")
    
    msg = bot.send_message(uid, "💰 أرسل السعر بالدولار ($):")
    bot.register_next_step_handler(msg, ad_p_price)

def ad_p_price(message):
    uid = message.from_user.id
    try:
        price = float(message.text)
        temp_product[uid]['price'] = price
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("⚡ تسليم تلقائي (أكواد وبطاقات)", callback_data="ad_ptype_auto"))
        markup.add(InlineKeyboardButton("🤝 تسليم يدوي (يتواصل العميل معك)", callback_data="ad_ptype_manual"))
        bot.send_message(uid, "⚙️ <b>اختر نوع تسليم هذا المنتج:</b>", reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        bot.send_message(uid, "❌ خطأ في السعر. الرجاء المحاولة مرة أخرى من القائمة.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_ptype_"))
def ad_p_final(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    is_manual = True if call.data == "ad_ptype_manual" else False
    p = temp_product.get(uid)
    if not p: return
    
    pid = str(int(time.time()))
    db.products.insert_one({
        'id': pid, 'name_ar': p['n_ar'], 'name_en': p['n_en'], 
        'desc_ar': p['d_ar'], 'desc_en': p['d_en'], 
        'price': p['price'], 'is_manual': is_manual, 'is_hidden': False
    })
    
    type_txt = "التسليم اليدوي 🤝" if is_manual else "التسليم التلقائي ⚡"
    bot.edit_message_text(f"✅ <b>تم إضافة المنتج بنجاح بنظام ({type_txt})!</b>", call.message.chat.id, call.message.message_id, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_edit")
def admin_edit_list(call):
    bot.answer_callback_query(call.id)
    l = get_lang(call.from_user.id)
    prods = list(db.products.find())
    
    # ترتيب أبجدي حسب لغة الأدمن
    def sort_key(x):
        if l == 'en':
            return str(x.get('name_en', x.get('name_ar', ''))).lower()
        return str(x.get('name_ar', x.get('name_en', ''))).lower()
    prods.sort(key=sort_key)
    
    markup = InlineKeyboardMarkup(row_width=1)
    for p in prods:
        pid = p.get('id', str(p.get('_id', '')))
        hidden_icon = " 👻" if p.get('is_hidden', False) else ""
        
        # عرض الاسم حسب لغة الأدمن
        p_name = p.get('name_en') if l == 'en' else p.get('name_ar')
        if not p_name:
            p_name = p.get('name_ar') or p.get('name_en') or 'بدون اسم'
        
        btn_text = f"📝 {clean_name(p_name)}{hidden_icon}"
        btn_kwargs = {'text': btn_text, 'callback_data': f"edit_p_{pid}"}
        
        # إضافة الإيموجي المميز (Premium Emoji) للزر
        custom_emoji_id = p.get('custom_emoji_id')
        if custom_emoji_id:
            btn_kwargs['icon_custom_emoji_id'] = custom_emoji_id
        
        markup.add(CustomInlineButton(**btn_kwargs))
    
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    bot.edit_message_text("👇 Select Product:", call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_p_"))
def admin_edit_opts(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("edit_p_", "")
    p = find_product(pid)
    if not p: return
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("💵 Price", callback_data=f"ep_price_{pid}"))
    markup.add(InlineKeyboardButton("📝 Desc (AR)", callback_data=f"ep_dar_{pid}"),
               InlineKeyboardButton("📝 Desc (EN)", callback_data=f"ep_den_{pid}"))
    markup.add(InlineKeyboardButton("✏️ Name (AR)", callback_data=f"ep_nar_{pid}"),
               InlineKeyboardButton("✏️ Name (EN)", callback_data=f"ep_nen_{pid}"))
    
    hide_txt = "👁️ Show Product" if p.get('is_hidden', False) else "🙈 Hide Product"
    markup.add(InlineKeyboardButton(hide_txt, callback_data=f"toggle_hide_{pid}"))
    
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="ad_p_edit"))
    
    try: bot.edit_message_text("⚙️ Options:", call.message.chat.id, call.message.message_id, reply_markup=markup)
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("toggle_hide_"))
def admin_toggle_hide(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("toggle_hide_", "")
    p = find_product(pid)
    if p:
        new_status = not p.get('is_hidden', False)
        db.products.update_one({'_id': p['_id']}, {'$set': {'is_hidden': new_status}})
        bot.answer_callback_query(call.id, "✅ Visibility updated!", show_alert=True)
        call.data = f"edit_p_{pid}"
        admin_edit_opts(call)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ep_"))
def admin_edit_prompt(call):
    bot.answer_callback_query(call.id)
    parts = call.data.split('_', 2)
    field = parts[1]; pid = parts[2]
    
    # جلب المنتج لعرض القيمة القديمة
    p = find_product(pid)
    if not p:
        bot.send_message(call.message.chat.id, "❌ المنتج غير موجود.")
        return
    
    # اسم المنتج للعرض (مع الإيموجي المميز إن وجد)
    p_name_raw = p.get('name_ar') or p.get('name_en') or 'بدون اسم'
    custom_emoji_id = p.get('custom_emoji_id')
    if custom_emoji_id:
        p_display_name = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> {clean_name(p_name_raw)}'
    else:
        p_display_name = f'📦 {clean_name(p_name_raw)}'
    
    # تحديد القيمة القديمة + التسمية حسب النوع
    if field == "price":
        current_price = float(p.get('price', 0))
        prompt_msg = (
            f"━━━━━━━━━━━━━━━\n"
            f"💵 <b>تعديل سعر المنتج</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"{p_display_name}\n\n"
            f"💰 <b>السعر الحالي:</b> <code>${current_price:.2f}</code>\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👇 <b>أرسل السعر الجديد بالدولار:</b>\n"
            f"<i>(مثال: 5.99 أو 10)</i>\n\n"
            f"💡 <i>أرسل <b>الغاء</b> للإلغاء.</i>"
        )
    elif field == "dar":
        old_desc = clean_name(p.get('desc_ar', '-'))
        prompt_msg = (
            f"━━━━━━━━━━━━━━━\n"
            f"📝 <b>تعديل الوصف العربي</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"{p_display_name}\n\n"
            f"📌 <b>الوصف الحالي:</b>\n<blockquote>{html.escape(str(old_desc))}</blockquote>\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👇 <b>أرسل الوصف الجديد:</b>\n"
            f"<i>(يدعم Premium Emojis - سيُترجم تلقائياً للإنجليزية)</i>\n\n"
            f"💡 <i>أرسل <b>الغاء</b> للإلغاء.</i>"
        )
    elif field == "den":
        old_desc = clean_name(p.get('desc_en', '-'))
        prompt_msg = (
            f"━━━━━━━━━━━━━━━\n"
            f"📝 <b>Edit English Description</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"{p_display_name}\n\n"
            f"📌 <b>Current Description:</b>\n<blockquote>{html.escape(str(old_desc))}</blockquote>\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👇 <b>Send new description:</b>\n"
            f"<i>(Supports Premium Emojis)</i>\n\n"
            f"💡 <i>Send <b>cancel</b> to abort.</i>"
        )
    elif field == "nar":
        old_name = clean_name(p.get('name_ar', '-'))
        prompt_msg = (
            f"━━━━━━━━━━━━━━━\n"
            f"✏️ <b>تعديل الاسم العربي</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"{p_display_name}\n\n"
            f"📌 <b>الاسم الحالي:</b> <code>{html.escape(str(old_name))}</code>\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👇 <b>أرسل الاسم الجديد:</b>\n"
            f"<i>(يدعم Premium Emojis - سيُترجم تلقائياً للإنجليزية)</i>\n\n"
            f"💡 <i>أرسل <b>الغاء</b> للإلغاء.</i>"
        )
    elif field == "nen":
        old_name = clean_name(p.get('name_en', '-'))
        prompt_msg = (
            f"━━━━━━━━━━━━━━━\n"
            f"✏️ <b>Edit English Name</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"{p_display_name}\n\n"
            f"📌 <b>Current Name:</b> <code>{html.escape(str(old_name))}</code>\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👇 <b>Send new name:</b>\n"
            f"<i>(Supports Premium Emojis)</i>\n\n"
            f"💡 <i>Send <b>cancel</b> to abort.</i>"
        )
    else:
        prompt_msg = "👇 أرسل القيمة الجديدة:"
    
    msg = bot.send_message(call.message.chat.id, prompt_msg, parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_save_edit, field, pid)

# ----------- حفظ تعديل المنتج + إشعار تخفيض السعر -----------
def admin_save_edit(message, field, pid):
    val = message.text or ""
    
    # دعم الإلغاء
    if val.strip().lower() in ['الغاء', 'cancel', '/cancel']:
        bot.send_message(message.chat.id, "❌ تم إلغاء التعديل.")
        return
    
    keys = {"price": "price", "dar": "desc_ar", "den": "desc_en", "nar": "name_ar", "nen": "name_en"}
    p = find_product(pid)
    if not p: 
        bot.send_message(message.chat.id, "❌ المنتج لم يعد موجوداً.")
        return

    if field == "price":
        try:
            new_price = float(val)
            if new_price < 0:
                bot.send_message(message.chat.id, "❌ السعر لا يمكن أن يكون سالباً.")
                return
            old_price = float(p.get('price', 0))
            db.products.update_one({'_id': p['_id']}, {'$set': {'price': new_price}})
            
            # رسالة توضح التغيير
            if new_price < old_price:
                change_emoji = "📉"
                change_txt = f"تم تخفيض السعر من <b>${old_price:.2f}</b> إلى <b>${new_price:.2f}</b>"
            elif new_price > old_price:
                change_emoji = "📈"
                change_txt = f"تم رفع السعر من <b>${old_price:.2f}</b> إلى <b>${new_price:.2f}</b>"
            else:
                change_emoji = "✅"
                change_txt = f"السعر بقي نفسه: <b>${new_price:.2f}</b>"
            
            bot.send_message(message.chat.id, f"{change_emoji} <b>تم التحديث!</b>\n{change_txt}", parse_mode="HTML")
            
            # برودكاست فقط لو السعر نزل
            if new_price < old_price:
                def broadcast_price_drop(admin_id):
                    users = list(db.users.find())
                    success = 0
                    fail = 0
                    for u in users:
                        try:
                            u_lang = u.get('lang', 'ar') if u.get('lang_chosen') else 'en'
                            if u_lang not in ['ar', 'en']: u_lang = 'en'
                            
                            custom_emoji_id = p.get('custom_emoji_id')
                            icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> ' if custom_emoji_id else '📦 '
                            p_name = icon_html + clean_name(p.get(f'name_{u_lang}', p.get('name_en')))
                            
                            alert_msg = get_text(u['user_id'], 'price_drop', p_name, old_price, new_price)
                            bot.send_message(u['user_id'], alert_msg, parse_mode="HTML")
                            success += 1
                            time.sleep(0.05)
                        except: 
                            fail += 1
                            continue
                    bot.send_message(admin_id, f"📢 <b>تقرير إشعار التخفيض للمنتج ({p.get('name_ar')}):</b>\n🟢 مستلمين: {success}\n🔴 محظورين: {fail}", parse_mode="HTML")
                
                threading.Thread(target=broadcast_price_drop, args=(message.chat.id,), daemon=True).start()
                bot.send_message(message.chat.id, "📢 تم بدء إرسال إشعار التخفيض للجميع وسيصلك تقرير قريباً.")
        except Exception as e: 
            bot.send_message(message.chat.id, f"❌ خطأ في السعر. تأكد من إرسال رقم صحيح (مثال: 5.99)")
    else:
        # للأسماء والأوصاف — دعم Premium Emojis + ترجمة تلقائية
        final_text = extract_custom_emojis_to_html(message)
        
        if field in ['nar', 'dar']:
            # عربي → ترجم للإنجليزي تلقائياً
            translated = safe_translate_for_cms(final_text, 'en')
            if field == 'nar':
                db.products.update_one({'_id': p['_id']}, {'$set': {'name_ar': final_text, 'name_en': translated}})
                bot.send_message(message.chat.id, f"✅ <b>تم تحديث الاسم!</b>\n\n🇸🇦 العربي: {final_text}\n🇬🇧 الإنجليزي (مترجم تلقائياً): {translated}", parse_mode="HTML")
            else:
                db.products.update_one({'_id': p['_id']}, {'$set': {'desc_ar': final_text, 'desc_en': translated}})
                bot.send_message(message.chat.id, "✅ <b>تم تحديث الوصف العربي + ترجمته للإنجليزي تلقائياً.</b>", parse_mode="HTML")
        else:
            # إنجليزي فقط
            db.products.update_one({'_id': p['_id']}, {'$set': {keys[field]: final_text}})
            bot.send_message(message.chat.id, "✅ <b>تم التحديث بنجاح.</b>", parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_p_del")
def admin_del_list(call):
    bot.answer_callback_query(call.id)
    l = get_lang(call.from_user.id)
    prods = list(db.products.find())
    
    # ترتيب أبجدي
    def sort_key(x):
        if l == 'en':
            return str(x.get('name_en', x.get('name_ar', ''))).lower()
        return str(x.get('name_ar', x.get('name_en', ''))).lower()
    prods.sort(key=sort_key)
    
    markup = InlineKeyboardMarkup(row_width=1)
    for p in prods:
        pid = p.get('id', str(p.get('_id', '')))
        
        p_name = p.get('name_en') if l == 'en' else p.get('name_ar')
        if not p_name:
            p_name = p.get('name_ar') or p.get('name_en') or 'بدون اسم'
        
        btn_text = f"🗑 {clean_name(p_name)}"
        btn_kwargs = {'text': btn_text, 'callback_data': f"del_p_{pid}"}
        
        # إيموجي مميز
        custom_emoji_id = p.get('custom_emoji_id')
        if custom_emoji_id:
            btn_kwargs['icon_custom_emoji_id'] = custom_emoji_id
        
        markup.add(CustomInlineButton(**btn_kwargs))
    
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    bot.edit_message_text("👇 Select Product to Delete:", call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_p_"))
def admin_del_exec(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("del_p_", "")
    try:
        p = find_product(pid)
        if not p: return
        pid_str = str(pid)
        queries = [{'product_id': pid_str}]
        if pid_str.isdigit(): queries.append({'product_id': int(pid_str)})
        try: queries.append({'product_id': float(pid_str)})
        except: pass
        db.product_stock.delete_many({'$or': queries})
        db.orders.delete_many({'$or': queries})
        db.products.delete_one({'_id': p['_id']})
        bot.answer_callback_query(call.id, "✅ Deleted Successfully!", show_alert=True)
        admin_main_ui(call)
    except: bot.answer_callback_query(call.id, "❌ Error", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == "ad_s_list")
def admin_stock_list_ui(call):
    bot.answer_callback_query(call.id)
    l = get_lang(call.from_user.id)
    prods = list(db.products.find({'is_manual': {'$ne': True}}))
    
    # ترتيب أبجدي
    def sort_key(x):
        if l == 'en':
            return str(x.get('name_en', x.get('name_ar', ''))).lower()
        return str(x.get('name_ar', x.get('name_en', ''))).lower()
    prods.sort(key=sort_key)
    
    markup = InlineKeyboardMarkup(row_width=1)
    if not prods: 
        bot.edit_message_text("📭 لا توجد منتجات تلقائية حالياً.", call.message.chat.id, call.message.message_id, parse_mode="HTML")
        return
    
    for p in prods:
        pid = p.get('id', str(p.get('_id', '')))
        stk_count = get_product_stock_count(pid)
        
        p_name = p.get('name_en') if l == 'en' else p.get('name_ar')
        if not p_name:
            p_name = p.get('name_ar') or p.get('name_en') or 'بدون اسم'
        
        btn_text = f"📦 {clean_name(p_name)} ({stk_count})"
        btn_kwargs = {'text': btn_text, 'callback_data': f"ad_s_opts_{pid}"}
        
        # إيموجي مميز
        custom_emoji_id = p.get('custom_emoji_id')
        if custom_emoji_id:
            btn_kwargs['icon_custom_emoji_id'] = custom_emoji_id
        
        markup.add(CustomInlineButton(**btn_kwargs))
    
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    bot.edit_message_text("📦 <b>اختر المنتج لإدارة الستوك الخاص به:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_s_opts_"))
def admin_stock_opts_ui(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("ad_s_opts_", "")
    p = find_product(pid)
    if not p: return
    stk_count = get_product_stock_count(pid)
    text = f"⚙️ <b>إدارة ستوك:</b> {clean_name(p.get('name_ar'))}\n📊 <b>المتوفر حالياً:</b> {stk_count} كود"
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("➕ إضافة أكواد", callback_data=f"stk_add_{pid}"))
    markup.add(InlineKeyboardButton("👁️ عرض الأكواد (ملف txt)", callback_data=f"stk_view_{pid}"))
    markup.add(InlineKeyboardButton("✏️ تعديل كود", callback_data=f"stk_edit_{pid}"),
               InlineKeyboardButton("🗑️ حذف كود", callback_data=f"stk_delcode_{pid}"))
    markup.add(InlineKeyboardButton("🧨 مسح كل الستوك", callback_data=f"stk_clear_{pid}"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="ad_s_list"))
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_add_"))
def admin_stock_input(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("stk_add_", "")
    msg = bot.send_message(call.from_user.id, "📥 <b>أرسل الأكواد (كود في كل سطر) أو قم برفع ملف (.txt) يحتوي على الأكواد:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_stock_save, pid)

# ----------- إشعار إضافة أكواد (ستوك) بتقرير -----------
def admin_stock_save(message, pid):
    lines = []
    if message.document:
        try:
            file_info = bot.get_file(message.document.file_id)
            downloaded_file = bot.download_file(file_info.file_path)
            content = downloaded_file.decode('utf-8')
            lines = content.split('\n')
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ حدث خطأ في قراءة الملف: {e}")
            return
    elif message.text:
        lines = message.text.split('\n')
    else:
        bot.send_message(message.chat.id, "❌ الرجاء إرسال نص أو ملف (.txt) فقط.")
        return

    count = 0
    for l in lines:
        if l.strip():
            db.product_stock.insert_one({'product_id': str(pid), 'code_line': l.strip(), 'is_sold': False})
            count += 1
    
    # عدد المستخدمين الكلي - للتأكد من إن البرودكاست بيشتغل
    total_users = db.users.count_documents({})
    logger.info(f"📢 بدء البرودكاست للستوك الجديد - منتج {pid} - عدد المستخدمين: {total_users}")
    
    bot.send_message(
        message.chat.id, 
        f"✅ <b>تم إضافة {count} كود بنجاح!</b>\n"
        f"⏳ <i>جاري إرسال الإشعارات لـ {total_users} مستخدم في الخلفية...</i>\n"
        f"📊 <i>الإرسال يستغرق تقريباً {round(total_users * 0.05)} ثانية - سيصلك تقرير عند الانتهاء</i>", 
        parse_mode="HTML"
    )

    def broadcast_new_stock(pid_for_thread, admin_id):
        try:
            logger.info(f"📢 Thread بدأ - admin_id={admin_id}, pid={pid_for_thread}")
            p = find_product(pid_for_thread)
            if not p: 
                logger.error(f"❌ المنتج غير موجود في broadcast_new_stock: {pid_for_thread}")
                bot.send_message(admin_id, "❌ فشل البرودكاست: المنتج غير موجود.")
                return
            stk_total = get_product_stock_count(pid_for_thread)
            users = list(db.users.find())
            logger.info(f"📢 جاري الإرسال لـ {len(users)} مستخدم")
            success = 0
            fail = 0
            for u in users:
                try: 
                    u_lang = u.get('lang', 'ar') if u.get('lang_chosen') else 'en'
                    if u_lang not in ['ar', 'en']: u_lang = 'en'
                    
                    custom_emoji_id = p.get('custom_emoji_id')
                    icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> ' if custom_emoji_id else '📦 '
                    p_name = icon_html + clean_name(p.get(f'name_{u_lang}', p.get('name_en')))
                    
                    alert_msg = get_text(u['user_id'], 'new_stock', p_name, stk_total)
                    bot.send_message(u['user_id'], alert_msg, parse_mode="HTML")
                    success += 1
                    time.sleep(0.05) 
                except Exception as send_err: 
                    fail += 1
                    logger.debug(f"فشل الإرسال للمستخدم {u.get('user_id')}: {send_err}")
                    pass
            logger.info(f"📢 انتهى البرودكاست - نجح: {success} | فشل: {fail}")
            bot.send_message(
                admin_id, 
                f"📢 <b>تقرير إشعار المخزون للمنتج ({p.get('name_ar')}):</b>\n"
                f"🟢 مستلمين: {success}\n"
                f"🔴 محظورين: {fail}\n"
                f"👥 المجموع: {success + fail}", 
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"❌ Thread Error في broadcast_new_stock: {e}")
            try:
                bot.send_message(admin_id, f"❌ فشل البرودكاست: {e}")
            except: pass

    threading.Thread(target=broadcast_new_stock, args=(pid, message.chat.id), daemon=True).start()

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_view_"))
def admin_stock_view(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("stk_view_", "")
    pid_str = str(pid)
    queries = [{'product_id': pid_str}]
    if pid_str.isdigit(): queries.append({'product_id': int(pid_str)})
    try: queries.append({'product_id': float(pid_str)})
    except: pass
    items = list(db.product_stock.find({'$or': queries, 'is_sold': False}))
    if not items:
        bot.answer_callback_query(call.id, "📭 الستوك فارغ لهذا المنتج!", show_alert=True)
        return
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز ملف الأكواد...")
    content = "\n".join([item['code_line'] for item in items])
    f = io.BytesIO(content.encode('utf-8'))
    f.name = f"Stock_{pid}.txt"
    bot.send_document(call.message.chat.id, f, caption=f"📦 الأكواد المتوفرة حالياً ({len(items)} كود)")

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_delcode_"))
def admin_stock_delcode_prompt(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("stk_delcode_", "")
    msg = bot.send_message(call.message.chat.id, "🗑️ <b>أرسل الكود بالضبط كما هو مكتوب لحذفه من الستوك:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_stock_delcode_exec, pid)

def admin_stock_delcode_exec(message, pid):
    code_to_del = message.text.strip()
    pid_str = str(pid)
    queries = [{'product_id': pid_str}]
    if pid_str.isdigit(): queries.append({'product_id': int(pid_str)})
    try: queries.append({'product_id': float(pid_str)})
    except: pass
    res = db.product_stock.delete_one({'$or': queries, 'code_line': code_to_del, 'is_sold': False})
    if res.deleted_count > 0:
        bot.send_message(message.chat.id, "✅ <b>تم حذف الكود بنجاح من الستوك!</b>", parse_mode="HTML")
    else:
        bot.send_message(message.chat.id, "❌ <b>لم يتم العثور على هذا الكود في الستوك.</b>", parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_clear_"))
def admin_stock_clear_exec(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("stk_clear_", "")
    pid_str = str(pid)
    queries = [{'product_id': pid_str}]
    if pid_str.isdigit(): queries.append({'product_id': int(pid_str)})
    try: queries.append({'product_id': float(pid_str)})
    except: pass
    res = db.product_stock.delete_many({'$or': queries, 'is_sold': False})
    bot.answer_callback_query(call.id, f"🧨 تم مسح {res.deleted_count} كود من الستوك بنجاح!", show_alert=True)
    admin_stock_opts_ui(call)

@bot.callback_query_handler(func=lambda call: call.data.startswith("stk_edit_"))
def admin_stock_edit_step1(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("stk_edit_", "")
    msg = bot.send_message(call.message.chat.id, "✏️ <b>أرسل الكود القديم الذي تريد تعديله:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_stock_edit_step2, pid)

def admin_stock_edit_step2(message, pid):
    old_code = message.text.strip()
    pid_str = str(pid)
    queries = [{'product_id': pid_str}]
    if pid_str.isdigit(): queries.append({'product_id': int(pid_str)})
    try: queries.append({'product_id': float(pid_str)})
    except: pass
    item = db.product_stock.find_one({'$or': queries, 'code_line': old_code, 'is_sold': False})
    if not item:
        bot.send_message(message.chat.id, "❌ <b>لم يتم العثور على الكود القديم!</b>", parse_mode="HTML")
        return
    temp_stock_edit[message.from_user.id] = item['_id']
    msg = bot.send_message(message.chat.id, "✅ تم العثور على الكود.\n\n✨ <b>أرسل الكود الجديد الآن لتبديله:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_stock_edit_step3)

def admin_stock_edit_step3(message):
    new_code = message.text.strip()
    item_id = temp_stock_edit.get(message.from_user.id)
    if item_id:
        db.product_stock.update_one({'_id': item_id}, {'$set': {'code_line': new_code}})
        bot.send_message(message.chat.id, "✅ <b>تم تعديل الكود وحفظه بنجاح!</b>", parse_mode="HTML")
    else:
        bot.send_message(message.chat.id, "❌ حدث خطأ، يرجى المحاولة مرة أخرى.", parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_ban_user")
def ad_ban_start(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "🚫 <b>أرسل الأيدي (ID) أو معرف المستخدم (@username) للحظر أو فك الحظر:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_ban_exec)

def ad_ban_exec(message):
    target = message.text.strip()
    if target.startswith('@') or not target.replace('-', '').isdigit():
        u = db.users.find_one({'username': target.replace('@', '').lower()})
    else: u = get_user_data_full(int(target))
    if u:
        if u['user_id'] == OWNER_ID:
            bot.send_message(message.chat.id, "❌ لا يمكنك حظر المالك الأساسي!")
            return
        current_status = u.get('is_banned', 0)
        new_status = 1 if current_status == 0 else 0
        db.users.update_one({'user_id': u['user_id']}, {'$set': {'is_banned': new_status}})
        status_text = "تم حظر 🚫" if new_status == 1 else "تم فك حظر ✅"
        bot.send_message(message.chat.id, f"✅ {status_text} المستخدم (<code>{u['user_id']}</code>) بنجاح.", parse_mode="HTML")
    else:
        bot.send_message(message.chat.id, "❌ لم يتم العثور على المستخدم في قاعدة البيانات.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_users_main")
def ad_users_main_ui(call):
    bot.answer_callback_query(call.id)
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("🔍 بحث عن مستخدم", callback_data="ad_u_search"),
               InlineKeyboardButton("🏆 أعلى 10 أرصدة", callback_data="ad_u_top"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    try: bot.edit_message_text("👥 <b>إدارة العملاء والأرصدة:</b>\nاختر العملية المطلوبة:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_u_top")
def ad_u_top_ui(call):
    bot.answer_callback_query(call.id)
    top_users = list(db.users.find().sort('balance', -1).limit(10))
    markup = InlineKeyboardMarkup(row_width=1)
    for tu in top_users:
        uname_display = f"@{tu['username']}" if tu.get('username') else tu['name']
        btn_text = f"💰 ${tu.get('balance', 0):.2f} | 👤 {clean_name(uname_display)[:15]}"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"ad_u_det_{tu['user_id']}"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="ad_users_main"))
    try: bot.edit_message_text("🏆 <b>أعلى 10 مستخدمين رصيداً:</b>\nاضغط على أي مستخدم لفتح ملفه:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_u_search")
def ad_u_search_prompt(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "🔍 <b>أرسل الأيدي (ID) أو معرف المستخدم (@username):</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_u_search_exec)

def ad_u_search_exec(message):
    target = message.text.strip()
    u = None
    if target.startswith('@') or not target.replace('-', '').isdigit():
        u = db.users.find_one({'username': target.replace('@', '').lower()})
    else: u = get_user_data_full(int(target))
    if u: show_user_admin_profile(message.chat.id, u['user_id'])
    else: bot.send_message(message.chat.id, "❌ لم يتم العثور على المستخدم.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_u_det_"))
def ad_u_det_router(call):
    bot.answer_callback_query(call.id)
    target_uid = int(call.data.replace("ad_u_det_", ""))
    show_user_admin_profile(call.message.chat.id, target_uid, call.message.message_id)

def show_user_admin_profile(chat_id, target_uid, message_id=None):
    u = get_user_data_full(target_uid)
    if not u: return
    buy_count = db.orders.count_documents({'user_id': target_uid})
    d_res = list(db.used_transactions.find({'user_id': target_uid}))
    dep_total = sum([float(d.get('amount', 0)) for d in d_res])
    uname_str = f"@{u['username']}" if u.get('username') else "لا يوجد"
    ban_str = "محظور 🚫" if u.get('is_banned') == 1 else "نشط ✅"
    
    text = f"📂 <b>ملف العميل (نظرة الإدارة)</b>\n\n👤 الاسم: <b>{clean_name(u.get('name', 'بدون'))}</b>\n🔗 المعرف: {uname_str}\n🆔 الأيدي: <code>{target_uid}</code>\n🛡️ الحالة: <b>{ban_str}</b>\n\n💰 الرصيد الحالي: <b>${u.get('balance', 0):.2f}</b>\n✅ المشتريات: <b>{buy_count}</b>\n📦 إجمالي الإيداعات: <b>${dep_total:.2f}</b>"
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("🛍 إيصالات المشتريات", callback_data=f"ad_uh_buy_{target_uid}"),
               InlineKeyboardButton("💳 إيصالات الإيداع", callback_data=f"ad_uh_dep_{target_uid}"))
    markup.add(InlineKeyboardButton("📄 تحميل سجل المشتريات (ملف)", callback_data=f"ad_dlbuy_{target_uid}"))
    markup.add(InlineKeyboardButton("💰 تعديل رصيده", callback_data=f"ad_ugift_{target_uid}"))
    markup.add(InlineKeyboardButton("🔙 رجوع للبحث", callback_data="ad_users_main"))
    try:
        if message_id: bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="HTML")
        else: bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_dlbuy_"))
def admin_download_buy_hist(call):
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز الملف...")
    target_uid = int(call.data.replace("ad_dlbuy_", ""))
    recs = list(db.orders.find({'user_id': target_uid}).sort('_id', -1))
    if not recs:
        bot.send_message(call.message.chat.id, "📭 العميل لم يقم بأي عمليات شراء.")
        return
        
    content = f"=== سجل مشتريات العميل {target_uid} ===\n\n"
    all_prods = {str(p.get('id', p.get('_id'))): p for p in db.products.find()}
    
    for i, r in enumerate(recs, 1):
        date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
        pid = str(r.get('product_id'))
        p = all_prods.get(pid)
        
        if pid in ['GitHub_Student', 'Gemini_Activation']: n = pid.replace('_', ' ')
        else: n = clean_name(p.get('name_ar', p.get('name_en'))) if p else "Unknown Product"
            
        code = r.get('code_delivered', '')
        content += f"{i}. التاريخ: {date_str}\nالمنتج: {n}\nالكود/التفاصيل: {code}\n{'-'*30}\n"
        
    f = io.BytesIO(content.encode('utf-8'))
    f.name = f"Purchases_User_{target_uid}.txt"
    bot.send_document(call.message.chat.id, f, caption=f"📄 سجل مشتريات العميل {target_uid}.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_uh_"))
def show_admin_hist_detail(call):
    bot.answer_callback_query(call.id)
    parts = call.data.split('_', 3); mode = parts[2]; target_uid = int(parts[3])
    out = f"📂 <b>سجلات العميل (<code>{target_uid}</code>) - أحدث 5:</b>\n\n"
    try:
        if mode == "buy":
            recs = list(db.orders.find({'user_id': target_uid}).sort('_id', -1).limit(5))
            if not recs: out += "📭 لا يوجد مشتريات."
            for r in recs:
                date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
                if r.get('product_id') in ['GitHub_Student', 'Gemini_Activation']:
                    out += f"🛍 <b>{r.get('product_id').replace('_', ' ')}</b>\n📅 <code>{date_str}</code>\n🔑 التفاصيل:\n<code>{r.get('code_delivered', '')}</code>\n---\n"
                    continue
                p = find_product(r['product_id'])
                n = clean_name(p['name_en'] if get_lang(call.from_user.id) == 'en' else p['name_ar']) if p else "Product"
                out += f"🛍 <b>{n}</b>\n📅 <code>{date_str}</code>\n🔑 الكود: <code>{r.get('code_delivered', '')}</code>\n---\n"
        else:
            recs = list(db.used_transactions.find({'user_id': target_uid}).sort('_id', -1).limit(5))
            if not recs: out += "📭 لا يوجد إيداعات."
            for r in recs: 
                date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
                out += f"💰 <b>${r.get('amount', 0):.2f}</b> | 📅 <code>{date_str}</code>\n🆔 <code>{r.get('transaction_id', '')}</code>\n"
    except Exception as e: out = f"❌ Error"
    
    markup = InlineKeyboardMarkup(); markup.add(InlineKeyboardButton("🔙 رجوع لملف العميل", callback_data=f"ad_u_det_{target_uid}"))
    try: bot.edit_message_text(out, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_ugift_"))
def ad_ugift_prompt(call):
    bot.answer_callback_query(call.id)
    target_uid = int(call.data.replace("ad_ugift_", ""))
    msg = bot.send_message(call.message.chat.id, "💰 <b>أرسل المبلغ المراد إضافته (أو خصمه باستخدام سالب -):</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_ugift_exec, target_uid)

def ad_ugift_exec(message, target_uid):
    try:
        val = float(message.text)
        db.users.update_one({'user_id': target_uid}, {'$inc': {'balance': val}})
        bot.send_message(message.chat.id, "✅ تم تعديل الرصيد بنجاح.")
        show_user_admin_profile(message.chat.id, target_uid)
    except: bot.send_message(message.chat.id, "❌ خطأ في الرقم.")

@bot.callback_query_handler(func=lambda call: call.data == "ad_fsub_list")
def admin_fsub_list(call):
    bot.answer_callback_query(call.id)
    chans = list(db.required_channels.find())
    markup = InlineKeyboardMarkup(row_width=1)
    if chans:
        for c in chans: markup.add(InlineKeyboardButton(f"❌ حذف {c['channel_id']}", callback_data=f"del_fsub_{c['channel_id']}"))
    markup.add(InlineKeyboardButton("➕ إضافة قناة باليوزر (@)", callback_data="ad_fsub_add"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    try: bot.edit_message_text("📢 <b>إدارة قنوات الاشتراك الإجباري:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_fsub_add")
def admin_fsub_add(call):
    bot.answer_callback_query(call.id)
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
    except:
        bot.send_message(message.chat.id, f"❌ البوت ليس أدمن في القناة، أو أن اليوزر غير صحيح!")

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_fsub_"))
def del_fsub_btn(call):
    bot.answer_callback_query(call.id)
    ch = call.data.replace("del_fsub_", "")
    db.required_channels.delete_one({'channel_id': ch})
    bot.answer_callback_query(call.id, "✅ تم حذف القناة بنجاح!", show_alert=True)
    admin_fsub_list(call)

@bot.callback_query_handler(func=lambda call: call.data == "ad_new_admin")
def admin_add_admin_start(call):
    bot.answer_callback_query(call.id)
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
    bot.answer_callback_query(call.id)
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

@bot.callback_query_handler(func=lambda call: call.data == "ad_logs_all")
def admin_all_logs(call):
    bot.answer_callback_query(call.id)
    recs = list(db.used_transactions.find().sort('_id', -1).limit(10))
    txt = "📜 <b>Last 10 Deposits:</b>\n\n"
    if not recs: txt = "📭 No records."
    for r in recs: txt += f"👤 <code>{r.get('user_id')}</code> | 💰 <b>${r.get('amount')}</b> | 🆔 <code>{r.get('transaction_id')}</code>\n"
    markup = InlineKeyboardMarkup(); markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_bc")
def admin_bc_init(call):
    bot.answer_callback_query(call.id)
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
    bot.answer_callback_query(call.id)
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("💳 Binance Pay ID", callback_data="set_v_wallet"))
    markup.add(InlineKeyboardButton("🟢 USDT (TRC20)", callback_data="set_v_usdt"),
               InlineKeyboardButton("🟡 USDT (BEP20)", callback_data="set_v_usdt_bep20"))
    markup.add(InlineKeyboardButton("💎 TON Address", callback_data="set_v_ton"),
               InlineKeyboardButton("🔵 LTC Address", callback_data="set_v_ltc"))
    markup.add(InlineKeyboardButton("📢 Logs Channel (@)", callback_data="set_v_log"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    try: bot.edit_message_text("⚙️ <b>Settings:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("set_v_"))
def admin_set_inputs(call):
    bot.answer_callback_query(call.id)
    mode = call.data
    msg = bot.send_message(call.from_user.id, "Send new value:")
    bot.register_next_step_handler(msg, admin_save_setting, mode)

def admin_save_setting(message, mode):
    val = message.text.strip()
    keys = {
        "set_v_log": "log_channel", 
        "set_v_usdt": "usdt_address", 
        "set_v_ltc": "ltc_address", 
        "set_v_wallet": "wallet_address",
        "set_v_usdt_bep20": "usdt_bep20_address",
        "set_v_ton": "ton_address"
    }
    db.settings.update_one({'key': keys[mode]}, {'$set': {'value': val}}, upsert=True)
    bot.send_message(message.chat.id, "✅ Updated.")

# ============================================================
# 🚀 15. التشغيل
# ============================================================
def run_bot():
    try: bot.delete_webhook(drop_pending_updates=True); time.sleep(1)
    except: pass
    while True:
        try: bot.polling(non_stop=True, skip_pending=True)
        except Exception as e: logger.error(f"Polling Error Critical: {e}"); time.sleep(5)

if __name__ == "__main__":
    run_bot()
