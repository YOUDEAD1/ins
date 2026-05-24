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

bot = telebot.TeleBot(TOKEN, use_class_middlewares=False)


# 🛡 دالة فحص صلاحية الأدمن (تُستخدم بكل دوال الأدمن)
def _is_admin_check(uid):
    """يتحقق إن المستخدم أدمن أو OWNER"""
    if uid == OWNER_ID:
        return True
    try:
        u = db.users.find_one({'user_id': int(uid)})
        return u and u.get('is_admin') == 1
    except Exception:
        return False


# 🛡 Middleware: يفحص كل callback queries اللي تبدأ بـ ad_ أو admin_
# لو المستخدم مو أدمن، يرفض الطلب فوراً قبل ما يصير أي شي
def _admin_only_middleware(call):
    """
    🛡 فحص أمني: يرفض أي محاولة وصول لدوال الأدمن من غير الأدمن.
    """
    if not call.data:
        return True  # ما فيه callback data - نسمح (مو دالة admin)
    
    # الـ callbacks المحمية
    is_admin_callback = (
        call.data.startswith('ad_') or 
        call.data.startswith('admin_') or
        call.data.startswith('edit_txt_') or
        call.data.startswith('toggle_amount_protection')
    )
    
    if not is_admin_callback:
        return True  # مو callback admin - نسمح
    
    # نشيك صلاحية المستخدم
    uid = call.from_user.id
    if _is_admin_check(uid):
        return True  # admin - مسموح
    
    # 🚨 محاولة وصول غير مصرح بها!
    try:
        bot.answer_callback_query(
            call.id, 
            bil(uid, "❌ ليس لديك صلاحية للوصول لهذه القائمة!", "❌ You don't have permission to access this menu!"),
            show_alert=True
        )
    except Exception:
        pass
    
    # نسجل المحاولة
    try:
        db.unauthorized_attempts.insert_one({
            'user_id': uid,
            'username': call.from_user.username or 'unknown',
            'callback_data': call.data[:200],
            'timestamp': int(time.time())
        })
    except Exception:
        pass
    
    logger.warning(f"🚨 محاولة وصول غير مصرح بها: المستخدم {uid} حاول {call.data}")
    return False  # رفض المعالجة


def admin_required(func):
    """
    🛡 Decorator يحمي دوال الأدمن من الوصول غير المصرح به.
    
    استخدامها:
    @admin_required
    def my_admin_function(call):
        ...
    """
    def wrapper(call, *args, **kwargs):
        uid = call.from_user.id
        if not _is_admin_check(uid):
            try:
                bot.answer_callback_query(
                    call.id,
                    bil(uid, "❌ ليس لديك صلاحية!", "❌ You don't have permission!"),
                    show_alert=True
                )
            except Exception:
                pass
            
            # نسجل المحاولة
            try:
                db.unauthorized_attempts.insert_one({
                    'user_id': uid,
                    'username': getattr(call.from_user, 'username', 'unknown') or 'unknown',
                    'callback_data': str(call.data)[:200] if hasattr(call, 'data') else 'message',
                    'timestamp': int(time.time())
                })
            except Exception:
                pass
            
            logger.warning(f"🚨 محاولة وصول غير مصرح بها: المستخدم {uid}")
            return
        return func(call, *args, **kwargs)
    
    wrapper.__name__ = func.__name__
    return wrapper

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


# 🆕 دوال للحصول على الإعدادات (قابلة للتعديل من الأدمن)
def get_referral_threshold():
    """يجلب عدد الإحالات المطلوبة لكل مكافأة (افتراضي: 10)"""
    try:
        val = get_setting('referral_threshold')
        if val and val != "Not Set":
            return max(1, int(val))
    except: pass
    return 10


def get_referral_reward():
    """يجلب قيمة المكافأة لكل دفعة إحالات (افتراضي: 0.10)"""
    try:
        val = get_setting('referral_reward')
        if val and val != "Not Set":
            return max(0.01, float(val))
    except: pass
    return REFERRAL_REWARD


def get_referral_purchase_reward():
    """يجلب قيمة المكافأة لكل شراء من المُحال (افتراضي: 0.10)"""
    try:
        val = get_setting('referral_purchase_reward')
        if val and val != "Not Set":
            return max(0.01, float(val))
    except: pass
    return REFERRAL_REWARD


def get_referral_min_purchase():
    """يجلب الحد الأدنى لقيمة الشراء (افتراضي: $2)"""
    try:
        val = get_setting('referral_min_purchase')
        if val and val != "Not Set":
            return max(0.10, float(val))
    except: pass
    return REFERRAL_MIN_PURCHASE
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

        # 🛡 4. حماية atomic ضد سرقة الحوالات (race conditions)
        # هذولي الـ indexes حرجين - يمنعون أي مستخدمين من استخدام نفس الـ hash بنفس اللحظة
        try:
            db.used_transactions.create_index('transaction_id', unique=True)
            logger.info("✅ تم إنشاء unique index على used_transactions")
        except Exception as e:
            # الـ index قد يكون موجود مسبقاً - هذا ok
            logger.debug(f"used_transactions index info: {e}")
        
        try:
            db.claimed_hashes.create_index('transaction_id', unique=True)
            logger.info("✅ تم إنشاء unique index على claimed_hashes")
        except Exception as e:
            logger.debug(f"claimed_hashes index info: {e}")

        # 4. وضع علامة أن النظام تم تهيئته
        db.settings.update_one(
            {'key': REF_V2_INIT_KEY},
            {'$set': {'value': 'done', 'init_time': int(time.time())}},
            upsert=True
        )

        logger.info("✅ تم تهيئة نظام الإحالات V2 بنجاح. (كل شي يبدأ من الصفر)")
    except Exception as e:
        logger.error(f"❌ فشل تهيئة نظام الإحالات V2: {e}")


# 🛡 دالة منفصلة لإنشاء الـ unique indexes - تتنفذ في كل تشغيل
def ensure_critical_indexes():
    """
    🛡 تضمن وجود الـ unique indexes الحرجة في كل تشغيل للبوت.
    هذي الـ indexes تمنع race conditions في race conditions.
    لازم تتنفذ مهما كانت حالة DB.
    """
    # 🛡 الخطوة 0: نرملز كل الـ used_transactions القديمة (للتوافق مع normalize_tx_id الجديد)
    try:
        # نجلب كل الـ records اللي transaction_id فيها مو متطابق مع normalize
        all_txs = list(db.used_transactions.find({}, {'transaction_id': 1}))
        normalized_count = 0
        for tx_doc in all_txs:
            old_tx_id = tx_doc.get('transaction_id', '')
            if not old_tx_id:
                continue
            new_tx_id = normalize_tx_id(old_tx_id)
            if new_tx_id != old_tx_id:
                # نشيك لو فيه duplicate بعد التطبيع
                existing = db.used_transactions.find_one({'transaction_id': new_tx_id})
                if existing and existing.get('_id') != tx_doc.get('_id'):
                    # في duplicate - نحذف القديم (الأقل) ونحتفظ بالأول
                    logger.warning(f"🚨 وجد duplicate بعد التطبيع: {old_tx_id[:30]} = {new_tx_id[:30]}")
                    db.used_transactions.delete_one({'_id': tx_doc['_id']})
                else:
                    # نحدث الـ transaction_id لقيمته المطبّعة
                    try:
                        db.used_transactions.update_one(
                            {'_id': tx_doc['_id']},
                            {'$set': {'transaction_id': new_tx_id}}
                        )
                        normalized_count += 1
                    except Exception:
                        pass
        if normalized_count > 0:
            logger.info(f"✅ تم تطبيع {normalized_count} transaction_id قديم")
    except Exception as e:
        logger.error(f"Failed to normalize old transactions: {e}")
    
    try:
        # 1. used_transactions - الحماية الأهم!
        # لو فيه duplicates من قبل، نحاول ننظفها أولاً
        try:
            db.used_transactions.create_index('transaction_id', unique=True)
            logger.info("✅ unique index على used_transactions.transaction_id جاهز")
        except Exception as idx_err:
            err_str = str(idx_err).lower()
            if 'duplicate' in err_str or 'e11000' in err_str:
                # في duplicates موجودة - نحذف المكررات أولاً (نحتفظ بالأقدم)
                logger.warning("⚠️ وُجدت duplicates في used_transactions - جاري التنظيف...")
                
                # نجد كل الـ duplicates
                pipeline = [
                    {'$group': {
                        '_id': '$transaction_id',
                        'count': {'$sum': 1},
                        'ids': {'$push': '$_id'},
                        'amounts': {'$push': '$amount'},
                        'users': {'$push': '$user_id'}
                    }},
                    {'$match': {'count': {'$gt': 1}}}
                ]
                
                duplicates = list(db.used_transactions.aggregate(pipeline))
                
                for dup in duplicates:
                    # نحتفظ بأول واحد فقط، نحذف الباقي
                    ids_to_keep = dup['ids'][0]
                    ids_to_delete = dup['ids'][1:]
                    
                    # تسجيل للأدمن (الناس اللي خسروا فلوس بسبب التلاعب)
                    logger.warning(
                        f"🚨 وجدت {len(ids_to_delete)} تكرار للـ tx {dup['_id'][:30]}\n"
                        f"   المستخدمين: {dup['users']}\n"
                        f"   المبالغ: {dup['amounts']}"
                    )
                    
                    # حذف المكررات
                    db.used_transactions.delete_many({'_id': {'$in': ids_to_delete}})
                
                # نحاول ننشئ الـ index مرة ثانية
                try:
                    db.used_transactions.create_index('transaction_id', unique=True)
                    logger.info("✅ تم إنشاء unique index بعد تنظيف المكررات")
                except Exception as e2:
                    logger.error(f"❌ فشل إنشاء unique index حتى بعد التنظيف: {e2}")
            else:
                logger.debug(f"used_transactions index info: {idx_err}")
        
        # 🆕 1.5. index على الـ fingerprint (للحماية ضد نفس الحوالة بـ tx_id مختلف)
        try:
            db.used_transactions.create_index('fingerprint', unique=True, sparse=True)
            logger.info("✅ unique index على used_transactions.fingerprint جاهز")
        except Exception as idx_err:
            err_str = str(idx_err).lower()
            if 'duplicate' in err_str or 'e11000' in err_str:
                logger.warning("⚠️ duplicates في fingerprint - جاري التنظيف...")
                # نحذف الـ records اللي ما عندهم fingerprint (records قديمة)
                # نحتفظ بالأقدم لكل fingerprint
                pipeline = [
                    {'$match': {'fingerprint': {'$exists': True, '$ne': None}}},
                    {'$group': {
                        '_id': '$fingerprint',
                        'count': {'$sum': 1},
                        'ids': {'$push': '$_id'}
                    }},
                    {'$match': {'count': {'$gt': 1}}}
                ]
                try:
                    duplicates = list(db.used_transactions.aggregate(pipeline))
                    for dup in duplicates:
                        ids_to_delete = dup['ids'][1:]  # نحتفظ بالأول
                        db.used_transactions.delete_many({'_id': {'$in': ids_to_delete}})
                    
                    db.used_transactions.create_index('fingerprint', unique=True, sparse=True)
                    logger.info("✅ تم إنشاء unique index على fingerprint بعد التنظيف")
                except Exception as e: 
                    logger.error(f"فشل: {e}")
            else:
                logger.debug(f"fingerprint index info: {idx_err}")
        
        # 🆕 1.6. index على method+amount+created_at للفحص السريع
        try:
            db.used_transactions.create_index([
                ('method', 1),
                ('amount', 1),
                ('created_at', 1)
            ])
            logger.info("✅ compound index على used_transactions جاهز")
        except Exception as e:
            logger.debug(f"compound index info: {e}")
        
        # 2. claimed_hashes
        try:
            db.claimed_hashes.create_index('transaction_id', unique=True)
            logger.info("✅ unique index على claimed_hashes جاهز")
        except Exception as idx_err:
            err_str = str(idx_err).lower()
            if 'duplicate' in err_str or 'e11000' in err_str:
                # نظف الـ claimed_hashes المكررة (احتفظ بالأقدم)
                pipeline = [
                    {'$group': {
                        '_id': '$transaction_id',
                        'count': {'$sum': 1},
                        'ids': {'$push': '$_id'}
                    }},
                    {'$match': {'count': {'$gt': 1}}}
                ]
                duplicates = list(db.claimed_hashes.aggregate(pipeline))
                for dup in duplicates:
                    ids_to_delete = dup['ids'][1:]
                    db.claimed_hashes.delete_many({'_id': {'$in': ids_to_delete}})
                
                try:
                    db.claimed_hashes.create_index('transaction_id', unique=True)
                    logger.info("✅ unique index على claimed_hashes جاهز (بعد التنظيف)")
                except: pass
            else:
                logger.debug(f"claimed_hashes index info: {idx_err}")
    except Exception as e:
        logger.error(f"❌ فشل critical: {e}")


# نُفّذها فوراً عند بدء البوت
initialize_referrals_v2()
ensure_critical_indexes()


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
        result = db.referrals_v2.find_one_and_update(
            {
                'invited_id': invited_id,
                'status': {'$ne': new_status}
            },
            {
                '$set': {
                    'status': new_status,
                    'updated_at': int(time.time())
                }
            },
            return_document=False
        )
        
        if result is None:
            return
        
        old_status = result.get('status', 'pending')
        referrer_id = result['referrer_id']
        
        # تحديث رصيد المُحيل
        update_referrer_balance(referrer_id)
        
        # 🆕 لما إحالة تصير active (جديدة)، نرسل إشعار "باقي X"
        if new_status == 'active' and old_status != 'active':
            try:
                send_progress_log_notification(referrer_id)
            except Exception as prog_err:
                logger.debug(f"Progress notification failed: {prog_err}")
    except Exception as e:
        logger.error(f"Error marking referral status: {e}")


def send_progress_log_notification(referrer_id):
    """
    📢 لكل إحالة نشطة جديدة:
    1. رسالة في قناة اللوق بالإنجليزي: "باقي X للمكافأة"
    2. DM للمُحيل: "شخص جديد انضم عبر رابطك"
    """
    try:
        threshold = get_referral_threshold()
        reward = get_referral_reward()

        active_count = db.referrals_v2.count_documents({
            'referrer_id': referrer_id,
            'status': 'active'
        })

        current_in_batch = active_count % threshold
        remaining = threshold - current_in_batch if current_in_batch > 0 else threshold

        # 1) DM للمُحيل (بلغته)
        try:
            referrer = db.users.find_one({'user_id': referrer_id})
            if referrer and referrer.get('is_banned') != 1:
                ref_lang = referrer.get('lang', 'ar')
                if ref_lang == 'ar':
                    dm_text = (
                        f"🎉 <b>شخص جديد انضم عبر رابطك!</b>\n\n"
                        f"✅ إحالاتك النشطة الآن: <b>{active_count}</b>\n"
                        f"⏳ باقي <b>{remaining}</b> فقط للحصول على <b>${reward:.2f}</b>"
                    )
                else:
                    dm_text = (
                        f"🎉 <b>Someone joined via your link!</b>\n\n"
                        f"✅ Your active referrals: <b>{active_count}</b>\n"
                        f"⏳ Only <b>{remaining}</b> more to earn <b>${reward:.2f}</b>"
                    )
                bot.send_message(referrer_id, dm_text, parse_mode="HTML")
        except Exception as dm_err:
            logger.debug(f"Progress DM failed for {referrer_id}: {dm_err}")

        # 2) قناة اللوق (بالإنجليزي دائماً)
        if remaining == threshold:
            return  # milestone - الـ update_referrer_balance سيرسل

        try:
            log_ch = get_setting('log_channel')
            if log_ch and log_ch != "Not Set":
                log_text = (
                    f"📈 <b>New Active Referral!</b>\n\n"
                    f"👤 Referrer: <b>**</b>\n"
                    f"✅ Active Referrals: <b>{active_count}</b>\n"
                    f"⏳ <b>{remaining}</b> more to earn <b>${reward:.2f}</b>"
                )
                bot.send_message(log_ch, log_text, parse_mode="HTML")
        except Exception as log_err:
            logger.debug(f"Progress log failed: {log_err}")

    except Exception as e:
        logger.error(f"send_progress_log_notification error: {e}")


def update_referrer_balance(referrer_id):
    """
    تحديث رصيد المُحيل.
    الإصلاح الرئيسي: الـ optimistic lock يفشل لو ref_v2_earned غير موجود في DB.
    """
    try:
        rid = int(referrer_id)
        with _get_referrer_lock(rid):
            threshold = get_referral_threshold()
            reward = get_referral_reward()
            
            active_count = db.referrals_v2.count_documents({'referrer_id': rid, 'status': 'active'})
            expected = round((active_count // threshold) * reward, 2)

            referrer = db.users.find_one({'user_id': rid})
            if not referrer:
                return

            # 🛡 الإصلاح: نتعامل مع الحقل المفقود أو القيمة الخاطئة
            raw_earned = referrer.get('ref_v2_earned', 0.0)
            try:
                current_earned = round(float(raw_earned), 2)
            except (ValueError, TypeError):
                current_earned = 0.0

            if expected == current_earned:
                return  # لا يوجد تغيير

            diff = round(expected - current_earned, 2)

            # 🛡 الإصلاح الأساسي: شرط يقبل الحقل المفقود أو 0.0
            if current_earned == 0.0:
                match_condition = {
                    'user_id': rid,
                    '$or': [
                        {'ref_v2_earned': 0.0},
                        {'ref_v2_earned': 0},
                        {'ref_v2_earned': ''},
                        {'ref_v2_earned': {'$exists': False}}
                    ]
                }
            else:
                match_condition = {
                    'user_id': rid,
                    'ref_v2_earned': current_earned
                }

            update_result = db.users.find_one_and_update(
                match_condition,
                {
                    '$inc': {'balance': diff},
                    '$set': {'ref_v2_earned': expected}
                },
                return_document=True
            )

            if update_result is None:
                # فشل الـ optimistic lock - نعيد المحاولة مرة واحدة
                update_result = db.users.find_one_and_update(
                    {'user_id': rid},
                    {
                        '$inc': {'balance': diff},
                        '$set': {'ref_v2_earned': expected}
                    },
                    return_document=True
                )

            if update_result is not None and diff > 0:
                new_balance = round(float(update_result.get('balance', 0)), 2)
                ref_lang = referrer.get('lang', 'ar')

                # 1) رسالة للمُحيل في الخاص
                try:
                    if ref_lang == 'ar':
                        milestone_msg = (
                            f"🎉🎊 <b>مبروك! وصلت لإنجاز جديد!</b> 🏆\n\n"
                            f"━━━━━━━━━━━━━━\n"
                            f"👥 <b>إحالاتك النشطة:</b> {active_count}\n"
                            f"💰 <b>مكافأتك:</b> <code>+${diff:.2f}</code>\n"
                            f"💼 <b>رصيدك الآن:</b> ${new_balance:.2f}\n"
                            f"━━━━━━━━━━━━━━\n\n"
                            f"🔥 شارك رابطك أكثر = اكسب أكثر!"
                        )
                    else:
                        milestone_msg = (
                            f"🎉🎊 <b>Congrats! New Milestone Reached!</b> 🏆\n\n"
                            f"━━━━━━━━━━━━━━\n"
                            f"👥 <b>Active Referrals:</b> {active_count}\n"
                            f"💰 <b>Your Reward:</b> <code>+${diff:.2f}</code>\n"
                            f"💼 <b>Balance Now:</b> ${new_balance:.2f}\n"
                            f"━━━━━━━━━━━━━━\n\n"
                            f"🔥 Share your link more = Earn more!"
                        )
                    bot.send_message(rid, milestone_msg, parse_mode="HTML")
                except Exception as dm_err:
                    logger.debug(f"Milestone DM failed for {rid}: {dm_err}")

                # 2) إشعار قناة اللوق (بالإنجليزي دائماً)
                try:
                    log_ch = get_setting('log_channel')
                    if log_ch and log_ch != "Not Set":
                        log_text = (
                            f"🏆 <b>Referral Milestone!</b>\n\n"
                            f"👤 User: <b>**</b>\n"
                            f"✅ Active Referrals: <b>{active_count}</b>\n"
                            f"💰 Reward Earned: <b>+${diff:.2f}</b>\n"
                            f"📊 Total from Referrals: <b>${expected:.2f}</b>"
                        )
                        bot.send_message(log_ch, log_text, parse_mode="HTML")
                except Exception as log_err:
                    logger.debug(f"Milestone log failed: {log_err}")

                # 3) إشعار للأدمن
                try:
                    admin_notif = (
                        f"💎 <b>Referral Milestone!</b>\n\n"
                        f"👤 ID: <code>{rid}</code>\n"
                        f"✅ Active: <b>{active_count}</b>\n"
                        f"💰 Reward: <b>+${diff:.2f}</b>\n"
                        f"💼 New Balance: <b>${new_balance:.2f}</b>"
                    )
                    notify_admins(admin_notif)
                except Exception:
                    pass

                logger.info(f"✅ Referral reward: user {rid} → +${diff:.2f} (active: {active_count})")

    except Exception as e:
        logger.error(f"Error in update_referrer_balance({referrer_id}): {e}")


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
        
        # 🆕 نجلب الإعدادات الحالية
        min_purchase = get_referral_min_purchase()
        purchase_reward = get_referral_purchase_reward()
        
        # ⚠️ شرط الحد الأدنى
        if purchase_amount <= min_purchase:
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
                    'balance': purchase_reward,
                    'ref_v2_purchase_earned': purchase_reward
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
                'amount': purchase_reward,
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
                f"{purchase_reward:.2f}",
                f"{new_balance:.2f}"
            )
            
            # نشيك على النص المخصص لو الأدمن عدّله من CMS
            custom_dm = db.custom_texts.find_one({'lang': 'en', 'key': 'ref_purchase_dm'})
            if custom_dm and custom_dm.get('value'):
                try:
                    celebration = custom_dm['value'].format(
                        buyer_display,
                        f"{purchase_reward:.2f}",
                        f"{new_balance:.2f}"
                    )
                except:
                    pass  # لو في خطأ format، نستخدم الافتراضي
            
            bot.send_message(referrer_id, celebration, parse_mode="HTML")
        except Exception as notify_err:
            logger.debug(f"Couldn't notify referrer {referrer_id}: {notify_err}")
        
        logger.info(f"🎁 مكافأة شراء: {referrer_id} ربح ${purchase_reward:.2f} من شراء {buyer_uid}")
        
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
                    f"{purchase_reward:.2f}"
                )
                
                # نشيك على النص المخصص لو الأدمن عدّله من CMS
                custom_log = db.custom_texts.find_one({'lang': 'en', 'key': 'log_ref_purchase'})
                if custom_log and custom_log.get('value'):
                    try:
                        log_text = custom_log['value'].format(
                            referrer_display,
                            f"{purchase_reward:.2f}"
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
        bot.send_message(uid, bil(uid, "❌ <b>النظام غير متصل (اليوزربوت معطل).</b> تم إرجاع رصيدك.", "❌ <b>System offline (userbot disabled).</b> Balance refunded."), parse_mode="HTML")
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
    """🔒 يخفي النص بـ ** فقط (مو a***d)"""
    return "**"

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


def bil(uid, ar_text, en_text):
    """يرجع النص بلغة المستخدم (عربي أو إنجليزي)"""
    return en_text if get_lang(uid) == 'en' else ar_text


def send_no_balance(uid):
    """يرسل رسالة رصيد غير كافٍ + زر الشحن مباشرة"""
    l = get_lang(uid)
    if l == 'ar':
        msg = "❌ <b>رصيدك غير كافٍ!</b>\n\nاشحن رصيدك وحاول مرة ثانية."
        btn = "💳 شحن الرصيد"
    else:
        msg = "❌ <b>Insufficient balance!</b>\n\nDeposit and try again."
        btn = "💳 Deposit Now"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(btn, callback_data="open_deposit"))
    bot.send_message(uid, msg, parse_mode="HTML", reply_markup=markup)


def safe_next_step(func):
    """
    🛡 Decorator يحمي next_step_handlers من CallbackQuery.
    لو وصل CallbackQuery بدل Message، يمسح الـ handler ويتجاهله.
    """
    def wrapper(message, *args, **kwargs):
        try:
            # لو CallbackQuery وصل بدل Message
            if not hasattr(message, 'text') or hasattr(message, 'data'):
                # نمسح أي step handlers معلقة
                try:
                    chat_id = message.message.chat.id if hasattr(message, 'message') else message.from_user.id
                    bot.clear_step_handler_by_chat_id(chat_id=chat_id)
                except Exception:
                    pass
                return
            return func(message, *args, **kwargs)
        except AttributeError:
            return
    wrapper.__name__ = func.__name__
    return wrapper

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
    
    # 🛡 نمسح أي next_step handlers معلقة (يمنع تفسير /start كـ tx_id)
    try:
        bot.clear_step_handler_by_chat_id(chat_id=chat_id)
    except Exception: pass
    
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
    
    # 🆕 مكافآت رجعية: نتأكد إن المستخدم استلم كل مكافآته (لو فاته شي)
    try:
        update_referrer_balance(uid)
    except Exception as ref_err:
        logger.debug(f"Retroactive reward check error: {ref_err}")

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
    try:
        bot.send_message(chat_id, welcome_message, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        if 'parse entities' in str(e).lower() or 'can\'t parse' in str(e).lower():
            # HTML مكسور في CMS - نرسل بدون parse_mode
            import re as _re
            clean_msg = _re.sub(r'<[^>]+>', '', welcome_message)
            try:
                bot.send_message(chat_id, clean_msg, reply_markup=markup)
            except Exception:
                bot.send_message(chat_id, "مرحباً! 👋", reply_markup=markup)
        else:
            raise


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
        bot.send_message(uid, bil(uid, "⏳ <b>يوجد عملية شراء قيد المعالجة!</b>\nانتظر انتهاءها أولاً.", "⏳ <b>You have a purchase in progress!</b>\nWait until it finishes."), parse_mode="HTML")
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
            send_no_balance(uid)
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
        bot.send_message(uid, bil(uid, "⏳ <b>يوجد عملية شراء أو تفعيل قيد المعالجة!</b>\nانتظر انتهاءها أولاً.", "⏳ <b>You have a purchase/activation in progress!</b>\nWait until it finishes."), parse_mode="HTML")
        return
    
    gh_price = float(get_setting("github_price", 15.0))
    u = get_user_data_full(uid)
    
    if float(u.get('balance', 0)) < gh_price:
        _release_purchase_lock(uid)  # حرّر القفل
        send_no_balance(uid)
        return
        
    temp_github_data[uid] = {'price': gh_price, 'lang': l}
    
    msg = bot.send_message(uid, get_text(uid, 'gh_prompt_user'), parse_mode="HTML")
    bot.register_next_step_handler(msg, process_gh_step_user)

@safe_next_step
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

@safe_next_step
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

@safe_next_step
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
                
                for i, r in enumerate(recs, 1): 
                    date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
                    amount = r.get('amount', 0)
                    method = r.get('method', '')
                    tx_id = r.get('transaction_id', '')
                    
                    if l == 'en':
                        out += (
                            f"━━━━━━━━━━━━━━\n"
                            f"#{i} <b>Deposit</b> ✅\n"
                            f"💰 <b>Amount:</b> <code>${amount:.2f}</code>\n"
                            f"📅 <b>Date:</b> <code>{date_str}</code>\n"
                        )
                        if method:
                            out += f"💳 <b>Method:</b> {method}\n"
                        if tx_id:
                            out += f"🆔 <b>TX ID:</b> <code>{tx_id}</code>\n"
                    else:
                        out += (
                            f"━━━━━━━━━━━━━━\n"
                            f"#{i} <b>إيداع</b> ✅\n"
                            f"💰 <b>المبلغ:</b> <code>${amount:.2f}</code>\n"
                            f"📅 <b>التاريخ:</b> <code>{date_str}</code>\n"
                        )
                        if method:
                            out += f"💳 <b>الطريقة:</b> {method}\n"
                        if tx_id:
                            out += f"🆔 <b>رقم العملية:</b> <code>{tx_id}</code>\n"
                
                out += "━━━━━━━━━━━━━━"
            
            markup = InlineKeyboardMarkup()
            markup.add(create_btn(uid, 'btn_back', callback_data="history_menu_callback"))
        
        try: bot.edit_message_text(out, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        except: pass
    except Exception as e:
        logger.error(f"Error in show_hist_detail: {e}")
        try: bot.send_message(uid, bil(uid, "❌ حدث خطأ، حاول مرة ثانية.", "❌ An error occurred, please try again."), parse_mode="HTML")
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
        bot.send_message(uid, bil(uid, "❌ فشل إرسال الملف، حاول مرة ثانية.", "❌ Failed to send file, please try again."))

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
    
    # 🆕 تحديث رجعي - لو فيه إحالات نشطة لم يستلم مكافأتها، يضيف الآن
    try:
        update_referrer_balance(uid)
        u = get_user_data_full(uid)
    except Exception as e:
        logger.error(f"Error updating referrer balance in invite_ui: {e}")
    
    # جلب الإحصائيات من الجدول الجديد
    pending_count, active_count, left_count, total_clicks = get_ref_counts(uid)
    
    # 🆕 الأرباح من الإحالات (milestones - كل 10 إحالات)
    earnings_from_referrals = round(float(u.get('ref_v2_earned', 0.0)), 2)
    
    # 🆕 الأرباح من مشتريات المُحالين
    earnings_from_purchases = round(float(u.get('ref_v2_purchase_earned', 0.0)), 2)
    
    # 🆕 الإجمالي
    total_earnings = round(earnings_from_referrals + earnings_from_purchases, 2)
    
    # 🆕 حساب باقي للمكافأة القادمة
    threshold = get_referral_threshold()
    reward = get_referral_reward()
    current_in_batch = active_count % threshold
    remaining_to_milestone = threshold - current_in_batch if current_in_batch > 0 else 0

    markup = InlineKeyboardMarkup()
    markup.add(create_btn(uid, 'btn_refresh', callback_data="open_invite"))
    markup.add(create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh"))
    
    # نبني الرسالة يدوياً (نتجاوز LANG عشان نضمن العرض الجديد)
    if l == 'en':
        final_text = (
            f"💎 <b>Referral System</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"📊 <b>Your Stats</b>\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"👥 Clicks:  <b>{total_clicks}</b>\n"
            f"⏳ Pending:  <b>{pending_count}</b>\n"
            f"✅ Active:  <b>{active_count}</b>\n"
            f"❌ Left:  <b>{left_count}</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💰 <b>Your Earnings</b>\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"🎯 From referrals:  <b>${earnings_from_referrals:.2f}</b>\n"
            f"🛍 From purchases:  <b>${earnings_from_purchases:.2f}</b>\n"
            f"💎 <b>Total:</b>  <b>${total_earnings:.2f}</b>\n"
        )
        
        if remaining_to_milestone > 0:
            final_text += (
                f"\n━━━━━━━━━━━━━━\n"
                f"⏳ <b>{remaining_to_milestone}</b> more active referrals to earn <b>${reward:.2f}</b>!\n"
            )
        
        final_text += (
            f"\n━━━━━━━━━━━━━━\n"
            f"🔗 <b>Your Link:</b>\n"
            f"<code>https://t.me/{b_n}?start={uid}</code>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"🎁 <b>Two Ways to Earn:</b>\n\n"
            f"🔥 Every {threshold} active joins = <b>${reward:.2f}</b>\n"
            f"💸 Friend buys > ${get_referral_min_purchase():.2f} = <b>${get_referral_purchase_reward():.2f}</b>\n\n"
            f"⚡ <i>Real-time updates</i>"
        )
    else:
        final_text = (
            f"💎 <b>نظام الإحالات</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"📊 <b>إحصائياتك</b>\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"👥 الزيارات:  <b>{total_clicks}</b>\n"
            f"⏳ معلق:  <b>{pending_count}</b>\n"
            f"✅ نشط:  <b>{active_count}</b>\n"
            f"❌ غادر:  <b>{left_count}</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💰 <b>أرباحك</b>\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"🎯 من الإحالات:  <b>${earnings_from_referrals:.2f}</b>\n"
            f"🛍 من المشتريات:  <b>${earnings_from_purchases:.2f}</b>\n"
            f"💎 <b>المجموع:</b>  <b>${total_earnings:.2f}</b>\n"
        )
        
        if remaining_to_milestone > 0:
            final_text += (
                f"\n━━━━━━━━━━━━━━\n"
                f"⏳ باقي <b>{remaining_to_milestone}</b> إحالة فقط للحصول على <b>${reward:.2f}</b>!\n"
            )
        
        final_text += (
            f"\n━━━━━━━━━━━━━━\n"
            f"🔗 <b>رابطك:</b>\n"
            f"<code>https://t.me/{b_n}?start={uid}</code>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"🎁 <b>طريقتان للربح:</b>\n\n"
            f"🔥 كل {threshold} إحالة نشطة = <b>${reward:.2f}</b>\n"
            f"💸 شراء صديق > ${get_referral_min_purchase():.2f} = <b>${get_referral_purchase_reward():.2f}</b>\n\n"
            f"⚡ <i>تحديثات لحظية</i>"
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
        bot.send_message(uid, bil(uid, "❌ عذراً، المنتج غير متوفر.", "❌ Sorry, product is unavailable."), parse_mode="HTML"); return
    
    u = get_user_data_full(uid)
    is_admin = (u.get('is_admin') == 1 or uid == OWNER_ID)
    if p.get('is_hidden', False) and not is_admin:
        bot.send_message(uid, bil(uid, "❌ عذراً، هذا المنتج غير متوفر حالياً.", "❌ Sorry, this product is currently unavailable."), parse_mode="HTML"); return

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
    
    # 🛡 زر التعديل للأدمن فقط
    if _is_admin_check(uid):
        markup.add(InlineKeyboardButton("⚙️ ...", callback_data=f"edit_p_{pid}"))
    
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
        bot.send_message(uid, bil(uid, "⏳ <b>يوجد طلب شراء قيد المعالجة لك بالفعل!</b>\nانتظر انتهاءه قبل بدء طلب جديد.", "⏳ <b>You have a purchase in progress!</b>\nWait until it finishes before starting another."), parse_mode="HTML")
        return

    try:
        u = get_user_data_full(uid)
        p = find_product(pid)
        if not p:
            bot.send_message(uid, bil(uid, "❌ المنتج غير موجود.", "❌ Product not found."), parse_mode="HTML")
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
            send_no_balance(uid)
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
                bot.send_message(uid, bil(uid, "❌ حدث خطأ في معالجة الطلب. تم إرجاع رصيدك.", "❌ Error processing request. Balance refunded."), parse_mode="HTML")
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

@safe_next_step
def process_stars_amount(message, lang):
    uid = message.from_user.id
    if not message.text:
        return
    
    text = message.text.strip()
    
    # 🛡 رفض الأوامر
    if text.startswith('/'):
        return
    
    # رفض الإلغاء
    if text.lower() in ['الغاء', 'cancel', 'إلغاء']:
        bot.send_message(uid, bil(uid, "❌ تم الإلغاء.", "❌ Cancelled."))
        return
    
    try:
        usd_amount = float(text.replace(',', '.').replace('$', ''))
    except ValueError:
        err = "❌ الرجاء إرسال أرقام فقط (مثال: 5 أو 10.5)." if lang == 'ar' else "❌ Please send numbers only (e.g., 5 or 10.5)."
        bot.send_message(uid, err, parse_mode="HTML")
        return
    
    # 🛡 validation
    if usd_amount < 0.1:
        err = "❌ الحد الأدنى للشحن هو $0.10" if lang == 'ar' else "❌ Minimum deposit is $0.10"
        bot.send_message(uid, err, parse_mode="HTML")
        return
    
    if usd_amount > 1000:
        err = "❌ الحد الأقصى للشحن هو $1000" if lang == 'ar' else "❌ Maximum deposit is $1000"
        bot.send_message(uid, err, parse_mode="HTML")
        return
        
    stars_amount = int(usd_amount * STARS_RATE)
    
    # 🛡 stars_amount يجب يكون >= 1 (Telegram يرفض 0)
    if stars_amount < 1:
        err = "❌ المبلغ صغير جداً. زده قليلاً." if lang == 'ar' else "❌ Amount too small. Please increase."
        bot.send_message(uid, err, parse_mode="HTML")
        return
    
    title = "شحن رصيد المتجر" if lang == 'ar' else "Shop Balance Deposit"
    desc = f"شحن حساب بمبلغ ${usd_amount:.2f}" if lang == 'ar' else f"Deposit ${usd_amount:.2f} to your account"
    prices = [LabeledPrice(label=f"Deposit ${usd_amount:.2f}", amount=stars_amount)]
    
    try:
        bot.send_invoice(
            chat_id=uid,
            title=title,
            description=desc,
            invoice_payload=f"dep_{uid}_{usd_amount}",
            provider_token="",
            currency="XTR",
            prices=prices
        )
    except Exception as e:
        err_str = str(e).lower()
        if 'currency_total_amount_invalid' in err_str:
            msg = (
                "❌ <b>المبلغ غير صحيح!</b>\n\n"
                "💡 جرّب مبلغ أكبر أو تحقق من معدل التحويل."
            ) if lang == 'ar' else (
                "❌ <b>Invalid amount!</b>\n\n"
                "💡 Try a larger amount or check the conversion rate."
            )
        else:
            msg = (
                "❌ <b>حدث خطأ في إنشاء فاتورة الدفع.</b>\n\n"
                "💡 حاول مرة ثانية بعد قليل."
            ) if lang == 'ar' else (
                "❌ <b>Error creating invoice.</b>\n\n"
                "💡 Please try again later."
            )
        bot.send_message(uid, msg, parse_mode="HTML")
        logger.error(f"send_invoice error for user {uid}: {e}")

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

    if l == 'ar':
        msg_text = (
            f"🟡 <b>الإيداع عبر Binance Pay</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💡 أرسل المبلغ بالدولار الذي تريد إيداعه:\n\n"
            f"📌 أمثلة: 5 / 10 / 25 / 50\n"
            f"⚠️ الحد الأدنى: <b>$1</b>"
        )
    else:
        msg_text = (
            f"🟡 <b>Deposit via Binance Pay</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💡 Send the USD amount you want to deposit:\n\n"
            f"📌 Examples: 5 / 10 / 25 / 50\n"
            f"⚠️ Minimum: <b>$1</b>"
        )

    msg = bot.send_message(uid, msg_text, parse_mode="HTML")
    bot.register_next_step_handler(msg, ask_binance_deposit_amount)


@safe_next_step
def ask_binance_deposit_amount(message):
    """يستلم المبلغ ويعطي مبلغ فريد لـ Binance Pay"""
    uid = message.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)

    if not message.text or message.text.startswith('/'):
        return

    if message.text.lower() in ['الغاء', 'cancel']:
        bot.send_message(uid, bil(uid, "❌ تم الإلغاء.", "❌ Cancelled."))
        return

    try:
        base_amount = float(message.text.strip().replace(',', '.').replace('$', ''))
    except ValueError:
        bot.send_message(uid, bil(uid, "❌ أرسل رقماً فقط. مثال: 10", "❌ Numbers only. Example: 10"), parse_mode="HTML")
        return

    if base_amount < 1:
        bot.send_message(uid, bil(uid, "❌ الحد الأدنى $1", "❌ Minimum $1"), parse_mode="HTML")
        return

    if base_amount > 10000:
        bot.send_message(uid, bil(uid, "❌ الحد الأقصى $10,000", "❌ Maximum $10,000"), parse_mode="HTML")
        return

    # نولّد مبلغ فريد بـ 4 خانات عشرية = 9000 احتمال
    unique_amount = generate_unique_amount_for_user(base_amount, uid, 'BINANCE')

    # نسجّل في DB
    pending = register_pending_deposit(uid, base_amount, unique_amount, 'BINANCE')
    if not pending:
        bot.send_message(uid, bil(uid, "❌ حدث خطأ. حاول مرة ثانية.", "❌ Error. Try again."), parse_mode="HTML")
        return

    wallet = get_setting('wallet_address')

    if l == 'ar':
        msg_text = (
            f"🟡 <b>تعليمات الإيداع - Binance Pay</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💰 <b>المبلغ المطلوب تحويله:</b>\n"
            f"<code>${unique_amount:.4f}</code>\n"
            f"☝️ <b>بالضبط هذا الرقم!</b>\n\n"
            f"📬 <b>المحفظة:</b>\n<code>{wallet}</code>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"⚠️ <b>تنبيهات:</b>\n"
            f"✅ حوّل بالضبط: <code>${unique_amount:.4f}</code>\n"
            f"❌ لا تحوّل ${base_amount:.2f} (المبلغ الأساسي)\n"
            f"❌ لا تغيّر الرقم\n\n"
            f"⏰ <b>صلاحية الطلب:</b> 30 دقيقة\n\n"
            f"✨ <b>سيُضاف الرصيد تلقائياً بمجرد استلام التحويل.</b>\n"
            f"<i>لا حاجة لأي إجراء آخر.</i>"
        )
    else:
        msg_text = (
            f"🟡 <b>Deposit Instructions - Binance Pay</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💰 <b>Amount to send:</b>\n"
            f"<code>${unique_amount:.4f}</code>\n"
            f"☝️ <b>EXACTLY this amount!</b>\n\n"
            f"📬 <b>Wallet:</b>\n<code>{wallet}</code>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"⚠️ <b>Important:</b>\n"
            f"✅ Send exactly: <code>${unique_amount:.4f}</code>\n"
            f"❌ Don't send ${base_amount:.2f} (base amount)\n"
            f"❌ Don't change the number\n\n"
            f"⏰ <b>Valid for:</b> 30 minutes\n\n"
            f"✨ <b>Balance added automatically once received.</b>\n"
            f"<i>No further action needed.</i>"
        )

    bot.send_message(uid, msg_text, parse_mode="HTML")
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)
    bot.clear_step_handler_by_chat_id(chat_id=uid)

    wallet = get_setting('wallet_address')

    if l == 'ar':
        msg_text = (
            f"🟡 <b>الإيداع عبر Binance Pay</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💡 أرسل المبلغ بالدولار الذي تريد إيداعه:\n\n"
            f"📌 أمثلة: 5 / 10 / 25 / 50\n"
            f"⚠️ الحد الأدنى: <b>$1</b>"
        )
    else:
        msg_text = (
            f"🟡 <b>Deposit via Binance Pay</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💡 Send the USD amount you want to deposit:\n\n"
            f"📌 Examples: 5 / 10 / 25 / 50\n"
            f"⚠️ Minimum: <b>$1</b>"
        )

    msg = bot.send_message(uid, msg_text, parse_mode="HTML")
    bot.register_next_step_handler(msg, ask_binance_deposit_amount)


@bot.callback_query_handler(func=lambda call: call.data.startswith("binance_check_"))
def binance_check_payment(call):
    """يفحص Binance Pay فوراً لما المستخدم يضغط الزر"""
    bot.answer_callback_query(call.id, "⏳ جاري الفحص..." if get_lang(call.from_user.id) == 'ar' else "⏳ Checking...")
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)
    
    # نرسل رسالة "جاري الفحص"
    checking_msg = bot.send_message(
        uid,
        bil(uid, "🔍 <b>جاري فحص Binance Pay...</b>\nانتظر ثوانٍ.", "🔍 <b>Checking Binance Pay...</b>\nPlease wait."),
        parse_mode="HTML"
    )
    
    found = False
    try:
        pay_response = execute_binance_call(
            lambda c: c.get_pay_trade_history(),
            max_retries=5
        )
        
        if pay_response:
            pay_history = pay_response.get('data', [])
            uid_str = str(uid)
            current_time_ms = int(time.time() * 1000)
            cutoff_ms = current_time_ms - (24 * 60 * 60 * 1000)
            
            import re as re_module
            
            for tx in pay_history[:50]:
                try:
                    # نجلب الـ Order ID
                    tx_id = str(
                        tx.get('orderId') or tx.get('transactionId') or
                        tx.get('merchantTradeNo') or ''
                    )
                    if not tx_id:
                        continue
                    
                    # نفحص الوقت
                    tx_time = int(tx.get('transactionTime') or tx.get('createTime') or 0)
                    if tx_time and tx_time < cutoff_ms:
                        continue
                    
                    tx_id_norm = normalize_tx_id(tx_id)
                    
                    # لو مستخدم مسبقاً
                    if db.used_transactions.find_one({'transaction_id': tx_id_norm}):
                        continue
                    
                    # المبلغ
                    amount = float(tx.get('amount') or tx.get('totalFee') or 0)
                    if amount <= 0:
                        continue
                    
                    # نقرأ كل الحقول الممكنة للـ remark
                    remark = str(
                        tx.get('remarks') or tx.get('remark') or
                        tx.get('memo') or tx.get('note') or
                        tx.get('orderInfo') or ''
                    ).strip()
                    
                    # نشوف لو الـ uid موجود في الـ remark
                    if uid_str not in remark:
                        continue
                    
                    # تأكيد: الـ uid موجود كرقم في الـ remark
                    if not re_module.search(r'\b' + re_module.escape(uid_str) + r'\b', remark):
                        continue
                    
                    # ✅ وجدنا الحوالة!
                    credit_user(uid, amount, tx_id_norm, l, "Binance Pay")
                    found = True
                    
                    try:
                        bot.delete_message(uid, checking_msg.message_id)
                    except: pass
                    break
                    
                except Exception:
                    continue
    
    except Exception as e:
        logger.error(f"binance_check_payment error: {e}")
    
    if not found:
        try:
            bot.edit_message_text(
                bil(uid,
                    "❌ <b>لم نجد حوالة بعد!</b>\n\n"
                    "💡 تأكد من:\n"
                    "• وضع رقمك في الملاحظة بالضبط\n"
                    f"• الرقم: <code>{uid}</code>\n"
                    "• إتمام التحويل بالكامل\n\n"
                    "🔄 حاول مرة ثانية بعد دقيقة.",
                    "❌ <b>No transaction found yet!</b>\n\n"
                    "💡 Make sure:\n"
                    "• Your ID is in the Remark exactly\n"
                    f"• Your ID: <code>{uid}</code>\n"
                    "• Transfer is completed\n\n"
                    "🔄 Try again in a minute."
                ),
                uid, checking_msg.message_id,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton(
                        "🔄 أعد المحاولة" if l == 'ar' else "🔄 Try Again",
                        callback_data=f"binance_check_{uid}"
                    )
                )
            )
        except: pass

# ============================================================
# 🛡 نظام المبلغ الفريد (يحمي من بوتات النصب اللي تراقب blockchain)
# ============================================================
# الفكرة: المستخدم يكتب المبلغ، البوت يضيف عشر سنتين عشوائيين
# مثلاً: المستخدم يبي $5 → البوت يطلب $5.0023 بالضبط
# هذا يربط الحوالة بالمستخدم بطريقة لا يقدر النصاب يكتشفها
# ============================================================

def generate_unique_amount_for_user(base_amount_usd, uid, coin):
    """
    🛡 يولّد مبلغ فريد عشوائي 100% لكل عملية إيداع.
    
    النطاق الجديد: 4-9 خانات عشرية = ملايين الاحتمالات
    مستحيل التكرار بين أي مستخدمين.
    
    أمثلة:
    - $5 → $5.001847
    - $5 → $5.009274
    - $5 → $5.003521
    """
    base = float(base_amount_usd)
    
    # نحاول 200 مرة لإيجاد مبلغ فريد
    for attempt in range(200):
        # 🎲 6 خانات عشرية عشوائية = مليون احتمال!
        # نطاق: 0.000100 إلى 0.009999 (لا يزال أقل من سنت!)
        random_micro = random.randint(100, 9999)
        extra = random_micro / 1000000.0
        unique_amount = round(base + extra, 6)
        
        # 🛡 نفحص ما في pending مشابه
        existing = db.pending_deposits.find_one({
            'unique_amount_usd': unique_amount,
            'coin': coin,
            'status': 'pending',
            'expires_at': {'$gt': int(time.time())}
        })
        
        if not existing:
            # ✅ مبلغ فريد!
            return unique_amount
    
    # احتياط: نضيف 7 خانات (10 مليون احتمال)
    for attempt in range(200):
        random_nano = random.randint(1000, 99999)
        extra = random_nano / 10000000.0
        unique_amount = round(base + extra, 7)
        
        existing = db.pending_deposits.find_one({
            'unique_amount_usd': unique_amount,
            'coin': coin,
            'status': 'pending'
        })
        
        if not existing:
            return unique_amount
    
    # احتياط أخير
    return round(base + random.uniform(0.000001, 0.009999), 6)


def register_pending_deposit(uid, base_amount_usd, unique_amount_usd, coin):
    """
    يسجّل عملية إيداع متوقعة - عشان نطابقها مع الـ tx لاحقاً.
    """
    try:
        # نحذف أي إيداعات سابقة معلقة لنفس المستخدم بنفس العملة (نظافة)
        db.pending_deposits.delete_many({
            'user_id': uid,
            'coin': coin,
            'status': 'pending'
        })
        
        pending_id = f"PD{uid}{int(time.time())}{random.randint(100, 999)}"
        record = {
            'pending_id': pending_id,
            'user_id': uid,
            'base_amount_usd': float(base_amount_usd),
            'unique_amount_usd': float(unique_amount_usd),
            'coin': coin,
            'status': 'pending',
            'created_at': int(time.time()),
            'expires_at': int(time.time()) + (60 * 30)  # 30 دقيقة
        }
        db.pending_deposits.insert_one(record)
        return record
    except Exception as e:
        logger.error(f"Error registering pending deposit: {e}")
        return None


def find_pending_deposit_for_amount(amount_usd, coin, tolerance=0.0001):
    """
    🛡 يبحث عن إيداع معلق مطابق للمبلغ (مع تسامح صغير).
    
    يستخدم تسامح صغير جداً (0.0001$) للتعامل مع تذبذب أسعار العملات.
    
    يرجع المستخدم الذي سجل الإيداع.
    """
    try:
        amount_usd = float(amount_usd)
        records = list(db.pending_deposits.find({
            'coin': coin,
            'status': 'pending',
            'unique_amount_usd': {
                '$gte': amount_usd - tolerance,
                '$lte': amount_usd + tolerance
            },
            'expires_at': {'$gt': int(time.time())}
        }).sort('created_at', 1))
        
        # نأخذ الأقدم (أول من سجل)
        return records[0] if records else None
    except Exception as e:
        logger.error(f"Error finding pending deposit: {e}")
        return None


# ============================================================
# 🤖 نظام Auto-Detect: يفحص blockchain تلقائياً ويضيف الرصيد
# ============================================================

def auto_credit_from_pending(pending, tx_id_for_record, method_label):
    """
    يضيف رصيد للمستخدم تلقائياً من pending deposit (بدون ما يرسل tx_id).
    """
    try:
        uid = pending['user_id']
        base_amount = float(pending.get('base_amount_usd', 0))
        unique_amount = float(pending.get('unique_amount_usd', 0))
        pending_id = pending.get('pending_id', '')
        
        # نعلم الـ pending كـ completed
        result = db.pending_deposits.update_one(
            {'pending_id': pending_id, 'status': 'pending'},
            {'$set': {'status': 'completed', 'completed_at': int(time.time()), 'tx_id_detected': tx_id_for_record}}
        )
        
        if result.modified_count == 0:
            # شخص آخر استخدمها قبلنا (race condition)
            return False
        
        # 🎉 رسالة تأكيد للمستخدم قبل ما نضيف الرصيد
        try:
            lang = get_lang(uid)
            # نعرض الـ tx_id مختصراً
            tx_short = tx_id_for_record[:20] + "..." if len(tx_id_for_record) > 20 else tx_id_for_record
            if lang == 'ar':
                msg = (
                    f"✅ <b>تم استلام إيداعك تلقائياً!</b> 🎉\n\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"💰 <b>المبلغ:</b> <b>${base_amount:.2f}</b>\n"
                    f"💳 <b>الطريقة:</b> {method_label}\n"
                    f"🆔 <b>رقم العملية:</b>\n<code>{tx_id_for_record}</code>\n"
                    f"━━━━━━━━━━━━━━\n\n"
                    f"💼 <i>تم إضافة الرصيد لحسابك.</i>"
                )
            else:
                msg = (
                    f"✅ <b>Deposit auto-detected!</b> 🎉\n\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"💰 <b>Amount:</b> <b>${base_amount:.2f}</b>\n"
                    f"💳 <b>Method:</b> {method_label}\n"
                    f"🆔 <b>Transaction ID:</b>\n<code>{tx_id_for_record}</code>\n"
                    f"━━━━━━━━━━━━━━\n\n"
                    f"💼 <i>Balance added to your account.</i>"
                )
            bot.send_message(uid, msg, parse_mode="HTML")
        except Exception: pass
        
        # نضيف الرصيد
        credit_user(uid, base_amount, tx_id_for_record, get_lang(uid), method_label)
        return True
    except Exception as e:
        logger.error(f"Error in auto_credit: {e}")
        return False


def check_ltc_blockchain_auto():
    """
    🔍 يفحص LTC blockchain ويلقى الحوالات اللي وصلت
    ويربطها بـ pending_deposits تلقائياً.
    """
    try:
        wallet_address = get_setting('ltc_address')
        if not wallet_address or wallet_address == "Not Set":
            return
        
        # نجيب الـ pending deposits لـ LTC
        pending_count = db.pending_deposits.count_documents({
            'coin': 'LTC',
            'status': 'pending',
            'expires_at': {'$gt': int(time.time())}
        })
        if pending_count == 0:
            return  # ما فيه pending - نوفر الـ API calls
        
        # نجيب آخر transactions من litecoinspace
        url = f"https://litecoinspace.org/api/address/{wallet_address}/txs"
        try:
            res = requests.get(url, timeout=10)
            if res.status_code != 200:
                return
            txs = res.json()
        except Exception:
            return
        
        # نجيب سعر LTC
        ltc_price = get_ltc_price_usd()
        if not ltc_price or ltc_price < 10:
            return
        
        current_time_ms = int(time.time() * 1000)
        cutoff_time_ms = current_time_ms - (45 * 60 * 1000)  # آخر 45 دقيقة
        
        for tx in txs[:30]:  # آخر 30 معاملة فقط
            try:
                tx_id = tx.get('txid', '')
                if not tx_id:
                    continue
                
                # شيك التاريخ - نتجاهل المعاملات القديمة
                tx_time = (tx.get('status', {}).get('block_time', 0) or 0) * 1000
                if tx_time and tx_time < cutoff_time_ms:
                    continue
                
                tx_id_normalized = normalize_tx_id(tx_id)
                
                # شيك إن المعاملة ما تم معالجتها
                if db.used_transactions.find_one({'transaction_id': tx_id_normalized}):
                    continue
                
                # نحسب الـ amount المُستلم
                received_ltc = 0.0
                for vout in tx.get('vout', []):
                    if vout.get('scriptpubkey_address') == wallet_address:
                        received_ltc += vout.get('value', 0) / 100000000  # satoshi → LTC
                
                if received_ltc <= 0:
                    continue
                
                usd_amount = round(received_ltc * ltc_price, 6)
                
                # نشوف لو في pending مطابق
                pending = find_pending_deposit_for_amount(usd_amount, 'LTC', tolerance=0.001)
                if pending:
                    success = auto_credit_from_pending(pending, tx_id_normalized, "Litecoin (LTC) Auto")
                    if success:
                        logger.info(f"✅ AUTO-CREDITED LTC: user {pending['user_id']} → ${pending['base_amount_usd']:.2f} (tx: {tx_id[:16]})")
            except Exception as tx_err:
                logger.debug(f"Skip tx: {tx_err}")
                continue
    except Exception as e:
        logger.error(f"check_ltc_blockchain_auto error: {e}")


def check_ton_blockchain_auto():
    """🔍 يفحص TON blockchain تلقائياً"""
    try:
        wallet_address = get_setting('ton_address')
        if not wallet_address or wallet_address == "Not Set":
            return
        
        pending_count = db.pending_deposits.count_documents({
            'coin': 'TON',
            'status': 'pending',
            'expires_at': {'$gt': int(time.time())}
        })
        if pending_count == 0:
            return
        
        # TONCenter API
        url = f"https://toncenter.com/api/v2/getTransactions?address={wallet_address}&limit=30"
        try:
            res = requests.get(url, timeout=10)
            if res.status_code != 200:
                return
            data = res.json()
            if not data.get('ok'):
                return
            txs = data.get('result', [])
        except Exception:
            return
        
        ton_price = get_ton_price_usd()
        if not ton_price or ton_price < 0.5:
            return
        
        current_time = int(time.time())
        cutoff_time = current_time - (45 * 60)  # 45 دقيقة
        
        for tx in txs:
            try:
                tx_hash = tx.get('transaction_id', {}).get('hash', '')
                if not tx_hash:
                    continue
                
                tx_time = tx.get('utime', 0)
                if tx_time and tx_time < cutoff_time:
                    continue
                
                tx_id_normalized = normalize_tx_id(tx_hash)
                
                if db.used_transactions.find_one({'transaction_id': tx_id_normalized}):
                    continue
                
                # نحسب الـ TON المُستلم
                in_msg = tx.get('in_msg', {})
                value_nanoton = int(in_msg.get('value', 0))
                if value_nanoton <= 0:
                    continue
                
                received_ton = value_nanoton / 1_000_000_000
                usd_amount = round(received_ton * ton_price, 6)
                
                # نشوف pending مطابق
                pending = find_pending_deposit_for_amount(usd_amount, 'TON', tolerance=0.001)
                if pending:
                    success = auto_credit_from_pending(pending, tx_id_normalized, "Toncoin (TON) Auto")
                    if success:
                        logger.info(f"✅ AUTO-CREDITED TON: user {pending['user_id']} → ${pending['base_amount_usd']:.2f} (tx: {tx_hash[:16]})")
            except Exception as tx_err:
                logger.debug(f"Skip TON tx: {tx_err}")
                continue
    except Exception as e:
        logger.error(f"check_ton_blockchain_auto error: {e}")


def check_usdt_blockchain_auto():
    """🔍 يفحص USDT (TRC-20 و BEP-20) تلقائياً عبر Binance API"""
    try:
        # نشيك إن في pending
        pending_count = db.pending_deposits.count_documents({
            'coin': {'$in': ['USDT', 'USDT_BEP20']},
            'status': 'pending',
            'expires_at': {'$gt': int(time.time())}
        })
        if pending_count == 0:
            return
        
        # نجلب آخر deposits من Binance
        res = execute_binance_call(
            lambda c: c.get_deposit_history(coin='USDT'),
            max_retries=3
        )
        
        if res is None:
            return
        
        current_time_ms = int(time.time() * 1000)
        cutoff_time_ms = current_time_ms - (45 * 60 * 1000)
        
        for d in res[:30]:
            try:
                tx_id = str(d.get('txId', ''))
                if not tx_id:
                    continue
                
                tx_time = int(d.get('insertTime', 0))
                if tx_time and tx_time < cutoff_time_ms:
                    continue
                
                # فقط الحوالات المؤكدة
                if int(d.get('status', -1)) != 1:
                    continue
                
                tx_id_normalized = normalize_tx_id(tx_id)
                
                if db.used_transactions.find_one({'transaction_id': tx_id_normalized}):
                    continue
                
                amt = float(d.get('amount', 0))
                if amt <= 0:
                    continue
                
                # نطابق مع pending (TRC20 أو BEP20)
                network = d.get('network', '').upper()
                coins_to_check = []
                if 'TRX' in network or 'TRC' in network:
                    coins_to_check = ['USDT']
                elif 'BSC' in network or 'BEP' in network:
                    coins_to_check = ['USDT_BEP20']
                else:
                    coins_to_check = ['USDT', 'USDT_BEP20']
                
                for coin_check in coins_to_check:
                    pending = find_pending_deposit_for_amount(amt, coin_check, tolerance=0.001)
                    if pending:
                        success = auto_credit_from_pending(pending, tx_id_normalized, f"USDT ({coin_check}) Auto")
                        if success:
                            logger.info(f"✅ AUTO-CREDITED USDT: user {pending['user_id']} → ${pending['base_amount_usd']:.2f}")
                        break
            except Exception as tx_err:
                logger.debug(f"Skip USDT tx: {tx_err}")
                continue
    except Exception as e:
        logger.error(f"check_usdt_blockchain_auto error: {e}")


def _binance_pay_request():
    """
    يجلب آخر معاملات Binance Pay باستخدام REST API مباشرة.
    يستخدم /sapi/v1/pay/transactions endpoint.
    يرجع list من المعاملات أو None لو فشل.
    """
    import hmac, hashlib, urllib.parse

    api_key = BINANCE_API_KEY
    api_secret = BINANCE_API_SECRET
    
    if not api_key or not api_secret:
        return None
    
    try:
        timestamp = int(time.time() * 1000)
        params = {
            'timestamp': timestamp,
            'limit': 100,
            'recvWindow': 60000
        }
        
        # نعمل signature
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        params['signature'] = signature
        
        headers = {'X-MBX-APIKEY': api_key}
        
        url = "https://api.binance.com/sapi/v1/pay/transactions"
        
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'SUCCESS' or data.get('code') == '000000':
                return data.get('data', [])
            else:
                logger.warning(f"Binance Pay API: {data.get('code')} - {data.get('errorMessage', data.get('msg', ''))}")
                return None
        else:
            logger.warning(f"Binance Pay API HTTP {resp.status_code}: {resp.text[:100]}")
            return None
    except Exception as e:
        logger.error(f"_binance_pay_request error: {e}")
        return None


def check_binance_pay_auto():
    """
    🔍 يفحص Binance Pay كل 10 ثواني بالمبلغ الفريد.
    المستخدم يحوّل المبلغ الفريد → البوت يطابقه مع pending_deposits → يضيف الرصيد.
    """
    try:
        if not BINANCE_API_KEY or not BINANCE_API_SECRET:
            return

        transactions = _binance_pay_request()
        
        if transactions is None:
            return
        
        if not transactions:
            return
        
        # نشيك إن في pending لـ BINANCE قبل ما نكلّم API
        pending_count = db.pending_deposits.count_documents({
            'coin': 'BINANCE',
            'status': 'pending',
            'expires_at': {'$gt': int(time.time())}
        })
        if pending_count == 0:
            return

        current_time_ms = int(time.time() * 1000)
        cutoff_ms = current_time_ms - (2 * 60 * 60 * 1000)  # آخر ساعتين

        for tx in transactions:
            try:
                tx_id = str(
                    tx.get('transactionId') or tx.get('orderId') or tx.get('bizOrderNo') or ''
                )
                if not tx_id:
                    continue

                tx_time = int(tx.get('transactionTime') or tx.get('createTime') or 0)
                if tx_time and tx_time < cutoff_ms:
                    continue

                tx_id_norm = normalize_tx_id(tx_id)

                if db.used_transactions.find_one({'transaction_id': tx_id_norm}):
                    continue

                amount = float(tx.get('amount') or tx.get('totalFee') or 0)
                if amount <= 0:
                    continue

                # 🛡 نطابق بالمبلغ الفريد (tolerance صغير جداً = 0.0001)
                pending = find_pending_deposit_for_amount(amount, 'BINANCE', tolerance=0.0001)
                if not pending:
                    continue

                uid = pending['user_id']
                base_amount = float(pending.get('base_amount_usd', amount))

                user = db.users.find_one({'user_id': uid})
                if not user or user.get('is_banned') == 1:
                    continue

                logger.info(f"✅ Binance Pay MATCH: user {uid} amount=${amount:.4f} → base=${base_amount:.2f}")
                success = auto_credit_from_pending(pending, tx_id_norm, "Binance Pay")
                if success:
                    logger.info(f"✅ Binance Pay credited: user {uid} ${base_amount:.2f}")

            except Exception as tx_err:
                logger.debug(f"Binance Pay tx error: {tx_err}")
                continue

    except Exception as e:
        logger.error(f"check_binance_pay_auto error: {e}")


def auto_deposit_monitor_thread():
    """Thread خلفي يفحص كل طرق الإيداع تلقائياً كل 10 ثواني"""
    time.sleep(30)  # ننتظر شوي قبل البداية
    while True:
        try:
            check_ltc_blockchain_auto()
            time.sleep(1)
            check_ton_blockchain_auto()
            time.sleep(1)
            check_usdt_blockchain_auto()
            time.sleep(1)
            check_binance_pay_auto()
            time.sleep(10)  # كل 10 ثواني
        except Exception as e:
            logger.error(f"Auto deposit monitor error: {e}")
            time.sleep(30)


# نشغل الـ monitor في الخلفية
threading.Thread(target=auto_deposit_monitor_thread, daemon=True).start()


def mark_pending_deposit_used(pending_id):
    """يعلّم إيداع معلق كـ مستخدم"""
    try:
        db.pending_deposits.update_one(
            {'pending_id': pending_id},
            {'$set': {'status': 'completed', 'completed_at': int(time.time())}}
        )
    except Exception:
        pass


def cleanup_expired_pending_deposits():
    """ينظف الإيداعات المعلقة المنتهية"""
    try:
        current_time = int(time.time())
        result = db.pending_deposits.delete_many({
            '$or': [
                {'expires_at': {'$lt': current_time}},
                {'status': 'completed', 'completed_at': {'$lt': current_time - 86400}}  # نحذف completed بعد يوم
            ]
        })
        if result.deleted_count > 0:
            logger.info(f"🧹 تم تنظيف {result.deleted_count} إيداع معلق منتهي.")
    except Exception as e:
        logger.error(f"Cleanup expired deposits error: {e}")


def reject_wrong_amount_deposit(uid, coin, usd_amount_precise, tx_id, coin_label="العملة"):
    """
    ❌ يرفض إيداع بمبلغ خاطئ + يرسل إشعار للأدمن لاسترداده يدوياً.
    """
    try:
        # نشيك لو فيه pending للمستخدم نفسه (مشكلة في الدقة)
        user_pending = db.pending_deposits.find_one({
            'user_id': uid,
            'coin': coin,
            'status': 'pending',
            'expires_at': {'$gt': int(time.time())}
        })
        
        expected_text = ""
        if user_pending:
            expected = user_pending.get('unique_amount_usd', 0)
            diff = abs(usd_amount_precise - expected)
            expected_text = (
                f"\n\n📊 <b>المبلغ المطلوب:</b> <code>${expected:.6f}</code>\n"
                f"📊 <b>المبلغ المستلم:</b> <code>${usd_amount_precise:.6f}</code>\n"
                f"📊 <b>الفرق:</b> <code>${diff:.6f}</code>"
            )
        
        # 🚨 إشعار للأدمن للمراجعة اليدوية
        try:
            u_data = db.users.find_one({'user_id': uid}) or {}
            uname = u_data.get('username', 'unknown')
            notify_admins(
                f"⚠️ <b>إيداع بمبلغ خاطئ - يحتاج مراجعة يدوية!</b>\n\n"
                f"👤 المستخدم: <code>{uid}</code> @{uname}\n"
                f"💳 العملة: <b>{coin_label}</b>\n"
                f"💰 المبلغ المستلم: <b>${usd_amount_precise:.6f}</b>"
                f"{expected_text}\n\n"
                f"🆔 الهاش:\n<code>{tx_id}</code>\n\n"
                f"💡 المستخدم حوّل مبلغ مختلف عن المطلوب.\n"
                f"يحتاج مراجعة يدوية لاسترداد المبلغ أو إضافته للرصيد."
            )
        except Exception: pass
        
        # نسجل في DB
        try:
            db.wrong_amount_deposits.insert_one({
                'user_id': uid,
                'username': uname if 'uname' in locals() else 'unknown',
                'coin': coin,
                'received_amount': usd_amount_precise,
                'expected_amount': user_pending.get('unique_amount_usd', 0) if user_pending else 0,
                'tx_id': tx_id,
                'timestamp': int(time.time()),
                'status': 'pending_admin_review'
            })
        except Exception: pass
        
        # رسالة للمستخدم - بدون ذكر الإدارة
        if get_lang(uid) == 'en':
            bot.send_message(
                uid,
                f"❌ <b>Incorrect amount transferred!</b>\n\n"
                f"💡 You sent a different amount than requested."
                f"{expected_text}\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"⚠️ <b>For next deposit:</b>\n"
                f"• Send <b>exactly</b> the amount the bot gives you\n"
                f"• Do not round the number\n"
                f"• Do not add or subtract even $0.0001\n\n"
                f"🔄 Start a new deposit from the menu.",
                parse_mode="HTML"
            )
        else:
            bot.send_message(
                uid,
                f"❌ <b>المبلغ المحوّل غير صحيح!</b>\n\n"
                f"💡 لقد أرسلت مبلغ مختلف عن المبلغ المطلوب."
                f"{expected_text}\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"⚠️ <b>عند الإيداع التالي:</b>\n"
                f"• حوّل المبلغ <b>بالضبط</b> كما يعطيك إياه البوت\n"
                f"• لا تقرّب الرقم\n"
                f"• لا تنقص أو تزيد ولا حتى $0.0001\n\n"
                f"🔄 ابدأ عملية إيداع جديدة من القائمة.",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Error in reject_wrong_amount: {e}")


def punish_steal_attempt_thief_only(thief_uid, tx_id_clean, pending_owner_uid, attempted_amount):
    """
    🚨 يعاقب فقط بوت النصاب (المستخدم الأصلي ما يتلمس).
    
    لما بوت النصاب يحاول يستخدم حوالة مسجّلة لمستخدم آخر:
    1. حظر بوت النصاب
    2. سجل في theft_attempts
    3. إشعار قوي للأدمن
    """
    try:
        # حظر النصاب فقط
        db.users.update_one({'user_id': thief_uid}, {'$set': {'is_banned': 1}})
        
        # تسجيل
        thief_data = db.users.find_one({'user_id': thief_uid}) or {}
        owner_data = db.users.find_one({'user_id': pending_owner_uid}) or {}
        
        try:
            db.theft_attempts.insert_one({
                'transaction_id': tx_id_clean,
                'pending_owner_id': pending_owner_uid,
                'pending_owner_username': owner_data.get('username', 'unknown'),
                'thief_user_id': thief_uid,
                'thief_username': thief_data.get('username', 'unknown'),
                'attempted_amount': float(attempted_amount),
                'attack_type': 'monitoring_bot_steal',
                'timestamp': int(time.time()),
                'status': 'auto_handled'
            })
        except Exception: pass
        
        # إشعار قوي للأدمن
        try:
            thief_username = thief_data.get('username', 'unknown')
            owner_username = owner_data.get('username', 'unknown')
            
            notify_admins(
                f"🚨 <b>هجوم بوت نصاب تم إيقافه!</b>\n\n"
                f"⚡ <b>بوت نصاب حاول سرقة حوالة من blockchain</b>\n"
                f"🛡 <b>تم حظر النصاب وحماية المستخدم الأصلي!</b>\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🤖 <b>بوت النصاب (محظور):</b>\n"
                f"   • ID: <code>{thief_uid}</code>\n"
                f"   • Username: @{thief_username}\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"😇 <b>المستخدم الأصلي (آمن):</b>\n"
                f"   • ID: <code>{pending_owner_uid}</code>\n"
                f"   • Username: @{owner_username}\n"
                f"   • <i>المستخدم الأصلي يقدر يكمل إيداعه بشكل طبيعي</i>\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"💰 المبلغ: <b>${attempted_amount:.4f}</b>\n"
                f"🆔 الهاش: <code>{tx_id_clean[:40]}...</code>"
            )
        except Exception: pass
        
        # رسالة للنصاب (بدون كشف وجود نظام حماية)
        try:
            bot.send_message(
                thief_uid,
                "❌ <b>تم حظر حسابك!</b>\n\n"
                "🚫 السبب: محاولة استخدام حوالة غير صحيحة.\n\n"
                "⚠️ <i>للاستفسار، تواصل مع الإدارة.</i>",
                parse_mode="HTML"
            )
        except Exception: pass
        
        logger.warning(
            f"🚨 MONITORING BOT BLOCKED: "
            f"thief {thief_uid} tried to steal tx {tx_id_clean[:30]} "
            f"that belongs to {pending_owner_uid}"
        )
        return True
    except Exception as e:
        logger.error(f"Error in punish_steal_attempt: {e}")
        return False


def is_amount_protection_enabled():
    """يشيك إن نظام المبلغ الفريد مفعّل"""
    return get_setting('amount_protection', 'on') == 'on'


# ============================================================
# 🛡 معالج بداية الإيداع (يطلب المبلغ ثم يعطي مبلغ فريد)
# ============================================================

# نخزّن المستخدمين اللي بدأوا إيداع بمبلغ فريد
PENDING_DEPOSIT_USERS = {}  # uid -> coin


def start_amount_protected_deposit(call, coin):
    """
    🛡 يبدأ عملية إيداع محمية بمبلغ فريد.
    """
    uid = call.from_user.id
    l = get_lang(uid)
    
    # رسالة طلب المبلغ
    coin_name = {
        'USDT': 'USDT (TRC-20)',
        'USDT_BEP20': 'USDT (BEP-20)',
        'TON': 'Toncoin (TON)',
        'LTC': 'Litecoin (LTC)'
    }.get(coin, coin)
    
    if l == 'ar':
        msg_text = (
            f"💵 <b>الإيداع عبر {coin_name}</b>\n\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"💡 أرسل المبلغ بالدولار الذي تريد إيداعه:\n\n"
            f"📌 <i>أمثلة: 5 / 10 / 25 / 50</i>\n"
            f"⚠️ الحد الأدنى: <b>$1</b>"
        )
    else:
        msg_text = (
            f"💵 <b>Deposit via {coin_name}</b>\n\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"💡 Send the USD amount you want to deposit:\n\n"
            f"📌 <i>Examples: 5 / 10 / 25 / 50</i>\n"
            f"⚠️ Minimum: <b>$1</b>"
        )
    
    msg = bot.send_message(uid, msg_text, parse_mode="HTML")
    bot.register_next_step_handler(msg, ask_deposit_amount, coin)


@safe_next_step
def ask_deposit_amount(message, coin):
    """يستلم المبلغ من المستخدم ويعطيه المبلغ الفريد"""
    uid = message.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)
    
    if not message.text:
        bot.send_message(uid, bil(uid, "❌ يجب إرسال رقم.", "❌ Please send a number."), parse_mode="HTML")
        return
    
    text = message.text.strip()
    
    # 🛡 فحص أوامر البوت - نوقف بصمت
    if text.startswith('/'):
        return
    
    # إلغاء
    if text.lower() in ['الغاء', 'cancel', 'إلغاء', 'اغلاق']:
        bot.send_message(uid, "❌ تم الإلغاء.")
        return
    
    # parsing المبلغ
    try:
        base_amount = float(text.replace(',', '.').replace('$', ''))
    except ValueError:
        bot.send_message(uid, bil(uid, "❌ أرسل رقم فقط.\nمثال: 5 أو 10.5", "❌ Send numbers only.\nExample: 5 or 10.5"), parse_mode="HTML")
        return
    
    # validation
    if base_amount < 1:
        bot.send_message(uid, bil(uid, "❌ الحد الأدنى للإيداع: <b>$1</b>", "❌ Minimum deposit: <b>$1</b>"), parse_mode="HTML")
        return
    
    if base_amount > 10000:
        bot.send_message(uid, bil(uid, "❌ المبلغ كبير جداً. الحد الأقصى: <b>$10,000</b>\n\nللإيداعات الكبيرة، تواصل مع الإدارة.", "❌ Amount too large. Max: <b>$10,000</b>\n\nFor large deposits, contact admin."), parse_mode="HTML")
        return
    
    # نولّد المبلغ الفريد
    unique_amount = generate_unique_amount_for_user(base_amount, uid, coin)
    
    # نسجّل في DB
    pending = register_pending_deposit(uid, base_amount, unique_amount, coin)
    if not pending:
        bot.send_message(uid, bil(uid, "❌ حدث خطأ. حاول مرة ثانية.", "❌ An error occurred. Please try again."), parse_mode="HTML")
        return
    
    # نجلب عنوان المحفظة
    if coin == "USDT": db_key = "usdt_address"
    elif coin == "USDT_BEP20": db_key = "usdt_bep20_address"
    elif coin == "TON": db_key = "ton_address"
    else: db_key = "ltc_address"
    wallet = get_setting(db_key)
    
    coin_name = {
        'USDT': 'USDT (TRC-20)',
        'USDT_BEP20': 'USDT (BEP-20)',
        'TON': 'Toncoin (TON)',
        'LTC': 'Litecoin (LTC)'
    }.get(coin, coin)
    
    # نحسب المبلغ بعملة الكريبتو
    crypto_amount_text = ""
    try:
        if coin == 'LTC':
            ltc_price = get_ltc_price_usd()
            crypto_amount = unique_amount / ltc_price
            crypto_amount_text = f"\n💰 <b>المبلغ بـ LTC:</b> <code>{crypto_amount:.8f}</code> LTC"
        elif coin == 'TON':
            ton_price = get_ton_price_usd()
            crypto_amount = unique_amount / ton_price
            crypto_amount_text = f"\n💰 <b>المبلغ بـ TON:</b> <code>{crypto_amount:.6f}</code> TON"
        elif coin in ['USDT', 'USDT_BEP20']:
            crypto_amount_text = f"\n💰 <b>المبلغ بـ USDT:</b> <code>{unique_amount:.4f}</code> USDT"
    except Exception: pass
    
    # رسالة التعليمات
    if l == 'ar':
        msg_text = (
            f"💵 <b>تعليمات الإيداع</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💰 <b>المبلغ المطلوب تحويله:</b>\n"
            f"<code>${unique_amount:.6f}</code>\n"
            f"☝️ <b>بالضبط هذا الرقم!</b>"
            f"{crypto_amount_text}\n\n"
            f"📬 <b>العنوان:</b>\n<code>{wallet}</code>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"⚠️ <b>تنبيهات هامة:</b>\n\n"
            f"✅ حوّل بالضبط: <code>${unique_amount:.6f}</code>\n"
            f"❌ <b>لا</b> تحوّل ${base_amount:.2f}\n"
            f"❌ <b>لا</b> تقرّب الرقم\n"
            f"❌ <b>لا</b> تنقص ولا تزيد ولا حتى $0.0001\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"⏰ <b>صلاحية الطلب:</b> 30 دقيقة\n\n"
            f"✨ <b>بعد التحويل بالمبلغ الصحيح، البوت سيضيف الرصيد تلقائياً خلال 1-3 دقائق.</b>\n\n"
            f"💡 <i>لا حاجة لإرسال أي شيء - فقط حوّل وانتظر.</i>"
        )
    else:
        msg_text = (
            f"💵 <b>Deposit Instructions</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💰 <b>Amount to send:</b>\n"
            f"<code>${unique_amount:.6f}</code>\n"
            f"☝️ <b>EXACTLY this amount!</b>"
            f"{crypto_amount_text}\n\n"
            f"📬 <b>Address:</b>\n<code>{wallet}</code>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"⚠️ <b>Important:</b>\n\n"
            f"✅ Send EXACTLY: <code>${unique_amount:.6f}</code>\n"
            f"❌ <b>DON'T</b> send ${base_amount:.2f}\n"
            f"❌ <b>DON'T</b> round the number\n"
            f"❌ <b>DON'T</b> change even by $0.0001\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"⏰ <b>Valid for:</b> 30 minutes\n\n"
            f"✨ <b>After sending the EXACT amount, balance will be added AUTOMATICALLY within 1-3 minutes.</b>\n\n"
            f"💡 <i>No need to send anything - just transfer and wait.</i>"
        )
    
    # رسالة الإيداع - بدون next_step (الكشف تلقائي)
    bot.send_message(uid, msg_text, parse_mode="HTML")
    
    # نسجّل المستخدم في PENDING
    PENDING_DEPOSIT_USERS[uid] = coin


@bot.callback_query_handler(func=lambda call: call.data.startswith("dep_crypto_"))
def dep_crypto_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    coin = call.data.replace('dep_crypto_', '')
    bot.clear_step_handler_by_chat_id(chat_id=uid)
    
    # 🛡 لو الحماية مفعّلة، نستخدم النظام الجديد (مبلغ فريد)
    if is_amount_protection_enabled():
        start_amount_protected_deposit(call, coin)
        return
    
    # النظام القديم (لو الحماية معطّلة)
    l = get_lang(uid)
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

@safe_next_step
def verify_binance_pay(message, lang):
    uid = message.from_user.id
    if is_user_banned(uid): return
    if not message.text:
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML"); return

    tx_id = message.text.strip()
    
    # 🛡 رفض أوامر البوت والرسائل العادية (مو tx_id فعلاً)
    if tx_id.startswith('/') or tx_id.lower() in ['الغاء', 'cancel', 'إلغاء', 'الغاء', 'اغلاق']:
        # المستخدم يستخدم أمر، نوقف عملية التحقق بصمت
        return
    
    if len(tx_id) < 5:
        bot.send_message(uid, bil(uid, "❌ <b>رقم العملية غير صحيح! الرجاء إرسال الـ Order ID بشكل صحيح.</b>", "❌ <b>Invalid Order ID! Please send a valid Order ID.</b>"), parse_mode="HTML")
        return
    
    # 🛡 توحيد الـ tx_id لمنع الالتفاف بـ 0x أو أحرف كبيرة
    tx_id_normalized = normalize_tx_id(tx_id)
    
    with tx_lock:
        if tx_id_normalized in PROCESSING_TXS:
            bot.send_message(uid, bil(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل، يرجى عدم التكرار.</b>", "⏳ <b>This transaction is already being processed, please do not repeat.</b>"), parse_mode="HTML")
            return
        if db.used_transactions.find_one({'transaction_id': tx_id_normalized}):
            bot.reply_to(message, get_text(uid, 'tx_used')); return
        PROCESSING_TXS.add(tx_id_normalized)
    
    # 🛡 حماية ضد سرقة الحوالات: حجز الـ hash لأول مستخدم
    claim_status = claim_tx_hash(tx_id_normalized, uid)
    if claim_status == 'stolen_attempt':
        bot.send_message(
            uid,
            "❌ <b>هذه الحوالة تخص مستخدم آخر!</b>\n\n"
            "🚫 لا يمكن استخدام نفس رقم العملية لأكثر من حساب.\n\n"
            "⚠️ <i>تم تسجيل المحاولة. أي محاولة سرقة قد تؤدي إلى حظر دائم.</i>",
            parse_mode="HTML"
        )
        PROCESSING_TXS.discard(tx_id_normalized)
        return
    elif claim_status == 'already_used':
        bot.reply_to(message, get_text(uid, 'tx_used'))
        PROCESSING_TXS.discard(tx_id_normalized)
        return
        
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
                
        if found: credit_user(uid, amt, tx_id_normalized, lang, "Binance Pay")
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
        PROCESSING_TXS.discard(tx_id_normalized)

@safe_next_step
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
    
    # 🛡 رفض أوامر البوت والرسائل العادية
    if tx_id.startswith('/') or tx_id in ['الغاء', 'cancel', 'إلغاء', 'اغلاق']:
        return
    
    if len(tx_id) < 5:
        bot.send_message(uid, bil(uid, "❌ <b>رقم الهاش (TxID) غير صحيح أو قصير جداً!</b>", "❌ <b>TxID is invalid or too short!</b>"), parse_mode="HTML")
        return
    
    # 🛡 توحيد الـ tx_id لمنع الالتفاف بـ 0x أو تنسيقات مختلفة
    tx_id_normalized = normalize_tx_id(tx_id)
    
    with tx_lock:
        if tx_id_normalized in PROCESSING_TXS:
            bot.send_message(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل، يرجى عدم التكرار.</b>", parse_mode="HTML")
            return
        if db.used_transactions.find_one({'transaction_id': tx_id_normalized}):
            bot.reply_to(message, get_text(uid, 'tx_used')); return
        PROCESSING_TXS.add(tx_id_normalized)
    
    # 🛡 حماية ضد سرقة الحوالات
    claim_status = claim_tx_hash(tx_id_normalized, uid)
    if claim_status == 'stolen_attempt':
        bot.send_message(
            uid,
            "❌ <b>هذه الحوالة تخص مستخدم آخر!</b>\n\n"
            "🚫 لا يمكن استخدام نفس الهاش لأكثر من حساب.\n\n"
            "⚠️ <i>تم تسجيل المحاولة. أي محاولة سرقة قد تؤدي إلى حظر دائم.</i>",
            parse_mode="HTML"
        )
        PROCESSING_TXS.discard(tx_id_normalized)
        return
    elif claim_status == 'already_used':
        bot.reply_to(message, get_text(uid, 'tx_used'))
        PROCESSING_TXS.discard(tx_id_normalized)
        return
        
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
            if status == 1:
                # 🛡 فحص المبلغ الفريد (حماية من بوتات النصب)
                if is_amount_protection_enabled():
                    # USDT = 1$ تقريباً، فالمبلغ بالعملة = المبلغ بالدولار
                    usd_amount_precise = round(float(amt), 4)
                    
                    pending = find_pending_deposit_for_amount(usd_amount_precise, coin, tolerance=0.0001)
                    if pending is None:
                        # نشيك مع USDT_BEP20 أيضاً
                        pending = find_pending_deposit_for_amount(usd_amount_precise, 'USDT_BEP20', tolerance=0.0001)
                    
                    if pending is None:
                        reject_wrong_amount_deposit(uid, coin, usd_amount_precise, tx_id_normalized, f"USDT ({coin})")
                        return
                    
                    if pending['user_id'] != uid:
                        punish_steal_attempt_thief_only(
                            thief_uid=uid,
                            tx_id_clean=tx_id_normalized,
                            pending_owner_uid=pending['user_id'],
                            attempted_amount=usd_amount_precise
                        )
                        return
                    
                    mark_pending_deposit_used(pending['pending_id'])
                    base_amount = pending.get('base_amount_usd', amt)
                    credit_user(uid, base_amount, tx_id_normalized, lang, f"Crypto {coin}")
                else:
                    credit_user(uid, amt, tx_id_normalized, lang, f"Crypto {coin}")
            else: bot.send_message(uid, get_text(uid, 'dep_pending'), parse_mode="HTML")
        else: bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
    except Exception as e:
        logger.debug(f"Unexpected error in verify_crypto_tx: {e}")
        # ⚠️ ما نطلع رسالة خطأ سيرفر - نقول حوالة غير موجودة
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
    finally:
        PROCESSING_TXS.discard(tx_id_normalized)


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


@safe_next_step
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
    
    # 🛡 رفض أوامر البوت والرسائل العادية
    if tx_id.startswith('/') or tx_id.lower() in ['الغاء', 'cancel', 'إلغاء', 'اغلاق']:
        return
    
    # 🛡 TON hashes case-sensitive - ما نسوي lower!
    tx_id_clean = tx_id.replace(' ', '').replace('\n', '')
    
    # 🛡 رفض base64 hash (in_msg hash) - نقبل فقط TxID الكامل
    # علامات الـ base64 hash اللي ما نقبله:
    # - يحتوي على = (padding)
    # - يحتوي على + أو / 
    # - قصير نسبياً (40-50 حرف)
    is_base64_hash = (
        ('=' in tx_id_clean or '+' in tx_id_clean or '/' in tx_id_clean)
        and len(tx_id_clean) < 60
    )
    
    if is_base64_hash:
        bot.send_message(
            uid,
            "❌ <b>هذا ليس TxID صحيحاً!</b>\n\n"
            "💡 <b>أنت أرسلت hash من نوع آخر (in_msg hash).</b>\n\n"
            "📌 <b>الـ TxID الصحيح:</b>\n"
            "• نص طويل (64 حرف على الأقل)\n"
            "• حروف وأرقام فقط (a-z, 0-9)\n"
            "• <b>ما يحتوي على</b> = أو + أو /\n\n"
            "🔍 <b>وين تلقى TxID الصحيح؟</b>\n"
            "1️⃣ افتح محفظتك (Tonkeeper / Tonhub)\n"
            "2️⃣ اضغط على الحوالة\n"
            "3️⃣ اضغط <b>'View in Explorer'</b> أو <b>'عرض في المتصفح'</b>\n"
            "4️⃣ انسخ <b>Transaction Hash</b> الطويل (مو in_msg)\n\n"
            "<i>أو من tonviewer.com / tonscan.org بعد البحث عن حوالتك.</i>",
            parse_mode="HTML"
        )
        return
    
    if len(tx_id_clean) < 32:
        bot.send_message(
            uid,
            "❌ <b>رقم الهاش (TxID) قصير جداً!</b>\n\n"
            "💡 <b>الـ TxID الصحيح لـ TON:</b>\n"
            "• 64 حرف (hex) — مثل: <code>abc123...def</code>\n"
            "• أو 44+ حرف base64 بدون = و + و /\n\n"
            "تأكد من نسخه بالكامل من المتصفح (Tonviewer).",
            parse_mode="HTML"
        )
        return
    
    if wallet_address == "Not Set" or len(wallet_address) < 10:
        bot.send_message(uid, bil(uid, "❌ <b>خطأ:</b> عنوان محفظة TON غير معين في البوت.", "❌ <b>Error:</b> TON wallet address not set in bot."), parse_mode="HTML")
        return
    
    # 🛡 استخدام normalize_tx_id للحماية الموحدة
    tx_track_key = normalize_tx_id(tx_id_clean)
    
    with tx_lock:
        if tx_track_key in PROCESSING_TXS:
            bot.send_message(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل، يرجى عدم التكرار.</b>", parse_mode="HTML")
            return
        if db.used_transactions.find_one({'transaction_id': tx_track_key}):
            bot.reply_to(message, get_text(uid, 'tx_used')); return
        PROCESSING_TXS.add(tx_track_key)
    
    # 🛡 حماية ضد سرقة الحوالات
    claim_status = claim_tx_hash(tx_track_key, uid)
    if claim_status == 'stolen_attempt':
        bot.send_message(
            uid,
            "❌ <b>هذه الحوالة تخص مستخدم آخر!</b>\n\n"
            "🚫 لا يمكن استخدام نفس الهاش لأكثر من حساب.\n\n"
            "⚠️ <i>تم تسجيل المحاولة. أي محاولة سرقة قد تؤدي إلى حظر دائم.</i>",
            parse_mode="HTML"
        )
        PROCESSING_TXS.discard(tx_track_key)
        return
    elif claim_status == 'already_used':
        bot.reply_to(message, get_text(uid, 'tx_used'))
        PROCESSING_TXS.discard(tx_track_key)
        return
    
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
        
        # 🛡 فحص المبلغ الفريد (حماية من بوتات النصب)
        if is_amount_protection_enabled():
            usd_amount_precise = round(received_ton * ton_price, 4)
            
            pending = find_pending_deposit_for_amount(usd_amount_precise, 'TON', tolerance=0.0001)
            
            if pending is None:
                reject_wrong_amount_deposit(uid, 'TON', usd_amount_precise, tx_track_key, "Toncoin (TON)")
                return
            
            if pending['user_id'] != uid:
                punish_steal_attempt_thief_only(
                    thief_uid=uid,
                    tx_id_clean=tx_track_key,
                    pending_owner_uid=pending['user_id'],
                    attempted_amount=usd_amount_precise
                )
                return
            
            mark_pending_deposit_used(pending['pending_id'])
            base_amount = pending.get('base_amount_usd', usd_amount)
            credit_user(uid, base_amount, tx_track_key, lang, "Toncoin (TON)")
        else:
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

@safe_next_step
def verify_ltc_public_blockchain(message, lang, wallet_address):
    uid = message.from_user.id
    if is_user_banned(uid): return
    if not message.text:
        bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML"); return
        
    tx_id = message.text.strip().lower()
    
    # 🛡 رفض أوامر البوت والرسائل العادية
    if tx_id.startswith('/') or tx_id in ['الغاء', 'cancel', 'إلغاء', 'اغلاق']:
        return
    
    if len(tx_id) < 5:
        bot.send_message(uid, bil(uid, "❌ <b>رقم الهاش (TxID) غير صحيح أو قصير جداً! تأكد من نسخه بالكامل.</b>", "❌ <b>TxID is invalid or too short! Make sure to copy it completely.</b>"), parse_mode="HTML")
        return
        
    if wallet_address == "Not Set" or len(wallet_address) < 10:
        bot.send_message(uid, bil(uid, "❌ <b>خطأ:</b> عنوان المحفظة غير معين.", "❌ <b>Error:</b> Wallet address not set."), parse_mode="HTML")
        return
    
    # 🛡 توحيد الـ tx_id لمنع الالتفاف
    tx_id_normalized = normalize_tx_id(tx_id)
    
    with tx_lock:
        if tx_id_normalized in PROCESSING_TXS:
            bot.send_message(uid, "⏳ <b>يتم معالجة هذه العملية بالفعل، يرجى عدم التكرار.</b>", parse_mode="HTML")
            return
        if db.used_transactions.find_one({'transaction_id': tx_id_normalized}):
            bot.reply_to(message, get_text(uid, 'tx_used')); return
        PROCESSING_TXS.add(tx_id_normalized)
    
    # 🛡 حماية ضد سرقة الحوالات
    claim_status = claim_tx_hash(tx_id_normalized, uid)
    if claim_status == 'stolen_attempt':
        bot.send_message(
            uid,
            "❌ <b>هذه الحوالة تخص مستخدم آخر!</b>\n\n"
            "🚫 لا يمكن استخدام نفس الهاش لأكثر من حساب.\n\n"
            "⚠️ <i>تم تسجيل المحاولة. أي محاولة سرقة قد تؤدي إلى حظر دائم.</i>",
            parse_mode="HTML"
        )
        PROCESSING_TXS.discard(tx_id_normalized)
        return
    elif claim_status == 'already_used':
        bot.reply_to(message, get_text(uid, 'tx_used'))
        PROCESSING_TXS.discard(tx_id_normalized)
        return
        
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
                
                # 🛡 فحص المبلغ الفريد (حماية من بوتات النصب)
                if is_amount_protection_enabled():
                    # نحسب المبلغ بدقة أكثر (4 خانات)
                    usd_amount_precise = round(received_ltc * ltc_price, 4)
                    
                    pending = find_pending_deposit_for_amount(usd_amount_precise, 'LTC', tolerance=0.0001)
                    
                    if pending is None:
                        reject_wrong_amount_deposit(uid, 'LTC', usd_amount_precise, tx_id, "Litecoin (LTC)")
                        return
                    
                    if pending['user_id'] != uid:
                        # 🚨 المبلغ مسجّل لمستخدم آخر - بوت نصاب!
                        punish_steal_attempt_thief_only(
                            thief_uid=uid,
                            tx_id_clean=tx_id_normalized,
                            pending_owner_uid=pending['user_id'],
                            attempted_amount=usd_amount_precise
                        )
                        return
                    
                    # ✅ المبلغ مطابق ولنفس المستخدم
                    mark_pending_deposit_used(pending['pending_id'])
                    # نستخدم المبلغ الأساسي (بدون عشر سنتين الإضافية)
                    base_amount = pending.get('base_amount_usd', usd_amount)
                    credit_user(uid, base_amount, tx_id_normalized, lang, "Litecoin (LTC)")
                else:
                    credit_user(uid, usd_amount, tx_id_normalized, lang, "Litecoin (LTC)")
            else: 
                bot.send_message(uid, get_text(uid, 'dep_pending'), parse_mode="HTML")
        else: 
            bot.send_message(uid, get_text(uid, 'dep_fail'), parse_mode="HTML")
            
    except Exception as e:
        bot.send_message(uid, fbil(uid, "❌ حدث خطأ أثناء فحص الشبكة.", "❌ Network check error."), parse_mode="HTML")
    finally:
        PROCESSING_TXS.discard(tx_id_normalized)

def generate_tx_fingerprint(amount, method, sender_addr=None, receiver_addr=None, tx_timestamp=None):
    """
    🛡 يولّد بصمة فريدة لكل حوالة (مو فقط tx_id).
    
    البصمة = MD5( amount + method + sender + receiver + minute_timestamp )
    
    هذا يمنع استخدام نفس الحوالة بـ tx_id مختلفة (مثل in_msg vs tx_hash في TON).
    """
    import hashlib
    
    # نقرب الـ timestamp لأقرب دقيقة (عشان لو في فرق ثوانٍ بين الـ APIs)
    if tx_timestamp:
        minute_ts = int(tx_timestamp / 60) * 60
    else:
        minute_ts = int(time.time() / 60) * 60
    
    # نطبّع العناوين
    sender = (sender_addr or '').strip().lower()
    receiver = (receiver_addr or '').strip().lower()
    
    # نقرّب المبلغ لـ 8 خانات عشرية
    amt_str = f"{float(amount):.8f}"
    
    # نولّد البصمة
    fingerprint_str = f"{amt_str}|{method}|{sender}|{receiver}|{minute_ts}"
    fingerprint = hashlib.md5(fingerprint_str.encode()).hexdigest()
    
    return fingerprint


def check_duplicate_transaction(uid, amount, method, sender_addr=None, receiver_addr=None, tx_timestamp=None, tx_id_clean=None):
    """
    🛡 يفحص لو الحوالة مستخدمة بالفعل (حتى لو الـ tx_id مختلف).
    
    يستخدم 3 طبقات فحص:
    1. tx_id المطبّع (لو موجود)
    2. بصمة الحوالة (fingerprint)
    3. فحص ذكي للحوالات المشابهة (نفس المبلغ + نفس الدقيقة)
    
    يرجع:
    - None لو الحوالة جديدة
    - dict فيه معلومات المُستخدم القديم لو فيه duplicate
    """
    try:
        # الطبقة 1: tx_id المطبّع
        if tx_id_clean:
            existing = db.used_transactions.find_one({'transaction_id': tx_id_clean})
            if existing:
                return {
                    'match_type': 'tx_id',
                    'original_uid': existing.get('user_id'),
                    'original_amount': existing.get('amount', 0),
                    'original_record': existing
                }
        
        # الطبقة 2: بصمة الحوالة
        fingerprint = generate_tx_fingerprint(amount, method, sender_addr, receiver_addr, tx_timestamp)
        existing_fp = db.used_transactions.find_one({'fingerprint': fingerprint})
        if existing_fp:
            return {
                'match_type': 'fingerprint',
                'original_uid': existing_fp.get('user_id'),
                'original_amount': existing_fp.get('amount', 0),
                'original_record': existing_fp
            }
        
        # الطبقة 3: فحص ذكي - نفس المبلغ + نفس الطريقة + نفس الدقيقة
        # (للحوالات اللي ما عندها sender/receiver معروف)
        if tx_timestamp:
            minute_start = int(tx_timestamp / 60) * 60
            minute_end = minute_start + 60
            
            # نبحث عن أي حوالة بنفس المبلغ والطريقة في نفس الدقيقة
            similar = db.used_transactions.find_one({
                'amount': float(amount),
                'method': method,
                'created_at': {
                    '$gte': minute_start,
                    '$lt': minute_end
                },
                'user_id': {'$ne': uid}  # مستخدم آخر
            })
            
            if similar:
                return {
                    'match_type': 'similar_time_amount',
                    'original_uid': similar.get('user_id'),
                    'original_amount': similar.get('amount', 0),
                    'original_record': similar
                }
        
        return None  # حوالة جديدة، آمنة
    except Exception as e:
        logger.error(f"Error in check_duplicate_transaction: {e}")
        return None


def punish_hash_collision_extended(original_uid, thief_uid, tx_id_clean, original_amount=0, match_type='tx_id'):
    """
    🚨 نسخة موسعة - تحظر الاثنين فقط (بدون سحب رصيد).
    """
    try:
        # نجلب رصيد الأصلي (للعرض فقط - بدون سحب)
        original_user = db.users.find_one({'user_id': original_uid})
        original_balance = float(original_user.get('balance', 0)) if original_user else 0
        
        # حظر الاثنين فقط (بدون أي تعديل على الرصيد)
        db.users.update_one({'user_id': original_uid}, {'$set': {'is_banned': 1}})
        db.users.update_one({'user_id': thief_uid}, {'$set': {'is_banned': 1}})
        
        # تسجيل
        original_data = db.users.find_one({'user_id': original_uid}) or {}
        thief_data = db.users.find_one({'user_id': thief_uid}) or {}
        
        try:
            db.theft_attempts.insert_one({
                'transaction_id': tx_id_clean,
                'original_user_id': original_uid,
                'original_username': original_data.get('username', 'unknown'),
                'thief_user_id': thief_uid,
                'thief_username': thief_data.get('username', 'unknown'),
                'current_balance': original_balance,
                'match_type': match_type,
                'timestamp': int(time.time()),
                'status': 'pending_admin_review'
            })
        except: pass
        
        # شرح نوع الكشف
        match_descriptions = {
            'tx_id': 'نفس رقم العملية (TxID)',
            'fingerprint': 'نفس بصمة الحوالة (مبلغ + وقت + عناوين)',
            'similar_time_amount': 'نفس المبلغ في نفس الدقيقة بنفس طريقة الدفع',
            'unknown': 'تطابق غير محدد'
        }
        match_desc = match_descriptions.get(match_type, match_type)
        
        # إشعار للأدمن
        try:
            original_username = original_data.get('username', 'unknown')
            thief_username = thief_data.get('username', 'unknown')
            
            admin_msg = (
                f"🚨 <b>تم اكتشاف تلاعب!</b>\n\n"
                f"⚠️ شخصان حاولا استخدام نفس الحوالة.\n"
                f"🔍 <b>نوع الكشف:</b> {match_desc}\n"
                f"💡 على الأرجح <b>نفس الشخص بحسابين</b>.\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🚫 <b>تم حظر الحسابين تلقائياً:</b>\n\n"
                f"👤 الحساب 1:\n"
                f"   • ID: <code>{original_uid}</code>\n"
                f"   • Username: @{original_username}\n"
                f"   • 💰 الرصيد الحالي: <b>${original_balance:.2f}</b>\n\n"
                f"👤 الحساب 2:\n"
                f"   • ID: <code>{thief_uid}</code>\n"
                f"   • Username: @{thief_username}\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🆔 الهاش/الرقم: <code>{tx_id_clean[:40]}...</code>"
            )
            notify_admins(admin_msg)
        except Exception as notify_err:
            logger.error(f"Failed to notify: {notify_err}")
        
        # رسائل للمستخدمين
        if True:  # نبني حسب اللغة
            ar_msg = (
                "❌ <b>تم اكتشاف تلاعب!</b>\n\n"
                "🚫 تم حظر حسابك بسبب محاولة استخدام نفس الحوالة مع حساب آخر.\n\n"
                "⚠️ <i>أنت قمت بتلاعب - تواصل مع الإدارة لو تعتقد أن هذا خطأ.</i>"
            )
            en_msg = (
                "❌ <b>Manipulation detected!</b>\n\n"
                "🚫 Your account has been banned for attempting to use the same transaction with another account.\n\n"
                "⚠️ <i>Contact admin if you believe this is a mistake.</i>"
            )
        
        try: bot.send_message(original_uid, bil(original_uid, ar_msg, en_msg), parse_mode="HTML")
        except: pass
        try: bot.send_message(thief_uid, bil(thief_uid, ar_msg, en_msg), parse_mode="HTML")
        except: pass
        
        logger.warning(
            f"🚨 EXTENDED COLLISION PUNISHMENT ({match_type}):\n"
            f"   tx: {tx_id_clean[:30]}\n"
            f"   original: {original_uid} (banned, ${original_balance:.2f} seized)\n"
            f"   thief: {thief_uid} (banned)"
        )
        return True
    except Exception as e:
        logger.error(f"Error in punish_hash_collision_extended: {e}")
        return False


def punish_hash_collision(original_uid, thief_uid, tx_id_clean, original_amount=0):
    """
    🚨 يحظر الاثنين (بدون سحب الرصيد):
    1. حظر الاثنين (الأصلي والمدّعي)
    2. تسجيل في theft_attempts للمراجعة
    3. إشعار قوي للأدمن لمراجعتهم يدوياً
    """
    try:
        # نجلب رصيد الأصلي (للعرض في الإشعار فقط - بدون سحب)
        original_user = db.users.find_one({'user_id': original_uid})
        original_balance = float(original_user.get('balance', 0)) if original_user else 0
        
        # 1. حظر الأصلي (بدون سحب الرصيد)
        db.users.update_one(
            {'user_id': original_uid},
            {'$set': {'is_banned': 1}}
        )
        
        # 2. حظر الـ thief
        db.users.update_one(
            {'user_id': thief_uid},
            {'$set': {'is_banned': 1}}
        )
        
        # 3. تسجيل في theft_attempts
        try:
            original_data = db.users.find_one({'user_id': original_uid}) or {}
            thief_data = db.users.find_one({'user_id': thief_uid}) or {}
            
            db.theft_attempts.insert_one({
                'transaction_id': tx_id_clean,
                'original_user_id': original_uid,
                'original_username': original_data.get('username', 'unknown'),
                'thief_user_id': thief_uid,
                'thief_username': thief_data.get('username', 'unknown'),
                'seized_balance': original_balance,
                'collision_type': 'post_success',  # شخصين بعد نجاح الإيداع الأول
                'timestamp': int(time.time()),
                'status': 'pending_admin_review'
            })
        except Exception as log_err:
            logger.error(f"Failed to log theft attempt: {log_err}")
        
        # 4. إشعار قوي للأدمن للمراجعة اليدوية
        try:
            original_username = original_data.get('username', 'unknown') if original_data else 'unknown'
            thief_username = thief_data.get('username', 'unknown') if thief_data else 'unknown'
            
            admin_msg = (
                f"🚨 <b>تم اكتشاف تلاعب!</b>\n\n"
                f"⚠️ شخصان حاولا استخدام نفس الحوالة.\n"
                f"💡 على الأرجح <b>نفس الشخص بحسابين</b>.\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🚫 <b>تم حظر الحسابين تلقائياً:</b>\n\n"
                f"👤 الحساب 1:\n"
                f"   • ID: <code>{original_uid}</code>\n"
                f"   • Username: @{original_username}\n"
                f"   • 💰 الرصيد الحالي: <b>${original_balance:.2f}</b>\n\n"
                f"👤 الحساب 2:\n"
                f"   • ID: <code>{thief_uid}</code>\n"
                f"   • Username: @{thief_username}\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🆔 الهاش: <code>{tx_id_clean[:40]}...</code>"
            )
            notify_admins(admin_msg)
        except Exception as notify_err:
            logger.error(f"Failed to notify admin about hash collision: {notify_err}")
        
        # رسائل بسيطة للمستخدمين
        manipulation_msg = (
            "❌ <b>تم اكتشاف تلاعب!</b>\n\n"
            "🚫 تم حظر حسابك بسبب محاولة استخدام نفس الحوالة مع حساب آخر.\n\n"
            "⚠️ <i>أنت قمت بتلاعب - تواصل مع الإدارة لو تعتقد أن هذا خطأ.</i>"
        )
        
        try:
            bot.send_message(original_uid, manipulation_msg, parse_mode="HTML")
        except Exception:
            pass
        
        try:
            bot.send_message(thief_uid, manipulation_msg, parse_mode="HTML")
        except Exception:
            pass
        
        logger.warning(
            f"🚨 HASH COLLISION PUNISHMENT APPLIED:\n"
            f"   tx: {tx_id_clean[:30]}\n"
            f"   original: {original_uid} (banned, balance ${original_balance:.2f} seized)\n"
            f"   thief: {thief_uid} (banned)"
        )
        return True
    except Exception as e:
        logger.error(f"Error in punish_hash_collision: {e}")
        return False


def normalize_tx_id(tx_id):
    """
    🛡 توحيد رقم العملية (TxID) لمنع الالتفاف على الحماية.
    
    يتعامل مع الحالات:
    - 0x1db4... و 1db4... → نفس الشي
    - ABCdef... و abcdef... → نفس الشي
    - " abc " (مع مسافات) → "abc"
    - "abc\n" (سطر جديد) → "abc"
    - أحرف غير مرئية → تُحذف
    """
    if not tx_id:
        return ""
    
    # 1. تحويل لـ string وتنظيف
    s = str(tx_id).strip()
    
    # 2. إزالة المسافات والأسطر الجديدة من داخل النص (احتياط)
    s = re.sub(r'\s+', '', s)
    
    # 3. إزالة أحرف غير مرئية (zero-width chars)
    s = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u2064\ufeff]', '', s)
    
    # 4. تحويل لـ lowercase
    s = s.lower()
    
    # 5. إزالة بادئات شائعة:
    # - 0x (Ethereum/USDT-BEP20/USDT-ERC20)
    # - 0X (نسخة كبيرة)
    if s.startswith('0x'):
        s = s[2:]
    
    # 6. إزالة أحرف غريبة (نحتفظ بأحرف وأرقام فقط)
    # ملاحظة: TON hashes ممكن تحتوي على = و / و +
    # فما نحذفها لو الـ tx_id فيها هذي الحروف
    if not any(c in s for c in '=/+'):
        # ما فيها رموز TON - نحذف أي رمز غير alphanumeric
        s = re.sub(r'[^a-z0-9]', '', s)
    
    return s


def claim_tx_hash(tx_id, uid):
    """
    🛡 يحجز الـ hash للمستخدم الأول اللي يقدّمه - atomic 100%.
    
    🔥 الاستراتيجية الجديدة (atomic-first):
    - نحاول insert في claimed_hashes أولاً (atomic + unique index)
    - لو نجح: أنت الأول → استمر
    - لو فشل (duplicate): شخص آخر سبقك → نحظره فوراً
    
    يرجع:
    - 'claimed' لو نجح الحجز (هذا أول استخدام - استمر)
    - 'already_used' لو الـ hash استُخدم سابقاً (إيداع مكتمل) - يُطبّق عقوبات
    - 'stolen_attempt' لو شخص آخر يحاول استخدام نفس الـ hash المعلق
    - 'own_pending' لو نفس المستخدم يعيد المحاولة (مسموح)
    - 'already_used_own' لو نفس المستخدم استخدمه قبل (مرفوض)
    """
    try:
        # 🛡 validation صارمة: ما نقبل أوامر أو نصوص قصيرة
        tx_id_str = str(tx_id).strip()
        if not tx_id_str or tx_id_str.startswith('/') or len(tx_id_str) < 5:
            logger.warning(f"⚠️ claim_tx_hash رفض tx_id غير صحيح: {tx_id_str[:30]} للمستخدم {uid}")
            return 'invalid'
        
        tx_id_clean = normalize_tx_id(tx_id)
        
        # 🛡 بعد التطبيع لازم يبقى أطول من 5 أحرف
        if not tx_id_clean or len(tx_id_clean) < 5:
            logger.warning(f"⚠️ claim_tx_hash رفض tx_id قصير بعد التطبيع: {tx_id_clean}")
            return 'invalid'
        
        uid = int(uid)
        
        # 🛡 الخطوة 1: نحاول insert atomic في claimed_hashes (الـ unique index يحمي)
        # هذا اللي يحل race condition - فقط مستخدم واحد ينجح
        try:
            db.claimed_hashes.insert_one({
                'transaction_id': tx_id_clean,
                'user_id': uid,
                'claimed_at': int(time.time()),
                'status': 'pending'
            })
            # ✅ نجح الـ insert - أنت الأول
            # نشيك إن الـ hash مو مستخدم في used_transactions أصلاً
            used_record = db.used_transactions.find_one({'transaction_id': tx_id_clean})
            if used_record:
                # حالة نادرة: الـ hash استخدم بالفعل لكن ما كان في claimed_hashes
                # نلغي الـ claim وننفذ العقوبات
                db.claimed_hashes.delete_one({'transaction_id': tx_id_clean, 'user_id': uid})
                
                original_uid = used_record.get('user_id')
                if original_uid == uid:
                    return 'already_used_own'
                
                # نطبق العقوبات
                original_amount = float(used_record.get('amount', 0))
                original_user = db.users.find_one({'user_id': original_uid})
                if original_user and original_user.get('is_banned') != 1:
                    punish_hash_collision(original_uid, uid, tx_id_clean, original_amount)
                return 'already_used'
            
            return 'claimed'
        except Exception as insert_err:
            # ❌ فشل الـ insert - شخص آخر سبقك (race condition محل)
            err_str = str(insert_err).lower()
            if 'duplicate' not in err_str and 'e11000' not in err_str:
                # خطأ ثاني مو duplicate
                logger.error(f"Unexpected claim error: {insert_err}")
                return 'claimed'  # نسمح بالمحاولة عشان ما نقفله بسبب خطأ DB
        
        # 🛡 الخطوة 2: لقينا duplicate - نشوف من اللي سبقنا
        # شيك على الـ used_transactions أولاً (أخطر حالة)
        used_record = db.used_transactions.find_one({'transaction_id': tx_id_clean})
        if used_record:
            original_uid = used_record.get('user_id')
            if original_uid == uid:
                return 'already_used_own'
            
            # 🚨 شخص ثاني بعد نجاح الأول - عقوبات كاملة
            original_amount = float(used_record.get('amount', 0))
            original_user = db.users.find_one({'user_id': original_uid})
            if original_user and original_user.get('is_banned') != 1:
                punish_hash_collision(original_uid, uid, tx_id_clean, original_amount)
            else:
                # الأصلي محظور أصلاً
                try:
                    db.users.update_one({'user_id': uid}, {'$set': {'is_banned': 1}})
                    thief_data = db.users.find_one({'user_id': uid}) or {}
                    thief_username = thief_data.get('username', 'unknown')
                    notify_admins(
                        f"🚨 <b>محاولة سرقة جديدة!</b>\n\n"
                        f"👤 المهاجم: <code>{uid}</code> @{thief_username}\n"
                        f"🆔 Hash: <code>{tx_id_clean[:30]}...</code>\n"
                        f"💡 الحساب الأصلي محظور بالفعل.\n"
                        f"🚫 تم حظر هذا المستخدم تلقائياً."
                    )
                except: pass
            return 'already_used'
        
        # 🛡 الخطوة 3: الـ hash موجود في claimed_hashes (شخص آخر حجزه)
        existing_claim = db.claimed_hashes.find_one({'transaction_id': tx_id_clean})
        
        if not existing_claim:
            # غريب - الـ insert فشل بـ duplicate لكن لا في claimed_hashes ولا used_transactions
            # نحاول مرة ثانية
            return claim_tx_hash(tx_id, uid)
        
        if existing_claim.get('user_id') == uid:
            return 'own_pending'  # نفس المستخدم
        
        # 🚨 شخصين مختلفين، نفس الـ hash، نفس الوقت → race condition!
        # نطبق عقوبات قوية فوراً لأن هذا سيناريو نصب واضح
        original_uid = existing_claim.get('user_id')
        
        # تسجيل المحاولة
        try:
            thief_data = db.users.find_one({'user_id': uid}) or {}
            original_data = db.users.find_one({'user_id': original_uid}) or {}
            
            db.theft_attempts.insert_one({
                'transaction_id': tx_id_clean,
                'original_claimer': original_uid,
                'original_username': original_data.get('username', 'unknown'),
                'thief_attempt': uid,
                'thief_username': thief_data.get('username', 'unknown'),
                'timestamp': int(time.time()),
                'collision_type': 'simultaneous_race',
                'status': 'pending_admin_review'
            })
        except Exception:
            pass
        
        # 🚨 حظر الاثنين فوراً (race condition = نصب واضح)
        try:
            # حظر الاثنين فقط (بدون سحب رصيد)
            db.users.update_one({'user_id': original_uid}, {'$set': {'is_banned': 1}})
            db.users.update_one({'user_id': uid}, {'$set': {'is_banned': 1}})
            
            # نجلب رصيد الأول (للعرض في الإشعار فقط)
            original_user = db.users.find_one({'user_id': original_uid})
            original_balance = float(original_user.get('balance', 0)) if original_user else 0
        except Exception as ban_err:
            logger.error(f"Failed to ban race users: {ban_err}")
        
        # إشعار قوي للأدمن
        try:
            original_username = original_data.get('username', 'unknown') if original_data else 'unknown'
            thief_username = thief_data.get('username', 'unknown') if thief_data else 'unknown'
            
            admin_msg = (
                f"🚨 <b>تم اكتشاف تلاعب!</b>\n\n"
                f"⚠️ شخصان أرسلا نفس الهاش/الأوردر في نفس اللحظة.\n"
                f"💡 على الأرجح <b>نفس الشخص بحسابين</b>.\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🚫 <b>تم حظر الحسابين تلقائياً:</b>\n\n"
                f"👤 الحساب 1:\n"
                f"   • ID: <code>{original_uid}</code>\n"
                f"   • Username: @{original_username}\n"
                f"   • 💰 الرصيد الحالي: <b>${original_balance:.2f}</b>\n\n"
                f"👤 الحساب 2:\n"
                f"   • ID: <code>{uid}</code>\n"
                f"   • Username: @{thief_username}\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🆔 الهاش: <code>{tx_id_clean[:40]}...</code>"
            )
            notify_admins(admin_msg)
        except Exception:
            pass
        
        # رسائل بسيطة للمستخدمين
        cancel_msg_ar = (
            "❌ <b>تم اكتشاف تلاعب!</b>\n\n"
            "🚫 تم حظر حسابك بسبب محاولة استخدام نفس الحوالة مع حساب آخر.\n\n"
            "⚠️ <i>أنت قمت بتلاعب - تواصل مع الإدارة لو تعتقد أن هذا خطأ.</i>"
        )
        cancel_msg_en = (
            "❌ <b>Manipulation detected!</b>\n\n"
            "🚫 Your account has been banned for trying to use the same transaction with another account.\n\n"
            "⚠️ <i>You attempted manipulation - contact admin if you believe this is a mistake.</i>"
        )
        
        try:
            bot.send_message(uid, cancel_msg_ar, parse_mode="HTML")
        except: pass
        try:
            bot.send_message(original_uid, cancel_msg_ar, parse_mode="HTML")
        except: pass
        
        logger.warning(
            f"🚨 SIMULTANEOUS RACE DETECTED: "
            f"users {original_uid} and {uid} both tried hash {tx_id_clean[:20]}... "
            f"at the same time. Both banned."
        )
        
        return 'stolen_attempt'
    except Exception as e:
        logger.error(f"Error in claim_tx_hash: {e}")
        return 'claimed'


def cleanup_old_claimed_hashes():
    """ينظف الـ hashes المعلقة القديمة (أكثر من 48 ساعة)"""
    try:
        cutoff = int(time.time()) - (48 * 60 * 60)
        result = db.claimed_hashes.delete_many({
            'claimed_at': {'$lt': cutoff},
            'status': 'pending'
        })
        if result.deleted_count > 0:
            logger.info(f"🧹 تم تنظيف {result.deleted_count} hash معلق قديم.")
    except Exception as e:
        logger.error(f"Error cleaning old claimed hashes: {e}")


def _cleanup_thread():
    """Thread يشتغل كل ساعة لتنظيف الـ claimed_hashes و pending_deposits"""
    while True:
        try:
            time.sleep(3600)  # كل ساعة
            cleanup_old_claimed_hashes()
            cleanup_expired_pending_deposits()
        except Exception as e:
            logger.error(f"Cleanup thread error: {e}")
            time.sleep(600)


threading.Thread(target=_cleanup_thread, daemon=True).start()


def credit_user(uid, amt, tx_id, lang, method):
    """
    🛡 إضافة رصيد للمستخدم بشكل atomic.
    """
    # نحفظ النسخة الأصلية للعرض في الإشعارات
    tx_id_original = str(tx_id).strip() if tx_id else ''
    # 🛡 الخطوة 0: Validation حرجة!
    try:
        amt = float(amt)
    except (ValueError, TypeError):
        logger.error(f"❌ مبلغ غير صحيح في credit_user: {amt!r} للمستخدم {uid}")
        return
    
    # 🛡 رفض المبالغ الصفر والسالبة (الحد الأدنى 0.10$)
    if amt <= 0:
        logger.error(f"🚨 محاولة إيداع بمبلغ صفر/سالب: amt={amt} للمستخدم {uid}")
        try:
            bot.send_message(uid, bil(uid, "❌ <b>المبلغ غير صالح.</b>", "❌ <b>Invalid amount.</b>"), parse_mode="HTML")
        except: pass
        return
    
    if amt < 0.10:
        logger.warning(f"⚠️ مبلغ إيداع تحت الحد الأدنى: amt={amt} للمستخدم {uid}")
        try:
            bot.send_message(uid, f"❌ <b>المبلغ ${amt:.2f} أقل من الحد الأدنى ($0.10).</b>", parse_mode="HTML")
        except: pass
        return
    
    # 🛡 رفض المبالغ الضخمة المشبوهة (أكثر من $50,000 = خطأ بالتأكيد)
    if amt > 50000:
        logger.error(f"🚨 مبلغ ضخم مشبوه: amt={amt} للمستخدم {uid} - تم رفضه!")
        try:
            notify_admins(
                f"🚨 <b>مبلغ ضخم مشبوه!</b>\n\n"
                f"👤 المستخدم: <code>{uid}</code>\n"
                f"💰 المبلغ: <b>${amt:.2f}</b>\n"
                f"💳 الطريقة: {method}\n\n"
                f"⚠️ تم رفض الإيداع تلقائياً. راجع يدوياً."
            )
        except: pass
        try:
            bot.send_message(uid, "❌ <b>المبلغ كبير جداً.</b>\n\nتواصل مع الإدارة.", parse_mode="HTML")
        except: pass
        return
    
    # 🛡 validation للـ tx_id
    if not tx_id or len(str(tx_id).strip()) < 5:
        logger.error(f"🚨 tx_id غير صحيح: {tx_id!r} للمستخدم {uid}")
        return
    
    # 🛡 الخطوة 1: توحيد tx_id - يشيل 0x والأحرف الكبيرة والمسافات
    tx_id_clean = normalize_tx_id(tx_id)
    
    # 🛡 رفض tx_id قصير جداً بعد التطبيع
    if len(tx_id_clean) < 5:
        logger.error(f"🚨 tx_id قصير بعد التطبيع: {tx_id_clean!r}")
        return
    
    # 🛡 الخطوة 2: فحص duplicate شامل (3 طبقات)
    # هذا يكتشف نفس الحوالة حتى لو الـ tx_id مختلف
    current_time = int(time.time())
    dup_check = check_duplicate_transaction(
        uid=uid,
        amount=amt,
        method=method,
        tx_timestamp=current_time,
        tx_id_clean=tx_id_clean
    )
    
    if dup_check:
        original_uid = dup_check.get('original_uid')
        match_type = dup_check.get('match_type', 'unknown')
        original_amount = float(dup_check.get('original_amount', 0))
        
        if original_uid != uid:
            # 🚨 شخص آخر استخدم نفس الحوالة (بأي طريقة)
            logger.warning(
                f"🚨 DUPLICATE DETECTED ({match_type}): "
                f"user {uid} حاول حوالة مستخدمة من user {original_uid}"
            )
            
            original_user = db.users.find_one({'user_id': original_uid})
            if original_user and original_user.get('is_banned') != 1:
                try:
                    punish_hash_collision_extended(
                        original_uid, uid, tx_id_clean, original_amount, match_type
                    )
                except Exception as punish_err:
                    logger.error(f"Failed extended punishment: {punish_err}")
            else:
                # الأصلي محظور بالفعل، نحظر الجديد
                try:
                    db.users.update_one({'user_id': uid}, {'$set': {'is_banned': 1}})
                    notify_admins(
                        f"🚨 <b>محاولة استخدام حوالة مستخدمة!</b>\n\n"
                        f"👤 المهاجم: <code>{uid}</code>\n"
                        f"🔍 نوع الكشف: <b>{match_type}</b>\n"
                        f"💰 المبلغ: <b>${amt:.2f}</b>\n"
                        f"🚫 تم حظره تلقائياً (الأصلي محظور بالفعل)"
                    )
                except: pass
            
            # رسالة للمستخدم
            try:
                bot.send_message(
                    uid,
                    "❌ <b>تم اكتشاف تلاعب!</b>\n\n"
                    "🚫 تم حظر حسابك بسبب محاولة استخدام حوالة مستخدمة بالفعل.\n\n"
                    "⚠️ <i>أنت قمت بتلاعب - تواصل مع الإدارة لو تعتقد أن هذا خطأ.</i>",
                    parse_mode="HTML"
                )
            except: pass
            return
        else:
            # نفس المستخدم - يا إما أعاد المحاولة، يا إما الإيداع موجود
            if dup_check.get('match_type') == 'tx_id':
                try:
                    bot.send_message(uid, "✅ <b>هذا الإيداع تم تأكيده بالفعل.</b>", parse_mode="HTML")
                except: pass
            return
    
    # 🛡 الخطوة 3: نحسب البصمة الفريدة للحوالة
    tx_fingerprint = generate_tx_fingerprint(
        amount=amt,
        method=method,
        tx_timestamp=current_time
    )
    
    # 🛡 الخطوة 4: فحص duplicate صريح ثاني (احتياط)
    existing_check = db.used_transactions.find_one({'transaction_id': tx_id_clean})
    if existing_check:
        original_uid = existing_check.get('user_id')
        if original_uid != uid:
            original_amount = float(existing_check.get('amount', 0))
            original_user = db.users.find_one({'user_id': original_uid})
            if original_user and original_user.get('is_banned') != 1:
                try:
                    punish_hash_collision(original_uid, uid, tx_id_clean, original_amount)
                except Exception as punish_err:
                    logger.error(f"Failed punishment in pre-check: {punish_err}")
        
        try:
            if lang == 'ar':
                bot.send_message(
                    uid,
                    "❌ <b>تم اكتشاف تلاعب!</b>\n\n"
                    "🚫 تم حظر حسابك بسبب محاولة استخدام نفس الحوالة مع حساب آخر.\n\n"
                    "⚠️ <i>أنت قمت بتلاعب - تواصل مع الإدارة لو تعتقد أن هذا خطأ.</i>",
                    parse_mode="HTML"
                )
            else:
                bot.send_message(
                    uid,
                    "❌ <b>Manipulation detected!</b>\n\n"
                    "🚫 Your account has been banned.\n\n"
                    "⚠️ <i>Contact admin if you believe this is a mistake.</i>",
                    parse_mode="HTML"
                )
        except Exception: pass
        return
    
    # 🛡 الخطوة 5: محاولة atomic insert (الـ unique index يحمي من race condition)
    try:
        db.used_transactions.insert_one({
            'transaction_id': tx_id_clean,
            'fingerprint': tx_fingerprint,
            'amount': amt,
            'user_id': uid,
            'method': method,
            'created_at': int(time.time())
        })
    except Exception as insert_err:
        err_str = str(insert_err).lower()
        if 'duplicate' in err_str or 'e11000' in err_str:
            # ❌ duplicate - race condition محل بالـ index
            logger.warning(
                f"🚨 RACE CONDITION CAUGHT: "
                f"user {uid} حاول tx {tx_id_clean[:30]} لكن سبقه آخر"
            )
            
            # نشوف من الأول
            existing = db.used_transactions.find_one({
                '$or': [
                    {'transaction_id': tx_id_clean},
                    {'fingerprint': tx_fingerprint}
                ]
            })
            if existing:
                original_uid = existing.get('user_id')
                if original_uid != uid:
                    original_amount = float(existing.get('amount', 0))
                    original_user = db.users.find_one({'user_id': original_uid})
                    if original_user and original_user.get('is_banned') != 1:
                        try:
                            punish_hash_collision_extended(
                                original_uid, uid, tx_id_clean, original_amount, 'race_condition'
                            )
                        except Exception as punish_err:
                            logger.error(f"Failed punishment: {punish_err}")
            
            # رسالة الإلغاء
            try:
                if lang == 'ar':
                    bot.send_message(
                        uid,
                        "❌ <b>تم اكتشاف تلاعب!</b>\n\n"
                        "🚫 تم حظر حسابك بسبب محاولة استخدام نفس الحوالة مع حساب آخر.\n\n"
                        "⚠️ <i>أنت قمت بتلاعب - تواصل مع الإدارة لو تعتقد أن هذا خطأ.</i>",
                        parse_mode="HTML"
                    )
                else:
                    bot.send_message(
                        uid,
                        "❌ <b>Manipulation detected!</b>\n\n"
                        "🚫 Your account has been banned.\n\n"
                        "⚠️ <i>Contact admin if you believe this is a mistake.</i>",
                        parse_mode="HTML"
                    )
            except Exception: pass
            return  # ⛔ ما نضيف رصيد
        else:
            logger.error(f"Failed to insert tx: {insert_err}")
            try:
                bot.send_message(
                    uid,
                    bil(uid, "❌ <b>حدث خطأ في معالجة الإيداع.</b>\n\nيرجى التواصل مع الإدارة.", "❌ <b>Error processing deposit.</b>\n\nPlease contact admin."),
                    parse_mode="HTML"
                )
            except: pass
            return
    
    # ✅ الخطوة 4: نجح الـ insert - نضيف الرصيد
    db.users.update_one({'user_id': uid}, {'$inc': {'balance': amt}})
    
    # 🛡 نظف الـ claimed_hashes بعد نجاح الإيداع
    try:
        db.claimed_hashes.delete_one({'transaction_id': tx_id_clean})
    except Exception:
        pass
    
    bot.send_message(uid, get_text(uid, 'dep_success', amt), parse_mode="HTML")
    
    u = get_user_data_full(uid)
    buyer_m = f"@{u['username']}" if u and u.get('username') else f"مستخدم"
    
    admin_msg = f"🔐 <b>إشعار إدارة (إيداع)</b>\n\n👤 العميل: {buyer_m} (<code>{uid}</code>)\n💰 المبلغ: <b>${amt:.2f}</b>\n💳 الطريقة: {method}\n🆔 رقم العملية:\n<code>{tx_id_original}</code>"
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
@bot.callback_query_handler(func=lambda call: call.data == "toggle_amount_protection")
@admin_required
def toggle_amount_protection(call):
    """🛡 يفعّل/يعطّل نظام الحماية بالمبلغ الفريد"""
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    
    current = is_amount_protection_enabled()
    new_value = 'off' if current else 'on'
    
    db.settings.update_one(
        {'key': 'amount_protection'},
        {'$set': {'value': new_value}},
        upsert=True
    )
    
    if new_value == 'on':
        msg = (
            "✅ <b>تم تفعيل نظام حماية الإيداعات!</b>\n\n"
            "🛡 الآن:\n"
            "• كل مستخدم يحتاج يسجّل عملية الإيداع أولاً (يكتب المبلغ)\n"
            "• البوت يعطيه مبلغ فريد (مثل $5.0023)\n"
            "• المستخدم يحوّل المبلغ الفريد بالضبط\n"
            "• بوتات النصب اللي تراقب blockchain ما تقدر تسرق\n\n"
            "✅ <b>هذا الحل الوحيد ضد بوتات النصب التلقائية!</b>"
        )
    else:
        msg = (
            "⚠️ <b>تم تعطيل نظام حماية الإيداعات!</b>\n\n"
            "🚨 <b>تحذير شديد:</b>\n"
            "بدون هذا النظام، أي بوت نصب يراقب blockchain يقدر يسرق حوالات مستخدميك!\n\n"
            "💡 ننصح بتفعيله فوراً."
        )
    
    bot.send_message(uid, msg, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "ad_ref_settings")
@admin_required
def ad_ref_settings_ui(call):
    """🛡 واجهة إعدادات نظام الإحالات"""
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    
    # نجلب الإعدادات الحالية
    threshold = get_referral_threshold()
    reward = get_referral_reward()
    purchase_reward = get_referral_purchase_reward()
    min_purchase = get_referral_min_purchase()
    
    text = (
        f"👥 <b>إعدادات نظام الإحالات</b>\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"📊 <b>الإعدادات الحالية:</b>\n\n"
        f"🎯 عدد الإحالات المطلوبة لكل مكافأة:\n"
        f"   <b>{threshold}</b> إحالة\n\n"
        f"💰 قيمة المكافأة لكل دفعة:\n"
        f"   <b>${reward:.2f}</b>\n\n"
        f"🛍 قيمة مكافأة الشراء (لكل شراء من مُحال):\n"
        f"   <b>${purchase_reward:.2f}</b>\n\n"
        f"💵 الحد الأدنى لقيمة الشراء:\n"
        f"   <b>${min_purchase:.2f}</b>\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"💡 <b>مثال على آلية العمل:</b>\n"
        f"• كل <b>{threshold}</b> إحالة نشطة = <b>${reward:.2f}</b>\n"
        f"• شراء أي مُحال > <b>${min_purchase:.2f}</b> = <b>${purchase_reward:.2f}</b> للمُحيل\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🛠 <b>اختر ما تريد تعديله:</b>"
    )
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton(f"🎯 عدد الإحالات المطلوبة ({threshold})", callback_data="ad_ref_set_threshold"))
    markup.add(InlineKeyboardButton(f"💰 قيمة مكافأة الإحالات (${reward:.2f})", callback_data="ad_ref_set_reward"))
    markup.add(InlineKeyboardButton(f"🛍 قيمة مكافأة الشراء (${purchase_reward:.2f})", callback_data="ad_ref_set_purchase_reward"))
    markup.add(InlineKeyboardButton(f"💵 الحد الأدنى للشراء (${min_purchase:.2f})", callback_data="ad_ref_set_min_purchase"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception:
        bot.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "ad_ref_set_threshold")
@admin_required
def ad_ref_set_threshold(call):
    """تغيير عدد الإحالات المطلوبة"""
    bot.answer_callback_query(call.id)
    current = get_referral_threshold()
    msg = bot.send_message(
        call.message.chat.id,
        f"🎯 <b>تغيير عدد الإحالات المطلوبة</b>\n\n"
        f"📊 القيمة الحالية: <b>{current}</b>\n\n"
        f"💡 <b>مثال:</b>\n"
        f"• 10 = كل 10 إحالات نشطة → مكافأة\n"
        f"• 5 = كل 5 إحالات نشطة → مكافأة\n\n"
        f"⚠️ <b>الحد الأدنى:</b> 1\n"
        f"⚠️ <b>الموصى به:</b> 5 - 20\n\n"
        f"📤 أرسل الرقم الجديد (أو 'الغاء' للإلغاء):",
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_ref_save_threshold)


@safe_next_step
def ad_ref_save_threshold(message):
    uid = message.from_user.id
    if not _is_admin_check(uid):
        return
    
    if message.text and message.text.strip().lower() in ['الغاء', 'cancel']:
        bot.send_message(message.chat.id, "❌ تم الإلغاء.")
        return
    
    try:
        value = int(message.text.strip())
        if value < 1:
            bot.send_message(message.chat.id, "❌ الحد الأدنى هو 1.")
            return
        if value > 1000:
            bot.send_message(message.chat.id, "❌ الحد الأقصى هو 1000.")
            return
        
        db.settings.update_one(
            {'key': 'referral_threshold'},
            {'$set': {'value': str(value)}},
            upsert=True
        )
        
        bot.send_message(
            message.chat.id,
            f"✅ <b>تم الحفظ!</b>\n\n"
            f"🎯 عدد الإحالات المطلوبة الآن: <b>{value}</b>\n\n"
            f"📌 <i>أي مستخدم يصل إلى {value} إحالة نشطة سيحصل على المكافأة تلقائياً.</i>",
            parse_mode="HTML"
        )
    except ValueError:
        bot.send_message(message.chat.id, "❌ أرسل رقم صحيح فقط.")
    except Exception as e:
        logger.error(f"Error saving threshold: {e}")
        bot.send_message(message.chat.id, "❌ حدث خطأ.")


@bot.callback_query_handler(func=lambda call: call.data == "ad_ref_set_reward")
@admin_required
def ad_ref_set_reward(call):
    """تغيير قيمة المكافأة"""
    bot.answer_callback_query(call.id)
    current = get_referral_reward()
    threshold = get_referral_threshold()
    msg = bot.send_message(
        call.message.chat.id,
        f"💰 <b>تغيير قيمة مكافأة الإحالات</b>\n\n"
        f"📊 القيمة الحالية: <b>${current:.2f}</b>\n\n"
        f"💡 <b>هذه المكافأة تُعطى لكل {threshold} إحالة نشطة.</b>\n\n"
        f"📌 <b>أمثلة:</b>\n"
        f"• 0.10 = $0.10 لكل {threshold} إحالة\n"
        f"• 0.50 = $0.50 لكل {threshold} إحالة\n"
        f"• 1.00 = $1.00 لكل {threshold} إحالة\n\n"
        f"⚠️ <b>الحد الأدنى:</b> 0.01\n\n"
        f"📤 أرسل القيمة الجديدة (أو 'الغاء' للإلغاء):",
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_ref_save_reward)


@safe_next_step
def ad_ref_save_reward(message):
    uid = message.from_user.id
    if not _is_admin_check(uid):
        return
    
    if message.text and message.text.strip().lower() in ['الغاء', 'cancel']:
        bot.send_message(message.chat.id, "❌ تم الإلغاء.")
        return
    
    try:
        value = float(message.text.strip().replace('$', '').replace(',', '.'))
        if value < 0.01:
            bot.send_message(message.chat.id, "❌ الحد الأدنى هو $0.01.")
            return
        if value > 100:
            bot.send_message(message.chat.id, "❌ الحد الأقصى هو $100.")
            return
        
        db.settings.update_one(
            {'key': 'referral_reward'},
            {'$set': {'value': str(value)}},
            upsert=True
        )
        
        threshold = get_referral_threshold()
        bot.send_message(
            message.chat.id,
            f"✅ <b>تم الحفظ!</b>\n\n"
            f"💰 المكافأة الآن: <b>${value:.2f}</b> لكل <b>{threshold}</b> إحالة نشطة.",
            parse_mode="HTML"
        )
    except ValueError:
        bot.send_message(message.chat.id, "❌ أرسل رقم صحيح (مثل: 0.10).")
    except Exception as e:
        logger.error(f"Error saving reward: {e}")
        bot.send_message(message.chat.id, "❌ حدث خطأ.")


@bot.callback_query_handler(func=lambda call: call.data == "ad_ref_set_purchase_reward")
@admin_required
def ad_ref_set_purchase_reward(call):
    """تغيير قيمة مكافأة الشراء"""
    bot.answer_callback_query(call.id)
    current = get_referral_purchase_reward()
    msg = bot.send_message(
        call.message.chat.id,
        f"🛍 <b>تغيير قيمة مكافأة الشراء</b>\n\n"
        f"📊 القيمة الحالية: <b>${current:.2f}</b>\n\n"
        f"💡 <b>هذه المكافأة تُعطى للمُحيل لما يشتري المُحال أي منتج فوق الحد الأدنى.</b>\n\n"
        f"📌 <b>أمثلة:</b>\n"
        f"• 0.10 = $0.10 لكل شراء\n"
        f"• 0.50 = $0.50 لكل شراء\n\n"
        f"📤 أرسل القيمة الجديدة (أو 'الغاء' للإلغاء):",
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_ref_save_purchase_reward)


@safe_next_step
def ad_ref_save_purchase_reward(message):
    uid = message.from_user.id
    if not _is_admin_check(uid):
        return
    
    if message.text and message.text.strip().lower() in ['الغاء', 'cancel']:
        bot.send_message(message.chat.id, "❌ تم الإلغاء.")
        return
    
    try:
        value = float(message.text.strip().replace('$', '').replace(',', '.'))
        if value < 0.01:
            bot.send_message(message.chat.id, "❌ الحد الأدنى هو $0.01.")
            return
        
        db.settings.update_one(
            {'key': 'referral_purchase_reward'},
            {'$set': {'value': str(value)}},
            upsert=True
        )
        
        bot.send_message(
            message.chat.id,
            f"✅ <b>تم الحفظ!</b>\n\n💰 مكافأة الشراء الآن: <b>${value:.2f}</b>",
            parse_mode="HTML"
        )
    except ValueError:
        bot.send_message(message.chat.id, "❌ أرسل رقم صحيح (مثل: 0.10).")
    except Exception as e:
        logger.error(f"Error saving purchase reward: {e}")


@bot.callback_query_handler(func=lambda call: call.data == "ad_ref_set_min_purchase")
@admin_required
def ad_ref_set_min_purchase(call):
    """تغيير الحد الأدنى لقيمة الشراء"""
    bot.answer_callback_query(call.id)
    current = get_referral_min_purchase()
    msg = bot.send_message(
        call.message.chat.id,
        f"💵 <b>تغيير الحد الأدنى لقيمة الشراء</b>\n\n"
        f"📊 القيمة الحالية: <b>${current:.2f}</b>\n\n"
        f"💡 <b>المُحال لازم يشتري بأكثر من هذه القيمة عشان يربح المُحيل المكافأة.</b>\n\n"
        f"📤 أرسل القيمة الجديدة (أو 'الغاء' للإلغاء):",
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_ref_save_min_purchase)


@safe_next_step
def ad_ref_save_min_purchase(message):
    uid = message.from_user.id
    if not _is_admin_check(uid):
        return
    
    if message.text and message.text.strip().lower() in ['الغاء', 'cancel']:
        bot.send_message(message.chat.id, "❌ تم الإلغاء.")
        return
    
    try:
        value = float(message.text.strip().replace('$', '').replace(',', '.'))
        if value < 0.10:
            bot.send_message(message.chat.id, "❌ الحد الأدنى هو $0.10.")
            return
        
        db.settings.update_one(
            {'key': 'referral_min_purchase'},
            {'$set': {'value': str(value)}},
            upsert=True
        )
        
        bot.send_message(
            message.chat.id,
            f"✅ <b>تم الحفظ!</b>\n\n💵 الحد الأدنى للشراء الآن: <b>${value:.2f}</b>",
            parse_mode="HTML"
        )
    except ValueError:
        bot.send_message(message.chat.id, "❌ أرسل رقم صحيح.")
    except Exception as e:
        logger.error(f"Error saving min purchase: {e}")


@bot.message_handler(commands=['fix_innocent_bans'])
@bot.message_handler(commands=['fix_referrals'])
def fix_referrals_cmd(message):
    """🔧 يصلح ref_v2_earned لكل المستخدمين ويعطيهم مكافآتهم الفائتة"""
    uid = message.from_user.id
    if not _is_admin_check(uid):
        return

    bot.send_message(message.chat.id, "⏳ جاري إصلاح نظام الإحالات لكل المستخدمين...", parse_mode="HTML")

    # الخطوة 1: إصلاح ref_v2_earned المكسورة (string فارغة أو مفقودة)
    try:
        # نحول كل "" أو null إلى 0.0
        db.users.update_many(
            {'$or': [
                {'ref_v2_earned': ''},
                {'ref_v2_earned': None},
                {'ref_v2_earned': {'$exists': False}}
            ]},
            {'$set': {'ref_v2_earned': 0.0}}
        )
        bot.send_message(message.chat.id, "✅ تم إصلاح حقول ref_v2_earned الفارغة")
    except Exception as e:
        bot.send_message(message.chat.id, f"⚠️ خطأ في الخطوة 1: {e}")

    # الخطوة 2: إعطاء كل مستخدم مكافآته الفائتة
    threshold = get_referral_threshold()
    reward = get_referral_reward()

    referrers = list(db.referrals_v2.distinct('referrer_id'))
    fixed = 0
    total_paid = 0.0

    for referrer_id in referrers:
        try:
            active_count = db.referrals_v2.count_documents({
                'referrer_id': referrer_id,
                'status': 'active'
            })
            expected = round((active_count // threshold) * reward, 2)
            if expected <= 0:
                continue

            user = db.users.find_one({'user_id': referrer_id})
            if not user:
                continue

            raw = user.get('ref_v2_earned', 0.0)
            try:
                current = round(float(raw), 2)
            except:
                current = 0.0

            if expected > current:
                diff = round(expected - current, 2)
                db.users.update_one(
                    {'user_id': referrer_id},
                    {
                        '$inc': {'balance': diff},
                        '$set': {'ref_v2_earned': expected}
                    }
                )
                fixed += 1
                total_paid += diff

                # نرسل للمستخدم
                try:
                    ref_lang = user.get('lang', 'ar')
                    if ref_lang == 'ar':
                        bot.send_message(
                            referrer_id,
                            f"🎉 <b>مبروك! تم إضافة مكافآتك الفائتة!</b>\n\n"
                            f"💰 المبلغ المضاف: <b>+${diff:.2f}</b>\n"
                            f"👥 إحالاتك النشطة: <b>{active_count}</b>",
                            parse_mode="HTML"
                        )
                    else:
                        bot.send_message(
                            referrer_id,
                            f"🎉 <b>Your missed referral rewards have been added!</b>\n\n"
                            f"💰 Amount Added: <b>+${diff:.2f}</b>\n"
                            f"👥 Active Referrals: <b>{active_count}</b>",
                            parse_mode="HTML"
                        )
                except:
                    pass
        except Exception as e:
            logger.error(f"fix_referrals error for {referrer_id}: {e}")

    bot.send_message(
        message.chat.id,
        f"✅ <b>اكتمل إصلاح الإحالات!</b>\n\n"
        f"👥 مستخدمين استلموا مكافآتهم: <b>{fixed}</b>\n"
        f"💰 إجمالي الدفع: <b>${total_paid:.2f}</b>\n"
        f"📊 العتبة: كل <b>{threshold}</b> إحالة = <b>${reward:.2f}</b>",
        parse_mode="HTML"
    )


def fix_innocent_bans_cmd(message):
    """🛡 يفك حظر المستخدمين اللي حُظروا بسبب bug في النظام (إرسال /start)"""
    uid = message.from_user.id
    if not _is_admin_check(uid):
        return
    
    # نبحث في theft_attempts عن المحاولات اللي الـ hash فيها /start أو نص قصير
    bug_attempts = list(db.theft_attempts.find({
        '$or': [
            {'transaction_id': {'$regex': '^/'}},
            {'transaction_id': {'$regex': '^start'}},
            {'transaction_id': {'$lte': 'aaaaaa'}}
        ]
    }))
    
    if not bug_attempts:
        bot.send_message(message.chat.id, "✅ ما في ضحايا للـ bug.")
        return
    
    unbanned_users = set()
    refunded_amounts = {}
    
    for attempt in bug_attempts:
        # نجمع IDs
        original_id = attempt.get('original_user_id') or attempt.get('original_claimer')
        thief_id = attempt.get('thief_user_id') or attempt.get('thief_attempt')
        seized = float(attempt.get('seized_balance', 0))
        
        for victim_id in [original_id, thief_id]:
            if not victim_id:
                continue
            try:
                victim_id = int(victim_id)
                # فك الحظر
                db.users.update_one(
                    {'user_id': victim_id},
                    {'$set': {'is_banned': 0}}
                )
                unbanned_users.add(victim_id)
                
                # إرجاع الرصيد لو فيه
                if victim_id == original_id and seized > 0:
                    refund = refunded_amounts.get(victim_id, 0) + seized
                    refunded_amounts[victim_id] = refund
                    db.users.update_one(
                        {'user_id': victim_id},
                        {'$inc': {'balance': seized}}
                    )
                    
                # نرسل رسالة للمستخدم
                try:
                    bot.send_message(
                        victim_id,
                        "✅ <b>تم فك حظر حسابك!</b>\n\n"
                        "🙏 نعتذر، كان هناك خطأ تقني في النظام حُظر بسببه حسابك.\n\n"
                        "💼 رصيدك مُرجع بالكامل.\n\n"
                        "<i>يمكنك استخدام البوت بشكل طبيعي الآن.</i>",
                        parse_mode="HTML"
                    )
                except: pass
            except Exception as e:
                logger.error(f"Failed to unban {victim_id}: {e}")
    
    # نحذف الـ records الخاطئة من theft_attempts
    db.theft_attempts.delete_many({
        '$or': [
            {'transaction_id': {'$regex': '^/'}},
            {'transaction_id': {'$regex': '^start'}}
        ]
    })
    
    # تقرير
    report = (
        f"✅ <b>تم فك حظر الضحايا!</b>\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"👥 عدد المستخدمين اللي فُك حظرهم: <b>{len(unbanned_users)}</b>\n"
        f"💰 إجمالي المبالغ المُرجعة: <b>${sum(refunded_amounts.values()):.2f}</b>\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"📋 <b>التفاصيل:</b>\n"
    )
    for u in list(unbanned_users)[:20]:
        refund = refunded_amounts.get(u, 0)
        report += f"   • <code>{u}</code>"
        if refund > 0:
            report += f" (+${refund:.2f})"
        report += "\n"
    
    if len(unbanned_users) > 20:
        report += f"\n... و {len(unbanned_users) - 20} مستخدم آخر"
    
    bot.send_message(message.chat.id, report, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "admin_panel_main")
@admin_required
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
        # 🆕 إعدادات نظام الإحالات
        markup.add(InlineKeyboardButton("👥 إعدادات الإحالات", callback_data="ad_ref_settings"))
        # 🛡 زر الحماية ضد سرقة الحوالات
        protection_on = is_amount_protection_enabled()
        protection_label = "✅ مفعّلة (موصى به)" if protection_on else "⚠️ معطّلة (خطر!)"
        markup.add(InlineKeyboardButton(f"🛡 حماية الإيداعات: {protection_label}", callback_data="toggle_amount_protection"))
        markup.add(InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="main_menu_refresh"))
        text = "👑 <b>لوحة القيادة (الإدارة):</b>"
        
    try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

# ============================================================
# ✏️ نظام تخصيص نصوص البوت والأزرار المتقدم (CMS)
# ============================================================
@bot.callback_query_handler(func=lambda call: call.data == "ad_texts_main")
@admin_required
def ad_texts_main_ui(call):
    bot.answer_callback_query(call.id)
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("📝 نصوص الرسائل", callback_data="ad_cms_msgs"),
               InlineKeyboardButton("🎛 أزرار البوت", callback_data="ad_cms_btns_cats"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    bot.edit_message_text("✏️ <b>نظام التخصيص (CMS):</b>\nاختر ماذا تريد أن تخصص:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "ad_cms_msgs")
@admin_required
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
@admin_required
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

@safe_next_step
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

@safe_next_step
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
@admin_required
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

@safe_next_step
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
@admin_required
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
@admin_required
def admin_set_userbot_vars(call):
    bot.answer_callback_query(call.id)
    key = 'userbot_session' if call.data == "ad_set_session" else 'provider_bot'
    msg = bot.send_message(call.message.chat.id, f"📝 <b>أرسل القيمة الجديدة لـ ({key}):</b>", parse_mode="HTML")
    
    def save_and_restart(message):
        if not hasattr(message, 'text') or hasattr(message, 'data'):
            return
        db.settings.update_one({'key': key}, {'$set': {'value': message.text.strip()}}, upsert=True)
        bot.send_message(message.chat.id, "✅ تم حفظ الإعداد بنجاح. جاري إعادة تشغيل اليوزربوت في الخلفية...")
        start_dynamic_userbot()
        
    bot.register_next_step_handler(msg, save_and_restart)

@bot.callback_query_handler(func=lambda call: call.data == "ad_gh_credits")
@admin_required
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
@admin_required
def admin_set_price(call):
    bot.answer_callback_query(call.id)
    key = 'github_price' if call.data == "ad_gh_price" else 'gemini_price'
    msg = bot.send_message(call.message.chat.id, "💰 <b>أرسل السعر الجديد بالدولار ($):</b>", parse_mode="HTML")
    
    def save_price(message):
        if not hasattr(message, 'text') or hasattr(message, 'data'):
            return
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
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
            custom_emoji_id = p.get('custom_emoji_id')
            users = list(db.users.find())
            logger.info(f"📢 جاري الإرسال لـ {len(users)} مستخدم")
            success = 0
            fail = 0
            for u in users:
                try:
                    uid_u = u['user_id']
                    u_lang = u.get('lang', 'ar') if u.get('lang_chosen') else 'en'
                    if u_lang not in ['ar', 'en']: u_lang = 'en'
                    
                    # اسم المنتج مع الإيموجي المميز (للرسالة)
                    icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> ' if custom_emoji_id else '📦 '
                    p_name = icon_html + clean_name(p.get(f'name_{u_lang}', p.get('name_en', '')))
                    p_name_plain = clean_name(p.get(f'name_{u_lang}', p.get('name_en', '')))
                    
                    alert_msg = get_text(uid_u, 'new_stock', p_name, stk_total)
                    
                    # 🟢 زر الشراء الأخضر (Bot API 9.4)
                    markup = InlineKeyboardMarkup()
                    btn_label = f"🛒 {p_name_plain}"
                    
                    buy_btn = CustomInlineButton(
                        text=btn_label,
                        callback_data=f"buy_qty_{pid_for_thread}",
                        style="success",
                        icon_custom_emoji_id=custom_emoji_id if custom_emoji_id else None
                    )
                    markup.add(buy_btn)
                    
                    bot.send_message(uid_u, alert_msg, parse_mode="HTML", reply_markup=markup)
                    success += 1
                    time.sleep(0.05)
                except Exception as send_err:
                    fail += 1
                    logger.debug(f"فشل الإرسال للمستخدم {u.get('user_id')}: {send_err}")
            
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
@admin_required
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
@admin_required
def ad_users_main_ui(call):
    bot.answer_callback_query(call.id)
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("🔍 بحث عن مستخدم", callback_data="ad_u_search"),
               InlineKeyboardButton("🏆 أعلى 10 أرصدة", callback_data="ad_u_top"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    try: bot.edit_message_text("👥 <b>إدارة العملاء والأرصدة:</b>\nاختر العملية المطلوبة:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_u_top")
@admin_required
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
@admin_required
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

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_uh_dep_"))
def ad_uh_dep_handler(call):
    """🛡 عرض كل إيداعات المستخدم للأدمن (مع الهاش الكامل)"""
    bot.answer_callback_query(call.id)
    target_uid = int(call.data.replace("ad_uh_dep_", ""))
    
    # نجلب كل الإيداعات (مو 10 فقط)
    recs = list(db.used_transactions.find({'user_id': target_uid}).sort('_id', -1))
    
    if not recs:
        text = f"📭 <b>لا توجد إيداعات لهذا المستخدم</b>\n\n🆔 ID: <code>{target_uid}</code>"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"ad_u_det_{target_uid}"))
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        except: pass
        return
    
    # إجمالي
    total_amount = sum([float(r.get('amount', 0)) for r in recs])
    
    # نجيب معلومات المستخدم
    u = get_user_data_full(target_uid)
    uname = f"@{u['username']}" if u and u.get('username') else "بدون"
    
    text = (
        f"💳 <b>سجل إيداعات المستخدم</b>\n\n"
        f"👤 المستخدم: {uname}\n"
        f"🆔 ID: <code>{target_uid}</code>\n"
        f"📊 عدد الإيداعات: <b>{len(recs)}</b>\n"
        f"💰 الإجمالي: <b>${total_amount:.2f}</b>\n\n"
    )
    
    # نعرض آخر 15 إيداع (لتجنب تجاوز حد الرسالة)
    display_count = min(len(recs), 15)
    for i, r in enumerate(recs[:display_count], 1):
        date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
        amount = r.get('amount', 0)
        method = r.get('method', 'غير محدد')
        tx_id = r.get('transaction_id', '')
        
        text += (
            f"━━━━━━━━━━━━━━\n"
            f"#{i} ✅\n"
            f"💰 <b>المبلغ:</b> <code>${amount:.2f}</code>\n"
            f"📅 <b>التاريخ:</b> <code>{date_str}</code>\n"
            f"💳 <b>الطريقة:</b> {method}\n"
            f"🆔 <b>رقم العملية:</b>\n<code>{tx_id}</code>\n"
        )
    
    text += "━━━━━━━━━━━━━━"
    
    if len(recs) > display_count:
        text += f"\n\n📌 <i>عُرضت آخر {display_count} من أصل {len(recs)} إيداع</i>"
    
    markup = InlineKeyboardMarkup()
    # زر تحميل ملف كامل لو فيه كثير إيداعات
    if len(recs) > 5:
        markup.add(InlineKeyboardButton("📄 تحميل السجل الكامل (ملف)", callback_data=f"ad_dldep_{target_uid}"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"ad_u_det_{target_uid}"))
    
    try:
        # نشيك إن الرسالة مو طويلة جداً
        if len(text) > 4000:
            # نرسل ملف بدلاً منها
            content = f"=== سجل إيداعات المستخدم {target_uid} ===\n"
            content += f"اليوزر: {uname}\n"
            content += f"العدد: {len(recs)}\n"
            content += f"الإجمالي: ${total_amount:.2f}\n"
            content += "=" * 50 + "\n\n"
            for i, r in enumerate(recs, 1):
                date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
                content += f"#{i}\n"
                content += f"التاريخ: {date_str}\n"
                content += f"المبلغ: ${r.get('amount', 0):.2f}\n"
                content += f"الطريقة: {r.get('method', 'غير محدد')}\n"
                content += f"الهاش: {r.get('transaction_id', '')}\n"
                content += "-" * 40 + "\n"
            
            f = io.BytesIO(content.encode('utf-8'))
            f.name = f"deposits_{target_uid}.txt"
            bot.send_document(call.message.chat.id, f, caption=f"📄 سجل إيداعات المستخدم {target_uid}")
        else:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error showing deposits to admin: {e}")
        try:
            bot.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode="HTML")
        except:
            bot.send_message(call.message.chat.id, "❌ تعذر عرض السجل، حاول مرة ثانية.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_uh_buy_"))
def ad_uh_buy_handler(call):
    """🛡 عرض كل مشتريات المستخدم للأدمن"""
    bot.answer_callback_query(call.id)
    target_uid = int(call.data.replace("ad_uh_buy_", ""))
    
    recs = list(db.orders.find({'user_id': target_uid}).sort('_id', -1))
    
    if not recs:
        text = f"📭 <b>لا توجد مشتريات لهذا المستخدم</b>\n\n🆔 ID: <code>{target_uid}</code>"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"ad_u_det_{target_uid}"))
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        except: pass
        return
    
    # نجمعها حسب المنتج
    grouped = {}
    for r in recs:
        pid = str(r.get('product_id', ''))
        if pid not in grouped:
            grouped[pid] = []
        grouped[pid].append(r)
    
    u = get_user_data_full(target_uid)
    uname = f"@{u['username']}" if u and u.get('username') else "بدون"
    
    text = (
        f"🛍 <b>سجل مشتريات المستخدم</b>\n\n"
        f"👤 المستخدم: {uname}\n"
        f"🆔 ID: <code>{target_uid}</code>\n"
        f"📊 إجمالي الطلبات: <b>{len(recs)}</b>\n"
        f"📦 المنتجات المختلفة: <b>{len(grouped)}</b>\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"📋 <b>تفاصيل حسب المنتج:</b>\n\n"
    )
    
    for pid, orders in grouped.items():
        if pid in ['GitHub_Student', 'Gemini_Activation']:
            p_name = pid.replace('_', ' ')
        else:
            p = find_product(pid)
            p_name = clean_name(p.get('name_ar', p.get('name_en', 'منتج'))) if p else "منتج محذوف"
        
        text += f"📦 <b>{p_name}</b>\n   • العدد: <b>{len(orders)}</b>\n\n"
    
    text += "━━━━━━━━━━━━━━"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📄 تحميل السجل الكامل (ملف)", callback_data=f"ad_dlbuy_{target_uid}"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"ad_u_det_{target_uid}"))
    
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error showing purchases to admin: {e}")


@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_dldep_"))
def ad_dldep_handler(call):
    """🛡 تحميل ملف كامل بكل إيداعات المستخدم"""
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز الملف...")
    target_uid = int(call.data.replace("ad_dldep_", ""))
    
    recs = list(db.used_transactions.find({'user_id': target_uid}).sort('_id', -1))
    if not recs:
        bot.send_message(call.message.chat.id, "📭 لا توجد إيداعات.")
        return
    
    u = get_user_data_full(target_uid)
    uname = f"@{u['username']}" if u and u.get('username') else "بدون"
    total_amount = sum([float(r.get('amount', 0)) for r in recs])
    
    content = f"=== سجل إيداعات المستخدم ===\n"
    content += f"ID: {target_uid}\n"
    content += f"Username: {uname}\n"
    content += f"عدد الإيداعات: {len(recs)}\n"
    content += f"الإجمالي: ${total_amount:.2f}\n"
    content += f"تاريخ التصدير: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    content += "=" * 50 + "\n\n"
    
    for i, r in enumerate(recs, 1):
        date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
        content += f"#{i}\n"
        content += f"التاريخ: {date_str}\n"
        content += f"المبلغ: ${r.get('amount', 0):.2f}\n"
        content += f"الطريقة: {r.get('method', 'غير محدد')}\n"
        content += f"رقم العملية: {r.get('transaction_id', '')}\n"
        content += "-" * 50 + "\n"
    
    try:
        f = io.BytesIO(content.encode('utf-8'))
        f.name = f"deposits_{target_uid}.txt"
        bot.send_document(call.message.chat.id, f, caption=f"📄 <b>سجل إيداعات المستخدم</b>\n\n👤 ID: <code>{target_uid}</code>\n💰 الإجمالي: <b>${total_amount:.2f}</b>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error sending deposits file: {e}")
        bot.send_message(call.message.chat.id, "❌ فشل إرسال الملف.")


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
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
