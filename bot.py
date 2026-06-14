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
import json
import urllib.parse
import base64
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

# ===== ChatGPT Business Seat Manager =====
try:
    from curl_cffi import requests as cffi_requests
    CFFI_AVAILABLE = True
except ImportError:
    CFFI_AVAILABLE = False
    logger_placeholder = None  # سيُعيَّن لاحقاً

import uuid as _uuid_mod
import datetime as _dt_mod
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
# ============================================================
# 🔗 API بسيط للمنتجات - يسمح لأي شخص يعرض منتجاتك في بوته/موقعه
# ============================================================

def _get_api_gateway():
    """ينشئ مسار سري للـ API - كل مستخدم له مسار مختلف"""
    try:
        s = db.settings.find_one({'key': 'api_secret_path'})
        if s and s.get('value'):
            return s['value']
    except: pass
    secret = secrets.token_hex(16)
    try:
        db.settings.update_one({'key': 'api_secret_path'}, {'$set': {'value': secret}}, upsert=True)
    except: pass
    return secret


def _get_server_url():
    """يكشف رابط السيرفر تلقائي من متغيرات المنصة"""
    for var in ['RENDER_EXTERNAL_URL', 'RAILWAY_PUBLIC_DOMAIN', 'KOYEB_PUBLIC_DOMAIN', 'HEROKU_APP_NAME', 'FLY_APP_NAME', 'APP_URL', 'BASE_URL']:
        val = os.getenv(var, '')
        if val:
            if not val.startswith('http'):
                val = f"https://{val}"
            return val.rstrip('/')
    return None


def _generate_connection_code(api_key):
    """يولّد كود اتصال مشفّر فيه كل المعلومات"""
    server_url = _get_server_url()
    gw = _get_api_gateway()
    if not server_url:
        return None
    full_url = f"{server_url}/{gw}"
    data = json.dumps({"k": api_key, "u": full_url}, separators=(',', ':'))
    encoded = base64.b64encode(data.encode()).decode()
    return f"conn_{encoded}"


def _generate_api_key():
    return f"sk_{secrets.token_hex(24)}"

def _get_api_user(api_key):
    try:
        doc = db.api_keys.find_one({'api_key': api_key, 'is_active': True})
        if not doc: return None, None
        return doc, get_user_data_full(doc['user_id'])
    except: return None, None

def _json_resp(h, code, data):
    h.send_response(code)
    h.send_header('Content-Type', 'application/json; charset=utf-8')
    h.send_header('Access-Control-Allow-Origin', '*')
    h.send_header('Access-Control-Allow-Headers', 'Authorization, Content-Type')
    h.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    h.end_headers()
    h.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

def _read_body(h):
    """قراءة body آمنة تدعم Content-Length و chunked encoding"""
    try:
        cl = h.headers.get('Content-Length')
        if cl:
            return h.rfile.read(int(cl)).decode('utf-8')
        # chunked أو بدون Content-Length
        te = h.headers.get('Transfer-Encoding', '').lower()
        if 'chunked' in te:
            chunks = []
            while True:
                line = h.rfile.readline().decode('utf-8').strip()
                size = int(line, 16)
                if size == 0:
                    break
                chunks.append(h.rfile.read(size).decode('utf-8'))
                h.rfile.readline()
            return ''.join(chunks)
        return ''
    except Exception:
        return ''

# Rate limiter بسيط: max 30 طلب/دقيقة لكل API key
_api_rate = {}
_api_rate_lock = __import__('threading').Lock()

def _check_rate_limit(api_key):
    """True = مسموح، False = محظور مؤقتاً"""
    import time as _t
    now = _t.time()
    window = 60  # ثانية
    max_req = 30
    with _api_rate_lock:
        if api_key not in _api_rate:
            _api_rate[api_key] = []
        # نحذف الطلبات القديمة
        _api_rate[api_key] = [ts for ts in _api_rate[api_key] if now - ts < window]
        if len(_api_rate[api_key]) >= max_req:
            return False
        _api_rate[api_key].append(now)
        return True


def _extract_emoji_ids_from_text(text):
    """🌟 يستخرج كل معرّفات الإيموجي المميز من نص يحتوي على وسوم <tg-emoji emoji-id="...">"""
    if not text:
        return []
    try:
        return re.findall(r'<tg-emoji\s+emoji-id="([^"]+)"', str(text))
    except Exception:
        return []


def _build_emoji_fields(pr, name_custom_emoji_id, desc_ar, desc_en):
    """
    🌟 يبني كل حقول الإيموجي المميز لمنتج (للاسم والوصف معاً).
    يرجّع dict جاهز يُدمج في رد الـ API.
    - desc_ar_html / desc_en_html: الوصف كامل جاهز للعرض مع parse_mode=HTML
    - desc_ar / desc_en: الوصف بدون تعديل (نص خام)
    """
    desc_ar_ids = _extract_emoji_ids_from_text(desc_ar)
    desc_en_ids = _extract_emoji_ids_from_text(desc_en)
    desc_ids = list(dict.fromkeys(desc_ar_ids + desc_en_ids))
    all_ids = []
    if name_custom_emoji_id:
        all_ids.append(str(name_custom_emoji_id))
    all_ids = list(dict.fromkeys(all_ids + desc_ids))

    # الوصف يحتوي أصلاً على وسوم <tg-emoji> — نُرجعه كما هو جاهزاً للعرض
    # لو ما فيه إيموجي يُرجع نص عادي أيضاً، لا فرق
    return {
        'desc_ar_html': desc_ar or '',       # جاهز للعرض المباشر في تيليغرام بـ parse_mode=HTML
        'desc_en_html': desc_en or '',       # جاهز للعرض المباشر في تيليغرام بـ parse_mode=HTML
        'desc_has_premium_emoji': bool(desc_ids),
        'desc_emoji_ids': desc_ids,
        'all_emoji_ids': all_ids
    }


def _build_api_owner_profile(uid):
    """
    🔍 يبني نص بيانات صاحب الـ API للإشعار (الأونر فقط):
    - الاسم الكامل + لينك البروفايل (قابل للنقر)
    - اليوزرنيم (لو موجود)
    - الأيدي + الرصيد الحالي
    """
    try:
        u = db.users.find_one({'user_id': uid}) or {}
        api_doc = db.api_keys.find_one({'user_id': uid, 'is_active': True}) or {}

        name = u.get('name', '') or api_doc.get('username', str(uid))
        username = u.get('username', '') or api_doc.get('username', '')
        balance = round(float(u.get('balance', 0)), 2) if u else 0.0

        # 🆙 محاولة جلب الاسم الحالي من تيليجرام (أحدث من DB)
        try:
            chat = bot.get_chat(uid)
            full_name = chat.first_name or ''
            if chat.last_name:
                full_name += f' {chat.last_name}'
            if full_name.strip():
                name = full_name.strip()
            if chat.username:
                username = chat.username.lower()
        except Exception:
            pass

        safe_name = html.escape(name or str(uid))

        # لينك بروفايل قابل للنقر (يشتغل حتى بدون يوزرنيم)
        profile_link = f'<a href="tg://user?id={uid}">{safe_name}</a>'

        lines = [
            f"👤 <b>صاحب الـ API:</b>",
            f"   🔗 {profile_link}",
            f"   🆔 <code>{uid}</code>",
        ]
        if username:
            lines.append(
                f'   📛 <a href="https://t.me/{username}">@{username}</a>'
            )
        lines.append(f"   💰 رصيده الحالي: <b>${balance:.2f}</b>")

        return "\n".join(lines)
    except Exception as e:
        logger.debug(f"_build_api_owner_profile error: {e}")
        return f"👤 API User: <code>{uid}</code>"


def _notify_all_admins_api_purchase(uid, pr, qty, total, order_id, buyer_info, is_manual=False):
    """
    يُرسل إشعار شراء API لكل الأدمن والأونر.
    uid = صاحب الـ API key
    pr  = المنتج
    """
    try:
        api_profile = _build_api_owner_profile(uid)
        product_name = clean_name(pr.get('name_ar') or pr.get('name_en', ''))
        custom_emoji_id = pr.get('custom_emoji_id', '')
        product_display = (
            f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> <b>{product_name}</b>'
            if custom_emoji_id else f'📦 <b>{product_name}</b>'
        )
        status_line = "⚠️ <i>يدوي — التسليم مطلوب</i>" if is_manual else "⚡ <i>تلقائي — تم التسليم فوراً</i>"
        type_label = "🔄 <b>API — يدوي</b>" if is_manual else "🤖 <b>API — تلقائي</b>"

        msg = (
            f"\U0001f6d2 {type_label}\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"{api_profile}\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"\U0001f4e6 <b>\u0627\u0644\u0645\u0646\u062a\u062c:</b> {product_display}\n"
            f"\U0001f522 <b>\u0627\u0644\u0643\u0645\u064a\u0629:</b> <b>{qty}</b>\n"
            f"\U0001f4b0 <b>\u0627\u0644\u0625\u062c\u0645\u0627\u0644\u064a:</b> <b>${total:.2f}</b>\n"
            f"\U0001f464 <b>\u0628\u064a\u0627\u0646\u0627\u062a \u0627\u0644\u0645\u0634\u062a\u0631\u064a:</b> {buyer_info or 'N/A'}\n"
            f"\U0001f194 <b>\u0631\u0642\u0645 \u0627\u0644\u0637\u0644\u0628:</b> <code>{order_id}</code>\n"
            f"{status_line}"
        )
        notify_admins(msg)
    except Exception as _ne:
        logger.debug(f"_notify_all_admins_api_purchase error: {_ne}")


# ================================================================
# ChatGPT Business Seat Manager — مدمج مع البوت
# ================================================================

class ChatGPTSeatManager:
    """
    مدير مقاعد ChatGPT Business.
    يعمل كـ singleton — استخدم get_cgpt_manager().
    """
    _instance = None

    def __init__(self):
        _raw_path = get_setting('cgpt_token_path')
        self.token_file = _raw_path if _raw_path and _raw_path != 'Not Set' else 'pasted_content.txt'
        self.data_file  = get_setting('cgpt_data_file') or 'cookie_seat_invites.json'
        self.access_token  = None
        self.session_token = None
        self.org_id        = None
        self.account_id    = None
        self.owner_email   = None
        self._loaded       = False
        self._load_tokens()
        self.invites_data  = self._load_data()
        self.allowed_emails = set(self.invites_data.get('allowed_emails', []))

    # ---------- token / data ----------
    def _load_tokens(self):
        # 1) نحاول من قاعدة البيانات أولاً (دائم على Render)
        try:
            db_doc = db.cgpt_cookies.find_one({'_id': 'main'})
            if db_doc and db_doc.get('data'):
                c = db_doc['data']
                self.access_token  = c.get('accessToken')
                self.session_token = c.get('sessionToken')
                acct = c.get('account', {})
                self.org_id     = acct.get('organizationId')
                self.account_id = acct.get('id')
                user = c.get('user', {})
                self.owner_email = user.get('email')
                self._loaded = bool(self.access_token and self.session_token)
                if self._loaded:
                    logger.info("[CGPT] Loaded tokens from DB")
                    return
        except Exception as db_err:
            logger.debug(f"[CGPT] DB load failed: {db_err}")

        # لا توجد كوكيز في DB
        logger.info("[CGPT] No cookies in DB yet — admin must add them")
        self._loaded = False

    def _headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type':  'application/json',
            'Cookie':        f'__Secure-next-auth.session-token={self.session_token}',
            'User-Agent':    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept':        'application/json',
            'Origin':        'https://chatgpt.com',
            'Referer':       'https://chatgpt.com/'
        }

    def _load_data(self):
        """تحميل بيانات الدعوات من MongoDB"""
        try:
            doc = db.cgpt_invites_data.find_one({'_id': 'main'})
            if doc and doc.get('data'):
                return doc['data']
        except Exception as e:
            logger.debug(f"[CGPT] load_data DB err: {e}")
        return {'invites': {}, 'allowed_emails': [self.owner_email] if self.owner_email else []}

    def _save_data(self):
        """حفظ بيانات الدعوات في MongoDB"""
        self.invites_data['allowed_emails'] = list(self.allowed_emails)
        try:
            db.cgpt_invites_data.update_one(
                {'_id': 'main'},
                {'$set': {'data': self.invites_data}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"[CGPT] save_data DB err: {e}")

    def reload_token(self):
        """إعادة تحميل الـ token من MongoDB"""
        self._load_tokens()
        return self._loaded

    # ---------- core API ----------
    def _get_org_users(self):
        if not CFFI_AVAILABLE or not self._loaded: return []
        try:
            url = f'https://chatgpt.com/backend-api/accounts/{self.account_id}/users'
            r = cffi_requests.get(url, headers=self._headers(), impersonate='chrome110', timeout=15)
            if r.status_code == 200:
                return r.json().get('items', [])
            logger.warning(f"[CGPT] get_org_users {r.status_code}: {r.text[:200]}")
        except Exception as e:
            logger.error(f"[CGPT] get_org_users error: {e}")
        return []

    def invite_user(self, email: str, minutes_valid: int) -> dict:
        """
        يدعو مستخدم ويرجع dict:
        {'ok': True/False, 'expires_at': '...', 'error': '...'}
        """
        if not CFFI_AVAILABLE:
            return {'ok': False, 'error': 'curl_cffi غير مثبتة'}
        if not self._loaded:
            return {'ok': False, 'error': 'لم يتم تحميل الـ token'}
        try:
            url = f'https://chatgpt.com/backend-api/accounts/{self.account_id}/invites'
            payload = {'email_addresses': [email], 'role': 'standard-user'}
            r = cffi_requests.post(url, headers=self._headers(), json=payload,
                                   impersonate='chrome110', timeout=15)
            if r.status_code in [200, 201]:
                expires_at = (_dt_mod.datetime.now() +
                              _dt_mod.timedelta(minutes=minutes_valid)).isoformat()
                self.invites_data['invites'][email] = {
                    'invited_at': _dt_mod.datetime.now().isoformat(),
                    'expires_at': expires_at,
                    'status': 'active',
                    'minutes': minutes_valid,
                    'telegram_uid': getattr(self, '_last_buyer_uid', None)
                }
                self.allowed_emails.add(email)
                self._save_data()
                return {'ok': True, 'expires_at': expires_at}
            else:
                err = r.text[:300]
                logger.error(f"[CGPT] invite failed {r.status_code}: {err}")
                return {'ok': False, 'error': f'{r.status_code}: {err}'}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def remove_user_by_email(self, email: str) -> bool:
        """يحذف مستخدم بالإيميل"""
        users = self._get_org_users()
        for u in users:
            if u.get('email') == email:
                return self._remove_user(u.get('id'), email)
        return False

    def _remove_user(self, user_id, email) -> bool:
        if email == self.owner_email: return False
        try:
            url = f'https://chatgpt.com/backend-api/accounts/{self.account_id}/users/{user_id}'
            r = cffi_requests.delete(url, headers=self._headers(), impersonate='chrome110', timeout=15)
            return r.status_code in [200, 204]
        except Exception as e:
            logger.error(f"[CGPT] remove error: {e}")
            return False

    def check_and_cleanup(self):
        """فحص المنتهين والمخالفين وحذفهم"""
        now = _dt_mod.datetime.now()
        emails_to_remove = []

        # منتهو الصلاحية
        for email, info in self.invites_data['invites'].items():
            if info.get('status') != 'active': continue
            try:
                exp = _dt_mod.datetime.fromisoformat(info['expires_at'])
                if now > exp:
                    emails_to_remove.append(email)
                    self.allowed_emails.discard(email)
            except: pass

        current_users = self._get_org_users()

        # مخالفون (دُعوا خارج البوت)
        unauthorized = [u.get('email') for u in current_users
                        if u.get('email') != self.owner_email
                        and u.get('email') not in self.allowed_emails
                        and u.get('email') not in emails_to_remove]

        if unauthorized:
            logger.warning(f"[CGPT] {len(unauthorized)} unauthorized users — punishing all active")

            # بناء تقرير كامل: إيميل المخالف + إيميل العميل المسبب
            violation_lines = []
            for unauth_email in unauthorized:
                violation_lines.append(f"  🔴 مضاف خارج البوت: <code>{unauth_email}</code>")

            # نعاقب كل النشطين، نرسل لهم رسالة، ونسجلهم
            punished_lines = []
            for email, info in self.invites_data['invites'].items():
                if info.get('status') == 'active':
                    emails_to_remove.append(email)
                    self.allowed_emails.discard(email)
                    punished_lines.append(f"  👤 عميل مُعاقب: <code>{email}</code>")
                    # إرسال رسالة مباشرة للعميل المخالف في تيليغرام
                    tg_uid = info.get('telegram_uid')
                    if tg_uid:
                        try:
                            violation_msg = (
                                "🚫 <b>تم إلغاء وصولك إلى ChatGPT Business</b>\n\n"
                                "❌ <b>السبب: مخالفة شروط الاستخدام</b>\n\n"
                                f"تم رصد إضافة مستخدم غير مصرح به من حسابك:\n"
                                + "\n".join([f"📧 <code>{ue}</code>" for ue in unauthorized]) +
                                "\n\n"
                                "⛔️ <b>قرار نهائي:</b>\n"
                                "• تم حذف وصولك فوراً\n"
                                "• <b>لن يتم تعويضك أو استرجاع أموالك</b>\n"
                                "• لا يحق لك المطالبة بأي تعويض\n\n"
                                "📜 لأنك خالفت القوانين المنصوص عليها عند الشراء، "
                                "فقدت حق الحماية والضمان."
                            )
                            bot.send_message(tg_uid, violation_msg, parse_mode="HTML")
                        except Exception as _nm:
                            logger.debug(f"[CGPT] notify violated user {tg_uid} failed: {_nm}")

            report = (
                f"⚠️ <b>ChatGPT — مخالفة اكتُشفت!</b>\n\n"
                f"<b>المستخدمون المضافون خارج البوت ({len(unauthorized)}):</b>\n"
                + "\n".join(violation_lines) +
                f"\n\n<b>العملاء الذين تم إلغاء صلاحيتهم ({len(punished_lines)}):</b>\n"
                + ("\n".join(punished_lines) if punished_lines else "  لا يوجد") +
                f"\n\n<i>تم حذف جميع المضافين وإلغاء كل الصلاحيات النشطة.</i>"
            )
            notify_admins(report)

        # تنفيذ الحذف
        for u in current_users:
            uid_org, email = u.get('id'), u.get('email')
            if email == self.owner_email: continue
            if email in emails_to_remove or email in unauthorized:
                ok = self._remove_user(uid_org, email)
                if ok and email in self.invites_data['invites']:
                    self.invites_data['invites'][email]['status'] = 'expired'
                    self.invites_data['invites'][email]['removed_at'] = now.isoformat()

        self._save_data()

    def list_active(self):
        """يرجع قائمة المدعوين النشطين"""
        now = _dt_mod.datetime.now()
        result = []
        for email, info in self.invites_data['invites'].items():
            if info.get('status') != 'active': continue
            try:
                exp = _dt_mod.datetime.fromisoformat(info['expires_at'])
                remaining = exp - now
                result.append({
                    'email': email,
                    'expires_at': info['expires_at'],
                    'remaining_hours': max(0, int(remaining.total_seconds() // 3600)),
                    'remaining_days':  max(0, remaining.days)
                })
            except: pass
        return result

    def get_stats(self):
        invites = self.invites_data.get('invites', {})
        total   = len(invites)
        active  = sum(1 for i in invites.values() if i.get('status') == 'active')
        expired = total - active
        return {'total': total, 'active': active, 'expired': expired}


# Singleton accessor
_cgpt_manager_instance = None
_cgpt_lock = __import__('threading').Lock()

def get_cgpt_manager() -> ChatGPTSeatManager:
    global _cgpt_manager_instance
    with _cgpt_lock:
        if _cgpt_manager_instance is None:
            _cgpt_manager_instance = ChatGPTSeatManager()
        return _cgpt_manager_instance


def _cgpt_daemon_loop():
    """Daemon thread يعمل كل 5 دقائق"""
    import time as _t
    logger.info("[CGPT] Daemon started")
    while True:
        try:
            # نجلب الـ interval بأمان — لو 'Not Set' أو أي قيمة غير رقمية نستخدم 300
            raw_interval = get_setting('cgpt_check_interval')
            try:
                interval = int(raw_interval) if str(raw_interval).isdigit() else 300
            except (ValueError, TypeError):
                interval = 300
            _t.sleep(interval)
            mgr = get_cgpt_manager()
            mgr.check_and_cleanup()
        except Exception as e:
            logger.error(f"[CGPT] daemon error: {e}")
            _t.sleep(300)  # لو صار خطأ ننام 5 دقائق بدل ما نعيد فوراً

# تشغيل الـ daemon في thread خلفية عند استيراد البوت
_cgpt_daemon_thread = __import__('threading').Thread(
    target=_cgpt_daemon_loop, daemon=True, name="cgpt_seat_daemon"
)
_cgpt_daemon_thread.start()


class APIHandler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', 'Authorization, Content-Type')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.end_headers()

    def _auth(self):
        auth = self.headers.get('Authorization', '')
        if not auth.startswith('Bearer '): return None, None
        key = auth[7:].strip()
        return _get_api_user(key)

    def do_GET(self):
        p = urllib.parse.urlparse(self.path).path.rstrip('/')
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        gw = _get_api_gateway()

        if p in ('', '/'):
            return _json_resp(self, 200, {'status': 'online'})

        # فحص المسار السري — لو ما يطابق، 404 عادي
        if not p.startswith(f'/{gw}'):
            return _json_resp(self, 404, {'error': 'Not found'})

        # نشيل المسار السري من الرابط ونحلل الباقي
        route = p[len(f'/{gw}'):]  # مثلاً /products أو /product/123

        doc, user = self._auth()
        if not doc or not user:
            return _json_resp(self, 401, {'error': 'Invalid API key'})
        uid = doc['user_id']

        if not _check_rate_limit(doc['api_key']):
            return _json_resp(self, 429, {'error': 'Rate limit exceeded. Max 30 requests/minute.'})

        if route == '/products':
            # is_hidden=True من الأدمن = محجوب من الجميع بلا استثناء
            prods = list(db.products.find({'is_hidden': {'$ne': True}}))
            # api_hidden = المطور أخفى المنتج من متجره هو فقط
            hidden_pids = set()
            for h in db.api_hidden.find({'api_user_id': uid}):
                hidden_pids.add(h['product_id'])
            # جلب أسعار المطوّر المخصصة
            custom_prices = {}
            for cp in db.api_pricing.find({'api_user_id': uid}):
                custom_prices[cp['product_id']] = cp
            result = []
            for pr in prods:
                pid = str(pr.get('id', str(pr.get('_id', ''))))
                if pid in hidden_pids:
                    continue
                manual = pr.get('is_manual', False)
                base_price = float(pr.get('price', 0))
                cp = custom_prices.get(pid, {})
                # 🌟 دعم Premium Emoji
                custom_emoji_id = pr.get('custom_emoji_id')
                p_name_ar = cp.get('name_ar') or pr.get('name_ar', '')
                p_name_en = cp.get('name_en') or pr.get('name_en', '')
                p_desc_ar = cp.get('desc_ar') or pr.get('desc_ar', '')
                p_desc_en = cp.get('desc_en') or pr.get('desc_en', '')
                # your_price: من sell_price المقفول، أو null لو لم يُضبط بعد
                your_price = cp.get('sell_price')
                item = {
                    'id': pid,
                    'name_ar': p_name_ar,
                    'name_en': p_name_en,
                    'desc_ar': p_desc_ar,
                    'desc_en': p_desc_en,
                    'store_price': base_price,         # سعر المتجر الحالي (يتغير)
                    'your_price': your_price,          # سعرك المقفول (لا يتغير بتغيير المتجر)
                    'price_locked': your_price is not None,
                    'stock': 'unlimited' if manual else get_product_stock_count(pid),
                    'is_manual': manual,
                    'discount_tiers': pr.get('discount_tiers', []),
                    # 🌟 حقول الإيموجي المميز (Premium Emoji) — الاسم
                    'custom_emoji_id': custom_emoji_id,
                    'has_premium_emoji': bool(custom_emoji_id),
                    'name_ar_html': f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> {p_name_ar}' if custom_emoji_id else p_name_ar,
                    'name_en_html': f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> {p_name_en}' if custom_emoji_id else p_name_en
                }
                # 🌟 إيموجي الوصف (desc_ar/desc_en تحتوي وسوم <tg-emoji> جاهزة للعرض بـ HTML)
                emoji_fields = _build_emoji_fields(pr, custom_emoji_id, p_desc_ar, p_desc_en)
                item.update(emoji_fields)
                # دليل الاستخدام الكامل للمطورين
                item['emoji_guide'] = {
                    'parse_mode': 'HTML',
                    'name_ar_html': item.get('name_ar_html', p_name_ar),
                    'name_en_html': item.get('name_en_html', p_name_en),
                    'desc_ar_html': item.get('desc_ar_html', p_desc_ar),
                    'desc_en_html': item.get('desc_en_html', p_desc_en),
                    'note': 'All _html fields are ready to send directly via Telegram with parse_mode=HTML'
                }
                result.append(item)
            return _json_resp(self, 200, {'success': True, 'products': result})

        if route.startswith('/product/'):
            pid = route.split('/product/')[1]
            pr = find_product(pid)
            if not pr or pr.get('is_hidden'):
                return _json_resp(self, 404, {'error': 'Product not found'})
            pid = str(pr.get('id', str(pr.get('_id', ''))))
            manual = pr.get('is_manual', False)
            base_price = float(pr.get('price', 0))
            cp = db.api_pricing.find_one({'api_user_id': uid, 'product_id': pid}) or {}
            # 🌟 دعم Premium Emoji
            custom_emoji_id = pr.get('custom_emoji_id')
            p_name_ar = cp.get('name_ar') or pr.get('name_ar', '')
            p_name_en = cp.get('name_en') or pr.get('name_en', '')
            p_desc_ar = cp.get('desc_ar') or pr.get('desc_ar', '')
            p_desc_en = cp.get('desc_en') or pr.get('desc_en', '')
            your_price_s = cp.get('sell_price') if cp else None
            product_obj = {
                'id': pid,
                'name_ar': p_name_ar,
                'name_en': p_name_en,
                'desc_ar': p_desc_ar,
                'desc_en': p_desc_en,
                'store_price': base_price,
                'your_price': your_price_s,
                'price_locked': your_price_s is not None,
                'stock': 'unlimited' if manual else get_product_stock_count(pid),
                'is_manual': manual, 'discount_tiers': pr.get('discount_tiers', []),
                # 🌟 حقول الإيموجي المميز (Premium Emoji) — الاسم
                'custom_emoji_id': custom_emoji_id,
                'has_premium_emoji': bool(custom_emoji_id),
                'name_ar_html': f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> {p_name_ar}' if custom_emoji_id else p_name_ar,
                'name_en_html': f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> {p_name_en}' if custom_emoji_id else p_name_en
            }
            # 🌟 إيموجي الوصف (desc_ar/desc_en تحتوي وسوم <tg-emoji> جاهزة للعرض بـ HTML)
            emoji_fields_s = _build_emoji_fields(pr, custom_emoji_id, p_desc_ar, p_desc_en)
            product_obj.update(emoji_fields_s)
            product_obj['emoji_guide'] = {
                'parse_mode': 'HTML',
                'name_ar_html': product_obj.get('name_ar_html', p_name_ar),
                'name_en_html': product_obj.get('name_en_html', p_name_en),
                'desc_ar_html': product_obj.get('desc_ar_html', p_desc_ar),
                'desc_en_html': product_obj.get('desc_en_html', p_desc_en),
                'note': 'All _html fields are ready to send directly via Telegram with parse_mode=HTML'
            }
            return _json_resp(self, 200, {'success': True, 'product': product_obj})

        if route == '/balance':
            u = get_user_data_full(uid)
            return _json_resp(self, 200, {'success': True, 'balance': round(u.get('balance', 0), 2), 'user_id': uid})

        if route == '/orders':
            limit = min(int(params.get('limit', ['20'])[0]), 100)
            orders = list(db.api_orders.find({'api_user_id': uid}).sort('_id', -1).limit(limit))
            result = []
            for o in orders:
                try:
                    order_date = o['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    order_date = o.get('date', '')
                result.append({
                    'order_id': o.get('order_id', ''),
                    'product_id': o.get('product_id', ''),
                    'product_name': o.get('product_name', ''),
                    'qty': o.get('qty', 1),
                    'unit_price': round(o.get('total_price', 0) / max(o.get('qty', 1), 1), 2),
                    'total_price': o.get('total_price', 0),
                    'codes': o.get('codes', []),
                    'codes_count': len(o.get('codes', [])),
                    'status': o.get('status', 'completed'),
                    'buyer_info': o.get('buyer_info', ''),
                    'date': order_date
                })
            return _json_resp(self, 200, {'success': True, 'orders': result})

        # GET /my_prices — أسعار المطوّر المخصصة
        if route == '/my_prices':
            customs = list(db.api_pricing.find({'api_user_id': uid}))
            result = []
            for cp in customs:
                pr = find_product(cp['product_id'])
                if not pr: continue
                base = float(pr.get('price', 0))
                sell = cp.get('sell_price', base)
                snap = cp.get('base_price_snapshot', base)
                item = {
                    'product_id': cp['product_id'],
                    'original_name': pr.get('name_ar', ''),
                    'your_name_ar': cp.get('name_ar', ''),
                    'your_name_en': cp.get('name_en', ''),
                    'store_price_now': base,           # سعر المتجر الحالي
                    'store_price_when_set': snap,      # سعر المتجر وقت ما ضبطت سعرك
                    'your_price': sell,                # سعرك المقفول
                    'profit_per_unit': round(sell - base, 2),  # الربح بناءً على سعر المتجر الحالي
                    'price_locked': True,
                    'price_set_at': cp.get('updated_at', '')
                }
                result.append(item)
            return _json_resp(self, 200, {'success': True, 'custom_products': result})

        # GET /order/{id} — جلب تفاصيل طلب واحد
        if route.startswith('/order/'):
            order_id = route.split('/order/')[1]
            o = db.api_orders.find_one({'order_id': order_id, 'api_user_id': uid})
            if not o:
                return _json_resp(self, 404, {'error': 'Order not found'})
            try:
                order_date = o['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                order_date = o.get('date', '')
            return _json_resp(self, 200, {
                'success': True,
                'order': {
                    'order_id': o.get('order_id', ''),
                    'product_id': o.get('product_id', ''),
                    'product_name': o.get('product_name', ''),
                    'qty': o.get('qty', 1),
                    'unit_price': round(o.get('total_price', 0) / max(o.get('qty', 1), 1), 2),
                    'total_price': o.get('total_price', 0),
                    'codes': o.get('codes', []),
                    'codes_count': len(o.get('codes', [])),
                    'status': o.get('status', 'completed'),
                    'buyer_info': o.get('buyer_info', ''),
                    'date': order_date
                }
            })

        # GET /stats — إحصائيات المطور
        if route == '/stats':
            total_orders = db.api_orders.count_documents({'api_user_id': uid})
            completed = db.api_orders.count_documents({'api_user_id': uid, 'status': 'completed'})
            pending = db.api_orders.count_documents({'api_user_id': uid, 'status': 'pending_manual'})
            revenue = sum(o.get('total_price', 0) for o in db.api_orders.find({'api_user_id': uid, 'status': 'completed'}, {'total_price': 1}))
            u_data = get_user_data_full(uid)
            return _json_resp(self, 200, {
                'success': True,
                'balance': round(u_data.get('balance', 0), 2),
                'total_orders': total_orders,
                'completed_orders': completed,
                'pending_orders': pending,
                'total_revenue': round(revenue, 2)
            })

        return _json_resp(self, 404, {'error': 'Not found'})

    def do_POST(self):
        p = urllib.parse.urlparse(self.path).path.rstrip('/')
        gw = _get_api_gateway()

        if not p.startswith(f'/{gw}'):
            return _json_resp(self, 404, {'error': 'Not found'})

        route = p[len(f'/{gw}'):]
        doc, user = self._auth()
        if not doc or not user:
            return _json_resp(self, 401, {'error': 'Invalid API key'})
        uid = doc['user_id']

        if not _check_rate_limit(doc['api_key']):
            return _json_resp(self, 429, {'error': 'Rate limit exceeded. Max 30 requests/minute.'})

        if route == '/purchase':
            try:
                body = json.loads(_read_body(self))
            except: return _json_resp(self, 400, {'error': 'Invalid JSON'})

            product_id = str(body.get('product_id', '')).strip()
            qty = body.get('qty', 1)
            buyer_info = str(body.get('buyer_info', ''))[:200]

            if not product_id: return _json_resp(self, 400, {'error': 'product_id required'})
            try:
                qty = int(qty)
                if qty < 1 or qty > 50: return _json_resp(self, 400, {'error': 'qty: 1-50'})
            except: return _json_resp(self, 400, {'error': 'qty must be integer'})

            pr = find_product(product_id)
            if not pr or pr.get('is_hidden'):
                return _json_resp(self, 404, {'error': 'Product not found'})

            pid = str(pr.get('id', str(pr.get('_id', ''))))
            is_manual = pr.get('is_manual', False)

            if not is_manual and get_product_stock_count(pid) < qty:
                return _json_resp(self, 409, {'error': 'Not enough stock', 'available': get_product_stock_count(pid)})

            # سعر الشراء الفعلي من المتجر (base) — يُستخدم للخصم من رصيد المطور
            cp_doc = db.api_pricing.find_one({'api_user_id': uid, 'product_id': pid}) or {}
            store_price = float(pr.get('price', 0))
            # الكمية → تطبيق خصومات المتجر على store_price
            purchase_unit = store_price
            for t in sorted(pr.get('discount_tiers', []), key=lambda x: x.get('min_qty', 0), reverse=True):
                if qty >= t.get('min_qty', 0):
                    purchase_unit = float(t.get('price', store_price))
                    break
            total = round(purchase_unit * qty, 2)
            # sell_price المقفول للـ response فقط (ما يؤثر على الخصم من رصيده)
            unit = float(cp_doc.get('sell_price', store_price))

            order_id = f"API_{int(time.time())}_{uid}"

            if is_manual:
                # atomic: فحص الرصيد والخصم في عملية واحدة
                updated = db.users.find_one_and_update(
                    {'user_id': uid, 'balance': {'$gte': total}},
                    {'$inc': {'balance': -total}},
                    return_document=True
                )
                if not updated:
                    return _json_resp(self, 402, {'error': 'Insufficient balance', 'required': total})
                db.api_orders.insert_one({'order_id': order_id, 'api_user_id': uid, 'product_id': pid, 'product_name': pr.get('name_en', pr.get('name_ar', '')), 'qty': qty, 'total_price': total, 'codes': [], 'buyer_info': buyer_info, 'status': 'pending_manual'})
                db.orders.insert_one({'user_id': uid, 'product_id': pid, 'code_delivered': f"API: {order_id}", 'qty': qty, 'total_price': total, 'via_api': True})
                # 📢 لوق القناة
                try:
                    log_ch = get_setting('log_channel')
                    if log_ch and log_ch != 'Not Set':
                        product_name_clean = clean_name(pr.get('name_en', pr.get('name_ar', '')))
                        custom_emoji_id = pr.get('custom_emoji_id')
                        product_name_log = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> <b>{product_name_clean}</b>' if custom_emoji_id else f'📦 <b>{product_name_clean}</b>'
                        
                        inner_msg = LANG['en']['log_purchase'].format('API User', product_name_log, qty)
                        custom_inner = db.custom_texts.find_one({'lang': 'en', 'key': 'log_purchase'})
                        if custom_inner and custom_inner.get('value'):
                            try: inner_msg = custom_inner['value'].format('API User', product_name_log, qty)
                            except: pass
                        
                        cms_api_log = db.custom_texts.find_one({'lang': 'en', 'key': 'api_log'})
                        if cms_api_log and cms_api_log.get('value'):
                            try: pub_msg = cms_api_log['value'].format(inner_msg)
                            except: pub_msg = LANG['en']['api_log'].format(inner_msg)
                        else:
                            pub_msg = LANG['en']['api_log'].format(inner_msg)
                        
                        bot.send_message(log_ch, pub_msg, parse_mode="HTML")
                except: pass
                # 🔔 إشعار لكل الأدمن
                try: _notify_all_admins_api_purchase(uid, pr, qty, total, order_id, buyer_info, is_manual=True)
                except: pass
                return _json_resp(self, 200, {
                    'success': True,
                    'order_id': order_id,
                    'status': 'pending_manual',
                    'product_id': pid,
                    'qty': qty,
                    'unit_price': unit,
                    'total_price': total,
                    'new_balance': round(updated.get('balance', 0), 2),
                    'note': 'Manual product — admin will deliver soon'
                })

            # تلقائي — حجز أكواد
            pid_str = str(pid)
            qs = [{'product_id': pid_str}]
            if pid_str.isdigit(): qs.append({'product_id': int(pid_str)})
            try: qs.append({'product_id': float(pid_str)})
            except: pass

            res_id = f"api_{uid}_{int(time.time()*1000)}"
            reserved = []
            for _ in range(qty):
                r = db.product_stock.find_one_and_update({'$or': qs, 'is_sold': False}, {'$set': {'is_sold': True, 'reservation_id': res_id}}, return_document=True)
                if not r:
                    db.product_stock.update_many({'reservation_id': res_id}, {'$set': {'is_sold': False}, '$unset': {'reservation_id': ''}})
                    return _json_resp(self, 409, {'error': 'Stock ran out during purchase'})
                reserved.append(r)

            updated = db.users.find_one_and_update({'user_id': uid, 'balance': {'$gte': total}}, {'$inc': {'balance': -total}}, return_document=True)
            if not updated:
                db.product_stock.update_many({'reservation_id': res_id}, {'$set': {'is_sold': False}, '$unset': {'reservation_id': ''}})
                return _json_resp(self, 402, {'error': 'Insufficient balance'})

            codes = []
            for item in reserved:
                db.product_stock.update_one({'_id': item['_id']}, {'$unset': {'reservation_id': ''}})
                codes.append(item.get('code_line', ''))
                db.orders.insert_one({'user_id': uid, 'product_id': pid, 'code_delivered': item.get('code_line', ''), 'qty': 1, 'price': unit, 'via_api': True, 'api_order_id': order_id})

            db.api_orders.insert_one({'order_id': order_id, 'api_user_id': uid, 'product_id': pid, 'product_name': pr.get('name_en', pr.get('name_ar', '')), 'qty': qty, 'total_price': total, 'codes': codes, 'buyer_info': buyer_info, 'status': 'completed'})

            # 📢 لوق القناة — نفس شكل الشراء العادي + CMS
            try:
                log_ch = get_setting('log_channel')
                if log_ch and log_ch != 'Not Set':
                    product_name_clean = clean_name(pr.get('name_en', pr.get('name_ar', '')))
                    custom_emoji_id = pr.get('custom_emoji_id')
                    product_name_log = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> <b>{product_name_clean}</b>' if custom_emoji_id else f'📦 <b>{product_name_clean}</b>'
                    
                    inner_msg = LANG['en']['log_purchase'].format('API User', product_name_log, qty)
                    custom_inner = db.custom_texts.find_one({'lang': 'en', 'key': 'log_purchase'})
                    if custom_inner and custom_inner.get('value'):
                        try: inner_msg = custom_inner['value'].format('API User', product_name_log, qty)
                        except: pass
                    
                    # غلاف API
                    cms_api_log = db.custom_texts.find_one({'lang': 'en', 'key': 'api_log'})
                    if cms_api_log and cms_api_log.get('value'):
                        try: pub_msg = cms_api_log['value'].format(inner_msg)
                        except: pub_msg = LANG['en']['api_log'].format(inner_msg)
                    else:
                        pub_msg = LANG['en']['api_log'].format(inner_msg)
                    
                    bot.send_message(log_ch, pub_msg, parse_mode="HTML")
            except: pass
            
            # 🔔 إشعار لكل الأدمن
            try: _notify_all_admins_api_purchase(uid, pr, qty, total, order_id, buyer_info, is_manual=False)
            except: pass

            # 🔔 تنبيه الستوك بعد البيع عبر API (يصل لكل الأدمن عشان يعيد التعبئة)
            try:
                remaining = get_product_stock_count(pid)
                pname = clean_name(pr.get('name_ar', pr.get('name_en', '')))
                if remaining == 0:
                    notify_admins(
                        f"🚨 <b>تنبيه: ستوك انتهى! (بيع عبر API)</b>\n\n"
                        f"📦 المنتج: <b>{pname}</b>\n"
                        f"📊 المتبقي: <b>0</b>\n\n"
                        f"⚠️ أعد إضافة ستوك لهذا المنتج الآن!"
                    )
                elif remaining <= 2:
                    notify_admins(
                        f"⚠️ <b>تنبيه: ستوك قارب على الانتهاء! (بيع عبر API)</b>\n\n"
                        f"📦 المنتج: <b>{pname}</b>\n"
                        f"📊 المتبقي: <b>{remaining}</b>\n\n"
                        f"⚠️ يُفضّل إضافة ستوك جديد قريباً!"
                    )
            except Exception as _stk_e:
                logger.debug(f"API stock alert error: {_stk_e}")

            return _json_resp(self, 200, {
                'success': True,
                'order_id': order_id,
                'status': 'completed',
                'product_id': pid,
                'qty': qty,
                'unit_price': unit,
                'total_price': total,
                'codes': codes,
                'codes_count': len(codes),
                'new_balance': round(updated.get('balance', 0), 2)
            })

        # POST /set_price — المطوّر يحدد سعر البيع لعملائه
        if route == '/set_price':
            try:
                body = json.loads(_read_body(self))
            except: return _json_resp(self, 400, {'error': 'Invalid JSON'})

            product_id = str(body.get('product_id', '')).strip()
            sell_price = body.get('price')

            if not product_id: return _json_resp(self, 400, {'error': 'product_id required'})

            pr = find_product(product_id)
            if not pr or pr.get('is_hidden'):
                return _json_resp(self, 404, {'error': 'Product not found'})

            pid = str(pr.get('id', str(pr.get('_id', ''))))
            base_price = float(pr.get('price', 0))

            try:
                sell_price = round(float(sell_price), 2)
                if sell_price < base_price:
                    return _json_resp(self, 400, {'error': f'Price cannot be less than base price (${base_price:.2f})'})
                if sell_price > 9999:
                    return _json_resp(self, 400, {'error': 'Price too high'})
            except (TypeError, ValueError):
                return _json_resp(self, 400, {'error': 'price must be a number'})

            import datetime as _dt2
            db.api_pricing.update_one(
                {'api_user_id': uid, 'product_id': pid},
                {'$set': {
                    'sell_price': sell_price,
                    'base_price_snapshot': base_price,  # snapshot وقت الضبط
                    'updated_at': _dt2.datetime.utcnow().strftime('%Y-%m-%d %H:%M')
                }},
                upsert=True
            )
            profit = round(sell_price - base_price, 2)
            return _json_resp(self, 200, {
                'success': True,
                'product_id': pid,
                'base_price': base_price,
                'your_price': sell_price,
                'profit_per_unit': profit,
                'note': 'Your price is locked. Store price changes will NOT affect it.'
            })

        # POST /set_product — المطوّر يعدّل اسم/وصف المنتج لعملائه
        if route == '/set_product':
            try:
                body = json.loads(_read_body(self))
            except: return _json_resp(self, 400, {'error': 'Invalid JSON'})

            product_id = str(body.get('product_id', '')).strip()
            if not product_id: return _json_resp(self, 400, {'error': 'product_id required'})

            pr = find_product(product_id)
            if not pr or pr.get('is_hidden'):
                return _json_resp(self, 404, {'error': 'Product not found'})

            pid = str(pr.get('id', str(pr.get('_id', ''))))
            update = {}
            if 'name_ar' in body: update['name_ar'] = str(body['name_ar'])[:100]
            if 'name_en' in body: update['name_en'] = str(body['name_en'])[:100]
            if 'desc_ar' in body: update['desc_ar'] = str(body['desc_ar'])[:500]
            if 'desc_en' in body: update['desc_en'] = str(body['desc_en'])[:500]
            if 'price' in body:
                try:
                    sp = round(float(body['price']), 2)
                    base_price = float(pr.get('price', 0))
                    if sp < base_price:
                        return _json_resp(self, 400, {'error': f'Price cannot be less than ${base_price:.2f}'})
                    update['sell_price'] = sp
                except: pass

            if not update:
                return _json_resp(self, 400, {'error': 'Nothing to update. Send: name_ar, name_en, desc_ar, desc_en, or price'})

            db.api_pricing.update_one(
                {'api_user_id': uid, 'product_id': pid},
                {'$set': update},
                upsert=True
            )
            return _json_resp(self, 200, {'success': True, 'product_id': pid, 'updated': list(update.keys())})

        return _json_resp(self, 404, {'error': 'Endpoint not found'})


def keep_alive():
    port = int(os.environ.get('PORT', 8080))
    HTTPServer(('0.0.0.0', port), APIHandler).serve_forever()

threading.Thread(target=keep_alive, daemon=True).start()

bot = telebot.TeleBot(TOKEN, use_class_middlewares=False)

# 🛡 حماية من خطأ "query is too old" — يصير لما المستخدم يضغط زر قديم
# بعد إعادة تشغيل البوت أو بعد 10 ثواني من ظهور الزر.
# هذا الحل يغطي كل أماكن answer_callback_query في البوت مرة واحدة.
_orig_answer_cbq = bot.answer_callback_query
def _safe_answer_cbq(callback_query_id, text='', show_alert=False, url=None, cache_time=None, **kwargs):
    try:
        return _orig_answer_cbq(
            callback_query_id,
            text=text,
            show_alert=show_alert,
            url=url,
            cache_time=cache_time,
            **kwargs
        )
    except Exception:
        pass  # query منتهي الصلاحية أو invalid — نتجاهل بهدوء
bot.answer_callback_query = _safe_answer_cbq


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
            db.referrals_archived.create_index('referrer_id')
            db.referrals_archived.create_index('invited_id')
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

# 🛡 ضمان أن الأونر مسجّل كأدمن (عشان يستقبل تنبيهات الستوك وكل إشعارات الأدمن بثبات)
try:
    if OWNER_ID:
        db.users.update_one({'user_id': OWNER_ID}, {'$set': {'is_admin': 1}})
        logger.info(f"✅ تم التأكد أن OWNER_ID ({OWNER_ID}) مسجّل كأدمن.")
except Exception as _owner_e:
    logger.debug(f"Owner promote skipped: {_owner_e}")


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

        # 1) DM للمُحيل (بلغته مع CMS)
        try:
            referrer = db.users.find_one({'user_id': referrer_id})
            if referrer and referrer.get('is_banned') != 1:
                ref_lang = referrer.get('lang', 'ar')
                
                # نجلب النص من CMS أو الافتراضي
                cms_key = db.custom_texts.find_one({'lang': 'en', 'key': 'ref_progress_dm'})
                if cms_key and cms_key.get('value'):
                    try:
                        dm_text = cms_key['value'].format(active_count, remaining, f"{reward:.2f}")
                    except:
                        dm_text = LANG['en']['ref_progress_dm'].format(active_count, remaining, f"{reward:.2f}")
                else:
                    lang_key = 'ar' if ref_lang == 'ar' else 'en'
                    dm_text = LANG[lang_key].get('ref_progress_dm',
                        LANG['en']['ref_progress_dm']
                    ).format(active_count, remaining, f"{reward:.2f}")
                
                bot.send_message(referrer_id, dm_text, parse_mode="HTML")
        except Exception as dm_err:
            logger.debug(f"Progress DM failed for {referrer_id}: {dm_err}")

        # 2) قناة اللوق (بالإنجليزي دائماً)
        if remaining == threshold:
            return  # milestone - الـ update_referrer_balance سيرسل

        try:
            log_ch = get_setting('log_channel')
            if log_ch and log_ch != "Not Set":
                # نجلب النص من CMS أو الافتراضي
                default_log = (
                    f"📈 <b>New Active Referral!</b>\n\n"
                    f"👤 Referrer: <b>**</b>\n"
                    f"✅ Active Referrals: <b>{active_count}</b>\n"
                    f"⏳ <b>{remaining}</b> more to earn <b>${reward:.2f}</b>"
                )
                cms = db.custom_texts.find_one({'lang': 'en', 'key': 'log_ref_progress'})
                if cms and cms.get('value'):
                    try:
                        log_text = cms['value'].format(active_count, remaining, f"{reward:.2f}")
                    except:
                        log_text = default_log
                else:
                    log_text = default_log
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

                # 1) رسالة للمُحيل في الخاص (من CMS)
                try:
                    cms_key = db.custom_texts.find_one({'lang': 'en', 'key': 'ref_milestone_dm'})
                    if cms_key and cms_key.get('value'):
                        try:
                            milestone_msg = cms_key['value'].format(active_count, f"{diff:.2f}", f"{new_balance:.2f}")
                        except:
                            milestone_msg = LANG['en']['ref_milestone_dm'].format(active_count, f"{diff:.2f}", f"{new_balance:.2f}")
                    else:
                        lang_key = 'ar' if ref_lang == 'ar' else 'en'
                        milestone_msg = LANG[lang_key].get('ref_milestone_dm',
                            LANG['en']['ref_milestone_dm']
                        ).format(active_count, f"{diff:.2f}", f"{new_balance:.2f}")
                    bot.send_message(rid, milestone_msg, parse_mode="HTML")
                except Exception as dm_err:
                    logger.debug(f"Milestone DM failed for {rid}: {dm_err}")

                # 2) إشعار قناة اللوق (يستخدم CMS)
                try:
                    log_ch = get_setting('log_channel')
                    if log_ch and log_ch != "Not Set":
                        # نجرب CMS أولاً
                        cms_log = db.custom_texts.find_one({'lang': 'en', 'key': 'log_ref_milestone'})
                        if cms_log and cms_log.get('value'):
                            try:
                                log_text = cms_log['value'].format(f"**", active_count, f"{diff:.2f}")
                            except:
                                log_text = LANG['en']['log_ref_milestone'].format(f"**", active_count, f"{diff:.2f}")
                        else:
                            log_text = LANG['en']['log_ref_milestone'].format(f"**", active_count, f"{diff:.2f}")
                        bot.send_message(log_ch, log_text, parse_mode="HTML")
                except Exception as log_err:
                    logger.debug(f"Milestone log failed: {log_err}")

                # 3) إشعار للأدمن
                try:
                    ref_user = get_user_data_full(rid)
                    ref_username = ref_user.get('username') if ref_user else None
                    ref_name     = ref_user.get('name', '') if ref_user else ''
                    user_display = f"@{ref_username}" if ref_username else f"<code>{rid}</code>"
                    admin_notif = (
                        f"💎 <b>Referral Milestone!</b>\n\n"
                        f"👤 {user_display}"
                        + (f" ({ref_name[:20]})" if ref_name else "") +
                        f"\n🆔 <code>{rid}</code>\n"
                        f"✅ Active Refs: <b>{active_count}</b>\n"
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
        # left من الأرشيف فقط (المؤكد مغادرتهم)
        left = db.referrals_archived.count_documents({'referrer_id': rid})
        total = pending + active + left
        return pending, active, left, total
    except Exception:
        return 0, 0, 0, 0


def background_referral_checker_v2():
    """
    فاحص خلفي ذكي - يتجنب Telegram Rate Limit
    
    🛡 الاستراتيجية:
    - يفحص pending و active فقط
    - لو شخص مو مشترك → يزيد عداد left_checks
    - لازم 3 فحوصات متتالية تأكد المغادرة
    - بعد التأكيد → ينقل للأرشيف (referrals_archived)
    - خطأ اتصال = يتجاهل ويصفّر العداد
    """
    
    # ═══ إصلاح لمرة واحدة: رجّع الإحالات اللي صارت 'left' بالغلط ═══
    try:
        fix_key = db.settings.find_one({'key': 'ref_left_fix_v1'})
        if not fix_key:
            wrong_left = db.referrals_v2.count_documents({'status': 'left'})
            if wrong_left > 0:
                db.referrals_v2.update_many(
                    {'status': 'left'},
                    {'$set': {'status': 'active', 'left_checks': 0}}
                )
                logger.info(f"🔧 Fixed {wrong_left} wrongly-marked 'left' referrals → reset to 'active' for re-verification")
            db.settings.insert_one({'key': 'ref_left_fix_v1', 'value': True})
    except Exception as e:
        logger.error(f"Error in referral fix: {e}")
    BATCH_SIZE = 10
    DELAY_BETWEEN_CHECKS = 0.15
    DELAY_BETWEEN_CYCLES = 60
    LEFT_CONFIRM_NEEDED = 3  # لازم 3 فحوصات متتالية تأكد إنه غادر
    
    # ⏳ انتظار 90 ثانية بعد تشغيل البوت عشان الاتصال يستقر
    time.sleep(90)
    logger.info("🔄 Background referral checker started.")
    
    while True:
        try:
            # نفحص pending و active فقط (left محفوظة في referrals_archived)
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
                    time.sleep(DELAY_BETWEEN_CHECKS)
                    continue
                
                # None = خطأ اتصال — نحافظ على الحالة + نصفّر العداد
                if is_subbed is None:
                    db.referrals_v2.update_one(
                        {'invited_id': inv_uid},
                        {'$set': {'left_checks': 0}}
                    )
                    time.sleep(DELAY_BETWEEN_CHECKS)
                    continue
                
                if is_subbed:
                    # مشترك ✅
                    if current_status != 'active':
                        mark_referral_status(inv_uid, 'active')
                    # نصفّر عداد المغادرة
                    db.referrals_v2.update_one(
                        {'invited_id': inv_uid},
                        {'$set': {'left_checks': 0}}
                    )
                else:
                    # مو مشترك — نزيد العداد
                    if current_status == 'active':
                        left_checks = r.get('left_checks', 0) + 1
                        if left_checks >= LEFT_CONFIRM_NEEDED:
                            # ✅ مؤكد غادر — ننقله للأرشيف
                            referrer_id = r.get('referrer_id')
                            db.referrals_archived.insert_one({
                                'invited_id': inv_uid,
                                'referrer_id': referrer_id,
                                'status': 'left',
                                'original_status': current_status,
                                'archived_at': int(time.time())
                            })
                            # نحذفه من الأساسي
                            db.referrals_v2.delete_one({'invited_id': inv_uid})
                            # نحدّث رصيد المُحيل
                            try: update_referrer_balance(referrer_id)
                            except: pass
                            logger.info(f"📤 Referral {inv_uid} archived as LEFT (confirmed {LEFT_CONFIRM_NEEDED}x)")
                        else:
                            db.referrals_v2.update_one(
                                {'invited_id': inv_uid},
                                {'$set': {'left_checks': left_checks}}
                            )
                    # pending ما يصير left — يبقى pending
                
                time.sleep(DELAY_BETWEEN_CHECKS)
                
                batch_count += 1
                if batch_count >= BATCH_SIZE:
                    batch_count = 0
                    time.sleep(2)
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
        'btn_check_sub': '🔄 تحقق من الاشتراك',
        'btn_api': '🤖 لوحة تحكم API'
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
        'btn_check_sub': '🔄 Verify Sub',
        'btn_api': '🤖 API Control Panel'
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
        
        # رسالة التقدم (كل إحالة نشطة جديدة) - {0}=active_count {1}=remaining {2}=reward
        'ref_progress_dm': "📈 <b>New Active Referral!</b>\n\n✅ Active Referrals: <b>{0}</b>\n⏳ <b>{1}</b> more to earn <b>${2}</b>",
        
        # رسالة لوق قناة الإحالات (كل إحالة) - {0}=active_count {1}=remaining {2}=reward
        'log_ref_progress': "📈 <b>New Active Referral!</b>\n\n👤 Referrer: <b>**</b>\n✅ Active Referrals: <b>{0}</b>\n⏳ <b>{1}</b> more to earn <b>${2}</b>",
        
        # رسالة المكافأة للمُحيل - {0}=active_count {1}=reward {2}=balance
        'ref_milestone_dm': "🎉 <b>Milestone Reached!</b> 🏆\n\n👥 Active Referrals: <b>{0}</b>\n💰 Reward: <code>+${1}</code>\n💼 Balance: <b>${2}</b>\n\n🔥 Keep sharing your link!",
        
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
        'api_welcome': "🔗 <b>Store API</b>\n\n🤖 اربط متجرنا ببوتك أو موقعك!\n\n📌 <b>كيف يشتغل؟</b>\n1️⃣ أنشئ كود اتصال من هنا\n2️⃣ أرسل الكود لأي ذكاء اصطناعي واطلب منه يسوي لك بوت أو موقع\n3️⃣ عملائك يشترون من بوتك ← المنتجات تجي تلقائي\n\n💰 <i>كل شراء يُخصم من رصيدك عندنا</i>",
        'api_created': "✅ <b>تم إنشاء كود الاتصال!</b>\n\n🔗 <b>الكود:</b>\n<code>{}</code>\n\n━━━━━━━━━━━━━━━━━━━\n\n📌 <b>هذا الكود فيه كل شيء:</b>\n• رابط السيرفر\n• مفتاح API\n• مسار الاتصال\n\nكلها مشفّرة في كود واحد فقط.\n\n━━━━━━━━━━━━━━━━━━━\n\n🤖 <b>كيف تستخدمه؟</b>\n\n1️⃣ اضغط زر 📋 <b>رسالة للذكاء الاصطناعي</b>\n2️⃣ انسخ الرسالة كاملة\n3️⃣ أرسلها لأي ذكاء اصطناعي (ChatGPT / Claude / Gemini)\n4️⃣ يسوي لك بوت أو موقع جاهز تلقائي\n\n⚠️ <b>لا تشارك الكود مع أحد غير موثوق!</b>",
        'api_howto': "📖 <b>كيف تربط بوتك</b>\n\n1️⃣ انسخ كود الاتصال\n2️⃣ أرسله لذكاء اصطناعي وقول:\n<i>سوّلي بوت متجر يستخدم هذا الكود</i>\n3️⃣ شغّل البوت الجاهز\n\n💡 أو أعطي الكود لمبرمج يربطه بموقعك\n\n⚠️ <i>لا تشارك الكود!</i>",
        'api_log': "🤖 <b>Auto Buy API</b>\n\n{}",
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
        'api_welcome': "🔗 <b>Store API</b>\n\n🤖 Connect your store to your bot or website!\n\n📌 <b>How it works:</b>\n1️⃣ Create a connection code here\n2️⃣ Send the code to any AI (ChatGPT/Claude) and ask it to build a bot or website\n3️⃣ Your customers buy from your bot ← products come automatically\n\n💰 <i>Every purchase is deducted from your balance here</i>",
        'api_created': "✅ <b>Connection Code Created!</b>\n\n🔗 <b>Code:</b>\n<code>{}</code>\n\n━━━━━━━━━━━━━━━━━━━\n\n📌 <b>This code contains everything:</b>\n• Server URL\n• API Key\n• Connection path\n\nAll encrypted in one single code.\n\n━━━━━━━━━━━━━━━━━━━\n\n🤖 <b>How to use:</b>\n\n1️⃣ Tap 📋 <b>Message for AI</b> button\n2️⃣ Copy the full message\n3️⃣ Send it to any AI (ChatGPT / Claude / Gemini)\n4️⃣ It builds you a bot or website automatically\n\n⚠️ <b>Don't share this code with anyone!</b>",
        'api_howto': "📖 <b>How to connect your bot</b>\n\n1️⃣ Copy your connection code\n2️⃣ Send it to any AI and say:\n<i>Build me a store bot using this code</i>\n3️⃣ Run the bot\n\n💡 Or give the code to a developer for your website\n\n⚠️ <i>Don't share the code!</i>",
        'api_log': "🤖 <b>Auto Buy API</b>\n\n{}",
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
    يترجم سطر واحد مع حفظ كامل لـ:
    - Premium Emojis <tg-emoji> (بالترقيم الموضعي الآمن)
    - HTML tags <b><i><code> إلخ
    - Placeholders {}
    - URLs
    - إيموجي عادية
    
    الاستراتيجية: نفصل كل عنصر محمي ونضعه في قاموس بـ UUID ثابت لا يتغير بالترجمة.
    """
    try:
        import uuid as _uuid
        
        # نستخدم UUIDs ثابتة بدل @@N@@ لأن Google لا يلمسها
        vault = {}  # uuid → original
        
        def protect_item(text_to_protect):
            key = f"XPROTX{_uuid.uuid4().hex[:8].upper()}XPROTX"
            vault[key] = text_to_protect
            return f" {key} "
        
        temp = line
        
        # 1. Premium Emojis — أعلى أولوية (قبل أي شيء)
        temp = re.sub(
            r'<tg-emoji[^>]*>.*?</tg-emoji>',
            lambda m: protect_item(m.group(0)),
            temp,
            flags=re.DOTALL
        )
        
        # 2. Code/Pre blocks
        temp = re.sub(r'<code>.*?</code>', lambda m: protect_item(m.group(0)), temp, flags=re.DOTALL)
        temp = re.sub(r'<pre>.*?</pre>', lambda m: protect_item(m.group(0)), temp, flags=re.DOTALL)
        
        # 3. URLs
        temp = re.sub(r'https?://[^\s<>"]+', lambda m: protect_item(m.group(0)), temp)
        
        # 4. Placeholders
        temp = re.sub(r'\{[^}]*\}', lambda m: protect_item(m.group(0)), temp)
        
        # 5. فواصل بصرية
        temp = re.sub(r'[━─═]{2,}', lambda m: protect_item(m.group(0)), temp)
        
        # 6. HTML tags (نحمي فقط الـ tags، محتواها يُترجم)
        temp = re.sub(r'</?[a-zA-Z][^>]*>', lambda m: protect_item(m.group(0)), temp)
        
        # 7. إيموجي عادية
        emoji_pat = re.compile(
            r"[\u2600-\u27BF\U0001F000-\U0001F9FF\U0001FA00-\U0001FAFF\u200d\ufe0f]+",
            flags=re.UNICODE
        )
        temp = emoji_pat.sub(lambda m: protect_item(m.group(0)), temp)
        
        # تحقق: لو ما بقي نص حقيقي
        clean_check = re.sub(r'XPROTX[A-F0-9]{8}XPROTX', '', temp).strip()
        if not clean_check or len(clean_check) < 2:
            return line
        
        # 🌐 الترجمة
        translated = GoogleTranslator(source='auto', target=target_lang).translate(temp)
        if not translated:
            return line
        
        # 🔁 معالجة أي مشاكل في المسافات أو حالة الأحرف قد يكون المترجم أدخلها على المفاتيح الخاصة بنا
        def normalize_key(match):
            hex_part = re.sub(r'\s+', '', match.group(1)).upper()
            return f"XPROTX{hex_part}XPROTX"
        translated = re.sub(r'(?i)x\s*p\s*r\s*o\s*t\s*x\s*([a-f0-9\s]{8,20})\s*x\s*p\s*r\s*o\s*t\s*x', normalize_key, translated)
        
        # 🔁 استرجاع العناصر المحمية — UUIDs ثابتة، مع دعم عدم حساسية حالة الأحرف كأمان إضافي
        for key, original in vault.items():
            translated = re.sub(re.escape(key), lambda m: original, translated, flags=re.IGNORECASE)
        
        # 🛡 فحص: لو بقي أي UUID لم يُسترجع → رجوع للأصل
        if re.search(r'(?i)XPROTX[A-F0-9]{8}XPROTX', translated):
            logger.warning("UUID leak in translation - returning original line")
            return line
        
        # 🛡 فحص نهائي: عدد tg-emoji لازم يكون نفسه
        orig_tg = len(re.findall(r'<tg-emoji', line))
        result_tg = len(re.findall(r'<tg-emoji', translated))
        if orig_tg != result_tg:
            logger.warning(f"tg-emoji count mismatch after translation ({orig_tg} vs {result_tg}) - returning original")
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
        # بحث بـ id field
        p = db.products.find_one({'id': pid_str})
        if p: return p
        if pid_str.isdigit():
            p = db.products.find_one({'id': int(pid_str)})
            if p: return p
        try:
            p = db.products.find_one({'id': float(pid_str)})
            if p: return p
        except: pass
        # بحث بـ _id كـ string (لمنتجات مثل cgpt_main_xxx)
        p = db.products.find_one({'_id': pid_str})
        if p: return p
        # بحث بـ _id كـ ObjectId
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
    """
    يفحص اشتراك المستخدم في كل القنوات الإجبارية.
    لازم يكون مشترك في كل القنوات — لو طلع من واحدة = غير مشترك.
    يرجع True (مشترك بالكل) / False (طلع من واحدة على الأقل) / None (خطأ اتصال)
    """
    if uid == OWNER_ID: return True
    user_db = get_user_data_full(uid)
    if user_db and user_db.get('is_admin') == 1: return True
    chans = list(db.required_channels.find())
    if not chans: return True
    
    had_error = False
    for c in chans:
        try:
            member = bot.get_chat_member(c['channel_id'], uid)
            if member.status in ['left', 'kicked']:
                return False  # طلع من قناة واحدة = مو مشترك أكيد
        except Exception:
            had_error = True
            continue  # نكمّل فحص باقي القنوات
    
    # لو كل القنوات نجحت بدون خطأ = مشترك بالكل
    # لو في خطأ بس ما لقينا أي "left" = مو متأكدين
    if had_error:
        return None
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


def notify_balance_gift(target_uid, amount, by_admin=True, note='', gift_type='manual'):
    """
    🎁 يُشعر المستخدم في الخاص + يرسل في قناة اللوق عند إضافة/خصم رصيد من الأدمن.
    - amount موجب = إضافة | سالب = خصم
    """
    try:
        amount = round(float(amount), 2)
    except (ValueError, TypeError):
        return
    if amount == 0:
        return

    # 1) إشعار المستخدم في الخاص (بلغته)
    try:
        u = get_user_data_full(target_uid)
        new_balance = round(float(u.get('balance', 0)), 2) if u else 0.0
        l = u.get('lang', 'ar') if u else 'ar'

        if amount > 0:
            type_label_ar = "🔄 تعويض" if gift_type == 'compensation' else "💰 إضافة رصيد"
            type_label_en = "🔄 Compensation" if gift_type == 'compensation' else "💰 Balance Added"
            if l == 'ar':
                user_msg = (
                    f"🎁 <b>تم إضافة رصيد لحسابك!</b>\n\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"📌 النوع: <b>{type_label_ar}</b>\n"
                    f"💰 المبلغ المضاف: <b>+${amount:.2f}</b>\n"
                    f"💼 رصيدك الحالي: <b>${new_balance:.2f}</b>\n"
                )
                if note:
                    user_msg += f"📝 الملاحظة: <code>{note}</code>\n"
                user_msg += f"━━━━━━━━━━━━━━\n\n<i>تمت الإضافة من قبل الإدارة. 🛡️</i>"
            else:
                user_msg = (
                    f"🎁 <b>Balance Added to Your Account!</b>\n\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"📌 Type: <b>{type_label_en}</b>\n"
                    f"💰 Amount Added: <b>+${amount:.2f}</b>\n"
                    f"💼 Your Balance: <b>${new_balance:.2f}</b>\n"
                )
                if note:
                    user_msg += f"📝 Note: <code>{note}</code>\n"
                user_msg += f"━━━━━━━━━━━━━━\n\n<i>Added by the administration. 🛡️</i>"
        else:
            if l == 'ar':
                user_msg = (
                    f"⚠️ <b>تم خصم رصيد من حسابك</b>\n\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"💰 المبلغ المخصوم: <b>${abs(amount):.2f}</b>\n"
                    f"💼 رصيدك الحالي: <b>${new_balance:.2f}</b>\n"
                    f"━━━━━━━━━━━━━━\n\n"
                    f"<i>تم التعديل من قبل الإدارة.</i>"
                )
            else:
                user_msg = (
                    f"⚠️ <b>Balance Deducted</b>\n\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"💰 Amount: <b>${abs(amount):.2f}</b>\n"
                    f"💼 Your Balance: <b>${new_balance:.2f}</b>\n"
                    f"━━━━━━━━━━━━━━\n\n"
                    f"<i>Adjusted by the administration.</i>"
                )
        bot.send_message(target_uid, user_msg, parse_mode="HTML")
    except Exception as e:
        logger.debug(f"notify_balance_gift user DM failed: {e}")

    # 2) قناة اللوق (عشان الأدمن يشوف العملية أيضاً)
    try:
        log_ch = get_setting('log_channel')
        if log_ch and log_ch != "Not Set":
            u = get_user_data_full(target_uid)
            obs_user = obscure_text(u.get('username') or str(target_uid)) if u else "**"
            if amount > 0:
                log_msg = (
                    f"🎁 <b>Admin Balance Gift!</b> 💰\n\n"
                    f"👤 <b>User:</b> {obs_user}\n"
                    f"💵 <b>Added:</b> <code>+${amount:.2f}</code>\n"
                    f"🛡 <b>By:</b> Admin\n\n"
                    f"<i>Manual top-up by administration ⚡</i>"
                )
                custom = db.custom_texts.find_one({'lang': 'en', 'key': 'log_admin_gift'})
                if custom and custom.get('value'):
                    try: log_msg = custom['value'].format(obs_user, f"{amount:.2f}")
                    except: pass
            else:
                log_msg = (
                    f"⚠️ <b>Admin Balance Adjustment</b>\n\n"
                    f"👤 <b>User:</b> {obs_user}\n"
                    f"💵 <b>Deducted:</b> <code>${abs(amount):.2f}</code>\n"
                    f"🛡 <b>By:</b> Admin"
                )
            bot.send_message(log_ch, log_msg, parse_mode="HTML")
    except Exception as e:
        logger.debug(f"notify_balance_gift log failed: {e}")


@bot.message_handler(commands=['diag'])
def diag_alerts_cmd(message):
    """🔧 تشخيص إشعارات الأدمن/الستوك — للأونر والأدمن فقط"""
    uid = message.from_user.id
    if not _is_admin_check(uid):
        return

    admins = list(db.users.find({'is_admin': 1}))
    admin_ids = [a.get('user_id') for a in admins]
    my_rec = db.users.find_one({'user_id': uid}) or {}
    owner_rec = db.users.find_one({'user_id': OWNER_ID}) if OWNER_ID else None

    # المستهدفون فعلياً (نفس منطق notify_admins)
    targets = set()
    if OWNER_ID:
        targets.add(OWNER_ID)
    for a in admin_ids:
        if a:
            targets.add(a)

    report = (
        f"🔧 <b>تشخيص الإشعارات</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"🆔 OWNER_ID في .env: <code>{OWNER_ID}</code>\n"
        f"🆔 آيديك: <code>{uid}</code>\n"
        f"{'✅' if uid == OWNER_ID else '❗'} OWNER_ID = آيديك؟ <b>{'نعم' if uid == OWNER_ID else 'لا'}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"👑 عدد الأدمن (is_admin=1): <b>{len(admin_ids)}</b>\n"
        f"📋 آيديهم: <code>{admin_ids or 'لا يوجد'}</code>\n"
        f"🛡 سجلك is_admin = <b>{my_rec.get('is_admin')}</b>\n"
        f"🛡 سجل OWNER_ID is_admin = <b>{owner_rec.get('is_admin') if owner_rec else 'غير موجود'}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"🎯 من يستقبل تنبيه الستوك: <b>{len(targets)}</b>\n"
        f"📋 <code>{list(targets) or 'لا أحد ❗'}</code>"
    )
    try:
        bot.send_message(message.chat.id, report, parse_mode="HTML")
    except Exception:
        bot.send_message(message.chat.id, report)

    # تجربة إرسال فعلية لكل مستهدف
    test_msg = "🔔 <b>تنبيه تجريبي للستوك</b>\n\nإذا وصلتك هذي الرسالة، فإشعارات الستوك تشتغل صح ✅"
    ok, fail = [], []
    for t in targets:
        try:
            bot.send_message(t, test_msg, parse_mode="HTML")
            ok.append(t)
        except Exception as e:
            fail.append(f"{t}: {str(e)[:40]}")

    # الخلاصة + التشخيص التلقائي
    if not targets:
        verdict = (
            "🚨 <b>المشكلة لقيتها:</b> ما فيه أي مستهدف للإشعارات!\n\n"
            "OWNER_ID = <code>0</code> أو غلط، وما فيه أدمن مسجّل.\n\n"
            "✅ <b>الحل:</b> حط OWNER_ID الصحيح في ملف <code>.env</code> وأعد تشغيل البوت."
        )
    elif uid not in targets:
        verdict = (
            "⚠️ <b>المشكلة لقيتها:</b> آيديك مو ضمن من يستقبل التنبيهات.\n\n"
            "✅ <b>الحل:</b> صحّح <code>OWNER_ID</code> في .env ليطابق آيديك "
            f"(<code>{uid}</code>) وأعد تشغيل البوت."
        )
    elif uid in ok:
        verdict = (
            "✅ <b>الإشعارات تشتغل تماماً!</b>\n\n"
            "لو ما جاك تنبيه ستوك حقيقي، السبب واحد من:\n"
            "1️⃣ ما رفعت آخر نسخة من <code>bot.py</code> للسيرفر\n"
            "2️⃣ ما أعدت تشغيل البوت بعد الرفع\n"
            "3️⃣ المنتج يدوي (التنبيه للمنتجات التلقائية فقط)\n"
            "4️⃣ تجرّب الشراء من حساب ثاني — التنبيه يروح لحساب الأدمن مو حساب المشتري"
        )
    else:
        verdict = (
            "❌ <b>البوت ما قدر يرسل لك!</b>\n"
            f"السبب: <code>{fail}</code>\n\n"
            "غالباً ما بدأت محادثة مع البوت من حسابك. أرسل /start للبوت ثم جرّب /diag مرة ثانية."
        )

    try:
        bot.send_message(message.chat.id, verdict, parse_mode="HTML")
    except Exception:
        bot.send_message(message.chat.id, verdict)


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
    
    markup.add(create_btn(uid, 'btn_products', callback_data="open_shop", style="primary"),
               create_btn(uid, 'btn_deposit', callback_data="open_deposit"))
    markup.add(create_btn(uid, 'btn_profile', callback_data="open_profile"),
               create_btn(uid, 'btn_invite', callback_data="open_invite"))
    markup.add(create_btn(uid, 'btn_support', url=f"https://t.me/{OWNER_USER}"),
               create_btn(uid, 'btn_lang', callback_data="toggle_language"))
    
    # 🆕 زر شروط الاستخدام
    markup.add(create_btn(uid, 'btn_terms', callback_data="open_terms"))
    markup.add(create_btn(uid, 'btn_api', callback_data="open_api"))
    
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
    # زر سجل التعويضات والتعديلات المالية
    bal_logs_count = db.balance_logs.count_documents({'user_id': uid})
    if bal_logs_count > 0:
        bal_btn_label = "📋 سجل التعويضات" if l == 'ar' else "📋 Balance Adjustments"
        markup.add(InlineKeyboardButton(bal_btn_label, callback_data="my_ballogs_0"))
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
    markup.add(create_btn(uid, 'btn_back', callback_data="main_menu_refresh"))
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
    markup.add(InlineKeyboardButton("👥 My Referrals" if l == 'en' else "👥 إحالاتي", callback_data="ref_list_0"))
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
# ═══ قائمة الإحالات بصفحات ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("ref_list_"))
def referral_list_page(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    l = get_lang(uid)
    page = int(call.data.replace("ref_list_", ""))
    PER_PAGE = 10
    
    # جلب الإحالات النشطة والمعلقة
    refs_active = list(db.referrals_v2.find({'referrer_id': uid}))
    # جلب المغادرين المؤكدين من الأرشيف
    refs_left = list(db.referrals_archived.find({'referrer_id': uid}))
    
    # دمج وترتيب: active أول، pending، ثم left
    status_order = {'active': 0, 'pending': 1, 'left': 2}
    all_refs = []
    for r in refs_active:
        all_refs.append({'id': r.get('invited_id'), 'status': r.get('status', 'pending'), 'source': 'v2'})
    for r in refs_left:
        all_refs.append({'id': r.get('invited_id'), 'status': 'left', 'source': 'archived'})
    all_refs.sort(key=lambda x: status_order.get(x['status'], 9))
    
    total = len(all_refs)
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = min(page, total_pages - 1)
    start = page * PER_PAGE
    end = start + PER_PAGE
    page_refs = all_refs[start:end]
    
    txt = f"👥 <b>{'My Referrals' if l == 'en' else 'إحالاتي'}</b> ({page+1}/{total_pages})\n\n"
    
    # هل المستخدم أدمن؟
    u_data = get_user_data_full(uid)
    is_admin = (u_data and (u_data.get('is_admin') == 1 or uid == OWNER_ID))
    
    if not all_refs:
        txt += "📭 No referrals yet." if l == 'en' else "📭 لا يوجد إحالات بعد."
    else:
        for r in page_refs:
            inv_id = r['id']
            status = r['status']
            
            if status == 'active':
                icon = "🟢"
            elif status == 'pending':
                icon = "🟡"
            else:
                icon = "🔴"
            
            if is_admin:
                # الأدمن يشوف كل شيء
                inv_user = get_user_data_full(inv_id)
                if inv_user:
                    name = inv_user.get('name', '')[:15]
                    uname = f"@{inv_user.get('username', '')}" if inv_user.get('username') else ''
                    txt += f"{icon} <code>{inv_id}</code> {name} {uname}\n"
                else:
                    txt += f"{icon} <code>{inv_id}</code>\n"
            else:
                # المستخدم العادي يشوف الآيدي فقط
                txt += f"{icon} <code>{inv_id}</code>\n"
    
    # أزرار التنقل
    markup = InlineKeyboardMarkup(row_width=2)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"ref_list_{page-1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"ref_list_{page+1}"))
    if nav_buttons:
        markup.add(*nav_buttons)
    
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="open_invite"))
    
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass


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
    
    # فحص هل في كتالوجات
    catalogs = list(db.catalogs.find().sort('order', 1))
    
    if catalogs:
        # ═══ عرض الكتالوجات أولاً (مرتبة أبجدياً) ثم المنتجات العادية ═══
        markup = InlineKeyboardMarkup(row_width=2)
        
        # ترتيب الكتالوجات أبجدياً
        name_key = 'name_en' if l == 'en' else 'name_ar'
        catalogs.sort(key=lambda c: (c.get(name_key) or c.get('name_ar', '')).lower())
        
        cat_buttons = []
        for cat in catalogs:
            cat_id = str(cat.get('_id', ''))
            emoji = cat.get('emoji', '📁')
            emoji_id = cat.get('emoji_id')
            name = cat.get('name_en', cat.get('name_ar', '')) if l == 'en' else cat.get('name_ar', cat.get('name_en', ''))
            prod_ids = cat.get('product_ids') or []
            count = 0
            for pid in prod_ids:
                p = find_product(pid)
                if p and (not p.get('is_hidden', False) or is_admin):
                    count += 1
            if count == 0 and not is_admin:
                continue
            btn_text = f"{name} ({count})" if emoji_id else f"{emoji} {name} ({count})"
            btn_kwargs = {'text': btn_text, 'callback_data': f"cat_{cat_id}", 'style': 'primary'}
            if emoji_id:
                btn_kwargs['icon_custom_emoji_id'] = emoji_id
            cat_buttons.append(CustomInlineButton(**btn_kwargs))
        
        # نضيف أزرار الكتالوجات 2 جنب بعض
        for i in range(0, len(cat_buttons), 2):
            if i + 1 < len(cat_buttons):
                markup.add(cat_buttons[i], cat_buttons[i + 1])
            else:
                markup.add(cat_buttons[i])
        
        # المنتجات بدون كتالوج
        all_catalog_pids = set()
        for cat in catalogs:
            all_catalog_pids.update([str(x) for x in (cat.get('product_ids') or [])])
        
        prods_no_cat = []
        for p in db.products.find():
            pid = str(p.get('id', str(p.get('_id', ''))))
            if pid not in all_catalog_pids:
                if p.get('is_hidden', False) and not is_admin:
                    continue
                prods_no_cat.append(p)
        
        # ترتيب المنتجات الغير مصنّفة أبجدياً
        prods_no_cat.sort(key=lambda p: clean_name(p.get('name_en' if l == 'en' else 'name_ar', '')).lower())
        
        # عرض المنتجات الغير مصنّفة كأزرار عادية
        for p in prods_no_cat:
            pid = p.get('id', str(p.get('_id', '')))
            is_manual = p.get('is_manual', False)
            is_cgpt = p.get('product_type') == 'cgpt_main'
            st = get_product_stock_count(pid)
            in_stock = is_manual or st > 0 or is_cgpt
            db_style = p.get('btn_style')
            btn_style = db_style if (db_style and in_stock) else ("success" if in_stock else "danger")
            is_hidden = p.get('is_hidden', False)
            hidden_icon = " 👻" if is_hidden else ""
            n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar'))
            short_n = n[:25] + ".." if len(n) > 25 else n
            if is_cgpt:
                btn_text = f"{short_n} | 📦 FW{hidden_icon}"
            else:
                st_text = "FW" if is_manual else str(st)
                btn_text = f"{short_n} | ${p.get('price', 0):.2f} | 📦 {st_text}{hidden_icon}"
            btn_kwargs = {'text': btn_text, 'callback_data': f"vi_p_{pid}", 'style': btn_style}
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
    else:
        # ═══ بدون كتالوجات — العرض العادي القديم ═══
        prods = list(db.products.find())
        prods.sort(key=lambda x: x.get('name_en' if l == 'en' else 'name_ar', '').lower())
        
        markup = InlineKeyboardMarkup(row_width=1)
        
        for p in prods:
            is_hidden = p.get('is_hidden', False)
            if is_hidden and not is_admin:
                continue
            is_manual = p.get('is_manual', False)
            pid = p.get('id', str(p.get('_id', '')))
            is_cgpt = p.get('product_type') == 'cgpt_main'
            st = get_product_stock_count(pid)
            in_stock = is_manual or st > 0 or is_cgpt
            db_style = p.get('btn_style')
            btn_style = db_style if (db_style and in_stock) else ("success" if in_stock else "danger")
            hidden_icon = " 👻(مخفي)" if is_hidden else ""
            n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar'))
            short_n = n[:25] + ".." if len(n) > 25 else n 
            if is_cgpt:
                btn_text = f"{short_n} | 📦 FW{hidden_icon}"
            else:
                st_text = "FW" if is_manual else str(st)
                btn_text = f"{short_n} | ${p.get('price', 0):.2f} | 📦 {st_text}{hidden_icon}"
            btn_kwargs = {'text': btn_text, 'callback_data': f"vi_p_{pid}", 'style': btn_style}
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


# ═══ عرض محتويات كتالوج معيّن ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("cat_"))
def catalog_view(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)
    
    cat_id = call.data.replace("cat_", "")
    from bson import ObjectId
    try:
        cat = db.catalogs.find_one({'_id': ObjectId(cat_id)})
    except:
        cat = None
    
    if not cat:
        bot.answer_callback_query(call.id, "❌", show_alert=True); return
    
    u = get_user_data_full(uid)
    is_admin = (u.get('is_admin') == 1 or uid == OWNER_ID)
    
    emoji = cat.get('emoji', '📁')
    emoji_id = cat.get('emoji_id')
    name = cat.get('name_en', cat.get('name_ar', '')) if l == 'en' else cat.get('name_ar', cat.get('name_en', ''))
    
    markup = InlineKeyboardMarkup(row_width=1)
    prod_ids = cat.get('product_ids') or []
    
    # نجمع المنتجات ونرتبها: المتوفر أول، الخالص آخر
    items = []
    for pid in prod_ids:
        p = find_product(str(pid))
        if not p: continue
        if p.get('is_hidden', False) and not is_admin: continue
        is_manual = p.get('is_manual', False)
        actual_pid = p.get('id', str(p.get('_id', '')))
        is_cgpt = p.get('product_type') == 'cgpt_main'
        st = get_product_stock_count(actual_pid)
        in_stock = is_manual or st > 0 or is_cgpt
        items.append((p, actual_pid, st, is_manual, in_stock))
    
    # cgpt_pinned أول دائماً، ثم المتوفر، ثم أبجدي
    items.sort(key=lambda x: (
        not x[0].get('cgpt_pinned', False),
        not x[4],
        clean_name(x[0].get('name_en' if l == 'en' else 'name_ar', '')).lower()
    ))
    
    for p, actual_pid, st, is_manual, in_stock in items:
        # لو المنتج عنده btn_style محدد (مثل ChatGPT الأخضر) نستخدمه، وإلا نحدده بالمخزون
        db_style = p.get('btn_style')
        btn_style = db_style if db_style and in_stock else ("success" if in_stock else "danger")
        hidden_icon = " 👻" if p.get('is_hidden', False) else ""
        n = clean_name(p.get('name_en') if l == 'en' else p.get('name_ar'))
        short_n = n[:25] + ".." if len(n) > 25 else n
        is_cgpt_main = p.get('product_type') == 'cgpt_main'
        if is_cgpt_main:
            # لا نعرض السعر لأن المدد لها أسعار مختلفة
            btn_text = f"{short_n} | 📦 FW{hidden_icon}"
        else:
            st_text = "FW" if is_manual else str(st)
            btn_text = f"{short_n} | ${p.get('price', 0):.2f} | 📦 {st_text}{hidden_icon}"
        btn_kwargs = {'text': btn_text, 'callback_data': f"vi_p_{actual_pid}_c_{cat_id}", 'style': btn_style}
        custom_emoji_id = p.get('custom_emoji_id')
        if custom_emoji_id:
            btn_kwargs['icon_custom_emoji_id'] = custom_emoji_id
        markup.add(CustomInlineButton(**btn_kwargs))
    
    markup.add(create_btn(uid, 'btn_back', callback_data="open_shop"))
    
    txt = f'<tg-emoji emoji-id="{emoji_id}">{emoji}</tg-emoji> <b>{name}</b>' if emoji_id else f"{emoji} <b>{name}</b>"
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("vi_p_"))
def shop_detail_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)
    raw = call.data.replace('vi_p_', '')
    # دعم vi_p_{pid}_c_{cat_id}
    if '_c_' in raw:
        pid, cat_id_back = raw.split('_c_', 1)
    else:
        pid = raw
        cat_id_back = None
    
    p = find_product(pid)
    if not p:
        bot.send_message(uid, bil(uid, "❌ عذراً، المنتج غير متوفر.", "❌ Sorry, product is unavailable."), parse_mode="HTML"); return

    u = get_user_data_full(uid)
    is_admin = (u.get('is_admin') == 1 or uid == OWNER_ID)
    if p.get('is_hidden', False) and not is_admin:
        bot.send_message(uid, bil(uid, "❌ عذراً، هذا المنتج غير متوفر حالياً.", "❌ Sorry, this product is currently unavailable."), parse_mode="HTML"); return

    # ── ChatGPT Main: صفحة موحدة بكل المدد ──
    if p.get('product_type') == 'cgpt_main' and p.get('cgpt_product_id'):
        cgpt_parent_id = p['cgpt_product_id']
        try:
            from bson import ObjectId as _ObjId
            parent = db.cgpt_products.find_one({'_id': _ObjId(cgpt_parent_id)})
        except:
            parent = None

        p_name = (parent.get('name', '') if parent else '') or clean_name(p.get('name_ar') or p.get('name_en', ''))
        p_desc = (parent.get('desc', '') if parent else '') or clean_name(p.get('desc_ar') or p.get('desc_en', ''))
        durations = (parent.get('durations', []) if parent else [])
        durations_sorted = sorted(durations, key=lambda x: x.get('price', 0))
        custom_emoji_id = p.get('custom_emoji_id')
        icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji>' if custom_emoji_id else '🤖'

        if l == 'ar':
            text = (
                f"{icon_html} <b>{p_name}</b>\n\n"
                f"📝 {p_desc}\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"⚡ <b>التسليم:</b> تلقائي فوري\n"
                f"📦 <b>المخزون:</b> غير محدود\n"
                f"━━━━━━━━━━━━━━\n\n"
                f"🗓 <b>اختر المدة:</b>"
            )
        else:
            text = (
                f"{icon_html} <b>{p_name}</b>\n\n"
                f"📝 {p_desc}\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"⚡ <b>Delivery:</b> Instant\n"
                f"📦 <b>Stock:</b> Unlimited\n"
                f"━━━━━━━━━━━━━━\n\n"
                f"🗓 <b>Choose duration:</b>"
            )

        back_cb = f"cat_{cat_id_back}" if cat_id_back else "open_shop"
        markup = InlineKeyboardMarkup(row_width=1)

        for dur in durations_sorted:
            dur_id    = dur.get('dur_id', '')
            dur_label = dur.get('label', '')
            dur_price = float(dur.get('price', 0))
            markup.add(CustomInlineButton(
                text=f"{dur_label} — ${dur_price:.2f}",
                callback_data=f"cgpt_buy_{cgpt_parent_id}_{dur_id}",
                style="success"
            ))

        markup.add(create_btn(uid, 'btn_back', callback_data=back_cb))
        if is_admin:
            edit_cb = f"edit_p_{pid}_c_{cat_id_back}" if cat_id_back else f"edit_p_{pid}"
            markup.add(InlineKeyboardButton("⚙️ ...", callback_data=edit_cb))

        try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        except: pass
        return

    # ── منتج عادي ──
    is_manual = p.get('is_manual', False)
    st = get_product_stock_count(pid)

    if l == 'ar':
        delivery_type = "يدوي 🤝 (تواصل مع الإدارة بعد الدفع)" if is_manual else "تلقائي ⚡ (تسليم فوري)"
        st_text = "غير محدود" if is_manual else f"{st} قطعة"
    else:
        delivery_type = "Manual 🤝 (Contact admin after payment)" if is_manual else "Auto ⚡ (Instant delivery)"
        st_text = "Unlimited" if is_manual else f"{st} pcs"

    n = p.get('name_en') if l == 'en' else p.get('name_ar')
    if not n: n = "بدون اسم"
    d = p.get('desc_en') if l == 'en' else p.get('desc_ar')
    if not d: d = ""
    custom_emoji_id = p.get('custom_emoji_id')
    icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji>' if custom_emoji_id else '📦'

    discount_tiers = p.get('discount_tiers', [])
    discount_text = ""
    if discount_tiers:
        sorted_tiers = sorted(discount_tiers, key=lambda x: x.get('min_qty', 0))
        if l == 'ar':
            discount_text = "\n\n🏷 <b>خصومات الكمية:</b>\n"
            for t in sorted_tiers:
                t_price = float(t.get('price', 0))
                discount_text += f"  • {t.get('min_qty')}+ قطعة = <b>${t_price:.2f}</b>/قطعة\n"
        else:
            discount_text = "\n\n🏷 <b>Quantity Discounts:</b>\n"
            for t in sorted_tiers:
                t_price = float(t.get('price', 0))
                discount_text += f"  • {t.get('min_qty')}+ units = <b>${t_price:.2f}</b>/unit\n"

    if l == 'en':
        text = f"{icon_html} <b>{n}</b>\n\n📝 {d}\n\n🚚 <b>Delivery:</b> {delivery_type}\n💰 <b>Price:</b> ${p.get('price', 0):.2f}\n📊 <b>Stock:</b> {st_text}{discount_text}"
    else:
        text = f"{icon_html} <b>{n}</b>\n\n📝 {d}\n\n🚚 <b>نوع التسليم:</b> {delivery_type}\n💰 <b>السعر:</b> ${p.get('price', 0):.2f}\n📊 <b>المتوفر:</b> {st_text}{discount_text}"

    back_cb = f"cat_{cat_id_back}" if cat_id_back else "open_shop"
    markup = InlineKeyboardMarkup()
    if is_manual or st > 0:
        qty_cb = f"buy_qty_{pid}_c_{cat_id_back}" if cat_id_back else f"buy_qty_{pid}"
        markup.add(create_btn(uid, 'btn_buy_now', callback_data=qty_cb))
    markup.add(create_btn(uid, 'btn_back', callback_data=back_cb))

    if _is_admin_check(uid):
        edit_cb = f"edit_p_{pid}_c_{cat_id_back}" if cat_id_back else f"edit_p_{pid}"
        markup.add(InlineKeyboardButton("⚙️ ...", callback_data=edit_cb))

    try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_qty_"))
def prompt_quantity(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)
    raw_bq = call.data.replace('buy_qty_', '')
    if '_c_' in raw_bq:
        pid, cat_id_back = raw_bq.split('_c_', 1)
    else:
        pid = raw_bq
        cat_id_back = None
    
    p = find_product(pid)
    if not p: return
    
    is_manual = p.get('is_manual', False)
    if not is_manual and get_product_stock_count(pid) == 0:
        bot.send_message(uid, get_text(uid, 'out_stock'), parse_mode="HTML")
        return
    
    unit_price = float(p.get('price', 0))
    p_name = clean_name(p.get('name_ar') if l == 'ar' else p.get('name_en', p.get('name_ar', '')))
    custom_emoji_id = p.get('custom_emoji_id')
    icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> ' if custom_emoji_id else '📦 '
    
    # نبني جدول الخصومات
    discount_tiers = sorted(p.get('discount_tiers', []), key=lambda x: x.get('min_qty', 0))
    
    if l == 'ar':
        qty_msg = (
            f"{icon_html}<b>{p_name}</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💰 <b>السعر:</b> ${unit_price:.2f} / قطعة\n"
        )
        if discount_tiers:
            qty_msg += "\n🏷 <b>خصومات الكمية:</b>\n"
            for t in discount_tiers:
                t_price = float(t.get('price', unit_price))
                qty_msg += f"  • {t['min_qty']}+ قطعة → <b>${t_price:.2f}</b>/قطعة\n"
        qty_msg += (
            f"\n━━━━━━━━━━━━━━\n"
            f"📝 <b>أرسل الكمية المطلوبة:</b>"
        )
    else:
        qty_msg = (
            f"{icon_html}<b>{p_name}</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"💰 <b>Price:</b> ${unit_price:.2f} / unit\n"
        )
        if discount_tiers:
            qty_msg += "\n🏷 <b>Quantity Discounts:</b>\n"
            for t in discount_tiers:
                t_price = float(t.get('price', unit_price))
                qty_msg += f"  • {t['min_qty']}+ units → <b>${t_price:.2f}</b>/unit\n"
        qty_msg += (
            f"\n━━━━━━━━━━━━━━\n"
            f"📝 <b>Send the quantity:</b>"
        )
    
    msg = bot.send_message(uid, qty_msg, parse_mode="HTML")
    bot.register_next_step_handler(msg, execute_bulk_buy, pid, l)

def execute_bulk_buy(message, pid, lang):
    uid = message.from_user.id
    if is_user_banned(uid): return
    if not hasattr(message, 'text') or hasattr(message, 'data'): return
    if not message.text or not message.text.strip().isdigit():
        bot.send_message(uid, get_text(uid, 'qty_invalid'), parse_mode="HTML"); return
        
    qty = int(message.text.strip())
    if qty <= 0:
        bot.send_message(uid, get_text(uid, 'qty_invalid'), parse_mode="HTML"); return
    
    # نجلب المنتج لحساب السعر مع الخصم
    p = find_product(pid)
    if not p:
        bot.send_message(uid, bil(uid, "❌ المنتج غير موجود.", "❌ Product not found."), parse_mode="HTML")
        return
    
    unit_price = float(p.get('price', 0))
    p_name = clean_name(p.get('name_ar') if lang == 'ar' else p.get('name_en', p.get('name_ar', '')))
    discount_tiers = sorted(p.get('discount_tiers', []), key=lambda x: x.get('discount', 0), reverse=True)
    
    # 🏷 حساب الخصم - سعر ثابت بالدولار
    discounted_unit = unit_price
    for tier in sorted(discount_tiers, key=lambda x: x.get('min_qty', 0), reverse=True):
        if qty >= tier.get('min_qty', 0):
            discounted_unit = float(tier.get('price', unit_price))
            break
    total_price = round(discounted_unit * qty, 2)
    has_discount = (discounted_unit < unit_price)
    
    # ملخص السعر للتأكيد
    if has_discount:
        if lang == 'ar':
            price_summary = (
                f"📦 <b>{p_name}</b>\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🔢 الكمية: <b>{qty}</b>\n"
                f"💵 السعر الأصلي: <strike>${unit_price:.2f}</strike>/قطعة\n"
                f"✅ سعر الخصم: <b>${discounted_unit:.2f}</b>/قطعة\n"
                f"━━━━━━━━━━━━━━\n"
                f"💰 <b>الإجمالي: ${total_price:.2f}</b>\n\n"
                f"هل تؤكد الشراء؟"
            )
        else:
            price_summary = (
                f"📦 <b>{p_name}</b>\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🔢 Qty: <b>{qty}</b>\n"
                f"💵 Original: <strike>${unit_price:.2f}</strike>/unit\n"
                f"✅ Discounted: <b>${discounted_unit:.2f}</b>/unit\n"
                f"━━━━━━━━━━━━━━\n"
                f"💰 <b>Total: ${total_price:.2f}</b>\n\n"
                f"Confirm purchase?"
            )
    else:
        if lang == 'ar':
            price_summary = (
                f"📦 <b>{p_name}</b>\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🔢 الكمية: <b>{qty}</b>\n"
                f"💵 السعر: <b>${unit_price:.2f}</b>/قطعة\n"
                f"━━━━━━━━━━━━━━\n"
                f"💰 <b>الإجمالي: ${total_price:.2f}</b>\n\n"
                f"هل تؤكد الشراء؟"
            )
        else:
            price_summary = (
                f"📦 <b>{p_name}</b>\n\n"
                f"━━━━━━━━━━━━━━\n"
                f"🔢 Qty: <b>{qty}</b>\n"
                f"💵 Price: <b>${unit_price:.2f}</b>/unit\n"
                f"━━━━━━━━━━━━━━\n"
                f"💰 <b>Total: ${total_price:.2f}</b>\n\n"
                f"Confirm purchase?"
            )
    
    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton(
            "✅ تأكيد" if lang == 'ar' else "✅ Confirm",
            callback_data=f"confirm_buy_{pid}_{qty}"
        ),
        InlineKeyboardButton(
            "❌ إلغاء" if lang == 'ar' else "❌ Cancel",
            callback_data="open_shop"
        )
    )
    bot.send_message(uid, price_summary, parse_mode="HTML", reply_markup=markup)



@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_buy_"))
def confirm_buy_handler(call):
    uid = call.from_user.id
    if is_user_banned(uid): return
    lang = get_lang(uid)
    
    # 🛡 أول شيء: نحذف أزرار الرسالة فوراً عشان ما يضغط مرتين
    try:
        bot.edit_message_reply_markup(
            call.message.chat.id,
            call.message.message_id,
            reply_markup=None
        )
    except: pass
    
    bot.answer_callback_query(call.id)
    
    parts = call.data.split('_')
    try:
        qty = int(parts[-1])
        pid = '_'.join(parts[2:-1])
    except:
        return

    if is_deposit_locked(uid):
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(
            "❌ إلغاء عملية الإيداع" if lang == 'ar' else "❌ Cancel Deposit",
            callback_data="cancel_deposit"
        ))
        bot.send_message(uid, bil(uid,
            "⚠️ <b>لديك عملية إيداع جارية!</b>\n\nيجب إكمال الإيداع أو إلغاؤه أولاً.",
            "⚠️ <b>You have a pending deposit!</b>\n\nPlease complete or cancel it first."
        ), parse_mode="HTML", reply_markup=markup)
        return

    # رسالة "جاري المعالجة" من CMS
    try:
        cms_proc = db.custom_texts.find_one({'lang': 'en', 'key': 'processing_msg'})
        proc_text = cms_proc['value'] if cms_proc and cms_proc.get('value') else bil(uid, "⏳ <b>جاري معالجة طلبك...</b>", "⏳ <b>Processing your order...</b>")
        bot.edit_message_text(proc_text, call.message.chat.id, call.message.message_id, parse_mode="HTML")
    except: pass

    _do_purchase(uid, pid, qty, lang)


# قاموس مؤقت لبيانات شراء ChatGPT
_cgpt_pending = {}

@bot.callback_query_handler(func=lambda call: call.data.startswith("cgpt_buy_"))
def cgpt_buy_duration(call):
    """الزبون يضغط على مدة محددة → نطلب الإيميل"""
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)

    raw = call.data.replace("cgpt_buy_", "")
    parts = raw.split("_", 1)
    if len(parts) != 2:
        return
    cgpt_pid, dur_id = parts[0], parts[1]

    try:
        from bson import ObjectId as _ObjId2
        parent = db.cgpt_products.find_one({'_id': _ObjId2(cgpt_pid)})
    except:
        parent = None

    if not parent:
        bot.send_message(uid, "❌ المنتج غير موجود." if l == 'ar' else "❌ Product not found.")
        return

    dur = next((d for d in parent.get('durations', []) if d.get('dur_id') == dur_id), None)
    if not dur:
        bot.send_message(uid, "❌ المدة غير موجودة." if l == 'ar' else "❌ Duration not found.")
        return

    price = float(dur.get('price', 0))
    label = dur.get('label', '')
    minutes = int(dur.get('minutes', 10080))
    u = get_user_data_full(uid)
    balance = round(float(u.get('balance', 0)), 2) if u else 0.0

    if balance < price:
        send_no_balance(uid)
        return

    # نحفظ بيانات الشراء
    order_id = "CG" + str(int(time.time()))[-6:] + str(uid)[-4:]
    _cgpt_pending[uid] = {
        'cgpt_pid': cgpt_pid,
        'dur_id': dur_id,
        'label': label,
        'price': price,
        'minutes': minutes,
        'order_id': order_id,
        'p_name': parent.get('name', ''),
    }

    if l == 'ar':
        msg_txt = (
            f"✅ <b>المدة المختارة: {label} — ${price:.2f}</b>\n\n"
            f"📧 <b>أرسل إيميل حساب ChatGPT الخاص بك:</b>"
        )
    else:
        msg_txt = (
            f"✅ <b>Selected: {label} — ${price:.2f}</b>\n\n"
            f"📧 <b>Send your ChatGPT account email:</b>"
        )
    msg = bot.send_message(uid, msg_txt, parse_mode="HTML")
    bot.register_next_step_handler(msg, cgpt_confirm_email_step, uid, l)

def cgpt_confirm_email_step(message, buyer_uid, lang):
    """الخطوة 1: يرسل الإيميل → نطلب تأكيده"""
    import re as _re3
    email = (message.text or "").strip().lower()
    if not _re3.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
        bot.send_message(buyer_uid,
            "❌ <b>إيميل غير صحيح، أرسله مجدداً.</b>" if lang == 'ar' else
            "❌ <b>Invalid email, please send again.</b>",
            parse_mode="HTML")
        pending = _cgpt_pending.get(buyer_uid)
        if pending:
            msg = bot.send_message(buyer_uid,
                "📧 <b>أرسل إيميل حساب ChatGPT:</b>" if lang == 'ar' else
                "📧 <b>Send your ChatGPT email:</b>",
                parse_mode="HTML")
            bot.register_next_step_handler(msg, cgpt_confirm_email_step, buyer_uid, lang)
        return

    # نطلب التأكيد
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("✅ نعم، صحيح" if lang == 'ar' else "✅ Yes, correct",
            callback_data=f"cgpt_email_ok_{buyer_uid}"),
        InlineKeyboardButton("✏️ تغييره" if lang == 'ar' else "✏️ Change it",
            callback_data=f"cgpt_email_change_{buyer_uid}")
    )
    _cgpt_pending[buyer_uid]['email'] = email
    bot.send_message(buyer_uid,
        f"📧 <b>الإيميل:</b> <code>{email}</code>\n\n"
        f"{'هل هذا الإيميل صحيح؟' if lang == 'ar' else 'Is this email correct?'}",
        parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("cgpt_email_change_"))
def cgpt_email_change(call):
    bot.answer_callback_query(call.id)
    buyer_uid = int(call.data.replace("cgpt_email_change_", ""))
    lang = get_lang(buyer_uid)
    msg = bot.send_message(call.message.chat.id,
        "📧 <b>أرسل الإيميل الصحيح:</b>" if lang == 'ar' else "📧 <b>Send the correct email:</b>",
        parse_mode="HTML")
    bot.register_next_step_handler(msg, cgpt_confirm_email_step, buyer_uid, lang)

@bot.callback_query_handler(func=lambda call: call.data.startswith("cgpt_email_ok_"))
def cgpt_email_confirmed(call):
    """تأكيد الإيميل → تنفيذ الشراء"""
    bot.answer_callback_query(call.id)
    buyer_uid = int(call.data.replace("cgpt_email_ok_", ""))
    lang = get_lang(buyer_uid)
    pending = _cgpt_pending.pop(buyer_uid, None)
    if not pending or 'email' not in pending:
        bot.send_message(call.message.chat.id,
            "❌ انتهت الجلسة." if lang == 'ar' else "❌ Session expired.")
        return

    email   = pending['email']
    price   = pending['price']
    minutes = pending['minutes']
    label   = pending['label']
    order_id = pending['order_id']
    p_name  = pending['p_name']

    # خصم الرصيد أتوميك
    updated = db.users.find_one_and_update(
        {'user_id': buyer_uid, 'balance': {'$gte': price}},
        {'$inc': {'balance': -price}},
        return_document=True
    )
    if not updated:
        send_no_balance(buyer_uid)
        return

    bot.send_message(buyer_uid,
        "⏳ <b>جاري إرسال الدعوة...</b>" if lang == 'ar' else "⏳ <b>Sending invite...</b>",
        parse_mode="HTML")

    mgr = get_cgpt_manager()
    mgr._last_buyer_uid = buyer_uid
    result = mgr.invite_user(email, minutes)
    days = round(minutes / 1440, 1)

    if result['ok']:
        expires_iso = result['expires_at']
        db.orders.insert_one({
            'user_id': buyer_uid, 'product_id': f"cgpt_{pending['cgpt_pid']}",
            'code_delivered': f"chatgpt_seat:{email}",
            'qty': 1, 'total_price': price, 'order_id': order_id,
            'cgpt_email': email, 'cgpt_expires_at': expires_iso, 'cgpt_minutes': minutes
        })
        u_data = get_user_data_full(buyer_uid)
        buyer_m = f"@{u_data.get('username')}" if u_data and u_data.get('username') else str(buyer_uid)
        if lang == 'ar':
            success = (
                f"✅ <b>تم إرسال الدعوة بنجاح!</b>\n\n"
                f"📧 <b>الإيميل:</b> <code>{email}</code>\n"
                f"⏱ <b>المدة:</b> {label}\n"
                f"📅 <b>ينتهي في:</b> {expires_iso[:10]}\n\n"
                f"<i>تفقد بريدك الإلكتروني وقبل الدعوة 🎉</i>"
            )
        else:
            success = (
                f"✅ <b>Invite sent!</b>\n\n"
                f"📧 <b>Email:</b> <code>{email}</code>\n"
                f"⏱ <b>Duration:</b> {label}\n"
                f"📅 <b>Expires:</b> {expires_iso[:10]}\n\n"
                f"<i>Check your inbox and accept the invite 🎉</i>"
            )
        bot.send_message(buyer_uid, success, parse_mode="HTML")
        notify_admins(
            f"🤖 <b>ChatGPT — شراء</b>\n"
            f"👤 {buyer_m} (<code>{buyer_uid}</code>)\n"
            f"📧 <code>{email}</code>\n"
            f"⏱ {label}\n💰 ${price:.2f}\n🆔 <code>{order_id}</code>"
        )
    else:
        db.users.update_one({'user_id': buyer_uid}, {'$inc': {'balance': price}})
        bot.send_message(buyer_uid,
            f"❌ <b>فشل إرسال الدعوة. تم إرجاع رصيدك.</b>\n<code>{result.get('error','')}</code>"
            if lang == 'ar' else
            f"❌ <b>Invite failed. Balance refunded.</b>\n<code>{result.get('error','')}</code>",
            parse_mode="HTML")

def _cgpt_handle_email(message, buyer_uid, lang):
    pending = _cgpt_pending.pop(buyer_uid, None)
    if not pending:
        bot.send_message(buyer_uid, "\u274c \u0627\u0646\u062a\u0647\u062a \u0635\u0644\u0627\u062d\u064a\u0629 \u0627\u0644\u0637\u0644\u0628. \u062a\u0648\u0627\u0635\u0644 \u0645\u0639 \u0627\u0644\u062f\u0639\u0645.", parse_mode="HTML")
        return
    import re as _re_email
    email = (message.text or "").strip().lower()
    if not _re_email.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
        db.users.update_one({'user_id': buyer_uid}, {'$inc': {'balance': pending['total_price']}})
        bot.send_message(buyer_uid,
            f"\u274c <b>\u0625\u064a\u0645\u064a\u0644 \u063a\u064a\u0631 \u0635\u062d\u064a\u062d. \u062a\u0645 \u0625\u0631\u062c\u0627\u0639 \u0631\u0635\u064a\u062f\u0643.</b>\n"
            f"\u0627\u0644\u0631\u0635\u064a\u062f \u0627\u0644\u0645\u064f\u0631\u062c\u0639: <b>${pending['total_price']:.2f}</b>",
            parse_mode="HTML")
        return
    bot.send_message(buyer_uid, "\u23f3 <b>\u062c\u0627\u0631\u064a \u0625\u0631\u0633\u0627\u0644 \u0627\u0644\u062f\u0639\u0648\u0629...</b>", parse_mode="HTML")
    mgr = get_cgpt_manager()
    mgr._last_buyer_uid = buyer_uid
    result = mgr.invite_user(email, pending['minutes'])
    days = round(pending['minutes'] / 1440, 1)
    if result['ok']:
        expires_iso = result['expires_at']
        db.orders.insert_one({
            'user_id': buyer_uid, 'product_id': pending['pid'],
            'code_delivered': f"chatgpt_seat:{email}",
            'qty': 1, 'total_price': pending['total_price'],
            'order_id': pending['order_id'],
            'cgpt_email': email, 'cgpt_expires_at': expires_iso,
            'cgpt_minutes': pending['minutes']
        })
        u_data = get_user_data_full(buyer_uid)
        buyer_m = f"@{u_data.get('username')}" if u_data and u_data.get('username') else str(buyer_uid)
        if lang == 'ar':
            success = (f"\u2705 <b>\u062a\u0645 \u0625\u0631\u0633\u0627\u0644 \u0627\u0644\u062f\u0639\u0648\u0629 \u0628\u0646\u062c\u0627\u062d!</b>\n\n"
                f"\U0001f4e7 <b>\u0627\u0644\u0625\u064a\u0645\u064a\u0644:</b> <code>{email}</code>\n"
                f"\u23f1 <b>\u0645\u062f\u0629 \u0627\u0644\u0648\u0635\u0648\u0644:</b> {days} \u064a\u0648\u0645\n"
                f"\U0001f4c5 <b>\u064a\u0646\u062a\u0647\u064a \u0641\u064a:</b> {expires_iso[:10]}\n\n"
                f"<i>\u062a\u0641\u0642\u062f \u0628\u0631\u064a\u062f\u0643 \u0627\u0644\u0625\u0644\u0643\u062a\u0631\u0648\u0646\u064a \u0648\u0642\u0628\u0644 \u0627\u0644\u062f\u0639\u0648\u0629 \U0001f389</i>")
        else:
            success = (f"\u2705 <b>Invite sent successfully!</b>\n\n"
                f"\U0001f4e7 <b>Email:</b> <code>{email}</code>\n"
                f"\u23f1 <b>Duration:</b> {days} days\n"
                f"\U0001f4c5 <b>Expires:</b> {expires_iso[:10]}\n\n"
                f"<i>Check your inbox and accept the invite \U0001f389</i>")
        bot.send_message(buyer_uid, success, parse_mode="HTML")
        notify_admins(
            f"\U0001f916 <b>ChatGPT Seat \u2014 \u0634\u0631\u0627\u0621</b>\n"
            f"\U0001f464 {buyer_m} (<code>{buyer_uid}</code>)\n"
            f"\U0001f4e7 <code>{email}</code>\n"
            f"\u23f1 {days} \u064a\u0648\u0645\n"
            f"\U0001f4b0 ${pending['total_price']:.2f}\n"
            f"\U0001f194 <code>{pending['order_id']}</code>"
        )
    else:
        db.users.update_one({'user_id': buyer_uid}, {'$inc': {'balance': pending['total_price']}})
        bot.send_message(buyer_uid,
            f"\u274c <b>\u0641\u0634\u0644 \u0625\u0631\u0633\u0627\u0644 \u0627\u0644\u062f\u0639\u0648\u0629. \u062a\u0645 \u0625\u0631\u062c\u0627\u0639 \u0631\u0635\u064a\u062f\u0643.</b>\n"
            f"\u0627\u0644\u0633\u0628\u0628: <code>{result.get('error', 'unknown')}</code>",
            parse_mode="HTML")
        notify_admins(f"\U0001f6a8 ChatGPT Seat \u2014 \u0641\u0634\u0644 \u062f\u0639\u0648\u0629\n<code>{buyer_uid}</code> / <code>{email}</code>\n{result.get('error', '')}")


def _do_purchase(uid, pid, qty, lang):
    """المنطق الفعلي للشراء"""
    # 🛡 Rate Limiting
    if not _acquire_purchase_lock(uid):
        bot.send_message(uid, bil(uid,
            "⏳ <b>يوجد طلب شراء قيد المعالجة!</b>\nانتظر انتهاءه.",
            "⏳ <b>Purchase in progress!</b>\nWait until it finishes."
        ), parse_mode="HTML")
        return

    # 🔒 منع الشراء أثناء عملية إيداع جارية
    if is_deposit_locked(uid):
        _release_purchase_lock(uid)
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(
            "❌ إلغاء عملية الإيداع" if lang == 'ar' else "❌ Cancel Deposit",
            callback_data="cancel_deposit"
        ))
        bot.send_message(uid, bil(uid,
            "⚠️ <b>لديك عملية إيداع جارية!</b>\n\nيجب إكمال الإيداع أو إلغاؤه أولاً.",
            "⚠️ <b>You have a pending deposit!</b>\n\nPlease complete or cancel it first."
        ), parse_mode="HTML", reply_markup=markup)
        return

    try:
        u = get_user_data_full(uid)
        p = find_product(pid)
        if not p:
            bot.send_message(uid, bil(uid, "❌ المنتج غير موجود.", "❌ Product not found."), parse_mode="HTML")
            return

        product_type = p.get('product_type', 'standard')
        is_manual = p.get('is_manual', False)
        unit_price = float(p.get('price', 0))

        # 🏷 نظام الخصومات - سعر ثابت بالدولار
        discounted_unit = unit_price
        discount_tiers = sorted(p.get('discount_tiers', []), key=lambda x: x.get('min_qty', 0), reverse=True)
        for tier in discount_tiers:
            if qty >= tier.get('min_qty', 0):
                discounted_unit = float(tier.get('price', unit_price))
                break
        total_price = round(discounted_unit * qty, 2)

        # 🛡 حجز الأكواد atomically
        reserved_items = []
        reservation_id = f"{uid}_{int(time.time() * 1000)}"

        if not is_manual:
            pid_str = str(pid)
            queries = [{'product_id': pid_str}]
            if pid_str.isdigit(): queries.append({'product_id': int(pid_str)})
            try: queries.append({'product_id': float(pid_str)})
            except: pass

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
                    _release_reservation(reservation_id)
                    bot.send_message(uid, get_text(uid, 'qty_not_enough', len(reserved_items)), parse_mode="HTML")
                    return
                reserved_items.append(reserved)

        # 🛡 خصم الرصيد atomically
        updated_user = db.users.find_one_and_update(
            {'user_id': uid, 'balance': {'$gte': total_price}},
            {'$inc': {'balance': -total_price}},
            return_document=True
        )

        if updated_user is None:
            _release_reservation(reservation_id)
            send_no_balance(uid)
            return

        u = updated_user
        support_user = f"@{OWNER_USER}" if OWNER_USER else "الإدارة"
        buyer_m = f"@{u['username']}" if u and u.get('username') else f"عضو جديد"
        log_ch = get_setting('log_channel')

        if product_type == 'chatgpt_seat':
            # ── ChatGPT Business Seat ──
            # نطلب الإيميل أولاً ثم ندعوه
            cgpt_minutes = int(p.get('cgpt_minutes', 10080))
            _release_purchase_lock(uid)  # نفك الـ lock ريثما يكتب الإيميل
            order_id = "CG" + str(int(time.time()))[-6:] + str(uid)[-4:]
            # نحفظ بيانات الشراء مؤقتاً
            _cgpt_pending[uid] = {
                'pid': str(pid), 'qty': qty, 'total_price': total_price,
                'order_id': order_id, 'minutes': cgpt_minutes,
                'p_name_ar': clean_name(p.get('name_ar', '')),
                'p_name_en': clean_name(p.get('name_en', p.get('name_ar', '')))
            }
            if lang == 'ar':
                msg_txt = (
                    f"✅ <b>تم خصم ${total_price:.2f} من رصيدك!</b>\n\n"
                    f"📧 <b>أرسل إيميل حساب ChatGPT الخاص بك:</b>\n"
                    f"<i>(يجب أن يكون مسجلاً على chatgpt.com)</i>"
                )
            else:
                msg_txt = (
                    f"✅ <b>${total_price:.2f} deducted!</b>\n\n"
                    f"📧 <b>Send your ChatGPT account email:</b>\n"
                    f"<i>(Must be registered on chatgpt.com)</i>"
                )
            msg = bot.send_message(uid, msg_txt, parse_mode="HTML")
            bot.register_next_step_handler(msg, _cgpt_handle_email, uid, lang)
            return  # نرجع — الباقي سيتم في _cgpt_handle_email

        elif is_manual:
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
                logger.error(f"Failed to insert manual order: {e}")
                db.users.update_one({'user_id': uid}, {'$inc': {'balance': total_price}})
                bot.send_message(uid, bil(uid, "❌ حدث خطأ في معالجة الطلب. تم إرجاع رصيدك.", "❌ Error processing request. Balance refunded."), parse_mode="HTML")
                return

            if lang == 'ar':
                msg_txt = f"✅ <b>تم الطلب بنجاح! (${total_price:.2f})</b>\n\nهذا المنتج يتطلب تسليم يدوي.\nرقم طلبك: <code>{order_id}</code>\n\nتواصل مع {support_user}"
            else:
                msg_txt = f"✅ <b>Order Placed! (${total_price:.2f} deducted)</b>\n\nManual delivery product.\nOrder ID: <code>{order_id}</code>\n\nContact {support_user}"
            bot.send_message(uid, msg_txt, parse_mode="HTML")
            notify_admins(f"🔐 <b>إشعار إدارة (يدوي)</b>\n👤 {buyer_m} (<code>{uid}</code>)\n📦 {clean_name(p.get('name_ar'))}\n🔢 {qty}\n💰 ${total_price:.2f}\n🔖 <code>{order_id}</code>")
        else:
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
                logger.error(f"Critical: Failed during code delivery: {e}")
                notify_admins(f"⚠️ <b>تنبيه!</b>\nفشل تسليم أكواد للمستخدم <code>{uid}</code>\nالخطأ: {e}")

            # إرسال الأكواد كملف
            try:
                p_name = p.get(f'name_{lang}', p.get('name_en', p.get('name_ar', 'product')))
                file_content = f"=== {clean_name(p_name)} ===\nQty: {qty} | Total: ${total_price:.2f}\nDate: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*40}\n\n"
                for i, code in enumerate(delivered_codes, 1):
                    file_content += f"{i}. {code}\n"

                f = io.BytesIO(file_content.encode('utf-8'))
                safe_name = re.sub(r'[^\w\-]', '_', str(pid))[:20]
                f.name = f"codes_{safe_name}.txt"

                if lang == 'ar':
                    success_msg = f"✅ <b>تم الشراء بنجاح!</b>\n\n📦 {clean_name(p.get('name_ar'))}\n🔢 الكمية: <b>{qty}</b>\n💰 <b>${total_price:.2f}</b>\n\n📄 الأكواد في الملف أدناه 👇"
                else:
                    success_msg = f"✅ <b>Purchase Successful!</b>\n\n📦 {clean_name(p.get('name_en', p.get('name_ar')))}\n🔢 Qty: <b>{qty}</b>\n💰 <b>${total_price:.2f}</b>\n\n📄 Codes in the file below 👇"

                bot.send_document(uid, f, caption=success_msg, parse_mode="HTML")
            except Exception as file_err:
                logger.error(f"Failed to send file: {file_err}")
                try:
                    bot.send_message(uid, "✅ Done!\n\n" + "\n".join(delivered_codes))
                except:
                    notify_admins(f"🚨 فشل تسليم أكواد للمستخدم <code>{uid}</code>!")

            notify_admins(f"🔐 <b>إشعار إدارة (شراء)</b>\n👤 {buyer_m} (<code>{uid}</code>)\n📦 {clean_name(p.get('name_ar'))}\n🔢 {qty}\n💰 ${total_price:.2f}")

        # لوق القناة
        if log_ch and log_ch != "Not Set":
            try:
                obs_user = obscure_text(u.get('username') or str(uid))
                product_name_clean = clean_name(p.get('name_en', p.get('name_ar', 'Product')))
                custom_emoji_id = p.get('custom_emoji_id')
                product_name_log = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> <b>{product_name_clean}</b>' if custom_emoji_id else f'📦 <b>{product_name_clean}</b>'
                pub_msg = LANG['en']['log_purchase'].format(obs_user, product_name_log, qty)
                custom_pub = db.custom_texts.find_one({'lang': 'en', 'key': 'log_purchase'})
                if custom_pub and custom_pub.get('value'):
                    try: pub_msg = custom_pub['value'].format(obs_user, product_name_log, qty)
                    except: pass
                bot.send_message(log_ch, pub_msg, parse_mode="HTML")
            except Exception as log_err:
                logger.debug(f"Log channel error: {log_err}")

        # مكافأة الإحالة
        try:
            award_purchase_referral_reward(uid, clean_name(p.get('name_ar') or p.get('name_en') or ''), total_price)
        except Exception as ref_err:
            logger.error(f"Error awarding referral: {ref_err}")

        # 🔔 تنبيه الستوك (يصل لكل الأدمن، مو OWNER_ID فقط)
        if not is_manual:
            try:
                remaining = get_product_stock_count(pid)
                pname = clean_name(p.get('name_ar', ''))
                if remaining == 0:
                    notify_admins(
                        f"🚨 <b>تنبيه: ستوك انتهى!</b>\n\n"
                        f"📦 المنتج: <b>{pname}</b>\n"
                        f"📊 المتبقي: <b>0</b>\n\n"
                        f"⚠️ أضف ستوك جديد الآن!"
                    )
                elif remaining <= 2:
                    notify_admins(
                        f"⚠️ <b>تنبيه: ستوك قارب على الانتهاء!</b>\n\n"
                        f"📦 المنتج: <b>{pname}</b>\n"
                        f"📊 المتبقي: <b>{remaining}</b>\n\n"
                        f"⚠️ أضف ستوك جديد!"
                    )
            except Exception as _stk_e:
                logger.debug(f"Stock alert error: {_stk_e}")

    finally:
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
@bot.callback_query_handler(func=lambda call: call.data == "cancel_deposit")
def cancel_deposit_handler(call):
    """يلغي عملية الإيداع الجارية"""
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    l = get_lang(uid)
    
    # نحذف الـ pending
    db.pending_deposits.update_many(
        {'user_id': uid, 'status': 'pending'},
        {'$set': {'status': 'cancelled'}}
    )
    
    # 🔓 فك القفل
    unlock_deposit(uid)
    
    if l == 'ar':
        msg = "❌ <b>تم إلغاء عملية الإيداع.</b>\n\nيمكنك البدء من جديد في أي وقت."
    else:
        msg = "❌ <b>Deposit cancelled.</b>\n\nYou can start a new deposit anytime."
    
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    except: pass
    bot.send_message(uid, msg, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "open_deposit")
def dep_init_ui(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    if not check_forced_sub(uid): start_handler(call.message); return
    
    u = get_user_data_full(uid)
    balance = float(u.get('balance', 0)) if u else 0
    name = clean_name(u.get('name', '')) if u else ''
    uname = f"@{u['username']}" if u and u.get('username') else ''
    l = get_lang(uid)
    
    # إجمالي الإيداعات
    deps = list(db.used_transactions.find({'user_id': uid}))
    total_dep = sum(float(d.get('amount', 0)) for d in deps)
    
    if l == 'ar':
        wallet_text = (
            f"👛 <b>محفظتي</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"👤 <b>الاسم:</b> {name}\n"
            f"{f'🔗 <b>المعرف:</b> {uname}' if uname else ''}\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"💰 <b>الرصيد الحالي:</b>\n"
            f"<b>${balance:.2f}</b>\n\n"
            f"📦 <b>إجمالي ما شحنته:</b> ${total_dep:.2f}\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"💳 اختر طريقة الشحن:"
        )
    else:
        wallet_text = (
            f"👛 <b>My Wallet</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"👤 <b>Name:</b> {name}\n"
            f"{f'🔗 <b>Username:</b> {uname}' if uname else ''}\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"💰 <b>Current Balance:</b>\n"
            f"<b>${balance:.2f}</b>\n\n"
            f"📦 <b>Total Deposited:</b> ${total_dep:.2f}\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"💳 Choose deposit method:"
        )
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(create_btn(uid, 'btn_stars', callback_data="dep_stars"))
    markup.add(create_btn(uid, 'btn_binance', callback_data="dep_binance"))
    markup.add(create_btn(uid, 'btn_usdt_trc20', callback_data="dep_crypto_USDT"))
    markup.add(create_btn(uid, 'btn_usdt_bep20', callback_data="dep_crypto_USDT_BEP20"))
    markup.add(create_btn(uid, 'btn_ton', callback_data="dep_crypto_TON"))
    markup.add(create_btn(uid, 'btn_ltc', callback_data="dep_crypto_LTC"))
    markup.add(create_btn(uid, 'btn_back', callback_data="main_menu_refresh"))
    
    try:
        bot.edit_message_text(wallet_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except:
        bot.send_message(uid, wallet_text, reply_markup=markup, parse_mode="HTML")

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
            f"🟡 <b>Binance Pay</b>\n\n"
            f"💰 <b>المبلغ:</b>\n<code>${unique_amount:.4f}</code>\n\n"
            f"📬 <b>الآيدي:</b>\n<code>{wallet}</code>\n\n"
            f"⏰ صلاحية: <b>30 دقيقة</b>\n"
            f"✨ <i>الرصيد يُضاف تلقائياً</i>"
        )
    else:
        msg_text = (
            f"🟡 <b>Binance Pay</b>\n\n"
            f"💰 <b>Amount:</b>\n<code>${unique_amount:.4f}</code>\n\n"
            f"📬 <b>Pay ID:</b>\n<code>{wallet}</code>\n\n"
            f"⏰ Valid: <b>30 minutes</b>\n"
            f"✨ <i>Balance added automatically</i>"
        )

    cancel_markup = InlineKeyboardMarkup(row_width=1)
    cancel_markup.add(InlineKeyboardButton(
        "🔍 فحص الدفع" if l == 'ar' else "🔍 Check Payment",
        callback_data=f"binance_check_{uid}"
    ))
    cancel_markup.add(InlineKeyboardButton(
        "❌ إلغاء" if l == 'ar' else "❌ Cancel",
        callback_data="cancel_deposit"
    ))
    
    bot.send_message(uid, msg_text, parse_mode="HTML", reply_markup=cancel_markup)
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
    # 🛡 مسافة أمان: لازم تكون أكبر من نطاق المطابقة (±0.0001) عشان ما يتقارب إيداعان أبداً
    MIN_SPACING = 0.0002

    def _is_free(amount):
        """يتأكد ما فيه إيداع معلّق قريب من هذا المبلغ (داخل مسافة الأمان)."""
        try:
            clash = db.pending_deposits.find_one({
                'coin': coin,
                'status': 'pending',
                'expires_at': {'$gt': int(time.time())},
                'unique_amount_usd': {
                    '$gte': amount - MIN_SPACING,
                    '$lte': amount + MIN_SPACING
                }
            })
            return clash is None
        except Exception:
            return True

    # المحاولة 1: مدى السنت (0.000100 - 0.009999) مع مسافة أمان
    for _ in range(400):
        unique_amount = round(base + random.randint(100, 9999) / 1000000.0, 6)
        if _is_free(unique_amount):
            return unique_amount

    # المحاولة 2: مدى أوسع شوي (لين ~0.03) لو ازدحمت الإيداعات بنفس المبلغ
    for _ in range(400):
        unique_amount = round(base + random.randint(100, 29999) / 1000000.0, 6)
        if _is_free(unique_amount):
            return unique_amount

    # احتياط أخير (نادر جداً): نرجّع مبلغ حتى لو ما لقينا مسافة مثالية
    return round(base + random.randint(100, 99999) / 1000000.0, 6)


def register_pending_deposit(uid, base_amount_usd, unique_amount_usd, coin):
    """
    يسجّل عملية إيداع متوقعة ويقفل المستخدم حتى يكتمل أو يُلغى.
    """
    try:
        db.pending_deposits.delete_many({'user_id': uid, 'coin': coin, 'status': 'pending'})
        
        pending_id = f"PD{uid}{int(time.time())}{random.randint(100, 999)}"
        expires = int(time.time()) + (60 * 30)
        record = {
            'pending_id': pending_id,
            'user_id': uid,
            'base_amount_usd': float(base_amount_usd),
            'unique_amount_usd': float(unique_amount_usd),
            'coin': coin,
            'status': 'pending',
            'created_at': int(time.time()),
            'expires_at': expires
        }
        db.pending_deposits.insert_one(record)
        
        # 🔒 قفل المستخدم أثناء الإيداع
        db.users.update_one(
            {'user_id': uid},
            {'$set': {
                'deposit_locked': True,
                'deposit_lock_pending_id': pending_id,
                'deposit_lock_expires': expires
            }}
        )
        return record
    except Exception as e:
        logger.error(f"Error registering pending deposit: {e}")
        return None


def unlock_deposit(uid):
    """فك قفل الإيداع عند الاكتمال أو الإلغاء"""
    try:
        db.users.update_one(
            {'user_id': uid},
            {'$unset': {'deposit_locked': '', 'deposit_lock_pending_id': '', 'deposit_lock_expires': ''}}
        )
    except Exception: pass


def is_deposit_locked(uid):
    """هل المستخدم في عملية إيداع جارية؟"""
    try:
        u = db.users.find_one({'user_id': uid}, {'deposit_locked': 1, 'deposit_lock_expires': 1})
        if not u or not u.get('deposit_locked'):
            return False
        # نفحص إن القفل ما انتهت صلاحيته
        if u.get('deposit_lock_expires', 0) < int(time.time()):
            unlock_deposit(uid)
            return False
        # نفحص إن في pending فعلاً
        pending = db.pending_deposits.find_one({
            'user_id': uid,
            'status': 'pending',
            'expires_at': {'$gt': int(time.time())}
        })
        if not pending:
            unlock_deposit(uid)
            return False
        return True
    except Exception:
        return False


_ambiguous_alert_cache = {}

def _alert_ambiguous_deposit(coin, amount_usd, records):
    """🚨 يبلّغ الأدمن عن إيداع غامض (مرة كل ساعة لنفس المبلغ لتفادي التكرار)."""
    try:
        key = f"{coin}:{round(float(amount_usd), 5)}"
        now = int(time.time())
        if now - _ambiguous_alert_cache.get(key, 0) < 3600:
            return
        _ambiguous_alert_cache[key] = now
        ids = ", ".join(
            f"{r.get('user_id')}=${float(r.get('unique_amount_usd', 0)):.6f}" for r in records[:5]
        )
        notify_admins(
            f"⚠️ <b>إيداع غامض — مراجعة يدوية مطلوبة!</b>\n\n"
            f"💳 العملة: <b>{coin}</b>\n"
            f"💰 المبلغ المُستلَم: <b>${float(amount_usd):.6f}</b>\n"
            f"🔀 يطابق أكثر من إيداع معلّق بنفس القرب:\n<code>{ids}</code>\n\n"
            f"🛡 <b>لم يُضَف الرصيد لأحد تلقائياً</b> حمايةً من إعطاء الشخص الخطأ.\n"
            f"راجع وأضف الرصيد يدوياً للشخص الصحيح من «👥 إدارة العملاء ← تعديل رصيده»."
        )
    except Exception:
        pass


def find_pending_deposit_for_amount(amount_usd, coin, tolerance=0.0001):
    """
    🛡 يبحث عن إيداع معلّق مطابق للمبلغ المُستلَم.

    الإصلاح المهم:
    - يطابق على الإيداع **الأقرب** لقيمة المبلغ المُستلَم بالضبط (مو الأقدم!).
    - لو فيه إيداعان بنفس القرب تماماً (غموض) → ما يرجّع أحد + يبلّغ الأدمن
      (أأمن من إعطاء الشخص الخطأ).
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
        }))

        if not records:
            return None
        if len(records) == 1:
            return records[0]

        # 🛡 أكثر من إيداع داخل النطاق — نرتّب بالأقرب للمبلغ بالضبط (مو الأقدم)
        records.sort(key=lambda r: (
            abs(float(r.get('unique_amount_usd', 0)) - amount_usd),
            r.get('created_at', 0)
        ))
        closest, second = records[0], records[1]
        d1 = abs(float(closest.get('unique_amount_usd', 0)) - amount_usd)
        d2 = abs(float(second.get('unique_amount_usd', 0)) - amount_usd)

        # لو الأقرب أوضح من الثاني بفارق كافٍ → هو الصحيح (آمن نعطيه)
        if (d2 - d1) >= 0.00005:
            return closest

        # 🚨 غموض حقيقي: إيداعان متساويان في القرب — نوقف ونبلّغ الأدمن، ما نخاطر
        _alert_ambiguous_deposit(coin, amount_usd, records)
        return None
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
            return False
        
        # 🔓 فك قفل الإيداع
        unlock_deposit(uid)
        
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
    يعيد حساب الـ timestamp والـ signature مع كل محاولة.
    """
    import hmac, hashlib, urllib.parse

    api_key = BINANCE_API_KEY
    api_secret = BINANCE_API_SECRET
    
    if not api_key or not api_secret:
        return None
    
    url = "https://api.binance.com/sapi/v1/pay/transactions"
    
    def _make_signed_params():
        """ينشئ params جديدة مع timestamp وsignature طازجين"""
        ts = int(time.time() * 1000)
        p = {'timestamp': ts, 'limit': 100, 'recvWindow': 60000}
        qs = urllib.parse.urlencode(p)
        sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        p['signature'] = sig
        return p

    def _parse_response(resp):
        """يحلل الرد ويرجع البيانات أو None"""
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'SUCCESS' or data.get('code') == '000000':
                return data.get('data', [])
            logger.warning(f"Binance Pay API: {data.get('code')} - {data.get('errorMessage', data.get('msg', ''))}")
            return None
        return False  # False = جرب بروكسي ثاني

    # نجرب مع بروكسيات أولاً
    if not VERIFIED_PROXIES:
        refresh_proxies(force=True)

    for proxy in list(VERIFIED_PROXIES[:10]) if VERIFIED_PROXIES else []:
        try:
            params = _make_signed_params()
            resp = requests.get(url, params=params, headers={'X-MBX-APIKEY': api_key},
                                timeout=12, proxies={'http': proxy, 'https': proxy})
            result = _parse_response(resp)
            if result is not False:
                return result
            # 400/403 = بروكسي يعدّل الطلب، نجرب غيره
            _remove_dead_proxy(proxy)
        except Exception:
            _remove_dead_proxy(proxy)
            continue

    # آخر محاولة بدون بروكسي
    try:
        params = _make_signed_params()
        resp = requests.get(url, params=params, headers={'X-MBX-APIKEY': api_key}, timeout=15)
        result = _parse_response(resp)
        if result is not False:
            return result
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
                # نفضّل orderId الرقمي (مثل 433332644536672256)
                # على transactionId الحروفي (مثل pa223f41nn6s7111a)
                order_id = str(tx.get('orderId') or '').strip()
                trans_id = str(tx.get('transactionId') or tx.get('bizOrderNo') or '').strip()
                
                if order_id and order_id.isdigit():
                    tx_id = order_id
                elif trans_id:
                    tx_id = trans_id
                else:
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
    """ينظف الإيداعات المعلقة المنتهية ويفك قفل المستخدمين ويبلّغهم"""
    try:
        current_time = int(time.time())
        
        # نجيب الإيداعات المنتهية قبل ما نحذفها عشان نبلّغ أصحابها
        expired = list(db.pending_deposits.find({
            'expires_at': {'$lt': current_time},
            'status': 'pending'
        }))
        
        for dep in expired:
            uid = dep.get('user_id')
            if uid:
                # فك القفل
                unlock_deposit(uid)
                # إبلاغ المستخدم
                try:
                    l = get_lang(uid)
                    if l == 'ar':
                        bot.send_message(uid,
                            "⏰ <b>انتهت صلاحية طلب الإيداع!</b>\n\n"
                            f"💰 المبلغ: <b>${dep.get('base_amount_usd', 0):.2f}</b>\n\n"
                            "⚠️ لم يتم استلام التحويل خلال 30 دقيقة.\n"
                            "يمكنك إنشاء طلب إيداع جديد.",
                            parse_mode="HTML")
                    else:
                        bot.send_message(uid,
                            "⏰ <b>Deposit request expired!</b>\n\n"
                            f"💰 Amount: <b>${dep.get('base_amount_usd', 0):.2f}</b>\n\n"
                            "⚠️ No transfer received within 30 minutes.\n"
                            "You can create a new deposit request.",
                            parse_mode="HTML")
                except: pass
        
        # حذف المنتهية والمكتملة القديمة
        result = db.pending_deposits.delete_many({
            '$or': [
                {'expires_at': {'$lt': current_time}, 'status': 'pending'},
                {'status': 'completed', 'completed_at': {'$lt': current_time - 86400}}
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
    coin_emoji = {'USDT': '🟢', 'USDT_BEP20': '🟡', 'TON': '💎', 'LTC': '🔵'}.get(coin, '💰')
    
    msg_text = (
        f"{coin_emoji} <b>{coin_name}</b>\n\n"
        f"💰 <b>Amount:</b>\n<code>${unique_amount:.6f}</code>"
        f"{crypto_amount_text}\n\n"
        f"📬 <b>Address:</b>\n<code>{wallet}</code>\n\n"
        f"⏰ Valid: <b>30 minutes</b>\n"
        f"✨ <i>Balance added automatically</i>"
    )
    
    cancel_markup = InlineKeyboardMarkup(row_width=1)
    cancel_markup.add(InlineKeyboardButton(
        "❌ إلغاء" if l == 'ar' else "❌ Cancel",
        callback_data="cancel_deposit"
    ))
    bot.send_message(uid, msg_text, parse_mode="HTML", reply_markup=cancel_markup)
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
        msg_txt = f"🟢 <b>USDT (TRC-20)</b>\n\n💰 <b>Amount:</b>\n<code>${base_amount:.2f}</code>\n\n📬 <b>Address:</b>\n<code>{wallet}</code>"
    elif coin == "USDT_BEP20":
        msg_txt = f"🟡 <b>USDT (BEP-20)</b>\n\n💰 <b>Amount:</b>\n<code>${base_amount:.2f}</code>\n\n📬 <b>Address:</b>\n<code>{wallet}</code>\n\n⚠️ <b>Network: BEP-20 ONLY</b>"
    elif coin == "TON":
        msg_txt = f"💎 <b>Toncoin (TON)</b>\n\n💰 <b>Amount:</b>\n<code>${base_amount:.2f}</code>\n\n📬 <b>Address:</b>\n<code>{wallet}</code>"
    else:
        msg_txt = f"🔵 <b>Litecoin (LTC)</b>\n\n💰 <b>Amount:</b>\n<code>${base_amount:.2f}</code>\n\n📬 <b>Address:</b>\n<code>{wallet}</code>"
    
    msg_txt += "\n\n⚠️ <i>Send TxID after transfer</i>"
    
    dep_markup = InlineKeyboardMarkup(row_width=1)
    dep_markup.add(InlineKeyboardButton(
        "❌ Cancel" if l == 'en' else "❌ إلغاء",
        callback_data="cancel_deposit"
    ))
    
    msg = bot.send_message(uid, msg_txt, parse_mode="HTML", reply_markup=dep_markup)
    
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
        markup.add(InlineKeyboardButton("📦 Product Management", callback_data="ad_prod_manage"))
        markup.add(InlineKeyboardButton("👥 Users & Balances", callback_data="ad_users_main"),
                   InlineKeyboardButton("🚫 Ban / Unban User", callback_data="ad_ban_user"))
        markup.add(InlineKeyboardButton("👑 Promote Admin", callback_data="ad_new_admin"),
                   InlineKeyboardButton("💰 Gift Balance", callback_data="ad_gift"))
        markup.add(InlineKeyboardButton("📜 Records", callback_data="ad_logs_all"),
                   InlineKeyboardButton("📢 Broadcast", callback_data="ad_bc"))
        markup.add(InlineKeyboardButton("✏️ Customize Bot (CMS)", callback_data="ad_texts_main"))
        markup.add(InlineKeyboardButton("⚙️ Settings", callback_data="ad_shop_settings"),
                   InlineKeyboardButton("📢 Forced Sub", callback_data="ad_fsub_list"))
        markup.add(InlineKeyboardButton("🎓 API Settings", callback_data="ad_api_main"))
        markup.add(InlineKeyboardButton("🤖 ChatGPT Business", callback_data="ad_cgpt_panel"))
        markup.add(InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu_refresh"))
        text = "👑 <b>Admin Dashboard:</b>"
    else:
        markup.add(InlineKeyboardButton("📦 إدارة المنتجات", callback_data="ad_prod_manage"))
        markup.add(InlineKeyboardButton("👥 إدارة العملاء", callback_data="ad_users_main"),
                   InlineKeyboardButton("🚫 حظر / فك حظر", callback_data="ad_ban_user"))
        markup.add(InlineKeyboardButton("👑 ترقية مدير", callback_data="ad_new_admin"),
                   InlineKeyboardButton("💰 شحن رصيد", callback_data="ad_gift"))
        markup.add(InlineKeyboardButton("📜 السجلات", callback_data="ad_logs_all"),
                   InlineKeyboardButton("📢 برودكاست للأعضاء", callback_data="ad_bc"))
        markup.add(InlineKeyboardButton("✏️ تخصيص البوت والأزرار", callback_data="ad_texts_main"))
        markup.add(InlineKeyboardButton("⚙️ إعدادات المتجر", callback_data="ad_shop_settings"),
                   InlineKeyboardButton("📢 الاشتراك الإجباري", callback_data="ad_fsub_list"))
        markup.add(InlineKeyboardButton("🎓 إعدادات التفعيلات", callback_data="ad_api_main"))
        markup.add(InlineKeyboardButton("🤖 ChatGPT Business", callback_data="ad_cgpt_panel"))
        markup.add(InlineKeyboardButton("📊 تقارير المبيعات (CSV)", callback_data="ad_reports"))
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

# ═══ قائمة إدارة المنتجات الفرعية ═══
@bot.callback_query_handler(func=lambda call: call.data == "ad_prod_manage")
@admin_required
def ad_prod_manage(call):
    bot.answer_callback_query(call.id)
    l = get_lang(call.from_user.id)
    
    prods_count = db.products.count_documents({})
    cats_count = db.catalogs.count_documents({})
    
    markup = InlineKeyboardMarkup(row_width=2)
    if l == 'en':
        txt = f"📦 <b>Product Management</b>\n\n📊 Products: <b>{prods_count}</b> | Catalogs: <b>{cats_count}</b>"
        markup.add(InlineKeyboardButton("➕ Add Product", callback_data="ad_p_add"),
                   InlineKeyboardButton("📝 Edit Product", callback_data="ad_p_edit"))
        markup.add(InlineKeyboardButton("🗑 Delete Product", callback_data="ad_p_del"),
                   InlineKeyboardButton("📦 Manage Stock", callback_data="ad_s_list"))
        markup.add(InlineKeyboardButton("🌟 Set Product Icon", callback_data="ad_prod_emoji_start"))
        markup.add(InlineKeyboardButton("📂 Manage Catalogs", callback_data="ad_catalog_list"))
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_main"))
    else:
        txt = f"📦 <b>إدارة المنتجات</b>\n\n📊 المنتجات: <b>{prods_count}</b> | الكتالوجات: <b>{cats_count}</b>"
        markup.add(InlineKeyboardButton("➕ أضف منتج", callback_data="ad_p_add"),
                   InlineKeyboardButton("📝 تعديل منتج", callback_data="ad_p_edit"))
        markup.add(InlineKeyboardButton("🗑 حذف منتج", callback_data="ad_p_del"),
                   InlineKeyboardButton("📦 إدارة الستوك", callback_data="ad_s_list"))
        markup.add(InlineKeyboardButton("🌟 أيقونة منتج", callback_data="ad_prod_emoji_start"))
        markup.add(InlineKeyboardButton("📂 إدارة الكتالوجات", callback_data="ad_catalog_list"))
        markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data == "ad_reports")
@admin_required
def ad_reports_ui(call):
    bot.answer_callback_query(call.id)
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📦 تقرير الطلبات الكاملة (CSV)", callback_data="ad_csv_orders"))
    markup.add(InlineKeyboardButton("💳 تقرير الإيداعات (CSV)", callback_data="ad_csv_deposits"))
    markup.add(InlineKeyboardButton("📊 تقرير المبيعات لكل منتج (CSV)", callback_data="ad_csv_products"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    try:
        bot.edit_message_text("📊 <b>تقارير المبيعات</b>\n\nاختر نوع التقرير:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except:
        bot.send_message(call.message.chat.id, "📊 <b>تقارير المبيعات</b>\n\nاختر نوع التقرير:", reply_markup=markup, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "ad_csv_orders")
@admin_required
def ad_csv_orders(call):
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز الملف...")
    chat_id = call.message.chat.id
    orders = list(db.orders.find().sort('_id', -1))
    if not orders:
        bot.send_message(chat_id, "📭 لا توجد طلبات."); return

    lines = ["التاريخ,المستخدم_ID,اليوزر,المنتج,الكمية,السعر/قطعة,الإجمالي,حالة التسليم"]
    all_prods = {str(p.get('id', p.get('_id'))): p for p in db.products.find()}

    for r in orders:
        try:
            date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
            uid_r = r.get('user_id', '')
            u = db.users.find_one({'user_id': uid_r})
            uname = f"@{u['username']}" if u and u.get('username') else str(uid_r)
            pid = str(r.get('product_id', ''))
            qty = int(r.get('quantity', 1))
            p = all_prods.get(pid)
            p_name = p.get('name_ar', p.get('name_en', pid)) if p else pid
            price = float(p.get('price', 0)) if p else 0
            total = round(price * qty, 2)
            delivered = "✅" if r.get('code_delivered') else "⏳"
            lines.append(f"{date_str},{uid_r},{uname},{p_name},{qty},{price:.2f},{total:.2f},{delivered}")
        except: pass

    content = "\n".join(lines)
    f = io.BytesIO(("\ufeff" + content).encode('utf-8-sig'))
    f.name = f"orders_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
    bot.send_document(chat_id, f, caption=f"📦 <b>تقرير الطلبات</b>\nإجمالي: <b>{len(orders)}</b> طلب", parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "ad_csv_deposits")
@admin_required
def ad_csv_deposits(call):
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز الملف...")
    chat_id = call.message.chat.id
    deps = list(db.used_transactions.find().sort('_id', -1))
    if not deps:
        bot.send_message(chat_id, "📭 لا توجد إيداعات."); return

    lines = ["التاريخ,المستخدم_ID,اليوزر,المبلغ,الطريقة,رقم_العملية"]
    for r in deps:
        try:
            date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
            uid_r = r.get('user_id', '')
            u = db.users.find_one({'user_id': uid_r})
            uname = f"@{u['username']}" if u and u.get('username') else str(uid_r)
            amount = float(r.get('amount', 0))
            method = r.get('method', '-')
            tx = r.get('transaction_id', '-')
            lines.append(f"{date_str},{uid_r},{uname},{amount:.2f},{method},{tx}")
        except: pass

    content = "\n".join(lines)
    f = io.BytesIO(("\ufeff" + content).encode('utf-8-sig'))
    f.name = f"deposits_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
    total = sum(float(d.get('amount', 0)) for d in deps)
    bot.send_document(chat_id, f, caption=f"💳 <b>تقرير الإيداعات</b>\nإجمالي: <b>${total:.2f}</b> من <b>{len(deps)}</b> عملية", parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "ad_csv_products")
@admin_required
def ad_csv_products(call):
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز الملف...")
    chat_id = call.message.chat.id
    products = list(db.products.find())
    if not products:
        bot.send_message(chat_id, "📭 لا توجد منتجات."); return

    lines = ["المنتج,الكمية_المباعة,إجمالي_الإيرادات,السعر_الحالي,المتوفر_الآن"]
    for p in products:
        try:
            pid = str(p.get('id', p.get('_id', '')))
            p_name = p.get('name_ar', p.get('name_en', pid))
            price = float(p.get('price', 0))
            sold_count = db.orders.count_documents({'product_id': pid})
            revenue = round(sold_count * price, 2)
            stock = db.stock.count_documents({'product_id': pid, 'is_sold': {'$ne': True}}) if 'stock' in db.list_collection_names() else 0
            lines.append(f"{p_name},{sold_count},{revenue:.2f},{price:.2f},{stock}")
        except: pass

    content = "\n".join(lines)
    f = io.BytesIO(("\ufeff" + content).encode('utf-8-sig'))
    f.name = f"products_report_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
    bot.send_document(chat_id, f, caption=f"📊 <b>تقرير المنتجات</b>\nإجمالي: <b>{len(products)}</b> منتج", parse_mode="HTML")


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
    # 🆕 Wallet وإيداع
    markup.add(InlineKeyboardButton("👛 رسالة الـ Wallet (عنوان + رصيد)", callback_data="edit_txt_wallet_header"))
    markup.add(InlineKeyboardButton("✅ رسالة إيداع تلقائي (تم استلام)", callback_data="edit_txt_auto_deposit_msg"))
    markup.add(InlineKeyboardButton("❌ رسالة مبلغ خاطئ (رفض)", callback_data="edit_txt_wrong_amount_msg"))
    # 🆕 شراء
    markup.add(InlineKeyboardButton("💰 رسالة رصيد غير كافٍ", callback_data="edit_txt_no_balance"))
    markup.add(InlineKeyboardButton("⏳ رسالة جاري المعالجة", callback_data="edit_txt_processing_msg"))
    markup.add(InlineKeyboardButton("✅ رسالة شراء ناجح", callback_data="edit_txt_buy_success"))
    # 🆕 إشعارات اللوق لنظام الإحالات
    markup.add(InlineKeyboardButton("🎁 لوق: مكافأة شراء إحالة", callback_data="edit_txt_log_ref_purchase"))
    markup.add(InlineKeyboardButton("🏆 لوق: إنجاز 10 إحالات", callback_data="edit_txt_log_ref_milestone"))
    markup.add(InlineKeyboardButton("💌 رسالة المُحيل (شراء صديقه)", callback_data="edit_txt_ref_purchase_dm"))
    markup.add(InlineKeyboardButton("📈 رسالة التقدم (باقي X للمكافأة)", callback_data="edit_txt_ref_progress_dm"))
    markup.add(InlineKeyboardButton("📊 لوق: إحالة جديدة (قناة اللوق)", callback_data="edit_txt_log_ref_progress"))
    markup.add(InlineKeyboardButton("🎉 رسالة المُحيل (وصل للمكافأة)", callback_data="edit_txt_ref_milestone_dm"))
    # 🆕 إشعارات اللوق العامة
    markup.add(InlineKeyboardButton("🛒 لوق: شراء بنجاح", callback_data="edit_txt_log_purchase"))
    markup.add(InlineKeyboardButton("💳 لوق: إيداع بنجاح", callback_data="edit_txt_log_deposit"))
    markup.add(InlineKeyboardButton("✨ لوق: تفعيل Gemini", callback_data="edit_txt_log_gemini"))
    markup.add(InlineKeyboardButton("🎓 لوق: تفعيل GitHub", callback_data="edit_txt_log_github"))
    markup.add(InlineKeyboardButton("📜 محتوى شروط الاستخدام", callback_data="edit_txt_terms_content"))
    # 🔗 API
    markup.add(InlineKeyboardButton("🤖 رسالة API (بدون مفتاح)", callback_data="edit_txt_api_welcome"))
    markup.add(InlineKeyboardButton("✅ رسالة إنشاء API ناجح", callback_data="edit_txt_api_created"))
    markup.add(InlineKeyboardButton("📖 رسالة كيف تتصل (API)", callback_data="edit_txt_api_howto"))
    markup.add(InlineKeyboardButton("🤖 لوق: Auto Buy API", callback_data="edit_txt_api_log"))
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
        'wallet_header': (
            "💡 <b>رسالة الـ Wallet - المتغيرات:</b>\n"
            "لا متغيرات ثابتة - الرصيد والاسم يُضافان تلقائياً من الكود\n\n"
            "✏️ <i>يمكنك تغيير العنوان والتنسيق فقط</i>"
        ),
        'auto_deposit_msg': (
            "💡 <b>رسالة الإيداع التلقائي - المتغيرات:</b>\n"
            "<code>{0}</code> = المبلغ (مثل 5.00)\n"
            "<code>{1}</code> = طريقة الدفع (Binance Pay / LTC ...)\n"
            "<code>{2}</code> = رقم العملية (TxID)\n\n"
            "💡 <i>تُرسل للمستخدم بعد كشف الإيداع تلقائياً</i>"
        ),
        'wrong_amount_msg': (
            "💡 <b>رسالة رفض المبلغ الخاطئ - بدون متغيرات</b>\n\n"
            "💡 <i>تُرسل لما يحوّل مبلغ مختلف عن المطلوب</i>"
        ),
        'no_balance': (
            "💡 <b>رسالة رصيد غير كافٍ - بدون متغيرات</b>\n\n"
            "💡 <i>تُرسل لما يحاول المستخدم الشراء ورصيده غير كافٍ</i>"
        ),
        'processing_msg': (
            "💡 <b>رسالة جاري المعالجة - بدون متغيرات</b>\n\n"
            "💡 <i>تظهر لما يضغط المستخدم تأكيد الشراء</i>"
        ),
        'buy_success': (
            "💡 <b>رسالة الشراء الناجح - المتغيرات:</b>\n"
            "<code>{}</code> 1 = اسم المنتج\n"
            "<code>{}</code> 2 = الكمية\n"
            "<code>{}</code> 3 = المبلغ الكلي\n\n"
            "💡 <i>تُرسل للمستخدم بعد اكتمال الشراء</i>"
        ),
        'ref_progress_dm': (
            "💡 <b>رسالة التقدم للمُحيل - المتغيرات:</b>\n"
            "<code>{0}</code> = عدد الإحالات النشطة (مثل 88)\n"
            "<code>{1}</code> = عدد الباقي للمكافأة (مثل 2)\n"
            "<code>{2}</code> = قيمة المكافأة (مثل 0.30)\n\n"
            "💡 <i>تُرسل للمُحيل كل ما انضم شخص عبر رابطه</i>"
        ),
        'log_ref_progress': (
            "💡 <b>رسالة لوق الإحالة الجديدة - المتغيرات:</b>\n"
            "<code>{0}</code> = عدد الإحالات النشطة\n"
            "<code>{1}</code> = عدد الباقي للمكافأة\n"
            "<code>{2}</code> = قيمة المكافأة\n\n"
            "💡 <i>تُرسل في قناة اللوق كل ما انضم شخص</i>"
        ),
        'welcome': (
            "💡 <b>المتغيرات في هذا النص (بالترتيب):</b>\n"
            "<code>{}</code> 1 = معرف المستخدم\n"
            "<code>{}</code> 2 = اسم المستخدم\n"
            "<code>{}</code> 3 = عدد مستخدمي البوت\n"
            "<code>{}</code> 4 = رصيد المستخدم"
        ),
        'api_welcome': (
            "💡 <b>رسالة API لما ما عنده مفتاح</b>\n\n"
            "بدون متغيرات — اكتب بحرية\n\n"
            "💡 <i>تظهر لما يفتح صفحة API وما عنده مفتاح بعد</i>"
        ),
        'api_created': (
            "💡 <b>رسالة إنشاء API بنجاح — متغير واحد:</b>\n"
            "<code>{}</code> 1 = كود الاتصال المشفّر\n\n"
            "💡 <i>تظهر بعد ما يولّد المفتاح</i>"
        ),
        'api_howto': (
            "💡 <b>رسالة كيف تتصل</b>\n\n"
            "بدون متغيرات — اكتب خطوات بحرية\n\n"
            "💡 <i>تظهر لما يضغط 'How to Connect'</i>"
        ),
        'api_log': (
            "💡 <b>رسالة لوق Auto Buy API — متغير واحد:</b>\n"
            "<code>{}</code> 1 = رسالة الشراء الأصلية\n\n"
            "💡 <i>تظهر في قناة اللوق لما يشتري أحد عبر API</i>"
        ),
    }
    
    info_section = placeholders_info.get(key, "")
    info_block = f"\n\n{info_section}\n\n<i>⚠️ تأكد من نسخ كل المتغيرات {{}}  بالعدد الصحيح والترتيب الصحيح!</i>" if info_section else ""

    # 🛡 نعرض النص الحالي كنص بمعالجة شاملة لـ HTML
    # المشكلة: لو النص يحتوي على <u> أو tag غير متوازن، Telegram يرفضه
    # الحل: نرسله كرسالة plain text (بدون parse_mode أصلاً)
    # 1) المعاينة بالتنسيق
    try:
        bot.send_message(
            call.message.chat.id,
            f"👁 <b>Preview:</b>\n\n{current_text}",
            parse_mode="HTML"
        )
    except:
        pass
    
    # 2) النص الخام للنسخ
    try:
        bot.send_message(
            call.message.chat.id, 
            f"📋 انسخ وعدّل:\n\n{current_text}"
        )
    except Exception as send_err:
        logger.error(f"Failed to send current text: {send_err}")
        bot.send_message(call.message.chat.id, "📝 النص الحالي غير قابل للعرض.")
    
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

# ================================================================
# 🤖 لوحة ChatGPT Business — مستقلة كاملة
# ================================================================

@bot.callback_query_handler(func=lambda call: call.data == "ad_cgpt_panel")
@admin_required
def ad_cgpt_panel(call):
    bot.answer_callback_query(call.id)
    mgr = get_cgpt_manager()
    stats = mgr.get_stats()
    loaded_icon = "\u2705" if mgr._loaded else "\u274c"
    products_count = db.cgpt_products.count_documents({})
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("\U0001f465 \u0627\u0644\u0639\u0645\u0644\u0627\u0621", callback_data="cgpt_customers"),
        InlineKeyboardButton(f"\U0001f4e6 \u0627\u0644\u0645\u0646\u062a\u062c\u0627\u062a ({products_count})", callback_data="cgpt_products_list"),
        InlineKeyboardButton("\u2795 \u0625\u0636\u0627\u0641\u0629 \u0645\u0646\u062a\u062c \u062c\u062f\u064a\u062f", callback_data="cgpt_add_product"),
        InlineKeyboardButton("\U0001f36a \u0625\u0636\u0627\u0641\u0629 / \u062a\u062d\u062f\u064a\u062b \u0627\u0644\u0643\u0648\u0643\u064a\u0632", callback_data="cgpt_set_cookies"),
        InlineKeyboardButton("\U0001f504 \u0641\u062d\u0635 \u0648\u062a\u0646\u0638\u064a\u0641 \u0627\u0644\u0622\u0646", callback_data="ad_cgpt_cleanup"),
        InlineKeyboardButton("\U0001f519 \u0631\u062c\u0648\u0639", callback_data="admin_panel")
    )
    txt = (
        "\U0001f916 <b>ChatGPT Business</b>\n\n"
        f"\U0001f36a \u0627\u0644\u0643\u0648\u0643\u064a\u0632: {loaded_icon}\n"
        f"\U0001f4e6 \u0627\u0644\u0645\u0646\u062a\u062c\u0627\u062a: <b>{products_count}</b>\n"
        f"\U0001f465 \u0646\u0634\u0637: <b>{stats['active']}</b> | \u0645\u0646\u062a\u0647\u064a: <b>{stats['expired']}</b>"
    )
    try:
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
    except:
        bot.send_message(call.message.chat.id, txt, parse_mode="HTML", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data == "cgpt_customers")
@admin_required
def cgpt_customers(call):
    bot.answer_callback_query(call.id)
    mgr = get_cgpt_manager()
    stats = mgr.get_stats()
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("\U0001f7e2 \u0627\u0644\u0646\u0634\u0637\u0648\u0646", callback_data="cgpt_cust_active"),
        InlineKeyboardButton("\U0001f534 \u0627\u0644\u0645\u0646\u062a\u0647\u0648\u0646", callback_data="cgpt_cust_expired"),
        InlineKeyboardButton("\u26d4\ufe0f \u0627\u0644\u0645\u062e\u0627\u0644\u0641\u0648\u0646", callback_data="cgpt_cust_violated"),
        InlineKeyboardButton("\U0001f519 \u0631\u062c\u0648\u0639", callback_data="ad_cgpt_panel")
    )
    txt = (
        "\U0001f465 <b>\u0627\u0644\u0639\u0645\u0644\u0627\u0621</b>\n\n"
        f"\U0001f7e2 \u0646\u0634\u0637: <b>{stats['active']}</b>\n"
        f"\U0001f534 \u0645\u0646\u062a\u0647\u064a: <b>{stats['expired']}</b>"
    )
    try:
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
    except:
        bot.send_message(call.message.chat.id, txt, parse_mode="HTML", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data in ["cgpt_cust_active", "cgpt_cust_expired", "cgpt_cust_violated"])
@admin_required
def cgpt_cust_view(call):
    bot.answer_callback_query(call.id)
    mgr = get_cgpt_manager()
    mode = call.data.replace("cgpt_cust_", "")
    invites = mgr.invites_data.get('invites', {})
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("\U0001f519 \u0631\u062c\u0648\u0639", callback_data="cgpt_customers"))
    titles = {"active": "\U0001f7e2 \u0627\u0644\u0646\u0634\u0637\u0648\u0646", "expired": "\U0001f534 \u0627\u0644\u0645\u0646\u062a\u0647\u0648\u0646", "violated": "\u26d4\ufe0f \u0627\u0644\u0645\u062e\u0627\u0644\u0641\u0648\u0646"}
    status_map = {"active": "active", "expired": "expired", "violated": "violated"}
    title = titles.get(mode, "")
    items = [(e, i) for e, i in invites.items() if i.get('status') == status_map[mode]]
    if not items:
        txt = title + "\n\n\u0644\u0627 \u064a\u0648\u062c\u062f."
    else:
        txt = title + "\n\n"
        for idx, (email, info) in enumerate(items[:20], 1):
            exp = info.get('expires_at', '')[:10]
            uid_tg = info.get('telegram_uid', '')
            txt += f"{idx}. <code>{email}</code>\n"
            if uid_tg:
                try:
                    u_data = db.users.find_one({'user_id': int(uid_tg)}, {'username': 1, 'name': 1})
                    uname = u_data.get('username') if u_data else None
                    uname_str = f"@{uname} | " if uname else ""
                    udisp = f"   \U0001f464 {uname_str}<code>{uid_tg}</code>\n"
                except:
                    udisp = f"   \U0001f464 <code>{uid_tg}</code>\n"
                txt += udisp
            if exp:
                try:
                    exp_dt = _dt_mod.datetime.fromisoformat(info.get('expires_at', ''))
                    rem = exp_dt - _dt_mod.datetime.now()
                    d = max(0, rem.days)
                    h = max(0, int(rem.total_seconds() // 3600) % 24)
                    txt += f"   \U0001f4c5 {exp} | \u0645\u062a\u0628\u0642\u064a: {d}\u064a {h}\u0633\n"
                except:
                    txt += f"   \U0001f4c5 {exp}\n"
            txt += "\n"
    try:
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
    except:
        bot.send_message(call.message.chat.id, txt, parse_mode="HTML", reply_markup=markup)


def _cgpt_show_products_list(chat_id, msg_id=None):
    products = list(db.cgpt_products.find())
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("\U0001f519 \u0631\u062c\u0648\u0639", callback_data="ad_cgpt_panel"))
    if not products:
        txt = "\U0001f4e6 <b>\u0644\u0627 \u064a\u0648\u062c\u062f \u0645\u0646\u062a\u062c\u0627\u062a \u0628\u0639\u062f.</b>"
    else:
        txt = "\U0001f4e6 <b>\u0645\u0646\u062a\u062c\u0627\u062a ChatGPT Business:</b>\n\n"
        for p in products:
            pid = str(p['_id'])
            name = p.get('name', '')
            desc = p.get('desc', '')
            durations = p.get('durations', [])
            txt += f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\U0001f4cc <b>{name}</b>\n"
            if desc:
                txt += f"\U0001f4dd {desc[:60]}{'...' if len(desc) > 60 else ''}\n"
            if durations:
                txt += "\U0001f5d3 \u0627\u0644\u0645\u062f\u062f:\n"
                for d in durations:
                    txt += f"  \u2022 {d['label']} \u2014 <b>${d['price']:.2f}</b>\n"
            markup.add(InlineKeyboardButton(f"\u2795 \u0645\u062f\u0629 \u0644\u0640 {name[:20]}", callback_data=f"cgpt_add_dur_{pid}"))
            markup.add(InlineKeyboardButton(f"\U0001f5d1 \u062d\u0630\u0641 {name[:20]}", callback_data=f"cgpt_del_prod_{pid}"))
    if msg_id:
        try:
            bot.edit_message_text(txt, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
            return
        except:
            pass
    bot.send_message(chat_id, txt, parse_mode="HTML", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data == "cgpt_products_list")
@admin_required
def cgpt_products_list(call):
    bot.answer_callback_query(call.id)
    _cgpt_show_products_list(call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda call: call.data == "cgpt_add_product")
@admin_required
def cgpt_add_product(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.from_user.id, "\U0001f4dd <b>\u0623\u0631\u0633\u0644 \u0627\u0633\u0645 \u0627\u0644\u0645\u0646\u062a\u062c:</b>\n<i>\u0645\u062b\u0627\u0644: ChatGPT Business</i>", parse_mode="HTML")
    bot.register_next_step_handler(msg, cgpt_add_product_name)

def cgpt_add_product_name(message):
    raw = (message.text or "").strip()
    if not raw:
        bot.send_message(message.chat.id, "\u274c \u0623\u0631\u0633\u0644 \u0627\u0633\u0645\u0627\u064b \u0635\u062d\u064a\u062d\u0627\u064b."); return
    
    # 🆕 التقاط الإيموجي المميز إن وجد في الاسم
    emoji_id = None
    if message.entities:
        for ent in message.entities:
            if ent.type == 'custom_emoji':
                emoji_id = ent.custom_emoji_id
                break
                
    # نستخرج الإيموجي المميز
    name_ar = extract_custom_emojis_to_html(message)
    # نترجم للإنجليزي مع حفظ كل التنسيقات والإيموجي
    name_en = safe_translate_for_cms(name_ar, 'en')
    try:
        preview = (
            f"\u2705 <b>\u062a\u0645 \u062d\u0641\u0638 \u0627\u0644\u0627\u0633\u0645!</b>\n\n"
            f"\U0001f1f8\U0001f1e6 {name_ar}\n"
            f"\U0001f1ec\U0001f1e7 {name_en}"
        )
        bot.send_message(message.chat.id, preview, parse_mode="HTML")
    except:
        bot.send_message(message.chat.id, "\u2705 \u062a\u0645 \u062d\u0641\u0638 \u0627\u0644\u0627\u0633\u0645!")
    msg = bot.send_message(message.chat.id,
        "\U0001f4c4 <b>\u0623\u0631\u0633\u0644 \u0648\u0635\u0641 \u0627\u0644\u0645\u0646\u062a\u062c:</b>\n"
        "<i>\u064a\u062f\u0639\u0645 Premium Emojis \u0648\u0627\u0644\u062a\u0646\u0633\u064a\u0642\u0627\u062a</i>",
        parse_mode="HTML")
    bot.register_next_step_handler(msg, cgpt_add_product_desc, name_ar, name_en, emoji_id)

def cgpt_add_product_desc(message, name_ar, name_en, emoji_id):
    raw = (message.text or "").strip()
    desc_ar = extract_custom_emojis_to_html(message) if raw else ''
    desc_en = safe_translate_for_cms(desc_ar, 'en') if desc_ar else ''
    result = db.cgpt_products.insert_one({
        'name': name_ar,
        'name_en': name_en,
        'desc': desc_ar,
        'desc_en': desc_en,
        'durations': [],
        'custom_emoji_id': emoji_id,
        'created_at': _dt_mod.datetime.now().isoformat()
    })
    pid = str(result.inserted_id)
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("\u2795 \u0625\u0636\u0627\u0641\u0629 \u0645\u062f\u0629 \u0648\u0633\u0639\u0631", callback_data=f"cgpt_add_dur_{pid}"),
        InlineKeyboardButton("\U0001f4e6 \u0639\u0631\u0636 \u0643\u0644 \u0627\u0644\u0645\u0646\u062a\u062c\u0627\u062a", callback_data="cgpt_products_list"),
        InlineKeyboardButton("\U0001f519 \u0631\u062c\u0648\u0639 \u0644\u0644\u0648\u062d\u0629", callback_data="ad_cgpt_panel")
    )
    try:
        bot.send_message(message.chat.id,
            f"\u2705 <b>\u062a\u0645 \u062d\u0641\u0638 \u0627\u0644\u0645\u0646\u062a\u062c!</b>\n\n"
            f"\U0001f4cc <b>{name_ar}</b>\n"
            f"\U0001f1ec\U0001f1e7 {name_en}\n\n"
            f"\U0001f4dd <b>\u0627\u0644\u0648\u0635\u0641 \u0639\u0631\u0628\u064a:</b>\n{desc_ar or '\u0628\u062f\u0648\u0646'}\n\n"
            f"\U0001f4dd <b>\u0627\u0644\u0648\u0635\u0641 \u0625\u0646\u062c\u0644\u064a\u0632\u064a:</b>\n{desc_en or '\u0628\u062f\u0648\u0646'}\n\n"
            f"\u0627\u0644\u0622\u0646 \u0623\u0636\u0641 \u0627\u0644\u0645\u062f\u062f \u0648\u0627\u0644\u0623\u0633\u0639\u0627\u0631:",
            parse_mode="HTML", reply_markup=markup)
    except:
        bot.send_message(message.chat.id, "\u2705 \u062a\u0645 \u062d\u0641\u0638 \u0627\u0644\u0645\u0646\u062a\u062c!", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("cgpt_add_dur_"))
@admin_required
def cgpt_add_duration(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("cgpt_add_dur_", "")
    msg = bot.send_message(call.from_user.id,
        "\u23f1 <b>\u0623\u0631\u0633\u0644 \u0627\u0644\u0645\u062f\u0629 \u0648\u0627\u0644\u0633\u0639\u0631 \u0628\u0627\u0644\u0635\u064a\u063a\u0629:</b>\n<code>\u0627\u0644\u0645\u062f\u0629_\u0627\u0644\u0633\u0639\u0631</code>\n\n\u0623\u0645\u062b\u0644\u0629:\n\u2022 <code>7 \u0623\u064a\u0627\u0645_5</code>\n\u2022 <code>15 \u064a\u0648\u0645_8</code>\n\u2022 <code>25 \u064a\u0648\u0645_12</code>",
        parse_mode="HTML")
    bot.register_next_step_handler(msg, cgpt_save_duration, pid)

def cgpt_save_duration(message, pid):
    try:
        text = (message.text or "").strip()
        if "_" not in text:
            raise ValueError("no underscore")
        parts = text.rsplit("_", 1)
        label = parts[0].strip()
        price = float(parts[1].strip())
        if price <= 0:
            raise ValueError("price must be positive")
        days_map = {'7': 10080, '15': 21600, '25': 36000, '30': 43200, '60': 86400, '90': 129600}
        minutes = 10080
        for d, m in days_map.items():
            if d in label:
                minutes = m; break
        from bson import ObjectId
        dur_id = str(int(time.time()))
        db.cgpt_products.update_one(
            {'_id': ObjectId(pid)},
            {'$push': {'durations': {'label': label, 'price': price, 'minutes': minutes, 'dur_id': dur_id}}}
        )
        p = db.cgpt_products.find_one({'_id': ObjectId(pid)})
        p_name_ar = p.get('name', '') if p else ''
        p_name_en = p.get('name_en') if p else p_name_ar
        p_desc_ar = p.get('desc', '') if p else ''
        p_desc_en = p.get('desc_en') if p else p_desc_ar
        p_emoji_id = p.get('custom_emoji_id') if p else None

        # نضمن وجود منتج رئيسي واحد في db.products (الزر الأزرق في المجلد)
        main_pid = f"cgpt_main_{pid}"
        db.products.update_one(
            {'_id': main_pid},
            {'$set': {
                '_id':           main_pid,
                'name_ar':       p_name_ar,
                'name_en':       p_name_en or p_name_ar,
                'desc_ar':       p_desc_ar,
                'desc_en':       p_desc_en or p_desc_ar,
                'price':         0,
                'is_manual':     False,
                'product_type':  'cgpt_main',
                'cgpt_product_id': pid,
                'is_hidden':     False,
                'btn_style':     'primary',
                'cgpt_pinned':   True,
                'custom_emoji_id': p_emoji_id,
            }},
            upsert=True
        )

        # نعرض المجلدات مباشرة للاختيار
        cats = list(db.catalogs.find().sort('order', 1))
        markup = InlineKeyboardMarkup(row_width=1)
        for cat in cats:
            cat_id = str(cat['_id'])
            cat_name = cat.get('name_ar', cat.get('name', ''))
            markup.add(InlineKeyboardButton(
                f"\U0001f4c1 {cat_name}",
                callback_data=f"cgpt_setcat_{pid}_{cat_id}"
            ))
        markup.add(
            InlineKeyboardButton("\U0001f6ab \u0628\u062f\u0648\u0646 \u0645\u062c\u0644\u062f", callback_data=f"cgpt_setcat_{pid}_none"),
            InlineKeyboardButton("\u2795 \u0625\u0636\u0627\u0641\u0629 \u0645\u062f\u0629 \u0623\u062e\u0631\u0649", callback_data=f"cgpt_add_dur_{pid}"),
            InlineKeyboardButton("\u2699\ufe0f \u0625\u062f\u0627\u0631\u0629 \u0627\u0644\u0645\u0646\u062a\u062c", callback_data=f"edit_p_{main_pid}"),
            InlineKeyboardButton("\U0001f519 \u0631\u062c\u0648\u0639 \u0644\u0644\u0648\u062d\u0629", callback_data="ad_cgpt_panel")
        )
        bot.send_message(message.chat.id,
            f"\u2705 <b>\u062a\u0645\u062a \u0625\u0636\u0627\u0641\u0629 \u0627\u0644\u0645\u062f\u0629!</b>\n\n"
            f"\U0001f5d3 <b>{label}</b> | \U0001f4b0 <b>${price:.2f}</b>\n\n"
            f"\U0001f4c1 <b>\u0627\u062e\u062a\u0631 \u0627\u0644\u0645\u062c\u0644\u062f \u0627\u0644\u0630\u064a \u064a\u0638\u0647\u0631 \u0641\u064a\u0647 \u0627\u0644\u0645\u0646\u062a\u062c:</b>",
            parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        bot.send_message(message.chat.id, f"\u274c \u0635\u064a\u063a\u0629 \u062e\u0627\u0637\u0626\u0629. \u0627\u0633\u062a\u062e\u062f\u0645: <code>7 \u0623\u064a\u0627\u0645_5</code>\n<i>{e}</i>", parse_mode="HTML")


# قاموس مؤقت لبيانات المدة قيد الانتظار
_cgpt_dur_pending = {}

@bot.callback_query_handler(func=lambda call: call.data.startswith("cgpt_dur_cat_"))
@admin_required
def cgpt_dur_cat_selected(call):
    """يحفظ المنتج في المجلد المختار — أخضر وفي أول الترتيب"""
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    pending = _cgpt_dur_pending.pop(uid, None)
    if not pending:
        bot.answer_callback_query(call.id, "\u274c \u0627\u0646\u062a\u0647\u062a \u0627\u0644\u062c\u0644\u0633\u0629.", show_alert=True)
        return

    cat_raw = call.data.replace("cgpt_dur_cat_", "")
    cat_id = None if cat_raw == "none" else cat_raw

    pid       = pending['pid']
    label     = pending['label']
    price     = pending['price']
    minutes   = pending['minutes']
    dur_id    = pending['dur_id']
    p_name    = pending['p_name']
    p_desc    = pending['p_desc']

    shop_product_id      = f"cgpt_{pid}_{dur_id}"
    shop_product_name_ar = f"{p_name} - {label}"
    shop_product_name_en = f"{p_name} - {label}"

    # نجد أقل order موجود في المجلد عشان يكون أول شيء
    if cat_id:
        first_order_doc = db.products.find_one(
            {'catalog_id': cat_id},
            sort=[('order', 1)]
        )
        new_order = (first_order_doc.get('order', 0) - 1) if first_order_doc else 0
    else:
        new_order = 0

    p_emoji_id = None
    try:
        from bson import ObjectId
        parent_cgpt = db.cgpt_products.find_one({'_id': ObjectId(pid)})
        if parent_cgpt:
            p_emoji_id = parent_cgpt.get('custom_emoji_id')
    except: pass

    db.products.update_one(
        {'_id': shop_product_id},
        {'$set': {
            '_id':             shop_product_id,
            'name_ar':         shop_product_name_ar,
            'name_en':         shop_product_name_en,
            'desc_ar':         p_desc,
            'desc_en':         p_desc,
            'price':           price,
            'is_manual':       False,
            'product_type':    'chatgpt_seat',
            'cgpt_minutes':    minutes,
            'cgpt_product_id': pid,
            'cgpt_dur_id':     dur_id,
            'is_hidden':       False,
            'catalog_id':      cat_id,
            'order':           new_order,
            'btn_style':       'success',   # أخضر
            'custom_emoji_id': p_emoji_id,
        }},
        upsert=True
    )

    # لو في مجلد نضيفه لقائمة product_ids
    if cat_id:
        db.catalogs.update_one(
            {'_id': __import__('bson').ObjectId(cat_id)},
            {'$addToSet': {'product_ids': shop_product_id}}
        )

    cat_name = ""
    if cat_id:
        cat_doc = db.catalogs.find_one({'_id': __import__('bson').ObjectId(cat_id)})
        cat_name = cat_doc.get('name_ar', '') if cat_doc else cat_id

    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("\u2795 \u0625\u0636\u0627\u0641\u0629 \u0645\u062f\u0629 \u0623\u062e\u0631\u0649", callback_data=f"cgpt_add_dur_{pid}"),
        InlineKeyboardButton("\U0001f4e6 \u0639\u0631\u0636 \u0627\u0644\u0645\u0646\u062a\u062c\u0627\u062a", callback_data="cgpt_products_list"),
        InlineKeyboardButton("\U0001f519 \u0631\u062c\u0648\u0639 \u0644\u0644\u0648\u062d\u0629", callback_data="ad_cgpt_panel")
    )
    bot.edit_message_text(
        f"\u2705 <b>\u062a\u0645 \u0646\u0634\u0631 \u0627\u0644\u0645\u0646\u062a\u062c \u0628\u0646\u062c\u0627\u062d!</b>\n\n"
        f"\U0001f4cc <b>{shop_product_name_ar}</b>\n"
        f"\U0001f5d3 <b>{label}</b> | \U0001f4b0 <b>${price:.2f}</b>\n"
        f"\U0001f4c1 \u0627\u0644\u0645\u062c\u0644\u062f: <b>{cat_name or '\u0628\u062f\u0648\u0646 \u0645\u062c\u0644\u062f'}</b>\n"
        f"\U0001f7e2 \u0627\u0644\u0632\u0631: \u0623\u062e\u0636\u0631 | \U0001f3c6 \u0623\u0648\u0644 \u0627\u0644\u0642\u0627\u0626\u0645\u0629",
        call.message.chat.id, call.message.message_id,
        parse_mode="HTML", reply_markup=markup
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("cgpt_setcat_"))
@admin_required
def cgpt_setcat(call):
    """يضيف المنتج الرئيسي للمجلد المختار في الأول"""
    bot.answer_callback_query(call.id)
    raw = call.data.replace("cgpt_setcat_", "")
    parts = raw.rsplit("_", 1)
    pid = parts[0]
    cat_id = parts[1] if len(parts) > 1 else "none"
    main_pid = f"cgpt_main_{pid}"

    # نزيله من أي مجلد قديم
    for c in db.catalogs.find():
        if main_pid in (c.get('product_ids') or []):
            db.catalogs.update_one(
                {'_id': c['_id']},
                {'$pull': {'product_ids': main_pid}}
            )

    if cat_id == "none":
        # بدون مجلد
        db.products.update_one({'_id': main_pid}, {'$set': {'catalog_id': None}})
        bot.send_message(call.from_user.id,
            "✅ <b>تم تعيين المنتج بدون مجلد.</b>",
            parse_mode="HTML")
        return

    from bson import ObjectId as _ObjId3
    # نضيفه في أول القائمة في المجلد
    try:
        db.products.update_one({'_id': main_pid}, {'$set': {'catalog_id': str(cat_id)}})
        db.catalogs.update_one(
            {'_id': _ObjId3(cat_id)},
            {'$push': {'product_ids': {'$each': [main_pid], '$position': 0}}}
        )
        cat = db.catalogs.find_one({'_id': _ObjId3(cat_id)})
        cat_name = cat.get('name_ar', cat.get('name', '')) if cat else cat_id
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("⚙️ إدارة المنتج", callback_data=f"edit_p_{main_pid}"),
            InlineKeyboardButton("🔙 رجوع للوحة", callback_data="ad_cgpt_panel")
        )
        bot.send_message(call.from_user.id,
            f"✅ <b>تم وضع المنتج في المجلد:</b>\n📁 <b>{cat_name}</b> (أول القائمة)",
            parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        bot.send_message(call.from_user.id, f"\u274c \u062e\u0637\u0623: {e}")


@bot.callback_query_handler(func=lambda call: call.data.startswith("cgpt_del_prod_"))
@admin_required
def cgpt_del_product(call):
    bot.answer_callback_query(call.id)
    pid = call.data.replace("cgpt_del_prod_", "")
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("\u2705 \u0646\u0639\u0645\u060c \u0627\u062d\u0630\u0641", callback_data=f"cgpt_del_confirm_{pid}"),
        InlineKeyboardButton("\u274c \u0625\u0644\u063a\u0627\u0621", callback_data="cgpt_products_list")
    )
    try:
        bot.edit_message_text("\U0001f5d1 <b>\u0647\u0644 \u062a\u0631\u064a\u062f \u062d\u0630\u0641 \u0647\u0630\u0627 \u0627\u0644\u0645\u0646\u062a\u062c \u0648\u0643\u0644 \u0645\u062f\u062f\u0647\u061f</b>",
            call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
    except:
        bot.send_message(call.message.chat.id, "\U0001f5d1 \u062a\u0623\u0643\u064a\u062f \u0627\u0644\u062d\u0630\u0641\u061f", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("cgpt_del_confirm_"))
@admin_required
def cgpt_del_confirm(call):
    pid = call.data.replace("cgpt_del_confirm_", "")
    from bson import ObjectId
    from bson import ObjectId as _ObjId2
    # نحذف المنتج الرئيسي والمنتجات الفرعية من المتجر
    db.products.delete_many({'cgpt_product_id': pid})
    db.products.delete_one({'_id': f"cgpt_main_{pid}"})
    db.cgpt_products.delete_one({'_id': _ObjId2(pid)})
    bot.answer_callback_query(call.id, "\u2705 \u062a\u0645 \u0627\u0644\u062d\u0630\u0641!", show_alert=True)
    _cgpt_show_products_list(call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda call: call.data == "cgpt_set_cookies")
@admin_required
def cgpt_set_cookies(call):
    bot.answer_callback_query(call.id)
    mgr = get_cgpt_manager()
    loaded_icon = "\u2705" if mgr._loaded else "\u274c"
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("\U0001f4cb \u0644\u0635\u0642 \u0627\u0644\u0643\u0648\u0643\u064a\u0632 (JSON)", callback_data="cgpt_paste_json"),
        InlineKeyboardButton("\U0001f504 \u0625\u0639\u0627\u062f\u0629 \u062a\u062d\u0645\u064a\u0644 \u0645\u0646 DB", callback_data="ad_cgpt_reload_token"),
        InlineKeyboardButton("\U0001f519 \u0631\u062c\u0648\u0639", callback_data="ad_cgpt_panel")
    )
    txt = f"\U0001f36a <b>\u0625\u0639\u062f\u0627\u062f \u0627\u0644\u0643\u0648\u0643\u064a\u0632</b>\n\n\u0627\u0644\u062d\u0627\u0644\u0629: {loaded_icon}\n\u0627\u062e\u062a\u0631 \u0637\u0631\u064a\u0642\u0629 \u0627\u0644\u0625\u0636\u0627\u0641\u0629:"
    try:
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
    except:
        bot.send_message(call.message.chat.id, txt, parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "cgpt_paste_json")
@admin_required
def cgpt_paste_json(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.from_user.id, "\U0001f4cb <b>\u0627\u0644\u0635\u0642 \u0645\u062d\u062a\u0648\u0649 \u0645\u0644\u0641 \u0627\u0644\u0643\u0648\u0643\u064a\u0632 (JSON \u0643\u0627\u0645\u0644):</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, cgpt_save_json_cookies)

def cgpt_save_json_cookies(message):
    try:
        data = json.loads(message.text.strip())
        # نحفظ في قاعدة البيانات (دائم)
        db.cgpt_cookies.update_one({'_id': 'main'}, {'$set': {'data': data}}, upsert=True)
        # نعيد تحميل المدير
        global _cgpt_manager_instance
        with _cgpt_lock:
            _cgpt_manager_instance = None
        ok = get_cgpt_manager()._loaded
        icon = "\u2705" if ok else "\u274c"
        bot.send_message(message.chat.id,
            f"{icon} <b>{'\u062a\u0645 \u062d\u0641\u0638 \u0627\u0644\u0643\u0648\u0643\u064a\u0632 \u0641\u064a \u0642\u0627\u0639\u062f\u0629 \u0627\u0644\u0628\u064a\u0627\u0646\u0627\u062a!' if ok else '\u062a\u0645 \u0627\u0644\u062d\u0641\u0638 \u0644\u0643\u0646 \u0641\u0634\u0644 \u0627\u0644\u062a\u062d\u0645\u064a\u0644!'}</b>",
            parse_mode="HTML")
    except Exception as e:
        bot.send_message(message.chat.id, f"\u274c <b>JSON \u063a\u064a\u0631 \u0635\u062d\u064a\u062d!</b>\n<code>{e}</code>", parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "ad_cgpt_cleanup")
@admin_required
def ad_cgpt_cleanup_now(call):
    bot.answer_callback_query(call.id, "\u23f3 \u062c\u0627\u0631\u064a \u0627\u0644\u0641\u062d\u0635...", show_alert=False)
    try:
        get_cgpt_manager().check_and_cleanup()
        bot.answer_callback_query(call.id, "\u2705 \u062a\u0645 \u0627\u0644\u0641\u062d\u0635 \u0648\u0627\u0644\u062a\u0646\u0638\u064a\u0641!", show_alert=True)
    except Exception as e:
        bot.answer_callback_query(call.id, f"\u274c \u062e\u0637\u0623: {e}", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data == "ad_cgpt_reload_token")
@admin_required
def ad_cgpt_reload_token(call):
    """إعادة تحميل الكوكيز من MongoDB"""
    bot.answer_callback_query(call.id)
    global _cgpt_manager_instance
    with _cgpt_lock:
        _cgpt_manager_instance = None
    ok = get_cgpt_manager()._loaded
    icon = "\u2705" if ok else "\u274c"
    bot.send_message(call.from_user.id,
        f"{icon} <b>{'\u062a\u0645 \u0625\u0639\u0627\u062f\u0629 \u062a\u062d\u0645\u064a\u0644 \u0627\u0644\u0643\u0648\u0643\u064a\u0632 \u0645\u0646 \u0642\u0627\u0639\u062f\u0629 \u0627\u0644\u0628\u064a\u0627\u0646\u0627\u062a!' if ok else '\u0644\u0627 \u062a\u0648\u062c\u062f \u0643\u0648\u0643\u064a\u0632 \u0641\u064a \u0642\u0627\u0639\u062f\u0629 \u0627\u0644\u0628\u064a\u0627\u0646\u0627\u062a!'}</b>",
        parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "ad_cgpt_invite")
@admin_required
def ad_cgpt_manual_invite(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.from_user.id, "\U0001f4e7 <b>\u0623\u0631\u0633\u0644 \u0627\u0644\u0625\u064a\u0645\u064a\u0644 \u0648\u0627\u0644\u0645\u062f\u0629:</b>\n<code>email@example.com 10080</code>", parse_mode="HTML")
    bot.register_next_step_handler(msg, _cgpt_manual_invite_exec)

def _cgpt_manual_invite_exec(message):
    parts = message.text.strip().split()
    if len(parts) != 2:
        bot.send_message(message.chat.id, "\u274c \u0635\u064a\u063a\u0629 \u062e\u0627\u0637\u0626\u0629.", parse_mode="HTML"); return
    email, mins_str = parts
    try:
        mins = int(mins_str)
    except:
        bot.send_message(message.chat.id, "\u274c \u0627\u0644\u0645\u062f\u0629 \u064a\u062c\u0628 \u0623\u0646 \u062a\u0643\u0648\u0646 \u0631\u0642\u0645\u0627\u064b."); return
    result = get_cgpt_manager().invite_user(email, mins)
    days = round(mins / 1440, 1)
    if result['ok']:
        bot.send_message(message.chat.id, f"\u2705 <b>\u062a\u0645\u062a \u0627\u0644\u062f\u0639\u0648\u0629!</b>\n\U0001f4e7 <code>{email}</code>\n\u23f1 {days} \u064a\u0648\u0645\n\U0001f4c5 \u064a\u0646\u062a\u0647\u064a: {result['expires_at'][:10]}", parse_mode="HTML")
    else:
        bot.send_message(message.chat.id, f"\u274c <b>\u0641\u0634\u0644\u062a \u0627\u0644\u062f\u0639\u0648\u0629!</b>\n<code>{result.get('error')}</code>", parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "ad_p_add")
@admin_required
def ad_p_step1(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    temp_product[uid] = {}
    msg = bot.send_message(uid,
        "📦 <b>أرسل اسم المنتج (بالعربية):</b>\n"
        "<i>يمكنك استخدام Premium Emojis والتنسيقات</i>",
        parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_p_step2)

def ad_p_step2(message):
    uid = message.from_user.id
    # نستخرج Premium Emojis ونحولها لـ HTML
    n_ar = extract_custom_emojis_to_html(message)
    # نترجم مع الحفاظ على التنسيق والإيموجي
    n_en = safe_translate_for_cms(n_ar, 'en')
    temp_product[uid] = {'n_ar': n_ar, 'n_en': n_en}
    try:
        bot.send_message(uid,
            f"✅ <b>تم حفظ الاسم!</b>\n\n"
            f"🇸🇦 العربي: {n_ar}\n"
            f"🇬🇧 الإنجليزي: {n_en}",
            parse_mode="HTML")
    except:
        bot.send_message(uid, "✅ تم حفظ الاسم!")
    msg = bot.send_message(uid,
        "📝 <b>أرسل وصف المنتج (بالعربية):</b>\n"
        "<i>يمكنك استخدام Premium Emojis، Bold، Italic</i>",
        parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_p_step3)

def ad_p_step3(message):
    uid = message.from_user.id
    # نستخرج Premium Emojis ونحولها لـ HTML
    d_ar = extract_custom_emojis_to_html(message)
    # نترجم مع الحفاظ على كل التنسيقات والإيموجي
    d_en = safe_translate_for_cms(d_ar, 'en')
    temp_product[uid].update({'d_ar': d_ar, 'd_en': d_en})
    try:
        bot.send_message(uid,
            f"✅ <b>تم حفظ الوصف!</b>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"🇸🇦 <b>العربي:</b>\n{d_ar}\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"🇬🇧 <b>الإنجليزي:</b>\n{d_en}\n"
            f"━━━━━━━━━━━━━━",
            parse_mode="HTML")
    except:
        bot.send_message(uid, "✅ تم حفظ الوصف!")
    msg = bot.send_message(uid, "💰 <b>أرسل السعر بالدولار ($):</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_p_price)

def ad_p_price(message):
    uid = message.from_user.id
    try:
        price = float(message.text.strip())
        temp_product[uid]['price'] = price
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("⚡ تسليم تلقائي (أكواد وبطاقات)", callback_data="ad_ptype_auto"))
        markup.add(InlineKeyboardButton("🤝 تسليم يدوي (يتواصل العميل معك)", callback_data="ad_ptype_manual"))
        markup.add(InlineKeyboardButton("🤖 مقعد ChatGPT Business (بالإيميل)", callback_data="ad_ptype_cgpt"))
        bot.send_message(uid, "⚙️ <b>اختر نوع تسليم هذا المنتج:</b>", reply_markup=markup, parse_mode="HTML")
    except:
        bot.send_message(uid, "❌ خطأ في السعر. أرسل رقماً فقط مثل: <code>5.99</code>", parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data == "ad_ptype_cgpt")
@admin_required
def ad_ptype_cgpt_handler(call):
    """يطلب مدة الوصول بالدقائق ثم يحفظ المنتج كـ chatgpt_seat"""
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    msg = bot.send_message(uid,
        "⏱ <b>كم دقيقة يكون الوصول صالحاً؟</b>\n\n"
        "أمثلة:\n"
        "• 7 أيام  → <code>10080</code>\n"
        "• 15 يوم → <code>21600</code>\n"
        "• 25 يوم → <code>36000</code>\n"
        "• 30 يوم → <code>43200</code>",
        parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_ptype_cgpt_save_minutes)

def ad_ptype_cgpt_save_minutes(message):
    uid = message.from_user.id
    try:
        minutes = int(message.text.strip())
        if minutes <= 0: raise ValueError
    except:
        bot.send_message(uid, "❌ أرسل رقم صحيح موجب.")
        return
    p = temp_product.get(uid)
    if not p: return
    p['cgpt_minutes'] = minutes
    p['product_type'] = 'chatgpt_seat'
    p['is_manual'] = False
    days = round(minutes / 1440, 1)
    pid = str(int(time.time()))
    cats = list(db.catalogs.find())
    markup = InlineKeyboardMarkup(row_width=1)
    if cats:
        for cat in cats:
            markup.add(InlineKeyboardButton(
                f"📁 {cat.get('name_ar', cat.get('name', '?'))}",
                callback_data=f"ad_p_cgpt_cat_{pid}_{cat['_id']}"
            ))
    markup.add(InlineKeyboardButton("➕ بدون مجلد", callback_data=f"ad_p_cgpt_cat_{pid}_none"))
    temp_product[uid]['_pid'] = pid
    bot.send_message(uid,
        f"✅ المدة: <b>{days} يوم ({minutes} دقيقة)</b>\n\n📁 اختر المجلد:",
        parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_p_cgpt_cat_"))
@admin_required
def ad_p_cgpt_cat_selected(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    parts = call.data.replace("ad_p_cgpt_cat_", "").split("_", 1)
    pid   = parts[0]
    cat_id = None if parts[1] == "none" else parts[1]
    p = temp_product.get(uid)
    if not p: return
    doc = {
        '_id': pid,
        'name_ar':      p.get('n_ar', ''),
        'name_en':      p.get('n_en', ''),
        'desc_ar':      p.get('d_ar', ''),
        'desc_en':      p.get('d_en', ''),
        'price':        float(p.get('price', 0)),
        'is_manual':    False,
        'product_type': 'chatgpt_seat',
        'cgpt_minutes': int(p.get('cgpt_minutes', 10080)),
        'catalog_id':   str(cat_id) if cat_id else None,
        'is_hidden':    False,
        'created_at':   _dt_mod.datetime.now().isoformat()
    }
    db.products.insert_one(doc)
    
    # 🆕 ربط المنتج بمصفوفة product_ids الخاصة بالمجلد
    if cat_id:
        from bson import ObjectId
        db.catalogs.update_one(
            {'_id': ObjectId(cat_id)},
            {'$addToSet': {'product_ids': pid}}
        )
        
    days = round(doc['cgpt_minutes'] / 1440, 1)
    bot.edit_message_text(
        f"✅ <b>تم إنشاء منتج ChatGPT Business!</b>\n\n"
        f"📦 <b>{doc['name_ar']}</b>\n"
        f"💰 <b>${doc['price']:.2f}</b>\n"
        f"⏱ <b>{days} يوم ({doc['cgpt_minutes']} دقيقة)</b>\n"
        f"📁 <b>المجلد:</b> {cat_id or 'بدون مجلد'}",
        call.message.chat.id, call.message.message_id, parse_mode="HTML"
    )
    temp_product.pop(uid, None)


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
    raw_ep = call.data.replace("edit_p_", "")
    if '_c_' in raw_ep:
        pid, cat_id_back = raw_ep.split('_c_', 1)
    else:
        pid = raw_ep
        cat_id_back = None
    p = find_product(pid)
    if not p: return
    
    # suffix لتمرير cat_id في كل الأزرار
    c_sfx = f"_c_{cat_id_back}" if cat_id_back else ""
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("💵 Price", callback_data=f"ep_price_{pid}{c_sfx}"))
    markup.add(InlineKeyboardButton("📝 Desc (AR)", callback_data=f"ep_dar_{pid}{c_sfx}"),
               InlineKeyboardButton("📝 Desc (EN)", callback_data=f"ep_den_{pid}{c_sfx}"))
    markup.add(InlineKeyboardButton("✏️ Name (AR)", callback_data=f"ep_nar_{pid}{c_sfx}"),
               InlineKeyboardButton("✏️ Name (EN)", callback_data=f"ep_nen_{pid}{c_sfx}"))
    markup.add(InlineKeyboardButton("📁 Change Folder", callback_data=f"ep_setcat_{pid}{c_sfx}"),
               InlineKeyboardButton("⭐ Custom Emoji", callback_data=f"ep_emoji_{pid}{c_sfx}"))
    markup.add(InlineKeyboardButton("🏷 خصومات الكمية", callback_data=f"ep_disc_{pid}{c_sfx}"))
    hide_txt = "👁️ Show Product" if p.get('is_hidden', False) else "🙈 Hide Product"
    markup.add(InlineKeyboardButton(hide_txt, callback_data=f"toggle_hide_{pid}{c_sfx}"))
    # زر رجوع: للمنتج نفسه وليس لقائمة الأدمن
    back_cb = f"vi_p_{pid}_c_{cat_id_back}" if cat_id_back else f"vi_p_{pid}"
    # زر جعله أول في مجلده
    p_doc = find_product(pid)
    if p_doc:
        p_cat = p_doc.get('catalog_id') or (cat_id_back if cat_id_back else None)
        if p_cat:
            markup.add(InlineKeyboardButton("⬆️ جعله أول في المجلد", callback_data=f"p_set_first_{pid}_{p_cat}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data=back_cb))
    
    try: bot.edit_message_text("⚙️ Options:", call.message.chat.id, call.message.message_id, reply_markup=markup)
    except: pass


# ═══ تعيين مجلد للمنتج من صفحة خيارات التعديل ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("ep_setcat_"))
@admin_required
def ep_setcat_handler(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    try:
        raw = call.data.replace("ep_setcat_", "")
        if '_c_' in raw:
            pid, cat_id_back = raw.split('_c_', 1)
        else:
            pid = raw
            cat_id_back = None
            
        p = find_product(pid)
        if not p: return
        
        c_sfx = f"_c_{cat_id_back}" if cat_id_back else ""
        
        cats = list(db.catalogs.find().sort('order', 1))
        markup = InlineKeyboardMarkup(row_width=1)
        for cat in cats:
            cat_id = str(cat['_id'])
            cat_name = cat.get('name_ar', cat.get('name', ''))
            markup.add(InlineKeyboardButton(
                f"📁 {cat_name}",
                callback_data=f"ep_dosetcat_{pid}_{cat_id}{c_sfx}"
            ))
        markup.add(InlineKeyboardButton("❌ Without Catalog (بدون مجلد)", callback_data=f"ep_dosetcat_{pid}_none{c_sfx}"))
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"edit_p_{pid}{c_sfx}"))
        
        txt = f"📁 <b>Choose a folder for product:</b>\n{clean_name(p.get('name_ar', p.get('name_en', '')))}"
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        logger.exception("Error in ep_setcat_handler:")
        try: bot.send_message(call.message.chat.id, f"❌ Error: {e}")
        except: pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("ep_dosetcat_"))
@admin_required
def ep_dosetcat_handler(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    try:
        raw = call.data.replace("ep_dosetcat_", "")
        if '_c_' in raw:
            parts_raw, cat_id_back = raw.split('_c_', 1)
        else:
            parts_raw = raw
            cat_id_back = None
            
        parts = parts_raw.rsplit("_", 1)
        pid = parts[0]
        cat_id = parts[1] if len(parts) > 1 else "none"
        
        p = find_product(pid)
        if not p: return
        
        # Remove from all old catalogs
        for c in db.catalogs.find():
            if pid in (c.get('product_ids') or []):
                db.catalogs.update_one(
                    {'_id': c['_id']},
                    {'$pull': {'product_ids': pid}}
                )
        
        # Add to new catalog if not none
        if cat_id != "none":
            from bson import ObjectId
            db.catalogs.update_one(
                {'_id': ObjectId(cat_id)},
                {'$push': {'product_ids': {'$each': [pid], '$position': 0}}}
            )
            db.products.update_one({'_id': p['_id']}, {'$set': {'catalog_id': str(cat_id)}})
        else:
            db.products.update_one({'_id': p['_id']}, {'$set': {'catalog_id': None}})
            
        bot.answer_callback_query(call.id, "✅ Folder updated successfully!", show_alert=True)
        
        # Go back to edit options
        c_sfx = f"_c_{cat_id_back}" if cat_id_back else ""
        call.data = f"edit_p_{pid}{c_sfx}"
        admin_edit_opts(call)
    except Exception as e:
        logger.exception("Error in ep_dosetcat_handler:")
        try: bot.send_message(call.message.chat.id, f"❌ Error: {e}")
        except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("ep_disc_"))
@admin_required
def ep_disc_ui(call):
    """إدارة خصومات الكمية للمنتج"""
    bot.answer_callback_query(call.id)
    pid = call.data.replace("ep_disc_", "")
    p = find_product(pid)
    if not p: return

    tiers = p.get('discount_tiers', [])
    tiers_sorted = sorted(tiers, key=lambda x: x.get('min_qty', 0))

    unit_price = float(p.get('price', 0))
    text = (
        f"🏷 <b>خصومات الكمية</b>\n"
        f"📦 {clean_name(p.get('name_ar', ''))}\n"
        f"💰 <b>السعر الأصلي: ${unit_price:.2f}/قطعة</b>\n\n"
    )
    if tiers_sorted:
        text += "<b>الخصومات الحالية:</b>\n"
        for t in tiers_sorted:
            t_price = float(t.get('price', 0))
            text += f"  • {t.get('min_qty')}+ قطعة = <b>${t_price:.2f}</b>/قطعة\n"
    else:
        text += "<i>لا توجد خصومات بعد.</i>\n"

    text += (
        f"\n━━━━━━━━━━━━━━\n"
        f"💡 <b>لإضافة خصم:</b>\n"
        f"أرسل: العدد ثم السعر الجديد\n\n"
        f"مثال: <code>3 {unit_price*0.9:.2f}</code> = 3+ قطع بسعر ${unit_price*0.9:.2f}\n"
        f"مثال: <code>5 {unit_price*0.8:.2f}</code> = 5+ قطع بسعر ${unit_price*0.8:.2f}\n\n"
        f"لحذف كل الخصومات: <code>clear</code>"
    )

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"edit_p_{pid}"))
    msg = bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)
    bot.register_next_step_handler(msg, _save_discount_tier, pid)


def _save_discount_tier(message, pid):
    if not hasattr(message, 'text') or hasattr(message, 'data'):
        return
    uid = message.from_user.id
    if not _is_admin_check(uid): return

    text = message.text.strip()

    if text.lower() == 'clear':
        db.products.update_one({'id': pid}, {'$set': {'discount_tiers': []}})
        db.products.update_one({'id': int(pid)} if str(pid).isdigit() else {'id': pid},
                               {'$set': {'discount_tiers': []}})
        bot.send_message(uid, "✅ تم حذف كل الخصومات.")
        return

    try:
        parts = text.split()
        min_qty = int(parts[0])
        price = float(parts[1])
        if min_qty < 1 or price <= 0:
            raise ValueError()
    except:
        bot.send_message(uid, "❌ صيغة خاطئة. مثال: <code>3 4.50</code>\n(العدد ثم السعر بالدولار)", parse_mode="HTML")
        return

    p = find_product(pid)
    if not p:
        bot.send_message(uid, "❌ المنتج غير موجود.")
        return

    unit_price = float(p.get('price', 0))
    tiers = p.get('discount_tiers', [])
    tiers = [t for t in tiers if t.get('min_qty') != min_qty]
    tiers.append({'min_qty': min_qty, 'price': price})
    tiers = sorted(tiers, key=lambda x: x.get('min_qty', 0))

    db.products.update_one({'_id': p['_id']}, {'$set': {'discount_tiers': tiers}})
    bot.send_message(
        uid,
        f"✅ <b>تم حفظ الخصم!</b>\n\n"
        f"كل <b>{min_qty}+</b> قطعة = <b>${price:.2f}</b>/قطعة\n\n"
        f"📋 الخصومات الحالية:\n" +
        "".join(f"  • {t['min_qty']}+ = ${t.get('price', 0):.2f}\n" for t in tiers),
        parse_mode="HTML"
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("toggle_hide_"))
def admin_toggle_hide(call):
    bot.answer_callback_query(call.id)
    raw_th = call.data.replace("toggle_hide_", "")
    if '_c_' in raw_th:
        pid, cat_id_back = raw_th.split('_c_', 1)
    else:
        pid = raw_th
        cat_id_back = None
    p = find_product(pid)
    if p:
        new_status = not p.get('is_hidden', False)
        db.products.update_one({'_id': p['_id']}, {'$set': {'is_hidden': new_status}})
        bot.answer_callback_query(call.id, "✅ Visibility updated!", show_alert=True)
        call.data = f"edit_p_{pid}_c_{cat_id_back}" if cat_id_back else f"edit_p_{pid}"
        admin_edit_opts(call)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ep_"))
def admin_edit_prompt(call):
    bot.answer_callback_query(call.id)
    raw_ep2 = call.data[len("ep_"):]  # بعد ep_
    # تحديد الـ field (price/dar/den/nar/nen/emoji)
    for _f in ['price', 'dar', 'den', 'nar', 'nen', 'emoji']:
        if raw_ep2.startswith(_f + '_'):
            field = _f
            rest_ep2 = raw_ep2[len(_f)+1:]
            break
    else:
        parts = call.data.split('_', 2)
        field = parts[1]
        rest_ep2 = parts[2]
    # استخراج pid و cat_id
    if '_c_' in rest_ep2:
        pid, cat_id_back = rest_ep2.split('_c_', 1)
    else:
        pid = rest_ep2
        cat_id_back = None
    
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
        old_desc = p.get('desc_ar', '-')
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
        old_desc = p.get('desc_en', '-')
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
        old_name = p.get('name_ar', '-')
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
        old_name = p.get('name_en', '-')
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
    elif field == "emoji":
        prompt_msg = (
            f"━━━━━━━━━━━━━━━\n"
            f"⭐ <b>تعديل الرمز التعبيري المميز</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"{p_display_name}\n\n"
            f"👇 <b>أرسل الآن الإيموجي المخصص (Premium Emoji) الجديد للمنتج:</b>\n"
            f"<i>(أرسله كرسالة عادية وسأقوم بالتقاطه)</i>\n\n"
            f"💡 <i>أرسل <b>الغاء</b> للإلغاء.</i>"
        )
    else:
        prompt_msg = "👇 أرسل القيمة الجديدة:"
    
    msg = bot.send_message(call.message.chat.id, prompt_msg, parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_save_edit, field, pid, cat_id_back)

# ----------- حفظ تعديل المنتج + إشعار تخفيض السعر -----------
def admin_save_edit(message, field, pid, cat_id_back=None):
    val = message.text or ""
    
    # زر رجوع: يرجع للمنتج وليس للقائمة
    back_cb = f"vi_p_{pid}_c_{cat_id_back}" if cat_id_back else f"vi_p_{pid}"
    
    # دعم الإلغاء
    if val.strip().lower() in ['الغاء', 'cancel', '/cancel']:
        markup_cancel = InlineKeyboardMarkup()
        markup_cancel.add(InlineKeyboardButton("🔙 رجوع للمنتج", callback_data=back_cb))
        bot.send_message(message.chat.id, "❌ تم إلغاء التعديل.", reply_markup=markup_cancel)
        return
    
    keys = {"price": "price", "dar": "desc_ar", "den": "desc_en", "nar": "name_ar", "nen": "name_en"}
    p = find_product(pid)
    if not p: 
        bot.send_message(message.chat.id, "❌ المنتج لم يعد موجوداً.")
        return

    if field == "emoji":
        emoji_id = None
        if message.entities:
            for ent in message.entities:
                if ent.type == 'custom_emoji':
                    emoji_id = ent.custom_emoji_id
                    break
        if not emoji_id:
            bot.send_message(message.chat.id, "❌ لم يتم العثور على رمز Premium. أعد المحاولة من قائمة تعديل المنتج.")
            return
        
        # Save it
        db.products.update_one({'_id': p['_id']}, {'$set': {'custom_emoji_id': emoji_id}})
        
        # Also sync to db.cgpt_products if it's a ChatGPT product
        cgpt_id = p.get('cgpt_product_id')
        if cgpt_id:
            try:
                from bson import ObjectId
                db.cgpt_products.update_one({'_id': ObjectId(cgpt_id)}, {'$set': {'custom_emoji_id': emoji_id}})
            except: pass
            
        back_markup = InlineKeyboardMarkup()
        back_markup.add(InlineKeyboardButton("🔙 رجوع للمنتج", callback_data=back_cb))
        bot.send_message(message.chat.id, f"✅ <b>تم تحديث الأيقونة المميزة للمنتج بنجاح!</b>", parse_mode="HTML", reply_markup=back_markup)
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
            
            back_markup = InlineKeyboardMarkup()
            back_markup.add(InlineKeyboardButton("🔙 رجوع للمنتج", callback_data=back_cb))
            bot.send_message(message.chat.id, f"{change_emoji} <b>تم التحديث!</b>\n{change_txt}", parse_mode="HTML", reply_markup=back_markup)
            
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
                
                # 🆕 مزامنة مع db.cgpt_products
                cgpt_id = p.get('cgpt_product_id')
                if cgpt_id:
                    try:
                        from bson import ObjectId
                        db.cgpt_products.update_one({'_id': ObjectId(cgpt_id)}, {'$set': {'name': final_text, 'name_en': translated}})
                    except: pass
                    
                back_markup2 = InlineKeyboardMarkup()
                back_markup2.add(InlineKeyboardButton("🔙 رجوع للمنتج", callback_data=back_cb))
                bot.send_message(message.chat.id, f"✅ <b>تم تحديث الاسم!</b>\n\n🇸🇦 العربي: {final_text}\n🇬🇧 الإنجليزي (مترجم تلقائياً): {translated}", parse_mode="HTML", reply_markup=back_markup2)
            else:
                db.products.update_one({'_id': p['_id']}, {'$set': {'desc_ar': final_text, 'desc_en': translated}})
                
                # 🆕 مزامنة مع db.cgpt_products
                cgpt_id = p.get('cgpt_product_id')
                if cgpt_id:
                    try:
                        from bson import ObjectId
                        db.cgpt_products.update_one({'_id': ObjectId(cgpt_id)}, {'$set': {'desc': final_text, 'desc_en': translated}})
                    except: pass
                    
                back_markup3 = InlineKeyboardMarkup()
                back_markup3.add(InlineKeyboardButton("🔙 رجوع للمنتج", callback_data=back_cb))
                bot.send_message(message.chat.id, "✅ <b>تم تحديث الوصف العربي + ترجمته للإنجليزي تلقائياً.</b>", parse_mode="HTML", reply_markup=back_markup3)
        else:
            # إنجليزي فقط
            db.products.update_one({'_id': p['_id']}, {'$set': {keys[field]: final_text}})
            
            # 🆕 مزامنة مع db.cgpt_products
            cgpt_id = p.get('cgpt_product_id')
            if cgpt_id:
                try:
                    from bson import ObjectId
                    field_name = keys[field]
                    if field_name == 'name_en':
                        db.cgpt_products.update_one({'_id': ObjectId(cgpt_id)}, {'$set': {'name_en': final_text}})
                    elif field_name == 'desc_en':
                        db.cgpt_products.update_one({'_id': ObjectId(cgpt_id)}, {'$set': {'desc_en': final_text}})
                except: pass
                
            back_markup4 = InlineKeyboardMarkup()
            back_markup4.add(InlineKeyboardButton("🔙 رجوع للمنتج", callback_data=back_cb))
            bot.send_message(message.chat.id, "✅ <b>تم التحديث بنجاح.</b>", parse_mode="HTML", reply_markup=back_markup4)

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
                    
                    icon_html = f'<tg-emoji emoji-id="{custom_emoji_id}">✨</tg-emoji> ' if custom_emoji_id else '📦 '
                    p_name = icon_html + clean_name(p.get(f'name_{u_lang}', p.get('name_en', '')))
                    p_name_plain = clean_name(p.get(f'name_{u_lang}', p.get('name_en', '')))
                    unit_price = float(p.get('price', 0))
                    
                    alert_msg = get_text(uid_u, 'new_stock', p_name, stk_total)
                    
                    # إضافة الخصومات لرسالة البرودكاست
                    discount_tiers = sorted(p.get('discount_tiers', []), key=lambda x: x.get('min_qty', 0))
                    if discount_tiers:
                        if u_lang == 'ar':
                            alert_msg += f"\n\n💰 <b>السعر:</b> ${unit_price:.2f}/قطعة\n🏷 <b>خصومات الكمية:</b>\n"
                        else:
                            alert_msg += f"\n\n💰 <b>Price:</b> ${unit_price:.2f}/unit\n🏷 <b>Qty Discounts:</b>\n"
                        for t in sorted(discount_tiers, key=lambda x: x.get('min_qty', 0)):
                            t_price = float(t.get('price', unit_price))
                            if u_lang == 'ar':
                                alert_msg += f"  • {t['min_qty']}+ قطعة → <b>${t_price:.2f}</b>/قطعة\n"
                            else:
                                alert_msg += f"  • {t['min_qty']}+ units → <b>${t_price:.2f}</b>/unit\n"
                    else:
                        alert_msg += f"\n\n💰 <b>{'السعر' if u_lang == 'ar' else 'Price'}:</b> ${unit_price:.2f}"
                    
                    # 🟢 زر الشراء الأخضر (Bot API 9.4)
                    markup = InlineKeyboardMarkup()
                    btn_label = f"🛒 {p_name_plain}"
                    
                    buy_btn = CustomInlineButton(
                        text=btn_label,
                        callback_data=f"vi_p_{pid_for_thread}",
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
    markup.add(
        InlineKeyboardButton("🛍 المشتريات", callback_data=f"ad_uh_buy_{target_uid}"),
        InlineKeyboardButton("💳 الإيداعات", callback_data=f"ad_uh_dep_{target_uid}")
    )
    markup.add(InlineKeyboardButton("📥 تحميل السجل الكامل", callback_data=f"ad_full_hist_{target_uid}"))
    markup.add(InlineKeyboardButton("💰 تعديل رصيده", callback_data=f"ad_ugift_{target_uid}"))
    markup.add(InlineKeyboardButton("📋 سجل التعديلات المالية", callback_data=f"ad_view_ballogs_{target_uid}_0"))
    markup.add(InlineKeyboardButton("👥 إحالاته", callback_data=f"ad_uref_{target_uid}_0"))
    markup.add(InlineKeyboardButton("🔙 رجوع للبحث", callback_data="ad_users_main"))
    try:
        if message_id: bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="HTML")
        else: bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
    except: pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_uref_"))
@admin_required
def admin_user_referrals_page(call):
    """👥 عرض إحالات مستخدم معيّن للأدمن (بصفحات وأسهم)"""
    bot.answer_callback_query(call.id)
    rest = call.data.replace("ad_uref_", "")
    parts = rest.rsplit("_", 1)
    try:
        target_uid = int(parts[0])
        page = int(parts[1])
    except Exception:
        return
    PER_PAGE = 10

    refs_active = list(db.referrals_v2.find({'referrer_id': target_uid}))
    refs_left = list(db.referrals_archived.find({'referrer_id': target_uid}))

    status_order = {'active': 0, 'pending': 1, 'left': 2}
    all_refs = []
    for r in refs_active:
        all_refs.append({'id': r.get('invited_id'), 'status': r.get('status', 'pending')})
    for r in refs_left:
        all_refs.append({'id': r.get('invited_id'), 'status': 'left'})
    all_refs.sort(key=lambda x: status_order.get(x['status'], 9))

    total = len(all_refs)
    active_n = sum(1 for r in all_refs if r['status'] == 'active')
    pending_n = sum(1 for r in all_refs if r['status'] == 'pending')
    left_n = sum(1 for r in all_refs if r['status'] == 'left')

    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * PER_PAGE
    page_refs = all_refs[start:start + PER_PAGE]

    # معلومات صاحب الإحالات + أرباحه
    target_user = get_user_data_full(target_uid)
    t_uname = f"@{target_user.get('username')}" if target_user and target_user.get('username') else ''
    earn_ref = round(float(target_user.get('ref_v2_earned', 0.0)), 2) if target_user else 0.0
    earn_buy = round(float(target_user.get('ref_v2_purchase_earned', 0.0)), 2) if target_user else 0.0

    txt = (
        f"👥 <b>إحالات المستخدم</b>\n"
        f"🆔 <code>{target_uid}</code> {t_uname}\n"
        f"━━━━━━━━━━━━━━\n"
        f"🟢 نشط: <b>{active_n}</b> | 🟡 معلق: <b>{pending_n}</b> | 🔴 غادر: <b>{left_n}</b>\n"
        f"💰 أرباح الإحالات: <b>${earn_ref:.2f}</b> | 🛍 المشتريات: <b>${earn_buy:.2f}</b>\n"
        f"📊 الإجمالي: <b>{total}</b>  (صفحة {page+1}/{total_pages})\n"
        f"━━━━━━━━━━━━━━\n\n"
    )

    if not all_refs:
        txt += "📭 لا يوجد إحالات لهذا المستخدم."
    else:
        for r in page_refs:
            inv_id = r['id']
            status = r['status']
            icon = "🟢" if status == 'active' else ("🟡" if status == 'pending' else "🔴")
            inv_user = get_user_data_full(inv_id)
            if inv_user:
                name = clean_name(inv_user.get('name', ''))[:15]
                uname = f"@{inv_user.get('username')}" if inv_user.get('username') else ''
                txt += f"{icon} <code>{inv_id}</code> {name} {uname}\n"
            else:
                txt += f"{icon} <code>{inv_id}</code>\n"

    markup = InlineKeyboardMarkup(row_width=2)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ السابق", callback_data=f"ad_uref_{target_uid}_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("التالي ▶️", callback_data=f"ad_uref_{target_uid}_{page+1}"))
    if nav:
        markup.add(*nav)
    markup.add(InlineKeyboardButton("🔙 رجوع لملف العميل", callback_data=f"ad_u_det_{target_uid}"))

    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
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
    """عرض مشتريات المستخدم للأدمن مع التفاصيل الكاملة"""
    bot.answer_callback_query(call.id)
    target_uid = int(call.data.replace("ad_uh_buy_", ""))
    
    recs = list(db.orders.find({'user_id': target_uid}).sort('_id', -1))
    u = get_user_data_full(target_uid)
    uname = f"@{u['username']}" if u and u.get('username') else "بدون"
    
    if not recs:
        text = f"📭 <b>لا توجد مشتريات</b>\n\n👤 {uname} | <code>{target_uid}</code>"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"ad_u_det_{target_uid}"))
        try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        except: pass
        return
    
    # إجمالي المبالغ
    total_spent = 0.0
    for r in recs:
        try:
            p = find_product(str(r.get('product_id', '')))
            price = float(p.get('price', 0)) if p else 0
            qty = int(r.get('quantity', 1))
            total_spent += price * qty
        except: pass
    
    text = (
        f"🛍 <b>مشتريات المستخدم</b>\n\n"
        f"👤 {uname} | <code>{target_uid}</code>\n"
        f"📊 عدد الطلبات: <b>{len(recs)}</b>\n"
        f"💰 إجمالي الإنفاق: <b>${total_spent:.2f}</b>\n"
        f"━━━━━━━━━━━━━━\n"
    )
    
    # آخر 10 مشتريات
    for i, r in enumerate(recs[:10], 1):
        try:
            date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M')
            pid = str(r.get('product_id', ''))
            qty = int(r.get('quantity', 1))
            
            if pid in ['GitHub_Student', 'Gemini_Activation']:
                p_name = pid.replace('_', ' ')
                price = 0
            else:
                p = find_product(pid)
                p_name = clean_name(p.get('name_ar', p.get('name_en', 'منتج محذوف'))) if p else 'منتج محذوف'
                price = float(p.get('price', 0)) if p else 0
            
            text += (
                f"\n#{i} 📦 <b>{p_name}</b>\n"
                f"   📅 {date_str}\n"
                f"   🔢 الكمية: {qty} | 💵 السعر: ${price:.2f}/قطعة\n"
                f"   💰 الإجمالي: ${price * qty:.2f}\n"
            )
        except: pass
    
    if len(recs) > 10:
        text += f"\n<i>... و {len(recs) - 10} طلب آخر في الملف</i>"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📄 تحميل الملف الكامل", callback_data=f"ad_dlbuy_{target_uid}"))
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"ad_u_det_{target_uid}"))
    
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except:
        bot.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_dlbuy_"))
@admin_required
def ad_dlbuy_handler(call):
    """تحميل ملف كامل بكل مشتريات المستخدم"""
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز الملف...")
    target_uid = int(call.data.replace("ad_dlbuy_", ""))
    
    recs = list(db.orders.find({'user_id': target_uid}).sort('_id', -1))
    u = get_user_data_full(target_uid)
    uname = f"@{u['username']}" if u and u.get('username') else "بدون"
    balance = float(u.get('balance', 0)) if u else 0
    
    total_spent = 0.0
    lines = []
    
    for i, r in enumerate(recs, 1):
        try:
            date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
            pid = str(r.get('product_id', ''))
            qty = int(r.get('quantity', 1))
            
            if pid in ['GitHub_Student', 'Gemini_Activation']:
                p_name = pid.replace('_', ' ')
                price = 0
            else:
                p = find_product(pid)
                p_name = clean_name(p.get('name_ar', p.get('name_en', 'منتج محذوف'))) if p else 'منتج محذوف'
                price = float(p.get('price', 0)) if p else 0
            
            subtotal = price * qty
            total_spent += subtotal
            
            lines.append(
                f"#{i}\n"
                f"  التاريخ    : {date_str}\n"
                f"  المنتج     : {p_name}\n"
                f"  الكمية     : {qty}\n"
                f"  سعر الوحدة : ${price:.2f}\n"
                f"  الإجمالي   : ${subtotal:.2f}\n"
                f"  {'─' * 40}"
            )
        except Exception as e:
            lines.append(f"#{i} - خطأ: {e}\n{'─' * 40}")
    
    content = (
        f"=== سجل مشتريات المستخدم ===\n"
        f"المستخدم : {uname}\n"
        f"الـ ID    : {target_uid}\n"
        f"الرصيد   : ${balance:.2f}\n"
        f"عدد الطلبات : {len(recs)}\n"
        f"إجمالي الإنفاق : ${total_spent:.2f}\n"
        f"{'=' * 50}\n\n"
        + "\n".join(lines)
    )
    
    try:
        f = io.BytesIO(content.encode('utf-8'))
        f.name = f"purchases_{target_uid}.txt"
        bot.send_document(
            call.message.chat.id, f,
            caption=(
                f"🛍 <b>مشتريات المستخدم</b>\n"
                f"👤 {uname} | <code>{target_uid}</code>\n"
                f"📊 العدد: {len(recs)}\n"
                f"💰 الإجمالي: ${total_spent:.2f}"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        bot.send_message(call.message.chat.id, f"❌ خطأ: {e}")


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

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_full_hist_"))
@admin_required
def ad_full_history_handler(call):
    """📥 تحميل السجل الكامل (مشتريات + إيداعات)"""
    bot.answer_callback_query(call.id, "⏳ جاري تجهيز الملف...")
    target_uid = int(call.data.replace("ad_full_hist_", ""))
    u = get_user_data_full(target_uid)
    uname = f"@{u['username']}" if u and u.get('username') else "بدون"
    balance = float(u.get('balance', 0)) if u else 0

    # المشتريات
    orders = list(db.orders.find({'user_id': target_uid}).sort('_id', 1))
    total_spent = 0.0
    buy_lines = []
    for i, r in enumerate(orders, 1):
        try:
            date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
            pid = str(r.get('product_id', ''))
            qty = int(r.get('quantity', 1))
            if pid in ['GitHub_Student', 'Gemini_Activation']:
                p_name = pid.replace('_', ' '); price = 0.0
            else:
                p = find_product(pid)
                p_name = clean_name(p.get('name_ar', p.get('name_en', 'محذوف'))) if p else 'محذوف'
                price = float(p.get('price', 0)) if p else 0.0
            subtotal = price * qty; total_spent += subtotal
            
            # نجمع كل الأكواد من هذا الطلب
            # الطريقة 1: code_delivered مباشرة
            single_code = r.get('code_delivered', '')
            # الطريقة 2: نبحث عن كل الأكواد في orders بنفس order_id
            all_codes = []
            order_id = r.get('order_id') or str(r.get('_id', ''))
            if order_id:
                code_recs = list(db.orders.find({
                    'order_id': order_id,
                    'user_id': target_uid
                }))
                if len(code_recs) > 1:
                    all_codes = [c.get('code_delivered', '') for c in code_recs if c.get('code_delivered')]
            
            if not all_codes and single_code:
                all_codes = [single_code]
            
            codes_text = ""
            if all_codes:
                codes_text = "    الأكواد/الحسابات المُسلَّمة:\n"
                for j, code in enumerate(all_codes, 1):
                    codes_text += f"      [{j}] {code}\n"
            
            buy_lines.append(
                f"  #{i}\n"
                f"    التاريخ  : {date_str}\n"
                f"    المنتج   : {p_name}\n"
                f"    الكمية   : {qty}\n"
                f"    السعر    : ${price:.2f} × {qty} = ${subtotal:.2f}\n"
                f"{codes_text}"
            )
        except Exception as e:
            buy_lines.append(f"  #{i} - خطأ: {e}\n")

    # الإيداعات
    deposits = list(db.used_transactions.find({'user_id': target_uid}).sort('_id', 1))
    total_deposited = 0.0
    dep_lines = []
    for i, r in enumerate(deposits, 1):
        try:
            date_str = r['_id'].generation_time.strftime('%Y-%m-%d %H:%M:%S')
            amount = float(r.get('amount', 0))
            method = r.get('method', 'غير محدد')
            tx_id = r.get('transaction_id', '-')
            total_deposited += amount
            m = method.lower()
            if 'admin' in m or 'gift' in m or 'هدية' in m or 'manual' in m:
                source = '🎁 هدية من الأدمن'
            elif 'auto' in m or 'تلقائي' in m:
                source = '⚡ تلقائي'
            elif 'stars' in m: source = '⭐ Telegram Stars'
            elif 'binance' in m: source = '🟡 Binance Pay'
            elif 'ltc' in m: source = '🔵 LTC'
            elif 'ton' in m: source = '💎 TON'
            elif 'usdt' in m: source = '🟢 USDT'
            else: source = f'💳 {method}'
            dep_lines.append(
                f"  #{i}\n"
                f"    التاريخ : {date_str}\n"
                f"    المبلغ  : ${amount:.2f}\n"
                f"    المصدر  : {source}\n"
                f"    رقم العملية: {tx_id}\n"
            )
        except Exception as e:
            dep_lines.append(f"  #{i} - خطأ: {e}\n")

    sep = "=" * 55
    content = (
        f"{sep}\n"
        f"           السجل الكامل للمستخدم\n"
        f"{sep}\n"
        f"المستخدم      : {uname}\n"
        f"الـ ID         : {target_uid}\n"
        f"الرصيد الحالي : ${balance:.2f}\n"
        f"الحالة         : {'محظور 🚫' if u and u.get('is_banned') else 'نشط ✅'}\n"
        f"{sep}\n\n"
        f"ملخص مالي:\n"
        f"  إجمالي الإيداعات : ${total_deposited:.2f}\n"
        f"  إجمالي الإنفاق   : ${total_spent:.2f}\n"
        f"  الرصيد الحالي    : ${balance:.2f}\n\n"
        f"{sep}\n"
        f"  المشتريات ({len(orders)} طلب | إجمالي ${total_spent:.2f})\n"
        f"{sep}\n"
        f"{''.join(buy_lines) or '  لا توجد مشتريات'}\n\n"
        f"{sep}\n"
        f"  الإيداعات ({len(deposits)} عملية | إجمالي ${total_deposited:.2f})\n"
        f"{sep}\n"
        f"{''.join(dep_lines) or '  لا توجد إيداعات'}\n\n"
        f"{sep}\n"
        f"تصدير: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"{sep}\n"
    )
    try:
        f = io.BytesIO(content.encode('utf-8'))
        f.name = f"full_history_{target_uid}.txt"
        bot.send_document(
            call.message.chat.id, f,
            caption=(
                f"📥 <b>السجل الكامل</b>\n\n"
                f"👤 {uname} | <code>{target_uid}</code>\n"
                f"💰 الرصيد: <b>${balance:.2f}</b>\n"
                f"🛍 مشتريات: <b>{len(orders)}</b> = <b>${total_spent:.2f}</b>\n"
                f"💳 إيداعات: <b>{len(deposits)}</b> = <b>${total_deposited:.2f}</b>"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        bot.send_message(call.message.chat.id, f"❌ خطأ: {e}")


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
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("🔄 تعويض (بدون نوت)", callback_data=f"ad_gift_comp_{target_uid}"),
        InlineKeyboardButton("💰 إضافة عادية (مع نوت)", callback_data=f"ad_gift_note_{target_uid}"),
        InlineKeyboardButton("❌ إلغاء", callback_data=f"ad_u_det_{target_uid}")
    )
    bot.send_message(call.message.chat.id,
        "💰 <b>اختر نوع تعديل الرصيد:</b>\n\n"
        "🔄 <b>تعويض</b> — يُضاف مباشرة بدون ملاحظة\n"
        "💰 <b>إضافة عادية</b> — تكتب نوت (مثل: hash أو order ID)",
        parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_gift_comp_"))
@admin_required
def ad_gift_comp_amount(call):
    bot.answer_callback_query(call.id)
    target_uid = int(call.data.replace("ad_gift_comp_", ""))
    msg = bot.send_message(call.message.chat.id,
        "🔄 <b>تعويض — أرسل المبلغ:</b>\n<i>(سالب للخصم، موجب للإضافة)</i>",
        parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_gift_comp_exec, target_uid)

def ad_gift_comp_exec(message, target_uid):
    try:
        val = float(message.text.strip())
        db.users.update_one({'user_id': target_uid}, {'$inc': {'balance': val}})
        u = get_user_data_full(target_uid)
        new_bal = round(float(u.get('balance', 0)), 2) if u else 0.0
        # حفظ السجل في قاعدة البيانات
        import datetime as _dt
        db.balance_logs.insert_one({
            'user_id': target_uid,
            'type': 'compensation',
            'amount': val,
            'note': '',
            'new_balance': new_bal,
            'by_admin': message.from_user.id,
            'date': _dt.datetime.utcnow()
        })
        bot.send_message(message.chat.id, f"✅ تم إضافة تعويض <b>${val:.2f}</b> بنجاح.", parse_mode="HTML")
        try: notify_balance_gift(target_uid, val, note='', gift_type='compensation')
        except Exception as _e: logger.debug(f"gift notify err: {_e}")
        show_user_admin_profile(message.chat.id, target_uid)
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ خطأ: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_gift_note_"))
@admin_required
def ad_gift_note_amount(call):
    bot.answer_callback_query(call.id)
    target_uid = int(call.data.replace("ad_gift_note_", ""))
    msg = bot.send_message(call.message.chat.id,
        "💰 <b>إضافة عادية — أرسل المبلغ:</b>\n<i>(سالب للخصم، موجب للإضافة)</i>",
        parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_gift_note_step2, target_uid)

def ad_gift_note_step2(message, target_uid):
    try:
        val = float(message.text.strip())
        msg = bot.send_message(message.chat.id,
            "📝 <b>أرسل الملاحظة (نوت):</b>\n<i>مثال: hash أو order ID أو أي سبب</i>",
            parse_mode="HTML")
        bot.register_next_step_handler(msg, ad_gift_note_exec, target_uid, val)
    except:
        bot.send_message(message.chat.id, "❌ خطأ في الرقم.")

def ad_gift_note_exec(message, target_uid, val):
    try:
        note = message.text.strip()
        db.users.update_one({'user_id': target_uid}, {'$inc': {'balance': val}})
        u = get_user_data_full(target_uid)
        new_bal = round(float(u.get('balance', 0)), 2) if u else 0.0
        import datetime as _dt
        db.balance_logs.insert_one({
            'user_id': target_uid,
            'type': 'manual',
            'amount': val,
            'note': note,
            'new_balance': new_bal,
            'by_admin': message.from_user.id,
            'date': _dt.datetime.utcnow()
        })
        bot.send_message(message.chat.id,
            f"✅ تم تعديل الرصيد <b>${val:.2f}</b>\n📝 النوت: <code>{note}</code>",
            parse_mode="HTML")
        try: notify_balance_gift(target_uid, val, note=note, gift_type='manual')
        except Exception as _e: logger.debug(f"gift notify err: {_e}")
        show_user_admin_profile(message.chat.id, target_uid)
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ خطأ: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_view_ballogs_"))
@admin_required
def ad_view_balance_logs(call):
    """عرض سجل التعديلات المالية لمستخدم معين (للأدمن)"""
    bot.answer_callback_query(call.id)
    parts = call.data.replace("ad_view_ballogs_", "").rsplit("_", 1)
    target_uid = int(parts[0])
    page = int(parts[1]) if len(parts) > 1 else 0
    per_page = 5
    logs = list(db.balance_logs.find({'user_id': target_uid}).sort('_id', -1).skip(page * per_page).limit(per_page))
    total = db.balance_logs.count_documents({'user_id': target_uid})
    if not logs:
        bot.answer_callback_query(call.id, "لا يوجد سجل بعد.", show_alert=True)
        return
    import datetime as _dt
    txt = f"📋 <b>سجل التعديلات المالية للمستخدم</b> <code>{target_uid}</code>\n\n"
    for log in logs:
        t = log.get('type', 'manual')
        amt = log.get('amount', 0)
        note = log.get('note', '')
        date = log.get('date', '')
        sign = "+" if amt >= 0 else ""
        type_label = "🔄 تعويض" if t == 'compensation' else "💰 إضافة عادية"
        date_str = date.strftime("%Y-%m-%d %H:%M") if hasattr(date, 'strftime') else str(date)[:16]
        txt += f"{type_label}\n💵 <b>{sign}${amt:.2f}</b> | 🕐 {date_str}\n"
        if note:
            txt += f"📝 النوت: <code>{note}</code>\n"
        txt += "──────────\n"
    markup = InlineKeyboardMarkup(row_width=2)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"ad_view_ballogs_{target_uid}_{page-1}"))
    if (page + 1) * per_page < total:
        nav.append(InlineKeyboardButton("➡️ التالي", callback_data=f"ad_view_ballogs_{target_uid}_{page+1}"))
    if nav: markup.add(*nav)
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"ad_u_det_{target_uid}"))
    try:
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except:
        bot.send_message(call.message.chat.id, txt, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("my_ballogs_"))
def user_view_balance_logs(call):
    """الزبون يشوف سجل التعديلات على رصيده"""
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    if is_user_banned(uid): return
    page = int(call.data.replace("my_ballogs_", ""))
    per_page = 5
    logs = list(db.balance_logs.find({'user_id': uid}).sort('_id', -1).skip(page * per_page).limit(per_page))
    total = db.balance_logs.count_documents({'user_id': uid})
    l = get_lang(uid)
    if not logs:
        bot.answer_callback_query(call.id, "لا يوجد سجل بعد." if l == 'ar' else "No records yet.", show_alert=True)
        return
    txt = "📋 <b>سجل تعديلات رصيدك</b>\n\n" if l == 'ar' else "📋 <b>Your Balance Adjustment History</b>\n\n"
    for log in logs:
        t = log.get('type', 'manual')
        amt = log.get('amount', 0)
        note = log.get('note', '')
        date = log.get('date', '')
        sign = "+" if amt >= 0 else ""
        date_str = date.strftime("%Y-%m-%d %H:%M") if hasattr(date, 'strftime') else str(date)[:16]
        if l == 'ar':
            type_label = "🔄 تعويض" if t == 'compensation' else "💰 إضافة"
            txt += f"{type_label} | <b>{sign}${amt:.2f}</b> | 🕐 {date_str}\n"
            if note: txt += f"📝 <code>{note}</code>\n"
        else:
            type_label = "🔄 Compensation" if t == 'compensation' else "💰 Manual Add"
            txt += f"{type_label} | <b>{sign}${amt:.2f}</b> | 🕐 {date_str}\n"
            if note: txt += f"📝 <code>{note}</code>\n"
        txt += "──────────\n"
    markup = InlineKeyboardMarkup(row_width=2)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️" , callback_data=f"my_ballogs_{page-1}"))
    if (page + 1) * per_page < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"my_ballogs_{page+1}"))
    if nav: markup.add(*nav)
    back_label = "🔙 رجوع" if l == 'ar' else "🔙 Back"
    markup.add(InlineKeyboardButton(back_label, callback_data="open_profile"))
    try:
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except:
        bot.send_message(call.message.chat.id, txt, reply_markup=markup, parse_mode="HTML")

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
    msg = bot.send_message(call.from_user.id, "👤 <b>Send User ID or @username:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, ad_gift_val)

def ad_gift_val(message):
    target = message.text.strip()
    if target.startswith('@') or not target.replace('-', '').isdigit():
        u = db.users.find_one({'username': target.replace('@', '').lower()})
    else: u = get_user_data_full(int(target))
    if u:
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("🔄 تعويض (بدون نوت)", callback_data=f"ad_gift_comp_{u['user_id']}"),
            InlineKeyboardButton("💰 إضافة عادية (مع نوت)", callback_data=f"ad_gift_note_{u['user_id']}")
        )
        bot.send_message(message.from_user.id,
            f"💰 <b>اختر نوع التعديل للمستخدم {u.get('name','')}</b>",
            parse_mode="HTML", reply_markup=markup)
    else: bot.send_message(message.chat.id, "❌ Not found.")

def ad_gift_finish(message, tid):
    try:
        val = float(message.text)
        db.users.update_one({'user_id': tid}, {'$inc': {'balance': val}})
        bot.send_message(message.from_user.id, "✅ Done.")
        try: notify_balance_gift(tid, val)
        except Exception as _e: logger.debug(f"gift notify err: {_e}")
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
    msg = bot.send_message(call.from_user.id,
        "📢 <b>أرسل رسالة البرودكاست:</b>\n"
        "<i>تدعم: نص، صور، فيديو، ملفات، إيموجي مميز، تنسيقات HTML</i>",
        parse_mode="HTML")
    bot.register_next_step_handler(msg, admin_bc_pick_product)

def admin_bc_pick_product(message):
    """الخطوة 2: اختيار منتج أو إرسال للكل"""
    uid = message.from_user.id
    _bc_pending[uid] = {
        'msg_id': message.message_id,
        'chat_id': message.chat.id,
        'product_id': None  # None = للكل
    }
    products = list(db.products.find({'is_hidden': {'$ne': True}}, {'_id': 1, 'name_ar': 1, 'name_en': 1}))
    total = db.users.count_documents({})
    markup = InlineKeyboardMarkup(row_width=1)
    for p in products[:20]:
        pid = str(p['_id'])
        name = clean_name(p.get('name_ar') or p.get('name_en', ''))[:30]
        markup.add(InlineKeyboardButton(f"📦 {name}", callback_data=f"bc_pick_{pid}"))
    markup.add(InlineKeyboardButton(f"✅ إرسال للكل ({total})", callback_data="bc_pick_all"))
    markup.add(InlineKeyboardButton("❌ إلغاء", callback_data="admin_panel_main"))
    bot.send_message(uid,
        "📦 <b>اختر منتجاً للإرسال لمشتريه فقط، أو اضغط إرسال للكل:</b>",
        parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("bc_pick_"))
@admin_required
def admin_bc_confirm(call):
    """الخطوة 3: تأكيد"""
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    pending = _bc_pending.get(uid)
    if not pending:
        bot.answer_callback_query(call.id, "❌ انتهت الجلسة.", show_alert=True); return

    target = call.data.replace("bc_pick_", "")
    if target == "all":
        target_ids = [u['user_id'] for u in db.users.find({}, {'user_id': 1})]
        label = f"👥 الكل ({len(target_ids)})"
        pending['product_id'] = None
    else:
        # نبحث بكل أشكال الـ product_id (string / int / float)
        pid_variants = [target]
        if target.isdigit():
            pid_variants.append(int(target))
            pid_variants.append(float(target))
        target_ids = list(db.orders.distinct('user_id', {'product_id': {'$in': pid_variants}}))
        p = find_product(target)
        p_name = clean_name(p.get('name_ar') or p.get('name_en', '')) if p else target
        label = f"📦 مشتري {p_name} ({len(target_ids)})"
        pending['product_id'] = target

    pending['target_ids'] = target_ids
    pending['label'] = label

    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("✅ موافق", callback_data="bc_go"),
        InlineKeyboardButton("❌ لا", callback_data="admin_panel_main")
    )
    bot.send_message(uid,
        f"📋 سيُرسل البرودكاست لـ <b>{label}</b>\n\nموافق؟",
        parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "bc_go")
@admin_required
def admin_bc_exe(call):
    """تنفيذ البرودكاست في thread خلفية"""
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    pending = _bc_pending.pop(uid, None)
    if not pending:
        bot.answer_callback_query(call.id, "❌ انتهت الجلسة.", show_alert=True); return

    src_msg_id = pending['msg_id']
    src_chat_id = pending['chat_id']
    target_ids  = pending.get('target_ids', [])
    label       = pending.get('label', '?')

    bot.send_message(uid,
        f"📢 <b>بدأ البرودكاست!</b> ({label})\n"
        f"⏳ البوت يعمل بشكل طبيعي. سيصلك تقرير عند الانتهاء.",
        parse_mode="HTML")

    def _bc_thread():
        sent = failed = blocked = 0
        for tuid in target_ids:
            try:
                bot.copy_message(tuid, src_chat_id, src_msg_id)
                sent += 1
            except Exception as e:
                err = str(e).lower()
                if any(w in err for w in ['blocked', 'deactivated', 'not found', 'kicked']):
                    blocked += 1
                else:
                    failed += 1
            time.sleep(0.035)
        try:
            bot.send_message(uid,
                f"✅ <b>اكتمل البرودكاست!</b>\n\n"
                f"🎯 {label}\n"
                f"📤 أُرسل: <b>{sent}</b>\n"
                f"🚫 محظور: <b>{blocked}</b>\n"
                f"❌ فشل: <b>{failed}</b>",
                parse_mode="HTML")
        except: pass

    threading.Thread(target=_bc_thread, daemon=True, name="bc_thread").start()

_bc_pending = {}
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
# 📂 14.6 نظام الكتالوجات (التصنيفات) — إدارة الأدمن
# ============================================================

@bot.callback_query_handler(func=lambda call: call.data == "ad_catalog_list")
@admin_required
def ad_catalog_list(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    catalogs = list(db.catalogs.find().sort('order', 1))
    
    txt = "📂 <b>إدارة الكتالوجات</b>\n\n"
    if not catalogs:
        txt += "📭 لا يوجد كتالوجات بعد.\nأنشئ كتالوج وأضف فيه منتجات.\n\n"
        txt += "💡 <i>لو ما في كتالوجات، المنتجات تظهر عادي بدون تصنيف.</i>"
    else:
        for i, cat in enumerate(catalogs, 1):
            emoji = cat.get('emoji', '📁')
            emoji_id = cat.get('emoji_id')
            name = cat.get('name_ar', '')
            count = len(cat.get('product_ids') or [])
            if emoji_id:
                txt += f'{i}. <tg-emoji emoji-id="{emoji_id}">{emoji}</tg-emoji> <b>{name}</b> — {count} منتج\n'
            else:
                txt += f"{i}. {emoji} <b>{name}</b> — {count} منتج\n"
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("➕ إنشاء كتالوج جديد", callback_data="ad_cat_create"))
    
    for cat in catalogs:
        cat_id = str(cat['_id'])
        emoji = cat.get('emoji', '📁')
        emoji_id = cat.get('emoji_id')
        name = cat.get('name_ar', '')[:20]
        btn_kwargs = {'text': f"✏️ {emoji} {name}", 'callback_data': f"ad_cat_edit_{cat_id}"}
        if emoji_id:
            btn_kwargs['icon_custom_emoji_id'] = emoji_id
        markup.add(CustomInlineButton(**btn_kwargs))
    
    markup.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel_main"))
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass


# ═══ إنشاء كتالوج ═══
@bot.callback_query_handler(func=lambda call: call.data == "ad_cat_create")
@admin_required
def ad_cat_create(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    msg = bot.send_message(call.message.chat.id,
        "📂 <b>Create new catalog</b>\n\n"
        "Send the catalog name:",
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_cat_create_step2)


def ad_cat_create_step2(message):
    name = message.text.strip() if message.text else ''
    if not name:
        bot.send_message(message.chat.id, "❌ Send a name."); return
    
    msg = bot.send_message(message.chat.id,
        f"✅ Name: <b>{name}</b>\n\n"
        "Now send the emoji for this catalog:",
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_cat_create_step3, name)


def ad_cat_create_step3(message, name):
    emoji_id = None
    emoji_text = message.text.strip() if message.text else '📁'
    if message.entities:
        for ent in message.entities:
            if ent.type == 'custom_emoji':
                emoji_id = str(ent.custom_emoji_id)
                break
    
    max_order = 0
    last = db.catalogs.find_one(sort=[('order', -1)])
    if last: max_order = last.get('order', 0)
    
    cat_doc = {
        'emoji': emoji_text,
        'name_ar': name,
        'name_en': name,
        'product_ids': [],
        'order': max_order + 1
    }
    if emoji_id:
        cat_doc['emoji_id'] = emoji_id
    
    db.catalogs.insert_one(cat_doc)
    
    display = f'<tg-emoji emoji-id="{emoji_id}">{emoji_text}</tg-emoji>' if emoji_id else emoji_text
    bot.send_message(message.chat.id,
        f"✅ <b>Catalog created!</b>\n\n{display} <b>{name}</b>\n\nNow add products from the edit menu.",
        parse_mode="HTML"
    )


# ═══ تعديل كتالوج ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cat_edit_"))
@admin_required
def ad_cat_edit(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    cat_id = call.data.replace("ad_cat_edit_", "")
    from bson import ObjectId
    try:
        cat = db.catalogs.find_one({'_id': ObjectId(cat_id)})
    except:
        cat = None
    if not cat:
        bot.answer_callback_query(call.id, "❌ الكتالوج غير موجود", show_alert=True); return
    
    emoji = cat.get('emoji', '📁')
    emoji_id = cat.get('emoji_id')
    name_ar = cat.get('name_ar', '')
    name_en = cat.get('name_en', '')
    prod_ids = cat.get('product_ids') or []
    
    display_emoji = f'<tg-emoji emoji-id="{emoji_id}">{emoji}</tg-emoji>' if emoji_id else emoji
    txt = f"✏️ <b>Edit Catalog:</b> {display_emoji} {name_en or name_ar}\n"
    if emoji_id:
        txt += f"⭐ Premium Emoji: ✅\n"
    txt += f"📦 Products: <b>{len(prod_ids)}</b>\n\n"
    
    if prod_ids:
        items = []
        for pid in prod_ids:
            p = find_product(str(pid))
            if p:
                items.append(p)
        items.sort(key=lambda p: clean_name(p.get('name_en', p.get('name_ar', ''))).lower())
        txt += "<b>Products:</b>\n"
        for p in items:
            txt += f"  • {clean_name(p.get('name_en', p.get('name_ar', '')))}\n"
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("➕ Add Products", callback_data=f"ad_cat_addp_{cat_id}"))
    markup.add(InlineKeyboardButton("↩️ Move to Regular (no catalog)", callback_data=f"ad_cat_remp_{cat_id}"))
    markup.add(InlineKeyboardButton("✏️ Edit Name/Emoji", callback_data=f"ad_cat_rename_{cat_id}"))
    markup.add(InlineKeyboardButton("🔼 Move Up", callback_data=f"ad_cat_up_{cat_id}"))
    markup.add(InlineKeyboardButton("🔽 Move Down", callback_data=f"ad_cat_down_{cat_id}"))
    markup.add(InlineKeyboardButton("🗑 Delete Catalog", callback_data=f"ad_cat_del_{cat_id}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="ad_catalog_list"))
    
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass


# ═══ إضافة منتجات للكتالوج ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("p_set_first_"))
@admin_required
def p_set_first(call):
    """يجعل المنتج أول شيء في مجلده"""
    try: bot.answer_callback_query(call.id)
    except: pass
    raw = call.data.replace("p_set_first_", "")
    parts = raw.rsplit("_", 1)
    pid = parts[0]
    cat_id = parts[1] if len(parts) > 1 else None
    if not cat_id:
        bot.answer_callback_query(call.id, "❌ لا يوجد مجلد محدد.", show_alert=True)
        return
    from bson import ObjectId
    try:
        db.catalogs.update_one(
            {'_id': ObjectId(cat_id)},
            {'$pull': {'product_ids': pid}}
        )
        db.catalogs.update_one(
            {'_id': ObjectId(cat_id)},
            {'$push': {'product_ids': {'$each': [pid], '$position': 0}}}
        )
        bot.answer_callback_query(call.id, "✅ أصبح أول المنتجات في المجلد!", show_alert=True)
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ {e}", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cat_addp_"))
@admin_required
def ad_cat_addp(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    try:
        cat_id = call.data.replace("ad_cat_addp_", "")
        from bson import ObjectId
        cat = db.catalogs.find_one({'_id': ObjectId(cat_id)})
        if not cat: return
        
        # نجمع كل المنتجات الموجودة في أي كتالوج
        all_catalog_pids = set()
        for c in db.catalogs.find():
            all_catalog_pids.update([str(x) for x in (c.get('product_ids') or [])])
        
        prods = list(db.products.find())
        prods.sort(key=lambda p: clean_name(p.get('name_en', p.get('name_ar', ''))).lower())
        
        markup = InlineKeyboardMarkup(row_width=1)
        found = False
        for p in prods:
            pid = str(p.get('id', str(p.get('_id', ''))))
            if pid in all_catalog_pids:
                continue
            found = True
            name = clean_name(p.get('name_en', p.get('name_ar', '')))[:30]
            emoji_id = p.get('custom_emoji_id')
            btn_kwargs = {'text': f"➕ {name}", 'callback_data': f"ad_cat_doadd_{cat_id}_{pid}"}
            if emoji_id:
                btn_kwargs['icon_custom_emoji_id'] = emoji_id
            markup.add(CustomInlineButton(**btn_kwargs))
        
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"ad_cat_edit_{cat_id}"))
        
        txt = "➕ <b>Choose a product to add:</b>" if found else "✅ <b>All products are already in catalogs.</b>"
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        logger.exception("Error in ad_cat_addp:")
        try: bot.send_message(call.message.chat.id, f"❌ Error loading products: {e}")
        except: pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cat_doadd_"))
@admin_required
def ad_cat_doadd(call):
    parts = call.data.replace("ad_cat_doadd_", "").split("_", 1)
    cat_id = parts[0]
    pid = parts[1] if len(parts) > 1 else ""
    from bson import ObjectId
    
    # 🆕 Sync catalog_id to product
    p = find_product(pid)
    if p:
        db.products.update_one({'_id': p['_id']}, {'$set': {'catalog_id': str(cat_id)}})
        
    # نضيف في البداية: نحذف لو موجود ثم نضيف في أول القائمة
    db.catalogs.update_one(
        {'_id': ObjectId(cat_id)},
        {'$pull': {'product_ids': pid}}
    )
    db.catalogs.update_one(
        {'_id': ObjectId(cat_id)},
        {'$push': {'product_ids': {'$each': [pid], '$position': 0}}}
    )
    bot.answer_callback_query(call.id, "✅ Added as first!", show_alert=True)
    call.data = f"ad_cat_addp_{cat_id}"
    ad_cat_addp(call)


# ═══ إزالة منتج من كتالوج ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cat_remp_"))
@admin_required
def ad_cat_remp(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    try:
        cat_id = call.data.replace("ad_cat_remp_", "")
        from bson import ObjectId
        cat = db.catalogs.find_one({'_id': ObjectId(cat_id)})
        if not cat: return
        
        prod_ids = cat.get('product_ids') or []
        # نجيب بيانات المنتجات ونرتبها أبجدياً
        items = []
        for pid in prod_ids:
            p = find_product(str(pid))
            if p:
                items.append((pid, p))
        items.sort(key=lambda x: clean_name(x[1].get('name_en', x[1].get('name_ar', ''))).lower())
        
        markup = InlineKeyboardMarkup(row_width=1)
        for pid, p in items:
            name = clean_name(p.get('name_en', p.get('name_ar', '')))[:30]
            emoji_id = p.get('custom_emoji_id')
            btn_kwargs = {'text': f"↩️ {name}", 'callback_data': f"ad_cat_dorem_{cat_id}_{pid}"}
            if emoji_id:
                btn_kwargs['icon_custom_emoji_id'] = emoji_id
            markup.add(CustomInlineButton(**btn_kwargs))
        
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"ad_cat_edit_{cat_id}"))
        bot.edit_message_text("↩️ <b>Choose a product to move back to regular:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        logger.exception("Error in ad_cat_remp:")
        try: bot.send_message(call.message.chat.id, f"❌ Error loading products to remove: {e}")
        except: pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cat_dorem_"))
@admin_required
def ad_cat_dorem(call):
    parts = call.data.replace("ad_cat_dorem_", "").split("_", 1)
    cat_id = parts[0]
    pid = parts[1] if len(parts) > 1 else ""
    from bson import ObjectId
    
    # 🆕 Sync catalog_id to product (set to None)
    p = find_product(pid)
    if p:
        db.products.update_one({'_id': p['_id']}, {'$set': {'catalog_id': None}})
        
    db.catalogs.update_one(
        {'_id': ObjectId(cat_id)},
        {'$pull': {'product_ids': pid}}
    )
    bot.answer_callback_query(call.id, "✅ Moved to regular!", show_alert=True)
    call.data = f"ad_cat_remp_{cat_id}"
    ad_cat_remp(call)


# ═══ تعديل اسم/رمز الكتالوج ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cat_rename_"))
@admin_required
def ad_cat_rename(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    cat_id = call.data.replace("ad_cat_rename_", "")
    msg = bot.send_message(call.message.chat.id,
        "✏️ <b>Send the new name:</b>",
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_cat_rename_step2, cat_id)


def ad_cat_rename_step2(message, cat_id):
    name = message.text.strip() if message.text else ''
    if not name:
        bot.send_message(message.chat.id, "❌ Send a name."); return
    
    msg = bot.send_message(message.chat.id,
        f"✅ Name: <b>{name}</b>\n\nNow send the new emoji:",
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, ad_cat_rename_step3, cat_id, name)


def ad_cat_rename_step3(message, cat_id, name):
    from bson import ObjectId
    emoji_id = None
    emoji_text = message.text.strip() if message.text else '📁'
    if message.entities:
        for ent in message.entities:
            if ent.type == 'custom_emoji':
                emoji_id = str(ent.custom_emoji_id)
                break
    
    update = {'emoji': emoji_text, 'name_ar': name, 'name_en': name}
    if emoji_id:
        update['emoji_id'] = emoji_id
    else:
        update['emoji_id'] = None
    
    db.catalogs.update_one({'_id': ObjectId(cat_id)}, {'$set': update})
    
    display = f'<tg-emoji emoji-id="{emoji_id}">{emoji_text}</tg-emoji>' if emoji_id else emoji_text
    bot.send_message(message.chat.id, f"✅ <b>Updated!</b> {display} {name}", parse_mode="HTML")


# ═══ ترتيب الكتالوجات ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cat_up_") or call.data.startswith("ad_cat_down_"))
@admin_required
def ad_cat_reorder(call):
    from bson import ObjectId
    direction = 'up' if call.data.startswith("ad_cat_up_") else 'down'
    cat_id = call.data.replace("ad_cat_up_", "").replace("ad_cat_down_", "")
    
    catalogs = list(db.catalogs.find().sort('order', 1))
    idx = None
    for i, c in enumerate(catalogs):
        if str(c['_id']) == cat_id:
            idx = i; break
    
    if idx is None:
        bot.answer_callback_query(call.id, "❌"); return
    
    swap_idx = idx - 1 if direction == 'up' else idx + 1
    if swap_idx < 0 or swap_idx >= len(catalogs):
        bot.answer_callback_query(call.id, "⚠️ لا يمكن التحريك أكثر", show_alert=True); return
    
    # تبديل الترتيب
    order_a = catalogs[idx].get('order', idx)
    order_b = catalogs[swap_idx].get('order', swap_idx)
    db.catalogs.update_one({'_id': catalogs[idx]['_id']}, {'$set': {'order': order_b}})
    db.catalogs.update_one({'_id': catalogs[swap_idx]['_id']}, {'$set': {'order': order_a}})
    
    bot.answer_callback_query(call.id, "✅")
    call.data = f"ad_cat_edit_{cat_id}"
    ad_cat_edit(call)


# ═══ حذف كتالوج ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("ad_cat_del_"))
@admin_required
def ad_cat_del(call):
    cat_id = call.data.replace("ad_cat_del_", "")
    from bson import ObjectId
    
    if not call.data.endswith("_confirmed"):
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("✅ نعم احذف", callback_data=f"ad_cat_del_{cat_id}_confirmed"),
            InlineKeyboardButton("❌ إلغاء", callback_data=f"ad_cat_edit_{cat_id}")
        )
        try: bot.edit_message_text("⚠️ <b>هل تريد حذف هذا الكتالوج؟</b>\n\nالمنتجات ما بتنحذف، بس بترجع بدون تصنيف.", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        except: pass
        return
    
    real_id = cat_id.replace("_confirmed", "")
    db.catalogs.delete_one({'_id': ObjectId(real_id)})
    bot.answer_callback_query(call.id, "✅ تم الحذف!", show_alert=True)
    call.data = "ad_catalog_list"
    ad_catalog_list(call)


# ============================================================
# 🔗 14.7 أزرار API في البوت
# ============================================================

@bot.callback_query_handler(func=lambda call: call.data == "open_api")
def open_api(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    if is_user_banned(uid): return
    l = get_lang(uid)

    existing = db.api_keys.find_one({'user_id': uid, 'is_active': True})
    gw = _get_api_gateway()

    if existing:
        # حساب الإحصائيات
        total_orders = db.api_orders.count_documents({'api_user_id': uid})
        total_spent = 0
        for o in db.api_orders.find({'api_user_id': uid}, {'total_price': 1}):
            total_spent += o.get('total_price', 0)
        user = get_user_data_full(uid)
        balance = user.get('balance', 0) if user else 0
        
        # عدد المنتجات المخفية
        hidden_count = db.api_hidden.count_documents({'api_user_id': uid})
        total_prods = db.products.count_documents({'is_hidden': {'$ne': True}})
        active_count = total_prods - hidden_count

        conn_code = _generate_connection_code(existing['api_key'])
        
        txt = f"🤖 <b>API Control Panel</b>\n"
        txt += f"━━━━━━━━━━━━━━━━━━━\n"
        txt += f"🟢 Status: <b>Connected</b>\n"
        txt += f"💰 Balance: <b>${balance:.2f}</b>\n"
        txt += f"📦 Products: <b>{active_count}/{total_prods}</b>\n"
        txt += f"📊 Orders: <b>{total_orders}</b> | Spent: <b>${total_spent:.2f}</b>\n"
        txt += f"━━━━━━━━━━━━━━━━━━━\n"
        if conn_code:
            txt += f"🔗 <code>{conn_code}</code>"
        else:
            txt += f"🔑 <code>{existing['api_key']}</code>"

        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("📦 Manage Products", callback_data="api_products"))
        markup.add(InlineKeyboardButton("📋 رسالة للذكاء الاصطناعي" if get_lang(uid) == 'ar' else "📋 Message for AI", callback_data="api_ai_prompt"))
        markup.add(InlineKeyboardButton("📖 How to Connect", callback_data="api_docs"))
        markup.add(InlineKeyboardButton("📜 Recent Orders", callback_data="api_orders"))
        markup.add(InlineKeyboardButton("🔄 Regenerate Key", callback_data="api_regen"))
        markup.add(InlineKeyboardButton("❌ Disable API", callback_data="api_disable"))
        markup.add(create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh"))
    else:
        txt = get_text(uid, 'api_welcome')
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🔑 Generate API Key", callback_data="api_gen"))
        markup.add(create_btn(uid, 'btn_main_menu', callback_data="main_menu_refresh"))

    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass


# ═══ إنشاء مفتاح ═══
@bot.callback_query_handler(func=lambda call: call.data == "api_gen")
def api_gen(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    if db.api_keys.find_one({'user_id': uid, 'is_active': True}):
        bot.answer_callback_query(call.id, "⚠️ You already have a key!", show_alert=True); return

    key = _generate_api_key()
    db.api_keys.insert_one({'user_id': uid, 'api_key': key, 'is_active': True, 'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), 'username': call.from_user.username or ''})

    gw = _get_api_gateway()
    conn_code = _generate_connection_code(key)
    
    if conn_code:
        # نجرب CMS أولاً
        cms = db.custom_texts.find_one({'lang': get_lang(uid), 'key': 'api_created'})
        if cms and cms.get('value'):
            try: txt = cms['value'].format(conn_code)
            except: txt = LANG['en']['api_created'].format(conn_code)
        else:
            txt = get_text(uid, 'api_created', conn_code)
    else:
        txt = f"✅ <b>API Key Generated!</b>\n\n🔑 <code>{key}</code>\n🛤 <code>/{gw}</code>\n\n⚠️ <i>Server URL not detected.</i>"
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📋 رسالة جاهزة للذكاء الاصطناعي" if get_lang(uid) == 'ar' else "📋 Ready message for AI", callback_data="api_ai_prompt"))
    markup.add(InlineKeyboardButton("🤖 Open Control Panel", callback_data="open_api"))
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass


# ═══ توثيق API ═══
@bot.callback_query_handler(func=lambda call: call.data == "api_docs")
def api_docs(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    
    existing = db.api_keys.find_one({'user_id': uid, 'is_active': True})
    conn_code = _generate_connection_code(existing['api_key']) if existing else None
    
    # نجرب CMS
    txt = get_text(uid, 'api_howto')
    if conn_code:
        txt += f"\n\n🔗 <b>Your code:</b>\n<code>{conn_code}</code>"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="open_api"))
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass


# ═══ رسالة جاهزة للذكاء الاصطناعي ═══
@bot.callback_query_handler(func=lambda call: call.data == "api_ai_prompt")
def api_ai_prompt(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    
    existing = db.api_keys.find_one({'user_id': uid, 'is_active': True})
    if not existing:
        bot.answer_callback_query(call.id, "❌", show_alert=True); return
    
    conn_code = _generate_connection_code(existing['api_key'])
    if not conn_code:
        bot.send_message(uid, "❌ Server URL not detected."); return
    
    l = get_lang(uid)
    
    if l == 'ar':
        bot.send_message(uid,
            "📋 <b>انسخ الرسالة التالية كاملة وأرسلها لأي ذكاء اصطناعي:</b>",
            parse_mode="HTML"
        )
        
        prompt = f"""أبي تسوي لي بوت تيليجرام بايثون (pyTelegramBotAPI + MongoDB) يشتغل كمتجر.

البوت يتصل بمتجر خارجي عبر كود اتصال مشفّر. الكود فيه كل المعلومات (الرابط + المفتاح).

كود الاتصال:
{conn_code}

كيف تفك الكود:
import base64, json
data = json.loads(base64.b64decode(code.replace("conn_", "")))
API_KEY = data["k"]
API_URL = data["u"]

كل الطلبات تكون:
GET/POST {{API_URL}}/endpoint
مع Header: Authorization: Bearer {{API_KEY}}

⚠️ مهم: لما ترسل أي رسالة فيها أسماء منتجات، استخدم parse_mode="HTML" عشان الإيموجي المميز (Premium Emoji) يبان.

الـ Endpoints:

GET /products → قائمة المنتجات
{{"success": true, "products": [{{
  "id": "1",
  "name_ar": "نتفلكس",
  "name_en": "Netflix",
  "name_ar_html": "<tg-emoji emoji-id=\\"5123\\">✨</tg-emoji> نتفلكس",
  "name_en_html": "<tg-emoji emoji-id=\\"5123\\">✨</tg-emoji> Netflix",
  "custom_emoji_id": "5123",
  "has_premium_emoji": true,
  "desc_ar": "وصف فيه إيموجي مميز جاهز داخل النص نفسه",
  "desc_en": "description with premium emoji ready inside the text",
  "desc_has_premium_emoji": true,
  "desc_emoji_ids": ["6677"],
  "all_emoji_ids": ["5123", "6677"],
  "your_price": 3.5,
  "stock": 45,
  "is_manual": false
}}]}}

🌟 كيف تعرض المنتج (الاسم + الوصف) مع الإيموجي المميز:
- الاسم في الرسائل: استخدم name_ar_html أو name_en_html (فيه وسم tg-emoji جاهز) مع parse_mode="HTML"
- الاسم في الأزرار (InlineKeyboardButton): استخدم name_ar العادي، ولو تبي الإيموجي داخل الزر أضف icon_custom_emoji_id = custom_emoji_id
- 📝 الوصف مهم: حقول desc_ar و desc_en تحتوي أصلاً على وسوم <tg-emoji> جاهزة داخل النص! اعرضهم مباشرة مع parse_mode="HTML" والإيموجي المميز بيبان داخل الوصف تلقائياً
- لو has_premium_emoji = false استخدم الاسم العادي بدون HTML
- 🌐 لو تبني موقع (مو بوت تيليجرام): استخدم all_emoji_ids (كل معرّفات الإيموجي في المنتج: الاسم + الوصف) لجلب صور الإيموجي من تيليجرام

مثال للأزرار مع الإيموجي:
btn = InlineKeyboardButton(text=p["name_ar"], callback_data=f"buy_{{p['id']}}")
if p.get("custom_emoji_id"):
    btn.icon_custom_emoji_id = p["custom_emoji_id"]

مثال للرسائل:
text = f"{{p['name_ar_html']}}\\n💰 السعر: ${{p['your_price']}}"
bot.send_message(chat_id, text, parse_mode="HTML")

GET /balance → الرصيد
{{"success": true, "balance": 25.00}}

POST /purchase → شراء
Body: {{"product_id":"1","qty":1,"buyer_info":"@customer"}}
نجاح: {{"success":true,"codes":["the_code"],"total_price":3.5,"new_balance":21.5}}
فشل: {{"error":"Insufficient balance"}} أو {{"error":"Not enough stock"}}

GET /orders → الطلبات السابقة

المطلوب في البوت:
- /start → زر المنتجات + زر الرصيد
- المنتجات تجي من API وتظهر كأزرار مع الإيموجي المميز (استخدم icon_custom_emoji_id)
- في رسالة تفاصيل المنتج استخدم name_ar_html مع parse_mode="HTML"
- شراء → POST /purchase → الأكواد ترسل للعميل
- لو الرصيد قليل → رسالة خطأ
- لو خلص الستوك → رسالة خطأ
- التخزين في MongoDB (كود الاتصال يتحفظ)
- لا متغيرات في الكود — كل شيء من MongoDB
- أمر /api للأدمن يلصق فيه كود الاتصال"""
    else:
        bot.send_message(uid,
            "📋 <b>Copy the following message and send it to any AI:</b>",
            parse_mode="HTML"
        )
        
        prompt = f"""Build me a Python Telegram bot (pyTelegramBotAPI + MongoDB) that works as a store.

The bot connects to an external store via an encrypted connection code. The code contains all info (URL + API key).

Connection code:
{conn_code}

How to decode:
import base64, json
data = json.loads(base64.b64decode(code.replace("conn_", "")))
API_KEY = data["k"]
API_URL = data["u"]

All requests:
GET/POST {{API_URL}}/endpoint
Header: Authorization: Bearer {{API_KEY}}

⚠️ IMPORTANT: When sending messages with product names, use parse_mode="HTML" so premium emojis render.

Endpoints:

GET /products → Product list
{{"success": true, "products": [{{
  "id": "1",
  "name_ar": "نتفلكس",
  "name_en": "Netflix",
  "name_ar_html": "<tg-emoji emoji-id=\\"5123\\">✨</tg-emoji> نتفلكس",
  "name_en_html": "<tg-emoji emoji-id=\\"5123\\">✨</tg-emoji> Netflix",
  "custom_emoji_id": "5123",
  "has_premium_emoji": true,
  "desc_ar": "وصف فيه إيموجي مميز جاهز داخل النص نفسه",
  "desc_en": "description with premium emoji ready inside the text",
  "desc_has_premium_emoji": true,
  "desc_emoji_ids": ["6677"],
  "all_emoji_ids": ["5123", "6677"],
  "your_price": 3.5,
  "stock": 45,
  "is_manual": false
}}]}}

🌟 How to display products (name + description) with premium emojis:
- Name in messages: use name_en_html or name_ar_html (already has the tg-emoji tag) with parse_mode="HTML"
- Name in buttons (InlineKeyboardButton): use the plain name_en, and to show the emoji inside the button add icon_custom_emoji_id = custom_emoji_id
- 📝 IMPORTANT description: desc_ar and desc_en ALREADY contain ready <tg-emoji> tags inside the text! Show them directly with parse_mode="HTML" and the premium emoji renders inside the description automatically
- If has_premium_emoji = false, use the plain name without HTML
- 🌐 If building a website (not a Telegram bot): use all_emoji_ids (all premium emoji IDs in the product: name + description) to fetch the emoji images from Telegram

Button example with emoji:
btn = InlineKeyboardButton(text=p["name_en"], callback_data=f"buy_{{p['id']}}")
if p.get("custom_emoji_id"):
    btn.icon_custom_emoji_id = p["custom_emoji_id"]

Message example:
text = f"{{p['name_en_html']}}\\n💰 Price: ${{p['your_price']}}"
bot.send_message(chat_id, text, parse_mode="HTML")

GET /balance → Balance
{{"success": true, "balance": 25.00}}

POST /purchase → Buy product
Body: {{"product_id":"1","qty":1,"buyer_info":"@customer"}}
Success: {{"success":true,"codes":["the_code"],"total_price":3.5,"new_balance":21.5}}
Error: {{"error":"Insufficient balance"}} or {{"error":"Not enough stock"}}

GET /orders → Order history

Requirements:
- /start → Products button + Balance button
- Products fetched from API, shown as buttons WITH premium emojis (use icon_custom_emoji_id)
- In product detail messages use name_en_html with parse_mode="HTML"
- Buy → POST /purchase → send codes to customer
- Low balance → error message
- Out of stock → error message
- Store connection in MongoDB
- No hardcoded variables — everything from MongoDB
- /api command for admin to paste connection code"""

    bot.send_message(uid, prompt)


# ═══ آخر الطلبات ═══
@bot.callback_query_handler(func=lambda call: call.data == "api_orders")
def api_orders_view(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    
    orders = list(db.api_orders.find({'api_user_id': uid}).sort('_id', -1).limit(10))
    txt = "📜 <b>Recent API Orders</b>\n\n"
    if not orders:
        txt += "📭 No orders yet."
    for o in orders:
        date_str = o['_id'].generation_time.strftime('%m-%d %H:%M') if hasattr(o.get('_id'), 'generation_time') else ''
        status = "✅" if o.get('status') == 'completed' else "⏳"
        txt += f"{status} {o.get('product_name', '?')[:20]} x{o.get('qty', 1)} | ${o.get('total_price', 0):.2f} | {date_str}\n"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="open_api"))
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass


# ═══ إدارة المنتجات — إخفاء/إظهار ═══
@bot.callback_query_handler(func=lambda call: call.data == "api_products")
def api_products_manage(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    
    # جلب المنتجات المخفية لهذا المطوّر
    hidden_pids = set()
    for h in db.api_hidden.find({'api_user_id': uid}):
        hidden_pids.add(h['product_id'])
    
    prods = list(db.products.find({'is_hidden': {'$ne': True}}))
    prods.sort(key=lambda p: clean_name(p.get('name_en', p.get('name_ar', ''))).lower())
    
    txt = "📦 <b>Manage Products</b>\n\n"
    txt += "🟢 = Visible in your bot\n🔴 = Hidden from your bot\n\n"
    txt += "<i>Tap to toggle:</i>"
    
    markup = InlineKeyboardMarkup(row_width=1)
    for p in prods:
        pid = str(p.get('id', str(p.get('_id', ''))))
        name = clean_name(p.get('name_en', p.get('name_ar', '')))[:28]
        is_hidden = pid in hidden_pids
        emoji_id = p.get('custom_emoji_id')
        
        icon = "🔴" if is_hidden else "🟢"
        btn_kwargs = {
            'text': f"{icon} {name}",
            'callback_data': f"api_toggle_{pid}",
            'style': 'danger' if is_hidden else 'success'
        }
        if emoji_id:
            btn_kwargs['icon_custom_emoji_id'] = emoji_id
        markup.add(CustomInlineButton(**btn_kwargs))
    
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="open_api"))
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass


# ═══ تبديل إخفاء/إظهار منتج ═══
@bot.callback_query_handler(func=lambda call: call.data.startswith("api_toggle_"))
def api_toggle_product(call):
    uid = call.from_user.id
    pid = call.data.replace("api_toggle_", "")
    
    existing = db.api_hidden.find_one({'api_user_id': uid, 'product_id': pid})
    if existing:
        db.api_hidden.delete_one({'_id': existing['_id']})
        bot.answer_callback_query(call.id, "🟢 Visible!", show_alert=False)
    else:
        db.api_hidden.insert_one({'api_user_id': uid, 'product_id': pid})
        bot.answer_callback_query(call.id, "🔴 Hidden!", show_alert=False)
    
    # تحديث القائمة
    call.data = "api_products"
    api_products_manage(call)


# ═══ إعادة توليد ═══
@bot.callback_query_handler(func=lambda call: call.data == "api_regen")
def api_regen(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    db.api_keys.update_many({'user_id': uid, 'is_active': True}, {'$set': {'is_active': False}})
    key = _generate_api_key()
    db.api_keys.insert_one({'user_id': uid, 'api_key': key, 'is_active': True, 'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), 'username': call.from_user.username or ''})
    gw = _get_api_gateway()
    conn_code = _generate_connection_code(key)
    if conn_code:
        txt = f"✅ <b>New Key Generated!</b>\n\n🔗 <code>{conn_code}</code>\n\n⚠️ Old key is now disabled."
    else:
        txt = f"✅ <b>New Key!</b>\n\n🔑 <code>{key}</code>\n🛤 <code>/{gw}</code>\n\n⚠️ Old key disabled."
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="open_api"))
    try: bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except: pass


# ═══ تعطيل ═══
@bot.callback_query_handler(func=lambda call: call.data == "api_disable")
def api_disable(call):
    try: bot.answer_callback_query(call.id)
    except: pass
    uid = call.from_user.id
    db.api_keys.update_many({'user_id': uid, 'is_active': True}, {'$set': {'is_active': False}})
    bot.answer_callback_query(call.id, "✅ API Disabled", show_alert=True)
    open_api(call)


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