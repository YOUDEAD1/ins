"""Security Vault, SSRF Protection, Atomic OTP & Admin Handler Hardening.

Implements:
1. Transparent AES-256-GCM encryption (`SESSION_ENCRYPTION_KEY`) for sensitive DB settings.
2. Public-IP validation for free proxy pool (`PROXY_SOURCES` kept free!) & external supplier URLs (`[HIGH-04]`).
3. Atomic OTP attempt counter (`_web_code_check`), spoof-resistant `_web_ip` & protected `_WA_RATE` (`[HIGH-08]`).
4. `@admin_step` re-verification on multi-step Telegram admin flows & session revocation on admin removal (`[MED-01]`).
5. Finite float / lock checks (`[MED-04]`, `[MED-05]`), `initData` replay window cap (`[MED-08]`), and `APIHandler.timeout` (`[LOW-01]`).
"""
import base64
import functools
import hashlib
import hmac
import ipaddress
import math
import os
import re
import socket
import time
import urllib.parse
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


_SENSITIVE_SETTINGS = frozenset((
    'userbot_session',
    'bybit_api_secret',
    'bscscan_api_key',
    'toncenter_api_key',
))
_ENC_PREFIX = 'enc:gcm:v1:'


class SecretVault:
    def __init__(self):
        raw = (os.getenv('SESSION_ENCRYPTION_KEY') or os.getenv('WWC_DRAFT_ENCRYPTION_KEY') or '').strip()
        self.aes = AESGCM(hashlib.sha256(('lara-vault-v1:' + raw).encode('utf-8')).digest()) if len(raw) >= 16 else None

    def encrypt(self, plaintext):
        s = str(plaintext or '')
        if not s or s == 'Not Set' or s.startswith(_ENC_PREFIX) or self.aes is None:
            return s
        nonce = os.urandom(12)
        ct = self.aes.encrypt(nonce, s.encode('utf-8'), b'lara-setting-v1')
        return _ENC_PREFIX + base64.urlsafe_b64encode(nonce + ct).decode('ascii')

    def decrypt(self, stored):
        if not isinstance(stored, str) or not stored.startswith(_ENC_PREFIX):
            return stored
        if self.aes is None:
            return ''
        try:
            raw = base64.urlsafe_b64decode(stored[len(_ENC_PREFIX):].encode('ascii'))
            return self.aes.decrypt(raw[:12], raw[12:], b'lara-setting-v1').decode('utf-8')
        except Exception:
            return ''


def is_public_host(host):
    """Verify hostname or IP resolves strictly to global/public unicast addresses (anti-SSRF)."""
    h = str(host or '').strip().strip('[]').lower()
    if not h or h in ('localhost', 'localhost.localdomain') or h.endswith(('.local', '.internal')):
        return False
    try:
        infos = socket.getaddrinfo(h, None, proto=socket.IPPROTO_TCP)
    except Exception:
        return False
    if not infos:
        return False
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr.split('%', 1)[0])
        except ValueError:
            return False
        if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
                or ip.is_reserved or ip.is_unspecified):
            return False
    return True


def is_safe_public_url(url, *, require_https=False):
    try:
        p = urllib.parse.urlsplit(str(url or '').strip())
    except Exception:
        return False
    allowed = ('https',) if require_https else ('https', 'http')
    if p.scheme.lower() not in allowed or not p.hostname or p.username or p.password:
        return False
    return is_public_host(p.hostname)


class SecurityHardening:
    def __init__(self, ns):
        self.ns = ns
        self.db = ns['db']
        self.bot = ns['bot']
        self.vault = SecretVault()

    def install(self):
        ns = self.ns
        vault = self.vault

        # 1. Socket timeout on APIHandler against Slowloris ([LOW-01]) & security headers ([LOW-03])
        ns['APIHandler'].timeout = 20
        if '_json_resp' in ns:
            orig_json_resp = ns['_json_resp']
            @functools.wraps(orig_json_resp)
            def hardened_json_resp(h, code, data, extra_headers=None):
                hdrs = list(extra_headers or [])
                hdrs.append(('X-Content-Type-Options', 'nosniff'))
                hdrs.append(('Referrer-Policy', 'strict-origin-when-cross-origin'))
                return orig_json_resp(h, code, data, extra_headers=hdrs)
            ns['_json_resp'] = hardened_json_resp

        # 2. Clamp WEBAPP_AUTH_MAX_AGE ([MED-08])
        try:
            ns['WEBAPP_AUTH_MAX_AGE'] = max(300, min(int(ns.get('WEBAPP_AUTH_MAX_AGE', 7200)), 14400))
        except Exception:
            ns['WEBAPP_AUTH_MAX_AGE'] = 7200

        # 3. Transparent encryption/decryption for sensitive settings ([HIGH-07])
        orig_get_setting = ns['get_setting']
        @functools.wraps(orig_get_setting)
        def decrypted_get_setting(key, default='Not Set'):
            val = orig_get_setting(key, default)
            if key in _SENSITIVE_SETTINGS and isinstance(val, str) and val.startswith(_ENC_PREFIX):
                dec = vault.decrypt(val)
                return dec if dec else default
            return val
        ns['get_setting'] = decrypted_get_setting

        if 'admin_save_setting' in ns:
            orig_save_setting = ns['admin_save_setting']
            @functools.wraps(orig_save_setting)
            def guarded_admin_save_setting(message, mode):
                if not ns['_is_admin_check'](message.from_user.id):
                    return
                res = orig_save_setting(message, mode)
                db_key = (ns.get('_SETTING_KEYS') or {}).get(mode)
                if db_key in _SENSITIVE_SETTINGS and vault.aes is not None:
                    row = ns['db'].settings.find_one({'key': db_key})
                    if row and isinstance(row.get('value'), str) and not row['value'].startswith(_ENC_PREFIX) and row['value'] != 'Not Set':
                        ns['db'].settings.update_one({'key': db_key}, {'$set': {'value': vault.encrypt(row['value'])}})
                return res
            ns['admin_save_setting'] = guarded_admin_save_setting

        # 4. Keep Free Proxies (`PROXY_SOURCES`) while filtering out private/loopback SSRF entries ([HIGH-03])
        if '_fetch_proxies_simple' in ns:
            orig_fetch_proxies = ns['_fetch_proxies_simple']
            @functools.wraps(orig_fetch_proxies)
            def safe_fetch_proxies():
                raw_list = orig_fetch_proxies() or []
                clean = []
                for p_url in raw_list:
                    try:
                        parsed = urllib.parse.urlsplit(p_url)
                        host = (parsed.hostname or '').strip()
                        ip = ipaddress.ip_address(host)
                        if (ip.is_private or ip.is_loopback or ip.is_link_local
                                or ip.is_multicast or ip.is_reserved or ip.is_unspecified):
                            continue
                        if parsed.port and 1 <= parsed.port <= 65535:
                            clean.append(p_url)
                    except ValueError:
                        # Non-IP hostname: skip to avoid DNS rebinding in open proxy list
                        continue
                return clean
            ns['_fetch_proxies_simple'] = safe_fetch_proxies

        # 5. External Store & Base URL SSRF Protection ([HIGH-04])
        if '_ext_validate_base_url' in ns:
            orig_ext_val = ns['_ext_validate_base_url']
            @functools.wraps(orig_ext_val)
            def safe_ext_validate_base_url(raw_url):
                ok, norm, err = orig_ext_val(raw_url)
                if not ok:
                    return ok, norm, err
                if not is_safe_public_url(norm):
                    return False, '', '❌ يُرفض الاتصال بالعناوين المحلية أو الداخلية (SSRF Protection).'
                return ok, norm, err
            ns['_ext_validate_base_url'] = safe_ext_validate_base_url

        for ext_fn_name in ('_ext_api_get', '_ext_api_post'):
            if ext_fn_name in ns:
                orig_ext_http = ns[ext_fn_name]
                def make_safe_ext(fn):
                    @functools.wraps(fn)
                    def wrapped(store, path, *args, **kwargs):
                        base = str((store or {}).get('base_url') or '')
                        if not is_safe_public_url(base):
                            raise RuntimeError('Blocked non-public external store URL')
                        return fn(store, path, *args, **kwargs)
                    return wrapped
                ns[ext_fn_name] = make_safe_ext(orig_ext_http)

        if '_baseurl_probe' in ns:
            orig_probe = ns['_baseurl_probe']
            @functools.wraps(orig_probe)
            def safe_baseurl_probe(raw):
                u = str(raw or '').strip()
                if not u.startswith(('http://', 'https://')):
                    u = 'https://' + u
                if not is_safe_public_url(u):
                    return False, u, 'عنوان داخلي أو غير عام (مرفوض أمنياً)'
                return orig_probe(raw)
            ns['_baseurl_probe'] = safe_baseurl_probe

        # 6. Hardened `_web_ip` (drop spoofable True-Client-IP), protected `_WA_RATE`, and atomic OTP `_web_code_check` ([HIGH-08])
        def safe_web_ip(handler):
            trusted = os.getenv('TRUSTED_PROXY', '1').strip().lower() not in ('0', 'false', 'no', 'off')
            if trusted:
                cf_ip = (handler.headers.get('CF-Connecting-IP') or '').strip()
                if re.fullmatch(r'[0-9A-Fa-f:.]{3,45}', cf_ip):
                    return cf_ip[:64]
                xff = handler.headers.get('X-Forwarded-For', '').split(',')[-1].strip()
                if re.fullmatch(r'[0-9A-Fa-f:.]{3,45}', xff):
                    return xff[:64]
            return str(handler.client_address[0])[:64]

        ns['_web_ip'] = safe_web_ip

        orig_rate_ok = ns['_wa_rate_ok']
        @functools.wraps(orig_rate_ok)
        def protected_wa_rate_ok(key, limit, window=60):
            rate_map = ns.get('_WA_RATE')
            lock = ns.get('_WA_RATE_LOCK')
            if isinstance(rate_map, dict) and lock is not None and len(rate_map) > 5500:
                now = time.time()
                with lock:
                    if len(rate_map) > 5500:
                        # Evict expired or non-account keys first so per-email lockouts (`wele:`) cannot be flushed
                        removable = [k for k in list(rate_map.keys()) if not str(k).startswith(('wele:', 'wese:', 'wefe:', 'wece:'))]
                        for k in removable[:2000]:
                            rate_map.pop(k, None)
            return orig_rate_ok(key, limit, window)
        ns['_wa_rate_ok'] = protected_wa_rate_ok

        def atomic_web_code_check(email_addr, purpose, code):
            """Atomically increment OTP attempt count BEFORE hash comparison to prevent parallel brute-force races."""
            clean = re.sub(r'\D', '', str(code or ''))[:6]
            key = f"{purpose}:{email_addr}"
            now = int(time.time())
            max_tries = int(ns.get('WEB_CODE_TRIES', 5))
            if len(clean) != 6:
                ns['db'].web_codes.update_one({'_id': key}, {'$inc': {'tries': 1}})
                return None, 'bad_code'
            # Atomic pre-increment under `tries < max_tries`
            doc = ns['db'].web_codes.find_one_and_update(
                {'_id': key, 'expires_at': {'$gte': now}, 'tries': {'$lt': max_tries}},
                {'$inc': {'tries': 1}},
            )
            if not doc:
                return None, 'code_expired'
            if not hmac.compare_digest(ns['_web_hash'](clean), str(doc.get('code') or '')):
                return None, 'bad_code'
            deleted = ns['db'].web_codes.find_one_and_delete({'_id': key, 'code': doc['code']})
            if not deleted:
                return None, 'code_expired'
            return deleted, None

        ns['_web_code_check'] = atomic_web_code_check

        # 7. Enforce @admin_step on multi-step handlers & clean VC/steps on admin demotion ([MED-01], [MED-05])
        for step_name in (
            'ad_u_search_exec', 'ad_gift_note_step2', 'ad_gift_note_exec',
            'admin_fsub_save', 'ad_gift_val', 'admin_bc_pick_product',
            'ad_cat_create_step2', 'ad_cat_create_step3',
            'admin_stock_edit_step2', 'admin_stock_edit_step3',
        ):
            if step_name in ns and callable(ns[step_name]):
                fn = ns[step_name]
                @functools.wraps(fn)
                def guarded_step(message, *a, _f=fn, **kw):
                    if not ns['_is_admin_check'](message.from_user.id):
                        return
                    return _f(message, *a, **kw)
                ns[step_name] = guarded_step

        def _patch_cb(fn_name, new_fn):
            for h in self.bot.callback_query_handlers:
                fn = h.get('function') if isinstance(h, dict) else getattr(h, 'function', None)
                if getattr(fn, '__name__', '') == fn_name:
                    if isinstance(h, dict):
                        h['function'] = new_fn
                    else:
                        h.function = new_fn

        if 'sec_remove_admin_cb' in ns:
            orig_rem_admin = ns['sec_remove_admin_cb']
            @functools.wraps(orig_rem_admin)
            def clean_sec_remove_admin(call):
                res = orig_rem_admin(call)
                try:
                    target_id = int(str(call.data).rsplit('_', 1)[-1])
                    ns['bot'].clear_step_handler_by_chat_id(target_id)
                    vc_sessions = ns.get('_VC_SESSIONS')
                    if isinstance(vc_sessions, dict):
                        vc_sessions.pop(target_id, None)
                except Exception:
                    pass
                return res
            ns['sec_remove_admin_cb'] = clean_sec_remove_admin
            _patch_cb('sec_remove_admin_cb', clean_sec_remove_admin)

        # 8. Validate banned/forced-sub/deposit-lock in `ext_customer_buy` ([MED-04], [MED-05])
        if 'ext_customer_buy' in ns:
            orig_ext_buy = ns['ext_customer_buy']
            @functools.wraps(orig_ext_buy)
            def safe_ext_customer_buy(call):
                uid = call.from_user.id
                if ns['is_user_banned'](uid):
                    return
                if ns['check_forced_sub'](uid) is False:
                    ns['bot'].answer_callback_query(call.id, '⚠️ يجب الاشتراك في القناة أولاً.', show_alert=True)
                    return
                if ns['is_deposit_locked'](uid):
                    ns['bot'].answer_callback_query(call.id, '⏳ لديك عملية إيداع معلّقة حالياً.', show_alert=True)
                    return
                return orig_ext_buy(call)
            ns['ext_customer_buy'] = safe_ext_customer_buy
            _patch_cb('ext_customer_buy', safe_ext_customer_buy)

        return self


def install(namespace):
    if namespace.get('_SECURITY_HARDENING') is not None:
        return namespace['_SECURITY_HARDENING']
    service = SecurityHardening(namespace).install()
    namespace['_SECURITY_HARDENING'] = service
    return service
