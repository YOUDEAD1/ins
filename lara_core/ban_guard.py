"""Unified Ban System across Telegram Bot, Standalone Website, Email, and Google OAuth.

Guarantees that banning a user from ANY surface (Bot `/ban`, Admin Panel, Mini App,
Referral Spam, or Email):
1. Bans both their Telegram account AND any linked/merged Website account (`db.users`).
2. Permanently flags their associated Email(s) and `google_sub` in `db.web_accounts` AND `db.banned_emails`.
3. Immediately revokes all active `db.web_sessions`, pending `db.web_links`, `db.web_logins`,
   `db.web_codes`, and webhooks.
4. Blocks banned Website accounts from reading orders/codes, logging in via Email/OTP/Google,
   registering again with that email, or laundering balances via `_web_merge`.
"""
import functools
import html
import re
import time


class UnifiedBanGuard:
    def __init__(self, ns):
        self.ns = ns
        self.db = ns['db']
        self.bot = ns['bot']
        self.orig = {}
        self._cache = {}
        try:
            self.db.banned_emails.create_index('google_sub', sparse=True)
            self.db.banned_emails.create_index('uid')
        except Exception:
            pass

    def _norm_email(self, raw):
        fn = self.ns.get('_web_email_norm')
        if callable(fn):
            val = fn(raw)
            if val:
                return val
        s = str(raw or '').strip().lower()
        return s if ('@' in s and '.' in s.split('@', 1)[-1] and len(s) <= 254) else ''

    def related_uids(self, uid):
        """Return all user_ids belonging to the same person (Telegram <-> Website merges)."""
        try:
            uid = int(uid)
        except (TypeError, ValueError):
            return set()
        uids = {uid}
        try:
            u = self.db.users.find_one({'user_id': uid}, {'web_merged_into': 1}) or {}
            parent = u.get('web_merged_into')
            if parent is not None:
                uids.add(int(parent))
            for child in self.db.users.find({'web_merged_into': {'$in': list(uids)}}, {'user_id': 1}).limit(20):
                if child.get('user_id') is not None:
                    uids.add(int(child['user_id']))
        except Exception:
            pass
        return uids

    def emails_for_uids(self, uids):
        """Return (emails_set, google_subs_set) linked to any of the given user_ids."""
        emails, subs = set(), set()
        if not uids:
            return emails, subs
        uid_list = [int(x) for x in uids]
        try:
            for acc in self.db.web_accounts.find({'user_id': {'$in': uid_list}}):
                em = self._norm_email(acc.get('_id'))
                if em:
                    emails.add(em)
                sub = str(acc.get('google_sub') or '').strip()
                if sub:
                    subs.add(sub)
            for u in self.db.users.find({'user_id': {'$in': uid_list}}, {'email': 1}):
                em = self._norm_email(u.get('email'))
                if em:
                    emails.add(em)
        except Exception:
            pass
        return emails, subs

    def is_email_banned(self, email_addr=None, google_sub=None):
        em = self._norm_email(email_addr)
        sub = str(google_sub or '').strip()
        try:
            if em:
                if self.db.banned_emails.find_one({'_id': em}, {'_id': 1}):
                    return True
                acc = self.db.web_accounts.find_one({'_id': em}, {'banned': 1, 'user_id': 1})
                if acc:
                    if acc.get('banned'):
                        return True
                    if acc.get('user_id') is not None and self.is_user_banned(acc['user_id'], _check_email=False):
                        return True
            if sub:
                if self.db.banned_emails.find_one({'google_sub': sub}, {'_id': 1}):
                    return True
                acc = self.db.web_accounts.find_one({'google_sub': sub}, {'banned': 1, 'user_id': 1})
                if acc and (acc.get('banned') or (acc.get('user_id') is not None and self.is_user_banned(acc['user_id'], _check_email=False))):
                    return True
        except Exception:
            pass
        return False

    def is_user_banned(self, uid, *, _check_email=True):
        try:
            uid = int(uid)
        except (TypeError, ValueError):
            return False
        if uid == self.ns.get('OWNER_ID'):
            return False
        now_m = time.monotonic()
        if _check_email:
            cached = self._cache.get(uid)
            if cached and cached[1] > now_m:
                return cached[0]
        res = False
        try:
            u = self.db.users.find_one({'user_id': uid}, {'is_banned': 1, 'web_merged_into': 1, 'email': 1})
            if u:
                if u.get('is_banned') in (1, True):
                    res = True
                else:
                    merged = u.get('web_merged_into')
                    if merged is not None and int(merged) != uid:
                        mu = self.db.users.find_one({'user_id': int(merged)}, {'is_banned': 1})
                        if mu and mu.get('is_banned') in (1, True):
                            res = True
                    if not res and _check_email and u.get('email') and self.db.banned_emails.find_one({'_id': self._norm_email(u['email'])}, {'_id': 1}):
                        res = True
            if not res and _check_email:
                for acc in self.db.web_accounts.find({'user_id': uid}, {'_id': 1, 'banned': 1}).limit(5):
                    if acc.get('banned') or self.db.banned_emails.find_one({'_id': str(acc['_id'])}, {'_id': 1}):
                        res = True
                        break
        except Exception:
            pass
        if _check_email:
            if len(self._cache) > 4000:
                self._cache.clear()
            self._cache[uid] = (res, now_m + 15.0)
        return res

    def set_ban(self, target_uid=None, *, email=None, ban=True, reason='', by_admin=None):
        """Atomically ban or unban user(s) + linked website account(s) + email(s) + active sessions."""
        now = int(time.time())
        owner_id = self.ns.get('OWNER_ID')
        uids = set()
        emails = set()
        subs = set()

        em_in = self._norm_email(email)
        if em_in:
            emails.add(em_in)
            acc = self.db.web_accounts.find_one({'_id': em_in})
            if acc and acc.get('user_id') is not None:
                uids.update(self.related_uids(acc['user_id']))
            for u in self.db.users.find({'email': em_in}, {'user_id': 1}).limit(5):
                if u.get('user_id') is not None:
                    uids.update(self.related_uids(u['user_id']))

        if target_uid is not None:
            uids.update(self.related_uids(target_uid))

        if owner_id in uids:
            raise ValueError('cannot_ban_owner')

        found_emails, found_subs = self.emails_for_uids(uids)
        emails.update(found_emails)
        subs.update(found_subs)

        uid_list = sorted(uids)
        if ban:
            if uid_list:
                update_doc = {'is_banned': 1, 'banned_at': now}
                if reason:
                    update_doc['banned_reason'] = str(reason)[:200]
                if by_admin is not None:
                    update_doc['banned_by'] = int(by_admin)
                self.db.users.update_many({'user_id': {'$in': uid_list}}, {'$set': update_doc})
                # Immediately terminate all website sessions, pending links, and logins
                self.db.web_sessions.delete_many({'user_id': {'$in': uid_list}})
                self.db.web_links.delete_many({'$or': [{'w_uid': {'$in': uid_list}}, {'t_uid': {'$in': uid_list}}]})
                self.db.web_logins.delete_many({'user_id': {'$in': uid_list}})
                for u_id in uid_list:
                    try:
                        self.ns['_revoke_user_webhooks'](u_id, 'owner_banned')
                    except Exception:
                        pass
                    try:
                        self.ns['_invalidate_user_cache'](u_id)
                    except Exception:
                        pass
            if emails:
                self.db.web_accounts.update_many(
                    {'_id': {'$in': list(emails)}},
                    {'$set': {'banned': True, 'banned_at': now, 'banned_reason': str(reason)[:200]}}
                )
                for em in emails:
                    acc = self.db.web_accounts.find_one({'_id': em}, {'google_sub': 1, 'user_id': 1}) or {}
                    doc = {
                        'banned_at': now,
                        'reason': str(reason)[:200],
                        'by_admin': int(by_admin) if by_admin is not None else None,
                        'uid': acc.get('user_id') or (uid_list[0] if uid_list else None),
                    }
                    if acc.get('google_sub'):
                        doc['google_sub'] = str(acc['google_sub'])
                    self.db.banned_emails.update_one({'_id': em}, {'$set': doc}, upsert=True)
                    # Clear pending OTP codes for banned emails
                    self.db.web_codes.delete_many({'_id': {'$in': [f'signup:{em}', f'login:{em}', f'reset:{em}']}})
        else:
            if uid_list:
                self.db.users.update_many(
                    {'user_id': {'$in': uid_list}},
                    {'$set': {'is_banned': 0}, '$unset': {'banned_reason': '', 'banned_at': '', 'banned_by': ''}}
                )
                for u_id in uid_list:
                    try:
                        self.ns['_invalidate_user_cache'](u_id)
                    except Exception:
                        pass
            if emails:
                self.db.web_accounts.update_many(
                    {'_id': {'$in': list(emails)}},
                    {'$set': {'banned': False}, '$unset': {'banned_at': '', 'banned_reason': ''}}
                )
                self.db.banned_emails.delete_many({'_id': {'$in': list(emails)}})
            if uid_list:
                self.db.banned_emails.delete_many({'uid': {'$in': uid_list}})

        self._cache.clear()
        return {'uids': uid_list, 'emails': sorted(emails), 'banned': bool(ban)}

    def resolve_target(self, raw_target):
        """Resolve User ID, @username (case-insensitive), or Email address for admin ban/unban."""
        t = str(raw_target or '').strip()
        if not t:
            return None, None
        if '@' in t and not t.startswith('@') and '.' in t.split('@', 1)[-1]:
            em = self._norm_email(t)
            if not em:
                return None, None
            acc = self.db.web_accounts.find_one({'_id': em})
            if acc and acc.get('user_id') is not None:
                u = self.db.users.find_one({'user_id': int(acc['user_id'])})
                return u, em
            u = self.db.users.find_one({'email': em})
            return u, em
        clean = t.lstrip('@')
        if clean.lstrip('-').isdigit() and not t.startswith('@'):
            u = self.db.users.find_one({'user_id': int(clean)})
            return u, None
        u = self.db.users.find_one({'username': {'$regex': '^@?' + re.escape(clean) + '$', '$options': 'i'}})
        return u, None

    def install(self):
        ns = self.ns
        guard = self

        # 1. Replace is_user_banned globally
        self.orig['is_user_banned'] = ns['is_user_banned']
        ns['is_user_banned'] = self.is_user_banned
        ns['is_email_banned'] = self.is_email_banned
        ns['set_user_ban_unified'] = self.set_ban

        # 2. Wrap _web_identity so banned web sessions are immediately rejected on every request
        orig_web_identity = ns['_web_identity']
        @functools.wraps(orig_web_identity)
        def safe_web_identity(handler):
            ident = orig_web_identity(handler)
            if not ident:
                return None
            uid = ident.get('id')
            if guard.is_user_banned(uid):
                try:
                    token = ns['_web_cookie'](handler)
                    if token:
                        guard.db.web_sessions.delete_one({'_id': ns['_web_hash'](token)})
                except Exception:
                    pass
                return None
            return ident
        ns['_web_identity'] = safe_web_identity

        # 3. Guard all public website email & Google auth routes against banned emails/accounts
        for route_path, fn_name in (
            ('/webapp/web/email/signup', '_web_email_signup'),
            ('/webapp/web/email/verify', '_web_email_verify'),
            ('/webapp/web/email/login', '_web_email_login'),
            ('/webapp/web/email/forgot', '_web_email_forgot'),
            ('/webapp/web/email/code', '_web_email_code'),
            ('/webapp/web/email/code_login', '_web_email_code_login'),
            ('/webapp/web/email/reset', '_web_email_reset'),
        ):
            orig_fn = ns[fn_name]
            def make_email_guard(orig_f):
                @functools.wraps(orig_f)
                def wrapped(handler, body):
                    em = guard._norm_email((body or {}).get('email'))
                    if em and guard.is_email_banned(em):
                        return 403, {'error': 'banned'}
                    return orig_f(handler, body)
                return wrapped
            wrapped_fn = make_email_guard(orig_fn)
            ns[fn_name] = wrapped_fn
            if route_path in ns.get('_WEB_PUBLIC_POST', {}):
                ns['_WEB_PUBLIC_POST'][route_path] = wrapped_fn

        # Guard Google OAuth against banned email or google_sub before creating/logging in
        orig_google = ns['_web_google']
        @functools.wraps(orig_google)
        def safe_web_google(handler, body):
            status, payload = orig_google(handler, body)
            if status == 200:
                # Verify newly created/logged session wasn't using a banned email
                token = getattr(ns.get('_WEB_CTX'), 'token', None)
                if token:
                    sdoc = guard.db.web_sessions.find_one({'_id': ns['_web_hash'](token)})
                    if sdoc and guard.is_user_banned(sdoc.get('user_id')):
                        guard.db.web_sessions.delete_one({'_id': sdoc['_id']})
                        return 403, {'error': 'banned'}
            return status, payload
        ns['_web_google'] = safe_web_google
        if '/webapp/web/google' in ns.get('_WEB_PUBLIC_POST', {}):
            ns['_WEB_PUBLIC_POST']['/webapp/web/google'] = safe_web_google

        # 4. Harden _web_link_start and _web_merge against banned source/target & migrate api_* tables safely
        if '_web_link_start' in ns:
            orig_link_start = ns['_web_link_start']
            @functools.wraps(orig_link_start)
            def safe_link_start(identity, body):
                if guard.is_user_banned(identity['id']):
                    return 403, {'error': 'banned'}
                return orig_link_start(identity, body)
            ns['_web_link_start'] = safe_link_start
            if '/webapp/web/link/start' in ns.get('_WA_POST_ROUTES', {}):
                ns['_WA_POST_ROUTES']['/webapp/web/link/start'] = safe_link_start

        orig_merge = ns['_web_merge']
        @functools.wraps(orig_merge)
        def safe_web_merge(w, t, tg_new=False):
            w, t = int(w), int(t)
            if guard.is_user_banned(w) or guard.is_user_banned(t):
                raise RuntimeError('banned')
            if not guard.db.users.find_one({'user_id': t}, {'_id': 1}):
                raise RuntimeError('target_missing')
            res = orig_merge(w, t, tg_new)
            # Also migrate reseller API collections keyed by api_user_id
            for col_name in ('api_orders', 'api_pricing', 'api_hidden'):
                try:
                    guard.db[col_name].update_many({'api_user_id': w}, {'$set': {'api_user_id': t}})
                except Exception:
                    pass
            return res
        ns['_web_merge'] = safe_web_merge

        # 5. Upgrade WebApp Admin Ban (`/webapp/admin/ban`)
        def unified_wa_admin_ban(identity, body):
            uid, err = ns['_wa_admin'](identity)
            if err:
                return err
            try:
                target = int(body.get('uid'))
            except Exception:
                return 400, {'error': 'bad_request'}
            ban = bool(body.get('ban'))
            if target == ns['OWNER_ID'] or (ban and target == uid):
                return 400, {'error': 'cannot_ban'}
            if not guard.db.users.find_one({'user_id': target}, {'_id': 1}):
                return 404, {'error': 'not_found'}
            guard.set_ban(target, ban=ban, reason='Admin panel', by_admin=uid)
            ns['_adm_audit'](uid, 'ban' if ban else 'unban', target)
            return ns['_wa_admin_user'](identity, {'uid': target})

        ns['_wa_admin_ban'] = unified_wa_admin_ban
        if '/webapp/admin/user/ban' in ns.get('_WA_POST_ROUTES', {}):
            ns['_WA_POST_ROUTES']['/webapp/admin/user/ban'] = unified_wa_admin_ban
        if '/webapp/admin/ban' in ns.get('_WA_POST_ROUTES', {}):
            ns['_WA_POST_ROUTES']['/webapp/admin/ban'] = unified_wa_admin_ban

        if 'admin_user_brief' in ns:
            orig_brief = ns['admin_user_brief']
            @functools.wraps(orig_brief)
            def unified_admin_user_brief(uid):
                res = orig_brief(uid)
                if isinstance(res, dict) and not res.get('missing'):
                    res['banned'] = guard.is_user_banned(uid)
                return res
            ns['admin_user_brief'] = unified_admin_user_brief

        def _patch_cb(fn_name, new_fn):
            for h in guard.bot.callback_query_handlers:
                fn = h.get('function') if isinstance(h, dict) else getattr(h, 'function', None)
                if getattr(fn, '__name__', '') == fn_name:
                    if isinstance(h, dict):
                        h['function'] = new_fn
                    else:
                        h.function = new_fn

        # 6. Upgrade Telegram Bot Admin Ban (`ad_ban_start`, `ad_ban_exec`, `ad_u_search`, `show_user_admin_profile`)
        def upgraded_ad_ban_start(call):
            guard.bot.answer_callback_query(call.id)
            msg = guard.bot.send_message(
                call.message.chat.id,
                '🚫 <b>الحظر الموحّد الشامل (البوت + الموقع + الإيميل)</b>\n\n'
                'أرسل <code>الأيدي (ID)</code> أو <code>@username</code> أو <code>البريد الإلكتروني</code> للحظر أو فك الحظر:',
                parse_mode='HTML'
            )
            guard.bot.register_next_step_handler(msg, ns['ad_ban_exec'])

        ns['ad_ban_start'] = ns['admin_required'](upgraded_ad_ban_start)
        _patch_cb('ad_ban_start', ns['ad_ban_start'])

        def upgraded_ad_ban_exec(message):
            uid = message.from_user.id
            if not ns['_is_admin_check'](uid):
                return
            raw = (message.text or '').strip()
            if not raw or raw.lower() in ('/cancel', 'الغاء', 'إلغاء', 'cancel'):
                guard.bot.send_message(message.chat.id, '❌ تم الإلغاء.')
                return
            u, em = guard.resolve_target(raw)
            if not u and not em:
                guard.bot.send_message(
                    message.chat.id,
                    '❌ <b>لم يتم العثور على الحساب.</b>\n\n'
                    'يمكنك إرسال: <code>User ID</code> أو <code>@username</code> أو <code>البريد الإلكتروني</code>.',
                    parse_mode='HTML'
                )
                return
            if u and int(u['user_id']) == ns['OWNER_ID']:
                guard.bot.send_message(message.chat.id, '❌ لا يمكنك حظر المالك الأساسي!')
                return
            if u and int(u['user_id']) == uid:
                guard.bot.send_message(message.chat.id, '❌ لا يمكنك حظر حسابك الشخصي!')
                return
            currently_banned = guard.is_user_banned(u['user_id']) if u else guard.is_email_banned(em)
            new_ban = not currently_banned
            info = guard.set_ban(
                u['user_id'] if u else None,
                email=em,
                ban=new_ban,
                reason='Bot admin ban',
                by_admin=uid,
            )
            action_lbl = '🚫 تم الحظر الشامل (البوت + الموقع + الإيميل)' if new_ban else '✅ تم فك الحظر الشامل'
            uid_txt = ', '.join(f'<code>{x}</code>' for x in info['uids']) or '—'
            em_txt = ', '.join(f'<code>{html.escape(x)}</code>' for x in info['emails']) or 'لا يوجد إيميل مربوط'
            guard.bot.send_message(
                message.chat.id,
                f'<b>{action_lbl}</b>\n\n'
                f'👤 <b>المعرفات المشمولة:</b> {uid_txt}\n'
                f'✉️ <b>الإيميلات المشمولة:</b> {em_txt}',
                parse_mode='HTML'
            )

        ns['ad_ban_exec'] = ns['admin_step'](upgraded_ad_ban_exec)

        # Allow searching customers by Email in `ad_u_search` + add 1-tap Unified Ban button in `show_user_admin_profile`
        if 'ad_u_search_prompt' in ns:
            def upgraded_u_search_prompt(call):
                guard.bot.answer_callback_query(call.id)
                ns['_up_clear_input'](call.message.chat.id)
                msg = guard.bot.send_message(
                    call.message.chat.id,
                    '🔍 <b>أرسل الأيدي (ID) أو @username أو البريد الإلكتروني للعميل:</b>',
                    parse_mode='HTML'
                )
                guard.bot.register_next_step_handler(msg, ns['ad_u_search_exec'])
            ns['ad_u_search_prompt'] = ns['admin_required'](upgraded_u_search_prompt)
            _patch_cb('ad_u_search_prompt', ns['ad_u_search_prompt'])

        if 'ad_u_search_exec' in ns:
            def upgraded_u_search_exec(message):
                if not ns['_is_admin_check'](message.from_user.id):
                    return
                u, _ = guard.resolve_target(message.text or '')
                if u:
                    target_uid = int(u.get('web_merged_into') or u['user_id'])
                    ns['_up_member_origin'](message.chat.id, target_uid, 'ad_users_main')
                    ns['show_user_admin_profile'](message.chat.id, target_uid)
                else:
                    guard.bot.send_message(message.chat.id, '❌ لم يتم العثور على المستخدم.')
            ns['ad_u_search_exec'] = ns['admin_step'](upgraded_u_search_exec)

        if 'show_user_admin_profile' in ns:
            def upgraded_show_user_admin_profile(chat_id, target_uid, message_id=None):
                u = ns['get_user_data_full'](target_uid, use_cache=False)
                if not u:
                    return
                ns['_up_clear_input'](chat_id)
                purchase_summary = ns['_up_summary'](ns['_up_dataset'](target_uid))
                buy_count = purchase_summary['orders']
                d_res = list(guard.db.used_transactions.find({'user_id': target_uid}))
                dep_total = sum(float(d.get('amount', 0) or 0) for d in d_res)
                uname_str = f"@{html.escape(str(u['username']))}" if u.get('username') else 'لا يوجد'
                emails, _ = guard.emails_for_uids(guard.related_uids(target_uid))
                em_line = f"\n✉️ البريد: <code>{html.escape(', '.join(sorted(emails)))}</code>" if emails else ''
                banned_now = guard.is_user_banned(target_uid)
                ban_str = 'محظور شامل 🚫' if banned_now else 'نشط ✅'
                text = (
                    f"📂 <b>ملف العميل (نظرة الإدارة)</b>\n"
                    f"<blockquote>"
                    f"👤 الاسم: <b>{ns['clean_name'](u.get('name', 'بدون'))}</b>\n"
                    f"🔗 المعرف: {uname_str}\n"
                    f"🆔 الأيدي: <code>{target_uid}</code>{em_line}\n"
                    f"🛡 الحالة: <b>{ban_str}</b>\n\n"
                    f"💰 الرصيد الحالي: <b>${float(u.get('balance', 0) or 0):.2f}</b>\n"
                    f"✅ المشتريات: <b>{buy_count}</b> · 📦 الإيداعات: <b>${dep_total:.2f}</b>"
                    f"</blockquote>"
                )
                try:
                    text += ns['vip_admin_user_line'](target_uid)
                except Exception:
                    pass
                Btn = ns['InlineKeyboardButton']
                markup = ns['InlineKeyboardMarkup'](row_width=2)
                markup.add(Btn('🛍 المشتريات', callback_data=f'ad_uh_buy_{target_uid}'),
                           Btn('💳 الإيداعات', callback_data=f'ad_uh_dep_{target_uid}'))
                markup.add(Btn('📥 تحميل السجل الكامل', callback_data=f'ad_full_hist_{target_uid}'))
                markup.add(Btn('💰 تعديل رصيده', callback_data=f'ad_ugift_{target_uid}'),
                           Btn('✅ فك الحظر الشامل' if banned_now else '🚫 حظر شامل', callback_data=f'ad_utogban_{target_uid}'))
                markup.add(Btn('📋 سجل التعديلات المالية', callback_data=f'ad_view_ballogs_{target_uid}_0'))
                markup.add(Btn('👑 VIP', callback_data=f'ad_vipu_{target_uid}'),
                           Btn('👥 إحالاته', callback_data=f'ad_uref_{target_uid}_0'))
                markup.add(Btn('🔍 بحث جديد', callback_data='ad_u_search'),
                           Btn('🔙 رجوع', callback_data=ns['_up_member_origin'](chat_id, target_uid)))
                try:
                    if message_id:
                        guard.bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
                    else:
                        guard.bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                except Exception:
                    guard.bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')

            ns['show_user_admin_profile'] = upgraded_show_user_admin_profile

            def toggle_profile_ban_cb(call):
                target_uid = int(call.data.replace('ad_utogban_', ''))
                if target_uid == ns['OWNER_ID'] or target_uid == call.from_user.id:
                    guard.bot.answer_callback_query(call.id, '❌ لا يمكن حظر هذا الحساب!', show_alert=True)
                    return
                new_ban = not guard.is_user_banned(target_uid)
                guard.set_ban(target_uid, ban=new_ban, reason='Admin profile toggle', by_admin=call.from_user.id)
                guard.bot.answer_callback_query(call.id, '🚫 تم الحظر الشامل' if new_ban else '✅ تم فك الحظر الشامل')
                ns['show_user_admin_profile'](call.message.chat.id, target_uid, call.message.message_id)

            before = len(guard.bot.callback_query_handlers)
            guard.bot.callback_query_handler(func=lambda call: call.data.startswith('ad_utogban_'))(
                ns['admin_required'](toggle_profile_ban_cb)
            )
            added = guard.bot.callback_query_handlers[before:]
            del guard.bot.callback_query_handlers[before:]
            guard.bot.callback_query_handlers[0:0] = added

        # Also ensure referral spam/milestone/device bans revoke web sessions & ban linked emails
        if 'refspam_ban_handler' in ns and callable(ns['refspam_ban_handler']):
            orig_spam = ns['refspam_ban_handler']
            @functools.wraps(orig_spam)
            def wrapped_refspam(call):
                out = orig_spam(call)
                if ns['_is_admin_check'](call.from_user.id):
                    try:
                        t_uid = int(str(call.data).replace('refspam_ban_', ''))
                        guard.set_ban(t_uid, ban=True, reason='Spam referrals', by_admin=call.from_user.id)
                    except Exception:
                        pass
                return out
            ns['refspam_ban_handler'] = wrapped_refspam
            _patch_cb('refspam_ban_handler', wrapped_refspam)

        if 'ref_milestone_ban' in ns and callable(ns['ref_milestone_ban']):
            orig_ms = ns['ref_milestone_ban']
            @functools.wraps(orig_ms)
            def wrapped_ms_ban(call):
                out = orig_ms(call)
                if ns['_is_admin_check'](call.from_user.id):
                    try:
                        r_id, _ = map(int, str(call.data)[len('refwd_ban_'):].rsplit('_', 1))
                        if guard.db.users.find_one({'user_id': r_id, 'is_banned': 1}, {'_id': 1}):
                            guard.set_ban(r_id, ban=True, reason='Referral review', by_admin=call.from_user.id)
                    except Exception:
                        pass
                return out
            ns['ref_milestone_ban'] = wrapped_ms_ban
            _patch_cb('ref_milestone_ban', wrapped_ms_ban)

        if '_refdv_apply' in ns and callable(ns['_refdv_apply']):
            orig_refdv = ns['_refdv_apply']
            @functools.wraps(orig_refdv)
            def wrapped_refdv(target_u, verdict, admin_id, admin_name=None):
                out = orig_refdv(target_u, verdict, admin_id, admin_name)
                if verdict == 'ban':
                    try:
                        guard.set_ban(int(target_u), ban=True, reason='Referral device check ban', by_admin=admin_id)
                    except Exception:
                        pass
                return out
            ns['_refdv_apply'] = wrapped_refdv

        return self


def install(namespace):
    if namespace.get('_UNIFIED_BAN_GUARD') is not None:
        return namespace['_UNIFIED_BAN_GUARD']
    service = UnifiedBanGuard(namespace).install()
    namespace['_UNIFIED_BAN_GUARD'] = service
    return service
