"""Luxury Telegram Bot UI, Automatic Premium Emoji Engine & Crisp Media Fallbacks.

Delivers:
1. Cohesive, luxury Telegram Bot cards (`welcome`, `profile_txt`, `invite_txt`, `store_title`,
   `dep_choose`, `admin_main_ui`, `admin_shop_settings`) with automatic `<tg-emoji>` Premium icons.
2. Proper Arabic translations for `ref_purchase_dm`, `ref_progress_dm`, `ref_milestone_dm` (fixing
   the English-in-Arabic bug) + missing `LANG['en']` referral keys.
3. Automatic Telegram Premium `icon_custom_emoji_id` on Bot inline buttons (`get_btn_data`) with
   transparent `_bx_make_request` auto-downgrade fallback if a token lacks Premium permissions.
4. Complete `_WA_SLOT_SRC` coverage (`admin`, `search`, `avail`) so WebApp slots never lack IDs.
5. Crisp `192x192` Lanczos + UnsharpMask RGBA WebP fallback in `_wa_emoji_webp` for iPhone/Safari.
6. Clean vector/geometric `/og.png` preview card without broken Arabic tofu glyphs, plus accurate
   `_net_server_route` asset classification.
"""
import functools
import html
import io
import json
import os
import re


# Curated Telegram Premium Custom Emoji IDs (matching the store's existing official pack IDs)
AUTO_BUTTON_EMOJIS = {
    'btn_products': '5312361253610475399',
    'btn_webapp': '5312361253610475399',
    'btn_deposit': '5445353829304387411',
    'btn_profile': '5262742999678329061',
    'btn_settings': '5346269127059196142',
    'btn_invite': '4942888689131848546',
    'btn_support': '5271619747891388291',
    'btn_lang': '5447410659077661506',
    'btn_admin': '5217822164362739968',
    'btn_terms': '5197288647275071607',
    'btn_stars': '5438496463044752972',
    'btn_binance': '4956574641075258382',
    'btn_bybit': '5291958423787683624',
    'btn_usdt_trc20': '5314431908883488060',
    'btn_usdt_bep20': '5314431908883488060',
    'btn_ton': '5370546279375982437',
    'btn_ltc': '5273967072072706245',
    'btn_buy_hist': '5400090058030075645',
    'btn_dep_hist': '5444856076954520455',
    'btn_refresh': '5346269127059196142',
    'btn_shop_search': '5231012545799666522',
    'btn_shop_avail': '5206607081334906820',
    'btn_buy_now': '5206607081334906820',
    'btn_check_sub': '5206607081334906820',
    'btn_api': '5282843764451195532',
}


def _strip_leading_unicode_emoji(label):
    """Remove leading plain emoji when `icon_custom_emoji_id` is attached so two icons don't collide."""
    s = str(label or '').strip()
    first, sep, rest = s.partition(' ')
    if sep and first and not any(ch.isalnum() for ch in first):
        return rest.strip()
    return s


def _strip_custom_emoji_from_params(params):
    """Fallback sanitizer if a non-Premium test bot encounters CUSTOM_EMOJI / BUTTON_ICON_INVALID."""
    out = dict(params)
    for field in ('text', 'caption'):
        if isinstance(out.get(field), str) and '<tg-emoji' in out[field]:
            out[field] = re.sub(r'<tg-emoji[^>]*>(.*?)</tg-emoji>', r'\1', out[field], flags=re.S)
    rm = out.get('reply_markup')
    if rm:
        try:
            rm_dict = json.loads(rm) if isinstance(rm, str) else (rm.to_dict() if hasattr(rm, 'to_dict') else dict(rm))
            changed = False
            for row in rm_dict.get('inline_keyboard', []):
                for btn in row:
                    if isinstance(btn, dict) and 'icon_custom_emoji_id' in btn:
                        btn.pop('icon_custom_emoji_id', None)
                        changed = True
            if changed:
                out['reply_markup'] = json.dumps(rm_dict, ensure_ascii=False)
        except Exception:
            pass
    return out


class BotThemeAndMediaUpgrade:
    def __init__(self, ns):
        self.ns = ns
        self.bot = ns['bot']
        self.db = ns['db']
        self._premium_supported = True

    def edit_or_send(self, call, text, markup):
        try:
            return self.bot.edit_message_text(
                text, call.message.chat.id, call.message.message_id,
                reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True
            )
        except Exception as exc:
            if 'message is not modified' in str(exc).lower():
                return None
            return self.bot.send_message(
                call.message.chat.id, text,
                reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True
            )

    def upgrade_lang_and_slots(self):
        ns = self.ns
        # 1. Complete missing WebApp Premium Emoji slots (`admin`, `search`, `avail`)
        slots = ns.get('_WA_SLOT_SRC')
        if isinstance(slots, dict):
            slots['admin'] = ('btn_admin', '5217822164362739968')
            if not (slots.get('search') or (None, None))[1]:
                slots['search'] = ('btn_shop_search', '5231012545799666522')
            if not (slots.get('avail') or (None, None))[1]:
                slots['avail'] = ('btn_shop_avail', '5206607081334906820')

        # 2. Expand _PRESERVE_RE so ⭐, ⏳, ⌨️, ⬅️ never split from \uFE0F during translation
        ns['_PRESERVE_RE'] = re.compile(
            r'(<tg-emoji[^>]*>.*?</tg-emoji>'
            r'|<(?:code|pre)[^>]*>.*?</(?:code|pre)>'
            r'|</?[a-zA-Z][^>]*>'
            r'|\{[0-9a-zA-Z_:.!,-]*\}'
            r'|https?://\S+'
            r'|@[A-Za-z0-9_]{3,}'
            r'|[\u2300-\u23FF\u2600-\u27BF\u2B00-\u2BFF\U0001F000-\U0001FAFF][\uFE0F\u200D\U0001F3FB-\U0001F3FF]*)'
        )

        # 3. Upgrade Bot LANG['ar'] & LANG['en'] with luxury cards + fixed Arabic referral DMs
        lang = ns.get('LANG', {})
        ar = lang.setdefault('ar', {})
        en = lang.setdefault('en', {})

        store_name = html.escape(str(ns.get('WEBAPP_STORE_NAME') or 'LARA STOCK'))

        ar.update({
            'welcome': (
                f'<tg-emoji emoji-id="5312361253610475399">🛍</tg-emoji> <b>أهلاً بك في {store_name}</b>\n'
                '<blockquote>'
                '<tg-emoji emoji-id="5262742999678329061">👤</tg-emoji> <b>العميل:</b> <b>{1}</b> · <code>{0}</code>\n'
                '<tg-emoji emoji-id="5445353829304387411">💳</tg-emoji> <b>الرصيد المتاح:</b> <b>${3:.2f}</b>\n'
                '<tg-emoji emoji-id="4942888689131848546">👥</tg-emoji> <b>أعضاء المتجر:</b> <b>{2}</b>'
                '</blockquote>\n'
                '<tg-emoji emoji-id="5258203794772085854">⚡</tg-emoji> <i>تسليم فوري آلي على مدار الساعة — اختر من القائمة بالأسفل:</i>'
            ),
            'store_title': '<tg-emoji emoji-id="5312361253610475399">🛍</tg-emoji> <b>كتالوج المنتجات المتوفرة</b>\n<i>اختر القسم أو المنتج لعرض التفاصيل والشراء الفوري:</i>',
            'profile_txt': (
                '<tg-emoji emoji-id="5262742999678329061">👤</tg-emoji> <b>بطاقة الحساب الشخصي</b>\n'
                '<blockquote>'
                '🆔 <b>المعرّف:</b> <code>{0}</code>\n'
                '👤 <b>الاسم:</b> <b>{1}</b>\n'
                '<tg-emoji emoji-id="5445353829304387411">💳</tg-emoji> <b>الرصيد الحالي:</b> <b>${2:.2f}</b>\n'
                '<tg-emoji emoji-id="5400090058030075645">🛍</tg-emoji> <b>إجمالي الطلبات:</b> <b>{3}</b>\n'
                '<tg-emoji emoji-id="5397916757333654639">💰</tg-emoji> <b>إجمالي الإيداعات:</b> <b>${4:.2f}</b>'
                '</blockquote>'
            ),
            'dep_choose': (
                '<tg-emoji emoji-id="5445353829304387411">💳</tg-emoji> <b>بوابة شحن الرصيد الآلي</b>\n'
                '<i>اختر وسيلة الدفع المناسبة لك — يُضاف الرصيد تلقائياً فور التأكيد:</i>'
            ),
            # Fix English strings that were mistakenly placed inside LANG['ar']!
            'ref_purchase_dm': (
                '<tg-emoji emoji-id="5206607081334906820">🎉</tg-emoji> <b>مكافأة إحالة جديدة!</b>\n'
                '<blockquote>'
                '🛍 قام أحد أصدقائك المدعوين (<b>{}</b>) بإتمام عملية شراء.\n'
                '💵 <b>مكافأتك المضافة:</b> <code>+${}</code>\n'
                '💼 <b>رصيدك الجديد:</b> <b>${}</b>'
                '</blockquote>\n'
                '<i>واصل مشاركة رابطك لتربح مع كل عملية شراء جديدة!</i>'
            ),
            'ref_progress_dm': (
                '<tg-emoji emoji-id="4942888689131848546">📈</tg-emoji> <b>إحالة نشطة جديدة!</b>\n'
                '<blockquote>'
                '✅ <b>إحالاتك النشطة الآن:</b> <b>{0}</b>\n'
                '⏳ <b>المتبقي للمكافأة القادمة:</b> <b>{1}</b> إحالة للحصول على <b>${2}</b>'
                '</blockquote>'
            ),
            'ref_milestone_dm': (
                '<tg-emoji emoji-id="5206607081334906820">🏆</tg-emoji> <b>اكتملت دفعة إحالات جديدة!</b>\n'
                '<blockquote>'
                '👥 <b>الإحالات المكتملة:</b> <b>{0}</b>\n'
                '💰 <b>المكافأة المضافة:</b> <code>+${1}</code>\n'
                '💼 <b>رصيدك الحالي:</b> <b>${2}</b>'
                '</blockquote>'
            ),
        })

        en.update({
            'welcome': (
                f'<tg-emoji emoji-id="5312361253610475399">🛍</tg-emoji> <b>Welcome to {store_name}</b>\n'
                '<blockquote>'
                '<tg-emoji emoji-id="5262742999678329061">👤</tg-emoji> <b>Account:</b> <b>{1}</b> · <code>{0}</code>\n'
                '<tg-emoji emoji-id="5445353829304387411">💳</tg-emoji> <b>Balance:</b> <b>${3:.2f}</b>\n'
                '<tg-emoji emoji-id="4942888689131848546">👥</tg-emoji> <b>Members:</b> <b>{2}</b>'
                '</blockquote>\n'
                '<tg-emoji emoji-id="5258203794772085854">⚡</tg-emoji> <i>24/7 Instant Automated Delivery — choose an option below:</i>'
            ),
            'store_title': '<tg-emoji emoji-id="5312361253610475399">🛍</tg-emoji> <b>Available Catalog</b>\n<i>Select a category or product for instant checkout:</i>',
            'profile_txt': (
                '<tg-emoji emoji-id="5262742999678329061">👤</tg-emoji> <b>Account Overview</b>\n'
                '<blockquote>'
                '🆔 <b>ID:</b> <code>{0}</code>\n'
                '👤 <b>Name:</b> <b>{1}</b>\n'
                '<tg-emoji emoji-id="5445353829304387411">💳</tg-emoji> <b>Balance:</b> <b>${2:.2f}</b>\n'
                '<tg-emoji emoji-id="5400090058030075645">🛍</tg-emoji> <b>Total Orders:</b> <b>{3}</b>\n'
                '<tg-emoji emoji-id="5397916757333654639">💰</tg-emoji> <b>Total Deposited:</b> <b>${4:.2f}</b>'
                '</blockquote>'
            ),
            'dep_choose': (
                '<tg-emoji emoji-id="5445353829304387411">💳</tg-emoji> <b>Instant Wallet Top-Up</b>\n'
                '<i>Select your preferred payment method — credited automatically upon confirmation:</i>'
            ),
            'ref_purchase_dm': (
                '<tg-emoji emoji-id="5206607081334906820">🎉</tg-emoji> <b>New Referral Bonus!</b>\n'
                '<blockquote>'
                '🛍 Your invited friend (<b>{}</b>) completed a purchase.\n'
                '💵 <b>Bonus Earned:</b> <code>+${}</code>\n'
                '💼 <b>New Balance:</b> <b>${}</b>'
                '</blockquote>'
            ),
            'ref_progress_dm': (
                '<tg-emoji emoji-id="4942888689131848546">📈</tg-emoji> <b>New Active Referral!</b>\n'
                '<blockquote>'
                '✅ <b>Active Referrals:</b> <b>{0}</b>\n'
                '⏳ <b>Remaining for next reward:</b> <b>{1}</b> more to earn <b>${2}</b>'
                '</blockquote>'
            ),
            'ref_milestone_dm': (
                '<tg-emoji emoji-id="5206607081334906820">🏆</tg-emoji> <b>Milestone Unlocked!</b>\n'
                '<blockquote>'
                '👥 <b>Completed Referrals:</b> <b>{0}</b>\n'
                '💰 <b>Reward Credited:</b> <code>+${1}</code>\n'
                '💼 <b>Current Balance:</b> <b>${2}</b>'
                '</blockquote>'
            ),
            'log_ref_progress': (
                '📈 <b>New Active Referral!</b>\n\n'
                '👤 Referrer: <b>**</b>\n'
                '✅ Active Referrals: <b>{0}</b>\n'
                '⏳ <b>{1}</b> more to earn <b>${2}</b>'
            ),
        })

    def install(self):
        ns = self.ns
        theme = self
        self.upgrade_lang_and_slots()

        # 1. Automatic Telegram Premium `icon_custom_emoji_id` on Bot Buttons (`get_btn_data`)
        orig_get_btn_data = ns['get_btn_data']
        @functools.wraps(orig_get_btn_data)
        def premium_get_btn_data(uid, key):
            text, emj_id = orig_get_btn_data(uid, key)
            use_auto = (os.getenv('WEBAPP_NAV_EMOJI', 'bot') or 'bot').strip().lower() != 'builtin'
            if not emj_id and use_auto and theme._premium_supported:
                auto_id = AUTO_BUTTON_EMOJIS.get(key)
                if auto_id:
                    return _strip_leading_unicode_emoji(text), auto_id
            elif emj_id:
                return _strip_leading_unicode_emoji(text), emj_id
            return text, emj_id
        ns['get_btn_data'] = premium_get_btn_data

        # 2. Resilient auto-downgrade in `telebot.apihelper._make_request` if bot lacks Premium emoji permission
        telebot_mod = ns['telebot']
        prev_make_request = telebot_mod.apihelper._make_request
        @functools.wraps(prev_make_request)
        def resilient_make_request(token, method_name, method='get', params=None, files=None, **kwargs):
            try:
                return prev_make_request(token, method_name, method=method, params=params, files=files, **kwargs)
            except Exception as exc:
                desc = str(exc).upper()
                if params and any(err_k in desc for err_k in ('BUTTON_ICON_INVALID', 'CUSTOM_EMOJI', 'DOCUMENT_INVALID')):
                    theme._premium_supported = False
                    cleaned = _strip_custom_emoji_from_params(params)
                    return prev_make_request(token, method_name, method=method, params=cleaned, files=files, **kwargs)
                raise
        telebot_mod.apihelper._make_request = resilient_make_request

        # 3. Executive Redesign of `admin_main_ui`, `admin_shop_settings` & `toggle_amount_protection`
        def luxury_admin_main_ui(call):
            self.bot.answer_callback_query(call.id)
            l = ns['get_lang'](call.from_user.id)
            mk = ns['InlineKeyboardMarkup'](row_width=2)
            Btn = ns['InlineKeyboardButton']
            prot_on = ns['is_amount_protection_enabled']()
            if l == 'en':
                mk.add(Btn('📦 Products & Stock', callback_data='ad_prod_manage'),
                       Btn('🔎 Smart Lookup', callback_data='ad_find'))
                mk.add(Btn('👥 Customers', callback_data='ad_users_main'),
                       Btn('🚫 Unified Ban', callback_data='ad_ban_user'))
                mk.add(Btn('💰 Credit / Debit', callback_data='ad_gift'),
                       Btn('🔍 Check TX / Hash', callback_data='ad_check_tx'))
                mk.add(Btn('💹 Profit Analytics', callback_data='ad_profit'),
                       Btn('📊 CSV Reports', callback_data='ad_reports'))
                mk.add(Btn('⚙️ Store & Wallets', callback_data='ad_shop_settings'),
                       Btn('🔬 Wallet Diagnostics', callback_data='ad_wallet_diag'))
                mk.add(Btn('✏️ Customize UI (CMS)', callback_data='ad_texts_main'),
                       Btn('📢 Broadcast', callback_data='ad_bc'))
                mk.add(Btn('🤖 ChatGPT Business', callback_data='ad_cgpt_panel'),
                       Btn('🔌 External Suppliers', callback_data='ext_api_main'))
                mk.add(Btn('🤝 Referrals', callback_data='ad_ref_settings'),
                       Btn('💎 VIP System', callback_data='ad_vip'))
                mk.add(Btn('📢 Forced Channels', callback_data='ad_fsub_list'),
                       Btn('🎓 Activations', callback_data='ad_api_main'))
                mk.add(Btn('🩺 Error Monitor', callback_data='ad_errors'),
                       Btn('📡 Network Meter', callback_data='ad_network'))
                mk.add(Btn('🛒 Cart Settings', callback_data='ad_cart_settings'),
                       Btn('👑 Admins', callback_data='ad_new_admin'))
                mk.add(Btn('📋 Available List', callback_data='ad_plist'),
                       Btn('📜 Logs', callback_data='ad_logs_all'))
                mk.add(Btn(f"🛡 Amount Protection: {'✅ ON' if prot_on else '⚠️ OFF'}", callback_data='toggle_amount_protection'))
                mk.add(Btn('🏠 Main Menu', callback_data='main_menu_refresh'))
                title = (
                    '<tg-emoji emoji-id="5217822164362739968">👑</tg-emoji> <b>Executive Control Center</b>\n'
                    '<blockquote>Manage inventory, live on-chain payments, unified security bans, and storefront settings:</blockquote>'
                )
            else:
                mk.add(Btn('📦 إدارة المنتجات والمخزون', callback_data='ad_prod_manage'),
                       Btn('🔎 البحث الذكي الشامل', callback_data='ad_find'))
                mk.add(Btn('👥 العملاء والأرصدة', callback_data='ad_users_main'),
                       Btn('🚫 الحظر الموحّد الشامل', callback_data='ad_ban_user'))
                mk.add(Btn('💰 شحن / تعديل رصيد', callback_data='ad_gift'),
                       Btn('🔍 فحص معاملة (هاش / ID)', callback_data='ad_check_tx'))
                mk.add(Btn('💹 الأرباح والصافي', callback_data='ad_profit'),
                       Btn('📊 تقارير المبيعات CSV', callback_data='ad_reports'))
                mk.add(Btn('⚙️ إعدادات المتجر والمحافظ', callback_data='ad_shop_settings'),
                       Btn('🔬 فحص اتصال المحافظ', callback_data='ad_wallet_diag'))
                mk.add(Btn('✏️ تخصيص الواجهة والأزرار', callback_data='ad_texts_main'),
                       Btn('📢 النشر والبرودكاست', callback_data='ad_bc'))
                mk.add(Btn('🤖 مقاعد ChatGPT', callback_data='ad_cgpt_panel'),
                       Btn('🔌 متاجر الموردين API', callback_data='ext_api_main'))
                mk.add(Btn('🤝 نظام الإحالات', callback_data='ad_ref_settings'),
                       Btn('💎 مستويات VIP', callback_data='ad_vip'))
                mk.add(Btn('📢 الاشتراك الإجباري', callback_data='ad_fsub_list'),
                       Btn('🎓 إعدادات التفعيلات', callback_data='ad_api_main'))
                mk.add(Btn('🩺 مراقبة الأخطاء', callback_data='ad_errors'),
                       Btn('📡 استهلاك الشبكة', callback_data='ad_network'))
                mk.add(Btn('🛒 إعدادات السلة', callback_data='ad_cart_settings'),
                       Btn('👑 إدارة المشرفين', callback_data='ad_new_admin'))
                mk.add(Btn('📋 جدول المتوفر للنشر', callback_data='ad_plist'),
                       Btn('📜 السجلات العامة', callback_data='ad_logs_all'))
                mk.add(Btn(f"🛡 حماية مبالغ الإيداع: {'✅ مفعّلة' if prot_on else '⚠️ معطّلة'}", callback_data='toggle_amount_protection'))
                mk.add(Btn('🏠 القائمة الرئيسية', callback_data='main_menu_refresh'))
                title = (
                    '<tg-emoji emoji-id="5217822164362739968">👑</tg-emoji> <b>مركز القيادة والإدارة التنفيذية</b>\n'
                    '<blockquote>تحكّم كامل في المخزون، فحص الشبكات المباشر، الحظر الشامل (بوت + موقع + إيميل)، وتخصيص الواجهة:</blockquote>'
                )
            theme.edit_or_send(call, title, mk)

        ns['admin_main_ui'] = luxury_admin_main_ui

        def luxury_admin_shop_settings(call):
            self.bot.answer_callback_query(call.id)
            mk = ns['InlineKeyboardMarkup'](row_width=2)
            Btn = ns['InlineKeyboardButton']
            mk.add(Btn('🔬 فحص اتصال المحافظ والشبكات الآن', callback_data='ad_wallet_diag'))
            mk.add(Btn('💎 عنوان محفظة TON', callback_data='set_v_ton'),
                   Btn('🔑 مفتاح TONCenter (اختياري)', callback_data='set_v_ton_key'))
            mk.add(Btn('🟢 USDT (TRC-20)', callback_data='set_v_usdt'),
                   Btn('🟡 USDT (BEP-20)', callback_data='set_v_usdt_bep20'))
            mk.add(Btn('🔵 عنوان Litecoin (LTC)', callback_data='set_v_ltc'),
                   Btn('💳 Binance Pay ID', callback_data='set_v_wallet'))
            mk.add(Btn('🟠 Bybit API Key', callback_data='set_v_bybit_key'),
                   Btn('🟠 Bybit Secret', callback_data='set_v_bybit_secret'))
            mk.add(Btn('🟠 Bybit UID', callback_data='set_v_bybit_uid'),
                   Btn('🔑 BscScan API Key', callback_data='set_v_bscscan'))
            mk.add(Btn('🟠 Bybit TRC20', callback_data='set_v_bybit_trc20'),
                   Btn('🟠 Bybit BEP20', callback_data='set_v_bybit_bep20'))
            mk.add(Btn('👁 إظهار / إخفاء طرق الدفع', callback_data='hide_coins_menu'))
            mk.add(Btn('📢 قناة السجلات (@)', callback_data='set_v_log'),
                   Btn('🌐 الرابط الأساسي والدومين', callback_data='ad_baseurl'))
            _no = str(ns['get_setting']('notify_open', 'site')) != 'bot'
            mk.add(Btn('📢 تخصيص أزرار النشر', callback_data='ad_cms_notice'))
            mk.add(Btn(f"🔔 وجهة الروابط: {'🛍 الموقع' if _no else '🤖 البوت'}", callback_data='ad_notify_open'))
            mk.add(Btn('🔙 رجوع للوحة الإدارة', callback_data='admin_panel_main'))

            ton_ok = ns['get_setting']('ton_address', 'Not Set') != 'Not Set'
            trc_ok = ns['get_setting']('usdt_address', 'Not Set') != 'Not Set'
            bep_ok = ns['get_setting']('usdt_bep20_address', 'Not Set') != 'Not Set'
            txt = (
                '⚙️ <b>إعدادات المتجر وبوابات الدفع</b>\n'
                '<blockquote>'
                f"💎 TON: {'🟢 مضبوط' if ton_ok else '⚪ غير مضبوط'} · "
                f"🟢 TRC20: {'🟢' if trc_ok else '⚪'} · "
                f"🟡 BEP20: {'🟢' if bep_ok else '⚪'}\n"
                'اضغط «🔬 فحص اتصال المحافظ» للتحقق المباشر من قراءة الشبكات والأسعار.'
                '</blockquote>'
            )
            theme.edit_or_send(call, txt, mk)

        ns['admin_shop_settings'] = ns['admin_required'](luxury_admin_shop_settings)

        # Update callback_query_handlers table for `ad_shop_settings` & `set_v_ton_key`
        orig_set_inputs = ns['admin_set_inputs']
        @functools.wraps(orig_set_inputs)
        def upgraded_admin_set_inputs(call):
            if call.data == 'set_v_ton_key':
                self.bot.answer_callback_query(call.id)
                uid = call.from_user.id
                cur = ns['get_setting']('toncenter_api_key', 'Not Set')
                shown = ('••••••' + cur[-4:]) if cur and cur != 'Not Set' and len(cur) > 4 else 'غير مضبوط (يعمل بالوضع المجاني التلقائي)'
                msg = self.bot.send_message(
                    uid,
                    f"🔑 <b>مفتاح TONCenter API (اختياري لتسريع الفحص)</b>\n\n"
                    f"📌 الحالة الحالية: <code>{html.escape(shown)}</code>\n"
                    f"💡 البوت يفحص TON مجاناً عبر TonAPI + TONCenter تلقائياً، ويمكنك وضع مفتاح من @tonapibot لرفع حد الطلبات، أو إرسال <code>off</code> لمسحه.\n\n"
                    f"👇 أرسل المفتاح الجديد (أو <b>الغاء</b>):",
                    parse_mode='HTML'
                )
                self.bot.register_next_step_handler(msg, ns['admin_save_setting'], 'set_v_ton_key')
                return
            return orig_set_inputs(call)
        ns['admin_set_inputs'] = upgraded_admin_set_inputs

        if '_validate_setting_value' in ns:
            orig_val_setting = ns['_validate_setting_value']
            @functools.wraps(orig_val_setting)
            def ext_validate_setting(mode, val):
                if mode == 'set_v_ton_key':
                    v = (val or '').strip()
                    if v.lower() in ('off', 'not set', 'حذف', 'مسح'):
                        return True, ''
                    if len(v) < 16 or ' ' in v:
                        return False, '❌ المفتاح غير صالح — انسخه كاملاً بدون مسافات أو أرسل off.'
                    return True, ''
                return orig_val_setting(mode, val)
            ns['_validate_setting_value'] = ext_validate_setting

        # Patch registered callback handlers pointing to old `admin_shop_settings` / `admin_set_inputs` / `toggle_amount_protection`
        orig_toggle_prot = ns['toggle_amount_protection']
        @functools.wraps(orig_toggle_prot)
        def refreshed_toggle_prot(call):
            res = orig_toggle_prot(call)
            try:
                luxury_admin_main_ui(call)
            except Exception:
                pass
            return res
        ns['toggle_amount_protection'] = refreshed_toggle_prot

        for h in self.bot.callback_query_handlers:
            fn = h.get('function') if isinstance(h, dict) else getattr(h, 'function', None)
            name = getattr(fn, '__name__', '')
            if name == 'admin_shop_settings':
                if isinstance(h, dict):
                    h['function'] = ns['admin_shop_settings']
                else:
                    h.function = ns['admin_shop_settings']
            elif name == 'admin_set_inputs':
                wrapped_set = ns['admin_required'](upgraded_admin_set_inputs)
                if isinstance(h, dict):
                    h['function'] = wrapped_set
                else:
                    h.function = wrapped_set
            elif name == 'toggle_amount_protection':
                wrapped_tp = ns['admin_required'](refreshed_toggle_prot)
                if isinstance(h, dict):
                    h['function'] = wrapped_tp
                else:
                    h.function = wrapped_tp

        # Add wallet diagnostics to Mini App Admin sections too!
        if isinstance(ns.get('_ADM_SECTIONS'), list):
            if not any(s[0] == 'wdiag' for s in ns['_ADM_SECTIONS']):
                ns['_ADM_SECTIONS'].insert(7, ('wdiag', '🔬', 'فحص اتصال المحافظ (TON/USDT)', 'Wallet & TON diagnostics', 'ad_wallet_diag'))

        # 4. Crisp 192x192 Lanczos + UnsharpMask WebP fallback for iPhone/Safari (`_wa_emoji_webp`)
        if '_wa_emoji_webp' in ns:
            orig_emoji_webp = ns['_wa_emoji_webp']
            @functools.wraps(orig_emoji_webp)
            def crisp_wa_emoji_webp(eid):
                res = orig_emoji_webp(eid)
                if not res or res[0] != 'image/webp':
                    return res
                try:
                    from PIL import Image, ImageFilter
                    with Image.open(io.BytesIO(res[1])) as im:
                        # Only sharpen/upscale static fallback thumbnails (< 140px), leave animated WebPs untouched
                        if getattr(im, 'n_frames', 1) == 1 and im.width < 160:
                            rgba = im.convert('RGBA').resize((192, 192), Image.Resampling.LANCZOS)
                            rgba = rgba.filter(ImageFilter.UnsharpMask(radius=1.1, percent=115, threshold=2))
                            buf = io.BytesIO()
                            rgba.save(buf, 'WEBP', quality=92, method=6)
                            out = ('image/webp', buf.getvalue())
                            ns['_wa_cache_put'](f"cew_{eid}", out[0], out[1])
                            return out
                except Exception:
                    pass
                return res
            ns['_wa_emoji_webp'] = crisp_wa_emoji_webp

        # 5. Clean Luxury `/og.png` Social Preview Generator without broken Arabic tofu glyphs
        if '_web_og_image' in ns:
            def clean_web_og_image():
                hit = ns['_WEB_ICON_CACHE'].get('og')
                if hit:
                    return hit
                try:   # designed cover (Figma) shipped with the site wins over the generated one
                    cover = os.path.join(ns['WEBAPP_DIR'], 'og-cover.png')
                    if os.path.exists(cover):
                        with open(cover, 'rb') as f:
                            data = f.read()
                        if data:
                            ns['_WEB_ICON_CACHE']['og'] = data
                            return data
                except Exception:
                    pass
                try:
                    from PIL import Image, ImageDraw, ImageFilter, ImageFont
                    W, H = 1200, 630
                    im = Image.new('RGBA', (W, H), (7, 10, 18, 255))
                    glow = Image.new('RGBA', (W, H), (0, 0, 0, 0))
                    gd = ImageDraw.Draw(glow)
                    gd.ellipse((140, -140, 1060, 560), fill=(37, 118, 255, 88))
                    gd.ellipse((360, 180, 840, 650), fill=(16, 185, 129, 44))
                    im = Image.alpha_composite(im, glow.filter(ImageFilter.GaussianBlur(95)))
                    d = ImageDraw.Draw(im)
                    # Subtle luxury card frame
                    d.rounded_rectangle((64, 54, W - 64, H - 54), radius=42, fill=(14, 21, 35, 210), outline=(96, 165, 250, 58), width=2)
                    src = ns['_web_logo_rgba']()
                    if src:
                        s = 230
                        ratio = min(s / src.width, s / src.height)
                        nw, nh = max(1, round(src.width * ratio)), max(1, round(src.height * ratio))
                        mark = src.resize((nw, nh), Image.Resampling.LANCZOS)
                        im.alpha_composite(mark, ((W - nw) // 2, 118 + (s - nh) // 2))
                    raw_name = str(ns.get('WEBAPP_STORE_NAME') or 'LARA STOCK').strip()
                    # Avoid Pillow default-font Arabic tofu boxes by using ASCII brand title if non-ASCII
                    safe_title = raw_name if all(ord(c) < 256 for c in raw_name) else 'LARA STOCK'
                    try:
                        f_title = ImageFont.truetype('arialbd.ttf', 58)
                        f_sub = ImageFont.truetype('arial.ttf', 26)
                    except Exception:
                        f_title = ImageFont.load_default(size=54)
                        f_sub = ImageFont.load_default(size=24)
                    d.text((W // 2, 425), safe_title.upper(), font=f_title, fill=(241, 245, 255, 255), anchor='mm')
                    d.text((W // 2, 488), 'INSTANT DIGITAL STORE  •  24/7 AUTOMATED DELIVERY', font=f_sub, fill=(96, 165, 250, 230), anchor='mm')
                    buf = io.BytesIO()
                    im.convert('RGB').save(buf, 'PNG', optimize=True)
                    data = buf.getvalue()
                    ns['_WEB_ICON_CACHE']['og'] = data
                    return data
                except Exception:
                    return b''
            ns['_web_og_image'] = clean_web_og_image

        # 6. Fix `_net_server_route` so standalone Website static assets aren't miscounted as `/api/other`
        if '_net_server_route' in ns:
            orig_route = ns['_net_server_route']
            @functools.wraps(orig_route)
            def accurate_net_server_route(path):
                clean = str(path or '').split('?', 1)[0]
                if clean.startswith(('/emoji/', '/lib/', '/logo', '/icon-', '/favicon', '/apple-touch-icon', '/og.png', '/manifest.webmanifest')):
                    return '/site/assets'
                return orig_route(path)
            ns['_net_server_route'] = accurate_net_server_route

        return self


def install(namespace):
    if namespace.get('_BOT_THEME_UPGRADE') is not None:
        return namespace['_BOT_THEME_UPGRADE']
    service = BotThemeAndMediaUpgrade(namespace).install()
    namespace['_BOT_THEME_UPGRADE'] = service
    return service
