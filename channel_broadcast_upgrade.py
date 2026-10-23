"""Additive channel composer: multiple products and immutable native formatting.

The original handlers remain available. No worker, polling loop or user balance
is introduced here; drafts use the existing channel_broadcasts collection.
"""
import copy
import html
import json
import re
import threading
import time

MAX_PRODUCTS = 20
MAX_MESSAGES = 20
PAGE_SIZE = 8
_HTML = re.compile(r'</?(?:b|strong|i|em|u|ins|s|strike|del|span|tg-spoiler|tg-emoji|tg-time|a|code|pre|blockquote)(?:\s|>)', re.I)
_NATIVE = {'bold', 'italic', 'underline', 'strikethrough', 'spoiler', 'code', 'pre',
           'blockquote', 'expandable_blockquote', 'text_link', 'text_mention', 'custom_emoji'}


def _plain(value):
    return json.loads(json.dumps(value, ensure_ascii=False,
        default=lambda v: v.to_dict() if hasattr(v, 'to_dict') else vars(v)))


def _clip(value, limit=60):
    value = str(value)
    while len(value.encode('utf-16-le')) // 2 > limit:
        value = value[:-1]
    return value


def message_snapshot(message):
    """Keep Telegram's UTF-16 offsets and entity metadata without HTML conversion."""
    kind = getattr(message, 'content_type', 'text')
    text = getattr(message, 'text', None) if kind == 'text' else getattr(message, 'caption', None)
    entities = _plain((getattr(message, 'entities', None) if kind == 'text'
                       else getattr(message, 'caption_entities', None)) or [])
    raw_html = bool(text and _HTML.search(text) and not any(e.get('type') in _NATIVE for e in entities))
    result = {'kind': kind, 'text': text, 'entities': entities,
              'syntax': 'HTML' if raw_html else 'native'}
    if kind == 'rich_message':
        source = getattr(message, 'json', None) or {}
        result['rich_message'] = _plain(source.get('rich_message') or getattr(message, 'rich_message', None))
    if kind in ('photo', 'video', 'animation', 'document', 'audio', 'voice'):
        media = getattr(message, kind, None)
        if kind == 'photo' and media:
            media = media[-1]
        if media and getattr(media, 'file_id', None):
            result['file_id'] = media.file_id
        for key in ('has_media_spoiler', 'show_caption_above_media'):
            value = getattr(message, key, None)
            if value is not None:
                result[key] = bool(value)
    options = getattr(message, 'link_preview_options', None)
    if kind == 'text' and options:
        result['link_preview_options'] = _plain(options)
    return result


def has_premium(payload):
    if not payload:
        return False
    def rich_premium(value):
        if isinstance(value, dict):
            return value.get('type') == 'custom_emoji' or any(rich_premium(v) for v in value.values())
        return isinstance(value, list) and any(rich_premium(v) for v in value)
    return bool(any(e.get('type') == 'custom_emoji' for e in payload.get('entities', []))
                or re.search(r'<tg-emoji\b', payload.get('text') or '', re.I)
                or rich_premium(payload.get('rich_message')))


class ChannelComposer:
    def __init__(self, namespace):
        self.g = namespace
        self.bot = namespace['bot']
        self.db = namespace['db']
        self.original = {name: namespace[name] for name in
                         ('_chbc_prompt', '_chbc_receive', '_chbc_controls', '_chbc_preview', '_chbc_publish')}
        self.catalog = None
        self.catalog_until = 0
        self.catalog_lock = threading.Lock()

    def load(self, token, uid):
        return self.g['_chbc_load'](token, uid)

    def save(self, doc, values, **extra):
        return self.db.channel_broadcasts.find_one_and_update(
            {'_id': doc['_id'], 'owner_id': doc['owner_id'], 'status': doc['status'],
             'revision': doc['revision']},
            {'$set': values, '$inc': {'revision': 1}, **extra}, return_document=True)

    def product_ids(self, doc):
        if 'product_ids' in doc:
            return list(doc['product_ids'])
        return [doc['product_id']] if doc.get('product_id') else []

    def parse_links(self, value):
        parts = [p for p in re.split(r'[\s,،;؛]+', str(value or '').strip()) if p]
        if not parts or len(parts) > 100:
            raise ValueError('أرسل روابط المنتجات أو اختصاراتها، كل رابط في سطر. الحد 20 منتجاً مختلفاً.')
        ids = []
        for part in parts:
            pid = self.g['_chbc_parse_link'](part)
            if pid not in ids:
                ids.append(pid)
        if None in ids and len(ids) > 1:
            raise ValueError('اختر رابط المتجر وحده، أو روابط المنتجات دون رابط المتجر العام.')
        if len(ids) > MAX_PRODUCTS:
            raise ValueError('الحد 20 منتجاً في المنشور الواحد؛ قسّم القائمة إلى منشورين.')
        return [] if ids == [None] else ids

    def prompt(self, uid, token, stage):
        if stage not in ('link', 'append'):
            return self.original['_chbc_prompt'](uid, token, stage)
        mk = self.g['InlineKeyboardMarkup'](row_width=1)
        doc = self.load(token, uid)
        if not doc:
            return
        if stage == 'link':
            mk.add(self.g['InlineKeyboardButton']('📦 اختيار عدة منتجات', callback_data=f'chbcx_pick_{token}_{doc["revision"]}_0'))
            if self.product_ids(doc):
                mk.add(self.g['InlineKeyboardButton']('✅ استخدام المنتجات المحددة', callback_data=f'chbcx_done_{token}_{doc["revision"]}'))
            mk.add(self.g['InlineKeyboardButton']('🏪 رابط المتجر كله', callback_data=f'chbcx_store_{token}_{doc["revision"]}'))
            text = ('🔗 أرسل روابط المنتجات معاً، كل رابط في سطر، أو اخترها من الأزرار.\n'
                    'تُقبل روابط البوت والميني آب واختصارات المنتجات. يمكنك ربط حتى 20 منتجاً.\n'
                    'يمكنك أيضاً إرسال رابط المتجر فقط. للإلغاء: /cancel')
        else:
            text = ('➕ أرسل الرسالة الإضافية بتنسيقات Telegram أو وسائطها ووصفها.\n'
                    'ستُضاف إلى الرسائل الحالية بالترتيب، ثم تظهر معاينة جديدة قبل التأكيد.\n'
                    'للإلغاء: /cancel')
        mk.add(self.g['InlineKeyboardButton']('❌ إلغاء', callback_data='chbc_cancel_' + token))
        msg = self.bot.send_message(uid, text, reply_markup=mk)
        self.bot.clear_step_handler_by_chat_id(uid)
        self.bot.register_next_step_handler(msg, self.g['_chbc_receive'], token, stage)

    def receive(self, message, token, stage):
        if stage not in ('message', 'link', 'append'):
            return self.original['_chbc_receive'](message, token, stage)
        doc = self.g['_chbc_step_doc'](message, token, stage)
        if not doc:
            return
        uid = message.from_user.id
        try:
            if stage in ('message', 'append'):
                if getattr(message, 'content_type', None) not in ('text', 'photo', 'video', 'animation',
                        'document', 'audio', 'voice', 'video_note', 'sticker', 'location', 'venue', 'contact', 'poll', 'dice', 'rich_message'):
                    raise ValueError('أرسل نصاً أو وسائط عادية قابلة للنشر.')
                sources = list(doc.get('sources', [])) if stage == 'append' else []
                payloads = dict(doc.get('payloads', {})) if stage == 'append' else {}
                if len(sources) >= MAX_MESSAGES:
                    raise ValueError('الحد 20 رسالة في المنشور؛ أنشئ منشوراً إضافياً.')
                mid = message.message_id
                sources.append(mid)
                payloads[str(mid)] = message_snapshot(message)
                saved = self.save(doc, {'source_chat': uid, 'sources': sources, 'payloads': payloads,
                                       'stage': 'preview' if stage == 'append' else 'link', 'kind': 'custom'})
                if saved:
                    if stage == 'append':
                        self.preview(token, uid)
                    else:
                        self.prompt(uid, token, 'link')
            else:
                ids = self.parse_links(message.text)
                if self.save(doc, {'product_ids': ids, 'product_id': ids[0] if ids else None, 'stage': 'preview'}):
                    self.preview(token, uid)
        except Exception as error:
            self.g['logger'].warning('Channel composer input failed: %s', self.g['_error_redact'](str(error)))
            current = self.load(token, uid)
            self.bot.send_message(uid, '❌ ' + self.g['_error_redact'](str(error))[:500],
                reply_markup=self.controls(current) if current and current.get('stage') == 'preview' else None)
            if current and current['status'] == 'draft' and current.get('stage') in ('message', 'link', 'append'):
                self.prompt(uid, token, current['stage'])

    def controls(self, doc):
        mk = self.original['_chbc_controls'](doc)
        if doc['kind'] == 'custom':
            token, rev = doc['_id'], doc['revision']
            mk.add(self.g['InlineKeyboardButton']('📦 المنتجات المرتبطة (' + str(len(self.product_ids(doc))) + ')',
                callback_data=f'chbcx_pick_{token}_{rev}_0'))
            if len(doc.get('sources', [])) < MAX_MESSAGES:
                mk.add(self.g['InlineKeyboardButton']('➕ إضافة رسالة أخرى', callback_data=f'chbcx_append_{token}_{rev}'))
        return mk

    def name(self, pid):
        if pid.startswith('ext_'):
            row = self.db.ext_products.find_one({'_id': self.g['ObjectId'](pid[4:])}) or {}
        elif pid.startswith('cgpt_main_'):
            row = self.db.cgpt_products.find_one({'_id': self.g['ObjectId'](pid[10:])}) or {}
        else:
            row = self.g['find_product'](pid) or {}
        return html.unescape(re.sub(r'<[^>]+>', '', str(row.get('name_en') or row.get('name') or row.get('name_ar') or pid)))

    def markup(self, doc):
        ids = self.product_ids(doc)
        if len(ids) <= 1:
            return self.g['_notice_pair'](doc['owner_id'], ids[0] if ids else None,
                                          channel=True, lang=doc['lang']).to_dict()
        rows = []
        for pid in ids:
            pair = self.g['_notice_pair'](doc['owner_id'], pid, channel=True, lang=doc['lang']).to_dict()
            buttons = [b for row in pair['inline_keyboard'] for b in row]
            name = self.name(pid)
            for button in buttons:
                button['text'] = _clip(button['text'] + ' · ' + name)
            rows.append(buttons)
        return {'inline_keyboard': rows}

    def render(self, target, doc, mid, markup, preview=False):
        payload = doc.get('payloads', {}).get(str(mid))
        # Native copy remains the fast path for unchanged plain text and other message types.
        rich = payload and (payload.get('entities') or payload.get('syntax') == 'HTML')
        if not rich or (preview and payload.get('syntax') != 'HTML'):
            return self.bot.copy_message(target, doc['source_chat'], mid,
                                         reply_markup=json.dumps(markup, ensure_ascii=False)), None
        options = {'reply_markup': json.dumps(markup, ensure_ascii=False), 'parse_mode': ''}
        text = payload.get('text') or ''
        native = payload.get('syntax') != 'HTML'
        if not native:
            options['parse_mode'] = 'HTML'
        else:
            entities = [self.g['types'].MessageEntity.de_json(e) for e in payload.get('entities', [])]
            options['entities' if payload['kind'] == 'text' else 'caption_entities'] = entities
        if payload['kind'] == 'text':
            if payload.get('link_preview_options'):
                options['link_preview_options'] = self.g['types'].LinkPreviewOptions.de_json(payload['link_preview_options'])
            result = self.bot.send_message(target, text, **options)
        elif payload.get('file_id'):
            if payload['kind'] in ('photo', 'video', 'animation'):
                if 'has_media_spoiler' in payload:
                    options['has_spoiler'] = payload['has_media_spoiler']
                if 'show_caption_above_media' in payload:
                    options['show_caption_above_media'] = payload['show_caption_above_media']
            result = getattr(self.bot, 'send_' + payload['kind'])(target, payload['file_id'], caption=text, **options)
        else:
            options.pop('entities', None)
            result = self.bot.copy_message(target, doc['source_chat'], mid, caption=text, **options)
        warning = None
        if has_premium(payload):
            actual = (result.get('entities') or result.get('caption_entities') or []) if isinstance(result, dict) else (
                getattr(result, 'entities', None) or getattr(result, 'caption_entities', None) or [])
            expected = {str(e['custom_emoji_id']) for e in payload.get('entities', []) if e.get('type') == 'custom_emoji'}
            expected.update(re.findall(r'''emoji-id\s*=\s*['"](\d+)['"]''', text))
            received = {str(e.get('custom_emoji_id') if isinstance(e, dict) else getattr(e, 'custom_emoji_id', '')) for e in actual}
            if expected - received:
                warning = 'Telegram لم يُرجع إيموجيات Premium المطلوبة في الرسالة المنشورة. تحقق من صلاحيات البوت لإرسالها في القنوات.'
        return result, warning

    def preview(self, token, uid):
        doc = self.load(token, uid)
        if not doc or doc['status'] not in ('draft', 'preview') or not doc.get('sources') or not doc.get('channel'):
            raise ValueError('أكمل الرسالة والقناة والرابط أولاً، أو ابدأ منشوراً جديداً.')
        self.g['_chbc_site_ready']()
        for pid in self.product_ids(doc):
            self.g['_chbc_parse_link'](pid)
        claimed = self.save(doc, {'status': 'previewing'})
        if not claimed:
            return
        try:
            self.g['_chbc_verify_channel'](doc['channel']['username'], uid, doc['channel']['id'])
            markup, frozen, payloads = self.markup(doc), [], {}
            for i, mid in enumerate(doc['sources']):
                mk = markup if i == len(doc['sources']) - 1 else {'inline_keyboard': []}
                result, _ = self.render(uid, doc, mid, mk, preview=True)
                frozen_mid = self.g['_chbc_mid'](result)
                frozen.append(frozen_mid)
                if str(mid) in doc.get('payloads', {}):
                    payloads[str(frozen_mid)] = copy.deepcopy(doc['payloads'][str(mid)])
            changes = {'status': 'preview', 'stage': 'preview', 'sources': frozen, 'source_chat': uid,
                       'payloads': payloads, 'markup': markup, 'previewed_at': time.time()}
            self.db.channel_broadcasts.update_one({'_id': token, 'status': 'previewing'}, {'$set': changes})
            claimed.update(changes)
            text = (f'👁 هذه معاينة المنشور النهائي بأزراره.\n📣 القناة: {doc["channel"]["username"]} — {doc["channel"]["title"]}\n'
                    f'📦 المنتجات: {len(self.product_ids(doc))} | الرسائل: {len(frozen)}\n'
                    f'🌐 لغة الأزرار: {"العربية" if doc["lang"] == "ar" else "English"}\n'
                    'هل توافق على نشر هذه النسخة؟ لم يُرسل شيء للقناة بعد.\n'
                    'زر الموقع في القناة يحتاج Main Mini App مفعّلة في BotFather.')
            if any(has_premium(p) for p in payloads.values()):
                text += '\nإيموجيات Premium في القنوات تعتمد على صلاحيات Telegram للبوت؛ ظهورها في الخاص وحده لا يثبت دعمها في القناة.'
            self.bot.send_message(uid, text, reply_markup=self.controls(claimed))
        except Exception:
            self.db.channel_broadcasts.update_one({'_id': token, 'status': 'previewing'}, {'$set': {'status': 'draft'}})
            raise

    def publish(self, token, uid, revision):
        doc = self.load(token, uid)
        if not doc or doc['status'] != 'preview' or doc['revision'] != revision:
            self.bot.send_message(uid, 'هذه الموافقة قديمة أو المحاولة نُفذت بالفعل. راجع أحدث معاينة.')
            return
        self.g['_chbc_verify_channel'](doc['channel']['username'], uid, doc['channel']['id'])
        self.g['_chbc_site_ready']()
        claimed = self.db.channel_broadcasts.find_one_and_update({'_id': token, 'owner_id': uid,
            'status': 'preview', 'revision': revision}, {'$set': {'status': 'sending',
            'started_at': time.time(), 'sent_messages': []}}, return_document=True)
        if not claimed:
            return
        sent, warnings = [], []
        try:
            for i, mid in enumerate(doc['sources']):
                if i:
                    self.g['time'].sleep(1.05)
                self.db.channel_broadcasts.update_one({'_id': token, 'status': 'sending'}, {'$set': {'inflight_index': i}})
                markup = doc['markup'] if i == len(doc['sources']) - 1 else {'inline_keyboard': []}
                result, warning = self.render(doc['channel']['id'], doc, mid, markup)
                sent.append(self.g['_chbc_mid'](result))
                if warning:
                    warnings.append(warning)
                self.db.channel_broadcasts.update_one({'_id': token, 'status': 'sending'},
                    {'$set': {'sent_messages': sent, 'format_warnings': warnings}, '$unset': {'inflight_index': ''}})
            self.db.channel_broadcasts.update_one({'_id': token, 'status': 'sending'},
                {'$set': {'status': 'sent', 'finished_at': time.time()}})
        except Exception as error:
            try:
                self.db.channel_broadcasts.update_one({'_id': token, 'status': 'sending'}, {'$set': {
                    'status': 'needs_review', 'sent_messages': sent, 'error': self.g['_error_redact'](str(error))[:500]}})
            except Exception:
                self.g['logger'].exception('Channel publication state could not be saved: reference=%s', token)
            self.g['logger'].exception('Channel publication needs review: reference=%s', token)
            self.bot.send_message(uid, f'⚠️ توقف النشر. الرسائل المؤكد إرسالها: {len(sent)}.\n'
                'قد تكون الرسالة الأخيرة وصلت رغم انقطاع الرد؛ افحص القناة. لن يعاد الإرسال تلقائياً.\n'
                + 'السبب: ' + self.g['_error_redact'](str(error))[:300] + '\n'
                + ('قد يكون رفض Premium مرتبطاً بصلاحيات Telegram للبوت.\n' if any(has_premium(p) for p in doc.get('payloads', {}).values()) else '')
                + f'مرجع المحاولة: {token}')
            return
        link = f'https://t.me/{doc["channel"]["username"].lstrip("@")}/{sent[-1]}'
        self.bot.send_message(uid, f'✅ نُشر في {doc["channel"]["username"]}.\nعدد الرسائل: {len(sent)}\n{link}'
            + ('\n⚠️ ' + '\n'.join(dict.fromkeys(warnings)) if warnings else ''))

    def candidates(self):
        with self.catalog_lock:
            if self.catalog is None or self.catalog_until <= time.monotonic():
                items, seen = [], set()
                for row in self.g['_plist_rows']('en', link_target='site'):
                    query = self.g['urllib'].parse.parse_qs(self.g['urllib'].parse.urlsplit(row['url']).query)
                    pid = (query.get('startapp') or query.get('start') or [''])[0]
                    if pid.startswith('p_'):
                        pid = pid[2:]
                    elif pid.startswith('vext_'):
                        pid = 'ext_' + pid[5:]
                    if pid.startswith('cgpt_') and not pid.startswith('cgpt_main_'):
                        pid = 'cgpt_main_' + pid[5:]
                    if pid and pid not in seen:
                        items.append({'id': pid, 'name': row['name'], 'emoji': row.get('emoji')})
                        seen.add(pid)
                    if len(items) >= 1000:
                        break
                self.catalog, self.catalog_until = items, time.monotonic() + 60
            return list(self.catalog)

    def picker(self, doc, page=0):
        items = self.candidates()
        page = max(0, min(page, max(0, (len(items) - 1) // PAGE_SIZE)))
        rows = items[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
        if doc['status'] == 'preview' or doc.get('stage') != 'link':
            doc = self.save(doc, {'status': 'draft', 'stage': 'link'})
            if not doc:
                return
        saved = self.db.channel_broadcasts.find_one_and_update({'_id': doc['_id'], 'owner_id': doc['owner_id'],
            'status': 'draft', 'revision': doc['revision']}, {'$set': {'picker_ids': [r['id'] for r in rows],
            'picker_page': page}, '$inc': {'revision': 1}}, return_document=True)
        if not saved:
            return
        token, rev, selected = saved['_id'], saved['revision'], self.product_ids(saved)
        mk = self.g['InlineKeyboardMarkup'](row_width=2)
        for index, row in enumerate(rows):
            mk.add(self.g['CustomInlineButton'](_clip(('✅ ' if row['id'] in selected else '▫️ ') + row['name']),
                icon_custom_emoji_id=row.get('emoji'),
                callback_data=f'chbcx_toggle_{token}_{rev}_{index}'))
        nav = []
        for p, label in ((page - 1, '⬅️ السابق'), (page + 1, 'التالي ➡️')):
            if 0 <= p and p * PAGE_SIZE < len(items):
                nav.append(self.g['InlineKeyboardButton'](label, callback_data=f'chbcx_pick_{token}_{rev}_{p}'))
        if nav:
            mk.row(*nav)
        mk.add(self.g['InlineKeyboardButton']('✅ تم الاختيار (' + str(len(selected)) + ')', callback_data=f'chbcx_done_{token}_{rev}'))
        mk.add(self.g['InlineKeyboardButton']('🏪 رابط المتجر كله', callback_data=f'chbcx_store_{token}_{rev}'))
        mk.add(self.g['InlineKeyboardButton']('❌ إلغاء', callback_data='chbc_cancel_' + token))
        msg = self.bot.send_message(doc['owner_id'],
            f'📦 اختر عدة منتجات ثم اضغط «تم الاختيار». المحدد: {len(selected)}/{MAX_PRODUCTS}.\n'
            'القائمة تعرض المنتجات المتوفرة. يمكنك أيضاً إرسال روابط المنتجات معاً، كل رابط في سطر.', reply_markup=mk)
        self.bot.clear_step_handler_by_chat_id(doc['owner_id'])
        self.bot.register_next_step_handler(msg, self.g['_chbc_receive'], token, 'link')

    def callback(self, call):
        self.bot.answer_callback_query(call.id)
        uid = call.from_user.id
        if call.message.chat.id != uid or call.message.chat.type != 'private':
            return
        match = re.fullmatch(r'chbcx_(pick|toggle|done|store|append)_([a-f0-9]{24})_(\d{1,10})(?:_(\d{1,5}))?', call.data)
        if not match:
            return
        action, token, rev, number = match.groups()
        doc = self.load(token, uid)
        if not doc or doc['status'] not in ('draft', 'preview') or doc['revision'] != int(rev):
            self.bot.send_message(uid, 'هذه القائمة قديمة؛ استخدم آخر معاينة أو قائمة منتجات.')
            return
        try:
            if action == 'append':
                if doc['kind'] != 'custom' or len(doc.get('sources', [])) >= MAX_MESSAGES:
                    return
                if self.save(doc, {'status': 'draft', 'stage': 'append'}):
                    self.prompt(uid, token, 'append')
            elif action == 'pick':
                if doc['kind'] == 'custom' and doc.get('sources'):
                    self.picker(doc, int(number or 0))
            elif action == 'toggle':
                if doc['status'] != 'draft' or doc.get('stage') != 'link' or number is None:
                    return
                index = int(number)
                if index >= len(doc.get('picker_ids', [])):
                    return
                pid = self.g['_chbc_parse_link'](doc['picker_ids'][index])
                selected = self.product_ids(doc)
                if pid in selected:
                    selected.remove(pid)
                else:
                    if len(selected) >= MAX_PRODUCTS:
                        raise ValueError('يمكنك اختيار حتى 20 منتجاً في المنشور الواحد.')
                    selected.append(pid)
                saved = self.save(doc, {'product_ids': selected, 'product_id': selected[0] if selected else None})
                if saved:
                    self.picker(saved, doc.get('picker_page', 0))
            else:
                if doc['status'] != 'draft' or doc.get('stage') != 'link':
                    return
                selected = [] if action == 'store' else self.product_ids(doc)
                if action == 'done' and not selected:
                    raise ValueError('حدد منتجاً أولاً أو اختر رابط المتجر كله.')
                saved = self.save(doc, {'product_ids': selected, 'product_id': selected[0] if selected else None, 'stage': 'preview'})
                if saved:
                    self.bot.clear_step_handler_by_chat_id(uid)
                    self.preview(token, uid)
        except Exception as error:
            self.bot.send_message(uid, '❌ ' + self.g['_error_redact'](str(error))[:500])


def install(namespace):
    composer = ChannelComposer(namespace)
    for name in ('prompt', 'receive', 'controls', 'preview', 'publish'):
        namespace['_chbc_' + name] = getattr(composer, name)
    namespace['_ADMIN_CB_PREFIXES'] += ('chbcx_',)
    bot = namespace['bot']
    before = len(bot.callback_query_handlers)
    bot.callback_query_handler(func=lambda call: str(call.data or '').startswith('chbcx_'))(
        namespace['admin_required'](composer.callback))
    handlers = bot.callback_query_handlers[before:]
    del bot.callback_query_handlers[before:]
    bot.callback_query_handlers[0:0] = handlers
    namespace['_channel_composer'] = composer
    return composer
