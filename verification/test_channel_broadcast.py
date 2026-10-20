"""Channel publishing is tested with fake Telegram responses; no messages leave this process."""
import json
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest
from harness import MESSAGES, reply
from test_integration import mod, isolate, product, server, request
from test_products_menu import callback, dispatch, buttons


def message(mod, text='', uid=9001, mid=111, content=None):
    data = {'message_id': mid, 'date': 1, 'chat': {'id': uid, 'type': 'private'},
            'from': {'id': uid, 'is_bot': False, 'first_name': 'Admin'}, 'text': text}
    if content:
        data.pop('text'); data.update(content)
    return mod.types.Message.de_json(data)


@pytest.fixture(autouse=True)
def telegram(mod, monkeypatch):
    monkeypatch.setattr(mod, '_wa_public_url', lambda: 'https://local.example/app/')
    monkeypatch.setattr(mod, 'get_bot_username', lambda: 'lara_local_test_bot')
    monkeypatch.setattr(mod.bot, 'get_me', lambda: types.SimpleNamespace(
        id=123456789, username='lara_local_test_bot', has_main_web_app=True))
    monkeypatch.setattr(mod.bot, 'get_chat', lambda name: types.SimpleNamespace(
        id=-100777, type='channel', username='news_store', title='News & Store'))
    monkeypatch.setattr(mod.bot, 'get_chat_member', lambda cid, uid: types.SimpleNamespace(
        status='administrator', can_post_messages=True))
    monkeypatch.setattr(mod.bot, 'register_next_step_handler', Mock())
    monkeypatch.setattr(mod.bot, 'clear_step_handler_by_chat_id', Mock())
    monkeypatch.setitem(mod._VC_ORIG, 'edit_message_text',
        lambda text, chat_id, message_id, **kw: reply(chat_id, text, **kw))
    copied = []
    lock = threading.Lock()
    def copy(target, source, mid, **kw):
        with lock:
            copied.append((target, source, mid, kw))
            new_mid = 20000 + len(copied)
        return types.SimpleNamespace(message_id=new_mid)
    monkeypatch.setattr(mod.bot, 'copy_message', copy)
    return copied


def draft(mod):
    dispatch(mod, 'chbc_new', 9001)
    return mod.db.channel_broadcasts.find_one({'owner_id': 9001}, sort=[('created_at', -1)])


def reviewed(mod, pid='101'):
    product(mod)
    d = draft(mod); token = d['_id']
    mod._chbc_receive(message(mod, '@news_store'), token, 'channel')
    mod._chbc_receive(message(mod, '🛍 A formatted original message', mid=112), token, 'message')
    mod._chbc_receive(message(mod, 'p_' + pid), token, 'link')
    return mod.db.channel_broadcasts.find_one({'_id': token})


def public(copied):
    return [c for c in copied if c[0] < 0]


def test_two_broadcast_modes_keep_original_member_flow(mod, telegram):
    dispatch(mod, 'ad_bc', 9001)
    cbs = {b.get('callback_data') for row in buttons(MESSAGES[-1]) for b in row}
    assert {'bc_members', 'chbc_new', 'ad_cms_notice'} <= cbs
    dispatch(mod, 'bc_members', 9001)
    assert mod.bot.register_next_step_handler.call_args.args[1] is mod.admin_bc_pick_product
    assert not telegram


@pytest.mark.parametrize('bad', ['-100777', '12345', 'https://t.me/news_store', '@ab', '@bad name', '@x_y_!'])
def test_channel_accepts_username_only_and_never_touches_forced_channels(mod, telegram, bad):
    mod.db.required_channels.insert_one({'channel_id': '@existing_channel'})
    d = draft(mod)
    mod._chbc_receive(message(mod, bad), d['_id'], 'channel')
    current = mod.db.channel_broadcasts.find_one({'_id': d['_id']})
    assert current['stage'] == 'channel' and 'channel' not in current
    assert mod.db.required_channels.count_documents({}) == 1
    assert not telegram


@pytest.mark.parametrize('name', ['@news_store', 'news_store'])
def test_channel_verification_works_without_forced_subscription(mod, telegram, name):
    d = draft(mod)
    mod._chbc_receive(message(mod, name), d['_id'], 'channel')
    current = mod.db.channel_broadcasts.find_one({'_id': d['_id']})
    assert current['channel']['id'] == -100777 and current['stage'] == 'message'
    assert mod.db.required_channels.count_documents({}) == 0 and not telegram


@pytest.mark.parametrize('who,status,can_post', [
    (123456789, 'member', True), (123456789, 'administrator', False),
    (9001, 'member', True), (9001, 'administrator', False)])
def test_bot_and_requesting_admin_must_both_have_channel_publishing_rights(mod, telegram, monkeypatch, who, status, can_post):
    monkeypatch.setattr(mod.bot, 'get_chat_member', lambda cid, uid: types.SimpleNamespace(
        status=status if uid == who else 'creator', can_post_messages=can_post if uid == who else True))
    d = draft(mod); mod._chbc_receive(message(mod, '@news_store'), d['_id'], 'channel')
    assert mod.db.channel_broadcasts.find_one({'_id': d['_id']})['stage'] == 'channel'
    assert not telegram


def test_groups_are_rejected_even_if_bot_is_admin(mod, telegram, monkeypatch):
    monkeypatch.setattr(mod.bot, 'get_chat', lambda name: types.SimpleNamespace(id=-100777, type='supergroup'))
    with pytest.raises(ValueError, match='قناة'):
        mod._chbc_verify_channel('@news_store', 9001)


@pytest.mark.parametrize('content', [None,
    {'photo': [{'file_id': 'FAKE-PHOTO', 'file_unique_id': 'FAKE', 'width': 10, 'height': 10}],
     'caption': '🛍 Rich caption', 'caption_entities': [{'type': 'bold', 'offset': 3, 'length': 4}]},
    {'document': {'file_id': 'FAKE-DOC', 'file_unique_id': 'FAKE'}, 'caption': 'Styled document'}])
def test_preview_copies_native_content_and_entities_without_reparsing(mod, telegram, content):
    product(mod); d = draft(mod); token = d['_id']
    mod._chbc_receive(message(mod, '@news_store'), token, 'channel')
    m = message(mod, '✨ Bold and Premium', mid=113, content=content)
    if content is None:
        m.entities = [mod.types.MessageEntity('bold', 3, 4),
                      mod.types.MessageEntity('custom_emoji', 0, 1, custom_emoji_id='5271619747891388291')]
    mod._chbc_receive(m, token, 'message')
    mod._chbc_receive(message(mod, 'https://t.me/lara_local_test_bot?start=p_101'), token, 'link')
    assert len(telegram) == 1 and telegram[0][0:3] == (9001, 9001, 113)
    assert set(telegram[0][3]) == {'reply_markup'}
    doc = mod.db.channel_broadcasts.find_one({'_id': token})
    assert doc['sources'] == [20001] and doc['status'] == 'preview'
    mk = json.loads(telegram[0][3]['reply_markup'])['inline_keyboard']
    assert len(mk) == 2 and all(b['style'] == 'success' for row in mk for b in row)
    assert mk[0][0]['url'].endswith('?startapp=p_101')
    assert mk[1][0]['url'].endswith('?start=p_101')
    assert all('web_app' not in b and 'callback_data' not in b for row in mk for b in row)
    assert not public(telegram)


@pytest.mark.parametrize('uid,site,bot_label', [
    (1001, 'Open website', 'Buy via bot'), (1002, 'فتح عبر الموقع', 'شراء عبر البوت')])
def test_private_notification_has_two_localized_destinations(mod, telegram, uid, site, bot_label):
    product(mod)
    mk = mod._notice_pair(uid, '101').to_dict()['inline_keyboard']
    assert site in mk[0][0]['text'] and bot_label in mk[1][0]['text']
    assert mk[0][0]['web_app']['url'].endswith('/app/?p=101')
    assert mk[1][0]['callback_data'] == 'vi_p_101'
    assert all(b['style'] == 'success' for row in mk for b in row)


def test_cms_custom_text_premium_and_color_apply_to_channel_and_private(mod, telegram):
    mod.db.custom_buttons.insert_one({'lang': 'en', 'key': 'btn_notice_site',
        'text': 'Visit shop', 'style': 'primary', 'emoji_id': '5271619747891388291'})
    mod._WA_BTN_CACHE.update(rows=None, exp=0)
    for is_channel in (False, True):
        button = mod._notice_pair(1001, channel=is_channel, lang='en').to_dict()['inline_keyboard'][0][0]
        assert button['text'] == 'Visit shop' and button['style'] == 'primary'
        assert button['icon_custom_emoji_id'] == '5271619747891388291'
    assert not mod._wa_bot_only('ad_cms_notice')
    assert not mod._wa_bot_only('edit_btn_btn_notice_site')
    assert mod._wa_bot_only('chbc_new')


def test_confirm_sends_exact_reviewed_copy_and_markup_only_once(mod, telegram):
    d = reviewed(mod)
    cb = f'chbc_send_{d["_id"]}_{d["revision"]}'
    dispatch(mod, cb, 9001); dispatch(mod, cb, 9001)
    sent = public(telegram)
    assert len(sent) == 1 and sent[0][0:3] == (-100777, 9001, d['sources'][0])
    assert json.loads(sent[0][3]['reply_markup']) == d['markup']
    assert mod.db.channel_broadcasts.find_one({'_id': d['_id']})['status'] == 'sent'


def test_simultaneous_confirmations_publish_once(mod, telegram):
    d = reviewed(mod)
    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(lambda _: mod._chbc_publish(d['_id'], 9001, d['revision']), range(5)))
    assert len(public(telegram)) == 1


def test_changed_language_invalidates_old_approval_and_keeps_frozen_content(mod, telegram):
    d = reviewed(mod)
    dispatch(mod, 'chbc_en_' + d['_id'], 9001)
    updated = mod.db.channel_broadcasts.find_one({'_id': d['_id']})
    assert updated['revision'] > d['revision'] and updated['lang'] == 'en'
    assert 'Open website' in updated['markup']['inline_keyboard'][0][0]['text']
    dispatch(mod, f'chbc_send_{d["_id"]}_{d["revision"]}', 9001)
    assert not public(telegram)
    assert telegram[-1][2] == d['sources'][0]  # re-preview the bot's immutable copy


@pytest.mark.parametrize('action', ['cancel', 'message', 'link', 'channel'])
def test_editing_or_cancelling_invalidates_confirmation(mod, telegram, action):
    d = reviewed(mod)
    dispatch(mod, f'chbc_{action}_' + d['_id'], 9001)
    dispatch(mod, f'chbc_send_{d["_id"]}_{d["revision"]}', 9001)
    assert not public(telegram)


def test_permissions_are_checked_again_before_any_public_send(mod, telegram, monkeypatch):
    d = reviewed(mod)
    monkeypatch.setattr(mod.bot, 'get_chat_member', lambda *a: types.SimpleNamespace(status='member'))
    dispatch(mod, f'chbc_send_{d["_id"]}_{d["revision"]}', 9001)
    assert not public(telegram)
    assert mod.db.channel_broadcasts.find_one({'_id': d['_id']})['status'] == 'preview'


def test_reassigned_username_cannot_publish_to_a_different_channel(mod, telegram, monkeypatch):
    d = reviewed(mod)
    monkeypatch.setattr(mod.bot, 'get_chat', lambda *a: types.SimpleNamespace(type='channel', id=-100888))
    dispatch(mod, f'chbc_send_{d["_id"]}_{d["revision"]}', 9001)
    assert not public(telegram)


def test_unknown_delivery_never_retries_or_resets_the_claim(mod, telegram, monkeypatch):
    d = reviewed(mod); original = mod.bot.copy_message
    def lost(target, *args, **kwargs):
        original(target, *args, **kwargs)
        raise TimeoutError('Lost Telegram reply after accepting the message')
    monkeypatch.setattr(mod.bot, 'copy_message', lost)
    cb = f'chbc_send_{d["_id"]}_{d["revision"]}'
    dispatch(mod, cb, 9001); dispatch(mod, cb, 9001)
    assert len(public(telegram)) == 1
    assert mod.db.channel_broadcasts.find_one({'_id': d['_id']})['status'] == 'needs_review'


@pytest.mark.parametrize('uid', [1001, 9002])
def test_non_admin_and_other_admin_cannot_use_another_owners_draft(mod, telegram, uid):
    d = reviewed(mod)
    if uid == 9002:
        mod.db.users.insert_one({'user_id': uid, 'is_admin': 1})
    dispatch(mod, f'chbc_send_{d["_id"]}_{d["revision"]}', uid)
    assert not public(telegram)


def test_expired_draft_and_cancel_input_cannot_publish(mod, telegram):
    d = reviewed(mod)
    mod.db.channel_broadcasts.update_one({'_id': d['_id']}, {'$set': {'session_until': time.time() - 1}})
    dispatch(mod, f'chbc_send_{d["_id"]}_{d["revision"]}', 9001)
    d = draft(mod); mod._chbc_receive(message(mod, '/cancel'), d['_id'], 'channel')
    assert mod.db.channel_broadcasts.find_one({'_id': d['_id']})['status'] == 'cancelled'
    assert not public(telegram)


@pytest.mark.parametrize('raw', ['p_101', '101', 'https://t.me/lara_local_test_bot?start=p_101',
    'https://t.me/lara_local_test_bot?startapp=p_101', 'https://local.example/app/?p=101'])
def test_product_short_codes_and_both_link_types_resolve_to_same_product(mod, telegram, raw):
    product(mod)
    assert mod._chbc_parse_link(raw) == '101'


@pytest.mark.parametrize('raw', ['https://t.me/another_store_bot?start=p_101',
    'https://outside.example/app/?p=101', 'p_nonexistent', 'https://t.me/lara_local_test_bot?start=p_101&start=p_102'])
def test_foreign_unknown_or_ambiguous_product_links_are_rejected(mod, telegram, raw):
    product(mod)
    with pytest.raises(ValueError): mod._chbc_parse_link(raw)


def test_table_is_english_and_product_names_link_to_mini_app_including_external_and_chatgpt(mod, telegram, monkeypatch):
    product(mod, discount_tiers=[{'min_qty': 3, 'price': 4}])
    ext = mod.db.ext_products.insert_one({'name': 'Remote', 'stock': 3, 'sell_price': 6}).inserted_id
    cg = mod.db.cgpt_products.insert_one({'name': 'ChatGPT', 'name_en': 'ChatGPT Business',
        'durations': [{'dur_id': '7', 'label': 'Week', 'price': 9, 'minutes': 10080}]}).inserted_id
    monkeypatch.setattr(mod, '_send_rich_raw', Mock(side_effect=ValueError('Use HTML fallback')))
    dispatch(mod, 'ad_plist', 9001)
    d = mod.db.channel_broadcasts.find_one({'kind': 'table'})
    assert d and len(d['sources']) >= 1 and d['lang'] == 'en'
    texts = '\n'.join(t for uid, t, kw in MESSAGES if kw.get('parse_mode') == 'HTML')
    assert 'Available products' in texts and '$5.00' in texts and '$4.00' in texts
    assert '?startapp=p_101' in texts and f'?startapp=p_ext_{ext}' in texts and f'?startapp=p_cgpt_{cg}' in texts
    assert '?start=p_' not in texts and '?start=vext_' not in texts
    assert not public(telegram)
    # Price edits after the snapshot do not silently change the approved report.
    mod.db.products.update_one({'id': '101'}, {'$set': {'price': 500}})
    dispatch(mod, 'chbc_table_' + d['_id'], 9001)
    mod._chbc_receive(message(mod, '@news_store'), d['_id'], 'channel')
    preview = mod.db.channel_broadcasts.find_one({'_id': d['_id']})
    dispatch(mod, f'chbc_send_{d["_id"]}_{preview["revision"]}', 9001)
    assert len(public(telegram)) == len(d['sources'])


def test_table_can_be_replaced_by_an_edited_native_message(mod, telegram, monkeypatch):
    product(mod); monkeypatch.setattr(mod, '_send_rich_raw', Mock(side_effect=ValueError('Use text')))
    token, _, _ = mod._chbc_table_new(9001)
    dispatch(mod, 'chbc_table_' + token, 9001)
    mod._chbc_receive(message(mod, '@news_store'), token, 'channel')
    dispatch(mod, 'chbc_message_' + token, 9001)
    mod._chbc_receive(message(mod, 'My edited table', mid=778), token, 'message')
    mod._chbc_receive(message(mod, 'https://t.me/lara_local_test_bot'), token, 'link')
    d = mod.db.channel_broadcasts.find_one({'_id': token})
    assert d['kind'] == 'custom' and d['status'] == 'preview'
    assert telegram[-1][2] == 778 and not public(telegram)


def test_main_miniapp_must_be_enabled_before_channel_confirmation(mod, telegram, monkeypatch):
    d = reviewed(mod)
    monkeypatch.setattr(mod.bot, 'get_me', lambda: types.SimpleNamespace(id=123456789, has_main_web_app=False))
    dispatch(mod, f'chbc_send_{d["_id"]}_{d["revision"]}', 9001)
    assert not public(telegram) and any('BotFather' in t for _, t, _ in MESSAGES)


def test_incomplete_miniapp_setup_can_be_fixed_and_preview_resumed(mod, telegram, monkeypatch):
    enabled = [False]
    monkeypatch.setattr(mod.bot, 'get_me', lambda: types.SimpleNamespace(id=123456789,
        username='lara_local_test_bot', has_main_web_app=enabled[0]))
    d = reviewed(mod)
    assert d['status'] == 'draft' and not public(telegram)
    assert any(b.get('callback_data') == 'chbc_preview_' + d['_id'] for row in buttons(MESSAGES[-1]) for b in row)
    enabled[0] = True
    dispatch(mod, 'chbc_preview_' + d['_id'], 9001)
    fresh = mod.db.channel_broadcasts.find_one({'_id': d['_id']})
    assert fresh['status'] == 'preview' and not public(telegram)
    dispatch(mod, f'chbc_send_{fresh["_id"]}_{fresh["revision"]}', 9001)
    assert len(public(telegram)) == 1


def test_regular_and_remote_product_edit_pages_show_two_distinct_links(mod, telegram):
    product(mod)
    dispatch(mod, 'edit_p_101', 9001)
    text = next(t for _, t, _ in reversed(MESSAGES) if 'Options for product' in t)
    assert '?start=p_101' in text and '?startapp=p_101' in text
    ext = mod.db.ext_products.insert_one({'name': 'Remote', 'stock': 1, 'sell_price': 5, 'base_price': 3}).inserted_id
    dispatch(mod, 'ext_p_' + str(ext), 9001)
    text = '\n'.join(t for _, t, _ in MESSAGES)
    assert f'?start=vext_{ext}' in text and f'?startapp=p_ext_{ext}' in text


@pytest.mark.parametrize('kind', ['new', 'stock', 'price'])
def test_supplier_broadcasts_keep_approval_worker_and_send_both_buttons(mod, telegram, monkeypatch, kind):
    ext = mod.db.ext_products.insert_one({'name': 'Remote', 'stock': 3, 'sell_price': 6}).inserted_id
    ep = mod.db.ext_products.find_one({'_id': ext})
    monkeypatch.setattr(mod.time, 'sleep', lambda _: None)
    if kind == 'new': mod._ext_broadcast_new_product(ep, _approved=True)
    elif kind == 'stock': mod._ext_broadcast_stock(ep, _approved=True)
    else: mod._ext_broadcast_price_drop(ep, 8, 6, _approved=True)
    messages = [r for r in MESSAGES if r[0] in (1001, 1002) and buttons(r)]
    assert len(messages) == 2
    for r in messages:
        mk = buttons(r)
        assert len(mk) == 2 and mk[0][0]['web_app']['url'].endswith('?p=ext_' + str(ext))
        assert mk[1][0]['callback_data'] == 'vext_' + str(ext)


def test_original_manual_member_broadcast_still_copies_original_with_pair(mod, telegram, monkeypatch):
    product(mod)
    monkeypatch.setattr(mod, '_bc_users', lambda: [{'user_id': 1001}, {'user_id': 1002}])
    monkeypatch.setattr(mod.time, 'sleep', lambda _: None)
    sent, failed = mod._run_manual_broadcast_job({'source_message': 113, 'source_chat': 9001, 'product_id': '101'})
    assert (sent, failed) == (2, 0)
    assert all(c[1:3] == (9001, 113) for c in telegram)
    assert all(len(json.loads(c[3]['reply_markup'])['inline_keyboard']) == 2 if isinstance(c[3]['reply_markup'], str)
               else len(c[3]['reply_markup'].keyboard) == 2 for c in telegram)


@pytest.mark.parametrize('kind', ['price', 'stock', 'cgpt'])
def test_normal_price_stock_and_chatgpt_announcements_have_two_buttons(mod, telegram, monkeypatch, kind):
    product(mod)
    allowed = {'broadcast_price_drop', 'broadcast_new_stock', '_do_broadcast'}
    def thread(target=None, args=(), kwargs=None, **extra):
        return types.SimpleNamespace(start=lambda: target(*args, **(kwargs or {})) if target.__name__ in allowed else None)
    monkeypatch.setattr(mod.threading, 'Thread', thread)
    monkeypatch.setattr(mod.time, 'sleep', lambda _: None)
    if kind == 'price':
        mod.admin_save_edit(message(mod, '4'), 'price', '101')
    elif kind == 'stock':
        mod.admin_stock_save(message(mod, 'LOCAL-NEW-STOCK-CODE'), '101')
    else:
        cg = mod.db.cgpt_products.insert_one({'name': 'ChatGPT Business', 'name_en': 'ChatGPT Business'}).inserted_id
        monkeypatch.setattr(mod, '_cgpt_all_accounts', lambda: [])
        dispatch(mod, 'cgpt_bsend_' + str(cg), 9001)
    recipients = [r for r in MESSAGES if r[0] in (1001, 1002) and buttons(r)]
    assert len(recipients) == 2
    for r in recipients:
        mk = buttons(r)
        assert len(mk) == 2 and 'web_app' in mk[0][0] and 'callback_data' in mk[1][0]


def test_cms_emoji_only_keeps_names_and_unavailable_translation_keeps_english(mod, telegram, monkeypatch):
    monkeypatch.setattr(mod, 'safe_translate_for_cms', lambda text, lang: text)
    m = message(mod, '✨')
    m.entities = [mod.types.MessageEntity('custom_emoji', 0, 1, custom_emoji_id='5271619747891388291')]
    mod.ad_save_custom_btn(m, 'btn_notice_site')
    ar = mod.db.custom_buttons.find_one({'lang': 'ar', 'key': 'btn_notice_site'})
    en = mod.db.custom_buttons.find_one({'lang': 'en', 'key': 'btn_notice_site'})
    assert 'فتح عبر الموقع' in ar['text'] and 'Open website' in en['text']
    assert ar['emoji_id'] == en['emoji_id'] == '5271619747891388291'


def test_cms_can_select_a_neutral_color_after_green_default(mod, telegram):
    dispatch(mod, 'btnstyle_btn_notice_site_default', 9001)
    assert 'style' not in mod._notice_pair(1001, channel=True).to_dict()['inline_keyboard'][0][0]


def test_partial_multi_message_channel_report_stops_and_never_replays(mod, telegram, monkeypatch):
    d = reviewed(mod)
    mod.db.channel_broadcasts.update_one({'_id': d['_id']}, {'$set': {'sources': [20001, 20002, 20003]}})
    original = mod.bot.copy_message
    def copy(target, src, mid, **kw):
        if target < 0 and mid == 20002: raise TimeoutError('Unknown second message outcome')
        return original(target, src, mid, **kw)
    monkeypatch.setattr(mod.bot, 'copy_message', copy)
    dispatch(mod, f'chbc_send_{d["_id"]}_{d["revision"]}', 9001)
    assert len(public(telegram)) == 1
    saved = mod.db.channel_broadcasts.find_one({'_id': d['_id']})
    assert saved['status'] == 'needs_review' and len(saved['sent_messages']) == 1
    dispatch(mod, f'chbc_send_{d["_id"]}_{d["revision"]}', 9001)
    assert len(public(telegram)) == 1


@pytest.mark.parametrize('persistent', [False, True])
def test_database_failure_after_publication_does_not_allow_a_second_send(mod, telegram, monkeypatch, persistent):
    d = reviewed(mod)
    update = mod.db.channel_broadcasts.update_one
    failed = []
    def write(query, changes, *args, **kwargs):
        if changes.get('$set', {}).get('sent_messages') and (persistent or not failed):
            failed.append(True)
            raise OSError('Simulated receipt write failure')
        return update(query, changes, *args, **kwargs)
    monkeypatch.setattr(mod.db.channel_broadcasts, 'update_one', write)
    cb = f'chbc_send_{d["_id"]}_{d["revision"]}'
    dispatch(mod, cb, 9001); dispatch(mod, cb, 9001)
    assert len(public(telegram)) == 1
    assert mod.db.channel_broadcasts.find_one({'_id': d['_id']})['status'] in ('sending', 'needs_review')
