"""Real SDK serialization and isolated multi-product publishing. No live Telegram."""
import copy
import json
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from harness import MESSAGES
from test_integration import mod, isolate, product
from test_products_menu import dispatch, buttons
from test_channel_broadcast import telegram, draft, message, public, reviewed

EMOJI = '5271619747891388291'


def nonempty_entities(values):
    # Optional Bot API fields may be omitted or serialized as null by the SDK.
    return [{key: value for key, value in entity.items() if value is not None} for entity in values]


@pytest.fixture(autouse=True)
def composer_setup(mod, monkeypatch):
    monkeypatch.setattr(mod._channel_composer, 'catalog', None)
    monkeypatch.setattr(mod._channel_composer, 'catalog_until', 0)
    monkeypatch.setattr(mod.time, 'sleep', lambda _: None)


def state(mod, token):
    return mod.db.channel_broadcasts.find_one({'_id': token})


def start_content(mod, text='A post', *, content=None):
    d = draft(mod)
    mod._chbc_receive(message(mod, '@news_store'), d['_id'], 'channel')
    m = message(mod, text, mid=555, content=content)
    mod._chbc_receive(m, d['_id'], 'message')
    return d['_id'], m


def approve(mod, doc):
    dispatch(mod, f'chbc_send_{doc["_id"]}_{doc["revision"]}', 9001)


def native_transport(mod, monkeypatch, *, strip_premium=False):
    """Exercise pyTelegramBotAPI's actual message/entity serialization."""
    calls = []
    def request(_token, endpoint, params=None, **kwargs):
        params = copy.deepcopy(params or {})
        calls.append((endpoint, params))
        uid = int(params['chat_id'])
        result = {'message_id': 50000 + len(calls), 'date': 1,
                  'chat': {'id': uid, 'type': 'channel' if uid < 0 else 'private'}}
        if endpoint == 'sendMessage':
            result['text'] = params['text']
            result['entities'] = json.loads(params.get('entities', '[]'))
            if strip_premium:
                result['entities'] = [e for e in result['entities'] if e['type'] != 'custom_emoji']
        elif endpoint == 'sendPhoto':
            result.update(photo=[{'file_id': 'PHOTO', 'file_unique_id': 'PHOTO', 'width': 1, 'height': 1}],
                          caption=params.get('caption'), caption_entities=json.loads(params.get('caption_entities', '[]')))
        return result
    monkeypatch.setattr(mod.telebot.apihelper, '_make_request', request)
    monkeypatch.setitem(mod._VC_ORIG, 'send_message',
        mod.telebot.TeleBot.send_message.__get__(mod.bot, type(mod.bot)))
    return calls


@pytest.mark.parametrize('separator', ['\n', ' ', ', ', '، '])
def test_multiple_product_links_are_saved_and_published_together(mod, telegram, separator):
    for pid in ['101', '102', '103']:
        product(mod, pid, name_en='Product ' + pid)
    token, _ = start_content(mod)
    mod._chbc_receive(message(mod, separator.join(['p_101', 'https://t.me/lara_local_test_bot?start=p_102',
        'https://local.example/app/?p=103'])), token, 'link')
    doc = state(mod, token)
    assert doc['status'] == 'preview' and doc['product_ids'] == ['101', '102', '103']
    assert len(doc['markup']['inline_keyboard']) == 3
    for pid, row in zip(doc['product_ids'], doc['markup']['inline_keyboard']):
        assert len(row) == 2 and 'Product ' + pid in row[0]['text']
        assert row[0]['url'].endswith('?startapp=p_' + pid)
        assert row[1]['url'].endswith('?start=p_' + pid)
    approve(mod, doc)
    assert len(public(telegram)) == 1
    assert json.loads(public(telegram)[0][3]['reply_markup']) == doc['markup']


def test_duplicate_links_collapse_and_bad_or_excess_products_leave_existing_draft_intact(mod, telegram):
    for i in range(101, 122):
        product(mod, str(i))
    c = mod._channel_composer
    assert c.parse_links('p_101\n101\nhttps://t.me/lara_local_test_bot?start=p_101') == ['101']
    with pytest.raises(ValueError):
        c.parse_links('p_101\np_missing')
    with pytest.raises(ValueError):
        c.parse_links('\n'.join('p_' + str(i) for i in range(101, 122)))
    with pytest.raises(ValueError):
        c.parse_links('p_101\nhttps://t.me/lara_local_test_bot')
    token, _ = start_content(mod)
    mod._chbc_receive(message(mod, 'p_101\np_missing'), token, 'link')
    assert state(mod, token)['stage'] == 'link' and not public(telegram)


def test_twenty_products_use_forty_green_link_buttons_with_localized_cms_labels(mod, telegram):
    for i in range(101, 121):
        product(mod, str(i), name_en='Long product name ' + ('😀' * 40))
    mod.db.custom_buttons.insert_one({'lang': 'en', 'key': 'btn_notice_site',
        'text': 'Visit shop', 'emoji_id': EMOJI, 'style': 'primary'})
    token, _ = start_content(mod)
    mod._chbc_receive(message(mod, '\n'.join('p_' + str(i) for i in range(101, 121))), token, 'link')
    dispatch(mod, 'chbc_en_' + token, 9001)
    doc = state(mod, token)
    rows = doc['markup']['inline_keyboard']
    assert len(rows) == 20
    for row in rows:
        assert row[0]['text'].startswith('Visit shop') and row[0]['style'] == 'primary'
        assert row[0]['icon_custom_emoji_id'] == EMOJI and row[1]['style'] == 'success'
        assert all(len(b['text'].encode('utf-16-le')) // 2 <= 64 for b in row)


def test_regular_external_and_chatgpt_products_can_share_one_post(mod, telegram):
    product(mod)
    ext = mod.db.ext_products.insert_one({'name': 'Remote', 'stock': 3, 'sell_price': 6}).inserted_id
    cg = mod.db.cgpt_products.insert_one({'name_en': 'ChatGPT',
        'durations': [{'dur_id': '7', 'label': 'Week', 'price': 9, 'minutes': 10080}]}).inserted_id
    token, _ = start_content(mod)
    mod._chbc_receive(message(mod, f'p_101\next_{ext}\ncgpt_main_{cg}'), token, 'link')
    doc = state(mod, token)
    assert doc['status'] == 'preview' and len(doc['markup']['inline_keyboard']) == 3
    assert f'?start=vext_{ext}' in doc['markup']['inline_keyboard'][1][1]['url']


def test_picker_selects_more_than_two_products_and_old_buttons_cannot_change_selection(mod, telegram):
    for i in range(101, 112):
        product(mod, str(i), name_en='Product ' + str(i))
    token, _ = start_content(mod)
    doc = state(mod, token)
    dispatch(mod, f'chbcx_pick_{token}_{doc["revision"]}_0', 9001)
    for index in range(3):
        doc = state(mod, token)
        data = f'chbcx_toggle_{token}_{doc["revision"]}_{index}'
        dispatch(mod, data, 9001)
        dispatch(mod, data, 9001)  # Stale callback must not undo the new choice.
    doc = state(mod, token)
    assert len(doc['product_ids']) == 3 and doc['stage'] == 'link'
    dispatch(mod, f'chbcx_pick_{token}_{doc["revision"]}_1', 9001)
    doc = state(mod, token)
    assert doc['picker_page'] == 1
    dispatch(mod, f'chbcx_toggle_{token}_{doc["revision"]}_0', 9001)
    doc = state(mod, token)
    assert len(doc['product_ids']) == 4
    assert all(len(b['callback_data'].encode()) <= 64 for row in buttons(MESSAGES[-1]) for b in row)
    dispatch(mod, f'chbcx_done_{token}_{doc["revision"]}', 9001)
    doc = state(mod, token)
    assert doc['status'] == 'preview' and len(doc['markup']['inline_keyboard']) == 4
    assert not public(telegram)


def test_picker_caches_catalog_instead_of_refetching_on_each_selection(mod, telegram, monkeypatch):
    product(mod)
    rows = Mock(wraps=mod._plist_rows)
    monkeypatch.setattr(mod, '_plist_rows', rows)
    token, _ = start_content(mod)
    doc = state(mod, token)
    dispatch(mod, f'chbcx_pick_{token}_{doc["revision"]}_0', 9001)
    for _ in range(3):
        doc = state(mod, token)
        dispatch(mod, f'chbcx_toggle_{token}_{doc["revision"]}_0', 9001)
    assert rows.call_count == 1


@pytest.mark.parametrize('uid', [1001, 9002])
def test_foreign_user_or_admin_cannot_edit_the_product_picker(mod, telegram, uid):
    product(mod)
    token, _ = start_content(mod)
    if uid == 9002:
        mod.db.users.insert_one({'user_id': uid, 'is_admin': 1})
    before = state(mod, token)
    dispatch(mod, f'chbcx_pick_{token}_{before["revision"]}_0', uid)
    assert state(mod, token) == before


def test_editing_product_selection_invalidates_approved_snapshot(mod, telegram):
    doc = reviewed(mod)
    dispatch(mod, f'chbcx_pick_{doc["_id"]}_{doc["revision"]}_0', 9001)
    approve(mod, doc)
    assert not public(telegram)
    assert state(mod, doc['_id'])['status'] == 'draft'


def test_new_channel_actions_remain_bot_only(mod):
    assert mod._wa_bot_only('chbcx_pick_' + 'a' * 24 + '_1_0')
    assert not mod._wa_bot_only('ad_cms_notice')


def test_native_entities_survive_refresh_and_sdk_publication_with_utf16_offsets(mod, telegram, monkeypatch):
    product(mod)
    token, _ = start_content(mod)
    # Replace the content using the existing Edit message button.
    dispatch(mod, 'chbc_message_' + token, 9001)
    text = '😀 ✨ Bold hidden'
    entities = [{'type': 'custom_emoji', 'offset': 3, 'length': 1, 'custom_emoji_id': EMOJI},
                {'type': 'bold', 'offset': 5, 'length': 4},
                {'type': 'spoiler', 'offset': 10, 'length': 6}]
    incoming = message(mod, text, mid=556)
    incoming.entities = [mod.types.MessageEntity.de_json(e) for e in entities]
    mod._chbc_receive(incoming, token, 'message')
    mod._chbc_receive(message(mod, 'p_101'), token, 'link')
    incoming.text = 'Later edit must not change approved text'
    incoming.entities.clear()
    dispatch(mod, 'chbc_en_' + token, 9001)
    doc = state(mod, token)
    payload = doc['payloads'][str(doc['sources'][0])]
    assert payload['text'] == text and nonempty_entities(payload['entities']) == entities
    transport = native_transport(mod, monkeypatch)
    approve(mod, doc)
    sent = [(m, p) for m, p in transport if int(p['chat_id']) < 0]
    assert len(sent) == 1 and sent[0][0] == 'sendMessage'
    params = sent[0][1]
    assert params['text'] == text and nonempty_entities(json.loads(params['entities'])) == entities
    assert 'parse_mode' not in params
    assert state(mod, token)['format_warnings'] == []


def test_html_is_rendered_for_preview_and_channel_instead_of_copied_as_literal_tags(mod, telegram, monkeypatch):
    product(mod)
    token, _ = start_content(mod, '<b>Sale</b> <i>today</i> <tg-spoiler>code</tg-spoiler>')
    transport = native_transport(mod, monkeypatch)
    mod._chbc_receive(message(mod, 'p_101'), token, 'link')
    doc = state(mod, token)
    assert doc['status'] == 'preview'
    assert any(p.get('parse_mode') == 'HTML' and '<b>Sale</b>' in p.get('text', '') for _, p in transport)
    approve(mod, doc)
    sent = [p for m, p in transport if m == 'sendMessage' and int(p['chat_id']) < 0]
    assert len(sent) == 1 and sent[0]['parse_mode'] == 'HTML'
    assert '<tg-spoiler>' in sent[0]['text']


def test_a_native_code_entity_containing_html_is_not_reinterpreted(mod, telegram, monkeypatch):
    product(mod)
    token, _ = start_content(mod)
    dispatch(mod, 'chbc_message_' + token, 9001)
    m = message(mod, '<b>literal code</b>')
    m.entities = [mod.types.MessageEntity('code', 0, len(m.text))]
    mod._chbc_receive(m, token, 'message')
    mod._chbc_receive(message(mod, 'p_101'), token, 'link')
    transport = native_transport(mod, monkeypatch)
    approve(mod, state(mod, token))
    sent = next(p for method, p in transport if int(p['chat_id']) < 0)
    assert 'parse_mode' not in sent and json.loads(sent['entities'])[0]['type'] == 'code'


def test_photo_caption_entities_and_spoiler_are_preserved_by_sdk(mod, telegram, monkeypatch):
    product(mod)
    content = {'photo': [{'file_id': 'PHOTO', 'file_unique_id': 'PHOTO', 'width': 10, 'height': 10}],
        'caption': '✨ Bold', 'caption_entities': [
            {'type': 'custom_emoji', 'offset': 0, 'length': 1, 'custom_emoji_id': EMOJI},
            {'type': 'bold', 'offset': 2, 'length': 4}], 'has_media_spoiler': True, 'show_caption_above_media': True}
    token, _ = start_content(mod, content=content)
    mod._chbc_receive(message(mod, 'p_101'), token, 'link')
    transport = native_transport(mod, monkeypatch)
    approve(mod, state(mod, token))
    sent = [(method, p) for method, p in transport if int(p['chat_id']) < 0]
    assert len(sent) == 1 and sent[0][0] == 'sendPhoto'
    params = sent[0][1]
    assert nonempty_entities(json.loads(params['caption_entities'])) == content['caption_entities']
    assert params['caption'] == '✨ Bold' and params['has_spoiler'] is True
    assert params['show_caption_above_media'] is True and 'parse_mode' not in params


def test_rich_message_with_table_and_premium_is_accepted_and_copied_without_flattening(mod, telegram):
    product(mod)
    rich = {'blocks': [{'type': 'paragraph', 'text': ['Sale ',
        {'type': 'custom_emoji', 'custom_emoji_id': EMOJI, 'alternative_text': '✨'}]},
        {'type': 'table', 'cells': [[{'text': 'Product', 'align': 'left', 'valign': 'top'},
            {'text': {'type': 'bold', 'text': '$5'}, 'align': 'center', 'valign': 'top'}]]}]}
    token, incoming = start_content(mod, content={'rich_message': rich})
    assert incoming.content_type == 'rich_message'
    assert state(mod, token)['stage'] == 'link'
    mod._chbc_receive(message(mod, 'p_101'), token, 'link')
    doc = state(mod, token)
    assert doc['payloads'][str(doc['sources'][0])]['rich_message'] == rich
    approve(mod, doc)
    assert len(public(telegram)) == 1
    assert set(public(telegram)[0][3]) == {'reply_markup'}


def test_missing_premium_in_telegram_reply_is_reported_without_duplicate_publication(mod, telegram, monkeypatch):
    product(mod)
    token, _ = start_content(mod)
    dispatch(mod, 'chbc_message_' + token, 9001)
    m = message(mod, '✨ Sale')
    m.entities = [mod.types.MessageEntity('custom_emoji', 0, 1, custom_emoji_id=EMOJI)]
    mod._chbc_receive(m, token, 'message')
    mod._chbc_receive(message(mod, 'p_101'), token, 'link')
    doc = state(mod, token)
    transport = native_transport(mod, monkeypatch, strip_premium=True)
    approve(mod, doc); approve(mod, doc)
    assert len([p for _, p in transport if int(p['chat_id']) < 0]) == 1
    saved = state(mod, token)
    assert saved['status'] == 'sent' and 'Telegram' in saved['format_warnings'][0]


def test_adding_more_than_two_messages_preserves_order_and_invalidates_old_approval(mod, telegram):
    product(mod)
    token, _ = start_content(mod)
    mod._chbc_receive(message(mod, 'p_101'), token, 'link')
    for i in range(2):
        old = state(mod, token)
        dispatch(mod, f'chbcx_append_{token}_{old["revision"]}', 9001)
        approve(mod, old)
        assert not public(telegram)
        mod._chbc_receive(message(mod, 'Extra message ' + str(i), mid=600+i), token, 'append')
    doc = state(mod, token)
    assert len(doc['sources']) == 3
    assert [doc['payloads'][str(mid)]['text'] for mid in doc['sources']] == ['A post', 'Extra message 0', 'Extra message 1']
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda _: mod._chbc_publish(token, 9001, doc['revision']), range(3)))
    assert len(public(telegram)) == 3 and state(mod, token)['status'] == 'sent'
    assert all(json.loads(row[3]['reply_markup']) == {'inline_keyboard': []} for row in public(telegram)[:-1])
    assert json.loads(public(telegram)[-1][3]['reply_markup']) == doc['markup']
