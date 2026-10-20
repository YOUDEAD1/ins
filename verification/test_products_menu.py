"""Customer availability and admin customization, with isolated Telegram/DB calls."""
import json
import time
from html.parser import HTMLParser
from unittest.mock import Mock

import pytest
from harness import MESSAGES, reply
from test_integration import mod, isolate, server, product, request


def callback(mod, data, uid=1001):
    return mod.types.CallbackQuery.de_json({
        'id': 'products-test', 'from': {'id': uid, 'is_bot': False, 'first_name': 'Test'},
        'chat_instance': 'test', 'data': data, 'message': {'message_id': 10, 'date': 1,
        'chat': {'id': uid, 'type': 'private'}, 'text': 'menu'}})


def dispatch(mod, data, uid=1001):
    mod.bot._run_middlewares_and_handler(callback(mod, data, uid),
        handlers=mod.bot.callback_query_handlers, middlewares=None, update_type='callback_query')


def buttons(record):
    mk = record[2].get('reply_markup')
    return mk.to_dict()['inline_keyboard'] if mk else []


@pytest.fixture
def rich_sent(mod, monkeypatch):
    sent = []
    monkeypatch.setattr(mod, '_send_rich_raw', lambda target, rich: sent.append((target, rich)))
    monkeypatch.setitem(mod._VC_ORIG, 'edit_message_text',
        lambda text, chat_id, message_id, **kw: reply(chat_id, text, **kw))
    return sent


@pytest.mark.parametrize('uid,search,available', [
    (1001, '🔎 Search products', 'Available'), (1002, '🔎 البحث عن منتج', 'المتوفر')])
@pytest.mark.parametrize('mode,catalogs', [('flat', False), ('folders', False), ('folders', True)])
def test_search_above_neutral_available_in_every_main_shop(mod, rich_sent, uid, search, available, mode, catalogs):
    product(mod)
    mod.db.users.update_one({'user_id': uid}, {'$set': {'shop_display_mode': mode}})
    if catalogs:
        mod.db.catalogs.insert_one({'name_ar': 'قسم', 'name_en': 'Category', 'product_ids': ['101']})
    dispatch(mod, 'open_shop', uid)
    rows = next(buttons(r) for r in reversed(MESSAGES) if buttons(r))
    assert rows[0] == [{'text': search, 'callback_data': 'shop_search'}]
    assert rows[1] == [{'text': available, 'callback_data': 'shop_available'}]
    assert any(b.get('callback_data', '').startswith(('vi_p_', 'cat_')) for row in rows[2:] for b in row)


@pytest.mark.parametrize('uid,lang,name', [(1001, 'en', 'Test Product'), (1002, 'ar', 'منتج')])
def test_customer_table_has_localized_prices_quantities_links_and_navigation(mod, rich_sent, uid, lang, name):
    product(mod, stock=3, discount_tiers=[{'min_qty': 5, 'price': 4.5}])
    product(mod, '102', stock=0)
    product(mod, '103', is_hidden=True)
    dispatch(mod, 'shop_available', uid)
    assert len(rich_sent) == 1 and rich_sent[0][0] == uid
    rich = rich_sent[0][1]
    table = next(b for b in rich['blocks'] if b['type'] == 'table')
    assert len(table['cells']) == 2
    linked = table['cells'][1][0]['text'][2]
    assert linked == {'type': 'url', 'text': name + ' (3)',
        'url': 'https://t.me/lara_local_test_bot?start=p_101'}
    price = json.dumps(table['cells'][1][1]['text'], ensure_ascii=False)
    assert '$5.00' in price and '$4.50' in price and ('قطعة' if lang == 'ar' else 'pcs') in price
    assert ('عدد المنتجات: 1' if lang == 'ar' else '1 products') in json.dumps(rich, ensure_ascii=False)
    nav = [b['callback_data'] for row in buttons(MESSAGES[-1]) for b in row]
    assert nav == ['shop_search', 'shop_available', 'shop_available_text', 'shop_avail_0', 'open_shop']
    assert not any(mod._is_admin_cb(cb) for cb in nav)
    assert mod.db.orders.count_documents({}) == 0


def test_refresh_tracks_stock_and_old_available_browser_still_works(mod, rich_sent):
    product(mod, stock=1)
    dispatch(mod, 'shop_avail_0')
    assert any(b.get('callback_data') == 'vi_p_101' for r in MESSAGES for row in buttons(r) for b in row)
    mod.db.product_stock.update_many({}, {'$set': {'is_sold': True}})
    dispatch(mod, 'shop_available')
    table = next(b for b in rich_sent[-1][1]['blocks'] if b['type'] == 'table')
    assert len(table['cells']) == 1


def test_search_button_registers_input_and_returns_matching_product(mod, rich_sent, monkeypatch):
    product(mod, name_en='Netflix Premium')
    step = Mock()
    monkeypatch.setattr(mod.bot, 'register_next_step_handler', step)
    dispatch(mod, 'shop_search')
    assert step.call_args.args[1] is mod.shop_search_msg
    msg = mod.types.Message.de_json({'message_id': 15, 'date': 1,
        'chat': {'id': 1001, 'type': 'private'},
        'from': {'id': 1001, 'is_bot': False, 'first_name': 'Test'}, 'text': 'netflix'})
    mod.shop_search_msg(msg)
    assert any(b.get('callback_data') == 'vi_p_101' for row in buttons(MESSAGES[-1]) for b in row)


@pytest.mark.parametrize('gate', ['banned', 'forced', 'payment'])
def test_public_table_respects_existing_access_gates(mod, rich_sent, monkeypatch, gate):
    product(mod)
    start = Mock()
    monkeypatch.setattr(mod, 'start_handler', start)
    if gate == 'banned':
        mod.db.users.update_one({'user_id': 1001}, {'$set': {'is_banned': 1}})
    elif gate == 'forced':
        monkeypatch.setattr(mod, 'check_forced_sub', lambda uid: False)
    else:
        mod.db.users.update_one({'user_id': 1001}, {'$set': {
            'deposit_locked': True, 'deposit_lock_expires': int(time.time()) + 600}})
    dispatch(mod, 'shop_available')
    assert not rich_sent
    if gate == 'forced':
        assert start.call_args.args[0].from_user.id == 1001
    if gate == 'payment':
        assert mod.is_deposit_locked(1001)


def test_chatgpt_hidden_parents_and_hidden_links_are_excluded_without_duplicates(mod, rich_sent):
    durations = [{'dur_id': 'month', 'label': 'Month', 'minutes': 43200, 'price': 8.5}]
    hidden = mod.db.cgpt_products.insert_one({'name': 'Hidden parent', 'is_hidden': True, 'durations': durations}).inserted_id
    linked = mod.db.cgpt_products.insert_one({'name': 'Hidden link', 'durations': durations}).inserted_id
    visible = mod.db.cgpt_products.insert_one({'name': 'عربي', 'name_en': 'Visible linked', 'durations': durations}).inserted_id
    standalone = mod.db.cgpt_products.insert_one({'name_en': 'Standalone', 'durations': durations}).inserted_id
    product(mod, '201', product_type='cgpt_main', cgpt_product_id=str(hidden), stock=0)
    product(mod, '202', product_type='cgpt_main', cgpt_product_id=str(linked), is_hidden=True, stock=0)
    product(mod, '203', product_type='cgpt_main', cgpt_product_id=str(visible), stock=0)
    rows = mod._plist_rows()
    assert {r['name'] for r in rows} == {'Visible linked', 'Standalone'}
    assert {r['url'].split('?start=')[1] for r in rows} == {f'p_cgpt_main_{visible}', f'p_cgpt_main_{standalone}'}
    assert all(r['stock'] == 8 and r['price'] == 8.5 and r['from_price'] for r in rows)


def test_manual_and_external_products_stay_available_but_hidden_external_stays_hidden(mod, rich_sent):
    product(mod, stock=0, manual=True)
    ext = mod.db.ext_products.insert_one({'name': 'Remote', 'stock': 2, 'sell_price': 6}).inserted_id
    mod.db.ext_products.insert_one({'name': 'Hidden remote', 'stock': 2, 'hidden': True})
    rows = mod._plist_rows()
    assert len(rows) == 2
    assert next(r for r in rows if r['name'] == 'Test Product')['stock'] is None
    assert next(r for r in rows if r['name'] == 'Remote')['url'].endswith(f'?start=vext_{ext}')


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls, self.depth = [], 0
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            assert self.depth == 0
            self.depth += 1
            self.urls.append(dict(attrs)['href'])
    def handle_endtag(self, tag):
        if tag == 'a':
            self.depth -= 1
            assert self.depth == 0


@pytest.mark.parametrize('as_text', [False, True])
def test_text_fallback_splits_between_complete_product_links(mod, monkeypatch, as_text):
    for i in range(64):
        product(mod, str(300 + i), stock=1, name_en=f'A & B <b>Product {i}</b> ' + 'Long name ' * 8)
    monkeypatch.setattr(mod, '_send_rich_raw', Mock(side_effect=RuntimeError('rich unavailable')))
    n, rich = mod._plist_send(1001, 1001, as_text=as_text)
    assert n == 64 and not rich and len(MESSAGES) > 1
    urls = []
    for _, text, opts in MESSAGES:
        assert len(text) <= 3900 and opts['parse_mode'] == 'HTML'
        parser = Links(); parser.feed(text); parser.close()
        assert parser.depth == 0
        urls += parser.urls
        assert '&amp;amp;' not in text
    assert len(urls) == len(set(urls)) == 64


@pytest.mark.parametrize('uid,lang', [(1001, 'en'), (1002, 'ar')])
def test_new_buttons_preserve_admin_labels_premium_and_color(mod, rich_sent, uid, lang):
    mod.db.custom_buttons.insert_many([
        {'lang': lang, 'key': 'btn_shop_search', 'text': 'Find', 'emoji_id': '5271619747891388291'},
        {'lang': lang, 'key': 'btn_shop_avail', 'text': 'Stock', 'emoji_id': '5368324170671202286', 'style': 'primary'}])
    mk = mod.InlineKeyboardMarkup()
    mod._shop_tools_row(mk, uid)
    rows = mk.to_dict()['inline_keyboard']
    assert rows[0][0]['text'] == 'Find' and rows[0][0]['icon_custom_emoji_id'] == '5271619747891388291'
    assert rows[1][0]['text'] == 'Stock' and rows[1][0]['style'] == 'primary'
    assert rows[1][0]['icon_custom_emoji_id'] == '5368324170671202286'
    dispatch(mod, 'shop_available', uid)
    assert rich_sent[-1][1]['blocks'][0]['text'][0]['custom_emoji_id'] == '5368324170671202286'


def test_cms_message_edits_keep_premium_in_table_search_and_footer(mod, rich_sent):
    emoji = '<tg-emoji emoji-id="5271619747891388291">⭐</tg-emoji>'
    for key, value in [('shop_available_title', emoji + ' My stock'), ('shop_available_count', 'Total: {0}'),
                       ('shop_search_prompt', emoji + ' Search here'), ('shop_available_hint', emoji + ' Choose below')]:
        mod.db.custom_texts.insert_one({'lang': 'en', 'key': key, 'value': value})
    product(mod)
    dispatch(mod, 'shop_available')
    assert rich_sent[-1][1]['blocks'][0]['text'][0]['custom_emoji_id'] == '5271619747891388291'
    assert 'Total: 1' in json.dumps(rich_sent[-1][1])
    assert MESSAGES[-1][1] == emoji + ' Choose below'
    dispatch(mod, 'shop_search')
    assert MESSAGES[-1][1] == emoji + ' Search here'
    assert emoji in mod._plist_html(mod._plist_rows())


@pytest.mark.parametrize('data', ['ad_cms_shop', 'edit_btn_btn_shop_avail', 'edit_txt_shop_available_hint',
                                  'btnstyle_btn_shop_avail_danger', 'ad_plist'])
def test_customer_cannot_change_cms_or_open_admin_table(mod, rich_sent, data):
    dispatch(mod, data)
    assert not rich_sent and mod.db.custom_buttons.count_documents({}) == 0
    assert mod.db.custom_texts.count_documents({}) == 0


def vc_request(mod, server, body):
    status, data, _ = request(server, '/webapp/admin/vc', body, uid=9001)
    assert status == 200, data
    assert not data.get('toasts'), data
    return data


def vc_press(mod, server, view, cb):
    msg = next(m for m in reversed(view['msgs']) if any(b.get('v') == cb for row in m.get('buttons', []) for b in row))
    return vc_request(mod, server, {'action': 'press', 'mid': msg['id'], 'data': cb})


def test_authenticated_console_edits_button_premium_color_and_message_end_to_end(mod, server, monkeypatch):
    monkeypatch.setattr(mod, '_vc_start', lambda session, fn, wait=None: mod._vc_run(session, fn))
    monkeypatch.setattr(mod, 'safe_translate_for_cms', lambda text, lang: 'Available now' if lang == 'en' else text)
    view = vc_request(mod, server, {'action': 'open', 'cb': 'ad_cms_shop'})
    view = vc_press(mod, server, view, 'edit_btn_btn_shop_avail')
    view = vc_request(mod, server, {'action': 'send', 'text': 'المتوفر الآن | 5271619747891388291'})
    view = vc_press(mod, server, view, 'btnstyle_btn_shop_avail_primary')
    button = mod.create_btn(1001, 'btn_shop_avail', callback_data='shop_available').to_dict()
    assert button['text'] == 'Available now' and button['style'] == 'primary'
    assert button['icon_custom_emoji_id'] == '5271619747891388291'
    view = vc_request(mod, server, {'action': 'open', 'cb': 'ad_cms_shop'})
    view = vc_press(mod, server, view, 'edit_txt_shop_available_hint')
    text = '<tg-emoji emoji-id="5271619747891388291">⭐</tg-emoji> اختر المنتج'
    monkeypatch.setattr(mod, 'safe_translate_for_cms', lambda text, lang: text.replace('اختر المنتج', 'Choose a product') if lang == 'en' else text)
    vc_request(mod, server, {'action': 'send', 'text': text})
    assert 'Choose a product' in mod.get_text(1001, 'shop_available_hint')
    assert '5271619747891388291' in mod.get_text(1002, 'shop_available_hint')
    view = vc_request(mod, server, {'action': 'open', 'cb': 'ad_cms_shop'})
    view = vc_press(mod, server, view, 'edit_txt_shop_available_hint')
    vc_press(mod, server, view, 'rst_txt_shop_available_hint')
    assert mod.get_text(1001, 'shop_available_hint') == mod.LANG['en']['shop_available_hint']


def test_console_exposes_only_requested_customization_and_keeps_other_cms_routing(mod, server):
    status, home, _ = request(server, '/webapp/admin/home', uid=9001)
    assert status == 200 and any(s['cb'] == 'ad_cms_shop' for s in home['sections'])
    for cb in ['ad_texts_main', 'ad_cms_vip', 'edit_txt_welcome', 'ad_bc']:
        status, data, _ = request(server, '/webapp/admin/vc', {'action': 'open', 'cb': cb}, uid=9001)
        assert status == 403 and data['error'] == 'bot_only'
    status, _, _ = request(server, '/webapp/admin/vc', {'action': 'open', 'cb': 'ad_cms_shop'})
    assert status == 403
