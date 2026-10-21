"""Real callback dispatch with Telegram's UTF-8 button size rule; no live APIs."""
from unittest.mock import Mock

import pytest
from bson import ObjectId
from harness import MESSAGES, reply
from test_integration import mod, isolate, product
from test_products_menu import buttons, dispatch


@pytest.fixture(autouse=True)
def telegram_limit(mod, isolate, monkeypatch):
    def checked(chat_id, text='', **kwargs):
        markup = kwargs.get('reply_markup')
        for row in markup.to_dict()['inline_keyboard'] if markup else []:
            for button in row:
                if 'callback_data' in button:
                    value = button['callback_data']
                    assert 1 <= len(value.encode('utf-8')) <= 64, value
        return reply(chat_id, text, **kwargs)
    monkeypatch.setitem(mod._VC_ORIG, 'send_message', checked)
    monkeypatch.setitem(mod._VC_ORIG, 'edit_message_text',
        lambda text, chat_id, message_id, **kwargs: checked(chat_id, text, **kwargs))


def catalog(mod, linked=False, mixed=False):
    parent = mod.db.cgpt_products.insert_one({'name': 'اشتراك ChatGPT', 'name_en': 'ChatGPT subscription',
        'custom_emoji_id': '5368324170671202286', 'durations': [
            {'dur_id': '1760960000', 'label': 'شهر', 'label_en': 'Month', 'minutes': 43200, 'price': 8}]}).inserted_id
    aliases = [f'cgpt_main_{parent}', f'cgpt_{parent}', str(parent)]
    if linked:
        p = product(mod, f'cgpt_main_{parent}', stock=0, product_type='cgpt_main', cgpt_product_id=str(parent))
        aliases.append(p['id'])
    if mixed:
        p = product(mod, str(ObjectId()), stock=2)
        aliases.append(p['id'])
        p = product(mod, '12345', stock=0, manual=True)
        aliases.append(p['id'])
        ext = mod.db.ext_products.insert_one({'name': 'Supplier product', 'stock': 4, 'price': 2}).inserted_id
        aliases.append('ext_' + str(ext))
    folder = mod.db.catalogs.insert_one({'_id': ObjectId('6a1c992f5c47b9ac7aeeedad'),
        'name_ar': 'اشتراكات', 'name_en': 'Subscriptions', 'product_ids': aliases}).inserted_id
    if mixed:
        mod.db.ext_products.update_one({'_id': ext}, {'$set': {'catalog_id': str(folder)}})
    return str(parent), str(folder)


@pytest.mark.parametrize('uid', [1001, 1002, 9001])
@pytest.mark.parametrize('linked', [False, True])
@pytest.mark.parametrize('edit', [False, True])
def test_chatgpt_folder_is_accepted_by_telegram_and_product_and_back_still_dispatch(mod, uid, linked, edit):
    parent, folder = catalog(mod, linked=linked)
    before = list(mod.db.users.find())
    assert mod.catalog_view_helper(uid, uid, folder, mod.get_lang(uid), message_id_to_edit=10 if edit else None)
    rows = buttons(MESSAGES[-1])
    button = next(b for row in rows for b in row if b.get('callback_data', '').startswith('vi_p_'))
    assert len(button['callback_data'].encode('utf-8')) <= 64
    assert button['icon_custom_emoji_id'] == '5368324170671202286'
    assert button['style'] == 'success'
    dispatch(mod, button['callback_data'], uid)
    detail = [b for row in buttons(MESSAGES[-1]) for b in row]
    assert any(b.get('callback_data') == f'cgpt_buy_{parent}_1760960000' for b in detail)
    back = next(b['callback_data'] for b in detail if b.get('callback_data') == 'cat_' + folder)
    dispatch(mod, back, uid)
    assert any(b.get('callback_data') == button['callback_data'] for row in buttons(MESSAGES[-1]) for b in row)
    assert list(mod.db.users.find()) == before
    assert mod.db.orders.count_documents({}) == 0


@pytest.mark.parametrize('uid', [1001, 1002, 9001])
def test_mixed_folder_all_buttons_and_native_quantity_and_chatgpt_duration_work(mod, monkeypatch, uid):
    parent, folder = catalog(mod, linked=True, mixed=True)
    assert mod.catalog_view_helper(uid, uid, folder, mod.get_lang(uid))
    keys = [b['callback_data'] for row in buttons(MESSAGES[-1]) for b in row]
    assert len([key for key in keys if key.startswith(('vi_p_', 'vext_'))]) == 4
    for key in keys:
        if key.startswith('vi_p_'):
            dispatch(mod, key, uid)
            detail = [b['callback_data'] for row in buttons(MESSAGES[-1]) for b in row]
            if not any(cb.startswith('cgpt_buy_') for cb in detail):
                buy = next(cb for cb in detail if cb.startswith('buy_qty_'))
                dispatch(mod, buy, uid)
    step = Mock()
    monkeypatch.setattr(mod.bot, 'register_next_step_handler', step)
    if uid == 9001:
        mod.db.users.update_one({'user_id': uid}, {'$set': {'balance': 100}})
        mod._USER_CACHE.clear()
    dispatch(mod, f'cgpt_buy_{parent}_1760960000', uid)
    assert step.call_args.args[1] is mod.cgpt_confirm_email_step
    assert mod._cgpt_pending[uid]['cgpt_pid'] == parent
    assert mod._cgpt_pending[uid]['price'] == 8


@pytest.mark.parametrize('prefix', ['', 'cgpt_', 'cgpt_main_'])
def test_legacy_and_compact_product_callbacks_remain_compatible(mod, prefix):
    parent, folder = catalog(mod)
    dispatch(mod, f'vi_p_{prefix}{parent}_c_{folder}')
    assert any(b.get('callback_data') == f'cgpt_buy_{parent}_1760960000'
               for row in buttons(MESSAGES[-1]) for b in row)
