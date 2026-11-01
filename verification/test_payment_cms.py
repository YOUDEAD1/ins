"""Native customization and website payment copy; all provider calls are fixtures."""
from test_integration import mod, isolate, server, request  # noqa: F401
from test_security_fixes_round2 import _msg
from test_products_menu import dispatch
from harness import MESSAGES, reply


def test_native_catalog_exposes_all_registered_texts_and_buttons(mod):
    texts = {key for kind, key, _ in mod._cms_catalog_entries('text')}
    buttons = {key for kind, key, _ in mod._cms_catalog_entries('button')}
    assert texts == {key for values in mod.LANG.values() for key, value in values.items() if isinstance(value, str)}
    assert buttons == {key for values in mod.DEFAULT_BUTTONS.values() for key in values}
    assert set(mod.DEPOSIT_CMS_TEXTS) <= texts
    assert mod.DEPOSIT_CMS_BUTTONS <= buttons


def test_payment_catalog_pages_reach_manual_prompt_and_buttons(mod, monkeypatch):
    monkeypatch.setitem(mod._VC_ORIG, 'edit_message_text', lambda text, uid, mid, **kw: reply(uid, text, **kw))
    entries = mod._cms_catalog_entries('deposit', deposit=True)
    seen = set()
    for page in range((len(entries) + 11) // 12):
        dispatch(mod, f'ad_cms_catalog_deposit_{page}', 9001)
        markup = MESSAGES[-1][2]['reply_markup'].to_dict()
        seen.update(b.get('callback_data') for row in markup['inline_keyboard'] for b in row)
    assert 'edit_txt_dep_binance_manual_prompt' in seen
    assert 'edit_txt_dep_binance_unavailable' in seen
    assert 'edit_btn_dep_btn_manual' in seen
    assert 'edit_btn_dep_btn_check' in seen


def test_custom_payment_prompt_and_button_apply_in_bot_and_site(mod):
    mod.db.custom_texts.insert_one({'lang': 'ar', 'key': 'dep_binance_manual_prompt', 'value': '<b>أرسل رقم العملية هنا</b>'})
    mod.db.custom_buttons.insert_one({'lang': 'ar', 'key': 'dep_btn_manual', 'text': 'رقم العملية',
                                     'emoji_id': '5271619747891388291', 'style': 'primary'})
    mod.register_pending_deposit(1002, 3, 3.002, 'BINANCE')
    mod.binance_manual_start(mod._WACall(1002, 'binance_manual_start'))
    assert MESSAGES[-1][1] == '<b>أرسل رقم العملية هنا</b>'
    button = mod._deposit_action_button(1002, 'dep_btn_manual', 'binance_manual_start').to_dict()
    assert button['text'] == 'رقم العملية' and button['style'] == 'primary'
    assert button['icon_custom_emoji_id'] == '5271619747891388291'
    site = mod._wa_texts()['ar']
    assert site['dep_binance_manual_prompt'] == '<b>أرسل رقم العملية هنا</b>'
    assert site['dep_btn_manual'] == 'رقم العملية'


def test_original_text_editor_saves_payment_copy_and_preserves_required_amount(mod, monkeypatch):
    monkeypatch.setattr(mod, 'safe_translate_for_cms', lambda text, lang: text)
    mod.ad_save_custom_text(_msg(mod, 'Enter the payment identifier here', 9001), 'dep_binance_manual_prompt')
    assert mod.get_text(1001, 'dep_binance_manual_prompt') == 'Enter the payment identifier here'
    mod.ad_save_custom_text(_msg(mod, 'Payment complete', 9001), 'dep_binance_verified')
    assert mod.db.custom_texts.count_documents({'key': 'dep_binance_verified'}) == 0


def test_payment_cms_is_available_in_authenticated_console_but_private_to_admin(mod, server, monkeypatch):
    monkeypatch.setattr(mod, '_vc_start', lambda session, fn, wait=None: mod._vc_run(session, fn))
    code, result, _ = request(server, '/webapp/admin/home', uid=9001)
    assert code == 200 and any(s['cb'] == 'ad_texts_main' for s in result['sections'])
    for callback in ['ad_texts_main', 'ad_cms_catalog_deposit_0', 'edit_txt_dep_binance_manual_prompt', 'edit_btn_dep_btn_manual']:
        code, result, _ = request(server, '/webapp/admin/vc', {'action': 'open', 'cb': callback}, uid=9001)
        assert code == 200 and result['msgs']
        assert request(server, '/webapp/admin/vc', {'action': 'open', 'cb': callback}, uid=1001)[0] == 403


def test_purchase_date_in_bot_includes_year_date_seconds_and_timezone(mod):
    assert mod._up_date(1793527748) == '2026-11-01 10:09:08 UTC'


def test_legacy_copy_customization_remains_visible_when_only_button_color_is_changed(mod):
    mod.db.custom_texts.insert_one({'lang': 'en', 'key': 'dep_btn_copy_amount',
        'value': '<tg-emoji emoji-id="5271619747891388291">📋</tg-emoji> Copy exact payment'})
    mod.db.custom_buttons.insert_one({'lang': 'en', 'key': 'dep_btn_copy_amount', 'style': 'primary'})
    label, emoji = mod.get_btn_data(1001, 'dep_btn_copy_amount')
    assert label == 'Copy exact payment' and emoji == '5271619747891388291'
    button = mod._deposit_copy_button(1001, 'dep_btn_copy_amount', '3.0023').to_dict()
    assert button['style'] == 'primary' and button['copy_text']['text'] == '3.0023'
    assert button['icon_custom_emoji_id'] == emoji
