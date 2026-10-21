"""Round 41: two referral links (bot + store) both register the referral; every new referral must pass the
emoji identity check on the website even when it already joined the forced channels; every N site-verified
referrals are paid automatically; texts/buttons that lived in the code are editable from the CMS."""
import http.client
import json

import pytest
from harness import MESSAGES, reply
from test_integration import mod, isolate, server, request, signed, _verified_ref_request, _ref_fp  # noqa: F401
from test_products_menu import dispatch, buttons
from test_referral_policy import message

CH = {'ok': True, 'ms': 1500, 'trusted': True, 'tries': 1}


@pytest.fixture(autouse=True)
def keep_edited_buttons(mod, monkeypatch):
    monkeypatch.setitem(mod._VC_ORIG, 'edit_message_text',
                        lambda text, chat_id=None, message_id=None, **kw: reply(chat_id, text, **kw))


def _site(server, path, uid, body=None, start_param=None):
    extra = {'start_param': start_param} if start_param else {}
    conn = http.client.HTTPConnection(*server, timeout=4)
    conn.request('POST', path, json.dumps(body or {}),
                 {'Content-Type': 'application/json', 'X-Telegram-Init-Data': signed(uid, **extra)})
    res = conn.getresponse()
    data = json.loads(res.read() or b'{}')
    conn.close()
    return res.status, data


def _links_on(mod, monkeypatch, on=True):
    monkeypatch.setattr(mod, '_bot_main_app', lambda: on)
    monkeypatch.setattr(mod, 'get_bot_username', lambda: 'lara_local_test_bot')


def _last_screen(uid=1001):
    return next(r for r in reversed(MESSAGES) if r[0] == uid and buttons(r))


def _copy_values(record):
    return [b['copy_text']['text'] for row in buttons(record) for b in row if 'copy_text' in b]


# ───────────────────────── 1) the two links ─────────────────────────
def test_bot_referral_screen_offers_both_links(mod, monkeypatch):
    _links_on(mod, monkeypatch)
    dispatch(mod, 'open_invite', 1001)
    rec = _last_screen()
    bot_link, app_link = 'https://t.me/lara_local_test_bot?start=1001', 'https://t.me/lara_local_test_bot?startapp=1001'
    assert bot_link in rec[1] and app_link in rec[1]
    assert _copy_values(rec) == [bot_link, app_link]
    assert any(b.get('callback_data') == 'ref_list_0' for row in buttons(rec) for b in row)


def test_store_link_needs_main_mini_app_and_only_the_admin_sees_how(mod, monkeypatch):
    _links_on(mod, monkeypatch, on=False)
    dispatch(mod, 'open_invite', 1001)
    rec = _last_screen()
    assert '?start=1001' in rec[1] and 'startapp' not in rec[1] and 'BotFather' not in rec[1]
    assert _copy_values(rec) == ['https://t.me/lara_local_test_bot?start=1001']
    dispatch(mod, 'open_invite', 9001)
    assert 'Main Mini App' in _last_screen(9001)[1]


def test_site_referral_card_receives_both_links(mod, server, monkeypatch):
    _links_on(mod, monkeypatch)
    s, r, _ = request(server, '/webapp/referrals', {})
    assert s == 200 and r['links'] == {'bot': 'https://t.me/lara_local_test_bot?start=1001',
                                        'app': 'https://t.me/lara_local_test_bot?startapp=1001'}
    assert r['link'] == r['links']['bot'] and r['auto_pay'] is True


def test_bot_link_registers_and_still_requires_the_emoji_check_after_joining_channels(mod, server, monkeypatch):
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: True)          # already joined every channel
    monkeypatch.setattr(mod, '_wa_public_url', lambda: 'https://local.example/app/')
    mod.start_handler(message(mod, 3001, '/start 1001'))
    rec = mod._ref_find(3001)
    assert rec and rec['referrer_id'] == 1001 and rec['status'] == 'pending' and mod.ref_dv_required(rec)
    mod.db.users.update_one({'user_id': 3001}, {'$set': {'lang_chosen': True, 'lang': 'en'}})
    mod._invalidate_user_cache(3001)
    sent = len(MESSAGES)
    mod.start_handler(message(mod, 3001, '/start'))
    gate = [m for m in MESSAGES[sent:] if m[0] == 3001]
    assert gate and 'One last step' in gate[-1][1]                                # the verify button, not the menu
    assert any('web_app' in b for row in buttons(gate[-1]) for b in row)
    assert mod._ref_find(3001)['status'] == 'pending'                             # channels alone don't count
    s, d, _ = _verified_ref_request(mod, server, 3001, _ref_fp('c' * 32), CH)
    assert s == 200 and mod._ref_find(3001)['status'] == 'active'


@pytest.mark.parametrize('device_check', ['on', 'off'])
def test_store_link_registers_and_requires_the_emoji_check(mod, server, monkeypatch, device_check):
    mod.db.settings.update_one({'key': 'ref_dv'}, {'$set': {'value': device_check}}, upsert=True)
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: True)
    s, boot = _site(server, '/webapp/bootstrap', 3002, start_param='1001')
    assert s == 200 and boot['verify'] is True                                    # the store opens on the check
    rec = mod._ref_find(3002)
    assert rec['referrer_id'] == 1001 and rec['status'] == 'pending'
    s, d, _ = _verified_ref_request(mod, server, 3002, _ref_fp('d' * 32), CH)
    assert s == 200 and mod._ref_find(3002)['status'] == 'active'
    s, boot = _site(server, '/webapp/bootstrap', 3002)
    assert boot['verify'] is False


def test_store_link_never_rewrites_an_existing_customer(mod, server):
    s, _ = _site(server, '/webapp/bootstrap', 1002, start_param='1001')
    assert s == 200 and not mod._ref_find(1002)


# ───────────────────────── 2) automatic reward every N site-verified referrals ─────────────────────────
def _verified_batch(mod, server, monkeypatch, n=10, start=4000):
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: True)
    for i in range(n):
        uid = start + i
        mod.db.users.insert_one({'user_id': uid, 'name': f'Inv{uid}', 'balance': 0., 'lang': 'en', 'is_admin': 0, 'is_banned': 0})
        mod.register_new_referral(uid, 1001)
        s, d, _ = _verified_ref_request(mod, server, uid, _ref_fp('%032x' % uid), CH)
        assert s == 200, d


def test_ten_verified_referrals_pay_the_batch_reward_automatically(mod, server, monkeypatch):
    reward = mod.get_referral_reward()
    sent = len(MESSAGES)
    _verified_batch(mod, server, monkeypatch)
    ms = mod.db.referral_milestones.find_one({'referrer_id': 1001, 'batch_no': 1})
    assert ms['status'] == 'paid' and ms['auto'] and ms['paid_by'] == 'auto' and ms['paid_amount'] == reward
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == round(100 + reward, 6)
    new = MESSAGES[sent:]
    assert [m for m in new if m[0] == 1001 and f"{reward:.2f}" in str(m[1])]          # the referrer is told
    assert [m for m in new if 'صُرفت مكافأة إحالات تلقائيًا' in str(m[1])]             # the admin gets the report
    mod._ref_resume_financial()
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == round(100 + reward, 6)   # never twice


def test_nine_verified_referrals_pay_nothing_yet(mod, server, monkeypatch):
    _verified_batch(mod, server, monkeypatch, n=9)
    assert not mod.db.referral_milestones.count_documents({})
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_batch_with_an_older_unverified_referral_waits_for_the_admin(mod, server, monkeypatch):
    mod.db.referrals_v2.insert_one({'invited_id': 3999, 'referrer_id': 1001, 'status': 'active', 'created_at': 1})  # pre-policy
    _verified_batch(mod, server, monkeypatch, n=9)
    ms = mod.db.referral_milestones.find_one({'referrer_id': 1001, 'batch_no': 1})
    assert ms and ms['status'] == 'pending' and mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_old_pending_batches_stay_for_the_admin(mod, server, monkeypatch):
    mod.db.referral_milestones.insert_one({'referrer_id': 1001, 'batch_no': 1, 'status': 'pending', 'threshold': 10,
                                           'reward': 1.0, 'members': [3990 + i for i in range(10)], 'created_at': 1})
    _verified_batch(mod, server, monkeypatch, n=10)
    old = mod.db.referral_milestones.find_one({'referrer_id': 1001, 'batch_no': 1})
    assert old['status'] == 'pending'                                  # not touched (owner's choice)
    assert mod.db.referral_milestones.find_one({'referrer_id': 1001, 'batch_no': 2})['status'] == 'paid'


def test_switch_off_returns_to_admin_approval(mod, server, monkeypatch):
    mod.db.settings.update_one({'key': 'ref_auto_pay'}, {'$set': {'value': 'off'}}, upsert=True)
    _verified_batch(mod, server, monkeypatch)
    assert mod.db.referral_milestones.find_one({'batch_no': 1})['status'] == 'pending'
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_admin_can_reject_after_the_automatic_payment_and_its_value_comes_back(mod, server, monkeypatch):
    reward = mod.get_referral_reward()
    _verified_batch(mod, server, monkeypatch)
    dispatch(mod, 'rfd_no_4003_1001_1_0', 9001)
    assert mod._ref_find(4003)['dv'] == 'rejected'
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == round(100 + reward - reward / 10, 6)


def test_automatic_reward_reaches_a_referrer_with_a_negative_balance(mod, server, monkeypatch):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -0.5}})
    reward = mod.get_referral_reward()
    _verified_batch(mod, server, monkeypatch)
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == round(-0.5 + reward, 6)


# ───────────────────────── 3) CMS ─────────────────────────
def _custom(mod, key, ar, en=None):
    for lang, value in (('ar', ar), ('en', en if en is not None else ar)):
        mod.db.custom_texts.update_one({'lang': lang, 'key': key}, {'$set': {'value': value}}, upsert=True)
    mod._load_custom_texts_cache()


def test_log_via_lines_are_editable(mod):
    _custom(mod, 'log_via_webapp', '🛰 <b>From:</b> our site', '🛰 <b>From:</b> our site')
    _custom(mod, 'log_via_bot', '🤖 <b>From:</b> the bot', '🤖 <b>From:</b> the bot')
    with mod._error_scope():
        mod._LOG_SRC.v = 'webapp'
        assert mod._log_src('New purchase').endswith('🛰 <b>From:</b> our site')
        mod._LOG_SRC.v = 'bot'
        assert mod._log_src('New purchase').endswith('🤖 <b>From:</b> the bot')
        out = mod._log_src(mod._log_src('Twice'))
        assert out.count('From:') == 1
        mod._LOG_SRC.v = None


def test_referral_screen_and_buttons_follow_the_cms(mod, monkeypatch):
    _links_on(mod, monkeypatch)
    _custom(mod, 'ref_screen', 'شاشتي {2} نشط\n{7}', 'My screen: {2} active\n{7}')
    mod.db.custom_buttons.insert_one({'lang': 'en', 'key': 'btn_copy_ref_app', 'text': '🛍 Copy shop link'})
    dispatch(mod, 'open_invite', 1001)
    rec = _last_screen()
    assert rec[1].startswith('My screen: 0 active') and '?startapp=1001' in rec[1]
    assert any(b.get('text') == '🛍 Copy shop link' for row in buttons(rec) for b in row)


def test_the_admins_old_referral_design_becomes_the_new_editable_screen(mod, monkeypatch):
    old_ar = ('<tg-emoji emoji-id="5235630047959727475">💎</tg-emoji> <b>نظام الإحالات</b>\n\n'
              '👥 <b>الزيارات:</b>  <b>{}</b>\n⌛ <b>المعلق:</b>  <b>{}</b>\n✅ <b>النشط:</b>  <b>{}</b>\n❌ <b>غادر:</b>  <b>{}</b>\n\n'
              '💵 <b>أرباحك:</b>  <code>${:.2f}</code>\n\n━━━━━━━━━━━━━━\n🔗 <b>رابطك:</b>\n<code>https://t.me/{}?start={}</code>\n\n'
              '🔥 كل <b>10</b> اشتراكات نشطة = <b>$0.10</b>\n💵 شراء صديقك > <b>$2</b> = <b>$0.10</b>')
    _custom(mod, 'invite_txt', old_ar, old_ar.replace('نظام الإحالات', 'Referral system'))
    mod.db.settings.delete_many({'key': 'ref_screen_v1'})
    mod._ref_screen_migrate()
    new = mod.db.custom_texts.find_one({'lang': 'ar', 'key': 'ref_screen'})['value']
    assert '5235630047959727475' in new and '{7}' in new and '<b>{8}</b>' in new and '{}' not in new
    _links_on(mod, monkeypatch)
    mod.db.users.update_one({'user_id': 1002}, {'$set': {'ref_v2_earned': 1.5}})
    mod.db.settings.update_one({'key': 'referral_reward'}, {'$set': {'value': '1.0'}}, upsert=True)
    dispatch(mod, 'open_invite', 1002)
    text = _last_screen(1002)[1]
    assert '$1.50' in text and '?start=1002' in text and '?startapp=1002' in text and '<b>$1.00</b>' in text
    mod._ref_screen_migrate()                                           # once only
    assert mod.db.custom_texts.count_documents({'key': 'ref_screen'}) == 2


def test_every_extra_cms_entry_opens_an_editor(mod):
    for key, (sec, label, ar, en) in mod.EXTRA_CMS_TEXTS.items():
        assert sec in dict(mod.EXTRA_CMS_SECTIONS) and mod.LANG['ar'][key] and mod.LANG['en'][key], key
    for key, (sec, label, ar, en) in mod.EXTRA_CMS_BUTTONS.items():
        assert mod.DEFAULT_BUTTONS['ar'][key] == ar and mod.DEFAULT_BUTTONS['en'][key] == en
    dispatch(mod, 'ad_cms_x', 9001)
    menu = _last_screen(9001)
    sections = [b['callback_data'] for row in buttons(menu) for b in row if b['callback_data'].startswith('ad_cms_x_')]
    assert len(sections) == len(mod.EXTRA_CMS_SECTIONS)
    seen = set()
    for cb in sections:
        dispatch(mod, cb, 9001)
        seen |= {b['callback_data'] for row in buttons(_last_screen(9001)) for b in row}
    for key in mod.EXTRA_CMS_TEXTS:
        assert 'edit_txt_' + key in seen, key
    for key in mod.EXTRA_CMS_BUTTONS:
        assert 'edit_btn_' + key in seen, key
    sent = len(MESSAGES)
    dispatch(mod, 'edit_txt_ref_screen', 9001)
    assert any('{7}' in str(m[1]) for m in MESSAGES[sent:])               # the admin sees what each value means


def test_converted_texts_no_longer_hard_coded(mod):
    src = (mod.__file__ and open(mod.__file__, encoding='utf-8').read())
    for literal in ('"👥 My Referrals" if l == \'en\'', "'📋 Copy order ID' if lang == 'en'",
                    '"📄 Here are all your purchases." if l == \'en\'', "\"🔑 Show connection details\" if l == 'en'",
                    "'🛍 Open store' if lang == 'en'", "'❌ Category unavailable.' if lang == 'en'"):
        assert literal not in src, literal
