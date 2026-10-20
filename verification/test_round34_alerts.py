"""Rounds 34–35: the purchase referral reward is sent even when the referrer's balance is negative (no error alert);
deductions on a negative balance stay protected; timed deletes of an already-deleted message stay quiet; browser-extension errors are not store errors."""
import types
from unittest.mock import Mock

import pytest
from telebot.apihelper import ApiTelegramException

from harness import MESSAGES
from test_integration import mod, isolate, server, request, APP  # noqa: F401  (shared fixtures)
from test_error_monitor import empty_monitor  # noqa: F401
from test_referral_integrity import referral


def _tg_400(description):
    return ApiTelegramException('deleteMessage', types.SimpleNamespace(status_code=400),
                                {'ok': False, 'error_code': 400, 'description': description})


def _alerts(mod):
    mod._error_drain()
    return list(mod.db.system_errors.find({'level': {'$in': ['ERROR', 'CRITICAL']}}))


def _bal(mod, uid=1001):
    return mod.db.users.find_one({'user_id': uid})['balance']


def test_negative_referrer_balance_still_gets_the_purchase_reward(mod, monkeypatch):
    """Owner's rule (update 35): the reward is sent even when the referrer's balance is negative."""
    referral(mod)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -.4}})
    notices = Mock()
    monkeypatch.setattr(mod, 'notify_admins', notices)
    reward = mod.get_referral_purchase_reward()
    sent = len(MESSAGES)
    assert mod.award_purchase_referral_reward(5001, 'Netflix', 10) is True
    assert _bal(mod) == round(-.4 + reward, 6)
    ev = mod.db.ref_money_events.find_one({'kind': 'ref_purchase'})
    assert ev['status'] == 'applied' and 'held' not in ev
    assert mod.db.purchase_rewards.count_documents({'ref_event': ev['_id'], 'buyer_id': 5001}) == 1
    assert [m for m in MESSAGES[sent:] if m[0] == 1001]                 # the referrer got the reward message
    assert _alerts(mod) == [] and notices.call_count == 0               # no error alert, no admin notice
    mod._ref_resume_financial()
    mod._ref_resume_financial()
    assert _bal(mod) == round(-.4 + reward, 6) and mod.db.purchase_rewards.count_documents({}) == 1


def test_deductions_on_a_negative_balance_are_still_protected(mod):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -.4}})
    for kind, amount in (('ref_clawback', -.5), ('ref_purchase', -.1)):
        with pytest.raises(ValueError):
            mod._ref_money(1001, amount, kind, 'test', event_key='neg-' + kind)
    assert _bal(mod) == -.4


def test_normal_referrer_reward_is_unchanged(mod):
    referral(mod)
    reward = mod.get_referral_purchase_reward()
    assert mod.award_purchase_referral_reward(5001, 'Netflix', 10) is True
    assert _bal(mod) == round(100 + reward, 6)
    ev = mod.db.ref_money_events.find_one({'kind': 'ref_purchase'})
    assert ev['status'] == 'applied' and 'held' not in ev
    assert mod.db.purchase_rewards.count_documents({'ref_event': ev['_id']}) == 1
    mod._ref_resume_financial()
    assert mod.db.purchase_rewards.count_documents({}) == 1 and _bal(mod) == round(100 + reward, 6)


def test_reward_saved_by_the_previous_version_is_credited_and_recorded_once(mod):
    """Today's $0.10: the old version saved it (balance was negative) and never recorded it."""
    import hashlib, time
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -.4}})
    op = 'f' * 48
    mod.db.ref_money_events.insert_one({'_id': hashlib.sha256(op.encode()).hexdigest()[:32], 'user_id': 1001,
        'amount': .1, 'kind': 'ref_purchase', 'note': 'مكافأة شراء إحالة (5001)', 'operation_key': op,
        'status': 'prepared', 'created_at': int(time.time()) - 600})
    sent = len(MESSAGES)
    mod._ref_resume_financial()
    mod._ref_resume_financial()
    assert _bal(mod) == -.3                                             # credited even though still negative
    assert mod.db.purchase_rewards.count_documents({'buyer_id': 5001, 'referrer_id': 1001}) == 1
    assert len([m for m in MESSAGES[sent:] if m[0] == 1001]) == 1      # told once


def test_interrupted_award_is_finished_and_recorded_once(mod, monkeypatch):
    referral(mod)
    reward = mod.get_referral_purchase_reward()
    original = mod.db.balance_logs.update_one
    monkeypatch.setattr(mod.db.balance_logs, 'update_one', Mock(side_effect=RuntimeError('Receipt unavailable')))
    assert mod.award_purchase_referral_reward(5001, 'Netflix', 10) is False
    monkeypatch.setattr(mod.db.balance_logs, 'update_one', original)
    assert mod.db.ref_money_events.find_one({})['held']
    sent = len(MESSAGES)
    mod._ref_resume_financial()
    mod._ref_resume_financial()
    assert _bal(mod) == round(100 + reward, 6)
    assert mod.db.purchase_rewards.count_documents({'buyer_id': 5001, 'product': 'Netflix'}) == 1
    assert len([m for m in MESSAGES[sent:] if m[0] == 1001]) == 1


def test_clawback_on_a_negative_balance_waits_quietly(mod, monkeypatch):
    referral(mod, paid=True)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -.4}})
    notices = Mock()
    monkeypatch.setattr(mod, 'notify_admins', notices)
    mod.mark_referral_status(5001, 'left')
    mod._ref_resume_financial()
    assert mod._ref_find(5001)['cb_status'] == 'processing' and _bal(mod) == -.4
    assert _alerts(mod) == [] and notices.call_count == 1
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': .25}})
    mod._ref_resume_financial()
    assert mod._ref_find(5001)['cb_status'] == 'applied'
    user = mod.db.users.find_one({'user_id': 1001})
    assert user['balance'] == 0 and user['ref_debt'] == .75


def test_timed_delete_of_an_already_deleted_message_is_quiet(mod, monkeypatch):
    def gone(*a, **k):
        raise _tg_400('Bad Request: message to delete not found')
    monkeypatch.setitem(mod._VC_ORIG, 'delete_message', gone)
    assert mod._delete_quietly(1001, 77) is False
    mod._error_drain()
    assert mod.db.system_errors.count_documents({}) == 0
    src = (APP / 'bot.py').read_text(encoding='utf-8')
    assert 'threading.Timer(_CREDS_TTL, _delete_quietly' in src and 'lambda: bot.delete_message' not in src


@pytest.mark.parametrize('text', ['Bad Request: message to delete not found', "Bad Request: message can't be deleted",
                                  'Bad Request: message is not modified: specified new message content and reply markup are exactly the same',
                                  'Bad Request: query is too old and response timeout expired or query ID is invalid'])
def test_benign_telegram_400_is_recorded_as_warning_without_alert(mod, monkeypatch, text):
    notices = Mock()
    monkeypatch.setattr(mod, 'notify_admins', notices)
    with mod._error_scope(source='background', operation='Thread-8293'):
        ref = mod._error_capture('Uncaught background exception', _tg_400(text))
    mod._error_drain()
    assert mod.db.system_errors.find_one({'_id': ref})['level'] == 'WARNING' and notices.call_count == 0


def test_real_telegram_errors_still_alert(mod, monkeypatch):
    notices = Mock()
    monkeypatch.setattr(mod, 'notify_admins', notices)
    exc = ApiTelegramException('sendMessage', types.SimpleNamespace(status_code=400),
                               {'ok': False, 'error_code': 400, 'description': "Bad Request: can't parse entities"})
    with mod._error_scope(source='background', operation='Thread-1'):
        mod._error_capture('Uncaught background exception', exc)
    mod._error_drain()
    assert notices.call_count == 1


def test_browser_extension_errors_are_not_store_errors(mod, server):
    mod._WA_RATE.clear()
    s, r, _ = request(server, '/webapp/client/error', {
        'message': "Uncaught TypeError: Cannot assign to read only property 'isTerraExtensionAvailable' of object '#<Window>'",
        'location': 'chrome-extension://lgmpcpglpngdoalbgeoldeajfclnhafa/inpage.js:164:178359', 'operation': 'runtime'})
    assert s == 200 and r.get('ignored') == 'browser_extension'
    for loc, stack in (('moz-extension://abc/content.js:1:2', ''), ('safari-web-extension://x/inpage.js:3:4', ''),
                       ('Unhandled promise rejection', 'Error: x\n    at f (chrome-extension://abc/inpage.js:1:9)'),
                       ('webkit-masked-url://hidden/:1:1', '')):
        s, r, _ = request(server, '/webapp/client/error', {'message': 'x', 'location': loc, 'stack': stack})
        assert s == 200 and r.get('ignored') == 'browser_extension', loc
    mod._error_drain()
    assert mod.db.system_errors.count_documents({}) == 0
    mod._WA_RATE.clear()
    s, r, _ = request(server, '/webapp/client/error', {'message': 'Our own failure',
                                                      'location': 'https://local.example/app/:120:5', 'operation': 'runtime',
                                                      'stack': 'Error\n    at render (https://local.example/app/:120:5)\n    at chrome-extension://abc/hook.js:1:1'})
    assert s == 200 and r.get('reference')
    mod._error_drain()
    assert mod.db.system_errors.count_documents({}) == 1


def test_site_reporter_skips_extension_scripts():
    html = (APP / 'webapp/index.html').read_text(encoding='utf-8')
    boot = html[html.index('id="lara-error-bootstrap"'):html.index('__LARA_ERROR_REPORTER__=')]
    assert 'chrome|moz|safari-web|safari|ms-browser' in boot and 'webkit-masked-url' in boot
    assert 'if(fromExtension(locationText,extra.stack))return;' in boot
    assert "const build='lara-38.1'" in html
