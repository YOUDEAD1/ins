"""Reported outages, delivery recovery and direct store links; external services stay blocked."""
import datetime
import types
from unittest.mock import Mock

import pytest
from harness import MESSAGES, reply
from test_integration import mod, isolate, server, request, product
from test_error_monitor import empty_monitor
from test_products_menu import buttons
from test_reliability import manager


class Timeout(Exception):
    code = 28


def account_doc(session='test-session'):
    return {'_id': 'test-account', 'data': {'accessToken': 'test-token', 'sessionToken': session,
        'user': {'email': 'owner@example.test'}, 'account': {'id': 'test-account'}}}


def message(mod, uid=9001, text='/start', chat=None):
    return mod.types.Message.de_json({'message_id': 1, 'date': 1,
        'chat': {'id': uid if chat is None else chat, 'type': 'private'},
        'from': {'id': uid, 'is_bot': False, 'first_name': 'Test'}, 'text': text})


@pytest.fixture(autouse=True)
def recovery_state(mod, isolate, empty_monitor, monkeypatch):
    mod._CGPT_MEMBER_HEALTH.clear()
    mod._ADMIN_NOTICE_PAUSED.clear()
    mod._ADMIN_NOTICE_DELIVERY_CACHE.clear()
    mod._ADMIN_NOTICE_WAKE.clear()
    monkeypatch.setattr(mod, 'CFFI_AVAILABLE', True)
    yield
    mod._CGPT_MEMBER_HEALTH.clear()
    mod._ADMIN_NOTICE_PAUSED.clear()
    mod._ADMIN_NOTICE_DELIVERY_CACHE.clear()


def test_timeout_preserves_expired_access_and_backoff_survives_rebuilt_manager(mod, monkeypatch):
    clock = [1800000000.]
    monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    m = manager(mod, (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat())
    http = Mock(side_effect=[Timeout('Operation timed out after 15002 milliseconds with 0 bytes received') for _ in range(2)])
    monkeypatch.setattr(mod.cffi_requests, 'get', http)
    remove = Mock(); monkeypatch.setattr(m, '_remove_user', remove)
    m.check_and_cleanup()
    assert not remove.called and m.invites_data['invites']['paid@example.test']['status'] == 'active'
    assert m._last_fetch_failed and m._last_fetch_status == 'timeout'
    assert http.call_args.kwargs['timeout'] == (5, 20)
    rebuilt = mod._cgpt_build_manager_from_doc(account_doc())
    assert rebuilt._get_org_users() == [] and http.call_count == 1
    clock[0] += 61
    rebuilt._get_org_users()
    assert http.call_count == 2
    assert mod._cgpt_member_health(rebuilt)['retry_at'] == clock[0] + 120
    mod._error_drain()
    rows = list(mod.db.system_errors.find())
    assert len(rows) == 1 and rows[0]['level'] == 'WARNING' and rows[0]['count'] == 2
    assert mod.db.api_purchase_notices.count_documents({}) == 0


def test_member_recovery_resets_old_status_and_resolves_only_its_warning(mod, monkeypatch):
    clock = [1800000000.]; monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    m = manager(mod)
    monkeypatch.setattr(mod.cffi_requests, 'get', Mock(side_effect=Timeout('Timed out')))
    m._last_fetch_status = 401
    assert m._get_org_users() == [] and m._last_fetch_status == 'timeout'
    other = mod._error_capture('Unrelated real error')
    mod._error_drain()
    clock[0] += 61
    users = [{'id': 'paid', 'email': 'paid@example.test'}]
    monkeypatch.setattr(mod.cffi_requests, 'get', lambda *a, **k: types.SimpleNamespace(
        status_code=200, json=lambda: {'items': users}))
    assert m._get_org_users() == users
    assert m._last_fetch_status == 200 and not m._last_fetch_failed
    assert mod._cgpt_member_health(m)['failures'] == 0
    assert mod.db.system_errors.find_one({'operation': 'cgpt:members:test-account'})['resolved']
    assert not mod.db.system_errors.find_one({'_id': other})['resolved']


def test_changed_cookie_credentials_allow_immediate_read_without_old_cooldown(mod, monkeypatch):
    m = manager(mod)
    http = Mock(side_effect=Timeout('Timed out')); monkeypatch.setattr(mod.cffi_requests, 'get', http)
    m._get_org_users()
    fresh = mod._cgpt_build_manager_from_doc(account_doc('replacement-test-session'))
    fresh._get_org_users()
    assert http.call_count == 2
    assert 'test-token' not in str(mod._CGPT_MEMBER_HEALTH)
    assert 'replacement-test-session' not in str(mod._CGPT_MEMBER_HEALTH)


@pytest.mark.parametrize('payload', [{}, {'items': {}}, {'items': [None]},
    {'items': [], 'has_more': True}, {'items': [], 'total': 2}])
def test_invalid_or_incomplete_member_response_never_expires_or_removes_customers(mod, monkeypatch, payload):
    m = manager(mod, (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat())
    monkeypatch.setattr(mod.cffi_requests, 'get', lambda *a, **k: types.SimpleNamespace(status_code=200, json=lambda: payload))
    remove = Mock(); monkeypatch.setattr(m, '_remove_user', remove)
    m.check_and_cleanup()
    assert m._last_fetch_failed and not remove.called
    assert m.invites_data['invites']['paid@example.test']['status'] == 'active'


@pytest.mark.parametrize('status,delay', [(401, 3600), (403, 3600), (429, 300), (503, 60)])
def test_member_http_failures_use_bounded_retry_and_keep_status(mod, monkeypatch, status, delay):
    clock = [1800000000.]; monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    m = manager(mod)
    http = Mock(return_value=types.SimpleNamespace(status_code=status, headers={'Retry-After': '300' if status == 429 else '0'}))
    monkeypatch.setattr(mod.cffi_requests, 'get', http)
    assert m._get_org_users() == [] and m._last_fetch_status == status and m._last_fetch_failed
    assert mod._cgpt_member_health(m)['retry_at'] == clock[0] + delay
    assert m._get_org_users() == [] and http.call_count == 1


def test_daemon_timeout_does_not_claim_cookies_expired_or_repeat_subscription_calls(mod, monkeypatch):
    m = manager(mod)
    subscription = Mock(return_value='active'); monkeypatch.setattr(m, 'check_subscription_status', subscription)
    monkeypatch.setattr(mod, '_cgpt_build_manager_from_doc', lambda d: m)
    monkeypatch.setattr(mod.cffi_requests, 'get', Mock(side_effect=Timeout('Timed out')))
    cookies = Mock(); monkeypatch.setattr(mod, '_cgpt_notify_cookie_expired', cookies)
    reminders = Mock(); monkeypatch.setattr(mod, '_cgpt_send_expiry_reminders', reminders)
    assert not mod._cgpt_check_account(account_doc())
    assert not mod._cgpt_check_account(account_doc())
    assert subscription.call_count == 1 and not cookies.called and reminders.call_count == 2


def test_unavailable_subscription_stops_extra_member_read_without_false_cookie_notice(mod, monkeypatch):
    m = manager(mod)
    monkeypatch.setattr(m, 'check_subscription_status', lambda: 'error')
    cleanup = Mock(); monkeypatch.setattr(m, 'check_and_cleanup', cleanup)
    monkeypatch.setattr(mod, '_cgpt_build_manager_from_doc', lambda d: m)
    cookies = Mock(); monkeypatch.setattr(mod, '_cgpt_notify_cookie_expired', cookies)
    assert not mod._cgpt_check_account(account_doc())
    assert not cookies.called and not cleanup.called


def test_daemon_preserves_real_unauthorized_cookie_alert_and_cooldown(mod, monkeypatch):
    m = manager(mod)
    monkeypatch.setattr(m, 'check_subscription_status', lambda: 'unauthorized')
    monkeypatch.setattr(mod, '_cgpt_build_manager_from_doc', lambda d: m)
    cookies = Mock(); monkeypatch.setattr(mod, '_cgpt_notify_cookie_expired', cookies)
    mod._cgpt_check_account(account_doc()); mod._cgpt_check_account(account_doc())
    assert cookies.call_count == 1 and mod._cgpt_member_health(m)['status'] == 401


@pytest.mark.parametrize('cause', ['Forbidden: bot was blocked by the user', 'Bad Request: chat not found',
    "Forbidden: bot can't initiate conversation with a user", 'Forbidden: user is deactivated'])
def test_admin_unreachable_is_one_persisted_incident_not_an_error_per_notice(mod, monkeypatch, cause):
    send = Mock(side_effect=RuntimeError(cause)); monkeypatch.setattr(mod.bot, 'send_message', send)
    assert not mod.notify_admins('Purchase notice', recipient_ids={9001})
    assert not mod.notify_admins('Second purchase notice', recipient_ids={9001})
    assert send.call_count == 1
    state = mod.db.admin_notice_delivery.find_one({'_id': '9001'})
    assert state['state'] == 'unreachable' and state['failures'] == 1
    mod._ADMIN_NOTICE_PAUSED.clear(); mod._ADMIN_NOTICE_DELIVERY_CACHE.clear()
    assert not mod.notify_admins('After restart', recipient_ids={9001}) and send.call_count == 1
    mod._error_drain()
    record = mod.db.system_errors.find_one({})
    assert record['level'] == 'WARNING' and record['count'] == 1 and '/start' in record['cause']


def test_admin_retry_backs_off_across_restart_without_new_hourly_incidents(mod, monkeypatch):
    clock = [1800000000.]; monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    send = Mock(side_effect=RuntimeError('Forbidden: bot was blocked by the user'))
    monkeypatch.setattr(mod.bot, 'send_message', send)
    mod.notify_admins('First', recipient_ids={9001})
    clock[0] += 3601
    mod._ADMIN_NOTICE_PAUSED.clear(); mod._ADMIN_NOTICE_DELIVERY_CACHE.clear()
    mod.notify_admins('Later', recipient_ids={9001})
    assert send.call_count == 2
    state = mod.db.admin_notice_delivery.find_one({'_id': '9001'})
    assert state['failures'] == 2 and state['retry_at'] == clock[0] + 7200
    mod._error_drain()
    assert mod.db.system_errors.count_documents({}) == 1
    assert mod.db.system_errors.find_one({})['count'] == 2


def test_blocked_admin_keeps_every_notice_and_other_admin_does_not_get_duplicates(mod, monkeypatch):
    mod.db.users.update_one({'user_id': 1002}, {'$set': {'is_admin': 1}})
    delivered = []
    def send(uid, text, **kw):
        if uid == 9001:
            raise RuntimeError('Forbidden: bot was blocked by the user')
        delivered.append((uid, text)); return reply(uid, text, **kw)
    monkeypatch.setattr(mod.bot, 'send_message', send)
    mod._queue_api_purchase_notice('ORDER-A', 'Paid order A')
    mod._queue_api_purchase_notice('ORDER-B', 'Paid order B')
    mod._retry_api_purchase_notices()
    assert len(delivered) == 2 and {uid for uid, _ in delivered} == {1002}
    assert mod.db.api_purchase_notices.count_documents({'status': 'pending'}) == 2
    assert all(d['sent_to'] == [1002] for d in mod.db.api_purchase_notices.find())
    monkeypatch.setattr(mod.bot, 'send_message', lambda uid, text, **kw: delivered.append((uid, text)) or reply(uid, text, **kw))
    mod.start_handler(message(mod))
    assert mod.db.admin_notice_delivery.find_one({'_id': '9001'})['state'] == 'reachable'
    assert mod._ADMIN_NOTICE_WAKE.is_set()
    mod._retry_api_purchase_notices(); mod._retry_api_purchase_notices()
    assert mod.db.api_purchase_notices.count_documents({'status': 'sent'}) == 2
    assert len([1 for uid, text in delivered if uid == 1002]) == 2
    assert len([1 for uid, text in delivered if uid == 9001 and text.startswith('Paid order')]) == 2
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_successful_retry_resolves_delivery_incident_and_releases_pause(mod, monkeypatch):
    clock = [1800000000.]; monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    monkeypatch.setattr(mod.bot, 'send_message', Mock(side_effect=RuntimeError('chat not found')))
    mod.notify_admins('First', recipient_ids={9001}); mod._error_drain()
    clock[0] += 3601
    monkeypatch.setattr(mod.bot, 'send_message', reply)
    assert mod.notify_admins('Retry', recipient_ids={9001})
    assert 9001 not in mod._ADMIN_NOTICE_PAUSED
    assert mod.db.admin_notice_delivery.find_one({'_id': '9001'})['state'] == 'reachable'
    assert mod.db.system_errors.find_one({})['resolved']


def test_unrelated_user_start_cannot_resume_owner_delivery(mod, monkeypatch):
    monkeypatch.setattr(mod.bot, 'send_message', Mock(side_effect=RuntimeError('chat not found')))
    mod.notify_admins('First', recipient_ids={9001})
    monkeypatch.setattr(mod.bot, 'send_message', reply)
    mod.start_handler(message(mod, uid=1001))
    assert mod.db.admin_notice_delivery.find_one({'_id': '9001'})['state'] == 'unreachable'


@pytest.mark.parametrize('uid,word', [(1001, 'out of date'), (1002, 'قديم')])
def test_old_product_link_offers_localized_search_and_stock_without_false_error(mod, uid, word):
    assert mod.shop_detail_ui_helper(uid, uid, '6aa1a46f38acd40b50800e8c', 'en' if uid == 1001 else 'ar') is False
    assert word in MESSAGES[-1][1]
    actions = [b['callback_data'] for row in buttons(MESSAGES[-1]) for b in row]
    assert actions == ['shop_search', 'shop_available', 'open_shop']
    assert all('btn_' not in b['text'] for row in buttons(MESSAGES[-1]) for b in row)
    mod._error_drain()
    assert mod.db.system_errors.count_documents({}) == 0 and mod.db.orders.count_documents({}) == 0


def test_real_product_database_failure_remains_an_error(mod, monkeypatch):
    monkeypatch.setattr(mod.db.products, 'find_one', Mock(side_effect=RuntimeError('Product storage unavailable')))
    mod.shop_detail_ui_helper(1001, 1001, '6aa1a46f38acd40b50800e8c', 'en')
    mod._error_drain()
    assert mod.db.system_errors.find_one({'level': 'ERROR'})


def test_existing_product_objectid_deep_link_still_opens_real_product(mod):
    p = product(mod)
    assert mod.shop_detail_ui_helper(1001, 1001, str(p['_id']), 'en')
    assert any('Test Product' in text for _, text, _ in MESSAGES)


def test_old_client_masked_script_errors_remain_counted_without_repeated_alerts(mod, server, monkeypatch):
    notices = Mock(); monkeypatch.setattr(mod, 'notify_admins', notices)
    for _ in range(3):
        status, data, _ = request(server, '/webapp/client/error', {'message': 'Script error.', 'location': '0', 'operation': 'runtime'})
        assert status == 200 and data['reference']
    mod._error_drain()
    record = mod.db.system_errors.find_one({})
    assert record['count'] == 3 and record['level'] == 'WARNING' and not notices.called
    assert 'cannot be identified' in record['trace']


@pytest.mark.parametrize('body', [
    {'message': 'Script error.', 'location': '/app/store.js:12', 'masked': True},
    {'message': 'Script error.', 'location': '0', 'stack': 'at store.js:14', 'masked': True},
    {'message': 'TypeError: cart failed', 'location': '0', 'masked': True},
    {'message': 'Failed to load JavaScript resource', 'location': '/app/missing.js', 'operation': 'script-load'}])
def test_real_browser_errors_cannot_be_downgraded_with_masked_flag(mod, server, monkeypatch, body):
    notices = Mock(); monkeypatch.setattr(mod, 'notify_admins', notices)
    status, _, _ = request(server, '/webapp/client/error', body)
    assert status == 200
    mod._error_drain()
    assert mod.db.system_errors.find_one({})['level'] == 'ERROR' and notices.call_count == 1


def test_recovery_before_error_worker_flush_keeps_record_resolved_and_no_stale_alert(mod, monkeypatch):
    notices = Mock(); monkeypatch.setattr(mod, 'notify_admins', notices)
    ref = mod._error_capture('Recovered incident')
    mod._error_resolve_automatic(ref, 'Verified recovery')
    mod._error_drain()
    assert mod.db.system_errors.find_one({'_id': ref})['resolved'] and not notices.called


def test_link_command_exposes_copyable_real_config_and_verified_main_miniapp(mod, monkeypatch):
    monkeypatch.setattr(mod, '_wa_public_url', lambda: 'https://store.example/app/')
    monkeypatch.setattr(mod.bot, 'get_me', lambda: types.SimpleNamespace(username='lara_local_test_bot', has_main_web_app=True))
    monkeypatch.setattr(mod, '_wa_build_catalog_uncached', Mock(side_effect=AssertionError('Link command must not scan products')))
    mod._wa_store_link_cmd(message(mod, text='/link'))
    text = MESSAGES[-1][1]
    assert 'https://store.example/app/' in text and 'https://t.me/lara_local_test_bot?startapp' in text
    rows = buttons(MESSAGES[-1])
    assert rows[0][0]['url'] == 'https://t.me/lara_local_test_bot?startapp'
    copies = [b['copy_text']['text'] for row in rows for b in row if 'copy_text' in b]
    assert copies == ['https://t.me/lara_local_test_bot?startapp', 'https://store.example/app/']


def test_link_command_never_claims_main_app_ready_when_disabled(mod, monkeypatch):
    monkeypatch.setattr(mod, '_wa_public_url', lambda: 'https://store.example/app/')
    monkeypatch.setattr(mod.bot, 'get_me', lambda: types.SimpleNamespace(username='lara_local_test_bot', has_main_web_app=False))
    mod._wa_store_link_cmd(message(mod, text='/link'))
    assert '?startapp' not in MESSAGES[-1][1] and '@BotFather' in MESSAGES[-1][1]
    assert buttons(MESSAGES[-1])[0][0]['web_app']['url'] == 'https://store.example/app/'


def test_link_command_can_read_existing_telegram_menu_when_file_has_no_url(mod, monkeypatch):
    monkeypatch.setattr(mod, '_wa_public_url', lambda: None)
    monkeypatch.setattr(mod.bot, 'get_chat_menu_button', lambda: types.SimpleNamespace(web_app=types.SimpleNamespace(url='https://existing-store.example/app/')))
    monkeypatch.setattr(mod.bot, 'get_me', Mock(side_effect=Timeout('Telegram unavailable')))
    mod._wa_store_link_cmd(message(mod, text='/link'))
    assert 'https://existing-store.example/app/' in MESSAGES[-1][1]
    assert buttons(MESSAGES[-1])[0][0]['web_app']['url'] == 'https://existing-store.example/app/'


def test_link_command_without_configuration_does_not_invent_a_domain_or_bot(mod, monkeypatch):
    monkeypatch.setattr(mod, '_wa_public_url', lambda: None)
    monkeypatch.setattr(mod.bot, 'get_chat_menu_button', Mock(side_effect=Timeout('Telegram unavailable')))
    monkeypatch.setattr(mod.bot, 'get_me', Mock(side_effect=Timeout('Telegram unavailable')))
    mod._wa_store_link_cmd(message(mod, text='/link'))
    assert 'APP_URL' in MESSAGES[-1][1] and 'https://' not in MESSAGES[-1][1]
    assert not buttons(MESSAGES[-1])


def test_link_command_is_admin_only(mod):
    mod._wa_store_link_cmd(message(mod, uid=1001, text='/link'))
    assert not MESSAGES


@pytest.mark.parametrize('url', ['https://[broken', 'http://localhost/app/',
    'https://user:PRIVATE_SECRET@host.example/app/', 'https://127.0.0.1/app/'])
def test_malformed_private_or_local_public_url_does_not_crash_or_leak_credentials(mod, monkeypatch, url):
    monkeypatch.setattr(mod, '_wa_public_url', lambda: url)
    monkeypatch.setattr(mod.bot, 'get_me', lambda: types.SimpleNamespace(username='lara_local_test_bot', has_main_web_app=False))
    mod._wa_store_link_cmd(message(mod, text='/link'))
    assert 'APP_URL' in MESSAGES[-1][1] and 'PRIVATE_SECRET' not in MESSAGES[-1][1]
    assert not buttons(MESSAGES[-1])
