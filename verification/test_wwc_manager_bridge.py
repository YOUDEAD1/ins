"""Dedicated manager: actual signed HTTP, original atomic wallet and private transport."""
import datetime as dt
import http.client
import json
import threading
from types import SimpleNamespace as NS
import pytest
from test_wwc_platform import mod, isolate, platform, SHOP
from test_integration import server
from harness import MESSAGES
from wwc_platform import signed_headers

SECRET = 'offline-manager-shared-secret-longer-than-32'

def call(server, p, action, data, *, headers=None, body=None):
    p.client.secret = SECRET
    path = '/wwc/manager/v1/' + action
    raw = body if body is not None else json.dumps(data, separators=(',', ':')).encode()
    headers = headers or signed_headers(SECRET, path, raw)
    conn = http.client.HTTPConnection(*server, timeout=5)
    conn.request('POST', path, body=raw, headers=headers)
    response = conn.getresponse()
    result = response.status, json.loads(response.read())
    conn.close()
    return result

def test_signed_account_read_is_private_and_never_changes_wallet(mod, platform, server):
    p, _, _, _ = platform
    status, body = call(server, p, 'account', {'actor_id': 1001})
    assert status == 200 and body['balance'] == '100.00'
    assert set(body) == {'ok', 'balance', 'currency', 'language'}
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    assert not MESSAGES

def test_signature_body_path_time_and_replay_are_enforced(mod, platform, server):
    p, _, _, _ = platform
    raw = b'{"actor_id":1001}'
    path = '/wwc/manager/v1/account'
    headers = signed_headers(SECRET, path, raw)
    assert call(server, p, 'account', {}, headers=headers, body=raw)[0] == 200
    assert call(server, p, 'account', {}, headers=headers, body=raw)[0] == 409
    assert call(server, p, 'account', {}, headers=headers, body=b'{"actor_id":1002}')[0] == 401
    assert call(server, p, 'source', {}, headers=headers, body=raw)[0] == 401
    old = signed_headers(SECRET, path, raw, timestamp=1000000000)
    assert call(server, p, 'account', {}, headers=old, body=raw)[0] == 401
    assert mod.db.wwc_manager_nonces.count_documents({}) == 1

@pytest.mark.parametrize('actor', [True, '1001', -1, 0, 2**54, None])
def test_signed_invalid_actor_cannot_select_an_account(mod, platform, server, actor):
    p, _, _, _ = platform
    assert call(server, p, 'account', {'actor_id': actor})[0] == 409

def test_source_requires_real_nss_account_and_uses_its_own_api_key(mod, platform, server):
    p, _, _, _ = platform
    assert call(server, p, 'source', {'actor_id': 7777})[1]['error'] == 'NSS_ACCOUNT_REQUIRED'
    a = call(server, p, 'source', {'actor_id': 1001})[1]
    b = call(server, p, 'source', {'actor_id': 1002})[1]
    assert a['source_key'] != b['source_key']
    assert a['source_url'].startswith('https://nss.example/')
    assert mod.db.api_keys.find_one({'api_key': a['source_key']})['user_id'] == 1001

def test_wallet_quote_confirm_replay_use_original_ledger_without_nss_messages(mod, platform, server):
    p, _, receipts, _ = platform
    quote = call(server, p, 'wallet_quote', {'actor_id':1001, 'shop_id':SHOP, 'amount':'0.01'})[1]['checkout']
    assert quote['amount'] == '5.00' and quote['state'] == 'wallet_quote'
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100
    assert not MESSAGES
    for _ in range(3):
        status, body = call(server, p, 'wallet_confirm', {'actor_id':1001, 'token':quote['token']})
        assert status == 200 and body['checkout']['state'] == 'applied'
    assert len(receipts) == 1
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 95
    assert mod.db.balance_logs.count_documents({'type':'wwc_subscription'}) == 1
    assert not MESSAGES
    assert call(server, p, 'wallet_status', {'actor_id':1002, 'token':quote['token']})[1]['error'] == 'CHECKOUT_NOT_FOUND'

def test_capture_is_thread_local_and_restores_after_exception(mod, platform):
    p, _, _, _ = platform
    with pytest.raises(RuntimeError):
        with p.manager_bridge.capture() as messages:
            p.send(1001, 'manager only')
            thread = threading.Thread(target=lambda: p.send(1002, 'ordinary bot'))
            thread.start(); thread.join()
            assert messages == [(1001, 'manager only', None)]
            raise RuntimeError('fixture')
    p.send(1001, 'restored')
    assert [text for _, text, _ in MESSAGES] == ['ordinary bot', 'restored']

def test_cancel_expiry_and_insufficient_funds_never_debit(mod, platform, server):
    p, _, _, _ = platform
    q = call(server, p, 'wallet_quote', {'actor_id':1001, 'shop_id':SHOP})[1]['checkout']
    assert call(server, p, 'wallet_cancel', {'actor_id':1001, 'token':q['token']})[1]['checkout']['state'] == 'cancelled'
    assert call(server, p, 'wallet_confirm', {'actor_id':1001, 'token':q['token']})[1]['checkout']['state'] == 'cancelled'
    q = call(server, p, 'wallet_quote', {'actor_id':1001, 'shop_id':SHOP})[1]['checkout']
    mod.db.users.update_one({'user_id':1001}, {'$set':{'balance':1}})
    call(server, p, 'wallet_confirm', {'actor_id':1001, 'token':q['token']})
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 1
    assert not mod.db.balance_logs.count_documents({'type':'wwc_subscription'})

def test_bridge_ban_and_arbitrary_operations_are_rejected(mod, platform, server):
    p, _, _, _ = platform
    assert call(server, p, 'set_balance', {'actor_id':1001, 'balance':200})[0] == 404
    mod.db.users.update_one({'user_id':1001}, {'$set':{'is_banned':1}})
    mod._USER_CACHE.clear()
    assert call(server, p, 'account', {'actor_id':1001})[1]['error'] == 'ACCOUNT_RESTRICTED'

def test_nss_menu_redirect_reuses_native_button_customization(mod, platform, monkeypatch):
    p, _, _, _ = platform
    monkeypatch.setattr(p, 'manager_username', 'fixture_manager_bot')
    markup = mod.types.InlineKeyboardMarkup()
    markup.row(mod.types.InlineKeyboardButton('Products', callback_data='open_shop'))
    markup.row(mod.types.InlineKeyboardButton('Settings', callback_data='open_settings'))
    mod.bot.send_message(1001, 'home', reply_markup=markup)
    buttons = [b for row in markup.keyboard for b in row]
    assert any(getattr(b, 'url', '') == 'https://t.me/fixture_manager_bot?start=open' for b in buttons)
    assert not any(getattr(b, 'callback_data', '') in ('wwc:create', 'wwc:list') for b in buttons)
