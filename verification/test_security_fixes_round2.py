"""Regression tests for the second round of security fixes (audit of b979036).

Every provider response is an in-process fixture; no network or production service is contacted.
"""
import base64
import time
from unittest.mock import patch

import requests

from test_integration import mod, isolate, server, request, product, _api  # noqa: F401
from harness import MESSAGES

GROUP = -100777


def _call(mod, data, uid, chat_id, chat_type='private'):
    return mod.types.CallbackQuery.de_json({
        'id': 'q-' + data, 'from': {'id': uid, 'is_bot': False, 'first_name': 'U%d' % uid},
        'chat_instance': 'local', 'data': data,
        'message': {'message_id': 50, 'date': 1, 'chat': {'id': chat_id, 'type': chat_type},
                    'from': {'id': 123456789, 'is_bot': True, 'first_name': 'Bot'}, 'text': 'panel'}})


def _msg(mod, text, uid, chat_id=None, chat_type='private'):
    chat_id = uid if chat_id is None else chat_id
    return mod.types.Message.de_json({
        'message_id': 60, 'date': 1, 'chat': {'id': chat_id, 'type': chat_type},
        'from': {'id': uid, 'is_bot': False, 'first_name': 'U%d' % uid}, 'text': text})


def _press(mod, call):
    mod.bot._run_middlewares_and_handler(call, handlers=mod.bot.callback_query_handlers,
                                         middlewares=None, update_type='callback_query')


class _Resp:
    def __init__(self, data, code=200):
        self._d, self.status_code = data, code

    def json(self):
        return self._d


def _protection_off(mod):
    mod.db.settings.update_one({'key': 'amount_protection'}, {'$set': {'value': 'off'}}, upsert=True)
    mod._SETTINGS_CACHE.clear()
    assert mod.is_amount_protection_enabled() is False


# ── admin flows: group chats, banned admins ──

def test_admin_panel_in_group_cannot_be_completed_by_other_member(mod, monkeypatch):
    monkeypatch.setattr(mod.bot, 'threaded', False)
    mod.bot.next_step_backend.handlers.clear()
    product(mod, pid='101', price=5.0, stock=2)
    _press(mod, _call(mod, 'ad_ban_user', 9001, GROUP, 'supergroup'))
    _press(mod, _call(mod, 'ep_price_101', 9001, GROUP, 'supergroup'))
    mod.bot.process_new_messages([_msg(mod, '@customer', 1002, GROUP, 'supergroup')])
    mod.bot.process_new_messages([_msg(mod, '0.01', 1002, GROUP, 'supergroup')])
    assert mod.db.users.find_one({'user_id': 1001})['is_banned'] == 0
    assert mod.db.products.find_one({'id': '101'})['price'] == 5.0


def test_admin_step_rejects_non_admin_sender_even_if_registered(mod, monkeypatch):
    monkeypatch.setattr(mod.bot, 'threaded', False)
    mod.ad_ban_exec(_msg(mod, '@customer', 1002, GROUP, 'supergroup'))
    assert mod.db.users.find_one({'user_id': 1001})['is_banned'] == 0
    mod.ad_ban_exec(_msg(mod, '@customer', 9001))          # real admin, private chat still works
    assert mod.db.users.find_one({'user_id': 1001})['is_banned'] == 1


def test_admin_flow_in_private_chat_still_works(mod, monkeypatch):
    monkeypatch.setattr(mod.bot, 'threaded', False)
    mod.bot.next_step_backend.handlers.clear()
    _press(mod, _call(mod, 'ad_ban_user', 9001, 9001))
    mod.bot.process_new_messages([_msg(mod, '@customer', 9001)])
    assert mod.db.users.find_one({'user_id': 1001})['is_banned'] == 1


def test_banned_admin_loses_admin_power(mod, server):
    mod.db.users.insert_one({'user_id': 1003, 'name': 'Rogue', 'balance': 0., 'lang': 'en', 'is_admin': 1, 'is_banned': 0})
    s, _, _ = request(server, '/webapp/admin/user/ban', {'uid': 1003, 'ban': True}, uid=9001)
    assert s == 200
    mod._invalidate_user_cache(1003)
    s2, _, _ = request(server, '/webapp/admin/user/balance', {'uid': 1003, 'amount': 500, 'note': 'x'}, uid=1003)
    assert s2 == 403
    assert mod.db.users.find_one({'user_id': 1003})['balance'] == 0
    assert mod._is_admin_check(1003) is False
    assert mod._is_admin_check(9001) is True


def test_cgpt_cancel_cannot_target_another_user(mod, monkeypatch):
    monkeypatch.setattr(mod.bot, 'threaded', False)
    mod._cgpt_pending[1002] = {'pid': '777', 'qty': 1, 'total_price': 7.0, 'order_id': 'CGVICTIM', 'minutes': 10080,
                               'p_name_ar': 'x', 'p_name_en': 'x'}
    try:
        bal = mod.db.users.find_one({'user_id': 1002})['balance']
        _press(mod, _call(mod, 'cgpt_cancel_buy_1002', 1001, 1001))
        assert 1002 in mod._cgpt_pending
        assert mod.db.users.find_one({'user_id': 1002})['balance'] == bal
    finally:
        mod._cgpt_pending.pop(1002, None)


# ── manual TON / LTC TxID: one on-chain transfer credits once ──

TON_SHOP = 'EQTESTADDRESS'
TON_RAW = bytes(range(1, 33))
TON_B64 = base64.b64encode(TON_RAW).decode()
TON_HEX = TON_RAW.hex()


def _ton_get(now):
    def get(url, params=None, timeout=None, **kw):
        if 'toncenter.com/api/v2/getTransactions' in url:
            return _Resp({'ok': True, 'result': [{
                'transaction_id': {'hash': TON_B64}, 'utime': now - 60,
                'in_msg': {'source': 'EQSENDER', 'destination': TON_SHOP, 'value': 2_000_000_000}}]})
        return _Resp({}, 404)
    return get


def test_ton_padded_truncated_and_reformatted_hash_credit_once(mod):
    _protection_off(mod)
    start = mod._wa_balance(1001)
    t0 = time.time()
    attempts = [TON_HEX, TON_B64.rstrip('=') + 'AAAAAAAAAAAAAAAAA', TON_HEX[:40], TON_HEX + 'ff', TON_B64,
                TON_B64.replace('+', '-').replace('/', '_')]
    with patch.object(mod.requests, 'get', _ton_get(int(t0))):
        for i, text in enumerate(attempts):
            with patch.object(mod.time, 'time', lambda i=i: t0 + 61 * (i + 1)):
                mod.verify_ton_public_blockchain(_msg(mod, text, 1001), 'en', TON_SHOP)
        with patch.object(mod.time, 'time', lambda: t0 + 61 * 9):
            mod.verify_ton_public_blockchain(_msg(mod, TON_HEX, 1002), 'en', TON_SHOP)
    assert mod._wa_balance(1001) == start + 10.0
    assert mod._wa_balance(1002) == 50.0
    assert mod.db.used_transactions.count_documents({}) == 1


def test_ton_manual_refuses_transfer_already_credited_under_legacy_key(mod):
    _protection_off(mod)
    mod.db.used_transactions.insert_one({'transaction_id': mod.normalize_tx_id(TON_B64), 'user_id': 1002})
    start = mod._wa_balance(1001)
    with patch.object(mod.requests, 'get', _ton_get(int(time.time()))):
        mod.verify_ton_public_blockchain(_msg(mod, TON_HEX, 1001), 'en', TON_SHOP)
    assert mod._wa_balance(1001) == start


LTC_WALLET = 'ltc1TESTADDRESS'
LTC_HASH = 'ab' * 32


def _ltc_get(url, params=None, timeout=None, **kw):
    prep = requests.Request('GET', url, params=params).prepare()
    sent = requests.adapters.HTTPAdapter().request_url(prep, {})
    if prep.url.startswith('https://litecoinspace.org/') and sent == '/api/tx/' + LTC_HASH:
        return _Resp({'txid': LTC_HASH, 'status': {'confirmed': True, 'block_time': int(time.time()) - 120},
                      'vin': [{'prevout': {'scriptpubkey_address': 'ltc1SENDER'}}],
                      'vout': [{'scriptpubkey_address': LTC_WALLET, 'value': 10_000_000}]})
    return _Resp({}, 404)


def test_ltc_fragment_and_suffix_variants_credit_once(mod):
    _protection_off(mod)
    start = mod._wa_balance(1001)
    t0 = time.time()
    with patch.object(mod.requests, 'get', _ltc_get):
        for i, text in enumerate([LTC_HASH, LTC_HASH + '#a', LTC_HASH + '#b', LTC_HASH + '?x=1', LTC_HASH.upper()]):
            with patch.object(mod.time, 'time', lambda i=i: t0 + 61 * (i + 1)):
                mod.verify_ltc_public_blockchain(_msg(mod, text, 1001), 'en', LTC_WALLET)
    assert mod._wa_balance(1001) == start + 10.0
    assert mod.db.used_transactions.count_documents({}) == 1


# ── Binance Pay: one history row credits once across the check button and the auto scan ──

def test_binance_row_with_two_ids_credits_once(mod, monkeypatch):
    uid = 9123456
    monkeypatch.setattr(mod, 'BINANCE_API_KEY', 'FAKE-KEY')
    monkeypatch.setattr(mod, 'BINANCE_API_SECRET', 'FAKE-SECRET')
    mod.db.users.insert_one({'user_id': uid, 'balance': 0., 'lang': 'en', 'is_banned': 0})
    pend = mod.register_pending_deposit(uid, 5.0, 5.0023, 'BINANCE')
    row = {'orderType': 'PAY', 'transactionId': 'P_FIXTURE_ABC123', 'orderId': '900777',
           'amount': '5.0023', 'currency': 'USDT', 'transactionTime': int(time.time() * 1000), 'note': str(uid)}
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: {'data': [row]})
    monkeypatch.setattr(mod, '_binance_pay_request', lambda: [row])
    mod.binance_check_payment(mod._WACall(uid, f'binance_check_{uid}'))
    assert mod.db.users.find_one({'user_id': uid})['balance'] == 5.0
    mod.check_binance_pay_auto()
    mod.check_binance_pay_auto()
    assert mod.db.users.find_one({'user_id': uid})['balance'] == 5.0
    assert pend


def test_manual_binance_id_credits_actual_amount_even_if_request_amount_differs(mod, monkeypatch):
    uid = 9123456
    monkeypatch.setattr(mod, 'BINANCE_API_KEY', 'FAKE-KEY')
    monkeypatch.setattr(mod, 'BINANCE_API_SECRET', 'FAKE-SECRET')
    mod.db.users.insert_one({'user_id': uid, 'balance': 0., 'lang': 'en', 'is_banned': 0})
    pending = mod.register_pending_deposit(uid, 5.0, 5.0023, 'BINANCE')
    row = {'orderType': 'PAY', 'transactionId': 'P_FIXTURE_MISMATCH_123', 'orderId': '900778',
           'amount': '3.00', 'currency': 'USDT', 'transactionTime': int(time.time() * 1000)}
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: {'data': [row]})

    mod.verify_binance_pay(_msg(mod, row['transactionId'], uid), 'en')

    assert mod.db.users.find_one({'user_id': uid})['balance'] == 3.0
    completed = mod.db.pending_deposits.find_one({'pending_id': pending['pending_id']})
    assert completed['status'] == 'completed'
    mod.verify_binance_pay(_msg(mod, row['transactionId'], uid), 'en')
    assert mod.db.users.find_one({'user_id': uid})['balance'] == 3.0


# ── reseller webhooks stop when the key is rotated/disabled or the owner is banned ──

KEY = 'sk_' + 'e' * 48


def _hook(mod, uid=1002):
    key = mod.db.api_keys.insert_one({'user_id': uid, 'api_key': KEY, 'is_active': True}).inserted_id
    mod.db.api_webhooks.insert_one({'api_user_id': uid, 'api_key_id': key, 'url': 'https://hook.example/c',
                                    'secret': 's', 'is_active': True, 'failures': 0})


def _fanout(mod, monkeypatch, evt):
    sent = []
    monkeypatch.setattr(mod, '_deliver_webhook', lambda h, e: sent.append(h['api_user_id']))
    for h in list(mod.db.api_webhooks.find({'is_active': True})):
        if mod._api_event_visible_to_user(evt, h.get('api_user_id')) and mod._webhook_subscriber_active(h):
            mod._deliver_webhook(h, evt)
    return sent


def test_webhook_stops_after_key_regen(mod, server, monkeypatch):
    _hook(mod)
    evt = {'event_id': 'e', 'event_type': 'product.updated', 'timestamp': 1, 'data': {'product_id': '101'}}
    assert _fanout(mod, monkeypatch, evt) == [1002]
    s, _, _ = request(server, '/webapp/api/regen', {}, uid=1002)
    assert s == 200
    assert mod.db.api_webhooks.find_one({'api_user_id': 1002})['is_active'] is False
    assert _fanout(mod, monkeypatch, evt) == []


def test_webhook_stops_after_owner_ban_and_worker_checks_subscriber(mod, server, monkeypatch):
    _hook(mod)
    s, _, _ = request(server, '/webapp/admin/user/ban', {'uid': 1002, 'ban': True}, uid=9001)
    assert s == 200
    assert mod.db.api_webhooks.find_one({'api_user_id': 1002})['is_active'] is False
    # even a still-active row is skipped for a banned owner (e.g. banned by another code path)
    mod.db.api_webhooks.update_one({'api_user_id': 1002}, {'$set': {'is_active': True}})
    assert mod._webhook_subscriber_active(mod.db.api_webhooks.find_one({'api_user_id': 1002})) is False


def test_webhook_worker_source_checks_subscriber(mod):
    import inspect
    src = inspect.getsource(mod._webhook_worker)
    assert '_webhook_subscriber_active(h)' in src


# ── hidden products: events carry no details ──

def test_hidden_product_events_publish_only_unavailable_notice(mod, server):
    key = 'sk_' + 'f' * 48
    mod.db.api_keys.insert_one({'user_id': 1002, 'api_key': key, 'is_active': True})
    mod._api_rate.clear()
    gw = mod._get_api_gateway()
    product(mod, pid='202', price=9.0, stock=1, is_hidden=True)
    since = int(time.time()) - 5
    mod._emit_event('product.updated', {'product_id': '202', 'name_en': 'Secret Launch Name', 'price': 9.0},
                    product_id='202')
    product(mod, pid='101', price=5.0, stock=1)
    mod._emit_event('product.updated', {'product_id': '101', 'name_en': 'Visible', 'price': 5.0}, product_id='101')
    st, data = _api(server, 'GET', f'/{gw}/changes?since={since}', key)
    assert st == 200
    text = repr(data['events'])
    assert 'Secret Launch Name' not in text and 'Visible' in text
    hidden = [e for e in data['events'] if e.get('product_id') == '202']
    assert hidden and hidden[0]['data'] == {'product_id': '202', 'available': False}
