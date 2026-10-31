"""Binance recovery fixtures. No real customer IDs, credentials, or provider traffic."""
import datetime
import time
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from test_integration import mod, isolate  # noqa: F401
from test_security_fixes_round2 import _msg


def payment(**changes):
    row = dict(orderType='C2C', transactionId='M_P_123456789012345678', amount='3.00',
               currency='USDT', transactionTime=int(time.time() * 1000))
    row.update(changes)
    return row


def test_numeric_id_recovers_prefixed_received_payment_before_request(mod, monkeypatch):
    pending = mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    row = payment(transactionTime=(pending['created_at'] - 3600) * 1000)
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: {'data': [row]})
    mod.verify_binance_pay(_msg(mod, '123456789012345678', 1001), 'en')
    assert mod._wa_balance(1001) == 103
    stored = mod.db.pending_deposits.find_one({'pending_id': pending['pending_id']})
    assert stored['status'] == 'completed' and stored['credited_amount_usd'] == 3
    mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    mod.verify_binance_pay(_msg(mod, 'M_P_123456789012345678', 1001), 'en')
    assert mod._wa_balance(1001) == 103


def test_manual_history_continues_past_full_page(mod, monkeypatch):
    calls = []
    class Client:
        def get_pay_trade_history(self, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return {'data': [payment(transactionId=f'UNRELATED_{i}') for i in range(100)]}
            return {'data': [payment()]}
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: fn(Client()))
    assert mod._binance_manual_lookup('123456789012345678')['amount'] == '3.00'
    assert len(calls) == 2 and calls[1]['startTime'] > calls[0]['startTime']


def test_provider_failure_is_not_reported_as_missing_payment(mod, monkeypatch):
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: {'code': '-2015', 'msg': 'Denied'})
    with pytest.raises(ConnectionError):
        mod._binance_manual_lookup('123456789012345678')


def test_id_comparison_never_accepts_partial_numeric_payload(mod, monkeypatch):
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: {'data': [payment()]})
    assert mod._binance_manual_lookup('12345678901234567') is None


def test_manual_review_keeps_outgoing_and_refund_records_ineligible(mod):
    pending = mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    assert not mod._binance_pending_match(pending, payment(amount='-3'), allow_amount_mismatch=True)
    assert not mod._binance_pending_match(pending, payment(orderType='PAY_REFUND'), allow_amount_mismatch=True)


def test_search_prefers_real_purchase_date_over_document_insertion_time(mod):
    doc = {'date': datetime.datetime(2024, 5, 6, 12), '_id': mod.ObjectId()}
    expected = int(datetime.datetime(2024, 5, 6, 12, tzinfo=datetime.timezone.utc).timestamp())
    assert mod._doc_ts(doc) == expected


def test_website_order_id_check_credits_real_amount_and_returns_it_in_status(mod, monkeypatch):
    pending = mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: {'data': [payment()]})
    class ImmediateThread:
        def __init__(self, target, **kwargs):
            self.target = target
        def start(self):
            self.target()
    monkeypatch.setattr(mod.threading, 'Thread', ImmediateThread)
    code, _ = mod._wa_deposit_check({'id': 1001}, {'transaction_id': '123456789012345678'})
    assert code == 200
    _, result = mod._wa_deposit_status({'id': 1001}, {'id': pending['pending_id']})
    assert result['status'] == 'completed' and result['amount'] == 3
    assert mod._wa_balance(1001) == 103


def test_website_manual_id_is_binance_only_and_rejects_banned_users(mod):
    mod.register_pending_deposit(1001, 5, 5.0023, 'BYBIT')
    assert mod._wa_deposit_check({'id': 1001}, {'transaction_id': '123456789012345678'})[0] == 400
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'is_banned': 1}})
    assert mod._wa_deposit_check({'id': 1001}, {})[0] == 403


def test_concurrent_note_scan_and_manual_order_id_credit_one_payment(mod, monkeypatch):
    uid = 9123456
    mod.db.users.insert_one({'user_id': uid, 'balance': 0., 'lang': 'en', 'is_banned': 0})
    pending = mod.register_pending_deposit(uid, 5, 5.0023, 'BINANCE')
    row = payment(transactionId='PA_FAKE_RECEIVED_123', orderId='123456789012345678', note=str(uid))
    monkeypatch.setattr(mod, 'BINANCE_API_KEY', 'FAKE')
    monkeypatch.setattr(mod, 'BINANCE_API_SECRET', 'FAKE')
    monkeypatch.setattr(mod, '_binance_pay_request', lambda: [row])
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: {'data': [row]})
    original = mod._binance_pending_match
    barrier = threading.Barrier(2)
    def simultaneous(*args, **kwargs):
        result = original(*args, **kwargs)
        barrier.wait(timeout=5)
        return result
    monkeypatch.setattr(mod, '_binance_pending_match', simultaneous)
    with ThreadPoolExecutor(max_workers=2) as pool:
        auto = pool.submit(mod.check_binance_pay_auto)
        manual = pool.submit(mod.verify_binance_pay, _msg(mod, row['orderId'], uid), 'en')
        auto.result(timeout=8)
        manual.result(timeout=8)
    assert mod._wa_balance(uid) == 3
    assert mod.db.used_transactions.count_documents({'user_id': uid}) == 1
    assert mod.db.pending_deposits.find_one({'pending_id': pending['pending_id']})['status'] == 'completed'


def test_note_scan_cannot_credit_while_manual_credit_has_not_written_its_receipt(mod, monkeypatch):
    uid = 9123456
    mod.db.users.insert_one({'user_id': uid, 'balance': 0., 'lang': 'en', 'is_banned': 0})
    mod.register_pending_deposit(uid, 5, 5.0023, 'BINANCE')
    row = payment(transactionId='PA_FAKE_PARALLEL_123', orderId='123456789012345678', note=str(uid))
    monkeypatch.setattr(mod, 'BINANCE_API_KEY', 'FAKE')
    monkeypatch.setattr(mod, 'BINANCE_API_SECRET', 'FAKE')
    monkeypatch.setattr(mod, '_binance_pay_request', lambda: [row])
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: {'data': [row]})
    original = mod.credit_user
    entered, resume = threading.Event(), threading.Event()
    def delayed(uid, amount, tx_id, lang, method, **kwargs):
        if method == 'Binance Pay':
            entered.set()
            assert resume.wait(timeout=5)
        return original(uid, amount, tx_id, lang, method, **kwargs)
    monkeypatch.setattr(mod, 'credit_user', delayed)
    with ThreadPoolExecutor(max_workers=1) as pool:
        manual = pool.submit(mod.verify_binance_pay, _msg(mod, row['orderId'], uid), 'en')
        try:
            assert entered.wait(timeout=5)
            mod.check_binance_pay_auto()
        finally:
            resume.set()
        manual.result(timeout=8)
    assert mod._wa_balance(uid) == 3
    assert mod.db.used_transactions.count_documents({'user_id': uid}) == 1
