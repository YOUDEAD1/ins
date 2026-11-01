"""Binance recovery fixtures. No real customer IDs, credentials, or provider traffic."""
import datetime
import time
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from test_integration import mod, isolate  # noqa: F401
from test_security_fixes_round2 import _msg
from harness import MESSAGES
from test_products_menu import dispatch


class HistoryClient:
    """Real endpoint constraints: max 100 rows, inclusive time bounds, no offset."""
    def __init__(self, rows, newest_first=False):
        self.rows = sorted(rows, key=lambda row: row['transactionTime'], reverse=newest_first)
        self.calls = []

    def get_pay_trade_history(self, **kwargs):
        self.calls.append(kwargs)
        assert kwargs['limit'] == 100
        assert kwargs['endTime'] - kwargs['startTime'] <= 90 * 86400000
        return {'code': '000000', 'success': True, 'data': [row for row in self.rows
                if kwargs['startTime'] <= row['transactionTime'] <= kwargs['endTime']][:100]}


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


@pytest.mark.parametrize('flow', ['manual', 'auto_note', 'button'])
def test_binance_receipt_displays_provider_order_id_but_retains_credit_identity(mod, monkeypatch, flow):
    uid = 9123456
    mod.db.users.insert_one({'user_id': uid, 'balance': 0., 'lang': 'en', 'is_banned': 0})
    row = payment(transactionId='PA_Fixture_Received_123', orderId='123456789012345678', note=str(uid))
    notices = []
    monkeypatch.setattr(mod, 'notify_admins', notices.append)
    monkeypatch.setattr(mod, 'BINANCE_API_KEY', 'FAKE')
    monkeypatch.setattr(mod, 'BINANCE_API_SECRET', 'FAKE')
    monkeypatch.setattr(mod, '_binance_pay_request', lambda: [row])
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: {'data': [row]})
    if flow == 'auto_note':
        mod.check_binance_pay_auto()
    else:
        mod.register_pending_deposit(uid, 3, 3, 'BINANCE')
        if flow == 'manual':
            mod.verify_binance_pay(_msg(mod, row['orderId'], uid), 'en')
        else:
            mod.binance_check_payment(mod._WACall(uid, f'binance_check_{uid}'))
    receipt = mod.db.used_transactions.find_one({'user_id': uid})
    assert receipt['transaction_id'] == mod.normalize_tx_id(row['transactionId'])
    assert receipt['display_transaction_id'] == row['orderId']
    assert any(f"<code>{row['orderId']}</code>" in message for message in notices)
    assert not any('pafixturereceived123' in message for message in notices)
    result = mod.admin_deep_search(row['orderId'])
    assert result['deposits'][0]['tx'] == row['orderId']
    mod.check_binance_pay_auto()
    assert mod._wa_balance(uid) == 3


def test_missing_order_id_displays_unmodified_provider_transaction_id(mod):
    row = payment(transactionId='PA_Fixture_Original_123')
    assert mod._binance_display_id(row) == row['transactionId']


@pytest.mark.parametrize('newest_first', [False, True])
def test_busy_old_history_does_not_delay_todays_order_id(mod, monkeypatch, newest_first):
    now = int(time.time() * 1000)
    rows = [payment(transactionId=f'OLD_{i:08}', transactionTime=now - (i + 25) * 3600000)
            for i in range(2000)]
    target = payment(transactionTime=now - 60000)
    client = HistoryClient(rows + [target], newest_first)
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: fn(client))
    assert mod._binance_manual_lookup('123456789012345678') == target
    assert len(client.calls) == 1
    assert client.calls[0]['endTime'] - client.calls[0]['startTime'] == 86400000


@pytest.mark.parametrize('newest_first', [False, True])
def test_large_history_resumes_saved_windows_until_exact_order_is_found(mod, monkeypatch, newest_first):
    now = int(time.time() * 1000)
    rows = [payment(transactionId=f'OTHER_{i:08}', transactionTime=now - i * 3600000)
            for i in range(2160)]
    target = payment(transactionTime=now - 45 * 86400000 - 60000)
    client = HistoryClient(rows + [target], newest_first)
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: fn(client))
    pending = mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    expected_next = None
    for _ in range(8):
        before = len(client.calls)
        try:
            result = mod._binance_manual_lookup('123456789012345678', pending=pending)
        except mod.BinanceHistorySearchPending:
            assert len(client.calls) - before <= 16
            if expected_next:
                assert [client.calls[before]['startTime'], client.calls[before]['endTime']] == expected_next
            pending = mod.db.pending_deposits.find_one({'pending_id': pending['pending_id']})
            saved = pending['binance_manual_search']
            assert saved['id_key'] == '123456789012345678'
            expected_next = saved['windows'][-1]
        else:
            assert expected_next is not None, 'fixture must exceed one search batch'
            assert result == target
            break
    else:
        pytest.fail('saved history cursor did not reach the older payment')


def test_last_permitted_request_completes_without_false_incomplete_error(mod, monkeypatch):
    # Four initial windows + six saturated windows' two children = 16 requests.
    calls = []
    def response(fn, **kwargs):
        calls.append(1)
        return {'data': [payment(transactionId='UNRELATED')] * (100 if len(calls) <= 6 else 0)}
    monkeypatch.setattr(mod, 'execute_binance_call', response)
    assert mod._binance_manual_lookup('NO_SUCH_ID_123') is None
    assert len(calls) == 16


def test_progress_is_retained_after_provider_failure_and_id_change_resets_it(mod, monkeypatch):
    pending = mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: None)
    with pytest.raises(ConnectionError):
        mod._binance_manual_lookup('123456789012345678', pending=pending)
    saved = mod.db.pending_deposits.find_one({'pending_id': pending['pending_id']})
    failed_window = saved['binance_manual_search']['windows'][-1]
    client = HistoryClient([])
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: fn(client))
    assert mod._binance_manual_lookup('123456789012345678', pending=saved) is None
    assert [client.calls[0]['startTime'], client.calls[0]['endTime']] == failed_window
    mod._binance_manual_lookup('DIFFERENT_ID_123', pending=saved)
    assert mod.db.pending_deposits.find_one({'pending_id': pending['pending_id']})['binance_manual_search']['id_key'] == 'different_id_123'


def test_bot_history_continuation_button_reuses_id_and_credits_once(mod, monkeypatch):
    pending = mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    row = payment()
    calls = []
    class Client:
        def get_pay_trade_history(self, **kwargs):
            calls.append(kwargs)
            if len(calls) <= 16:
                return {'data': [payment(transactionId=f'UNRELATED_{i}') for i in range(100)]}
            return {'data': [row]}
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: fn(Client()))
    errors = []
    monkeypatch.setattr(mod.logger, 'error', lambda *a, **kw: errors.append(a))
    mod.verify_binance_pay(_msg(mod, '123456789012345678', 1001), 'en')
    saved = mod.db.pending_deposits.find_one({'pending_id': pending['pending_id']})
    assert saved['last_manual_check'] == 'search_pending'
    assert mod._wa_balance(1001) == 100
    assert not errors
    buttons = MESSAGES[-1][2]['reply_markup'].to_dict()['inline_keyboard']
    assert any(b.get('callback_data') == 'binance_manual_resume' for line in buttons for b in line)
    assert mod._wa_deposit_status({'id': 1001}, {'id': pending['pending_id']})[1]['manual_status'] == 'search_pending'
    expected_next = saved['binance_manual_search']['windows'][-1]
    dispatch(mod, 'binance_manual_resume', 1001)
    assert [calls[16]['startTime'], calls[16]['endTime']] == expected_next
    assert mod._wa_balance(1001) == 103
    dispatch(mod, 'binance_manual_resume', 1001)
    assert mod._wa_balance(1001) == 103
    assert mod.db.used_transactions.count_documents({'user_id': 1001}) == 1


def test_resume_cannot_read_another_users_search(mod, monkeypatch):
    mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    monkeypatch.setattr(mod, '_binance_manual_lookup', lambda *a, **kw: pytest.fail('must not query provider'))
    mod.binance_manual_start(mod._WACall(1002, 'binance_manual_resume'))
    assert mod._wa_balance(1002) == 50


def test_website_check_button_resumes_its_owned_search_after_reload(mod, monkeypatch):
    pending = mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    now = int(time.time() * 1000)
    window = [now - 3600000, now]
    mod.db.pending_deposits.update_one({'pending_id': pending['pending_id']}, {'$set': {
        'last_manual_check': 'search_pending', 'binance_manual_search': {
            'version': 1, 'id_key': '123456789012345678', 'supplied': '123456789012345678',
            'started_at': now, 'windows': [window]}}})
    client = HistoryClient([payment(transactionTime=now - 60000)])
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: fn(client))
    class ImmediateThread:
        def __init__(self, target, **kwargs):
            self.target = target
        def start(self):
            self.target()
    monkeypatch.setattr(mod.threading, 'Thread', ImmediateThread)
    # No transaction_id needed after the page has been closed/reopened.
    assert mod._wa_deposit_check({'id': 1001}, {})[0] == 200
    assert [client.calls[0]['startTime'], client.calls[0]['endTime']] == window
    assert mod._wa_balance(1001) == 103


def test_cursor_does_not_restart_when_retry_is_several_minutes_later(mod, monkeypatch):
    pending = mod.register_pending_deposit(1001, 5, 5.0023, 'BINANCE')
    now = int(time.time() * 1000)
    started = now - 5 * 60000
    oldest = [started - 90 * 86400000 + 120000, started - 45 * 86400000]
    state = dict(version=1, id_key='123456789012345678', supplied='123456789012345678',
                 started_at=started, windows=[oldest])
    mod.db.pending_deposits.update_one({'pending_id': pending['pending_id']},
                                      {'$set': {'binance_manual_search': state}})
    pending = mod.db.pending_deposits.find_one({'pending_id': pending['pending_id']})
    client = HistoryClient([])
    monkeypatch.setattr(mod, 'execute_binance_call', lambda fn, **kw: fn(client))
    assert mod._binance_manual_lookup('123456789012345678', pending=pending) is None
    assert [client.calls[0]['startTime'], client.calls[0]['endTime']] == oldest
    assert len(client.calls) == 1
