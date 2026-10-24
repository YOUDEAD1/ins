"""No live provider calls: paid-row reads, polling wakeups and private diagnostics."""
import json
import time
import types
import xml.etree.ElementTree as ET
from unittest.mock import Mock

import pytest

from harness import MESSAGES
from test_integration import mod, isolate
from test_products_menu import dispatch


@pytest.fixture(autouse=True)
def credentials(mod, monkeypatch):
    monkeypatch.setattr(mod, 'BINANCE_API_KEY', 'FAKE-KEY')
    monkeypatch.setattr(mod, 'BINANCE_API_SECRET', 'FAKE-SECRET')
    monkeypatch.setattr(mod, 'bybit_is_configured', lambda: True)
    monkeypatch.setattr(mod, '_NET_METER', mod._NetworkMeter())


def binance_row(tx='900001', **extra):
    return {'orderId': tx, 'amount': '5', 'currency': 'USDT',
            'transactionTime': int(time.time() * 1000), **extra}


def test_one_hundred_old_binance_transactions_use_one_projected_read_not_one_hundred_full_reads(mod, monkeypatch):
    rows = [binance_row(str(900000 + i)) for i in range(100)]
    mod.db.used_transactions.insert_many([{'transaction_id': row['orderId'], 'large_receipt': 'x' * 10000} for row in rows])
    monkeypatch.setattr(mod, '_binance_pay_request', lambda: rows)
    one = Mock(wraps=mod.db.used_transactions.find_one)
    many = Mock(wraps=mod.db.used_transactions.find)
    monkeypatch.setattr(mod.db.used_transactions, 'find_one', one)
    monkeypatch.setattr(mod.db.used_transactions, 'find', many)
    service = mod._TRAFFIC_EFFICIENCY
    monkeypatch.setattr(mod, '_TRAFFIC_EFFICIENCY', None)
    mod.check_binance_pay_auto()
    assert one.call_count == 100
    one.reset_mock(); many.reset_mock()
    monkeypatch.setattr(mod, '_TRAFFIC_EFFICIENCY', service)
    mod.check_binance_pay_auto()
    assert one.call_count == 0 and many.call_count == 1
    assert many.call_args.args[1] == {'transaction_id': 1, '_id': 0}
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_a_new_binance_note_transaction_still_credits_once_after_the_bulk_read(mod, monkeypatch):
    uid = 9123456
    mod.db.users.insert_one({'user_id': uid, 'balance': 0., 'lang': 'en', 'is_banned': 0})
    monkeypatch.setattr(mod, '_binance_pay_request', lambda: [binance_row(note=str(uid))])
    mod.check_binance_pay_auto(); mod.check_binance_pay_auto()
    assert mod.db.users.find_one({'user_id': uid})['balance'] == 5
    assert mod.db.used_transactions.count_documents({'transaction_id': '900001'}) == 1


def test_unmatched_binance_rows_are_not_negatively_cached_and_match_a_later_pending_deposit(mod, monkeypatch):
    monkeypatch.setattr(mod, '_binance_pay_request', lambda: [binance_row()])
    mod.check_binance_pay_auto()
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    assert mod.register_pending_deposit(1001, 5, 5, 'BINANCE')
    mod.check_binance_pay_auto(); mod.check_binance_pay_auto()
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 105
    assert mod.db.used_transactions.count_documents({'transaction_id': '900001'}) == 1


def test_bulk_read_failure_falls_back_to_the_original_per_transaction_check(mod, monkeypatch):
    mod.db.used_transactions.insert_one({'transaction_id': '900001'})
    monkeypatch.setattr(mod, '_binance_pay_request', lambda: [binance_row()])
    original = mod.db.used_transactions.find
    def unavailable(*args, **kwargs):
        if len(args) > 1 and args[1] == {'transaction_id': 1, '_id': 0}:
            raise OSError('Simulated bulk read outage')
        return original(*args, **kwargs)
    monkeypatch.setattr(mod.db.used_transactions, 'find', unavailable)
    one = Mock(wraps=mod.db.used_transactions.find_one)
    monkeypatch.setattr(mod.db.used_transactions, 'find_one', one)
    mod.check_binance_pay_auto()
    assert one.call_count == 1
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_paid_row_state_is_scoped_to_one_provider_response_and_not_kept_as_an_unbounded_cache(mod):
    rows = [binance_row()]
    assert not mod._TRAFFIC_EFFICIENCY.known_used(rows, 'binance')
    mod.db.used_transactions.insert_one({'transaction_id': '900001'})
    assert mod._TRAFFIC_EFFICIENCY.known_used(rows, 'binance') == frozenset({'900001'})
    mod.db.used_transactions.delete_one({'transaction_id': '900001'})
    assert not mod._TRAFFIC_EFFICIENCY.known_used(rows, 'binance')


def test_binance_transaction_id_fallback_and_normalization_are_unchanged(mod):
    rows = [{'orderId': 'not-numeric', 'transactionId': ' 0xABCDEF '},
            {'bizOrderNo': 'BIZ-OLD'}, {}, 'malformed']
    ids = [mod.normalize_tx_id('0xABCDEF'), mod.normalize_tx_id('BIZ-OLD')]
    mod.db.used_transactions.insert_many([{'transaction_id': value} for value in ids])
    assert mod._TRAFFIC_EFFICIENCY.known_used(rows, 'binance') == frozenset(ids)


def test_streaming_provider_input_is_left_for_the_original_matcher(mod):
    rows = iter([binance_row()])
    assert not mod._TRAFFIC_EFFICIENCY.known_used(rows, 'binance')
    assert next(rows)['orderId'] == '900001'


def pending(mod, coin, uid=1001):
    return mod.register_pending_deposit(uid, 5, 5.001, coin, sender_uid='555')


@pytest.mark.parametrize('coin,kind', [('BYBIT_UID', 'internal'), ('BYBIT_TRC20', 'onchain'),
    ('BYBIT_BEP20', 'onchain'), ('USDT', 'onchain'), ('USDT_BEP20', 'onchain')])
def test_bybit_only_fetches_a_history_kind_that_can_match_current_pending_coins(mod, monkeypatch, coin, kind):
    assert pending(mod, coin)
    get = Mock(return_value=[])
    monkeypatch.setattr(mod, 'get_bybit_deposits', get)
    mod.check_bybit_auto()
    assert get.call_count == 1 and get.call_args.kwargs['kind'] == kind
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_bybit_fetches_both_kinds_if_both_payment_methods_are_pending(mod, monkeypatch):
    assert pending(mod, 'BYBIT_UID') and pending(mod, 'USDT')
    get = Mock(return_value=[])
    monkeypatch.setattr(mod, 'get_bybit_deposits', get)
    mod.check_bybit_auto()
    assert [call.kwargs['kind'] for call in get.call_args_list] == ['internal', 'onchain']


def test_bybit_kind_read_failure_keeps_both_recovery_paths_enabled(mod, monkeypatch):
    assert pending(mod, 'BYBIT_UID')
    monkeypatch.setattr(mod.db.pending_deposits, 'distinct', Mock(side_effect=OSError('Read unavailable')))
    get = Mock(return_value=[])
    monkeypatch.setattr(mod, 'get_bybit_deposits', get)
    mod.check_bybit_auto()
    assert get.call_count == 2


def test_bybit_no_pending_deposit_does_not_query_provider(mod, monkeypatch):
    get = Mock(return_value=[])
    monkeypatch.setattr(mod, 'get_bybit_deposits', get)
    mod.check_bybit_auto()
    get.assert_not_called()


def test_bybit_used_transactions_are_batched_before_individual_history_rows(mod, monkeypatch):
    pending(mod, 'USDT')
    rows = [{'txID': str(100000 + i), 'status': 3, 'coin': 'USDT', 'amount': '5.001',
             'successAt': str(int(time.time() * 1000)), 'chain': 'TRX'} for i in range(100)]
    assert all(mod._bybit_row_is_success(row, 'onchain') for row in rows)
    mod.db.used_transactions.insert_many([{'transaction_id': mod.normalize_tx_id(row['txID'])} for row in rows])
    monkeypatch.setattr(mod, 'get_bybit_deposits', Mock(return_value=rows))
    one = Mock(wraps=mod.db.used_transactions.find_one)
    monkeypatch.setattr(mod.db.used_transactions, 'find_one', one)
    mod.check_bybit_auto()
    assert one.call_count == 0


@pytest.mark.parametrize('elapsed,woke,extra_sleep', [(0, True, 12), (4, True, 8),
                                                    (30, True, 0), (60, False, 0)])
def test_payment_wakeup_cannot_shorten_active_interval_but_wakes_an_idle_worker(mod, monkeypatch, elapsed, woke, extra_sleep):
    clock = [1000.]
    class Event:
        def wait(self, timeout):
            assert timeout == 60
            clock[0] += elapsed
            return woke
        def clear(self):
            pass
    monkeypatch.setitem(mod._NET_PAY_EVENTS, 'check_binance_pay_auto', Event())
    monkeypatch.setattr(mod, '_net_payment_interval', lambda interval: 60)
    monkeypatch.setattr(mod.time, 'monotonic', lambda: clock[0])
    sleep = Mock()
    monkeypatch.setattr(mod.time, 'sleep', sleep)
    mod._net_wait(mod.check_binance_pay_auto, 12)
    if extra_sleep:
        sleep.assert_called_once_with(extra_sleep)
    else:
        sleep.assert_not_called()


def test_unrelated_background_tasks_keep_their_existing_schedule(mod, monkeypatch):
    sleep = Mock()
    monkeypatch.setattr(mod.time, 'sleep', sleep)
    def unrelated():
        pass
    mod._net_wait(unrelated, 300)
    sleep.assert_called_once_with(300)


def test_telegram_sources_are_not_lost_when_the_old_global_detail_table_is_full(mod):
    meter = mod._NET_METER
    for i in range(500):
        meter.record('mongo', 'find', sent=100, detail='users.find @ source_' + str(i))
    for i in range(700):
        meter.record('http_client', 'api.telegram.org/sendMessage', sent=200, received=40,
            errors=int(i < 5), detail='broadcast_price_drop <- admin_save_edit')
    snapshot = meter.snapshot()
    row = next(row for row in snapshot['hotspots'] if row['label'].endswith('/sendMessage'))
    assert row['requests'] == 700 and row['errors'] == 5 and row['sent'] == 140000
    assert row['origin'] == 'broadcast_price_drop <- admin_save_edit'
    assert sum(row['requests'] for row in snapshot['rows']) == 1200


def test_late_frequent_sources_replace_early_rare_sources_with_explicit_observation_bounds(mod):
    meter = mod._NET_METER
    for i in range(1000):
        meter.record('mongo', 'find', sent=1, detail='users.find @ early_' + str(i))
    for _ in range(5000):
        meter.record('mongo', 'find', sent=20, detail='used_transactions.find @ check_binance_pay_auto')
    snapshot = meter.snapshot()
    row = next(row for row in snapshot['hotspots'] if 'check_binance_pay_auto' in row['origin'])
    assert row['requests'] == 5000 and row['admission_error_bound'] > 0
    assert len(snapshot['hotspots']) <= 24 and len(snapshot['details']) <= 193
    assert sum(row['requests'] for row in snapshot['rows']) == 6000


def test_diagnostic_buckets_have_a_hard_memory_bound_and_keep_main_totals(mod):
    for i in range(3000):
        mod._NET_METER.record('http_client', 'fixture-' + str(i), sent=5, detail='source_' + str(i))
    snapshot = mod._NET_METER.snapshot()
    assert len(snapshot['hotspots']) <= 100 * 24
    assert sum(row['requests'] for row in snapshot['rows']) == 3000
    assert sum(row['sent'] for row in snapshot['rows']) == 15000


def test_new_report_remains_private_shows_failure_counts_and_preserves_export_compatibility(mod, monkeypatch):
    for i in range(5):
        mod._NET_METER.record('http_client', 'api.telegram.org/sendMessage', sent=123,
            errors=1, detail='notify_admins <- _retry_api_purchase_notices')
    mod._NET_METER.record('http_server', '/api/products', sent=100,
        detail='api_products:customer_1001:200')
    dispatch(mod, 'ad_network', 1001)
    assert not MESSAGES
    dispatch(mod, 'ad_network', 9001)
    text = MESSAGES[-1][1]
    assert 'فشل 5' in text and 'notify_admins' in text and 'استقبال' in text
    assert 'سحب المنتجات حسب حساب API' in text and 'customer_1001' in text and 'غير مقاس' in text
    assert len(text) <= 3900
    ET.fromstring('<root>' + text + '</root>')
    files = []
    monkeypatch.setattr(mod.bot, 'send_document', lambda cid, data, **kwargs: files.append(json.loads(data.getvalue())))
    dispatch(mod, 'ad_network_export', 9001)
    assert files[0]['revision'] == 40 and files[0]['traffic_efficiency_version'] == 1
    assert files[0]['hotspots'][0]['errors'] == 5


def test_visible_report_stays_within_telegram_limit_without_breaking_html(mod):
    for group in ('mongo', 'http_client', 'curl_client', 'http_server'):
        for i in range(30):
            mod._NET_METER.record(group, 'x' * 90 + str(i), sent=100000,
                detail='source_' + 'x' * 170 + str(i))
    call = types.SimpleNamespace(id='', message=types.SimpleNamespace(chat=types.SimpleNamespace(id=9001)))
    mod._TRAFFIC_EFFICIENCY.show_network(call)
    text = MESSAGES[-1][1]
    assert len(text) <= 3900
    ET.fromstring('<root>' + text + '</root>')
