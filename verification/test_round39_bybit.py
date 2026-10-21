"""403 outage, cooldown, diagnostics and payment safety with mocked transport only."""
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from harness import MESSAGES
from test_integration import mod, isolate
from test_products_menu import callback
from test_round37_bybit import bybit_state, response


@pytest.fixture(autouse=True)
def state(mod, bybit_state, monkeypatch):
    mod._BYBIT_CHECK_COOLDOWN.clear()
    while not mod._ERROR_QUEUE.empty():
        mod._ERROR_QUEUE.get_nowait()
        mod._ERROR_QUEUE.task_done()
    mod._ERROR_MEMORY.clear()
    notices = Mock()
    monkeypatch.setattr(mod, 'notify_admins', notices)
    return notices


def pending(mod, uid=1001):
    return mod.db.pending_deposits.insert_one({'pending_id': 'FIXTURE-PENDING', 'user_id': uid,
        'coin': 'BYBIT_UID', 'status': 'pending', 'expires_at': int(time.time()) + 1800,
        'base_amount_usd': 5, 'unique_amount_usd': 5.0012}).inserted_id


def test_repeated_403_is_one_recoverable_warning_and_backoff_is_bounded(mod, state, monkeypatch):
    clock = [time.time()]
    monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    get = Mock(return_value=response({'retCode': 0, 'result': {'rows': []}}, status=403))
    monkeypatch.setattr(mod.requests, 'get', get)
    for n, delay in enumerate([600, 1200, 2400, 3600, 3600], 1):
        assert mod._bybit_signed_request('/v5/test') is None
        assert get.call_count == n
        assert mod._BYBIT_HEALTH['retry_at'] - clock[0] == delay
        for i in range(25):
            assert mod._bybit_signed_request('/v5/test') is None
        assert get.call_count == n
        clock[0] += delay
    mod._error_drain()
    records = list(mod.db.system_errors.find())
    assert len(records) == 1 and records[0]['level'] == 'WARNING' and records[0]['count'] == 1
    state.assert_not_called()
    mod.refresh_proxies.assert_not_called()
    assert mod._BYBIT_INVALID_KEY_COUNT == 0
    assert not mod.db.settings.find_one({'key': 'bybit_disabled'})


def test_recovery_clears_outage_and_a_later_outage_gets_a_new_first_warning(mod, monkeypatch):
    clock = [time.time()]
    monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    get = Mock(return_value=response({}, status=403))
    monkeypatch.setattr(mod.requests, 'get', get)
    mod._bybit_signed_request('/v5/test')
    assert mod._BYBIT_HEALTH['since'] == clock[0]
    clock[0] += 600
    get.return_value = response({'retCode': 0, 'result': {'rows': []}})
    assert mod._bybit_signed_request('/v5/test') == {'rows': []}
    health = mod._BYBIT_HEALTH
    assert not health['reason'] and health['failures'] == 0 and health['reported_at'] == 0
    assert health['last_success'] == clock[0] and health['since'] == 0
    get.return_value = response({}, status=403)
    mod._bybit_signed_request('/v5/test')
    assert health['failures'] == 1 and health['retry_at'] - clock[0] == 600
    assert len([r for r in mod._ERROR_MEMORY if '[BYBIT]' in r['message']]) == 2


def test_changing_credentials_clears_backoff_count_and_cached_success(mod, monkeypatch):
    get = Mock(return_value=response({}, status=403))
    monkeypatch.setattr(mod.requests, 'get', get)
    mod._bybit_signed_request('/v5/test')
    monkeypatch.setattr(mod, '_bybit_creds', lambda: ('new-key', 'new-secret', '123', 'https://api.bybit.com'))
    mod._bybit_signed_request('/v5/test')
    assert get.call_count == 2 and mod._BYBIT_HEALTH['failures'] == 1
    assert mod._BYBIT_HEALTH['last_success'] == 0


def test_transport_outage_keeps_pending_deposits_balances_orders_and_transaction_ledger_unchanged(mod, monkeypatch):
    pending(mod)
    names = ['users', 'pending_deposits', 'used_transactions', 'balance_logs', 'orders']
    before = {name: list(mod.db[name].find()) for name in names}
    get = Mock(return_value=response({}, status=403))
    monkeypatch.setattr(mod.requests, 'get', get)
    for i in range(10):
        mod.check_bybit_auto()
    assert get.call_count == 1
    assert {name: list(mod.db[name].find()) for name in names} == before


@pytest.mark.parametrize('uid,expected', [(1001, 'temporarily unavailable'), (1002, 'تعذّر التحقق')])
def test_manual_check_explains_unavailable_verification_without_claiming_transfer_missing(mod, monkeypatch, uid, expected):
    pending(mod, uid)
    get = Mock(return_value=response({}, status=403))
    monkeypatch.setattr(mod.requests, 'get', get)
    before = mod.db.users.find_one({'user_id': uid})['balance']
    mod.bybit_check_payment(callback(mod, f'bybit_check_{uid}', uid))
    text = MESSAGES[-1][1]
    assert expected in text
    assert "hasn't arrived" not in text and 'لم نستلم' not in text
    assert ('do not pay again' if uid == 1001 else 'لا تُعد التحويل') in text
    assert get.call_count == 1 and mod.db.users.find_one({'user_id': uid})['balance'] == before


def test_bybit_status_shows_403_and_retry_without_network_secrets_or_writes(mod, monkeypatch):
    for key, value in [('bybit_api_key', 'SECRET-FIXTURE-KEY'), ('bybit_api_secret', 'SECRET-FIXTURE-SECRET'), ('bybit_uid', '123')]:
        mod.db.settings.insert_one({'key': key, 'value': value})
    monkeypatch.setattr(mod.requests, 'get', Mock(return_value=response({}, status=403)))
    mod._bybit_signed_request('/v5/test')
    network = Mock(side_effect=AssertionError('Diagnostics must not make HTTP calls'))
    monkeypatch.setattr(mod.requests, 'get', network)
    before = list(mod.db.settings.find())
    mod.bybit_status_command(NS(from_user=NS(id=9001), chat=NS(id=9001)))
    text = MESSAGES[-1][1]
    assert 'HTTP: 403' in text and 'إعادة المحاولة' in text and 'قيود IP' in text
    assert 'SECRET-FIXTURE' not in text and 'fixture-key' not in text and 'fixture-secret' not in text
    assert list(mod.db.settings.find()) == before
    network.assert_not_called()


def test_bybit_status_remains_admin_only(mod):
    mod.bybit_status_command(NS(from_user=NS(id=1001), chat=NS(id=1001)))
    assert not MESSAGES


def test_healthy_or_never_checked_status_does_not_claim_key_is_verified(mod, monkeypatch):
    assert 'لم يُسجّل فحص' in '\n'.join(mod._bybit_health_lines())
    monkeypatch.setattr(mod.requests, 'get', Mock(return_value=response({'retCode': 0, 'result': {}})))
    mod._bybit_signed_request('/v5/test')
    assert 'آخر اتصال' in '\n'.join(mod._bybit_health_lines())


def test_unexpected_api_error_keeps_error_monitor_alerting_once_per_outage(mod, monkeypatch, state):
    monkeypatch.setattr(mod.requests, 'get', Mock(return_value=response({'retMsg': 'SECRET-FIXTURE-BODY'})))
    for i in range(3):
        mod._BYBIT_HEALTH['retry_at'] = 0
        assert mod._bybit_signed_request('/v5/test') is None
    mod._error_drain()
    assert state.call_count == 1
    assert 'invalid_api_envelope' in state.call_args.args[0]
    assert 'SECRET-FIXTURE-BODY' not in state.call_args.args[0]


def test_confirmed_invalid_api_key_still_disables_after_five_real_failures(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, 'get', Mock(return_value=response({'retCode': 10003, 'retMsg': 'fixture'})))
    for i in range(5):
        mod._BYBIT_HEALTH['retry_at'] = 0
        mod._bybit_signed_request('/v5/test')
    assert mod._BYBIT_INVALID_KEY_COUNT == 5
    assert mod.db.settings.find_one({'key': 'bybit_disabled'})['value'] == '1'
