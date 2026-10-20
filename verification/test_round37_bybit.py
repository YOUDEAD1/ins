"""Bybit response/transport fixtures only; no real keys, HTTP or payments."""
import hashlib
import hmac
import time
from unittest.mock import Mock

import pytest
import requests
from test_integration import mod, isolate


@pytest.fixture(autouse=True)
def bybit_state(mod, isolate, monkeypatch):
    mod._BYBIT_HEALTH.update(credentials=None, retry_at=0, reason='', http=None, ret_code=None, reported_at=0)
    monkeypatch.setattr(mod, '_BYBIT_INVALID_KEY_COUNT', 0)
    monkeypatch.setattr(mod, '_bybit_creds', lambda: ('fixture-key', 'fixture-secret', '123', 'https://api.bybit.com'))
    monkeypatch.setattr(mod, 'VERIFIED_PROXIES', [])
    monkeypatch.setattr(mod, 'LAST_GOOD_PROXY', None)
    monkeypatch.setattr(mod, 'refresh_proxies', Mock())
    monkeypatch.setattr(mod, '_remove_dead_proxy', Mock())


def response(body=None, status=200, error=None):
    res = Mock(status_code=status)
    if error: res.json.side_effect = error
    else: res.json.return_value = body
    return res


@pytest.mark.parametrize('body', [{}, {'message': 'gateway'}, [], None, {'retCode': '0'}, {'retCode': False},
                                 {'retCode': 0, 'result': None}, {'retCode': 0, 'result': []}])
def test_malformed_envelopes_never_disable_keys_or_become_deposit_success_and_are_throttled(mod, monkeypatch, body):
    get = Mock(return_value=response(body)); monkeypatch.setattr(mod.requests, 'get', get)
    assert mod._bybit_signed_request('/v5/test') is None
    assert mod._bybit_signed_request('/v5/test') is None
    assert get.call_count == 1 and mod._BYBIT_INVALID_KEY_COUNT == 0
    assert mod._BYBIT_HEALTH['reason'] in ('invalid_api_envelope', 'invalid_result')
    assert mod._BYBIT_HEALTH['retry_at'] > time.time()
    assert not mod.db.settings.find_one({'key': 'bybit_disabled'})
    mod.refresh_proxies.assert_not_called()


@pytest.mark.parametrize('status,minimum', [(403, 590), (429, 50), (502, 50)])
def test_http_rejections_record_status_without_proxy_storms(mod, monkeypatch, status, minimum):
    get = Mock(return_value=response({'retCode': 0, 'result': {'rows': []}}, status=status))
    monkeypatch.setattr(mod.requests, 'get', get)
    assert mod._bybit_signed_request('/v5/test') is None
    assert mod._BYBIT_HEALTH['http'] == status and mod._BYBIT_HEALTH['reason'] == 'http_rejected'
    assert mod._BYBIT_HEALTH['retry_at'] - time.time() >= minimum
    mod.refresh_proxies.assert_not_called()


def test_html_or_non_json_response_is_a_transport_response_problem_not_an_invalid_key(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, 'get', Mock(return_value=response(error=ValueError('fixture HTML'))))
    assert mod._bybit_signed_request('/v5/test') is None
    assert mod._BYBIT_HEALTH['reason'] == 'non_json_response' and mod._BYBIT_INVALID_KEY_COUNT == 0


def test_real_auth_failure_is_counted_but_success_after_retry_resets_health(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, 'get', Mock(return_value=response({'retCode': 10003, 'retMsg': 'fixture'})))
    assert mod._bybit_signed_request('/v5/test') is None
    assert mod._BYBIT_INVALID_KEY_COUNT == 1 and mod._BYBIT_HEALTH['ret_code'] == 10003
    mod._BYBIT_HEALTH['retry_at'] = 0
    monkeypatch.setattr(mod.requests, 'get', Mock(return_value=response({'retCode': 0, 'result': {}})))
    assert mod._bybit_signed_request('/v5/test') == {}
    assert mod._BYBIT_INVALID_KEY_COUNT == 0 and not mod._BYBIT_HEALTH['reason']


def test_fixing_credentials_retries_immediately_without_waiting_for_old_cooldown(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, 'get', Mock(return_value=response({})))
    mod._bybit_signed_request('/v5/test')
    monkeypatch.setattr(mod, '_bybit_creds', lambda: ('new-fixture-key', 'new-fixture-secret', '123', 'https://api.bybit.com'))
    get = Mock(return_value=response({'retCode': 0, 'result': {'rows': []}}))
    monkeypatch.setattr(mod.requests, 'get', get)
    assert mod._bybit_signed_request('/v5/test') == {'rows': []}
    assert get.call_count == 1


def test_connection_failure_keeps_bounded_proxy_fallback_and_re_signs_it(mod, monkeypatch):
    clock = [time.time()]
    monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    monkeypatch.setattr(mod, 'VERIFIED_PROXIES', ['http://fixture-one', 'http://fixture-two', 'http://fixture-three'])
    attempts = []
    def get(url, **kwargs):
        attempts.append((url, {**kwargs, 'headers': dict(kwargs['headers'])}))
        if len(attempts) == 1:
            clock[0] += 10
            raise requests.Timeout('fixture timeout')
        return response({'retCode': 0, 'result': {'rows': []}})
    monkeypatch.setattr(mod.requests, 'get', get)
    assert mod._bybit_signed_request('/v5/test', {'coin': 'USDT'}) == {'rows': []}
    assert len(attempts) == 2
    assert attempts[0][1]['headers']['X-BAPI-TIMESTAMP'] != attempts[1][1]['headers']['X-BAPI-TIMESTAMP']
    headers = attempts[1][1]['headers']
    expected = hmac.new(b'fixture-secret', (headers['X-BAPI-TIMESTAMP'] + 'fixture-key20000coin=USDT').encode(), hashlib.sha256).hexdigest()
    assert headers['X-BAPI-SIGN'] == expected
    assert mod.LAST_GOOD_PROXY == 'http://fixture-one'


def test_all_transport_failures_try_at_most_two_proxies_then_back_off(mod, monkeypatch):
    monkeypatch.setattr(mod, 'VERIFIED_PROXIES', ['http://fixture-one', 'http://fixture-two', 'http://fixture-three'])
    get = Mock(side_effect=requests.Timeout('fixture timeout')); monkeypatch.setattr(mod.requests, 'get', get)
    assert mod._bybit_signed_request('/v5/test') is None
    assert get.call_count == 3 and mod._BYBIT_HEALTH['reason'] == 'connection_failed'
    assert mod._bybit_signed_request('/v5/test') is None and get.call_count == 3


def test_response_error_diagnostic_never_logs_raw_bodies_keys_or_missing_none_pair(mod, monkeypatch):
    log = Mock(); monkeypatch.setattr(mod.logger, 'error', log)
    monkeypatch.setattr(mod.requests, 'get', Mock(return_value=response({'retMsg': 'SECRET-FIXTURE-BODY'})))
    mod._bybit_signed_request('/v5/test')
    logged = ' '.join(str(call) for call in log.call_args_list)
    assert 'invalid_api_envelope' in logged
    assert 'SECRET-FIXTURE-BODY' not in logged and 'fixture-secret' not in logged
    assert 'retCode=None retMsg=None' not in logged
