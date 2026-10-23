"""Transient membership outages stay fail-closed without per-visitor alerts."""
from unittest.mock import Mock

import pytest

from test_integration import isolate, mod, request, server
from test_error_monitor import empty_monitor
from test_referral_integrity import challenge, referral, verify_payload


def test_repeated_membership_outage_does_not_alert_or_accumulate_error_records(mod, server, monkeypatch):
    notices = Mock()
    monkeypatch.setattr(mod, 'notify_admins', notices)
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **kw: None)
    before_balance = mod.db.users.find_one({'user_id': 1001})['balance']

    for uid in (5001, 5002, 5003):
        referral(mod, invited=uid, proof=False, status='pending')
        issued = challenge(mod, server, uid)
        for path in ('/webapp/verify', '/webapp/verify/', '/webapp/verify?retry=1'):
            status, body, headers = request(server, path, verify_payload(issued), uid=uid)
            assert status == 503
            assert body == {'ok': False, 'error': 'verification_unavailable'}
            assert headers['Cache-Control'] == 'no-store'
        rec = mod._ref_find(uid)
        assert rec['status'] == 'pending' and not mod._ref_web_verified(rec)
        assert not mod.db.ref_verification_challenges.find_one({'_id': issued['challenge']})['used']

    assert mod._ERROR_QUEUE.empty() and not mod._ERROR_MEMORY
    mod._error_drain()
    assert mod.db.system_errors.count_documents({}) == 0
    assert not notices.called
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == before_balance
    assert mod.db.ref_money_events.count_documents({}) == 0


def test_verification_recovers_using_the_same_unconsumed_challenge(mod, server, monkeypatch):
    referral(mod, proof=False, status='pending')
    issued = challenge(mod, server)
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **kw: None)
    assert request(server, '/webapp/verify', verify_payload(issued), uid=5001)[0] == 503
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **kw: True)
    status, body, _ = request(server, '/webapp/verify', verify_payload(issued), uid=5001)
    assert status == 200 and body['ok']
    assert mod._ref_web_verified(mod._ref_find(5001))
    assert mod._ref_find(5001)['status'] == 'active'
    assert mod.db.ref_verification_challenges.find_one({'_id': issued['challenge']})['used']
    mod._error_drain()
    assert mod.db.system_errors.count_documents({}) == 0


def test_unexpected_verification_exception_still_alerts(mod, server, monkeypatch):
    notices = Mock()
    monkeypatch.setattr(mod, 'notify_admins', notices)
    referral(mod, proof=False, status='pending')
    issued = challenge(mod, server)

    def broken_membership(*args, **kwargs):
        raise RuntimeError('Unexpected membership backend failure')

    monkeypatch.setattr(mod, 'check_forced_sub', broken_membership)
    status, body, _ = request(server, '/webapp/verify', verify_payload(issued), uid=5001)
    assert status == 500 and body == {'error': 'server_error'}
    mod._error_drain()
    rec = mod.db.system_errors.find_one({})
    assert rec['exception'] == 'RuntimeError'
    assert rec['operation'] == '/webapp/verify' and rec['user_id'] == 5001
    assert 'Unexpected membership backend failure' in rec['cause']
    assert notices.call_count == 1
    assert not mod._ref_web_verified(mod._ref_find(5001))


@pytest.mark.parametrize('path,code,error', [
    ('/webapp/verify', 503, 'server_error'),
    ('/webapp/verify', 500, 'verification_unavailable'),
    ('/webapp/test/other-outage', 503, 'verification_unavailable'),
])
def test_other_server_failures_still_alert(mod, server, monkeypatch, path, code, error):
    notices = Mock()
    monkeypatch.setattr(mod, 'notify_admins', notices)
    monkeypatch.setitem(mod._WA_POST_ROUTES, path,
                        lambda identity, body: (code, {'ok': False, 'error': error}))
    status, body, _ = request(server, path)
    assert status == code and body['error'] == error
    mod._error_drain()
    rec = mod.db.system_errors.find_one({})
    assert rec['operation'] == path and rec['user_id'] == 1001
    assert rec['message'] == 'HTTP operation failed: ' + error
    assert notices.call_count == 1


def test_handled_unavailability_does_not_hide_an_already_logged_failure(mod, server, monkeypatch):
    notices = Mock()
    monkeypatch.setattr(mod, 'notify_admins', notices)
    referral(mod, proof=False, status='pending')
    issued = challenge(mod, server)

    def logged_membership_failure(*args, **kwargs):
        mod.logger.error('Actionable membership provider fault')
        return None

    monkeypatch.setattr(mod, 'check_forced_sub', logged_membership_failure)
    status, body, _ = request(server, '/webapp/verify', verify_payload(issued), uid=5001)
    assert status == 503 and body['error'] == 'verification_unavailable'
    mod._error_drain()
    assert mod.db.system_errors.count_documents({}) == 1
    assert mod.db.system_errors.find_one({})['message'] == 'Actionable membership provider fault'
    assert notices.call_count == 1


def test_verification_still_requires_a_signed_telegram_identity(mod, server):
    status, body, _ = request(server, '/webapp/verify', auth=False)
    assert status == 401 and body == {'error': 'auth'}
