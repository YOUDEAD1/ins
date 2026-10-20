"""Concrete request/body reductions; fake services, real loopback HTTP only."""
import ast
import datetime as dt
import gzip
import json
import types
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from bson import BSON
from harness import APP
from test_integration import mod, isolate, server, request
from test_reliability import manager
from test_referral_integrity import referral


def metric(name, value):
    path = APP.parent / 'round29-network-benchmarks.json'
    data = json.loads(path.read_text()) if path.exists() else {}
    data[name] = value
    path.write_text(json.dumps(data, indent=2))


def baseline(mod, name):
    source = (APP / 'verification/round29_network_baseline.py').read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == name)
    scope = dict(mod.__dict__)
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<offline-baseline>', 'exec'), scope)
    return scope[name]


def account():
    return {'_id': 'benchmark-account', 'data': {'accessToken': 'FAKE-ACCESS', 'sessionToken': 'FAKE-SESSION',
        'account': {'id': 'benchmark-account'}, 'user': {'email': 'owner@example.test'}}}


def provider(monkeypatch, mod, seats=5):
    members = [[{'id': 'owner', 'email': 'owner@example.test'}, {'id': 'member', 'email': 'member@example.test'}]]
    def get(url, **kwargs):
        if url.endswith('/users'):
            body = {'items': members[0]}
        elif '/accounts/check/' in url:
            body = {'accounts': {}}
        else:
            body = {'max_seats': seats} if seats else {}
        return types.SimpleNamespace(status_code=200, json=lambda: body, text='', headers={})
    http = Mock(side_effect=get)
    monkeypatch.setattr(mod.cffi_requests, 'get', http)
    return http, members


def test_repeated_inventory_reads_reduce_remote_requests_and_purchase_validation_stays_fresh(mod, monkeypatch):
    http, members = provider(monkeypatch, mod)
    old = baseline(mod, '_cgpt_account_seat_info')
    for _ in range(20):
        assert old(account())['available_seats'] == 3
    before = http.call_count
    http.reset_mock()
    mod._cgpt_stock_invalidate(capacity=True)
    for _ in range(20):
        assert mod._cgpt_account_seat_info(account())['available_seats'] == 3
    after = http.call_count
    assert before == 40 and after == 2
    members[0] = members[0][:1]
    assert mod._cgpt_account_seat_info(account(), fresh=True)['available_seats'] == 4
    assert http.call_count == after + 1
    metric('twenty_inventory_reads', {'requests_before': before, 'requests_after': after,
        'fresh_purchase_reads_current_members': True})


def test_simultaneous_stock_readers_share_one_cold_refresh(mod, monkeypatch):
    http, _ = provider(monkeypatch, mod)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: mod._cgpt_account_seat_info(account()), range(16)))
    assert all(r['available_seats'] == 3 for r in results) and http.call_count == 2


def test_unknown_seat_capacity_is_not_probed_on_four_endpoints_every_time(mod, monkeypatch):
    http, _ = provider(monkeypatch, mod, seats=None)
    for _ in range(10):
        assert mod._cgpt_account_seat_info(account(), fresh=True)['total_seats'] is None
    assert http.call_count == 13


def test_explicit_admin_diagnosis_can_refresh_capacity_metadata(mod, monkeypatch):
    http, _ = provider(monkeypatch, mod)
    mod._cgpt_account_seat_info(account())
    m = mod._cgpt_build_manager_from_doc(account())
    assert m.diagnose()['total_seats'] == 5 and http.call_count == 4


def test_unchanged_invites_no_longer_rewrite_the_whole_document(mod, monkeypatch):
    m = manager(mod)
    original = mod.db.cgpt_invites_data.update_one
    writes = Mock(wraps=original)
    monkeypatch.setattr(mod.db.cgpt_invites_data, 'update_one', writes)
    old = baseline(mod, '_save_data')
    for _ in range(20): old(m)
    before = writes.call_count
    bytes_before = sum(len(BSON.encode(c.args[1])) for c in writes.call_args_list)
    writes.reset_mock()
    for _ in range(20): m._save_data()
    assert before == 20 and writes.call_count == 0
    m.invites_data['invites']['new@example.test'] = {'status': 'active', 'expires_at': '2027-01-01T00:00:00'}
    m._save_data()
    assert writes.call_count == 1
    assert mod.db.cgpt_invites_data.find_one({'_id': 'test-account'})['data']['invites']['new@example.test']
    metric('twenty_unchanged_invite_saves', {'writes_before': before, 'writes_after': 0,
        'update_payload_bytes_before': bytes_before, 'update_payload_bytes_after': 0})


def test_quiet_cleanup_does_not_save_but_real_expiry_is_persisted(mod, monkeypatch):
    m = manager(mod)
    monkeypatch.setattr(m, '_get_org_users', lambda: [{'id': 'paid', 'email': 'paid@example.test'}])
    writes = Mock(wraps=mod.db.cgpt_invites_data.update_one)
    monkeypatch.setattr(mod.db.cgpt_invites_data, 'update_one', writes)
    m.check_and_cleanup()
    assert writes.call_count == 0
    # Replace the payload: Mongo dot notation cannot address email keys with dots.
    data = mod.db.cgpt_invites_data.find_one({'_id': 'test-account'})['data']
    data['invites']['paid@example.test']['expires_at'] = '2020-01-01T00:00:00'
    mod.db.cgpt_invites_data.update_one({'_id': 'test-account'}, {'$set': {'data': data}})
    writes.reset_mock()
    monkeypatch.setattr(m, '_remove_user', lambda *a: True)
    m.check_and_cleanup()
    assert writes.call_count == 1
    assert mod.db.cgpt_invites_data.find_one({'_id': 'test-account'})['data']['invites']['paid@example.test']['status'] == 'expired'


def test_unverified_referrals_generate_no_background_membership_requests(mod, monkeypatch):
    mod.db.referrals_v2.insert_many([{'invited_id': 6000 + i, 'referrer_id': 1001,
        'status': 'active' if i % 2 else 'pending', 'dv': 'need', 'ref_policy_version': 30} for i in range(2000)])
    check = Mock(return_value=True); monkeypatch.setattr(mod, 'check_forced_sub', check)
    assert mod._ref_checker_tick() == 0 and check.call_count == 0
    metric('two_thousand_unverified_referrals', {'background_membership_checks_before': 2000,
        'background_membership_checks_after': check.call_count})


def test_only_due_verified_referrals_are_checked_in_bounded_batches(mod, monkeypatch):
    mod.db.referrals_v2.insert_many([{'invited_id': 7000 + i, 'referrer_id': 1001,
        'status': 'active', 'dv': 'ok', 'dv_at': 1} for i in range(120)])
    check = Mock(return_value=True); monkeypatch.setattr(mod, 'check_forced_sub', check)
    assert [mod._ref_checker_tick(50) for _ in range(4)] == [50, 50, 20, 0]
    for _ in range(6): assert mod._ref_checker_tick(50) == 0
    assert check.call_count == 120
    metric('ten_healthy_membership_cycles', {'checks_before': 1200, 'checks_after': check.call_count})


def test_leave_needs_three_real_confirmations_and_rejoin_keeps_history_without_double_count(mod, monkeypatch):
    referral(mod, paid=True)
    clock = [1800000000.]; monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: False)
    for n in range(3):
        assert mod._ref_checker_tick() == 1
        if n < 2: assert mod._ref_find(5001)['status'] == 'active'
        clock[0] += 61
    assert mod._ref_find(5001)['status'] == 'left' and mod.db.referrals_archived.count_documents({}) == 1
    assert mod.get_ref_counts(1001) == (0, 0, 1, 1)
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: True)
    mod.db.referrals_v2.update_one({'invited_id': 5001}, {'$set': {'sub_next_check_at': 0}})
    assert mod._ref_checker_tick() == 1
    assert mod.get_ref_counts(1001) == (0, 1, 0, 1)
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_telegram_membership_events_wake_only_the_relevant_referral(mod):
    referral(mod)
    mod.db.required_channels.insert_one({'channel_id': '@test_channel'})
    event = types.SimpleNamespace(chat=types.SimpleNamespace(id=-1001, username='test_channel'),
        new_chat_member=types.SimpleNamespace(user=types.SimpleNamespace(id=5001)))
    mod._ref_channel_membership_changed(event)
    assert mod._REF_SCAN_WAKE.is_set()
    assert mod._ref_find(5001)['membership_event_at']
    assert 'chat_member' in mod._ref_allowed_updates() and 'message' in mod._ref_allowed_updates()


def test_static_emoji_revalidation_sends_no_second_image_body(mod, server):
    asset = max((APP / 'webapp/emoji').glob('*.webp'), key=lambda p: p.stat().st_size)
    path = '/app/' + asset.relative_to(APP / 'webapp').as_posix()
    status, raw, headers = request(server, path, method='GET', auth=False)
    assert status == 200 and raw == asset.read_bytes()
    for _ in range(9):
        status, body, _ = request(server, path, method='GET', auth=False, headers={'If-None-Match': headers['ETag']})
        assert status == 304 and body == b''
    metric('ten_emoji_revalidations', {'response_body_bytes_before': len(raw) * 10,
        'response_body_bytes_after': len(raw), 'conditional_status': 304})


def test_static_gzip_is_not_encoded_twice_and_a_changed_file_gets_a_new_etag(mod, server, monkeypatch, tmp_path):
    import os
    data = ('console.log("local proof");\n' * 100).encode()
    path = tmp_path / 'proof.js'; path.write_bytes(data)
    monkeypatch.setattr(mod, 'WEBAPP_DIR', str(tmp_path))
    status, body, headers = request(server, '/app/proof.js', method='GET', auth=False, headers={'Accept-Encoding': 'gzip'})
    assert status == 200 and headers['Content-Encoding'] == 'gzip' and gzip.decompress(body) == data
    first = headers['ETag']; changed = data + b'// updated\n'; path.write_bytes(changed)
    os.utime(path, (1800000001, 1800000001))
    status, body, headers = request(server, '/app/proof.js', method='GET', auth=False, headers={'If-None-Match': first})
    assert status == 200 and body == changed and headers['ETag'] != first


def test_network_labels_distinguish_expensive_methods_without_identifiers(mod):
    assert mod._net_client_label('https://api.telegram.org/bot123:PRIVATE_TOKEN/getChatMember?user_id=SECRET') == 'api.telegram.org/getChatMember'
    assert mod._net_client_label('https://chatgpt.com/backend-api/accounts/PRIVATE_ACCOUNT/users') == 'chatgpt.com/members'
    assert 'PRIVATE' not in mod._net_client_label('https://api.telegram.org/file/bot123:PRIVATE_TOKEN/PRIVATE_FILENAME')
