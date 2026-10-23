"""Request volume, fresh membership proof, settings and private diagnostics; fake providers only."""
import ast
import http.client
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from pymongo import InsertOne
from harness import APP, MESSAGES
from test_integration import mod, isolate, server, product, request
from test_products_menu import dispatch


@pytest.fixture(autouse=True)
def state(mod, isolate, monkeypatch):
    mod._settings_cache_invalidate()
    mod._fsub_invalidate()
    monkeypatch.setattr(mod, '_NET_METER', mod._NetworkMeter())


def baseline(mod, name):
    text = (APP / 'verification/round40_network_baseline.py').read_text()
    node = next(n for n in ast.parse(text).body if isinstance(n, ast.FunctionDef) and n.name == name)
    scope = dict(mod.__dict__)
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<offline-previous-update>', 'exec'), scope)
    return scope[name]


def metric(name, value):
    path = APP.parent / 'round40-network-benchmarks.json'
    data = json.loads(path.read_text()) if path.exists() else {}
    data[name] = value
    path.write_text(json.dumps(data, indent=2))


def channels(mod, count=2):
    mod.db.required_channels.insert_many([{'channel_id': '@fixture_' + str(i), 'title': 'Fixture channel',
        'kind': 'channel', 'username': 'fixture_' + str(i), 'invite_link': 'https://t.me/fixture_' + str(i),
        'meta_at': int(time.time())} for i in range(count)])


def test_twenty_repeated_configuration_reads_share_one_query_and_keep_defaults(mod, monkeypatch):
    find = Mock(wraps=mod.db.settings.find_one)
    monkeypatch.setattr(mod.db.settings, 'find_one', find)
    old = baseline(mod, 'get_setting')
    assert [old('wallet_address') for i in range(20)] == ['12345678'] * 20
    before = find.call_count
    find.reset_mock()
    assert [mod.get_setting('wallet_address') for i in range(20)] == ['12345678'] * 20
    after = find.call_count
    assert (before, after) == (20, 1)
    assert mod.get_setting('missing-setting', 'first') == 'first'
    assert mod.get_setting('missing-setting', 'second') == 'second'
    metric('twenty_configuration_reads', {'mongo_find_before': before, 'mongo_find_after': after})


@pytest.mark.parametrize('method', ['insert_one', 'insert_many', 'update_one', 'update_many', 'replace_one',
    'delete_one', 'delete_many', 'find_one_and_update', 'find_one_and_replace', 'find_one_and_delete', 'bulk_write', 'drop'])
def test_all_configuration_write_paths_invalidate_immediately(mod, method):
    c = mod.db.settings
    q = {'key': 'cache-fixture'}
    if method not in ('insert_one', 'insert_many', 'bulk_write'):
        c.insert_one(dict(q, value='old'))
    assert mod.get_setting('cache-fixture', 'missing') == ('missing' if method in ('insert_one', 'insert_many', 'bulk_write') else 'old')
    if method == 'insert_one': c.insert_one(dict(q, value='new'))
    elif method == 'insert_many': c.insert_many([dict(q, value='new')])
    elif method == 'bulk_write': c.bulk_write([InsertOne(dict(q, value='new'))])
    elif method == 'drop': c.drop()
    elif method in ('delete_one', 'delete_many', 'find_one_and_delete'): getattr(c, method)(q)
    elif method in ('replace_one', 'find_one_and_replace'): getattr(c, method)(q, dict(q, value='new'))
    else: getattr(c, method)(filter=q, update={'$set': {'value': 'new'}})
    assert mod.get_setting('cache-fixture', 'missing') == ('missing' if method.startswith('delete') or method in ('drop', 'find_one_and_delete') else 'new')


def test_cached_mutable_settings_cannot_be_modified_by_a_reader(mod):
    mod.db.settings.insert_one({'key': 'mutable-fixture', 'value': {'values': [1]}})
    value = mod.get_setting('mutable-fixture')
    value['values'].append(2)
    assert mod.get_setting('mutable-fixture') == {'values': [1]}


def test_concurrent_configuration_reads_share_one_cold_query(mod, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    find = mod.db.settings.find_one
    def blocked(*a, **k):
        entered.set()
        assert release.wait(3)
        return find(*a, **k)
    calls = Mock(side_effect=blocked)
    monkeypatch.setattr(mod.db.settings, 'find_one', calls)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(mod.get_setting, 'wallet_address') for i in range(16)]
        assert entered.wait(3)
        release.set()
        assert [f.result(3) for f in futures] == ['12345678'] * 16
    assert calls.call_count == 1


def test_configuration_write_during_inflight_read_cannot_repopulate_stale_cache(mod, monkeypatch):
    find = mod.db.settings.find_one
    entered, release = threading.Event(), threading.Event()
    def first(*a, **k):
        doc = find(*a, **k)
        entered.set()
        assert release.wait(3)
        return doc
    calls = Mock(side_effect=first)
    monkeypatch.setattr(mod.db.settings, 'find_one', calls)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(mod.get_setting, 'wallet_address')
        assert entered.wait(3)
        mod.db.settings.update_one({'key': 'wallet_address'}, {'$set': {'value': 'updated'}})
        release.set()
        assert future.result(3) == '12345678'
    assert mod.get_setting('wallet_address') == 'updated' and calls.call_count == 2


def test_failed_read_is_not_cached_and_external_changes_are_seen_after_ttl(mod, monkeypatch):
    calls = Mock(side_effect=[RuntimeError('fixture'), {'value': 'old'}, {'value': 'external'}])
    monkeypatch.setattr(mod.db.settings, 'find_one', calls)
    clock = [time.monotonic()]
    monkeypatch.setattr(mod.time, 'monotonic', lambda: clock[0])
    with pytest.raises(RuntimeError): mod.get_setting('ttl-fixture')
    assert mod.get_setting('ttl-fixture') == 'old'
    clock[0] += 31
    assert mod.get_setting('ttl-fixture') == 'external' and calls.call_count == 3


def test_check_and_channel_list_stop_duplicate_negative_membership_requests(mod, monkeypatch):
    channels(mod)
    member = Mock(side_effect=lambda cid, uid: NS(status='member' if cid.endswith('0') else 'left'))
    monkeypatch.setattr(mod.bot, 'get_chat_member', member)
    check, status = baseline(mod, 'check_forced_sub'), baseline(mod, 'forced_sub_status')
    for i in range(20):
        assert check(1001) is False
        assert [c['joined'] for c in status(1001)] == [True, False]
    before = member.call_count
    member.reset_mock()
    mod._fsub_invalidate()
    for i in range(20):
        assert mod.check_forced_sub(1001) is False
        assert [c['joined'] for c in mod.forced_sub_status(1001)] == [True, False]
    after = member.call_count
    assert (before, after) == (80, 2)
    metric('twenty_failed_join_screens_two_channels', {'telegram_requests_before': before, 'telegram_requests_after': after})


def test_explicit_website_check_rechecks_immediately_after_a_user_joins(mod, server, monkeypatch):
    channels(mod)
    member = Mock(return_value=NS(status='left'))
    monkeypatch.setattr(mod.bot, 'get_chat_member', member)
    assert mod.check_forced_sub(1001) is False
    member.return_value = NS(status='member')
    status, result, _ = request(server, '/webapp/forced/check', {})
    assert status == 200 and result['subscribed'] is True and result['forced'] is None
    assert member.call_count == 3


@pytest.mark.parametrize('member', [NS(status='restricted', is_member=False), NS(status='left'), NS(status='kicked')])
def test_denied_channel_states_do_not_turn_into_a_successful_join(mod, monkeypatch, member):
    channels(mod, 1)
    monkeypatch.setattr(mod.bot, 'get_chat_member', Mock(return_value=member))
    assert mod.check_forced_sub(1001) is False
    assert mod.forced_sub_status(1001)[0]['joined'] is False


def test_network_failure_is_unknown_and_is_not_a_false_leave_or_approval(mod, monkeypatch):
    channels(mod, 1)
    member = Mock(side_effect=RuntimeError('fixture offline'))
    monkeypatch.setattr(mod.bot, 'get_chat_member', member)
    for i in range(20):
        assert mod.check_forced_sub(1001) is None
        assert mod.forced_sub_status(1001)[0]['joined'] is None
    assert member.call_count == 1
    assert mod.db.ref_money_events.count_documents({}) == 0


def test_many_simultaneous_membership_screens_share_one_channel_read(mod, monkeypatch):
    channels(mod, 1)
    member = Mock(return_value=NS(status='left'))
    monkeypatch.setattr(mod.bot, 'get_chat_member', member)
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(mod.check_forced_sub, [1001] * 32)) == [False] * 32
    assert member.call_count == 1


def test_changing_required_channels_invalidates_all_old_positive_results(mod, monkeypatch):
    channels(mod, 1)
    member = Mock(return_value=NS(status='member'))
    monkeypatch.setattr(mod.bot, 'get_chat_member', member)
    assert mod.check_forced_sub(1001) is True
    mod.db.required_channels.update_one({}, {'$set': {'channel_id': '@different'}})
    member.return_value = NS(status='left')
    assert mod.check_forced_sub(1001) is False and member.call_count == 2


def test_membership_event_invalidates_cached_user_and_prioritizes_verification(mod, monkeypatch):
    channels(mod, 1)
    member = Mock(return_value=NS(status='member'))
    monkeypatch.setattr(mod.bot, 'get_chat_member', member)
    assert mod.check_forced_sub(1001)
    mod._ref_channel_membership_changed(NS(chat=NS(id=-100, username='fixture_0'), new_chat_member=NS(user=NS(id=1001))))
    member.return_value = NS(status='left')
    assert mod.check_forced_sub(1001) is False
    assert mod._REF_SCAN_WAKE.is_set()


def seed_refs(mod, n=200):
    mod.db.referrals_v2.insert_many([{'invited_id': 20000 + i, 'referrer_id': 1001,
        'status': 'active', 'dv': 'ok', 'dv_at': 1, 'sub_next_check_at': 0} for i in range(n)])


def test_runtime_background_budget_reduces_cold_three_channel_checks_from_300_to_75(mod, monkeypatch):
    channels(mod, 3)
    member = Mock(return_value=NS(status='member'))
    monkeypatch.setattr(mod.bot, 'get_chat_member', member)
    monkeypatch.setattr(mod, '_ref_resume_financial', Mock())
    monkeypatch.setattr(mod.time, 'sleep', Mock())
    class StopCycle(BaseException): pass
    monkeypatch.setattr(mod, '_REF_SCAN_WAKE', NS(wait=Mock(side_effect=StopCycle), clear=Mock()))
    seed_refs(mod)
    with pytest.raises(StopCycle): baseline(mod, 'background_referral_checker_v2')()
    before = member.call_count
    mod.db.referrals_v2.update_many({}, {'$set': {'sub_next_check_at': 0}})
    mod._fsub_invalidate()
    member.reset_mock()
    with pytest.raises(StopCycle): mod.background_referral_checker_v2()
    after = member.call_count
    assert (before, after) == (300, 75)
    metric('one_background_cycle_three_channels', {'telegram_requests_before': before,
        'telegram_requests_after': after, 'routine_batch_before': 100, 'routine_batch_after': 25})


@pytest.mark.parametrize('field,value', [('membership_event_pending', True), ('left_checks', 1)])
def test_events_and_followup_confirmations_are_processed_before_an_old_backlog(mod, monkeypatch, field, value):
    seed_refs(mod)
    mod.db.referrals_v2.update_one({'invited_id': 20199}, {'$set': {field: value, 'sub_next_check_at': 1}})
    check = Mock(return_value=True)
    monkeypatch.setattr(mod, 'check_forced_sub', check)
    assert mod._ref_checker_tick(limit=25) == 25
    assert check.call_args_list[0].args[0] == 20199
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_new_membership_event_arriving_during_a_check_is_not_cleared(mod, monkeypatch):
    seed_refs(mod, 1)
    mod.db.referrals_v2.update_one({}, {'$set': {'membership_event_at': 1}})
    def check(uid):
        mod.db.referrals_v2.update_one({'invited_id': uid}, {'$set': {'membership_event_at': 2, 'membership_event_pending': True}})
        return True
    monkeypatch.setattr(mod, 'check_forced_sub', check)
    assert mod._ref_checker_tick() == 1
    assert mod.db.referrals_v2.find_one()['membership_event_pending'] is True


def test_mongo_diagnostics_match_collections_to_out_of_order_replies_without_payloads(mod):
    listener = mod._NetworkMongoListener()
    for key, collection in [(1, 'settings'), (2, 'users')]:
        listener.started(NS(command_name='find', request_id=key, command={'find': collection, 'filter': {'token': 'PRIVATE-FIXTURE'}}))
    listener.succeeded(NS(command_name='find', request_id=2, reply={'ok': 1, 'data': 'PRIVATE-REPLY'}))
    listener.succeeded(NS(command_name='find', request_id=1, reply={'ok': 1}))
    snap = mod._NET_METER.snapshot()
    assert 'PRIVATE-' not in json.dumps(snap)
    assert len(snap['details']) == 2 and sum(r['requests'] for r in snap['rows']) == 2
    assert all(r['received'] > 0 and r['sent'] > 0 for r in snap['details'])
    assert not listener.pending


def test_get_query_bytes_are_counted_without_recording_the_query(mod):
    small = mod.requests.Request('GET', 'https://fixture.example/translate?q=hello').prepare()
    large = mod.requests.Request('GET', 'https://fixture.example/translate?q=' + 'PRIVATE-FIXTURE' * 1000).prepare()
    assert mod._net_request_size(large) - mod._net_request_size(small) > 10000
    assert 'PRIVATE' not in json.dumps(mod._NET_METER.snapshot())


def test_products_response_diagnostics_identify_consumer_and_304_without_exposing_api_key(mod, server):
    product(mod)
    key = 'sk_' + 'f' * 48
    mod.db.api_keys.insert_one({'user_id': 1001, 'api_key': key, 'is_active': True})
    mod._api_rate.clear()
    path = '/' + mod._get_api_gateway() + '/products'
    conn = http.client.HTTPConnection(*server)
    conn.request('GET', path, headers={'Authorization': 'Bearer ' + key})
    r = conn.getresponse(); r.read(); tag = r.getheader('ETag')
    assert r.status == 200
    conn.close()
    conn = http.client.HTTPConnection(*server)
    conn.request('GET', path, headers={'Authorization': 'Bearer ' + key, 'If-None-Match': tag})
    r = conn.getresponse(); assert r.status == 304 and r.read() == b''
    conn.close()
    origins = {r['origin'] for r in mod._NET_METER.snapshot()['details']}
    # Receiving the body does not imply that the server thread's finally block
    # has committed its metrics yet. Wait for that lifecycle boundary.
    deadline=time.monotonic()+1
    while 'api_products:customer_1001:304' not in origins and time.monotonic()<deadline:
        time.sleep(0.005)
        origins={r['origin'] for r in mod._NET_METER.snapshot()['details']}
    assert {'api_products:customer_1001:200', 'api_products:customer_1001:304'} <= origins
    assert key not in json.dumps(mod._NET_METER.snapshot())


def test_detailed_network_export_and_command_are_private_and_do_not_query_providers(mod, monkeypatch):
    mod._NET_METER.record('mongo', 'find', sent=200, received=400, detail='settings.find @ get_setting')
    files = []
    monkeypatch.setattr(mod.bot, 'send_document', lambda cid, data, **kw: files.append((cid, json.loads(data.getvalue()))))
    dispatch(mod, 'ad_network_export', 1001)
    assert not files
    dispatch(mod, 'ad_network_export', 9001)
    assert files[0][0] == 9001 and files[0][1]['revision'] == 40
    assert files[0][1]['details'][0]['origin'] == 'settings.find @ get_setting'
    assert 'ad_network_export' in mod._ADM_OPEN_OK and not mod._wa_bot_only('ad_network_export')
    MESSAGES.clear()
    mod.network_command(NS(from_user=NS(id=1001), chat=NS(id=1001)))
    assert not MESSAGES
    mod.network_command(NS(from_user=NS(id=9001), chat=NS(id=9001)))
    assert any('settings.find' in text for cid, text, kw in MESSAGES)


def test_diagnostic_memory_is_bounded_and_main_totals_do_not_double_count_details(mod):
    for i in range(1000):
        mod._NET_METER.record('mongo', 'find', sent=100, detail='settings.find @ function_' + str(i))
    snap = mod._NET_METER.snapshot()
    assert len(snap['details']) <= 193
    assert sum(r['requests'] for r in snap['rows']) == 1000
    assert sum(r['sent'] for r in snap['rows']) == 100000


def test_reusing_channel_proof_does_not_extend_its_original_five_minute_lifetime(mod, monkeypatch):
    channels(mod, 1)
    clock = [time.time()]
    monkeypatch.setattr(mod.time, 'time', lambda: clock[0])
    member = Mock(return_value=NS(status='member'))
    monkeypatch.setattr(mod.bot, 'get_chat_member', member)
    assert mod.check_forced_sub(1001) is True
    first_expiry = mod._FORCED_SUB_CACHE[1001][1]
    clock[0] += 299
    mod._FORCED_SUB_CACHE.pop(1001)
    assert mod.check_forced_sub(1001) is True and member.call_count == 1
    assert mod._FORCED_SUB_CACHE[1001][1] == first_expiry
    clock[0] += 2
    member.return_value = NS(status='left')
    assert mod.check_forced_sub(1001) is False and member.call_count == 2


def test_membership_event_during_request_cannot_repopulate_a_stale_positive_cache(mod, monkeypatch):
    channels(mod, 1)
    def changed(cid, uid):
        mod._fsub_invalidate_user(uid)
        return NS(status='member')
    member = Mock(side_effect=changed)
    monkeypatch.setattr(mod.bot, 'get_chat_member', member)
    assert mod.check_forced_sub(1001) is True
    assert 1001 not in mod._FORCED_SUB_CACHE
    member.side_effect = None
    member.return_value = NS(status='left')
    assert mod.check_forced_sub(1001) is False and member.call_count == 2


def test_network_command_dispatches_through_real_telegram_message_handlers(mod):
    message = mod.types.Message.de_json({'message_id': 99, 'date': 1,
        'from': {'id': 9001, 'is_bot': False, 'first_name': 'Fixture Admin'},
        'chat': {'id': 9001, 'type': 'private'}, 'text': '/network',
        'entities': [{'type': 'bot_command', 'offset': 0, 'length': 8}]})
    mod.bot._run_middlewares_and_handler(message, handlers=mod.bot.message_handlers,
        middlewares=None, update_type='message')
    assert any('استهلاك الشبكة' in text for cid, text, kw in MESSAGES)
