"""Regressions for the concrete sources in lara-network-report.json; no live services."""
import collections
import concurrent.futures
import json
import time
import types
from unittest.mock import Mock

import pytest

from harness import APP, MESSAGES
from test_integration import mod, isolate, product, server
from test_channel_broadcast import message
from test_network import api_raw


@pytest.fixture(autouse=True)
def report_state(mod, monkeypatch):
    assert getattr(mod, '_NETWORK_REPORT_FIXES', None) is not None, 'The optimization must load, not silently fall back'
    service = mod._NETWORK_REPORT_FIXES
    service.invalidate('products', 'ext_products', 'cgpt_products', 'notice_buttons', 'stock_counts', 'seat_count')
    service.counters.clear(); service.seen_contacts.clear(); service.rpc_health.clear(); service.http_statuses.clear()
    service.binance_health.update(credentials=None, failures=0, retry_at=0.)
    vars(service.context).clear()
    monkeypatch.setattr(mod.time, 'sleep', lambda _: None)
    monkeypatch.setattr(mod, '_wa_public_url', lambda: 'https://local.example/app/')
    yield
    vars(service.context).clear()


def cgpt(mod, monkeypatch):
    cg = mod.db.cgpt_products.insert_one({'name': 'ChatGPT', 'durations': [
        {'dur_id': 'day', 'price': 4, 'minutes': 1440},
        {'dur_id': 'week', 'price': 12, 'minutes': 10080}]}).inserted_id
    mod.db.cgpt_accounts.insert_one({'name': 'Private account', 'data': {
        'accessToken': 'PRIVATE_ACCOUNT_TOKEN', 'unused_payload': 'x' * 200000}})
    monkeypatch.setattr(mod, '_cgpt_account_seat_info', lambda doc, fresh=False: {
        'connected': True, 'available_seats': 8})
    return str(cg)


def synchronous_broadcasts(mod, monkeypatch):
    def thread(target=None, args=(), kwargs=None, **extra):
        return types.SimpleNamespace(start=lambda: target(*args, **(kwargs or {}))
            if getattr(target, '__name__', '') in ('broadcast_new_stock', 'broadcast_price_drop') else None)
    monkeypatch.setattr(mod.threading, 'Thread', thread)


def test_twenty_consumers_share_heavy_reads_but_keep_their_own_catalogues(mod, monkeypatch):
    product(mod, desc_en='Public description ' * 400)
    cgpt(mod, monkeypatch)
    users = list(range(20000, 20020))
    for uid in users:
        mod.db.api_pricing.insert_one({'api_user_id': uid, 'product_id': '101',
                                      'sell_price': uid / 1000, 'name_en': f'PRIVATE-NAME-{uid}'})
    accounts = Mock(wraps=mod.db.cgpt_accounts.find)
    monkeypatch.setattr(mod.db.cgpt_accounts, 'find', accounts)
    payload = mod._net_api_products_payload
    raw_payload = payload.__wrapped__
    seats = mod._cgpt_total_available_seats
    monkeypatch.setattr(mod, '_cgpt_total_available_seats', seats.__wrapped__)
    before = {uid: raw_payload(uid) for uid in users}
    baseline_reads = accounts.call_count
    accounts.reset_mock()
    monkeypatch.setattr(mod, '_cgpt_total_available_seats', seats)
    mod._invalidate_products_cache()
    service = mod._NETWORK_REPORT_FIXES
    read_counts = collections.Counter()
    cached = service.cached
    def measured(key, groups, load, **kwargs):
        def counted():
            read_counts['stock_counts' if key[0] == 'stock_counts' else groups[0]] += 1
            return load()
        return cached(key, groups, counted, **kwargs)
    monkeypatch.setattr(service, 'cached', measured)
    after = {uid: payload(uid) for uid in users}
    assert after == before
    assert baseline_reads == 20 and accounts.call_count == 1
    assert read_counts['stock_counts'] == 1 and read_counts['ext_products'] == 1
    assert read_counts['cgpt_products'] == 1 and read_counts['products'] == 2
    for uid, result in after.items():
        own = next(p for p in result['products'] if p['id'] == '101')
        assert own['name_en'] == f'PRIVATE-NAME-{uid}' and own['your_price'] == uid / 1000
        assert {p['stock'] for p in result['products'] if p['id'].startswith('cgpt_')} == {8}
        assert 'PRIVATE_ACCOUNT_TOKEN' not in json.dumps(result)
    report = {'consumers': 20, 'outputs_identical': True,
        'cgpt_account_reads_before': baseline_reads, 'cgpt_account_reads_after': accounts.call_count,
        'heavy_account_bytes_per_read_in_fixture': 200000,
        'shared_source_loads_after': dict(read_counts),
        'scope': 'Synthetic local comparison, same original API builder; not a production bandwidth promise'}
    (APP/'verification/NETWORK-REPORT-BENCHMARK.json').write_text(json.dumps(report, indent=2))


def test_concurrent_consumers_do_not_stampede_the_account_read(mod, monkeypatch):
    cgpt(mod, monkeypatch)
    calls = Mock(wraps=mod._cgpt_all_accounts)
    monkeypatch.setattr(mod, '_cgpt_all_accounts', calls)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(mod._net_api_products_payload, range(25000, 25020)))
    assert calls.call_count == 1
    assert all(len(result['products']) == 2 for result in results)


@pytest.mark.parametrize('change', ['price', 'hide', 'stock', 'folder', 'private_price', 'private_hide'])
def test_local_changes_bypass_all_display_caches_immediately(mod, change):
    product(mod)
    assert mod._net_api_products_payload(1001)['products'][0]['stock'] == 3
    if change == 'price': mod.db.products.update_one({'id': '101'}, {'$set': {'price': 7}})
    elif change == 'hide': mod.db.products.update_one({'id': '101'}, {'$set': {'is_hidden': True}})
    elif change == 'stock': mod.db.product_stock.update_many({'product_id': '101'}, {'$set': {'is_sold': True}})
    elif change == 'folder': mod.db.catalogs.insert_one({'product_ids': ['101'], 'is_hidden': True})
    elif change == 'private_price': mod.db.api_pricing.insert_one({'api_user_id': 1001, 'product_id': '101', 'sell_price': 11})
    else: mod.db.api_hidden.insert_one({'api_user_id': 1001, 'product_id': '101'})
    own = mod._net_api_products_payload(1001)['products']
    if change in ('hide', 'folder', 'private_hide'): assert own == []
    elif change == 'price': assert own[0]['store_price'] == 7
    elif change == 'stock': assert own[0]['stock'] == 0
    else: assert own[0]['your_price'] == 11
    if change in ('private_price', 'private_hide'):
        other = mod._net_api_products_payload(1002)['products']
        assert len(other) == 1 and other[0]['your_price'] is None


def test_cgpt_hidden_duration_never_leaks_to_another_reseller(mod, monkeypatch):
    key = cgpt(mod, monkeypatch)
    mod.db.api_hidden.insert_one({'api_user_id': 1001, 'product_id': 'cgpt_' + key + '_day'})
    first = mod._net_api_products_payload(1001)['products']
    second = mod._net_api_products_payload(1002)['products']
    assert [p['id'] for p in first] == ['cgpt_' + key + '_week']
    assert len(second) == 2
    assert mod._NETWORK_REPORT_FIXES.counters['hidden_checks_reused'] == 6


def test_parent_visibility_changes_override_previously_shared_cgpt_data(mod, monkeypatch):
    key = cgpt(mod, monkeypatch)
    assert mod._net_api_products_payload(1001)['products']
    mod.db.products.insert_one({'id': 'cgpt_main_' + key, 'product_type': 'cgpt_main',
                               'cgpt_product_id': key, 'is_hidden': True})
    assert not mod._net_api_products_payload(1001)['products']


def test_read_cache_is_scoped_to_catalogue_display_and_never_auth_or_wallet(mod):
    product(mod)
    mod._net_api_products_payload(1001)
    mod.db.products.update_one({'id': '101'}, {'$set': {'price': 9}})
    assert mod.db.products.find_one({'id': '101'})['price'] == 9
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': 2}})
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 2
    assert not getattr(mod._NETWORK_REPORT_FIXES.context, 'api', False)


def test_cached_results_are_copied_and_do_not_transfer_mutations_between_consumers(mod):
    service = mod._NETWORK_REPORT_FIXES
    load = Mock(return_value=[{'name': 'original', 'nested': [1]}])
    first = service.cached(('copy',), ('products',), load)
    first[0]['nested'].append(2)
    second = service.cached(('copy',), ('products',), load)
    second[0]['name'] = 'edited'
    assert service.cached(('copy',), ('products',), load) == [{'name': 'original', 'nested': [1]}]
    assert load.call_count == 1


def test_write_during_inflight_read_prevents_stale_cache_commit(mod):
    service = mod._NETWORK_REPORT_FIXES
    def read_then_write():
        service.invalidate('products')
        return {'stale': True}
    assert service.cached(('race',), ('products',), read_then_write) == {'stale': True}
    fresh = Mock(return_value={'fresh': True})
    assert service.cached(('race',), ('products',), fresh) == {'fresh': True}
    fresh.assert_called_once()


def test_expired_or_failed_reads_are_not_remembered_as_empty_stock(mod, monkeypatch):
    service = mod._NETWORK_REPORT_FIXES
    clock = [1000.]
    monkeypatch.setattr(mod.time, 'monotonic', lambda: clock[0])
    load = Mock(return_value={'count': 3})
    service.cached(('expiry',), ('products',), load)
    clock[0] += 31
    load.side_effect = OSError('Read offline')
    with pytest.raises(OSError): service.cached(('expiry',), ('products',), load)
    load.side_effect = None; load.return_value = {'count': 8}
    assert service.cached(('expiry',), ('products',), load) == {'count': 8}
    assert load.call_count == 3


def test_bounded_cache_limits_and_oversized_entries(mod):
    service = mod._NETWORK_REPORT_FIXES
    for i in range(120): service.cached(('limit', i), ('products',), lambda: 'x' * 100000)
    assert len(service.entries) <= service.MAX_ENTRIES
    assert sum(e['bytes'] for e in service.entries.values()) <= service.MAX_BYTES
    load = Mock(return_value='x' * (service.MAX_BYTES + 1))
    service.cached(('huge',), ('products',), load); service.cached(('huge',), ('products',), load)
    assert load.call_count == 2


def test_stacked_catalogue_caches_never_extend_the_oldest_snapshot_ttl(mod, monkeypatch):
    product(mod)
    clock = [1000.]
    monkeypatch.setattr(mod.time, 'monotonic', lambda: clock[0])
    mod._net_api_products_payload(1001)
    clock[0] += 29
    mod._net_api_products_payload(1002)
    entry = mod._PRODUCTS_CACHE['_users'][1002]
    assert 0 < entry['expires'] - time.time() <= 1.01


def test_fresh_seat_checks_bypass_the_shared_display_count(mod, monkeypatch):
    cgpt(mod, monkeypatch)
    assert mod._cgpt_total_available_seats() == 8
    monkeypatch.setattr(mod, '_cgpt_account_seat_info', lambda doc, fresh=False: {'connected': True, 'available_seats': 2})
    assert mod._cgpt_total_available_seats() == 8
    assert mod._cgpt_total_available_seats(fresh=True) == 2
    assert mod._cgpt_total_available_seats() == 2


def test_unknown_seats_remain_unknown_and_have_only_a_short_retry_cache(mod, monkeypatch):
    cgpt(mod, monkeypatch)
    monkeypatch.setattr(mod, '_cgpt_account_seat_info', lambda doc, fresh=False: {'connected': False})
    assert mod._cgpt_total_available_seats() == 0 and mod._CGPT_SEAT_PROOF.known is False
    entry = mod._NETWORK_REPORT_FIXES.entries[('seat_count',)]
    assert 0 < entry['expires'] - time.monotonic() <= 5
    mod._CGPT_SEAT_PROOF.known = True
    assert mod._cgpt_total_available_seats() == 0 and mod._CGPT_SEAT_PROOF.known is False


def test_cgpt_account_update_invalidates_existing_api_counts_immediately(mod, monkeypatch):
    cgpt(mod, monkeypatch)
    assert mod._net_api_products_payload(1001)['products'][0]['stock'] == 8
    monkeypatch.setattr(mod, '_cgpt_account_seat_info', lambda doc, fresh=False: {'connected': True, 'available_seats': 1})
    mod.db.cgpt_accounts.update_one({}, {'$set': {'manual_seats': 1}})
    assert mod._net_api_products_payload(1001)['products'][0]['stock'] == 1


@pytest.mark.parametrize('kind', ['stock', 'price'])
def test_broadcast_reuses_recipient_language_and_cms_buttons_without_per_user_reads(mod, monkeypatch, kind):
    product(mod, custom_emoji_id='123456789', discount_tiers=[{'min_qty': 3, 'price': 4}])
    mod.db.users.update_many({}, {'$set': {'lang_chosen': True}})
    mod.db.custom_buttons.insert_one({'lang': 'en', 'key': 'btn_notice_bot', 'text': 'Custom buy', 'emoji_id': '999888777', 'style': 'success'})
    users = Mock(wraps=mod.db.users.find_one)
    buttons = Mock(wraps=mod.db.custom_buttons.find_one)
    monkeypatch.setattr(mod.db.users, 'find_one', users)
    monkeypatch.setattr(mod.db.custom_buttons, 'find_one', buttons)
    synchronous_broadcasts(mod, monkeypatch)
    if kind == 'stock': mod.admin_stock_save(message(mod, 'FRESH-CODE'), '101')
    else: mod.admin_save_edit(message(mod, '4'), 'price', '101')
    recipients = [row for row in MESSAGES if row[0] in (1001, 1002) and row[2].get('reply_markup')]
    assert len(recipients) == 2
    assert buttons.call_count == 0 and users.call_count < 3
    english = next(row for row in recipients if row[0] == 1001)
    raw = english[2]['reply_markup'].to_dict()['inline_keyboard']
    assert raw[-1][0]['text'] == 'Custom buy' and raw[-1][0]['icon_custom_emoji_id'] == '999888777'
    assert 'web_app' in raw[0][0] and 'callback_data' in raw[-1][0]
    assert '<tg-emoji emoji-id="123456789">' in english[1]
    assert mod._NETWORK_REPORT_FIXES.counters['broadcast_recipient_batches'] == 1
    assert not mod._NETWORK_REPORT_FIXES.in_fanout()


def test_no_stock_broadcast_for_empty_input_or_a_hidden_product(mod, monkeypatch):
    product(mod, is_hidden=True)
    synchronous_broadcasts(mod, monkeypatch)
    mod.admin_stock_save(message(mod, ' \n\t'), '101')
    assert not mod._NETWORK_REPORT_FIXES.counters['broadcast_runs']
    MESSAGES.clear()
    mod.admin_stock_save(message(mod, 'NEW-CODE'), '101')
    assert not [r for r in MESSAGES if r[0] in (1001, 1002)]
    assert mod.db.product_stock.count_documents({'product_id': '101', 'is_sold': False}) == 4


@pytest.mark.parametrize('kind', ['unreachable', 'other', 'format', 'rate_limit'])
def test_only_confirmed_unreachable_recipients_are_paused_for_future_stock_notices(mod, monkeypatch, kind):
    service = mod._NETWORK_REPORT_FIXES
    monkeypatch.setattr(mod, '_telegram_delivery_kind', lambda exc: kind)
    service.context.fanout = True; service.context.languages = {1001: 'en'}
    operation = Mock(side_effect=RuntimeError('synthetic explicit rejection'))
    with pytest.raises(RuntimeError): service.send(operation, 1001, 'Stock')
    saved = mod.db.users.find_one({'user_id': 1001})
    assert bool(saved.get('stock_notice_unreachable')) == (kind == 'unreachable')
    assert saved['balance'] == 100 and saved['is_banned'] == 0


def test_blocked_users_are_skipped_then_resume_after_a_genuine_incoming_interaction(mod, monkeypatch):
    product(mod)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'stock_notice_unreachable': True}})
    mod.db.users.update_one({'user_id': 1002}, {'$set': {'is_banned': 1}})
    synchronous_broadcasts(mod, monkeypatch)
    mod.admin_stock_save(message(mod, 'ONE'), '101')
    assert not [r for r in MESSAGES if r[0] in (1001, 1002)]
    service = mod._NETWORK_REPORT_FIXES
    event = types.SimpleNamespace(from_user=types.SimpleNamespace(id=1001, is_bot=False))
    forward = Mock()
    service.incoming(forward, [event]); service.incoming(forward, [event])
    assert forward.call_count == 2
    assert not mod.db.users.find_one({'user_id': 1001}).get('stock_notice_unreachable')
    MESSAGES.clear(); mod.admin_stock_save(message(mod, 'TWO'), '101')
    assert len([r for r in MESSAGES if r[0] == 1001]) == 1


def test_cms_button_edits_are_visible_on_the_next_broadcast(mod, monkeypatch):
    product(mod)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'lang_chosen': True}})
    synchronous_broadcasts(mod, monkeypatch)
    mod.admin_stock_save(message(mod, 'ONE'), '101')
    mod.db.custom_buttons.insert_one({'lang': 'en', 'key': 'btn_notice_bot', 'text': 'Changed now'})
    MESSAGES.clear(); mod.admin_stock_save(message(mod, 'TWO'), '101')
    own = next(r for r in MESSAGES if r[0] == 1001)
    assert own[2]['reply_markup'].to_dict()['inline_keyboard'][-1][0]['text'] == 'Changed now'


def test_failed_rpc_nodes_back_off_without_caching_transaction_responses(mod, monkeypatch):
    monkeypatch.setattr(mod, 'BSC_PUBLIC_RPCS', ['https://bad.example', 'https://good.example'])
    monkeypatch.setattr(mod.random, 'shuffle', lambda nodes: None)
    calls = []
    def rpc(url, payload, **kwargs):
        calls.append((url, payload['params']))
        return None if 'bad' in url else {'result': '0x' + str(len(calls))}
    monkeypatch.setattr(mod, '_http_post_json', rpc)
    first = mod._bsc_rpc_call('eth_blockNumber', [])
    second = mod._bsc_rpc_call('eth_blockNumber', ['new'])
    assert first != second and len(calls) == 3
    assert sum('bad' in url for url, _ in calls) == 1
    assert calls[-1][1] == ['new']


def test_all_rpc_outages_remain_unknown_and_retry_after_cooldown(mod, monkeypatch):
    monkeypatch.setattr(mod, 'BSC_PUBLIC_RPCS', ['https://offline.example'])
    clock = [1000.]; monkeypatch.setattr(mod.time, 'monotonic', lambda: clock[0])
    rpc = Mock(return_value=None); monkeypatch.setattr(mod, '_http_post_json', rpc)
    assert mod._bsc_rpc_call('eth_getLogs', []) is None
    assert mod._bsc_rpc_call('eth_getLogs', []) is None and rpc.call_count == 1
    clock[0] += 31; rpc.return_value = {'result': []}
    assert mod._bsc_rpc_call('eth_getLogs', []) == [] and rpc.call_count == 2


def test_valid_null_receipt_does_not_mark_a_healthy_rpc_as_failed(mod, monkeypatch):
    monkeypatch.setattr(mod, 'BSC_PUBLIC_RPCS', ['https://healthy.example'])
    rpc = Mock(return_value={'result': None}); monkeypatch.setattr(mod, '_http_post_json', rpc)
    assert mod._bsc_rpc_call('eth_getTransactionReceipt', ['pending']) is None
    rpc.return_value = {'result': {'status': '0x1'}}
    assert mod._bsc_rpc_call('eth_getTransactionReceipt', ['confirmed']) == {'status': '0x1'}
    assert rpc.call_count == 2


def test_display_cache_failure_cannot_change_the_result_of_a_committed_wallet_write(mod, monkeypatch):
    monkeypatch.setattr(mod._NETWORK_REPORT_FIXES, 'changed', Mock(side_effect=RuntimeError('Cache invalidation failed')))
    result = mod.db.users.update_one({'user_id': 1001}, {'$inc': {'balance': 1}})
    assert result.modified_count == 1 and mod.db.users.find_one({'user_id': 1001})['balance'] == 101


def test_binance_only_failed_cycles_back_off_and_new_credentials_reset_it(mod, monkeypatch):
    service = mod._NETWORK_REPORT_FIXES
    clock = [1000.]; monkeypatch.setattr(mod.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(mod, 'BINANCE_API_KEY', 'fake-A'); monkeypatch.setattr(mod, 'BINANCE_API_SECRET', 'fake-secret')
    provider = Mock(return_value=None)
    assert service.binance(provider) is None
    assert service.binance(provider) is None and provider.call_count == 1
    clock[0] += 16
    assert service.binance(provider) is None and provider.call_count == 2
    monkeypatch.setattr(mod, 'BINANCE_API_KEY', 'fake-B')
    provider.return_value = []
    assert service.binance(provider) == [] and provider.call_count == 3
    assert service.binance(provider) == [] and provider.call_count == 4
    assert service.binance_health['retry_at'] == 0


def test_new_report_counts_http_statuses_and_never_exports_cache_contents_or_credentials(mod, server):
    product(mod)
    status, headers, body = api_raw(mod, server)
    assert status == 200
    status, headers, body = api_raw(mod, server, headers={'If-None-Match': headers['ETag']})
    assert status == 304
    data = mod._NET_METER.snapshot()['network_report_fixes']
    assert any(row['status'] == 200 for row in data['http_statuses'])
    # The response write can complete before the server's finally block finishes.
    deadline = time.monotonic() + 2
    while not any(row['status'] == 304 for row in data['http_statuses']) and time.monotonic() < deadline:
        data = mod._NET_METER.snapshot()['network_report_fixes']
    assert any(row['status'] == 304 for row in data['http_statuses'])
    assert 'entries' not in data and 'credentials' not in json.dumps(data)
