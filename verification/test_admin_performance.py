"""Admin performance, report equivalence, cache coherence and access isolation."""
import copy
import datetime as dt
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from bson import BSON, ObjectId

from test_integration import mod, isolate, server, request, product


def oid(when, serial=0):
    return ObjectId(ObjectId.from_datetime(when).binary[:4] + int(serial).to_bytes(8, 'big'))


def seed_reports(mod):
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    product(mod, price=7, stock=3, cost=2)
    for n, days in enumerate((0, 0, 2, 12, 40, 200, 500)):
        when = now - dt.timedelta(days=days)
        mod.db.orders.insert_one({'_id': oid(when, 100 + n), 'user_id': 1001 if n % 2 else 1002,
            'product_id': '101', 'price': 5, 'qty': 2, 'cost_total': 3,
            'order_id': 'GROUP-A' if n < 2 else 'GROUP-' + str(n)})
        mod.db.used_transactions.insert_one({'_id': oid(when, 200 + n), 'user_id': 1001,
            'amount': 3.125, 'transaction_id': 'DASH-TX-' + str(n), 'method': 'Binance' if n % 2 else 'TON'})
        mod.db.users.insert_one({'_id': oid(when, 300 + n), 'user_id': 7000 + n, 'balance': 0})
    mod.db.used_transactions.insert_many([
        {'method': 'Admin GIFT', 'amount': 500, 'transaction_id': 'DASH-GIFT'},
        {'method': 'broken amount', 'amount': 1e8, 'transaction_id': 'DASH-INVALID'}])
    mod.db.balance_logs.insert_many([
        {'user_id': 1001, 'type': 'ref_reward', 'amount': 1},
        {'user_id': 1001, 'type': 'ref_clawback', 'amount': -.2},
        {'user_id': 1001, 'type': 'ref_refund', 'amount': .1},
        {'user_id': 1001, 'type': 'compensation', 'amount': .3}])
    mod.db.purchase_rewards.insert_one({'timestamp': int(time.time()), 'amount': .1})
    ep = mod.db.ext_products.insert_one({'base_price': 1.25}).inserted_id
    mod.db.ext_orders.insert_many([
        {'status': 'success', 'price': 8, 'cost_total': 2, 'quantity': 2},
        {'status': 'success', 'price': 3, 'ext_product_id': str(ep), 'quantity': 2},
        {'status': 'pending', 'price': 500, 'cost_total': 1}])


def test_bootstrap_admin_menu_contains_every_existing_section_without_running_reports(mod, server, monkeypatch):
    reports = Mock(side_effect=AssertionError('A bootstrap must never wait for admin statistics'))
    monkeypatch.setattr(mod, '_adm_stats_all', reports)
    status, data, _ = request(server, '/webapp/bootstrap', uid=9001)
    assert status == 200 and data['admin_shell']['is_owner']
    assert [s['cb'] for s in data['admin_shell']['sections']] == [s[4] for s in mod._ADM_SECTIONS]
    assert data['admin_shell']['stats'] == {} and data['admin_shell']['alerts'] == {}
    reports.assert_not_called()


def test_customer_bootstrap_never_contains_admin_metadata(mod, server):
    status, data, _ = request(server, '/webapp/bootstrap', uid=1001)
    assert status == 200 and not data['user']['is_admin']
    assert 'admin_shell' not in data


def test_optimized_four_period_statistics_match_the_original_report_exactly(mod):
    seed_reports(mod)
    mod._ADMIN_PERFORMANCE.invalidate()
    before = mod._adm_stats_all.__wrapped__()
    after = mod._adm_stats_all()
    assert after == before
    assert set(after) == {'d1', 'd7', 'd30', 'y'}
    assert all(after[k]['label_ar'] and after[k]['label_en'] for k in after)
    assert after['d1']['orders'] < mod.db.orders.count_documents({}) + 2
    assert not mod._ADMIN_PERFORMANCE.partial()


def test_original_stock_product_emojis_and_vip_rows_are_preserved_with_a_projection(mod, monkeypatch):
    product(mod, cost=2, custom_emoji_id='5368324170671202286',
            discount_tiers=[{'min_qty': 2, 'price': 4}], description='X' * 100000)
    product(mod, pid='102', is_hidden=True, stock=0)
    product(mod, pid='103', manual=True, stock=0)
    original = mod._adm_products_raw.__wrapped__()
    query = Mock(wraps=mod.db.products.find)
    monkeypatch.setattr(mod.db.products, 'find', query)
    updated = mod._adm_products_raw()
    assert updated == original
    assert updated[0]['emoji_id'] == '5368324170671202286'
    assert query.call_args.args[1] == mod._ADMIN_PERFORMANCE.PRODUCT_FIELDS
    full = list(query({}, None))
    projected = list(query({}, mod._ADMIN_PERFORMANCE.PRODUCT_FIELDS))
    assert sum(len(BSON.encode(row)) for row in projected) < sum(len(BSON.encode(row)) for row in full) / 10


def test_four_period_windows_use_three_queries_and_warm_reads_use_none(mod, monkeypatch):
    seed_reports(mod)
    mod._ADMIN_PERFORMANCE.invalidate()
    operations = []
    for name in ('orders', 'used_transactions', 'users'):
        collection = mod.db[name]
        for method in ('aggregate', 'count_documents'):
            original = getattr(collection, method)
            def measured(*args, original=original, name=name, method=method, **kwargs):
                operations.append((name, method))
                return original(*args, **kwargs)
            monkeypatch.setattr(collection, method, measured)
    mod._adm_stats_all.__wrapped__()
    assert len(operations) == 12
    operations.clear()
    cold = mod._adm_stats_all()
    assert len(operations) == 3 and all(method == 'aggregate' for _, method in operations)
    operations.clear()
    for _ in range(10):
        assert mod._adm_stats_all() == cold
    assert operations == []


def test_simulated_database_latency_is_paid_for_three_windows_not_twelve(mod, monkeypatch):
    seed_reports(mod)
    mod._ADMIN_PERFORMANCE.invalidate()
    for name in ('orders', 'used_transactions', 'users'):
        for method in ('aggregate', 'count_documents'):
            original = getattr(mod.db[name], method)
            def latency(*args, original=original, **kwargs):
                time.sleep(.02)
                return original(*args, **kwargs)
            monkeypatch.setattr(mod.db[name], method, latency)
    start = time.perf_counter(); mod._adm_stats_all.__wrapped__(); before = time.perf_counter() - start
    start = time.perf_counter(); mod._adm_stats_all(); after = time.perf_counter() - start
    assert before > .22 and after < before * .65
    from pathlib import Path
    import json
    (Path(__file__).parents[1] / 'verification/ADMIN-PERFORMANCE-BENCHMARK.json').write_text(json.dumps({
        'scope': 'local memory DB with 20ms injected per database operation; not production latency',
        'period_window_requests_before': 12, 'period_window_requests_after': 3,
        'before_seconds': before, 'after_seconds': after,
        'fresh_repeated_stats_database_queries': 0}, indent=2))


def test_concurrent_admin_reads_share_one_cold_computation(mod, monkeypatch):
    seed_reports(mod)
    mod._ADMIN_PERFORMANCE.invalidate()
    original = mod._ADMIN_PERFORMANCE._windows
    calls = Mock(wraps=original)
    monkeypatch.setattr(mod._ADMIN_PERFORMANCE, '_windows', calls)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: mod._adm_stats_all(), range(16)))
    assert all(value == results[0] for value in results)
    assert calls.call_count == 1
    results[0]['d1']['sales'] = 99999
    assert mod._adm_stats_all()['d1']['sales'] != 99999


def test_new_order_written_by_bot_or_api_invalidates_statistics_immediately(mod):
    seed_reports(mod)
    before = mod._adm_stats_all()['d1']['sales']
    mod.db.orders.insert_one({'user_id': 1001, 'product_id': '101', 'price': 9, 'cost_total': 2})
    assert mod._adm_stats_all()['d1']['sales'] == before + 9


@pytest.mark.parametrize('collection,document', [
    ('used_transactions', {'user_id': 1001, 'method': 'LTC', 'amount': 5, 'transaction_id': 'DASH-NEW'}),
    ('balance_logs', {'user_id': 1001, 'type': 'ref_reward', 'amount': .1}),
    ('purchase_rewards', {'user_id': 1001, 'amount': .1, 'timestamp': int(time.time())}),
    ('ext_orders', {'user_id': 1001, 'status': 'success', 'price': 4, 'cost_total': 2}),
    ('users', {'user_id': 8001, 'balance': 0})])
def test_report_source_writes_invalidate_the_cache(mod, collection, document):
    mod._adm_stats_all()
    assert mod._ADMIN_PERFORMANCE.cache is not None
    mod.db[collection].insert_one(document)
    assert mod._ADMIN_PERFORMANCE.cache is None


def test_existing_user_balance_or_name_updates_do_not_force_a_join_report_scan(mod):
    mod._adm_stats_all()
    cached = mod._ADMIN_PERFORMANCE.cache
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': 105., 'name': 'Renamed'}})
    assert mod._ADMIN_PERFORMANCE.cache is cached


def test_upserting_a_new_user_invalidates_join_counts(mod):
    mod._adm_stats_all()
    mod.db.users.update_one({'user_id': 8001}, {'$setOnInsert': {'balance': 0}}, upsert=True)
    assert mod._ADMIN_PERFORMANCE.cache is None


def test_product_cost_and_report_timezone_changes_invalidate_but_visibility_does_not(mod):
    seed_reports(mod)
    mod._adm_stats_all()
    cached = mod._ADMIN_PERFORMANCE.cache
    mod.db.products.update_one({'id': '101'}, {'$set': {'is_hidden': True}})
    assert mod._ADMIN_PERFORMANCE.cache is cached
    mod.db.products.update_one({'id': '101'}, {'$set': {'cost': 3}})
    assert mod._ADMIN_PERFORMANCE.cache is None
    mod._adm_stats_all()
    mod.db.settings.update_one({'key': 'report_tz'}, {'$set': {'value': 0}}, upsert=True)
    assert mod._ADMIN_PERFORMANCE.cache is None


def test_stock_alerts_and_permission_flags_are_live_even_with_cached_statistics(mod, server):
    product(mod, stock=1)
    first = request(server, '/webapp/admin/home', uid=9001)[1]
    assert first['is_owner'] and len(first['alerts']['low']) == 1
    mod.db.product_stock.update_many({'product_id': '101'}, {'$set': {'is_sold': True}})
    second = request(server, '/webapp/admin/home', uid=9001)[1]
    assert second['alerts']['oos_n'] == 1 and not second['alerts']['low']
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'is_admin': 1}})
    assert not request(server, '/webapp/admin/home', uid=1001)[1]['is_owner']
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'is_admin': 0}})
    assert request(server, '/webapp/admin/home', uid=1001)[0] == 403


def test_manual_refresh_bypasses_report_cache_but_unauthorized_refresh_does_not(mod, server, monkeypatch):
    request(server, '/webapp/admin/home', uid=9001)
    calls = Mock(wraps=mod._ADMIN_PERFORMANCE._windows)
    monkeypatch.setattr(mod._ADMIN_PERFORMANCE, '_windows', calls)
    assert request(server, '/webapp/admin/home', {'refresh': True}, uid=1001)[0] == 403
    assert calls.call_count == 0
    assert request(server, '/webapp/admin/home', {'refresh': True}, uid=9001)[0] == 200
    assert calls.call_count == 1
    assert request(server, '/webapp/admin/home', uid=9001, auth=False)[0] == 401


def test_cache_expiry_recomputes_within_thirty_seconds(mod, monkeypatch):
    clock = [1000.]
    monkeypatch.setattr(mod._admin_perf_module.time, 'monotonic', lambda: clock[0])
    mod._ADMIN_PERFORMANCE.invalidate()
    original = mod._ADMIN_PERFORMANCE._windows
    calls = Mock(wraps=original)
    monkeypatch.setattr(mod._ADMIN_PERFORMANCE, '_windows', calls)
    mod._adm_stats_all()
    clock[0] += 29
    mod._adm_stats_all()
    assert calls.call_count == 1
    clock[0] += 2
    mod._adm_stats_all()
    assert calls.call_count == 2


def test_a_write_during_a_slow_report_is_not_blocked_or_hidden_by_a_stale_cache(mod, monkeypatch):
    seed_reports(mod)
    mod._ADMIN_PERFORMANCE.invalidate()
    entered, release = threading.Event(), threading.Event()
    original = mod._ADMIN_PERFORMANCE._windows
    def blocked(periods):
        entered.set()
        assert release.wait(3)
        return original(periods)
    monkeypatch.setattr(mod._ADMIN_PERFORMANCE, '_windows', blocked)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(mod._adm_stats_all)
        assert entered.wait(2)
        start = time.perf_counter()
        mod.db.orders.insert_one({'product_id': '101', 'price': 5, 'cost_total': 2, 'user_id': 1001})
        assert time.perf_counter() - start < .5
        release.set(); future.result(timeout=3)
    assert mod._ADMIN_PERFORMANCE.cache is None
    monkeypatch.setattr(mod._ADMIN_PERFORMANCE, '_windows', original)
    mod._adm_stats_all()
    assert mod._ADMIN_PERFORMANCE.cache is not None


def test_failed_statistics_are_marked_partial_and_never_cached_as_zeroes(mod, server, monkeypatch):
    mod._ADMIN_PERFORMANCE.invalidate()
    original = mod.db.orders.aggregate
    monkeypatch.setattr(mod.db.orders, 'aggregate', Mock(side_effect=RuntimeError('Temporary read failure')))
    response = request(server, '/webapp/admin/home', uid=9001)[1]
    assert response['ok'] and response['stats_partial']
    assert response['sections'] and mod._ADMIN_PERFORMANCE.cache is None
    monkeypatch.setattr(mod.db.orders, 'aggregate', original)
    assert not request(server, '/webapp/admin/home', uid=9001)[1]['stats_partial']


def test_statistics_cache_is_read_only_and_does_not_modify_financial_or_referral_records(mod):
    seed_reports(mod)
    before = {name: copy.deepcopy(list(mod.db[name].find({}))) for name in mod.db.list_collection_names()}
    mod._adm_stats_all(); mod._adm_stats_all()
    assert before == {name: list(mod.db[name].find({})) for name in before}
