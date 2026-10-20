"""Confirmed inventory, folder counts and manual overrides; isolated database, no supplier calls."""
import ast
import functools
import time
from unittest.mock import Mock

import pytest
from bson import ObjectId
from harness import MESSAGES, APP
from test_integration import mod, isolate, server, request, product
from test_products_menu import callback, dispatch


@functools.lru_cache(maxsize=1)
def real_seat_cache_code():
    tree = ast.parse((APP / 'bot.py').read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_cgpt_get_seats_cached')
    return compile(ast.Module(body=[node], type_ignores=[]), '<production-seat-cache>', 'exec')


@pytest.fixture(autouse=True)
def visibility_state(mod, monkeypatch):
    mod._FOLDER_INDEX.update(expires=0, aliases={}, hidden={})
    monkeypatch.setitem(mod._CGPT_DISPLAY_SEATS, 'value', None)
    monkeypatch.setitem(mod._CGPT_DISPLAY_SEATS, 'confirmed_until', 0)
    monkeypatch.setitem(mod._CGPT_SEATS_CACHE, 'exp', 0)
    monkeypatch.setitem(mod._CGPT_SEATS_CACHE, 'confirmed_until', 0)
    monkeypatch.setattr(mod, '_cgpt_display_seats', lambda: None)


def folder(mod, ids=(), **extra):
    doc = {'name_ar': 'مجلد', 'name_en': 'Folder', 'product_ids': list(ids), 'order': 1, **extra}
    mod.db.catalogs.insert_one(doc)
    return doc


def test_three_days_means_continuous_confirmed_outage_not_product_age(mod):
    p = product(mod, stock=0)
    start = int(time.time())
    assert mod._stock_visibility_tick(start) == 1
    row = mod.db.products.find_one({'_id': p['_id']})
    assert row['stock_oos_since'] == start and not row['is_hidden']
    assert mod._stock_visibility_tick(start + 3 * 86400 - 1) == 0
    assert mod._stock_visibility_tick(start + 3 * 86400) == 1
    row = mod.db.products.find_one({'_id': p['_id']})
    assert row['is_hidden'] and row['stock_auto_hidden']
    assert mod._stock_visibility_tick(start + 4 * 86400) == 0
    assert not mod.db.orders.count_documents({}) and not mod.db.balance_logs.count_documents({})


@pytest.mark.parametrize('kind', ['reg', 'ext'])
def test_restart_uses_saved_outage_and_restock_unhides_automatically(mod, kind):
    start = int(time.time()) - 4 * 86400
    if kind == 'reg':
        p = product(mod, stock=0, stock_oos_since=start)
        coll = mod.db.products
    else:
        p = {'name': 'API product', 'stock': 0, 'hidden': False, 'stock_oos_since': start}
        coll = mod.db.ext_products; coll.insert_one(p)
    mod._stock_visibility_tick()
    assert coll.find_one({'_id': p['_id']}).get('stock_auto_hidden')
    if kind == 'reg':
        mod.db.product_stock.insert_one({'product_id': p['id'], 'is_sold': False, 'code_line': 'LOCAL TEST'})
    else:
        coll.update_one({'_id': p['_id']}, {'$set': {'stock': 1}})
    restored = coll.find_one({'_id': p['_id']})
    assert not restored.get('hidden' if kind == 'ext' else 'is_hidden')
    assert 'stock_oos_since' not in restored and not restored.get('stock_auto_hidden')


def test_brief_restock_resets_the_clock_even_if_sold_before_next_sweep(mod):
    p = product(mod, stock=0, stock_oos_since=int(time.time()) - 2 * 86400)
    st = {'product_id': p['id'], 'is_sold': False}; mod.db.product_stock.insert_one(st)
    mod.db.product_stock.update_one({'_id': st['_id']}, {'$set': {'is_sold': True}})
    now = int(time.time()); mod._stock_visibility_tick(now)
    row = mod.db.products.find_one({'_id': p['_id']})
    assert row['stock_oos_since'] == now and not row['is_hidden']


def test_returning_reserved_stock_counts_as_restock(mod):
    p = product(mod, stock=0, is_hidden=True, stock_auto_hidden=True,
                stock_oos_since=int(time.time()) - 4 * 86400)
    mod.db.product_stock.insert_one({'product_id': p['id'], 'is_sold': True, 'reservation_id': 'fake'})
    mod.db.product_stock.update_many({'reservation_id': 'fake'},
        {'$set': {'is_sold': False}, '$unset': {'reservation_id': ''}})
    assert not mod.db.products.find_one({'_id': p['_id']})['is_hidden']


@pytest.mark.parametrize('kind', ['reg', 'ext', 'cgpt'])
def test_manual_hide_stays_hidden_and_manual_show_overrides_old_outage(mod, kind, monkeypatch):
    if kind == 'reg':
        p = product(mod, stock=0); coll = mod.db.products
    elif kind == 'ext':
        p = {'name': 'API', 'stock': 0}; coll = mod.db.ext_products; coll.insert_one(p)
    else:
        p = {'name': 'Business'}; coll = mod.db.cgpt_products; coll.insert_one(p)
        monkeypatch.setitem(mod._CGPT_DISPLAY_SEATS, 'value', 0)
        monkeypatch.setitem(mod._CGPT_DISPLAY_SEATS, 'confirmed_until', time.time() + 1000)
    field = 'hidden' if kind == 'ext' else 'is_hidden'
    old = int(time.time()) - 4 * 86400
    coll.update_one({'_id': p['_id']}, {'$set': {'stock_oos_since': old}})
    mod._stock_visibility_tick()
    mod._set_product_visibility(coll, coll.find_one({'_id': p['_id']}), True)
    mod._stock_visibility_observe(coll, coll.find_one({'_id': p['_id']}), True)
    assert coll.find_one({'_id': p['_id']})[field]
    coll.update_one({'_id': p['_id']}, {'$set': {'stock_oos_since': old}})
    mod._set_product_visibility(coll, coll.find_one({'_id': p['_id']}), False)
    mod._stock_visibility_tick()
    assert not coll.find_one({'_id': p['_id']})[field]


def test_unknown_external_stock_and_unconfirmed_stale_seats_do_not_start_or_hide(mod, monkeypatch):
    cp = {'name': 'Business', 'stock_oos_since': int(time.time()) - 4 * 86400}
    mod.db.cgpt_products.insert_one(cp)
    for stock in [None, 'unknown', float('nan')]:
        mod.db.ext_products.insert_one({'stock': stock})
    monkeypatch.setitem(mod._CGPT_DISPLAY_SEATS, 'value', 0)
    monkeypatch.setitem(mod._CGPT_DISPLAY_SEATS, 'expires', time.time() + 1000)
    mod._stock_visibility_tick()
    assert not mod.db.cgpt_products.find_one({'_id': cp['_id']}).get('is_hidden')
    assert all('stock_oos_since' not in p for p in mod.db.ext_products.find())


def test_inventory_db_failure_aborts_before_any_hide_or_reset(mod, monkeypatch):
    p = product(mod, stock=0, stock_oos_since=1)
    before = mod.db.products.find_one({'_id': p['_id']})
    monkeypatch.setattr(mod.db.product_stock, 'aggregate', Mock(side_effect=RuntimeError('test unavailable')))
    with pytest.raises(RuntimeError): mod._stock_visibility_tick()
    assert mod.db.products.find_one({'_id': p['_id']}) == before


def test_sweep_reads_only_grouped_stock_ids_not_product_bodies_or_provider_http(mod, monkeypatch):
    product(mod, stock=0)
    monkeypatch.setattr(mod.db.products, 'find', Mock(side_effect=AssertionError('full product download')))
    monkeypatch.setattr(mod.db.ext_products, 'find', Mock(side_effect=AssertionError('external product download')))
    monkeypatch.setattr(mod, '_cgpt_display_seats', Mock(side_effect=AssertionError('provider refresh')))
    mod._stock_visibility_tick()


def test_empty_all_hidden_deleted_and_duplicate_alias_folders_have_exact_public_counts(mod, server):
    p = product(mod); hidden = product(mod, '102', is_hidden=True)
    live = folder(mod, [p['id'], int(p['id']), p['_id'], str(p['_id']), hidden['id'], 'deleted'])
    empty = folder(mod); all_hidden = folder(mod, [hidden['id'], 'deleted'])
    status, data, _ = request(server, '/webapp/catalog')
    assert status == 200
    assert [(c['id'], c['count']) for c in data['catalogs']] == [(str(live['_id']), 1)]
    assert mod._catalog_visible_count(live) == 1
    assert mod.catalog_view_helper(1001, 1001, str(live['_id']), 'en')
    markup = MESSAGES[-1][2]['reply_markup'].to_dict()
    product_buttons = [b for row in markup['inline_keyboard'] for b in row if b.get('callback_data', '').startswith('vi_p_')]
    assert len(product_buttons) == 1
    admin = mod._wa_build_catalog(True)
    assert {c['id'] for c in admin['catalogs']} == {str(c['_id']) for c in [live, empty, all_hidden]}


def test_external_products_and_multiple_cgpt_links_count_once(mod, monkeypatch):
    cp = {'name': 'Business', 'durations': [{'dur_id': 'd', 'minutes': 1440, 'price': 5}]}
    mod.db.cgpt_products.insert_one(cp); hexid = str(cp['_id'])
    p1 = product(mod, '201', stock=0, product_type='cgpt_main', cgpt_product_id=hexid)
    p2 = product(mod, '202', stock=0, product_type='cgpt_main', cgpt_product_id=hexid)
    cat = folder(mod, [p1['id'], p1['_id'], p2['id'], hexid, 'cgpt_' + hexid])
    mod.db.ext_products.insert_one({'name': 'API', 'stock': 1, 'catalog_id': str(cat['_id'])})
    data = mod._wa_build_catalog(False)
    assert len(data['products']) == 2
    assert data['catalogs'][0]['count'] == 2
    assert mod._catalog_visible_count(cat) == 1
    assert mod._catalog_ext_counts([str(cat['_id'])]) == {str(cat['_id']): 1}


def test_auto_hide_last_product_hides_folder_and_restock_restores_both(mod):
    p = product(mod, stock=0, stock_oos_since=int(time.time()) - 4 * 86400)
    cat = folder(mod, [p['id']])
    mod._stock_visibility_tick()
    assert not mod._wa_build_catalog(False)['catalogs']
    mod.db.product_stock.insert_one({'product_id': p['id'], 'is_sold': False})
    assert mod._wa_build_catalog(False)['catalogs'][0]['id'] == str(cat['_id'])


def test_manual_folder_hide_blocks_direct_site_bot_and_api_purchase_without_charging(mod, server):
    p = product(mod); cat = folder(mod, [p['id']])
    mod.db.catalogs.update_one({'_id': cat['_id']}, {'$set': {'is_hidden': True}})
    assert not mod._wa_build_catalog(False)['products']
    assert not mod._shop_items(1001)
    assert not mod._net_api_products_payload(1001)['products']
    assert request(server, '/webapp/buy', {'product_id': p['id']})[0] == 404
    mod._do_purchase(1001, p['id'], 1, 'en')
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    assert mod.db.product_stock.count_documents({'is_sold': False}) == 3
    assert not mod.db.orders.count_documents({})
    assert not mod.catalog_view_helper(1001, 1001, str(cat['_id']), 'en')


def test_product_in_two_folders_remains_visible_in_the_public_folder(mod):
    p = product(mod); hidden = folder(mod, [p['id']], is_hidden=True); public = folder(mod, [p['_id']])
    cat = mod._wa_build_catalog(False)
    assert cat['products'][0]['cats'] == [str(public['_id'])]
    assert mod._customer_product_visible(p)
    assert [c['id'] for c in cat['catalogs']] == [str(public['_id'])]


def test_admin_folder_toggle_and_bulk_controls_preserve_products_and_empty_folders(mod):
    p = product(mod); a = folder(mod, [p['id']]); b = folder(mod)
    dispatch(mod, 'ad_cat_visibility_' + str(a['_id']), 1001)
    assert not mod.db.catalogs.find_one({'_id': a['_id']}).get('is_hidden')
    dispatch(mod, 'ad_cat_visibility_' + str(a['_id']), 9001)
    assert mod.db.catalogs.find_one({'_id': a['_id']})['is_hidden']
    dispatch(mod, 'ad_cat_visibility_all_hide', 9001)
    assert mod.db.catalogs.count_documents({'is_hidden': True}) == 2
    dispatch(mod, 'ad_cat_visibility_all_show', 9001)
    assert mod.db.catalogs.count_documents({'is_hidden': True}) == 0
    assert len(mod._wa_build_catalog(False)['catalogs']) == 1
    assert mod.db.products.count_documents({}) == 1 and mod.db.catalogs.count_documents({}) == 2


def test_manual_unhide_cgpt_after_auto_hide_restores_parent_and_store_link(mod, monkeypatch):
    cp = {'name': 'Business', 'is_hidden': True, 'stock_auto_hidden': True,
          'stock_oos_since': 1, 'durations': [{'dur_id': 'd', 'minutes': 1440, 'price': 5}]}
    mod.db.cgpt_products.insert_one(cp)
    p = product(mod, 'cgpt_main_' + str(cp['_id']), stock=0, product_type='cgpt_main',
                cgpt_product_id=str(cp['_id']), is_hidden=True, stock_auto_hidden=True, stock_oos_since=1)
    mod._set_product_visibility(mod.db.products, p, False)
    assert mod._cgpt_product_visible(mod.db.cgpt_products.find_one({'_id': cp['_id']}))
    assert len(mod._wa_build_catalog(False)['products']) == 1


@pytest.mark.parametrize('report,confirmed', [
    ({'connected': False}, False), ({'connected': True, 'available_seats': None}, False),
    ({'connected': True, 'available_seats': 0}, True),
    ({'connected': True, 'available_seats': 2}, True)])
def test_provider_connection_failure_or_unknown_capacity_is_not_confirmed_zero(mod, monkeypatch, report, confirmed):
    cp = {'name': 'Business', 'stock_oos_since': int(time.time()) - 4 * 86400}
    mod.db.cgpt_products.insert_one(cp)
    monkeypatch.setattr(mod, '_cgpt_all_accounts', lambda: [{'_id': 'local-test-account'}])
    monkeypatch.setattr(mod, '_cgpt_account_seat_info', lambda *a, **k: report)
    # The shared harness intentionally replaces this one function with a fixed seat count.
    # Exercise its production definition with the same isolated globals and mocked provider.
    local = {}; exec(real_seat_cache_code(), mod.__dict__, local)
    assert local['_cgpt_get_seats_cached']() == (report.get('available_seats') or 0)
    assert bool(mod._CGPT_SEATS_CACHE['confirmed_until']) == confirmed
    mod._stock_visibility_tick()
    row = mod.db.cgpt_products.find_one({'_id': cp['_id']})
    assert bool(row.get('is_hidden')) == (confirmed and report['available_seats'] == 0)
    if not confirmed:
        assert row['stock_oos_since'] == cp['stock_oos_since']


def test_bulk_supplier_restock_with_changed_filter_and_keywords_restores_all_auto_hidden_items(mod):
    mod.db.ext_products.insert_many([{'stock': 0, 'hidden': True, 'stock_auto_hidden': True,
        'stock_oos_since': 1} for _ in range(2)])
    mod.db.ext_products.update_many(filter={'stock': 0}, update={'$set': {'stock': 3}})
    assert all(not row['hidden'] and 'stock_oos_since' not in row for row in mod.db.ext_products.find())


def test_auxiliary_metadata_failure_does_not_block_returning_inventory(mod, monkeypatch):
    p = product(mod, stock=0, is_hidden=True, stock_auto_hidden=True, stock_oos_since=1)
    st = {'product_id': p['id'], 'is_sold': True}; mod.db.product_stock.insert_one(st)
    monkeypatch.setattr(mod.db.product_stock, 'distinct', Mock(side_effect=RuntimeError('fixture metadata read failed')))
    mod.db.product_stock.update_one({'_id': st['_id']}, {'$set': {'is_sold': False}})
    assert mod.db.product_stock.find_one({'_id': st['_id']})['is_sold'] is False
    mod._stock_visibility_tick()
    assert not mod.db.products.find_one({'_id': p['_id']})['is_hidden']


def test_hiding_folder_invalidates_already_warm_reseller_catalogue(mod):
    p = product(mod); cat = folder(mod, [p['id']])
    assert mod._net_api_products_payload(1001)['products']
    mod.db.catalogs.update_one({'_id': cat['_id']}, {'$set': {'is_hidden': True}})
    assert not mod._net_api_products_payload(1001)['products']


@pytest.mark.parametrize('external', [False, True])
def test_folder_hidden_after_cart_quote_cannot_be_purchased_from_stale_cart(mod, server, external):
    if external:
        cat = folder(mod)
        p = {'name': 'API', 'stock': 3, 'sell_price': 5, 'catalog_id': str(cat['_id'])}
        mod.db.ext_products.insert_one(p); pid = 'ext_' + str(p['_id'])
    else:
        p = product(mod); cat = folder(mod, [p['id']]); pid = p['id']
    body = {'items': [{'product_id': pid, 'qty': 1}]}
    status, quote, _ = request(server, '/webapp/cart/quote', body)
    assert status == 200 and quote['valid']
    mod.db.catalogs.update_one({'_id': cat['_id']}, {'$set': {'is_hidden': True}})
    status, hidden, _ = request(server, '/webapp/cart/quote', body)
    assert status == 200 and not hidden['valid'] and hidden['lines'][0]['error'] == 'not_found'
    status, checkout, _ = request(server, '/webapp/cart/checkout', {'quote_id': quote['quote_id']})
    assert status == 409
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    assert not mod.db.orders.count_documents({}) and not mod.db.ext_orders.count_documents({})


def test_hidden_folder_stops_old_business_confirmation_before_debit_or_provider_call(mod, monkeypatch):
    cp = {'name': 'Business'}; mod.db.cgpt_products.insert_one(cp)
    folder(mod, ['cgpt_' + str(cp['_id'])], is_hidden=True)
    mod._cgpt_pending[1001] = {'cgpt_pid': str(cp['_id']), 'dur_id': 'd', 'label': 'Day',
        'price': 5, 'minutes': 1440, 'order_id': 'CGLOCAL', 'p_name': 'Business', 'email': 'test@example.test'}
    provider = Mock(side_effect=AssertionError('Hidden product must not contact provider'))
    monkeypatch.setattr(mod, '_cgpt_find_available_account', provider)
    mod.cgpt_email_confirmed(callback(mod, 'cgpt_email_ok_1001_CGLOCAL'))
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    assert not mod.db.orders.count_documents({})
    provider.assert_not_called()
