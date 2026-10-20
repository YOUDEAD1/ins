"""Historical outages and identical owner/customer storefront visibility; no live integrations."""
import datetime as dt
import time
from unittest.mock import Mock

import pytest
from bson import ObjectId
from harness import MESSAGES, reply
from test_integration import mod, isolate, server, request, product
from test_round37_visibility import folder, visibility_state
from test_products_menu import callback, dispatch, buttons


@pytest.fixture(autouse=True)
def state(mod, isolate, visibility_state, monkeypatch):
    mod._STOCK_VISIBILITY_STATE.update(last_success=0, retry_at=0)
    monkeypatch.setitem(mod._VC_ORIG, 'edit_message_text',
        lambda text, chat_id, message_id, **kw: reply(chat_id, text, **kw))


def oid(stamp):
    return ObjectId.from_datetime(dt.datetime.fromtimestamp(stamp, dt.timezone.utc))


@pytest.mark.parametrize('field', ['_supplier_empty_since', '_missing_since'])
@pytest.mark.parametrize('unit', ['seconds', 'milliseconds', 'iso', 'datetime'])
def test_legacy_supplier_outage_hides_immediately_without_waiting_three_more_days(mod, field, unit):
    now = int(time.time()); old = now - 4 * 86400
    value = {'seconds': old, 'milliseconds': old * 1000,
             'iso': dt.datetime.fromtimestamp(old, dt.timezone.utc).isoformat(),
             'datetime': dt.datetime.fromtimestamp(old, dt.timezone.utc)}[unit]
    p = {'name': 'Old supplier product', 'stock': 0, field: value,
         'stock_oos_since': now - 10}  # A short v37 timer must not conceal the older record.
    mod.db.ext_products.insert_one(p)
    mod._stock_visibility_ensure()
    saved = mod.db.ext_products.find_one({'_id': p['_id']})
    assert saved['hidden'] and saved['stock_auto_hidden']
    assert saved['stock_oos_since'] == old and saved['stock_oos_source'] == 'supplier_history'


@pytest.mark.parametrize('days', [2, 4])
def test_local_sold_inventory_and_last_completed_sale_seed_the_real_outage(mod, days):
    now = int(time.time()); sold = now - days * 86400
    p = product(mod, stock=0)
    mod.db.product_stock.insert_one({'_id': oid(now - 7 * 86400), 'product_id': 101,
                                    'is_sold': True, 'sale_order_id': 'LOCAL-HISTORY'})
    mod.db.orders.insert_one({'_id': oid(sold), 'product_id': '101', 'user_id': 1001,
                             'date': dt.datetime.fromtimestamp(sold, dt.timezone.utc), 'order_id': 'LOCAL-HISTORY'})
    before = mod.db.users.find_one({'user_id': 1001})
    mod._stock_visibility_ensure()
    saved = mod.db.products.find_one({'_id': p['_id']})
    assert saved['stock_oos_since'] == sold
    assert saved['stock_oos_source'] == 'completed_sales'
    assert bool(saved.get('is_hidden')) == (days >= 3)
    assert mod.db.users.find_one({'user_id': 1001}) == before


def test_old_creation_time_alone_never_proves_three_days_without_stock(mod):
    now = int(time.time())
    p = product(mod, stock=0, _id=oid(now - 10 * 86400), created_at=now - 10 * 86400)
    mod._stock_visibility_ensure()
    saved = mod.db.products.find_one({'_id': p['_id']})
    assert not saved['is_hidden'] and saved['stock_oos_since'] >= now
    assert saved['stock_oos_source'] == 'observed'


def test_newer_inventory_without_matching_completed_sale_does_not_inherit_an_old_sale(mod):
    now = int(time.time()); p = product(mod, stock=0)
    mod.db.product_stock.insert_one({'_id': oid(now - 86400), 'product_id': '101', 'is_sold': True})
    mod.db.orders.insert_one({'_id': oid(now - 7 * 86400), 'product_id': '101', 'user_id': 1001})
    mod._stock_visibility_ensure()
    saved = mod.db.products.find_one({'_id': p['_id']})
    assert not saved['is_hidden'] and saved['stock_oos_source'] == 'observed'


def test_in_progress_reservation_cannot_be_backdated_using_an_old_purchase(mod):
    now = int(time.time()); p = product(mod, stock=0)
    mod.db.product_stock.insert_one({'_id': oid(now - 7 * 86400), 'product_id': '101',
                                    'is_sold': True, 'reservation_id': 'IN-PROGRESS'})
    mod.db.orders.insert_one({'_id': oid(now - 4 * 86400), 'product_id': '101', 'user_id': 1001})
    mod._stock_visibility_ensure()
    assert not mod.db.products.find_one({'_id': p['_id']})['is_hidden']


def test_restock_invalidates_older_supplier_outage_evidence(mod):
    now = int(time.time())
    p = {'stock': 0, '_supplier_empty_since': now - 8 * 86400, 'stock_oos_since': now - 8 * 86400}
    mod.db.ext_products.insert_one(p)
    mod.db.ext_products.update_one({'_id': p['_id']}, {'$set': {'stock': 3}})
    mod.db.ext_products.update_one({'_id': p['_id']}, {'$set': {'stock': 0}})
    mod._stock_visibility_ensure()
    saved = mod.db.ext_products.find_one({'_id': p['_id']})
    assert not saved.get('hidden') and saved['stock_oos_since'] >= now


@pytest.mark.parametrize('mode', ['folders', 'flat'])
@pytest.mark.parametrize('uid', [1001, 9001])
def test_owner_and_customer_both_see_only_public_products_and_nonempty_folders(mod, uid, mode):
    visible = product(mod)
    expired = product(mod, '102', stock=0, stock_oos_since=int(time.time()) - 4 * 86400)
    live = folder(mod, [visible['id']]); gone = folder(mod, [expired['id']]); empty = folder(mod)
    mod.db.users.update_one({'user_id': uid}, {'$set': {'shop_display_mode': mode}})
    dispatch(mod, 'open_shop', uid)
    keys = [b['callback_data'] for row in buttons(MESSAGES[-1]) for b in row if b.get('callback_data')]
    assert 'cat_' + str(empty['_id']) not in keys and 'cat_' + str(gone['_id']) not in keys
    assert not any('102' in key for key in keys)
    assert ('cat_' + str(live['_id']) in keys) if mode == 'folders' else any('101' in key for key in keys)
    assert all('102' not in row['cb'] for row in mod._shop_items(uid, is_admin=True))
    # Hidden items and empty folders remain available in the actual management screens.
    snapshot = mod._wa_build_catalog(True)
    assert {'101', '102'} <= {p['id'] for p in snapshot['products']}
    assert len(snapshot['catalogs']) == 3


def test_direct_owner_folder_link_uses_same_visibility_and_exact_count(mod):
    a = product(mod); b = product(mod, '102', is_hidden=True)
    cat = folder(mod, [a['id'], b['id']])
    assert mod.catalog_view_helper(9001, 9001, str(cat['_id']), 'en')
    keys = [b['callback_data'] for row in buttons(MESSAGES[-1]) for b in row if b.get('callback_data')]
    assert sum(key.startswith('vi_p_') for key in keys) == 1
    assert not any('102' in key for key in keys)


def test_first_site_bootstrap_catches_legacy_empty_folder_before_background_worker(mod, server):
    old = int(time.time()) - 4 * 86400
    cat = folder(mod)
    mod.db.ext_products.insert_one({'name': 'Old', 'stock': 0, 'catalog_id': str(cat['_id']), '_missing_since': old})
    status, data, _ = request(server, '/webapp/bootstrap', uid=9001)
    assert status == 200 and not data['products'] and not data['catalogs']


def test_on_open_catchup_is_throttled_and_never_calls_supplier(mod, monkeypatch):
    product(mod, stock=0)
    supplier = Mock(side_effect=AssertionError('No supplier requests in visibility sweep'))
    monkeypatch.setattr(mod, '_cgpt_display_seats', supplier)
    spy = Mock(wraps=mod._stock_visibility_tick); monkeypatch.setattr(mod, '_stock_visibility_tick', spy)
    mod._stock_visibility_ensure(); mod._stock_visibility_ensure(); mod._stock_visibility_ensure()
    assert spy.call_count == 1
    supplier.assert_not_called()


def test_repeated_outage_history_reads_do_not_rewrite_unchanged_metadata(mod):
    p = {'stock': 0, '_missing_since': int(time.time()) - 4 * 86400}; mod.db.ext_products.insert_one(p)
    mod._stock_visibility_tick()
    snapshot = mod.db.ext_products.find_one({'_id': p['_id']})
    assert mod._stock_visibility_tick() == 0
    assert mod.db.ext_products.find_one({'_id': p['_id']}) == snapshot
