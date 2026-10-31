"""Regression tests for the money-path fixes (audit of b979036). Offline: mongomock, stubbed Telegram/supplier."""
import json
import types

import telebot

from test_integration import mod, isolate, server, product, request  # noqa: F401
from harness import reply


def _blocked(fn):
    return telebot.apihelper.ApiTelegramException(
        fn, None, {'ok': False, 'error_code': 403, 'description': 'Forbidden: bot was blocked by the user'})


# ── A buyer who blocks the bot must not get a refund for codes already readable in order history ──

def test_blocked_buyer_keeps_sale_and_codes_are_not_resold(mod, server, monkeypatch):
    product(mod, price=5., stock=2)
    leaked = []
    orig_doc, orig_msg = mod._VC_ORIG['send_document'], mod._VC_ORIG['send_message']

    def send_document(cid, doc, *a, **kw):
        if int(cid) == 1001:
            s, d, _ = request(server, '/webapp/orders', {}, uid=1001)
            for g in d.get('orders', []):
                leaked.extend(g.get('codes') or [])
            raise _blocked('sendDocument')
        return orig_doc(cid, doc, *a, **kw)

    def send_message(cid, text='', *a, **kw):
        if int(cid) == 1001:
            raise _blocked('sendMessage')
        return orig_msg(cid, text, *a, **kw)

    monkeypatch.setitem(mod._VC_ORIG, 'send_document', send_document)
    monkeypatch.setitem(mod._VC_ORIG, 'send_message', send_message)
    s, d, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 2}, uid=1001)
    assert s == 200 and sorted(d['codes']) == ['101-SECRET-0', '101-SECRET-1']
    assert mod._wa_balance(1001) == 90.0                       # charged, not refunded
    assert mod.db.product_stock.count_documents({'is_sold': False}) == 0
    monkeypatch.setitem(mod._VC_ORIG, 'send_document', orig_doc)
    monkeypatch.setitem(mod._VC_ORIG, 'send_message', orig_msg)
    s2, d2, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 2}, uid=1002)
    assert s2 != 200 and not set(d2.get('codes') or []) & set(leaked)
    assert mod._wa_balance(1002) == 50.0


# ── Supplier-synced negative / overflowing prices make the product unpurchasable ──

def _ext_setup(mod):
    sid = mod.db.ext_stores.insert_one({'name': 'Dummy Supplier', 'base_url': 'https://supplier.invalid',
                                        'api_key': 'dummy'}).inserted_id
    eid = mod.db.ext_products.insert_one({
        'name': 'Remote Item', 'store_id': str(sid), 'ext_id': '25', 'stock': 10,
        'sell_price': 4.0, 'base_price': 2.0, 'cost_price': 2.0,
        'markup_type': 'percent', 'markup_value': 100, '_supplier_available': True}).inserted_id
    return sid, eid


def _sync_with(mod, monkeypatch, raw_json):
    data = json.loads(raw_json)
    monkeypatch.setattr(mod, '_net_sync_fetch', lambda store: data)
    mod._auto_sync_ext_stores()


def _supplier(mod, monkeypatch):
    calls = []

    def supplier_order(store, path, body, idem_key=None):
        calls.append(body)
        return True, {'order': {'id': 'EXT-1', 'status': 'completed', 'codes': ['SUPPLIER-CODE-1']}}
    monkeypatch.setattr(mod, '_ext_api_post', supplier_order)
    return calls


def test_supplier_negative_cost_is_ignored_and_product_hidden(mod, server, monkeypatch):
    sid, eid = _ext_setup(mod)
    _sync_with(mod, monkeypatch, '{"products":[{"id":"25","name":"Remote Item","price":-10,"cost":-10,"stock":10}]}')
    ep = mod.db.ext_products.find_one({'_id': eid})
    assert ep['sell_price'] == 4.0 and ep.get('hidden') is True
    calls = _supplier(mod, monkeypatch)
    s, d, _ = request(server, '/webapp/buy', {'product_id': f'ext_{eid}', 'qty': 1}, uid=1001)
    assert s != 200 and mod._wa_balance(1001) == 100.0 and calls == []


def test_invalid_stored_ext_price_cannot_be_bought_on_any_path(mod, server, monkeypatch):
    for bad in (-20.0, float('inf'), 0.0):
        mod.db.ext_products.delete_many({})
        sid, eid = _ext_setup(mod)
        mod.db.ext_products.update_one({'_id': eid}, {'$set': {'sell_price': bad}})
        calls = _supplier(mod, monkeypatch)
        s, d, _ = request(server, '/webapp/buy', {'product_id': f'ext_{eid}', 'qty': 3}, uid=1001)
        assert s != 200, bad
        s, q, _ = request(server, '/webapp/cart/quote', {'items': [{'product_id': f'ext_{eid}', 'qty': 2}]}, uid=1001)
        assert not q.get('valid'), bad
        out = mod._ext_execute_buy(types.SimpleNamespace(from_user=types.SimpleNamespace(id=1001), text='1',
                                                         chat=types.SimpleNamespace(id=1001)), str(eid), 'en')
        assert isinstance(out, dict) and out.get('ok') is False, bad
        assert mod._wa_balance(1001) == 100.0 and calls == [], bad


# ── GitHub activation refunds at most once and never after success ──

class _Resp:
    def __init__(self, code, data):
        self.status_code, self._d, self.text = code, data, json.dumps(data)

    def json(self):
        return self._d


def _run_github(mod, monkeypatch, job_status, fail_from_edit=2):
    monkeypatch.setattr(mod, 'GITHUB_API_KEY', 'dummy-local-key')
    monkeypatch.setattr(mod, 'GITHUB_BASE_URL', 'https://github-provider.invalid', raising=False)
    monkeypatch.setattr(mod.requests, 'post', lambda *a, **k: _Resp(200, {'job_id': 'J1'}))
    monkeypatch.setattr(mod.requests, 'get', lambda *a, **k: _Resp(200, {'status': job_status, 'app_id': 'A1'}))
    monkeypatch.setattr(mod.time, 'sleep', lambda s: None)
    edits = []

    def edit(text, *a, **kw):
        edits.append(text)
        if len(edits) >= fail_from_edit:
            raise telebot.apihelper.ApiTelegramException(
                'editMessageText', None, {'ok': False, 'error_code': 400,
                                          'description': 'Bad Request: message to edit not found'})
        return reply(kw.get('chat_id', 1001), text)

    monkeypatch.setitem(mod._VC_ORIG, 'edit_message_text', edit)
    started = []
    monkeypatch.setattr(mod.threading.Thread, 'start', lambda self: started.append(self))
    mod.temp_github_data[1001] = {'price': 15.0, 'lang': 'en',
                                  'user': mod.encrypt_sensitive('dummy-user'),
                                  'pass': mod.encrypt_sensitive('dummy-pass')}
    msg = types.SimpleNamespace(from_user=types.SimpleNamespace(id=1001), text='000000', message_id=7,
                                chat=types.SimpleNamespace(id=1001))
    mod.process_gh_step_2fa(msg)
    after_debit = mod._wa_balance(1001)
    for t in started:
        t.run()
    return after_debit, mod._wa_balance(1001)


def test_github_failed_job_refunded_once_even_if_message_deleted(mod, monkeypatch):
    assert _run_github(mod, monkeypatch, 'failed') == (85.0, 100.0)


def test_github_successful_job_not_refunded_even_if_message_deleted(mod, monkeypatch):
    assert _run_github(mod, monkeypatch, 'submitted') == (85.0, 85.0)
    assert mod.db.orders.count_documents({'user_id': 1001, 'product_id': 'GitHub_Student'}) == 1


def test_github_failed_job_still_refunded_when_every_edit_fails(mod, monkeypatch):
    assert _run_github(mod, monkeypatch, 'failed', fail_from_edit=1) == (85.0, 100.0)
