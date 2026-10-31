"""Regression tests for the October 2026 security fixes. No production services are contacted."""
import time

from test_integration import mod, isolate, server, product, request, _api  # noqa: F401


KEY_A = 'sk_' + 'a' * 48
KEY_B = 'sk_' + 'b' * 48


def _resellers(mod):
    mod.db.api_keys.insert_one({'user_id': 1001, 'api_key': KEY_A, 'is_active': True})
    mod.db.api_keys.insert_one({'user_id': 1002, 'api_key': KEY_B, 'is_active': True})
    mod._api_rate.clear()
    return mod._get_api_gateway()


# ── events: /changes and webhooks must not leak other resellers' orders or buyer IDs ──

def test_changes_hide_other_resellers_orders_and_buyer_ids(mod, server):
    gw = _resellers(mod)
    product(mod, stock=5)
    since = int(time.time()) - 5
    st, bought = _api(server, 'POST', f'/{gw}/purchase', KEY_A, {'product_id': '101', 'qty': 1, 'buyer_info': '@x'})
    assert st == 200
    mod._emit_event('stock.sold', {'product_id': '101', 'qty_sold': 2, 'remaining_stock': 2,
                                   'is_manual': False, 'via_api': False, 'buyer_user_id': 7777}, product_id='101')

    st, mine = _api(server, 'GET', f'/{gw}/changes?since={since}', KEY_A)
    st2, other = _api(server, 'GET', f'/{gw}/changes?since={since}', KEY_B)
    assert st == st2 == 200
    assert '7777' not in repr(mine['events']) and '7777' not in repr(other['events'])
    assert any(e['event_type'] == 'order.completed' for e in mine['events'])
    assert not any(e['event_type'] == 'order.completed' for e in other['events'])
    assert bought['order_id'] not in repr(other['events'])
    assert '1001' not in repr(other['events'])


def test_webhook_worker_skips_events_owned_by_other_resellers(mod):
    full = {'order_id': 'API_1', 'product_id': '101', 'total_price': 5.0, 'api_user_id': 1001}
    evt = {'event_id': 'e1', 'event_type': 'order.completed', 'timestamp': 1, 'data': full}
    assert mod._api_event_visible_to_user(evt, 1001) is True
    assert mod._api_event_visible_to_user(evt, 1002) is False
    sale = {'event_type': 'stock.sold', 'data': {'product_id': '101', 'buyer_user_id': 5}}
    assert mod._api_event_visible_to_user(sale, 1002) is False


# ── banned users cannot keep using their reseller key ──

def test_banned_user_api_key_is_rejected(mod, server):
    gw = _resellers(mod)
    product(mod, stock=2)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'is_banned': 1}})
    st, _ = _api(server, 'GET', f'/{gw}/products', KEY_A)
    assert st == 401
    st, _ = _api(server, 'POST', f'/{gw}/purchase', KEY_A, {'product_id': '101', 'qty': 1})
    assert st == 401
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


# ── reseller-controlled text is escaped in admin HTML notices ──

def test_admin_purchase_notice_escapes_buyer_info(mod, monkeypatch):
    captured = []
    monkeypatch.setattr(mod, '_queue_api_purchase_notice', lambda oid, msg: captured.append(msg))
    mod._notify_all_admins_api_purchase(1001, {'name_en': 'P'}, 1, 1.0, 'API_1',
                                        '<a href="https://evil.example">click</a>', False)
    assert captured and 'href="https://evil.example"' not in captured[0]
    assert '&lt;a href=&quot;https://evil.example&quot;&gt;' in captured[0]


# ── webhook SSRF: the checked address must be the address we connect to (DNS rebinding) ──

def test_webhook_delivery_is_pinned_to_the_checked_public_ip(mod, monkeypatch):
    import socket
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    hits = []

    class Sink(BaseHTTPRequestHandler):
        def do_POST(self):
            hits.append(self.path)
            self.rfile.read(int(self.headers.get('Content-Length') or 0))
            self.send_response(204)
            self.end_headers()

        def log_message(self, *a):
            pass

    sink = ThreadingHTTPServer(('127.0.0.1', 0), Sink)
    t = threading.Thread(target=sink.serve_forever, daemon=True)
    t.start()
    port = sink.server_address[1]
    real = socket.getaddrinfo
    calls = {'n': 0}

    def rebinding(host, *a, **kw):
        if host == 'rebind.lara-test.example':
            calls['n'] += 1
            ip = '1.1.1.1' if calls['n'] <= 2 else '127.0.0.1'
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (ip, a[0] if a else 80))]
        return real(host, *a, **kw)

    monkeypatch.setattr(socket, 'getaddrinfo', rebinding)
    seen = []

    def fake_request(self, method, url, *a, **kw):
        seen.append((url, (kw.get('headers') or {}).get('Host')))
        raise ConnectionError('offline test: public address not contacted')

    monkeypatch.setattr(mod.requests.sessions.Session, 'request', fake_request)
    import time as _time
    monkeypatch.setattr(_time, 'sleep', lambda s: None)
    hook = {'_id': 'h1', 'api_user_id': 1001, 'url': f'http://rebind.lara-test.example:{port}/internal?x=1',
            'secret': 's', 'failures': 0}
    mod.db.api_webhooks.insert_one(dict(hook))
    ok = mod._deliver_webhook(hook, {'event_id': 'e', 'event_type': 'webhook.test', 'timestamp': 1, 'data': {}})
    sink.shutdown(); sink.server_close(); t.join()
    assert ok is False
    assert hits == []                       # loopback service never reached
    assert seen and all(u.startswith(f'http://1.1.1.1:{port}/internal') for u, _ in seen)
    assert all(h == f'rebind.lara-test.example:{port}' for _, h in seen)


# ── reseller price must be a finite, non-negative number at or above the store price ──

def test_set_price_rejects_nan_and_below_base(mod, server):
    gw = _resellers(mod)
    product(mod, price=5.0, stock=1)
    for bad in ('NaN', 'nan', 'Infinity', '-1', 4.99):
        st, _ = _api(server, 'POST', f'/{gw}/set_price', KEY_A, {'product_id': '101', 'price': bad})
        assert st == 400, bad
    assert mod.db.api_pricing.count_documents({}) == 0
