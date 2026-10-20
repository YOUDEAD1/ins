"""Round 32: float dust in balances (0.5499999999999999 shown as $0.55 but refused) and clear site errors instead of purchase_failed."""
import pathlib
import re

from test_integration import mod, isolate, server, request, product, APP  # noqa: F401  (shared fixtures)


def _user(mod, uid, balance):
    mod.db.users.delete_many({'user_id': uid})
    mod.db.users.insert_one({'user_id': uid, 'name': 'Dust', 'balance': balance, 'lang': 'en', 'lang_chosen': True, 'is_admin': 0, 'is_banned': 0})
    mod._USER_CACHE.clear()


def test_dust_balance_buys_on_the_site(mod, server):
    product(mod, price=0.55, stock=3)
    _user(mod, 1001, 0.5499999999999999)           # shows $0.55
    s, r, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 1})
    assert s == 200 and len(r['codes']) == 1, (s, r)
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 0.0          # no -1e-16 left behind


def test_dust_balance_buys_in_the_bot(mod):
    product(mod, price=0.55, stock=3)
    _user(mod, 1001, 0.5499999999999999)
    mod._do_purchase(1001, '101', 1, 'en')
    assert mod.db.orders.count_documents({'user_id': 1001}) == 1
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 0.0


def test_every_balance_write_is_settled(mod):
    _user(mod, 1001, 0.1)
    mod.db.users.update_one({'user_id': 1001}, {'$inc': {'balance': 0.2}})
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 0.3
    doc = mod.db.users.find_one_and_update({'user_id': 1001}, {'$inc': {'balance': -0.3}}, return_document=True)
    assert doc['balance'] == 0.0 and mod.db.users.find_one({'user_id': 1001})['balance'] == 0.0
    _user(mod, 1002, 0.60)
    mod.db.users.update_one({'user_id': 1002}, {'$inc': {'balance': -0.55}})
    assert mod.db.users.find_one({'user_id': 1002})['balance'] == 0.05


def test_genuinely_short_balance_still_refused(mod, server):
    product(mod, price=0.55, stock=3)
    _user(mod, 1001, 0.30)
    s, r, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 1})
    assert s == 402 and r['error'] == 'insufficient_balance'


def test_site_explains_why_instead_of_purchase_failed(mod, server, monkeypatch):
    product(mod, price=0.55, stock=3)
    _user(mod, 1001, 5.0)
    monkeypatch.setattr(mod, '_do_purchase', lambda *a, **k: None)      # purchase stopped without an order
    monkeypatch.setattr(mod, 'is_deposit_locked', lambda uid: False)
    s, r, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 1})
    assert s == 500 and r['error'] == 'purchase_failed'                   # truly unknown → still reported
    calls = {'n': 0}
    def locked_after_check(uid):        # the site's own pre-check passes; the purchase then meets an open deposit
        calls['n'] += 1
        return calls['n'] > 1
    monkeypatch.setattr(mod, 'is_deposit_locked', locked_after_check)
    monkeypatch.setattr(mod, '_wa_pending_view', lambda uid, rec=None: None)
    mod._WA_RATE.clear()
    s, r, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 1})
    assert s == 409 and r['error'] == 'deposit_pending', (s, r)


def test_all_deductions_accept_float_dust():
    src = (APP / 'bot.py').read_text(encoding='utf-8')
    assert len(re.findall(r"'balance': \{'\$gte': \w+ - MONEY_EPS\}", src)) == 11
    assert not re.findall(r"'balance': \{'\$gte': \w+\}", src)


def test_site_tg_early_events_and_read_retry():
    html = (APP / 'webapp/index.html').read_text(encoding='utf-8')
    early, sdk, replay = html.index('id="lara-tg-early"'), html.index('telegram-web-app.js'), html.index('id="lara-tg-replay"')
    assert early < sdk < replay
    m = re.search(r"const API_READ = /(.+)/;", html)
    assert m and not re.search(r'buy|deposit\\/create|cart', m.group(1).replace('deposit\\/status', ''))
    assert "navigator.onLine !== false && document.visibilityState !== 'hidden'" in html
