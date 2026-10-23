"""Add-on integration tests. MongoDB, Telegram and the controller stay offline."""
import datetime as dt
import hashlib
from types import SimpleNamespace as NS
from unittest.mock import Mock
import pytest
from cryptography.fernet import Fernet
from test_integration import mod, isolate, server, request
from harness import MESSAGES

SHOP = 's_' + 'a' * 24

@pytest.fixture
def platform(mod, monkeypatch):
    p = mod._wwc_platform
    monkeypatch.setattr(p, 'cipher', Fernet(Fernet.generate_key()))
    monkeypatch.setattr(p, 'super_ids', {9001})
    monkeypatch.setattr(p, 'indexed', False)
    p.draft_ids.clear()
    shop = dict(id=SHOP, owner_id=1001, bot_username='fixture_child_bot',
                status='active', plan='free', paid_until=None, idle_strikes=0)
    receipts = set()
    def controller(action, actor, **data):
        if action in ('info', 'dashboard') and actor not in (1001, 9001):
            raise RuntimeError('FORBIDDEN')
        if action == 'config': return dict(ok=True, stars=500, days=30, wallet_price_usd='5.00', wallet_currency='USD')
        if action == 'list': return dict(ok=True, shops=[shop], next_offset=None)
        if action == 'schema': return dict(ok=True, sql='-- isolated fixture schema')
        if action == 'dashboard': return dict(ok=True, shop=shop, stats=dict(users=4, orders=2, revenue=12, known_supplier_profit=3))
        if action == 'subscribe': receipts.add(data['receipt']); return dict(ok=True, shop=shop)
        return dict(ok=True, shop=shop)
    client = NS(enabled=True, call=Mock(side_effect=controller))
    monkeypatch.setattr(p, 'client', client)
    monkeypatch.setitem(p.g, '_get_server_url', lambda: 'https://nss.example')
    monkeypatch.setattr(mod.bot, 'delete_message', Mock())
    monkeypatch.setattr(mod.bot, 'clear_step_handler_by_chat_id', Mock())
    monkeypatch.setattr(mod.bot, 'send_invoice', Mock())
    monkeypatch.setattr(mod.bot, 'answer_pre_checkout_query', Mock())
    p.outbox_event.clear()
    yield p, client, receipts, controller
    p.draft_ids.clear()

def message(text='', uid=1001, **extra):
    return NS(text=text, from_user=NS(id=uid, first_name='Fixture', username='fixture'),
              chat=NS(id=uid, type='private'), message_id=7, **extra)

def test_addon_handlers_precede_legacy_catchalls(mod, platform):
    assert mod.bot.message_handlers[0]['function'].__name__ == 'command'
    assert mod.bot.callback_query_handlers[0]['function'].__name__ == 'callback'
    names = {h['function'].__name__ for h in mod.bot.message_handlers}
    assert 'start_handler' in names and 'got_payment' in names

def test_wizard_encrypts_credentials_sends_sql_and_keeps_balance(mod, platform):
    p, client, _, _ = platform
    token = '123456789:' + 'x' * 35
    p.create(1001)
    p.receive(message(token))
    raw = mod.db.wwc_drafts.find_one({'_id':1001})
    assert token not in str(raw) and p.draft(1001)['bot_token'] == token
    p.receive(message('https://fixture.supabase.co'))
    p.receive(message('sb_secret_' + 'x' * 40))
    assert mod.db.wwc_drafts.count_documents({}) == 0
    assert any(text == '[document]' for _, text, _ in MESSAGES)
    assert any(c.args[0] == 'info' for c in client.call.call_args_list)
    assert mod.bot.delete_message.call_count == 3
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100

def test_native_source_keys_are_distinct_and_account_scoped(mod, platform):
    p, _, _, _ = platform
    _, key1 = p.source_connection(1001)
    _, key2 = p.source_connection(1002)
    assert key1 != key2 and p.source_connection(1001)[1] == key1
    assert mod.db.api_keys.find_one({'api_key':key1})['user_id'] == 1001
    assert mod.db.api_keys.find_one({'api_key':key2})['user_id'] == 1002

def test_cancel_removes_only_the_current_draft(mod, platform):
    p, _, _, _ = platform
    p.create(1001)
    mod.db.wwc_shop_links.insert_one({'_id':SHOP, 'owner_id':1001})
    p.command(message('/cancelstore'))
    assert not p.draft(1001)
    assert mod.db.wwc_shop_links.find_one({'_id':SHOP})

def test_stars_outbox_survives_outage_and_never_grants_twice(mod, platform):
    p, client, receipts, controller = platform
    p.subscribe(1001, SHOP, method='stars')
    d = mod.db.wwc_subscriptions.find_one({})
    assert p.valid_invoice(d['_id'], 1001, 'XTR', 500)
    assert not p.valid_invoice(d['_id'], 1002, 'XTR', 500)
    assert not p.valid_invoice(d['_id'], 1001, 'USD', 500)
    assert not p.valid_invoice(d['_id'], 1001, 'XTR', 1)
    payment = NS(invoice_payload=d['_id'], currency='XTR', total_amount=500,
                 telegram_payment_charge_id='opaque/telegram+charge=fixture')
    client.call.side_effect = RuntimeError('PLATFORM_UNAVAILABLE')
    p.payment(message(successful_payment=payment))
    saved = mod.db.wwc_subscriptions.find_one({'_id':d['_id']})
    assert saved['state'] == 'payment_received' and saved['charge_id'] == payment.telegram_payment_charge_id
    client.call.side_effect = controller
    mod.db.wwc_subscriptions.update_one({'_id':d['_id']}, {'$set':{'next_retry':dt.datetime(2000,1,1)}})
    p.flush_outbox()
    p.payment(message(successful_payment=payment))
    assert len(receipts) == 1
    assert next(iter(receipts)) == 'tg:' + hashlib.sha256(payment.telegram_payment_charge_id.encode()).hexdigest()
    assert mod.db.wwc_subscriptions.find_one({'_id':d['_id']})['state'] == 'applied'
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100

def test_customer_cannot_grant_subscription(mod, platform):
    p, client, _, _ = platform
    call = NS(id='fixture', data='wwc:do:grant:' + SHOP,
              from_user=NS(id=1001), message=message())
    p.callback(call)
    assert not any(c.args[0] == 'grant' for c in client.call.call_args_list)
    assert 'FORBIDDEN' in MESSAGES[-1][1]

def test_english_dashboard_contains_stats_and_subscription(mod, platform):
    p, _, _, _ = platform
    p.dashboard(1001, SHOP)
    _, text, kwargs = MESSAGES[-1]
    assert 'Paid orders: 2' in text and '$12.00' in text and '$3.00' in text
    assert 'المتجر' not in text
    assert any(b.callback_data == 'wwc:subscribe:' + SHOP for row in kwargs['reply_markup'].keyboard for b in row)

def test_request_recovery_endpoint_is_scoped_to_source_account(mod, server, platform):
    p, _, _, _ = platform
    _, key1 = p.source_connection(1001)
    _, key2 = p.source_connection(1002)
    ref = 'fixture_order_1'
    idem = 'IDEM_' + hashlib.sha256(f'1001:{ref}'.encode()).hexdigest()[:40]
    mod.db.api_idempotency.insert_one({'_id':idem, 'api_user_id':1001,
        'status':'done', 'code':200, 'data':{'codes':['PRIVATE_DELIVERY']}})
    path = '/' + mod._get_api_gateway() + '/platform_request/' + ref
    status, data, _ = request(server, path, method='GET', auth=False, headers={'Authorization':'Bearer ' + key1})
    assert status == 200 and data['data']['codes'] == ['PRIVATE_DELIVERY']
    status, data, _ = request(server, path, method='GET', auth=False, headers={'Authorization':'Bearer ' + key2})
    assert status == 404 and 'PRIVATE_DELIVERY' not in str(data)

def test_new_start_buttons_keep_the_original_menu(mod, platform, monkeypatch):
    mod.db.users.update_one({'user_id':1001}, {'$set':{'lang_chosen':True}})
    mod._USER_CACHE.clear()
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **kw: True)
    mod.start_handler(message('/start'))
    keyboards = [kw['reply_markup'] for _, _, kw in MESSAGES if kw.get('reply_markup')]
    values = {getattr(b, 'callback_data', '') for k in keyboards if hasattr(k,'keyboard') for row in k.keyboard for b in row}
    assert {'open_shop', 'open_settings', 'wwc:create', 'wwc:list'} <= values

def test_ordinary_messages_do_not_poll_drafts(mod, platform, monkeypatch):
    p, _, _, _ = platform
    p.draft_ids.clear()
    read = Mock(side_effect=AssertionError('unnecessary draft query'))
    monkeypatch.setattr(p, 'draft', read)
    handler = next(h for h in mod.bot.message_handlers if h['function'].__name__ == 'receive_safe')
    for _ in range(20): assert not handler['filters']['func'](message('ordinary customer text'))
    read.assert_not_called()
