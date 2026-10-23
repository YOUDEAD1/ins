"""NSS wallet subscriptions: real handlers, atomic mock DB, injected failures."""
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace as NS
from unittest.mock import Mock
import pytest
from test_wwc_platform import mod, isolate, platform, SHOP, message
from harness import MESSAGES


def quote_for(mod, p, uid=1001):
    p.subscribe(uid, SHOP)
    return mod.db.wwc_subscriptions.find_one({'payment_method':'nss_wallet','state':'wallet_quote'})


def token_of(quote):
    return quote['_id'].split(':')[-1]


def test_default_checkout_is_nss_balance_with_explicit_confirmation(mod, platform):
    p, client, receipts, _ = platform
    quote = quote_for(mod, p)
    assert quote['amount_usd'] == '5.00' and quote['days'] == 30
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100
    mod.bot.send_invoice.assert_not_called()
    assert not receipts
    assert 'Current NSS balance: $100.00' in MESSAGES[-1][1]
    callbacks = [b.callback_data for row in MESSAGES[-1][2]['reply_markup'].keyboard for b in row]
    assert 'wwc:walletpay:' + token_of(quote) in callbacks
    assert 'open_deposit' in callbacks
    assert all(len(v.encode()) <= 64 for v in callbacks)


def test_confirm_debits_existing_wallet_records_receipt_and_activates_once(mod, platform):
    p, client, receipts, _ = platform
    quote = quote_for(mod, p)
    p.callback(NS(id='confirm', data='wwc:walletpay:' + token_of(quote), from_user=NS(id=1001), message=message()))
    p.wallet_confirm(1001, token_of(quote))
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 95
    assert mod.db.wwc_subscriptions.find_one({'_id':quote['_id']})['state'] == 'applied'
    assert receipts == {'nss:' + token_of(quote)}
    log = mod.db.balance_logs.find_one({'wwc_payment_id':quote['_id']})
    assert log['amount'] == -5 and log['old_balance'] == 100 and log['new_balance'] == 95
    assert mod.db.balance_logs.count_documents({'wwc_payment_id':quote['_id']}) == 1
    calls = [c for c in client.call.call_args_list if c.args[0] == 'subscribe']
    assert len(calls) == 1
    assert calls[0].kwargs['payment_method'] == 'nss_wallet'
    assert calls[0].kwargs['amount'] == '5.00' and calls[0].kwargs['currency'] == 'USD'
    assert 'stars' not in calls[0].kwargs


@pytest.mark.parametrize('balance',[0,3,-50,4.9999])
def test_insufficient_or_negative_balance_is_never_overdrawn_or_recalculated(mod, platform, balance):
    p, _, receipts, _ = platform
    mod.db.users.update_one({'user_id':1001},{'$set':{'balance':balance}})
    quote = quote_for(mod, p)
    p.wallet_confirm(1001, token_of(quote))
    assert mod.db.users.find_one({'user_id':1001})['balance'] == balance
    assert not receipts and mod.db.balance_logs.count_documents({}) == 0
    assert mod.db.wwc_subscriptions.find_one({'_id':quote['_id']})['state'] == 'wallet_quote'
    assert 'Insufficient NSS balance' in MESSAGES[-1][1]


def test_missing_price_fails_closed_and_does_not_invent_a_dollar_conversion(mod, platform):
    p, client, receipts, controller = platform
    client.call.side_effect = lambda action, actor, **data: (
        dict(ok=True, stars=500, days=30, wallet_price_usd=None) if action == 'config'
        else controller(action,actor,**data))
    p.subscribe(1001, SHOP)
    assert mod.db.wwc_subscriptions.count_documents({}) == 0
    assert not receipts and 'has not set' in MESSAGES[-1][1]
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100


def test_paid_request_survives_controller_outage_and_lost_success_response(mod, platform):
    p, client, receipts, controller = platform
    quote = quote_for(mod, p)
    def outage(action, actor, **data):
        if action == 'subscribe':
            controller(action, actor, **data)  # Remote commit succeeded; reply lost.
            raise RuntimeError('PLATFORM_UNAVAILABLE')
        return controller(action, actor, **data)
    client.call.side_effect = outage
    p.wallet_confirm(1001, token_of(quote))
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 95
    assert mod.db.wwc_subscriptions.find_one({'_id':quote['_id']})['state'] == 'payment_received'
    assert 'do not pay again' in MESSAGES[-1][1]
    p.subscribe(1001, SHOP)
    assert mod.db.wwc_subscriptions.count_documents({}) == 1
    client.call.side_effect = controller
    mod.db.wwc_subscriptions.update_one({'_id':quote['_id']},{'$set':{'next_retry':dt.datetime(2000,1,1)}})
    p.flush_outbox()
    p.wallet_confirm(1001, token_of(quote))
    assert receipts == {'nss:' + token_of(quote)}
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 95
    assert mod.db.wwc_subscriptions.find_one({'_id':quote['_id']})['state'] == 'applied'


def test_crash_after_atomic_debit_before_journal_and_ledger_can_resume(mod, platform, monkeypatch):
    p, _, receipts, _ = platform
    quote = quote_for(mod, p)
    collection = mod.db.balance_logs
    original = collection.update_one
    monkeypatch.setattr(collection, 'update_one', Mock(side_effect=RuntimeError('simulated write outage')))
    p.wallet_confirm(1001, token_of(quote))
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 95
    assert mod.db.wwc_subscriptions.find_one({'_id':quote['_id']})['state'] == 'wallet_debiting'
    assert not receipts
    monkeypatch.setattr(collection,'update_one', original)
    mod.db.wwc_subscriptions.update_one({'_id':quote['_id']},{'$set':{'next_retry':dt.datetime(2000,1,1)}})
    # Rebuild the add-on as on an application restart; no in-memory checkout
    # state or lock is needed to recognize the already debited receipt.
    restarted = type(p)(p.g)
    restarted.client = p.client
    restarted.flush_outbox()
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 95
    assert mod.db.balance_logs.count_documents({'wwc_payment_id':quote['_id']}) == 1
    assert len(receipts) == 1
    assert mod.db.wwc_subscriptions.find_one({'_id':quote['_id']})['state'] == 'applied'


def test_topup_after_declined_checkout_needs_a_new_payment_confirmation(mod, platform):
    p, _, receipts, _ = platform
    mod.db.users.update_one({'user_id':1001},{'$set':{'balance':3}})
    quote = quote_for(mod,p)
    p.wallet_confirm(1001,token_of(quote))
    mod.db.users.update_one({'user_id':1001},{'$inc':{'balance':7}})
    p.flush_outbox()
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 10 and not receipts
    p.wallet_confirm(1001,token_of(quote))
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 5 and len(receipts) == 1


def test_parallel_confirmations_and_old_buttons_do_not_debit_twice(mod, platform):
    p, _, receipts, _ = platform
    quote = quote_for(mod, p)
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda _:p.wallet_confirm(1001,token_of(quote)),range(16)))
    p.flush_outbox()
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 95
    assert len(receipts) == 1 and mod.db.balance_logs.count_documents({}) == 1


def test_expired_cancelled_and_foreign_quotes_cannot_charge(mod, platform):
    p, _, receipts, _ = platform
    quote = quote_for(mod, p)
    with pytest.raises(RuntimeError,match='FORBIDDEN'):p.wallet_confirm(1002,token_of(quote))
    p.wallet_cancel(1001,token_of(quote))
    p.wallet_confirm(1001,token_of(quote))
    quote = quote_for(mod, p)
    mod.db.wwc_subscriptions.update_one({'_id':quote['_id']},{'$set':{'expires_at':dt.datetime(2000,1,1)}})
    p.wallet_confirm(1001,token_of(quote))
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100 and not receipts


def test_exact_balance_float_dust_topup_and_new_renewal(mod, platform):
    p, _, receipts, _ = platform
    mod.db.users.update_one({'user_id':1001},{'$set':{'balance':4.999999999999999}})
    quote = quote_for(mod, p)
    p.wallet_confirm(1001, token_of(quote))
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 0
    mod.db.users.update_one({'user_id':1001},{'$inc':{'balance':10}})
    renewal = quote_for(mod, p)
    assert renewal['_id'] != quote['_id']
    p.wallet_confirm(1001,token_of(quote))  # Replayed first payment does not renew.
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 10
    p.wallet_confirm(1001,token_of(renewal))
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 5 and len(receipts) == 2


def test_arabic_checkout_uses_the_customers_language_and_persists_quote_price(mod, platform):
    p, client, _, controller = platform
    mod.db.users.update_one({'user_id':1001},{'$set':{'lang':'ar'}});mod._USER_CACHE.clear()
    quote = quote_for(mod,p)
    assert 'رصيد NSS الحالي' in MESSAGES[-1][1]
    client.call.side_effect = lambda action,actor,**data: (
        dict(ok=True, stars=500, days=90, wallet_price_usd='50.00') if action == 'config'
        else controller(action,actor,**data))
    p.wallet_confirm(1001,token_of(quote))
    call = next(c for c in client.call.call_args_list if c.args[0] == 'subscribe')
    assert call.kwargs['amount'] == '5.00' and call.kwargs['days'] == 30


def test_runtime_check_is_visible_in_nss_dashboard(mod, platform):
    p, client, _, controller = platform
    def running(action,actor,**data):
        response = controller(action,actor,**data)
        if action == 'dashboard':response['stats'].update(working=True, ready=True)
        return response
    client.call.side_effect = running
    p.dashboard(1001,SHOP)
    assert 'Runtime check: 🟢 Connected and ready' in MESSAGES[-1][1]
