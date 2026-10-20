"""Financial and subscription regressions. No production services are contacted."""
import ast
import datetime
import json
import threading
import types
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from harness import APP, MESSAGES
from test_integration import mod, isolate, server, product, request, _api


def payment(uid, charge):
    return types.SimpleNamespace(from_user=types.SimpleNamespace(id=uid),
        successful_payment=types.SimpleNamespace(invoice_payload=f'dep_{uid}_5.0',
            telegram_payment_charge_id=charge, currency='XTR', total_amount=600))


@pytest.mark.parametrize('same_user', [False, True])
def test_independent_stars_payments_same_amount_minute(mod, monkeypatch, same_user):
    monkeypatch.setattr(mod.time, 'time', lambda: 1792440000)
    second = 1001 if same_user else 1002
    mod.got_payment(payment(1001, 'TEST-STARS-CHARGE-FIRST'))
    mod.got_payment(payment(second, 'TEST-STARS-CHARGE-SECOND'))
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == (110 if same_user else 105)
    assert mod.db.users.find_one({'user_id': 1002})['balance'] == (50 if same_user else 55)
    assert mod.db.users.count_documents({'is_banned': 1}) == 0
    receipts = list(mod.db.used_transactions.find())
    assert len(receipts) == len({r['fingerprint'] for r in receipts}) == 2


def test_duplicate_stars_update_credits_once_in_parallel(mod):
    mod.db.used_transactions.create_index('transaction_id', unique=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: mod.got_payment(payment(1001, 'TEST-STARS-ONE-REPLAYED')), range(4)))
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 105
    assert mod.db.used_transactions.count_documents({}) == 1
    assert mod.db.users.count_documents({'is_banned': 1}) == 0


@pytest.mark.parametrize('fail_at', [1, 2])
def test_order_storage_failure_refunds_all_and_restores_all_stock(mod, server, monkeypatch, fail_at):
    product(mod, stock=2)
    old = mod.db.orders.insert_one({'user_id':1001,'product_id':'101','order_id':'OLD-ORDER',
        'code_delivered':'101-SECRET-0','price':5,'qty':1}).inserted_id
    original = mod.db.orders.insert_one
    count = []
    def fail(doc, *a, **kw):
        count.append(doc)
        if len(count) == fail_at:
            raise RuntimeError('Simulated order storage failure')
        return original(doc, *a, **kw)
    monkeypatch.setattr(mod.db.orders, 'insert_one', fail)
    status, data, _ = request(server, '/webapp/buy', {'product_id':'101','qty':2})
    assert status == 502 and data['refunded'] is True
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100
    assert mod.db.product_stock.count_documents({'is_sold':False}) == 2
    assert mod.db.orders.count_documents({}) == 1 and mod.db.orders.find_one({'_id':old})
    mod._retry_stock_refunds(); mod._retry_stock_refunds()
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100


def test_refund_failure_is_durable_and_retried_once(mod, server, monkeypatch):
    product(mod, stock=2)
    monkeypatch.setattr(mod.db.orders,'insert_one',Mock(side_effect=RuntimeError('write failed')))
    update = mod.db.users.update_one
    def fail_refund(query, changes, *a, **kw):
        if changes.get('$inc',{}).get('balance',0) > 0:
            raise RuntimeError('refund temporarily unavailable')
        return update(query, changes, *a, **kw)
    monkeypatch.setattr(mod.db.users,'update_one',fail_refund)
    status, data, _ = request(server, '/webapp/buy', {'product_id':'101','qty':2})
    assert status == 502 and data['refunded'] is False
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 90
    assert mod.db.stock_rollback_jobs.count_documents({'status':'pending'}) == 1
    monkeypatch.setattr(mod.db.users,'update_one',update)
    mod._retry_stock_refunds(); mod._retry_stock_refunds()
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100
    assert mod.db.stock_rollback_jobs.count_documents({'status':'pending'}) == 0


@pytest.mark.parametrize('where', ['order', 'api_receipt', 'stock_finalize'])
def test_reseller_storage_failure_is_refunded_without_partial_receipt(mod, server, monkeypatch, where):
    product(mod, stock=2)
    key='sk_'+'r'*48
    mod.db.api_keys.insert_one({'user_id':1001,'api_key':key,'is_active':True}); mod._api_rate.clear()
    collection, method = {'order':(mod.db.orders,'insert_one'),
        'api_receipt':(mod.db.api_orders,'insert_one'),
        'stock_finalize':(mod.db.product_stock,'update_many')}[where]
    original=getattr(collection,method)
    def fail(*a,**kw):
        if where != 'stock_finalize' or '$set' in a[1] and 'sale_order_id' in a[1]['$set']:
            raise RuntimeError('Simulated storage failure')
        return original(*a,**kw)
    monkeypatch.setattr(collection,method,fail)
    status, data = _api(server,'POST',f'/{mod._get_api_gateway()}/purchase',key,
        {'product_id':'101','qty':2},{'Idempotency-Key':'storage-failure'})
    assert status == 503 and data['refunded'] is True
    assert mod.db.users.find_one({'user_id':1001})['balance'] == 100
    assert mod.db.orders.count_documents({}) == mod.db.api_orders.count_documents({}) == 0
    assert mod.db.product_stock.count_documents({'is_sold':False}) == 2


def test_api_profit_uses_actual_debit_instead_of_reseller_markup(mod, server):
    product(mod, stock=2, cost=2)
    key='sk_'+'m'*48
    mod.db.api_keys.insert_one({'user_id':1001,'api_key':key,'is_active':True}); mod._api_rate.clear()
    mod.db.api_pricing.insert_one({'api_user_id':1001,'product_id':'101','sell_price':20})
    st, data = _api(server,'POST',f'/{mod._get_api_gateway()}/purchase',key,{'product_id':'101','qty':2})
    assert st == 200 and data['total_price'] == 10
    report=mod.profit_stats(datetime.datetime(2020,1,1))
    assert report['sales'] == 10 and report['cost'] == 4 and report['profit'] == 6


@pytest.mark.parametrize('via', ['site','api'])
def test_post_delivery_housekeeping_failure_still_returns_saved_receipt(mod, server, monkeypatch, via):
    product(mod,stock=2)
    original=mod.db.users.update_one
    def fail_marker_cleanup(query,change,*a,**kw):
        if any(k.startswith('_stock_debits.') for k in change.get('$unset',{})):
            raise RuntimeError('Housekeeping unavailable')
        return original(query,change,*a,**kw)
    monkeypatch.setattr(mod.db.users,'update_one',fail_marker_cleanup)
    if via=='site':
        st,data,_=request(server,'/webapp/buy',{'product_id':'101','qty':2})
        assert data['ok'] and len(data['codes'])==2
    else:
        key='sk_'+'j'*48
        mod.db.api_keys.insert_one({'user_id':1001,'api_key':key,'is_active':True});mod._api_rate.clear()
        st,data=_api(server,'POST',f'/{mod._get_api_gateway()}/purchase',key,{'product_id':'101','qty':2})
        assert data['success'] and len(data['codes'])==2
    assert st==200 and mod.db.users.find_one({'user_id':1001})['balance']==90
    assert mod.db.orders.count_documents({})==2


def manager(mod, expires=None, allowed=True):
    expires=expires or (datetime.datetime.now()+datetime.timedelta(days=10)).isoformat()
    doc={'_id':'test-account','data':{'accessToken':'test-token','sessionToken':'test-session',
        'user':{'email':'owner@example.test'},'account':{'id':'test-account'}}}
    mod.db.cgpt_invites_data.insert_one({'_id':'test-account','data':{
        'invites':{'paid@example.test':{'status':'active','expires_at':expires,'telegram_uid':1001}},
        'allowed_emails':['owner@example.test']+(['paid@example.test'] if allowed else [])}})
    return mod._cgpt_build_manager_from_doc(doc)


@pytest.mark.parametrize('allowed', [True, False])
def test_cleanup_preserves_paid_customer_and_removes_only_unexpected_member(mod, monkeypatch, allowed):
    m=manager(mod, allowed=allowed)
    monkeypatch.setattr(m,'_get_org_users',lambda:[{'id':'paid','email':'paid@example.test'},
        {'id':'outside','email':'outside@example.test'}])
    removed=[]
    monkeypatch.setattr(m,'_remove_user',lambda id,email:removed.append(email) or True)
    m.check_and_cleanup()
    assert removed == ['outside@example.test']
    saved=mod.db.cgpt_invites_data.find_one({'_id':'test-account'})['data']['invites']['paid@example.test']
    assert saved['status'] == 'active'


def test_cleanup_fetch_failure_never_removes_or_expires(mod, monkeypatch):
    m=manager(mod,(datetime.datetime.now()-datetime.timedelta(days=1)).isoformat())
    def failed():m._last_fetch_failed=True;return []
    monkeypatch.setattr(m,'_get_org_users',failed)
    remove=Mock();monkeypatch.setattr(m,'_remove_user',remove)
    m.check_and_cleanup()
    assert not remove.called and m.invites_data['invites']['paid@example.test']['status']=='active'


@pytest.mark.parametrize('remove_ok', [True, False])
def test_expired_member_is_marked_expired_only_after_success(mod, monkeypatch, remove_ok):
    m=manager(mod,(datetime.datetime.now()-datetime.timedelta(days=1)).isoformat())
    monkeypatch.setattr(m,'_get_org_users',lambda:[{'id':'paid','email':'paid@example.test'}])
    monkeypatch.setattr(m,'_remove_user',lambda *a:remove_ok)
    m.check_and_cleanup()
    assert m.invites_data['invites']['paid@example.test']['status']==('expired' if remove_ok else 'active')
    assert ('paid@example.test' in m.allowed_emails) is not remove_ok


@pytest.mark.parametrize('mode', ['revoked','absent','get_failure','delete_failure','incomplete'])
def test_expired_pending_invite_is_checked_and_revoked_safely(mod, monkeypatch, mode):
    m=manager(mod,(datetime.datetime.now()-datetime.timedelta(days=1)).isoformat())
    monkeypatch.setattr(m,'_get_org_users',lambda:[])
    body={'items':[] if mode=='absent' else [{'id':'pending-id','email_address':'paid@example.test'}]}
    if mode=='incomplete':body={'items':[],'has_more':True}
    monkeypatch.setattr(mod.cffi_requests,'get',lambda *a,**kw:types.SimpleNamespace(
        status_code=401 if mode=='get_failure' else 200,json=lambda:body))
    delete=Mock(return_value=types.SimpleNamespace(status_code=500 if mode=='delete_failure' else 204))
    monkeypatch.setattr(mod.cffi_requests,'delete',delete)
    m.check_and_cleanup()
    ok=mode in ('revoked','absent')
    assert m.invites_data['invites']['paid@example.test']['status']==('expired' if ok else 'active')
    if mode=='revoked':
        assert delete.call_count==1 and delete.call_args.args[0].endswith('/pending-id')
        assert any(cid==1001 and 'expired' in text.lower() for cid,text,_ in MESSAGES)


@pytest.mark.parametrize('visibility', ['parent_hidden','all_links_hidden','reseller_parent_hidden','reseller_plan_hidden'])
def test_hidden_chatgpt_plan_is_not_listed_or_sold_by_reseller_api(mod, server, monkeypatch, visibility):
    pid=mod.db.cgpt_products.insert_one({'name':'ChatGPT','is_hidden':visibility=='parent_hidden',
        'durations':[{'dur_id':'w','price':7,'minutes':10080}]}).inserted_id
    product(mod,'201',stock=0,product_type='cgpt_main',cgpt_product_id=str(pid),is_hidden=visibility=='all_links_hidden')
    key='sk_'+'h'*48
    mod.db.api_keys.insert_one({'user_id':1001,'api_key':key,'is_active':True});mod._api_rate.clear()
    full=f'cgpt_{pid}_w'
    if visibility.startswith('reseller_'):
        mod.db.api_hidden.insert_one({'api_user_id':1001,'product_id':full if visibility=='reseller_plan_hidden' else f'cgpt_{pid}'})
    invite=Mock(side_effect=AssertionError('Hidden plan must never invite'))
    monkeypatch.setattr(mod,'_cgpt_find_available_account',invite)
    prefix=f'/{mod._get_api_gateway()}'
    st,data=_api(server,'GET',prefix+'/products',key)
    assert st==200 and not any(p['id']==full for p in data['products'])
    assert _api(server,'GET',prefix+'/product/'+full,key)[0]==404
    assert _api(server,'POST',prefix+'/purchase',key,{'product_id':full,'email':'buyer@example.test'})[0]==404
    assert not invite.called and mod.db.users.find_one({'user_id':1001})['balance']==100


def renewal(mod):
    acc=mod.db.cgpt_accounts.insert_one({'data':{'user':{'email':'owner@example.test'}}}).inserted_id
    exp=(datetime.datetime.now()+datetime.timedelta(days=3)).isoformat()
    mod.db.cgpt_invites_data.insert_one({'_id':str(acc),'data':{'invites':{
        'paid@example.test':{'status':'active','telegram_uid':1001,'expires_at':exp}},
        'allowed_emails':['owner@example.test','paid@example.test']}})
    pid=mod.db.cgpt_products.insert_one({'name':'ChatGPT','cost':1.25,'durations':[
        {'dur_id':'m1','price':4,'minutes':43200,'label':'Month'}]}).inserted_id
    sub={'email':'paid@example.test','account_id':str(acc)}
    return sub,pid,exp


@pytest.mark.parametrize('via', ['bot','webapp'])
def test_renewal_has_one_order_and_correct_profit_snapshot(mod, server, via):
    sub,pid,exp=renewal(mod)
    if via=='webapp':
        st,result,_=request(server,'/webapp/cgpt/renew',{'email':sub['email'],'product_id':str(pid),'dur_id':'m1'})
        assert st==200 and result['ok']
    else:assert mod._cgpt_renew_core(1001,sub,str(pid),'m1')['ok']
    order=mod.db.orders.find_one({'type':'cgpt_renewal'})
    assert order and order['via']==via and order['cost_total']==1.25
    mod.db.cgpt_products.update_one({'_id':pid},{'$set':{'cost':3}})
    report=mod.profit_stats(datetime.datetime(2020,1,1))
    assert report['sales']==4 and report['cost']==1.25 and report['profit']==2.75 and report['orders']==1
    assert mod.db.users.find_one({'user_id':1001})['balance']==96


def test_renewal_receipt_storage_failure_is_recovered_without_double_charge(mod, monkeypatch):
    sub,pid,exp=renewal(mod)
    original=mod.db.orders.update_one
    monkeypatch.setattr(mod.db.orders,'update_one',Mock(side_effect=RuntimeError('history unavailable')))
    result=mod._cgpt_renew_core(1001,sub,str(pid),'m1')
    assert result['ok'] and mod.db.users.find_one({'user_id':1001})['balance']==96
    doc=mod.db.cgpt_invites_data.find_one({'_id':sub['account_id']})
    assert len(doc['renewal_receipts'])==1 and doc['data']['invites'][sub['email']]['expires_at']!=exp
    monkeypatch.setattr(mod.db.orders,'update_one',original)
    mod._cgpt_retry_renewal_receipts();mod._cgpt_retry_renewal_receipts()
    assert mod.db.orders.count_documents({'type':'cgpt_renewal'})==1
    assert mod.db.users.find_one({'user_id':1001})['balance']==96


@pytest.mark.parametrize('committed', [True, False])
def test_uncertain_renewal_write_does_not_refund_a_committed_extension(mod, monkeypatch, committed):
    sub,pid,exp=renewal(mod)
    original=mod.db.cgpt_invites_data.update_one
    def fail(query, change, *a, **kw):
        if any(k.startswith('renewal_receipts.') for k in change.get('$set',{})):
            if committed:original(query,change,*a,**kw)
            raise RuntimeError('database acknowledgement lost')
        return original(query,change,*a,**kw)
    monkeypatch.setattr(mod.db.cgpt_invites_data,'update_one',fail)
    result=mod._cgpt_renew_core(1001,sub,str(pid),'m1')
    assert result['ok'] is committed
    assert mod.db.users.find_one({'user_id':1001})['balance']==(96 if committed else 100)
    mod._cgpt_retry_renewal_receipts()
    assert mod.db.orders.count_documents({'type':'cgpt_renewal'})==(1 if committed else 0)


def test_cleanup_refreshes_stale_snapshot_after_renewal(mod, monkeypatch):
    sub,pid,exp=renewal(mod)
    past=(datetime.datetime.now()-datetime.timedelta(minutes=1)).isoformat()
    doc=mod.db.cgpt_invites_data.find_one({'_id':sub['account_id']})
    doc['data']['invites'][sub['email']]['expires_at']=past
    mod.db.cgpt_invites_data.update_one({'_id':sub['account_id']},{'$set':{'data':doc['data']}})
    m=mod._cgpt_build_manager_from_doc({'_id':sub['account_id'],'data':{'user':{'email':'owner@example.test'}}})
    assert mod._cgpt_renew_core(1001,sub,str(pid),'m1')['ok']
    monkeypatch.setattr(m,'_get_org_users',lambda:[{'id':'paid','email':sub['email']}])
    remove=Mock();monkeypatch.setattr(m,'_remove_user',remove)
    m.check_and_cleanup()
    assert not remove.called and m.invites_data['invites'][sub['email']]['status']=='active'
    assert m.invites_data['invites'][sub['email']]['expires_at']!=past


def test_cleanup_of_one_account_does_not_block_other_accounts(mod, monkeypatch):
    first=manager(mod)
    second=mod._cgpt_build_manager_from_doc({'_id':'another-account',
        'data':{'user':{'email':'another-owner@example.test'}}})
    started=threading.Event();release=threading.Event()
    def slow():
        started.set()
        assert release.wait(4)
        return []
    monkeypatch.setattr(first,'_get_org_users',slow)
    monkeypatch.setattr(second,'_get_org_users',lambda:[])
    with ThreadPoolExecutor(max_workers=2) as pool:
        one=pool.submit(first.check_and_cleanup)
        try:
            assert started.wait(2)
            pool.submit(second.check_and_cleanup).result(timeout=2)
        finally:
            release.set()
        one.result(timeout=2)


def test_startup_keeps_pending_payment_updates(mod, monkeypatch):
    class StopPolling(BaseException):pass
    webhook=Mock();poll=Mock(side_effect=StopPolling)
    monkeypatch.setattr(mod.bot,'delete_webhook',webhook)
    monkeypatch.setattr(mod.bot,'polling',poll)
    monkeypatch.setattr(mod.time,'sleep',lambda *a:None)
    monkeypatch.setattr(mod.threading.Thread,'start',lambda *a:None)
    with pytest.raises(StopPolling):mod.run_bot()
    assert webhook.call_args.kwargs['drop_pending_updates'] is False
    assert poll.call_args.kwargs['skip_pending'] is False


def test_original_definitions_are_preserved():
    baseline=json.loads((APP/'verification/baseline-definitions.json').read_text())
    def names(tree):
        found=[]
        def scan(nodes,prefix=''):
            for n in nodes:
                if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):
                    name=prefix+n.name;found.append(name)
                    if isinstance(n,ast.ClassDef):scan(n.body,name+'.')
        scan(tree.body);return set(found)
    assert set(baseline)<=names(ast.parse((APP/'bot.py').read_text(encoding='utf-8-sig')))
