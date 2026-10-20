"""Cart regressions: in-memory DB and mocked Telegram/suppliers only."""
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
import pytest
from test_integration import mod, isolate, server, product, request
from harness import MESSAGES


def quote(server,items,uid=1001):
    s,d,_=request(server,'/webapp/cart/quote',{'items':items},uid=uid)
    assert s==200,d
    return d


def checkout(server,q,uid=1001):
    return request(server,'/webapp/cart/checkout',{'quote_id':q['quote_id']},uid=uid)[:2]


def test_cart_delivers_multiple_products_through_original_bot(mod,server):
    product(mod,stock=5);product(mod,'102',price=3,stock=4)
    q=quote(server,[{'product_id':'101','qty':2},{'product_id':'102','qty':3}])
    assert q['total']==19 and mod._wa_balance(1001)==100 and not mod.db.orders.count_documents({})
    s,r=checkout(server,q)
    assert s==200 and r['status']=='completed' and r['spent']==19 and r['balance']==81
    assert len([c for x in r['lines'] for c in x['result']['codes']])==5
    assert mod.db.orders.count_documents({'cart_checkout_id':q['quote_id']})==5
    assert mod.db.product_stock.count_documents({'is_sold':False})==4
    assert any(uid==1001 and text=='[document]' for uid,text,kw in MESSAGES)


def test_duplicate_lines_bulk_discount_and_vip(mod,server):
    product(mod,stock=10,discount_tiers=[{'min_qty':3,'price':4}])
    mod.db.users.update_one({'user_id':1001},{'$set':{'vip_cents':30000}});mod.vip_set_class('101','high')
    q=quote(server,[{'product_id':'101','qty':1},{'product_id':'101','qty':2}])
    expected=mod.vip_quote(1001,'101',4,3)['total']
    assert len(q['lines'])==1 and q['total']==expected and q['subtotal']==15
    assert checkout(server,q)[1]['spent']==expected and mod._wa_balance(1001)==100-expected


@pytest.mark.parametrize('change',['price','tier','vip','stock','hidden'])
def test_quote_changes_require_new_confirmation(mod,server,change):
    product(mod,stock=6);q=quote(server,[{'product_id':'101','qty':3}])
    if change=='price':mod.db.products.update_one({'id':'101'},{'$set':{'price':9}})
    if change=='tier':mod.db.products.update_one({'id':'101'},{'$set':{'discount_tiers':[{'min_qty':3,'price':4}]}})
    if change=='vip':
        mod.db.users.update_one({'user_id':1001},{'$set':{'vip_cents':30000}});mod._invalidate_user_cache(1001);mod.vip_set_class('101','high')
    if change=='stock':mod.db.product_stock.update_many({},{'$set':{'is_sold':True}})
    if change=='hidden':mod.db.products.update_one({'id':'101'},{'$set':{'is_hidden':True}})
    mod._invalidate_products_cache()
    s,r=checkout(server,q)
    assert s==409 and r['error'] in ('price_changed','cart_changed')
    assert mod._wa_balance(1001)==100 and not mod.db.orders.count_documents({})


def test_last_moment_price_change_does_not_debit(mod,server,monkeypatch):
    product(mod);q=quote(server,[{'product_id':'101','qty':1}]);original=mod._cart_execute
    def execute(uid,line,lang):
        mod.db.products.update_one({'id':'101'},{'$set':{'price':40}})
        return original(uid,line,lang)
    monkeypatch.setattr(mod,'_cart_execute',execute)
    s,r=checkout(server,q)
    assert s==200 and r['lines'][0]['result']['error']=='price_changed'
    assert r['spent']==0 and mod._wa_balance(1001)==100
    assert mod.db.product_stock.count_documents({'is_sold':False})==3


def test_parallel_and_repeated_confirmation_charge_once(mod,server):
    product(mod,stock=5);q=quote(server,[{'product_id':'101','qty':2}])
    with ThreadPoolExecutor(max_workers=3) as pool:results=list(pool.map(lambda _:checkout(server,q),range(3)))
    assert all(s in (200,202,429) for s,r in results)
    s,r=checkout(server,q)
    assert s==200 and r['status']=='completed' and r['spent']==10
    assert mod._wa_balance(1001)==90 and mod.db.orders.count_documents({})==2


def test_entire_cart_balance_checked_before_first_purchase(mod,server):
    product(mod,price=60);product(mod,'102',price=60)
    q=quote(server,[{'product_id':'101','qty':1},{'product_id':'102','qty':1}])
    assert q['valid'] and not q['can_checkout']
    assert checkout(server,q)[0]==402 and mod._wa_balance(1001)==100
    assert not mod.db.orders.count_documents({})


@pytest.mark.parametrize('items',[[],None,{},[None],[{'product_id':'101','qty':True}],
    [{'product_id':'101','qty':1.5}],[{'product_id':'101','qty':0}],[{'product_id':'101','qty':501}],
    [{'product_id':'../101','qty':1}],[{'product_id':'101','qty':1}]*21])
def test_bad_cart_input_never_changes_balance(mod,server,items):
    product(mod);s,d,_=request(server,'/webapp/cart/quote',{'items':items})
    assert s==400 and mod._wa_balance(1001)==100


@pytest.mark.parametrize('gate',['banned','forced','deposit','disabled'])
def test_checkout_access_gates(mod,server,monkeypatch,gate):
    product(mod);q=quote(server,[{'product_id':'101','qty':1}])
    if gate=='banned':mod.db.users.update_one({'user_id':1001},{'$set':{'is_banned':1}})
    if gate=='forced':monkeypatch.setattr(mod,'check_forced_sub',lambda uid:False)
    if gate=='deposit':mod.db.users.update_one({'user_id':1001},{'$set':{'deposit_locked':True,'deposit_lock_expires':int(mod.time.time())+600}})
    if gate=='disabled':mod.db.settings.update_one({'key':'webapp_cart_enabled'},{'$set':{'value':'off'}},upsert=True)
    assert checkout(server,q)[0] in (403,409)
    assert mod._wa_balance(1001)==100 and not mod.db.orders.count_documents({})


def test_quote_ownership_expiry_and_cancel(mod,server):
    product(mod);q=quote(server,[{'product_id':'101','qty':1}])
    assert checkout(server,q,1002)[0]==404
    assert request(server,'/webapp/cart/status',{'checkout_id':q['quote_id']},uid=1002)[0]==404
    mod.db.cart_checkouts.update_one({'_id':q['quote_id']},{'$set':{'quote_expires':0}})
    assert checkout(server,q)[1]['error']=='quote_expired'
    q=quote(server,[{'product_id':'101','qty':1}])
    assert request(server,'/webapp/cart/cancel',{'checkout_id':q['quote_id']})[1]['status']=='cancelled'
    assert checkout(server,q)[1]['status']=='cancelled' and mod._wa_balance(1001)==100


def test_manual_email_and_chatgpt_invitation(mod,server,monkeypatch):
    product(mod,stock=0,manual=True,requires_email=True)
    pid=mod.db.cgpt_products.insert_one({'name':'ChatGPT','durations':[{'dur_id':'w','minutes':10080,'price':7,'label':'Week'}]}).inserted_id
    mgr=Mock();mgr.invite_user.return_value={'ok':True,'expires_at':'2026-11-01T00:00:00'}
    monkeypatch.setattr(mod,'_cgpt_find_available_account',lambda:{'mgr':mgr})
    q=quote(server,[{'product_id':'101','qty':2,'email':'customer@example.test'},
        {'product_id':f'cgpt_{pid}','qty':1,'email':'invite@example.test','dur_id':'w'}])
    assert q['valid'] and q['total']==17
    s,r=checkout(server,q)
    assert s==200 and r['status']=='completed' and r['spent']==17
    assert [x['result']['status'] for x in r['lines']]==['manual','chatgpt']
    assert mod.db.orders.find_one({'buyer_email':'customer@example.test'})
    mgr.invite_user.assert_called_once_with('invite@example.test',10080)


@pytest.mark.parametrize('email,qty,dur',[('bad',1,'w'),('ok@example.test',2,'w'),('ok@example.test',1,'bad')])
def test_chatgpt_configuration_validation(mod,server,email,qty,dur):
    pid=mod.db.cgpt_products.insert_one({'name':'GPT','durations':[{'dur_id':'w','minutes':10080,'price':7}]}).inserted_id
    q=quote(server,[{'product_id':f'cgpt_{pid}','qty':qty,'email':email,'dur_id':dur}])
    assert not q['valid'] and not q['quote_id'] and mod._wa_balance(1001)==100


def test_hidden_parent_and_combined_seat_demand(mod,server,monkeypatch):
    pid=mod.db.cgpt_products.insert_one({'name':'GPT','is_hidden':True,'durations':[{'dur_id':'w','minutes':10080,'price':7}]}).inserted_id
    items=[{'product_id':f'cgpt_{pid}','qty':1,'email':e,'dur_id':'w'} for e in ['a@example.test','b@example.test']]
    assert not quote(server,items[:1])['valid']
    mod.db.cgpt_products.update_one({'_id':pid},{'$set':{'is_hidden':False}});monkeypatch.setattr(mod,'_cgpt_display_seats',lambda:1)
    assert all(x['error']=='not_enough_stock' for x in quote(server,items)['lines'])


def test_partial_checkout_keeps_exact_charge_and_stops_remaining_items(mod,server,monkeypatch):
    for pid in ('101','102','103'):product(mod,pid)
    q=quote(server,[{'product_id':p,'qty':1} for p in ('101','102','103')]);original=mod._cart_execute
    monkeypatch.setattr(mod,'_cart_execute',lambda uid,line,lang:(502,{'error':'supplier_failed','refunded':True}) if line['product_id']=='102' else original(uid,line,lang))
    s,r=checkout(server,q)
    assert s==200 and r['status']=='partial' and r['spent']==5 and r['balance']==95
    assert r['lines'][0]['result']['ok'] and 'result' not in r['lines'][2]
    checkout(server,q)
    assert mod._wa_balance(1001)==95 and mod.db.orders.count_documents({})==1


def test_supplier_uncertain_result_stops_and_is_not_repeated(mod,server,monkeypatch):
    sid=mod.db.ext_stores.insert_one({'base_url':'https://supplier.example','api_key':'fake-only'}).inserted_id
    eid=mod.db.ext_products.insert_one({'store_id':str(sid),'ext_id':'p1','name':'Remote','stock':5,'sell_price':4,'base_price':2}).inserted_id
    product(mod,'102');calls=Mock(return_value=(False,{'uncertain':True,'error':'timeout'}));monkeypatch.setattr(mod,'_ext_api_post',calls)
    q=quote(server,[{'product_id':f'ext_{eid}','qty':1},{'product_id':'102','qty':1}])
    s,r=checkout(server,q)
    assert s==200 and r['status']=='verifying' and r['spent']==4 and r['balance']==96
    assert 'result' not in r['lines'][1]
    checkout(server,q);assert calls.call_count==1 and mod._wa_balance(1001)==96


def test_receipt_write_failure_recovers_real_orders_without_rebuy(mod,server,monkeypatch):
    product(mod);q=quote(server,[{'product_id':'101','qty':2}]);original=mod.db.cart_checkouts.update_one
    def fail(query,update,*a,**k):
        if 'lines.0.result' in update.get('$set',{}):raise RuntimeError('receipt write interruption')
        return original(query,update,*a,**k)
    monkeypatch.setattr(mod.db.cart_checkouts,'update_one',fail)
    assert checkout(server,q)[0]==500 and mod._wa_balance(1001)==90
    monkeypatch.setattr(mod.db.cart_checkouts,'update_one',original)
    s,r,_=request(server,'/webapp/cart/status',{'checkout_id':q['quote_id']})
    assert s==200 and r['status']=='completed' and len(r['lines'][0]['result']['codes'])==2
    checkout(server,q);assert mod._wa_balance(1001)==90 and mod.db.orders.count_documents({})==2


def test_client_price_and_owner_fields_are_ignored(mod,server):
    product(mod);q=quote(server,[{'product_id':'101','qty':2,'price':0,'total':0}]);assert q['total']==10
    s,r,_=request(server,'/webapp/cart/checkout',{'quote_id':q['quote_id'],'total':0,'user_id':1002})
    assert s==200 and r['spent']==10 and mod._wa_balance(1001)==90 and mod._wa_balance(1002)==50


def test_unlimited_supplier_product_can_be_added_and_delivered(mod,server,monkeypatch):
    sid=mod.db.ext_stores.insert_one({'base_url':'https://supplier.example','api_key':'fake-only'}).inserted_id
    eid=mod.db.ext_products.insert_one({'store_id':str(sid),'ext_id':'unlimited','name':'Remote unlimited','stock':0,'unlimited':True,'sell_price':4,'base_price':2}).inserted_id
    monkeypatch.setattr(mod,'_ext_api_post',lambda *a,**k:(True,{'success':True,'codes':['LOCAL-REMOTE-1','LOCAL-REMOTE-2'],'order':{'id':'fake-order'}}))
    q=quote(server,[{'product_id':f'ext_{eid}','qty':2}])
    assert q['valid'] and q['lines'][0]['stock'] is None
    s,r=checkout(server,q)
    assert s==200 and r['status']=='completed' and r['spent']==8
    assert r['lines'][0]['result']['codes']==['LOCAL-REMOTE-1','LOCAL-REMOTE-2'] and mod._wa_balance(1001)==92
