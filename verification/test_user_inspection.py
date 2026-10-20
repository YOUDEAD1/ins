"""Admin inspection regression: real callbacks and signed HTTP, fake integrations."""
import datetime as dt
import json
from unittest.mock import Mock
import pytest
from bson import ObjectId
from harness import MESSAGES
from test_integration import mod,isolate,server,product,request
from test_products_menu import dispatch,buttons

def order(mod,pid='101',uid=1001,ref='ORDER-A',**kw):
    row={'user_id':uid,'product_id':pid,'order_id':ref,'qty':1,'price':5.,
         'code_delivered':'mail@example.test:PASS-SECRET','via':'bot',**kw}
    row['_id']=mod.db.orders.insert_one(row).inserted_id
    return row

def mixed(mod):
    product(mod)
    order(mod,ref='BULK',code_delivered='A1',price=4.)
    order(mod,ref='BULK',code_delivered='A2',price=4.,via='webapp')
    order(mod,ref='API-ONE',code_delivered='API: API-ONE',via_api=True,price=9.,qty=2)
    mod.db.api_orders.insert_one({'api_user_id':1001,'product_id':'101','order_id':'API-ONE',
        'qty':2,'total_price':9.,'codes':['API-CODE-1','API-CODE-2'],'status':'completed'})
    ext=ObjectId()
    mod.db.ext_orders.insert_one({'user_id':1001,'ext_product_id':str(ext),'order_id':'SUPPLIER',
        'product_name':'External Product','quantity':1,'price':3.,'codes':['EXT-ACCOUNT'],
        'status':'success','via':'webapp','created_at':1700000000})
    order(mod,uid=1002,ref='BULK',code_delivered='OTHER-USER-SECRET')

def vc(mod,action,**kw):
    return mod._wa_admin_vc({'id':9001,'first_name':'Owner'},{'action':action,**kw})[1]

def page(data):
    return next(m for m in reversed(data['msgs']) if m['from']=='bot' and m['id']>=data.get('section_start',0) and m['buttons'])

def press(mod,data,cb):
    m=next(m for m in reversed(data['msgs']) if any(b.get('v')==cb for row in m['buttons'] for b in row))
    return vc(mod,'press',mid=m['id'],data=cb)

def test_mixed_ledgers_show_products_once_and_merge_api_projection(mod):
    mixed(mod)
    data=mod._up_products(1001)
    assert data['summary']=={'orders':3,'products':2,'qty':5,'spent':20.,'unknown_prices':0}
    assert sorted(p['orders'] for p in data['products'])==[1,2]
    serialized=json.dumps(data)
    for private in ['PASS-SECRET','A1','API-CODE-1','EXT-ACCOUNT','OTHER-USER-SECRET']:
        assert private not in serialized
    rows=mod._up_dataset(1001)
    bulk=next(r for r in rows if r['ref']=='BULK')
    assert bulk['qty']==2 and bulk['total']==8
    api=next(r for r in rows if r['ref']=='API-ONE')
    detail=mod._up_detail(1001,api['key'])
    assert detail['codes']==['API-CODE-1','API-CODE-2'] and detail['total']==9

@pytest.mark.parametrize('uid',[1001,'1001'])
def test_legacy_owner_and_product_id_types_are_supported(mod,uid):
    product(mod)
    order(mod,pid=101,uid=uid,ref=None)
    assert mod._up_products(1001)['products'][0]['orders']==1

def test_same_order_id_on_distinct_products_is_not_merged(mod):
    order(mod,pid='101');order(mod,pid='102')
    assert mod._up_products(1001)['summary']['orders']==2

def test_distinct_identical_codes_are_not_deduplicated_as_distinct_sales(mod):
    order(mod,ref='FIRST');order(mod,ref='SECOND')
    assert mod._up_products(1001)['summary']['orders']==2

def test_supplier_and_api_same_reference_count_as_one_transaction(mod):
    pid=ObjectId()
    mod.db.api_orders.insert_one({'api_user_id':1001,'product_id':'ext_'+str(pid),'order_id':'LINK',
        'qty':2,'total_price':7.,'codes':['X','Y'],'status':'completed'})
    mod.db.ext_orders.insert_one({'user_id':1001,'ext_product_id':str(pid),'order_id':'LINK',
        'quantity':2,'price':7.,'codes':['X','Y'],'status':'success'})
    data=mod._up_products(1001)
    assert data['summary']['orders']==1 and data['summary']['spent']==7
    row=mod._up_dataset(1001)[0]
    assert mod._up_detail(1001,row['key'])['codes']==['X','Y']

def test_full_history_beyond_old_15_and_120_caps_has_pagination(mod):
    product(mod)
    for n in range(145):order(mod,ref='OLD-'+str(n))
    data=mod._up_products(1001)
    records=mod._up_product(1001,data['products'][0]['key'],99)
    assert records['summary']['orders']==145 and records['page']==records['pages']==13
    assert len(records['records'])==1
    text=mod._up_export(1001)
    assert all('Order: OLD-'+str(n)+'\n' in text for n in range(145))

def test_product_pages_have_stable_order_and_no_credential_fields(mod):
    for n in range(16):order(mod,pid=str(n+1),ref=str(n))
    a=mod._up_products(1001,1);b=mod._up_products(1001,2)
    assert len(a['products'])==12 and len(b['products'])==4
    assert set(p['key'] for p in a['products']).isdisjoint(p['key'] for p in b['products'])
    assert not any(k.startswith('_') for p in a['products'] for k in p)

def test_deleted_product_snapshot_and_multiline_credentials_are_preserved(mod):
    order(mod,pid='deleted-product',product_name='Archived Product',code_delivered='mail@example.test\nPassword: <secret>&\nRecovery: X')
    data=mod._up_products(1001)
    assert data['products'][0]['name']=='Archived Product'
    row=mod._up_dataset(1001)[0]
    detail=mod._up_detail(1001,row['key'])
    assert detail['codes']==['mail@example.test\nPassword: <secret>&\nRecovery: X']
    assert 'Password: <secret>&' in mod._up_export(1001,row['key'])

def test_legacy_missing_price_is_explicit_and_not_current_product_price(mod):
    product(mod,price=100.)
    d=order(mod);mod.db.orders.update_one({'_id':d['_id']},{'$unset':{'price':''}})
    detail=mod._up_detail(1001,mod._up_dataset(1001)[0]['key'])
    assert detail['total']==0 and not detail['price_known']
    assert 'السعر التاريخي غير محفوظ' in mod._up_export(1001)

@pytest.mark.parametrize('state',['failed','cancelled','refunded','pending_verification'])
def test_supplier_status_is_honest_and_failures_not_added_to_spent(mod,state):
    mod.db.ext_orders.insert_one({'user_id':1001,'ext_product_id':'old','price':6.,'status':state})
    data=mod._up_products(1001)
    assert data['summary']['orders']==1
    assert data['summary']['spent']==(6. if state=='pending_verification' else 0.)
    assert mod._up_dataset(1001)[0]['status']==('verifying' if state=='pending_verification' else state)

@pytest.mark.parametrize('stored_pid',['raw','cgpt','cgpt_main',''])
def test_subscription_current_expiry_renewal_and_private_cookie_exclusion(mod,stored_pid):
    pid=ObjectId();base='cgpt_'+str(pid)
    mod.db.cgpt_products.insert_one({'_id':pid,'name':'ChatGPT Business','durations':[]})
    order(mod,pid=base,ref='FIRST',code_delivered='chatgpt_seat:mail@example.test',
        cgpt_expires_at='2026-01-01T00:00:00',cgpt_email='mail@example.test')
    order(mod,pid=base,ref='RENEW',type='cgpt_renewal',code_delivered='chatgpt_seat:mail@example.test')
    future=(dt.datetime.now(dt.timezone.utc)+dt.timedelta(days=10)).isoformat()
    variant={'raw':str(pid),'cgpt':base,'cgpt_main':'cgpt_main_'+str(pid),'':''}[stored_pid]
    mod.db.cgpt_invites_data.insert_one({'_id':'workspace','cookie':'OWNER-COOKIE-SECRET','data':{'invites':{
        'mail@example.test':{'telegram_uid':'1001','status':'active','expires_at':future,'product_id':variant},
        'other@example.test':{'telegram_uid':1002,'status':'active','expires_at':future}}}})
    detail=mod._up_detail(1001,mod._up_dataset(1001)[0]['key'])
    assert detail['subscriptions'][0]['expires']==future
    assert detail['subscriptions'][0]['status']=='active'
    assert detail['subscriptions'][0]['remaining_seconds']>9*86400
    assert 'OWNER-COOKIE-SECRET' not in json.dumps(detail) and 'other@example.test' not in json.dumps(detail)
    assert mod._up_products(1001)['summary']['products']==1

def test_elapsed_subscription_is_not_reported_active_and_missing_one_not_invented(mod):
    pid='cgpt_'+str(ObjectId())
    order(mod,pid=pid,code_delivered='chatgpt_seat:mail@example.test')
    row=mod._up_dataset(1001)[0]
    assert mod._up_detail(1001,row['key'])['subscriptions']==[]
    mod.db.cgpt_invites_data.insert_one({'_id':'x','data':{'invites':{
        'mail@example.test':{'telegram_uid':1001,'status':'active','expires_at':'2020-01-01T00:00:00Z'}}}})
    detail=mod._up_detail(1001,row['key'])
    assert detail['subscriptions'][0]['status']=='expired'
    assert detail['subscriptions'][0]['stored_status']=='active'

def test_codes_paginate_but_export_preserves_every_account(mod):
    mod.db.api_orders.insert_one({'api_user_id':1001,'product_id':'101','order_id':'BIG',
        'qty':73,'total_price':73.,'codes':['CODE-'+str(n) for n in range(73)],'status':'completed'})
    row=mod._up_dataset(1001)[0]
    detail=mod._up_detail(1001,row['key'],4)
    assert len(detail['codes'])==13 and detail['code_pages']==4
    text=mod._up_export(1001,row['key'])
    assert all('CODE-'+str(n)+'\n' in text for n in range(73))

def test_export_does_not_reload_entire_user_ledger_per_order(mod,monkeypatch):
    for n in range(25):order(mod,ref=str(n))
    original=mod._up_dataset;spy=Mock(wraps=original);monkeypatch.setattr(mod,'_up_dataset',spy)
    assert mod._up_export(1001)
    assert spy.call_count==1

@pytest.mark.parametrize('route',['purchases','purchases/product','purchases/detail','purchases/export'])
@pytest.mark.parametrize('uid',[1001,1002])
def test_native_routes_require_admin(server,route,uid):
    status,_,_=request(server,'/webapp/admin/user/'+route,{'uid':1001,'key':'a'*24},uid=uid)
    assert status==403

def test_signed_http_drilldown_and_scoped_keys_do_not_return_other_user(server,mod):
    mixed(mod)
    s,data,_=request(server,'/webapp/admin/user/purchases',{'uid':1001},uid=9001)
    assert s==200 and data['summary']['orders']==3
    s,records,_=request(server,'/webapp/admin/user/purchases/product',{'uid':1001,'key':data['products'][0]['key']},uid=9001)
    assert s==200 and records['records']
    key=records['records'][0]['key']
    for route in ['detail','export']:
        s,d,_=request(server,'/webapp/admin/user/purchases/'+route,{'uid':1002,'key':key},uid=9001)
        assert s==404 and 'OTHER-USER-SECRET' not in json.dumps(d)
    s,profile,_=request(server,'/webapp/admin/user',{'uid':1001},uid=9001)
    assert s==200 and profile['user']['orders']==3 and profile['purchases']['summary']['products']==2

@pytest.mark.parametrize('body',[{'uid':'bad'},{'uid':-1},{'uid':1001,'page':'x'},{'uid':1001,'page':{} }])
def test_bad_arguments_return_400(server,body):
    assert request(server,'/webapp/admin/user/purchases',body,uid=9001)[0]==400

def test_bot_product_buttons_detail_escape_and_back_page_context(mod,monkeypatch):
    product(mod,name_en='Netflix Premium')
    order(mod,code_delivered='<script>alert(1)</script>\nmail:pass')
    monkeypatch.setitem(mod._VC_ORIG,'edit_message_text',lambda text,chat_id,message_id,**kw: __import__('harness').reply(chat_id,text,**kw))
    dispatch(mod,'ad_uh_buy_1001',9001)
    b=next(b for row in buttons(MESSAGES[-1]) for b in row if b.get('callback_data','').startswith('ad_up_'))
    assert 'Netflix Premium' in b['text'] and len(b['callback_data'].encode())<=64
    dispatch(mod,b['callback_data'],9001)
    b=next(b for row in buttons(MESSAGES[-1]) for b in row if ':d:' in b.get('callback_data',''))
    dispatch(mod,b['callback_data'],9001)
    assert '&lt;script&gt;' in MESSAGES[-1][1] and '<script>' not in MESSAGES[-1][1]
    back=next(b['callback_data'] for row in buttons(MESSAGES[-1]) for b in row if ':p:' in b.get('callback_data',''))
    dispatch(mod,back,9001)
    assert 'سجلات' in MESSAGES[-1][1] or 'عمليات' in MESSAGES[-1][1] or 'عملية' in MESSAGES[-1][1]

def test_vc_member_back_restores_prior_screens_without_executing_financial_action(mod):
    order(mod)
    start=vc(mod,'open',cb='ad_u_det_1001')
    data=press(mod,start,'ad_ugift_1001')
    data=press(mod,data,'ad_gift_comp_1001')
    assert data['step']
    balance=mod.db.users.find_one({'user_id':1001})['balance']
    data=vc(mod,'back')
    assert not data['step'] and 'اختر نوع تعديل الرصيد' in page(data)['html']
    data=vc(mod,'back')
    assert 'ملف العميل' in page(data)['html']
    assert mod.db.users.find_one({'user_id':1001})['balance']==balance
    assert mod.db.balance_logs.count_documents({})==0
    assert vc(mod,'back')['close_section']

def test_vc_new_section_does_not_choose_old_section_page_and_all_read_routes_survive(mod):
    previous=vc(mod,'open',cb='ad_users_main')
    current=vc(mod,'open',cb='ad_u_det_1001')
    assert current['section_start']>previous['section_start']
    assert page(current)['id']>=current['section_start']
    assert not current['back_available']

def test_vip_save_returns_back_to_profile_once_without_restoring_stale_settings_page(mod):
    data=vc(mod,'open',cb='ad_u_det_1001')
    data=press(mod,data,'ad_vipu_1001')
    data=press(mod,data,'ad_vipu_ov_1001')
    data=press(mod,data,'ad_vipu_set_1001_0')
    assert mod.db.users.find_one({'user_id':1001})['vip_override']==0
    data=vc(mod,'back')
    assert 'ملف العميل' in page(data)['html']
    assert mod.db.users.find_one({'user_id':1001})['vip_override']==0

def test_returning_to_member_profile_clears_pending_input_and_preserves_top_list_origin(mod,monkeypatch):
    session=mod._vc_session({'id':9001})
    session.step=(Mock(),(),{})
    mod._VCTX.s=session
    try:
        mod.show_user_admin_profile(9001,1001)
        assert session.step is None
        mod._up_member_origin(9001,1001,'ad_u_top')
        mod.show_user_admin_profile(9001,1001)
        assert any(b['v']=='ad_u_top' for row in page(session.view())['buttons'] for b in row)
    finally:mod._VCTX.s=None

def test_revoked_admin_cannot_restore_console_or_access_deliveries(server,mod):
    mod.db.users.update_one({'user_id':1002},{'$set':{'is_admin':1}})
    assert request(server,'/webapp/admin/user/purchases',{'uid':1001},uid=1002)[0]==200
    mod.db.users.update_one({'user_id':1002},{'$set':{'is_admin':0}});mod._invalidate_user_cache(1002)
    assert request(server,'/webapp/admin/vc',{'action':'back'},uid=1002)[0]==403

