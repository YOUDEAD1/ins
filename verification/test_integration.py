import ast
import base64
import hashlib
import hmac
import http.client
import io
import json
import pathlib
import re
import threading
import time
import types
import urllib.parse
from http.server import ThreadingHTTPServer
from unittest.mock import patch
import pytest
import requests
from bson import ObjectId
from harness import load, TOKEN, MESSAGES, ROOT, APP, offline

@pytest.fixture(scope='session')
def mod():
    return load()

@pytest.fixture(autouse=True)
def isolate(mod,monkeypatch):
    for name in mod.db.list_collection_names():
        mod.db[name].delete_many({})
    mod.db.users.insert_many([
        {'user_id':1001,'name':'Test User','username':'customer','balance':100.,'lang':'en','is_admin':0,'is_banned':0},
        {'user_id':1002,'name':'Other User','balance':50.,'lang':'ar','is_admin':0,'is_banned':0},
        {'user_id':9001,'name':'Owner','balance':0.,'lang':'ar','is_admin':1,'is_banned':0}])
    settings={'wallet_address':'12345678','usdt_address':'TTESTADDRESS','usdt_bep20_address':'0xTESTADDRESS',
        'ton_address':'EQTESTADDRESS','ltc_address':'ltc1TESTADDRESS','amount_protection':'on',
        'server_url':'https://local.example','api_gateway':'local_test_api'}
    for k,v in settings.items():mod.db.settings.insert_one({'key':k,'value':v})
    for n in ['_USER_CACHE','_VIP_STATE_CACHE','_WA_CAT_CACHE','_WA_RATE','_WA_BUY_LOCKS','_cgpt_pending','_purchase_locks','_VC_SESS']:
        getattr(mod,n).clear()
    for n in ('_CGPT_STOCK_CACHE', '_CGPT_CAPACITY_CACHE', '_FORCED_SUB_CACHE', '_FSUB_MEMBER_CACHE'):
        getattr(mod, n).clear()
    mod._FSUB_CHANNEL_CACHE.update(expires=0, rows=[])
    mod._REF_SCAN_HEALTH['retry_at'] = 0
    mod._REF_SCAN_WAKE.clear()
    mod._STOCK_VISIBILITY_STATE.update(last_success=0, retry_at=0)
    mod._PRODUCTS_CACHE=None
    mod._invalidate_products_cache()
    mod._VIP_CFG['v']=None
    mod._CUSTOM_TEXTS_CACHE={};mod._CUSTOM_TEXTS_LOADED=[False]
    mod._WA_BTN_CACHE.update(rows=None,exp=0)
    MESSAGES.clear()
    monkeypatch.setattr(requests.sessions.Session,'request',offline)
    monkeypatch.setattr(mod.cffi_requests,'get',offline)
    monkeypatch.setattr(mod.cffi_requests,'post',offline)
    monkeypatch.setattr(mod.cffi_requests,'delete',offline)
    monkeypatch.setattr(mod,'get_ton_price_usd',lambda:5.)
    monkeypatch.setattr(mod,'get_ltc_price_usd',lambda:100.)
    monkeypatch.setattr(mod,'bybit_is_configured',lambda:True)
    monkeypatch.setattr(mod,'_bybit_creds',lambda:('testkey','testsecret','123456'))
    # Real Telegram API calls are forbidden in this harness.
    monkeypatch.setattr(mod.telebot.apihelper,'_make_request',offline)
    yield

@pytest.fixture(scope='session')
def server(mod):
    srv=ThreadingHTTPServer(('127.0.0.1',0),mod.APIHandler)
    t=threading.Thread(target=srv.serve_forever,daemon=True);t.start()
    yield srv.server_address
    srv.shutdown();srv.server_close();t.join()

def signed(uid=1001,**extra):
    obj={'auth_date':str(int(time.time())),'user':json.dumps({'id':uid,'first_name':'Test','language_code':'en'},separators=(',',':'))}
    obj.update(extra)
    check='\n'.join(f'{k}={obj[k]}' for k in sorted(obj))
    secret=hmac.new(b'WebAppData',TOKEN.encode(),hashlib.sha256).digest()
    obj['hash']=hmac.new(secret,check.encode(),hashlib.sha256).hexdigest()
    return urllib.parse.urlencode(obj)

def request(server,path,body=None,uid=1001,method='POST',auth=True,headers=None):
    h={'Content-Type':'application/json'}
    if auth:h['X-Telegram-Init-Data']=signed(uid)
    h.update(headers or {})
    conn=http.client.HTTPConnection(*server,timeout=4)
    data=json.dumps(body if body is not None else {}) if method=='POST' else None
    conn.request(method,path,data,h)
    res=conn.getresponse();raw=res.read();status=res.status;rh=dict(res.getheaders());conn.close()
    try:payload=json.loads(raw)
    except (ValueError,UnicodeDecodeError):payload=raw
    return status,payload,rh

def product(mod,pid='101',price=5.,stock=3,manual=False,**extra):
    p={'id':pid,'name_ar':'منتج','name_en':'Test Product','price':price,'is_manual':manual,'is_hidden':False,**extra}
    mod.db.products.insert_one(p)
    for i in range(stock):mod.db.product_stock.insert_one({'product_id':pid,'is_sold':False,'code_line':f'{pid}-SECRET-{i}'})
    mod._PRODUCTS_CACHE=None;mod._invalidate_products_cache();mod._wa_invalidate_catalog()
    return p

READ_ROUTES=['bootstrap','catalog','me','forced/check','orders','wallet','history','deposit/status','referrals','terms','api/state','api/orders','api/products']
@pytest.mark.parametrize('path',READ_ROUTES)
def test_read_routes(server,path):
    status,data,_=request(server,'/webapp/'+path)
    assert status==200,(path,data)
    assert data.get('ok'),(path,data)

@pytest.mark.parametrize('path',READ_ROUTES+['buy','deposit/create','admin/home'])
def test_anonymous_rejected(server,path):
    s,d,_=request(server,'/webapp/'+path,auth=False)
    assert s==401,(path,d)

@pytest.mark.parametrize('v',[None,'','bad','user=broken&hash=0'])
def test_invalid_auth(mod,v):assert mod._wa_verify(v) is None

def test_valid_and_tampered_signature(mod):
    assert mod._wa_verify(signed())['id']==1001
    assert mod._wa_verify(signed().replace('Test','Fake')) is None
    assert mod._wa_verify(signed(auth_date=str(int(time.time())-90000))) is None
    assert mod._wa_verify(signed(auth_date=str(int(time.time())+600))) is None
    assert mod._wa_verify(signed()+'&auth_date=1') is None

@pytest.mark.parametrize('route',['home','search','order','user','user/balance','user/ban','products','product','product/update','product/stock','feed','open','protection','vc'])
def test_customer_cannot_admin(server,route):
    s,d,_=request(server,'/webapp/admin/'+route)
    assert s==403,(route,d)

@pytest.mark.parametrize('route,body',[('home',{}),('products',{}),('search',{'q':'customer'}),('user',{'uid':1001}),('feed',{}),('vc',{'action':'state'})])
def test_owner_reads(server,route,body):
    s,d,_=request(server,'/webapp/admin/'+route,body,uid=9001)
    assert s==200,(route,d)

@pytest.mark.parametrize('path,mime',[('/app/','text/html'),('/app/logo.webp','image/webp'),('/app/emoji/cart.webp','image/webp'),('/app/emoji/static/cart.png','image/png'),('/app/lib/lottie_light.min.js','application/javascript')])
def test_static(server,path,mime):
    s,d,h=request(server,path,method='GET',auth=False)
    assert s==200,(path,d)
    assert h['Content-Type'].startswith(mime)
    assert len(d)>100

def test_ping_and_redirect(server):
    s,d,_=request(server,'/webapp/ping',method='GET',auth=False);assert s==200 and d['webapp'] is True
    s,d,h=request(server,'/app?demo=1',method='GET',auth=False);assert s==301 and h['Location']=='/app/?demo=1'

@pytest.mark.parametrize('path',['/app/../../bot.py','/app/%2e%2e/bot.py','/app/missing.png'])
def test_static_nonexistent(server,path):
    s,d,_=request(server,path,method='GET',auth=False);assert s==404

def test_catalog_no_codes_and_hidden(mod,server):
    product(mod);product(mod,'102',is_hidden=True)
    s,d,_=request(server,'/webapp/catalog')
    assert [p['id'] for p in d['products']]==['101']
    assert 'SECRET' not in json.dumps(d)
    assert d['products'][0]['stock']==3

def test_real_purchase_reaches_bot_storage(mod,server):
    product(mod)
    s,d,_=request(server,'/webapp/buy',{'product_id':'101','qty':2})
    assert s==200,d
    assert len(d['codes'])==2 and d['balance']==90 and d['total']==10
    assert mod.db.users.find_one({'user_id':1001})['balance']==90
    orders=list(mod.db.orders.find({'user_id':1001}));assert len(orders)==2
    assert all(o.get('via')=='webapp' for o in orders)
    assert mod.db.product_stock.count_documents({'is_sold':False})==1
    assert any(cid==1001 and txt=='[document]' for cid,txt,kw in MESSAGES)
    s,data,_=request(server,'/webapp/orders');assert s==200 and data

@pytest.mark.parametrize('qty',[0,-1,1001,'bad',None])
def test_invalid_quantities_no_debit(mod,server,qty):
    product(mod);s,d,_=request(server,'/webapp/buy',{'product_id':'101','qty':qty})
    assert s==400,d
    assert mod._wa_balance(1001)==100

def test_insufficient_stock_and_balance(mod,server):
    product(mod,stock=1)
    s,d,_=request(server,'/webapp/buy',{'product_id':'101','qty':2});assert s==409 and mod._wa_balance(1001)==100
    mod.db.users.update_one({'user_id':1001},{'$set':{'balance':1}})
    s,d,_=request(server,'/webapp/buy',{'product_id':'101'});assert s==402
    assert mod.db.product_stock.count_documents({'is_sold':False})==1

def test_manual_purchase_email(mod,server):
    product(mod,manual=True,stock=0,requires_email=True)
    s,d,_=request(server,'/webapp/buy',{'product_id':'101','email':'bad'});assert s==400
    s,d,_=request(server,'/webapp/buy',{'product_id':'101','email':'buyer@example.test'});assert s==200,d
    order=mod.db.orders.find_one({'user_id':1001});assert order['buyer_email']=='buyer@example.test'
    assert mod._wa_balance(1001)==95

@pytest.mark.parametrize('method',['binance','bybit','USDT','USDT_BEP20','TON','LTC','stars'])
def test_create_deposit_and_cancel(mod,server,method):
    s,d,_=request(server,'/webapp/deposit/create',{'method':method,'amount':5})
    assert s==200,d
    if method=='stars':assert d['kind']=='invoice' and d['stars']==600;return
    assert d['kind']=='transfer' and d['pending']['address']
    assert mod.is_deposit_locked(1001)
    s,e,_=request(server,'/webapp/deposit/status',{'id':d['pending']['id']});assert s==200 and e['status']=='pending'
    s,e,_=request(server,'/webapp/deposit/cancel');assert s==200 and not mod.is_deposit_locked(1001)
    assert mod._wa_balance(1001)==100

@pytest.mark.parametrize('amount',['nan','inf','-inf',0,-1,100000])
def test_invalid_deposit_amount(mod,server,amount):
    s,d,_=request(server,'/webapp/deposit/create',{'method':'USDT','amount':amount})
    assert s==400,d
    assert mod.db.pending_deposits.count_documents({})==0

def test_payment_lock_blocks_purchase(mod,server):
    product(mod)
    s,d,_=request(server,'/webapp/deposit/create',{'method':'USDT','amount':5});assert s==200,d
    s,d,_=request(server,'/webapp/buy',{'product_id':'101'});assert s==409 and d['error']=='deposit_pending'
    assert mod._wa_balance(1001)==100

def test_api_key_lifecycle(mod,server):
    s,a,_=request(server,'/webapp/api/generate');assert s==200 and a['key']
    s,d,_=request(server,'/webapp/api/docs',{'lang':'en'});assert s==200 and d['markdown'] and d['prompt']
    s,b,_=request(server,'/webapp/api/regen');assert s==200 and b['key']!=a['key']
    assert mod._get_api_user(a['key'])==(None,None)
    s,c,_=request(server,'/webapp/api/disable');assert s==200 and not c['has_key']

def test_order_privacy(mod,server):
    product(mod)
    s,d,_=request(server,'/webapp/buy',{'product_id':'101'});assert s==200
    s,d,_=request(server,'/webapp/orders',uid=1002);assert s==200 and 'SECRET' not in json.dumps(d)

def test_json_array_rejected(server):
    s,d,_=request(server,'/webapp/bootstrap',body=[]);assert s==400,d

def test_all_frontend_api_literals_have_routes(mod):
    source=(APP/'webapp/index.html').read_text()
    paths=set(re.findall(r"\bapi\(['\"]([^'\"]+)['\"]",source))
    assert paths
    assert not {p for p in paths if '/webapp/'+p not in mod._WA_POST_ROUTES}

@pytest.mark.parametrize('method',['binance','USDT','USDT_BEP20','TON','LTC','bybit'])
def test_unavailable_unique_amount_is_recoverable(mod,server,monkeypatch,method):
    monkeypatch.setattr(mod,'generate_unique_amount_for_user',lambda *a:None)
    s,d,_=request(server,'/webapp/deposit/create',{'method':method,'amount':5})
    assert s==409 and d['error']=='busy_amount',d
    assert mod.db.pending_deposits.count_documents({})==0

@pytest.mark.parametrize('success',[True,False])
def test_chatgpt_purchase_and_refund(mod,server,monkeypatch,success):
    pid=mod.db.cgpt_products.insert_one({'name':'ChatGPT','durations':[{'dur_id':'week','minutes':10080,'price':7,'label':'Week'}]}).inserted_id
    mgr=types.SimpleNamespace(invite_user=lambda email,minutes: {'ok':True,'expires_at':'2026-11-01T00:00:00'} if success else {'ok':False,'error':'Mock invite rejected'})
    monkeypatch.setattr(mod,'_cgpt_find_available_account',lambda:{'mgr':mgr})
    s,d,_=request(server,'/webapp/buy',{'product_id':f'cgpt_{pid}','dur_id':'week','email':'buyer@example.test'})
    assert s==(200 if success else 502),d
    assert mod._wa_balance(1001)==(93 if success else 100)
    assert mod.db.orders.count_documents({})==(1 if success else 0)
    if success:assert mod.db.orders.find_one({}).get('via')=='webapp'

@pytest.mark.parametrize('response_kind',['delivered','rejected','uncertain','no_codes'])
def test_supplier_delivery_or_refund(mod,server,monkeypatch,response_kind):
    sid=mod.db.ext_stores.insert_one({'name':'Mock Supplier','base_url':'https://supplier.example','api_key':'dummy'}).inserted_id
    pid=mod.db.ext_products.insert_one({'name':'Supplier Product','store_id':str(sid),'ext_id':'25','stock':10,'sell_price':4}).inserted_id
    responses={
        'delivered':(True,{'order':{'id':'EXT-1','codes':['SUPPLIER-SECRET']}}),
        'rejected':(False,{'error':'out_of_stock'}),
        'uncertain':(False,{'uncertain':True,'error':'timeout'}),
        'no_codes':(True,{'order':{'id':'EXT-1','status':'processing'}})}
    monkeypatch.setattr(mod,'_ext_api_post',lambda *a,**kw:responses[response_kind])
    s,d,_=request(server,'/webapp/buy',{'product_id':f'ext_{pid}','qty':1})
    if response_kind=='delivered':assert s==200 and d['codes']==['SUPPLIER-SECRET'],d
    elif response_kind=='rejected':assert s==502 and mod._wa_balance(1001)==100,d
    else:assert s==202 and d['status']=='verifying' and mod._wa_balance(1001)==96,d


def test_failed_second_purchase_never_returns_previous_order(mod,server,monkeypatch):
    product(mod,manual=True,stock=0)
    s,a,_=request(server,'/webapp/buy',{'product_id':'101'});assert s==200,a
    before=mod.db.orders.count_documents({})
    monkeypatch.setattr(mod,'_do_purchase',lambda *a,**kw:None)
    s,b,_=request(server,'/webapp/buy',{'product_id':'101'})
    assert s!=200,b
    assert mod.db.orders.count_documents({})==before
    assert mod._wa_balance(1001)==95


def test_cors_allows_frontend_auth_header(server):
    s,d,h=request(server,'/webapp/bootstrap',method='OPTIONS',auth=False)
    assert s==200
    assert 'x-telegram-init-data' in h['Access-Control-Allow-Headers'].lower()


def test_hidden_chatgpt_parent_never_appears_or_sells(mod,server):
    pid=mod.db.cgpt_products.insert_one({'name':'Hidden ChatGPT','is_hidden':True,'durations':[{'dur_id':'w','minutes':10080,'price':7}]}).inserted_id
    product(mod,'201',stock=0,product_type='cgpt_main',cgpt_product_id=str(pid))
    s,d,_=request(server,'/webapp/catalog');assert s==200
    assert not [p for p in d['products'] if p['id']==f'cgpt_{pid}']
    s,d,_=request(server,'/webapp/buy',{'product_id':f'cgpt_{pid}','dur_id':'w','email':'buyer@example.test'})
    assert s==404,d

ADMIN_SECTIONS=['ad_prod_manage','ad_users_main','ad_ban_user','ad_new_admin','ad_gift','ad_logs_all','ad_shop_settings','ad_fsub_list','ad_api_main','ext_api_main','ad_check_tx','ad_cgpt_panel','ad_reports','ad_ref_settings','ad_vip','ad_find','admin_panel_main']
@pytest.mark.parametrize('cb',ADMIN_SECTIONS)
def test_virtual_admin_sections_execute_original_handlers(mod,server,cb,caplog):
    product(mod)
    s,d,_=request(server,'/webapp/admin/vc',{'action':'open','cb':cb},uid=9001)
    assert s==200,d
    assert not [r.message for r in caplog.records if r.levelname=='ERROR'],cb
    assert d.get('msgs'),d
    assert not any('صار خطأ' in t.get('text','') for t in d.get('toasts',[])),d


BOT_ONLY_SECTIONS=['ad_bc','ad_texts_main']
@pytest.mark.parametrize('cb',BOT_ONLY_SECTIONS)
def test_cms_and_broadcast_stay_inside_the_bot(mod,server,cb):
    # التخصيص والبرودكاست من البوت فقط: الموقع يرفض فتحها
    s,d,_=request(server,'/webapp/admin/vc',{'action':'open','cb':cb},uid=9001)
    assert s==403 and d.get('error')=='bot_only',d


def test_supplier_timeout_not_retried_or_refunded(mod,monkeypatch):
    calls=[]
    def timeout(*a,**kw):calls.append(a);raise TimeoutError('test timeout')
    monkeypatch.setattr(mod.requests,'post',timeout)
    ok,resp=mod._ext_api_post({'base_url':'https://supplier.example','api_key':'dummy'},'/orders',{'product_id':25,'quantity':1},'test-idem')
    assert not ok and resp.get('uncertain') is True
    assert len(calls)==1


def test_quantity_and_vip_price_same_in_bot_and_web(mod,server):
    product(mod,price=5,stock=8,discount_tiers=[{'min_qty':3,'price':4}])
    mod.db.users.update_one({'user_id':1001},{'$set':{'vip_cents':30000}})
    mod.vip_set_class('101','high')
    q=mod.vip_quote(1001,'101',mod.vip_qty_unit(mod.find_product('101'),3),3)
    s,d,_=request(server,'/webapp/buy',{'product_id':'101','qty':3})
    assert s==200,d
    assert d['total']==q['total'] and d['balance']==round(100-q['total'],2)


def test_bot_purchase_updates_visible_website_stock(mod,server):
    product(mod,stock=3)
    s,d,_=request(server,'/webapp/catalog');assert d['products'][0]['stock']==3
    mod._do_purchase(1001,'101',1,'en')
    s,d,_=request(server,'/webapp/catalog');assert d['products'][0]['stock']==2


def test_hidden_store_link_does_not_reappear_as_chatgpt(mod,server):
    pid=mod.db.cgpt_products.insert_one({'name':'ChatGPT','durations':[{'dur_id':'w','minutes':10080,'price':7}]}).inserted_id
    product(mod,'201',stock=0,is_hidden=True,product_type='cgpt_main',cgpt_product_id=str(pid))
    s,d,_=request(server,'/webapp/catalog');assert s==200
    assert not [p for p in d['products'] if p['id']==f'cgpt_{pid}']
    s,d,_=request(server,'/webapp/buy',{'product_id':f'cgpt_{pid}','dur_id':'w','email':'buyer@example.test'})
    assert s==404,d


def test_concurrent_deposit_creation_has_one_pending(mod,server,monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    original=mod.generate_unique_amount_for_user
    def delayed(*a):time.sleep(.05);return original(*a)
    monkeypatch.setattr(mod,'generate_unique_amount_for_user',delayed)
    gate=threading.Barrier(2)
    def create():gate.wait();return request(server,'/webapp/deposit/create',{'method':'USDT','amount':5})
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:create(),range(2)))
    assert sorted(r[0] for r in results) in ([200,409],[200,429]),results
    assert mod.db.pending_deposits.count_documents({'user_id':1001,'status':'pending'})==1


def test_duplicate_credit_never_increases_balance_twice(mod):
    mod.credit_user(1001,5,'LOCAL-ONLY-TRANSACTION-UNIQUE-ID-111111','en','Telegram Stars ⭐️',trusted_txid=True)
    assert mod._wa_balance(1001)==105
    mod.credit_user(1001,5,'LOCAL-ONLY-TRANSACTION-UNIQUE-ID-111111','en','Telegram Stars ⭐️',trusted_txid=True)
    assert mod._wa_balance(1001)==105


# ───── round 6: source line in admin notices + referral review at the milestone ─────
def _admin_texts(mod):
    return [d.get('text', '') for d in mod.db.api_purchase_notices.find()]

def test_admin_purchase_notice_says_site_or_bot(mod, server):
    product(mod, stock=4)
    s, d, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 1}); assert s == 200, d
    site = [t for t in _admin_texts(mod) if 'إشعار إدارة (شراء)' in t]
    assert site and 'المصدر: 🌐' in site[-1] and 'المصدر: 💬' not in site[-1]
    assert any(str(c) == '1001' for c, *_ in MESSAGES), 'site purchase must also send the receipt in the chat'
    mod.db.api_purchase_notices.delete_many({})
    mod._do_purchase(1001, '101', 1, 'en')
    bot = [t for t in _admin_texts(mod) if 'إشعار إدارة (شراء)' in t]
    assert bot and 'المصدر: 💬' in bot[-1]

def test_admin_deposit_notice_says_bot(mod):
    mod.credit_user(1001, 5.0, 'TX-ROUND6-1', 'en', 'Binance Pay (Auto)')
    dep = [t for t in _admin_texts(mod) if 'إشعار إدارة (إيداع)' in t]
    assert dep and 'المصدر: 💬' in dep[-1]

def _ref_fp(did, **kw):
    fp = {'ua': 'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 Chrome/129.0 Mobile Safari/537.36', 'plat': 'Linux armv8l',
          'tgp': 'android', 'tgv': '8.0', 'tz': 'Asia/Riyadh', 'langs': 'ar', 'hw': 8, 'mem': 8, 'touch': 5, 'scr': '384x832x2.81x24',
          'wd': False, 'did': did, 'gl': 'Qualcomm|Adreno (TM) 740', 'cv': did}
    fp.update(kw); return fp

def _verified_ref_request(mod, server, uid, fp, ch):
    status, challenge, _ = request(server, '/webapp/verify/challenge', uid=uid)
    assert status == 200
    mod.db.ref_verification_challenges.update_one({'_id': challenge['challenge']}, {'$inc': {'issued_at': -1}})
    return request(server, '/webapp/verify', {'fp': fp, 'challenge': challenge['challenge'],
        'ch': dict(ch, answer=challenge['target'])}, uid=uid)

def test_suspicious_referral_counts_and_admin_reviews_at_milestone(mod, server):
    mod.db.settings.update_one({'key': 'referral_threshold'}, {'$set': {'value': '2'}}, upsert=True)
    for uid in (2001, 2002):
        mod.db.users.insert_one({'user_id': uid, 'name': f'Inv{uid}', 'balance': 0., 'lang': 'en', 'is_admin': 0, 'is_banned': 0})
        mod.register_new_referral(uid, 1001)
        mod.db.referrals_v2.update_one({'invited_id': uid}, {'$set': {'dv': 'need'}})
    ch = {'ok': True, 'ms': 1500, 'trusted': True, 'tries': 1}
    s, d, _ = _verified_ref_request(mod, server, 2001, _ref_fp('a' * 32, wd=True), ch)
    assert s == 200 and d['state'] == 'done'
    r = mod.db.referrals_v2.find_one({'invited_id': 2001})
    assert r['dv'] == 'flag' and r['status'] == 'active'          # counted, marked
    assert not [t for t in _admin_texts(mod) if 'مشكوك' in t]       # no message per referral
    assert not [m for m in MESSAGES if 'إنجاز إحالات' in str(m[1])]
    s, d, _ = _verified_ref_request(mod, server, 2002, _ref_fp('b' * 32), ch)
    # round 41: every member verified on the site → paid automatically (suspicious ones too, owner's choice);
    # the admin still gets the device report and can reject a referral afterwards from the checklist
    ms = [m for m in MESSAGES if 'صُرفت مكافأة إحالات تلقائيًا' in str(m[1])]
    assert ms and 'مشكوك فيها: <b>1</b>' in ms[0][1] and 'rfl_1001_1_0' in json.dumps(ms[0][2].get('reply_markup').to_dict())
    assert mod.db.referral_milestones.find_one({'referrer_id': 1001, 'batch_no': 1})['status'] == 'paid'


def test_suspicious_referral_waits_for_admin_when_automatic_reward_is_off(mod, server):
    mod.db.settings.update_one({'key': 'referral_threshold'}, {'$set': {'value': '2'}}, upsert=True)
    mod.db.settings.update_one({'key': 'ref_auto_pay'}, {'$set': {'value': 'off'}}, upsert=True)
    for uid in (2001, 2002):
        mod.db.users.insert_one({'user_id': uid, 'name': f'Inv{uid}', 'balance': 0., 'lang': 'en', 'is_admin': 0, 'is_banned': 0})
        mod.register_new_referral(uid, 1001)
        mod.db.referrals_v2.update_one({'invited_id': uid}, {'$set': {'dv': 'need'}})
    ch = {'ok': True, 'ms': 1500, 'trusted': True, 'tries': 1}
    _verified_ref_request(mod, server, 2001, _ref_fp('a' * 32, wd=True), ch)
    _verified_ref_request(mod, server, 2002, _ref_fp('b' * 32), ch)
    ms = [m for m in MESSAGES if 'إنجاز إحالات' in str(m[1])]
    assert ms and 'مشكوك فيها: <b>1</b>' in ms[0][1] and 'refwd_ok_1001_1' in json.dumps(ms[0][2].get('reply_markup').to_dict())
    assert mod.db.referral_milestones.find_one({'referrer_id': 1001, 'batch_no': 1})['status'] == 'pending'


# ───── round 7: admin whole-database search (email / any part of an account line) ─────
def test_admin_finds_account_by_email_even_after_sale(mod, server):
    product(mod, stock=0)
    mod.db.product_stock.insert_one({'product_id': '101', 'is_sold': False, 'code_line': 'acct.one@example.test:Secret1'})
    mod.db.product_stock.insert_one({'product_id': '101', 'is_sold': False, 'code_line': 'acct.two@example.test:Secret2'})
    mod._PRODUCTS_CACHE = None; mod._invalidate_products_cache(); mod._wa_invalidate_catalog()
    s, d, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 1}); assert s == 200, d
    sold = d['codes'][0]
    s, d, _ = request(server, '/webapp/admin/search', {'q': sold.split(':')[0]}, uid=9001)
    assert s == 200 and d['kind'] == 'deep', d
    a = d['accounts'][0]
    assert a['line'] == sold and a['state'] == 'sold'
    assert a['sales'][0]['user']['id'] == 1001 and a['sales'][0]['order_id'].startswith('LS') and a['sales'][0]['via'] == 'webapp'
    s, d, _ = request(server, '/webapp/admin/search', {'q': 'EXAMPLE.TEST'}, uid=9001)
    states = {x['line']: x['state'] for x in d['accounts']}
    assert len(states) == 2 and 'in_stock' in states.values() and 'sold' in states.values()
    s, d, _ = request(server, '/webapp/admin/search', {'q': 'example.test'}, uid=1001)
    assert s == 403


# ───── round 8: 🔍 check transaction (hash / order ID) works from the site like the bot ─────
def test_check_transaction_from_site_like_bot(mod, server):
    h = 'ab' * 32
    mod.db.used_transactions.insert_one({'transaction_id': mod.normalize_tx_id(h), 'user_id': 1001, 'amount': 9.5,
                                         'method': 'Toncoin (TON) Auto', 'created_at': int(time.time())})
    s, d, _ = request(server, '/webapp/admin/vc', {'action': 'open', 'cb': 'ad_check_tx'}, uid=9001)
    assert s == 200 and d['step'] is True
    s, d, _ = request(server, '/webapp/admin/vc', {'action': 'send', 'text': h}, uid=9001)
    txt = ' '.join(m['html'] for m in d['msgs'] if m['from'] == 'bot')
    assert s == 200 and d['busy'] is False and '1001' in txt and '9.5' in txt
    s, d, _ = request(server, '/webapp/admin/search', {'q': h[:24]}, uid=9001)
    assert d['kind'] == 'deep' and d['deposits'][0]['user']['id'] == 1001


# ───── round 9: changing the referral reward / size only affects new batches ─────
def test_referral_price_change_keeps_old_batches(mod, monkeypatch):
    from types import SimpleNamespace as NS
    monkeypatch.setattr(mod.bot, 'answer_callback_query', lambda *a, **k: None)
    monkeypatch.setattr(mod.bot, 'edit_message_text', lambda *a, **k: None)
    for k, v in (('referral_threshold', '2'), ('referral_reward', '1.0')):
        mod.db.settings.update_one({'key': k}, {'$set': {'value': v}}, upsert=True)
    def activate(n):
        for i in range(n):
            inv = 3000 + mod.db.referrals_v2.count_documents({})
            mod.db.users.insert_one({'user_id': inv, 'name': 'Inv', 'balance': 0., 'lang': 'en', 'is_admin': 0, 'is_banned': 0})
            mod.db.referrals_v2.insert_one({'invited_id': inv, 'referrer_id': 1001, 'status': 'pending', 'created_at': 1, 'updated_at': 1, 'dv': 'ok', 'dv_at': 1})
            mod.mark_referral_status(inv, 'active')
    def approve(b):
        mod.ref_milestone_approve(NS(id='c', data=f'refwd_ok_1001_{b}', from_user=NS(id=9001, first_name='Owner'),
                                     message=NS(chat=NS(id=9001), message_id=1, text='', html_text='')))
    activate(2)
    assert mod.db.referral_milestones.find_one({'referrer_id': 1001, 'batch_no': 1})['reward'] == 1.0
    mod.db.settings.update_one({'key': 'referral_reward'}, {'$set': {'value': '5'}})
    approve(1)
    u = mod.db.users.find_one({'user_id': 1001})
    assert u['balance'] == 101.0 and u['ref_v2_earned'] == 1.0          # paid at the batch's own price
    activate(2); approve(2)
    u = mod.db.users.find_one({'user_id': 1001})
    assert u['balance'] == 106.0 and u['ref_v2_earned'] == 6.0          # new batch at the new price
    mod.db.settings.update_one({'key': 'referral_reward'}, {'$set': {'value': '0.5'}})
    mod.update_referrer_balance(1001)                                   # no retroactive re-pricing
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 106.0


# ───── round 10: a referral that leaves after its batch was paid is charged back at that batch's price ─────
def test_referral_leaving_is_charged_back_at_its_batch_price(mod, monkeypatch):
    from types import SimpleNamespace as NS
    monkeypatch.setattr(mod.bot, 'answer_callback_query', lambda *a, **k: None)
    monkeypatch.setattr(mod.bot, 'edit_message_text', lambda *a, **k: None)
    for k, v in (('referral_threshold', '2'), ('referral_reward', '1.0')):
        mod.db.settings.update_one({'key': k}, {'$set': {'value': v}}, upsert=True)
    invs = []
    for i in range(2):
        inv = 4000 + i
        mod.db.users.insert_one({'user_id': inv, 'name': 'Inv', 'balance': 0., 'lang': 'en', 'is_admin': 0, 'is_banned': 0})
        mod.db.referrals_v2.insert_one({'invited_id': inv, 'referrer_id': 1001, 'status': 'pending', 'created_at': 1, 'updated_at': 1, 'dv': 'ok', 'dv_at': 1})
        mod.mark_referral_status(inv, 'active'); invs.append(inv)
    mod.ref_milestone_approve(NS(id='c', data='refwd_ok_1001_1', from_user=NS(id=9001, first_name='Owner'),
                                 message=NS(chat=NS(id=9001), message_id=1, text='', html_text='')))
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 101.0
    mod.db.settings.update_one({'key': 'referral_reward'}, {'$set': {'value': '10'}})   # price changes later
    mod.mark_referral_status(invs[0], 'left')
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100.5                   # $1 ÷ 2, not $10 ÷ 2
    mod.mark_referral_status(invs[0], 'left')
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100.5                   # once only
    mod.mark_referral_status(invs[0], 'active')
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 101.0                   # refunded on return


# ───── round 11: a customer who forges admin button data (modified Telegram client) is refused ─────
@pytest.mark.parametrize('data', ['stk_view_101', 'stk_clear_101', 'stk_clrok_101', 'stk_add_101', 'ep_price_101', 'toggle_hide_101',
                                  'set_v_usdt', 'ad_u_det_1002', 'ad_uh_buy_1002', 'ad_ugift_1001', 'del_fsub_@x', 'edit_p_101',
                                  'ad_profit', 'ad_pcost_0', 'pcost_101', 'pprofit_101',
                                  'ad_baseurl', 'baseurl_set', 'baseurl_force', 'baseurl_reset', 'baseurl_notify_ok',
                                  'ad_cms_site_0', 'wasl_avail', 'ad_plist', 'ad_plist_txt'])
def test_customer_cannot_use_admin_buttons(mod, data, monkeypatch):
    product(mod)
    mod.db.required_channels.insert_one({'channel_id': '@x'})
    edits = []
    monkeypatch.setattr(mod.bot, 'answer_callback_query', lambda *a, **k: None)
    monkeypatch.setattr(mod.bot, 'edit_message_text', lambda *a, **k: edits.append(a))
    before = (mod.db.product_stock.count_documents({}), mod.db.products.find_one({'id': '101'}), mod.db.required_channels.count_documents({}))
    call = mod.telebot.types.CallbackQuery.de_json({'id': 'q', 'from': {'id': 1001, 'is_bot': False, 'first_name': 'Customer'}, 'chat_instance': 'x',
                                                'data': data, 'message': {'message_id': 5, 'date': 1, 'chat': {'id': 1001, 'type': 'private'},
                                                                          'from': {'id': 1, 'is_bot': True, 'first_name': 'Bot'}, 'text': 'x'}})
    MESSAGES.clear()
    mod.bot._run_middlewares_and_handler(call, handlers=mod.bot.callback_query_handlers, middlewares=None, update_type='callback_query')
    after = (mod.db.product_stock.count_documents({}), mod.db.products.find_one({'id': '101'}), mod.db.required_channels.count_documents({}))
    assert not MESSAGES and not edits and before == after


# ───── round 12: profit (today / 7 days / month / year) + cost price + deposit copy buttons ─────
def test_profit_uses_paid_price_and_cost_saved_at_purchase(mod, server):
    product(mod, stock=5, cost=3.0)
    s, d, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 2}); assert s == 200, d
    assert [o.get('cost_total') for o in mod.db.orders.find()] == [3.0, 3.0]
    mod.db.products.update_one({'id': '101'}, {'$set': {'cost': 4.0, 'price': 9.0}}); mod._invalidate_products_cache()
    mod._do_purchase(1001, '101', 1, 'en')
    assert sorted(o.get('cost_total') for o in mod.db.orders.find()) == [3.0, 3.0, 4.0]   # old orders keep their cost
    per = {k: sn for k, _a, _e, sn in mod.profit_periods()}
    assert list(per) == ['d1', 'd7', 'd30', 'y']
    st = mod.profit_stats(per['d1'])
    assert (st['sales'], st['cost'], st['profit']) == (19.0, 10.0, 9.0), st                  # 5+5 then 9 (price paid, not today's price)
    mod._PROFIT_CACHE['v'] = None
    s, home, _ = request(server, '/webapp/admin/home', {}, uid=9001); assert s == 200, home
    assert set(home['stats']) == {'d1', 'd7', 'd30', 'y'} and home['stats']['y']['profit'] == 9.0
    assert any(x['cb'] == 'ad_profit' for x in home['sections'])

def test_bot_profit_report_and_cost_editor(mod, monkeypatch):
    product(mod, stock=2)
    sent = []
    monkeypatch.setattr(mod.bot, 'answer_callback_query', lambda *a, **k: None)
    monkeypatch.setattr(mod.bot, 'edit_message_text', lambda text, *a, **k: sent.append(text))
    monkeypatch.setattr(mod.bot, 'register_next_step_handler', lambda *a, **k: None)
    def press(data):
        call = mod.telebot.types.CallbackQuery.de_json({'id': 'q', 'from': {'id': 9001, 'is_bot': False, 'first_name': 'Owner'}, 'chat_instance': 'x',
                                                        'data': data, 'message': {'message_id': 5, 'date': 1, 'chat': {'id': 9001, 'type': 'private'},
                                                                                  'from': {'id': 1, 'is_bot': True, 'first_name': 'Bot'}, 'text': 'x'}})
        mod.bot._run_middlewares_and_handler(call, handlers=mod.bot.callback_query_handlers, middlewares=None, update_type='callback_query')
    press('ad_profit')
    assert sent and all(w in sent[-1] for w in ('اليوم', 'آخر 7 أيام', 'هذا الشهر', 'هذه السنة', 'الربح'))
    msg = mod.telebot.types.Message.de_json({'message_id': 9, 'date': 1, 'chat': {'id': 9001, 'type': 'private'},
                                             'from': {'id': 9001, 'is_bot': False, 'first_name': 'Owner'}, 'text': '1.25'})
    mod.ad_pcost_save(msg, '101')
    assert mod.db.products.find_one({'id': '101'})['cost'] == 1.25

def _copy_buttons(kw):
    rm = kw.get('reply_markup')
    return [(b.text, b.copy_text.text) for row in (rm.keyboard if rm else []) for b in row if getattr(b, 'copy_text', None)]

def test_binance_deposit_has_pay_id_copy_button(mod):
    msg = mod.telebot.types.Message.de_json({'message_id': 9, 'date': 1, 'chat': {'id': 1001, 'type': 'private'},
                                             'from': {'id': 1001, 'is_bot': False, 'first_name': 'Test'}, 'text': '10'})
    MESSAGES.clear(); mod.ask_binance_deposit_amount(msg)
    btns = [b for _c, _t, kw in MESSAGES for b in _copy_buttons(kw)]
    assert ('📋 Copy Binance Pay ID', '12345678') in btns, btns          # user 1001 is English
    assert any(v == '1001' for _l, v in btns)                            # the user-ID copy button is still there

def test_usdt_deposit_has_one_amount_copy_button(mod):
    msg = mod.telebot.types.Message.de_json({'message_id': 9, 'date': 1, 'chat': {'id': 1001, 'type': 'private'},
                                             'from': {'id': 1001, 'is_bot': False, 'first_name': 'Test'}, 'text': '7'})
    MESSAGES.clear(); mod.ask_deposit_amount(msg, 'USDT')
    btns = [b for _c, _t, kw in MESSAGES for b in _copy_buttons(kw)]
    amounts = [v for _l, v in btns if v != 'TTESTADDRESS']
    pend = mod.db.pending_deposits.find_one({'user_id': 1001, 'coin': 'USDT'})
    assert len(amounts) == 1 and abs(float(amounts[0]) - pend['unique_amount_usd']) < 1e-9, btns


# ───── round 13: every price keeps its own profit; price changes tell the profit per item ─────
def test_price_change_keeps_old_profit_and_reports_new_one(mod, server, monkeypatch):
    product(mod, stock=6, cost=3.0)
    s, d, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 2}); assert s == 200, d
    old = sorted((o.get('price'), o.get('cost_total')) for o in mod.db.orders.find())
    p = mod.db.products.find_one({'id': '101'})
    hist = mod.db.price_history
    t0 = int(time.time())
    for i, o in enumerate(list(mod.db.orders.find())):     # الطلبات صارت قبل تغيير السعر بوضوح
        mod.db.orders.delete_one({'_id': o['_id']}); o['_id'] = ObjectId.from_datetime(datetime_from(t0 - 60 - i)); mod.db.orders.insert_one(o)
    MESSAGES.clear()
    mod.admin_save_edit(mod._AdmMsg(9001, '6'), 'price', '101')
    sent = ' '.join(str(t) for _c, t, _k in MESSAGES)
    assert 'ربحك بالقطعة: <b>$3.00</b>' in sent and 'تبقى بسعرها وربحها' in sent
    h = hist.find_one({'product_id': '101'})
    assert h and h['old'] == 5.0 and h['new'] == 6.0 and h['cost'] == 3.0
    assert sorted((o.get('price'), o.get('cost_total')) for o in mod.db.orders.find()) == old    # القديم ما تغيّر
    P = mod.product_profit(mod.find_product('101'))
    assert [(x['price'], x['qty'], x['profit']) for x in P['periods']] == [(6.0, 0, 0.0), (5.0, 2, 4.0)]
    s, r, _ = request(server, '/webapp/admin/product/update', {'id': '101', 'cost': 6.5}, uid=9001)
    assert s == 200 and r['profit']['profit_unit'] == -0.5 and r['product']['cost'] == 6.5
    s, r, _ = request(server, '/webapp/admin/product/update', {'id': '101', 'cost': -2}, uid=9001)
    assert s == 400
    s, r, _ = request(server, '/webapp/admin/product/update', {'id': '101', 'cost': 1}, uid=1001)
    assert s == 403

def datetime_from(ts):
    import datetime as _dt
    return _dt.datetime.utcfromtimestamp(ts)


# ───── round 14: API retries are safe on every product; base URL can be changed ─────
def _api(server, method, path, key, body=None, headers=None):
    import os as _os
    conn = http.client.HTTPConnection(*server, timeout=10)
    h = {'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'}
    h.update(headers or {})
    conn.request(method, path, json.dumps(body) if body is not None else None, h)
    r = conn.getresponse(); data = json.loads(r.read() or b'{}'); st = r.status; conn.close()
    return st, data

def test_api_purchase_idempotent_and_unique_order_ids(mod, server):
    product(mod, stock=5)
    mod.db.api_keys.insert_one({'user_id': 1001, 'api_key': 'sk_' + 'c' * 48, 'is_active': True})
    gw = mod._get_api_gateway(); mod._api_rate.clear()
    k = 'sk_' + 'c' * 48
    s1, a = _api(server, 'POST', f'/{gw}/purchase', k, {'product_id': '101', 'qty': 1}, {'Idempotency-Key': 'k1'})
    s2, b = _api(server, 'POST', f'/{gw}/purchase', k, {'product_id': '101', 'qty': 1}, {'Idempotency-Key': 'k1'})
    assert s1 == s2 == 200 and a['order_id'] == b['order_id'] and a['codes'] == b['codes'] and b.get('idempotent_replay')
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 95
    s3, c = _api(server, 'POST', f'/{gw}/purchase', k, {'product_id': '101', 'qty': 1})
    assert s3 == 200 and c['order_id'] != a['order_id']                    # same second → still a different order ID
    s4, d = _api(server, 'POST', f'/{gw}/purchase', k, {'product_id': '101', 'qty': 2}, {'Idempotency-Key': 'k1'})
    assert s4 == 409

def test_base_url_panel_overrides_env_and_render(mod, monkeypatch):
    monkeypatch.setenv('RENDER_EXTERNAL_URL', 'https://bot.onrender.com')
    monkeypatch.setenv('BASE_URL', 'https://env.example.com')
    mod._BASEURL_CACHE.update(t=0, v=None)
    assert mod._get_server_url() == 'https://env.example.com'
    mod.db.settings.insert_one({'key': 'public_base_url', 'value': 'https://api.mystore.com'})
    mod._BASEURL_CACHE.update(t=0, v=None)
    assert mod._get_server_url() == 'https://api.mystore.com'
    assert mod._wa_public_url() == 'https://api.mystore.com/app/'
    assert mod._baseurl_valid('https://api.mystore.com') and not mod._baseurl_valid('http://api.mystore.com') and not mod._baseurl_valid('https://x.com/path')
    mod.db.settings.delete_one({'key': 'public_base_url'}); mod._BASEURL_CACHE.update(t=0, v=None)


# ───── round 15: folder counts, light menus, product search (short forms / typos) ─────
def test_product_search_short_forms_and_typos(mod):
    items = [{'name_ar': 'نتفلكس بريميوم', 'name_en': 'Netflix Premium', 'in_stock': True},
             {'name_ar': 'يوتيوب بريميوم', 'name_en': 'YouTube Premium', 'in_stock': True},
             {'name_ar': 'شات جي بي تي', 'name_en': 'ChatGPT Plus', 'in_stock': False}]
    names = lambda q: [i['name_en'] for i in mod.shop_search(q, items)['items']]
    assert names('nf') == ['Netflix Premium'] and names('gpt') == ['ChatGPT Plus'] and names('يوتوب') == ['YouTube Premium']
    r = mod.shop_search('netflx', items)
    assert [i['name_en'] for i in r['items']] == ['Netflix Premium'] and r['corrected'] == 'netflix'
    r = mod.shop_search('nettflixxx', items)
    assert not r['items'] and r['suggest'][:1] == ['netflix']

def test_folder_count_ignores_hidden_and_deleted(mod):
    product(mod, pid='101')
    mod.db.products.insert_one({'id': '102', 'name_ar': 'مخفي', 'name_en': 'Hidden', 'price': 1.0, 'is_hidden': True})
    mod._invalidate_products_cache()
    cat = {'product_ids': ['101', 101, '102', '999']}
    assert mod._catalog_visible_count(cat) == 1
    assert all('desc_ar' not in p for p in mod._get_all_products_cached())

def test_deleting_product_keeps_customer_orders(mod, monkeypatch):
    product(mod, stock=2)
    mod._do_purchase(1001, '101', 1, 'en')
    mod.db.catalogs.insert_one({'name_ar': 'x', 'product_ids': ['101']})
    monkeypatch.setattr(mod.bot, 'answer_callback_query', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'admin_main_ui', lambda *a, **k: None)
    call = mod.telebot.types.CallbackQuery.de_json({'id': 'q', 'from': {'id': 9001, 'is_bot': False, 'first_name': 'Owner'}, 'chat_instance': 'x',
                                                    'data': 'del_p_ok_101', 'message': {'message_id': 5, 'date': 1, 'chat': {'id': 9001, 'type': 'private'},
                                                                                        'from': {'id': 1, 'is_bot': True, 'first_name': 'Bot'}, 'text': 'x'}})
    mod.bot._run_middlewares_and_handler(call, handlers=mod.bot.callback_query_handlers, middlewares=None, update_type='callback_query')
    assert mod.db.products.count_documents({'id': '101'}) == 0
    assert mod.db.orders.count_documents({'product_id': '101'}) == 1
    assert mod.db.catalogs.count_documents({'product_ids': '101'}) == 0
    assert mod._deleted_product_name('101', 'en') == 'Test Product'


# ───── round 16: admin «available products» table (Telegram rich message) ─────
def test_available_products_rich_table(mod, monkeypatch):
    product(mod, stock=3, discount_tiers=[{'min_qty': 5, 'price': 4.5}], custom_emoji_id='5368324170671202286')
    product(mod, pid='102', stock=0)
    sent = []
    real = mod.telebot.apihelper._make_request
    monkeypatch.setattr(mod.telebot.apihelper, '_make_request',
                        lambda token, name, method='get', params=None, files=None: sent.append((name, params)) or {'message_id': 1, 'date': 1, 'chat': {'id': 9001, 'type': 'private'}})
    n, rich = mod._plist_send(9001, 9001)
    assert n == 1 and rich and sent and sent[0][0] == 'sendRichMessage'
    rm = json.loads(sent[0][1]['rich_message'])
    table = [b for b in rm['blocks'] if b['type'] == 'table'][0]
    row = table['cells'][1]
    assert row[0]['text'][2]['type'] == 'url' and row[0]['text'][2]['url'].endswith('?start=p_101')
    assert row[0]['text'][0] == {'type': 'custom_emoji', 'custom_emoji_id': '5368324170671202286', 'alternative_text': '📦'}
    assert '5+ pcs $4.50' in json.dumps(row[1]['text'])
    assert all(c['align'] and c['valign'] for r in table['cells'] for c in r)


# ───── round 17: download purchased codes as a TXT file from the site ─────
def test_codes_txt_download_link_and_chat(mod, server):
    product(mod, stock=4)
    s, d, _ = request(server, '/webapp/buy', {'product_id': '101', 'qty': 2}); assert s == 200, d
    s, r, _ = request(server, '/webapp/codes/txt', {'ref': d['order_id']}); assert s == 200 and r['url'].startswith('/webapp/dl/'), r
    conn = http.client.HTTPConnection(*server, timeout=4); conn.request('GET', r['url']); res = conn.getresponse(); body = res.read().decode(); conn.close()
    assert res.status == 200 and all(c in body for c in d['codes']) and 'attachment' in res.getheader('Content-Disposition', '')
    s, r2, _ = request(server, '/webapp/codes/txt', {'ref': d['order_id']}, uid=1002); assert s == 404
    MESSAGES.clear()
    s, r3, _ = request(server, '/webapp/codes/txt', {'ref': d['order_id'], 'send': True}); assert s == 200 and r3.get('sent')
    assert any(str(c) == '1001' and txt == '[document]' for c, txt, kw in MESSAGES)


# ───── round 18: API connection details only after «Show connection details» ─────
def test_api_credentials_hidden_until_button(mod, monkeypatch):
    mod.db.api_keys.insert_one({'user_id': 1001, 'api_key': 'sk_' + 'f' * 48, 'is_active': True})
    monkeypatch.setattr(mod.bot, 'answer_callback_query', lambda *a, **k: None)
    edits = []
    monkeypatch.setattr(mod.bot, 'edit_message_text', lambda text, *a, **k: edits.append(text))
    def press(data):
        call = mod.telebot.types.CallbackQuery.de_json({'id': 'q', 'from': {'id': 1001, 'is_bot': False, 'first_name': 'T'}, 'chat_instance': 'x',
                                                        'data': data, 'message': {'message_id': 5, 'date': 1, 'chat': {'id': 1001, 'type': 'private'},
                                                                                  'from': {'id': 1, 'is_bot': True, 'first_name': 'Bot'}, 'text': 'x'}})
        mod.bot._run_middlewares_and_handler(call, handlers=mod.bot.callback_query_handlers, middlewares=None, update_type='callback_query')
    press('open_api')
    assert edits and 'sk_' + 'f' * 48 not in edits[-1]
    MESSAGES.clear(); press('api_creds')
    assert any('sk_' + 'f' * 48 in str(t) for _c, t, _k in MESSAGES)


# ───── round 19: bot ↔ site linkage ─────
def test_any_bot_write_reaches_site_catalog_at_once(mod, server):
    product(mod, stock=3)
    s, c, _ = request(server, '/webapp/catalog'); p = next(x for x in c['products'] if x['id'] == '101'); assert p['price'] == 5.0 and p['stock'] == 3
    mod.db.products.update_one({'id': '101'}, {'$set': {'price': 7.25, 'custom_emoji_id': '5368324170671202286'}})   # any bot handler, no manual cache clear
    mod.db.product_stock.insert_one({'product_id': '101', 'is_sold': False, 'code_line': 'NEW-1'})
    s, c, _ = request(server, '/webapp/catalog'); p = next(x for x in c['products'] if x['id'] == '101')
    assert p['price'] == 7.25 and p['stock'] == 4 and p['emoji_id'] == '5368324170671202286', p


def test_site_language_choice_sets_bot_language(mod, server):
    s, r, _ = request(server, '/webapp/lang', {'lang': 'ar'}); assert s == 200 and r['ok']
    assert mod.db.users.find_one({'user_id': 1001})['lang'] == 'ar'
    s, r, _ = request(server, '/webapp/lang', {'lang': 'ru'}); assert s == 200 and r['changed'] is False
    assert mod.db.users.find_one({'user_id': 1001})['lang'] == 'ar'
    s, b, _ = request(server, '/webapp/bootstrap'); assert b['user']['lang'] == 'ar'


def test_download_all_purchases_same_file_as_bot(mod, server):
    product(mod, stock=4)
    request(server, '/webapp/buy', {'product_id': '101', 'qty': 1}); request(server, '/webapp/buy', {'product_id': '101', 'qty': 2})
    s, r, _ = request(server, '/webapp/codes/txt', {'all': True}); assert s == 200 and 'My_Purchases_1001' in r['name'], r
    conn = http.client.HTTPConnection(*server, timeout=4); conn.request('GET', r['url']); res = conn.getresponse(); body = res.read().decode(); conn.close()
    assert res.status == 200 and body.count('LOCAL-ONLY-') >= 3 or body.count('CODE') >= 3 or body.count('\n1. ') >= 1, body[:300]
    content, _ = mod._purchases_txt(1001); assert content == body


def test_chatgpt_subscriptions_on_site_share_bot_renewal(mod, server):
    import datetime as _d
    acc = mod.db.cgpt_accounts.insert_one({'name': 'a', 'data': {'user': {'email': 'o@biz.test'}}}).inserted_id
    exp = (_d.datetime.now() + _d.timedelta(days=3, hours=2)).isoformat()
    mod.db.cgpt_invites_data.insert_one({'_id': str(acc), 'data': {'invites': {'me@mail.test': {'status': 'active', 'telegram_uid': 1001, 'expires_at': exp}}}})
    pid = mod.db.cgpt_products.insert_one({'name': 'ChatGPT Plus', 'durations': [{'dur_id': 'm1', 'label': 'شهر', 'label_en': '1 month', 'price': 4.0, 'minutes': 43200}]}).inserted_id
    s, r, _ = request(server, '/webapp/cgpt/subs'); assert s == 200 and r['subs'][0]['email'] == 'me@mail.test' and r['subs'][0]['days'] == 3, r
    assert r['options'][0]['price'] == 4.0
    s, r, _ = request(server, '/webapp/cgpt/subs', uid=1002); assert s == 200 and r['subs'] == []
    s, r, _ = request(server, '/webapp/cgpt/renew', {'email': 'me@mail.test', 'product_id': str(pid), 'dur_id': 'm1'}, uid=1002); assert s == 404
    s, r, _ = request(server, '/webapp/cgpt/renew', {'email': 'me@mail.test', 'product_id': str(pid), 'dur_id': 'm1'}); assert s == 200 and r['ok'], r
    new = _d.datetime.fromisoformat(mod.db.cgpt_invites_data.find_one({'_id': str(acc)})['data']['invites']['me@mail.test']['expires_at'])
    assert (new - _d.datetime.now()).days >= 32 and abs(mod.db.users.find_one({'user_id': 1001})['balance'] - 96.0) < 1e-9


def test_every_site_error_code_has_a_message():
    src = (APP / 'bot.py').read_text(encoding='utf-8'); html = (APP / 'webapp/index.html').read_text(encoding='utf-8')
    tree = ast.parse(src); codes = set()
    for f in ast.walk(tree):
        if isinstance(f, ast.FunctionDef) and f.name.startswith('_wa_'):
            for n in ast.walk(f):
                if isinstance(n, ast.Dict):
                    for k, v in zip(n.keys, n.values):
                        if isinstance(k, ast.Constant) and k.value == 'error' and isinstance(v, ast.Constant) and re.fullmatch(r'[a-z_]+', str(v.value)):
                            codes.add(v.value)
    en = html[html.index('const I = {'):html.index('ar:{', html.index('const I = {'))]
    missing = sorted(c for c in codes if f'e_{c}:' not in en)
    assert not missing, missing


def test_every_fixed_bot_button_has_a_handler(mod):
    src = (APP / 'bot.py').read_text(encoding='utf-8'); tree = ast.parse(src); datas = set()
    for n in ast.walk(tree):
        kws = [kw.value for kw in getattr(n, 'keywords', []) if kw.arg == 'callback_data'] if isinstance(n, ast.Call) else \
              [v for k, v in zip(n.keys, n.values) if isinstance(k, ast.Constant) and k.value == 'callback_data'] if isinstance(n, ast.Dict) else []
        for v in kws:
            if isinstance(v, ast.Constant) and isinstance(v.value, str): datas.add(v.value)
    def matched(d):
        c = mod.telebot.types.CallbackQuery.de_json({'id': 'q', 'from': {'id': 9001, 'is_bot': False, 'first_name': 'O'}, 'chat_instance': 'x', 'data': d,
              'message': {'message_id': 1, 'date': 1, 'chat': {'id': 9001, 'type': 'private'}, 'from': {'id': 1, 'is_bot': True, 'first_name': 'B'}, 'text': 'x'}})
        return any(h['filters']['func'](c) for h in mod.bot.callback_query_handlers)
    dead = sorted(d for d in datas if not matched(d))
    assert len(datas) > 150 and not dead, dead


# ───── round 20: notification buy buttons open the site at the product ─────
def test_notification_button_opens_site_at_product(mod, monkeypatch):
    product(mod)
    monkeypatch.setattr(mod, '_wa_public_url', lambda: 'https://shop.example/app/')
    assert mod._wa_site_pid('101') == '101' and mod._wa_site_pid('cgpt_main_abc') == 'cgpt_abc' and mod._wa_site_pid('vext_e1') == 'ext_e1'
    b = mod._notify_btn('Buy', 'vi_p_101', url=mod._notify_url('101')).to_dict()
    assert b['web_app']['url'] == 'https://shop.example/app/?p=101' and 'callback_data' not in b and b['style'] == 'success'
    assert mod._notify_url(cat='c1') == 'https://shop.example/app/?c=c1'
    mod.db.settings.insert_one({'key': 'notify_open', 'value': 'bot'})
    b = mod._notify_btn('Buy', 'vi_p_101', url=mod._notify_url('101')).to_dict()
    assert b.get('callback_data') == 'vi_p_101' and 'web_app' not in b
    mod.db.settings.delete_many({'key': 'notify_open'})
    monkeypatch.setattr(mod, '_wa_public_url', lambda: None)
    assert mod._notify_url('101') is None


def test_site_opens_linked_product_only_after_gates():
    html = (APP / 'webapp/index.html').read_text(encoding='utf-8')
    assert "q.get('p')" in html and 'function openDeep()' in html
    assert "if (S.screen !== 'app') return false;" in html          # never behind the channels / verification screens
    assert 'setTimeout(dvEnter, 1300)' in html and 'openDeep();' in html
