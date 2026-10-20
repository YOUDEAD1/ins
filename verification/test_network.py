"""Network regressions and before/after measurements. All providers and DBs are fake."""
import ast,gzip,http.client,io,json,time,types
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
import pytest
from harness import APP,MESSAGES
from test_integration import mod,isolate,server,product,request
from test_products_menu import dispatch

BENCH=APP.parent/'network-benchmarks.json'

def metric(key,value):
    data=json.loads(BENCH.read_text()) if BENCH.exists() else {}
    data[key]=value;BENCH.write_text(json.dumps(data,indent=2))

@pytest.fixture(autouse=True)
def network_state(mod,monkeypatch):
    monkeypatch.setattr(mod,'_NET_METER',mod._NetworkMeter())
    mod._NET_REMOTE.clear();mod._NET_SYNC_STATE.clear()
    mod._NET_SYNC_CONTEXT.active=False
    mod._NET_PAY_CACHE.update(expires=0,active=True,generation=0)
    for e in mod._NET_PAY_EVENTS.values():e.clear()

def response(code=200,data=None,headers=None):
    encoded=json.dumps(data or {}).encode()
    return types.SimpleNamespace(status_code=code,content=encoded,_content=encoded,
        headers=headers or {},json=lambda:data)

def supplier(mod,**extra):
    doc={'base_url':'https://supplier.example','api_key':'FAKE-SUPPLIER-KEY',**extra}
    doc['_id']=mod.db.ext_stores.insert_one(dict(doc)).inserted_id
    return doc

def original_function(mod,name):
    source=(APP/'verification/network_baseline.py').read_text()
    node=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name==name)
    scope=dict(mod.__dict__);exec(compile(ast.Module(body=[node],type_ignores=[]),'<baseline>','exec'),scope)
    return scope[name]

class Handler:
    def __init__(self,encoding=''):
        self.headers={'Accept-Encoding':encoding};self.wfile=io.BytesIO();self.sent={};self.events=[]
    def send_response(self,code):self.code=code;self.events.append('response')
    def send_header(self,k,v):self.sent[k]=v
    def end_headers(self):pass

@pytest.mark.parametrize('value,expected',[('',False),('identity',False),('gzip',True),
    ('br, gzip',True),('gzip;q=0',False),('gzip;q=0.5',True),('*;q=0.8',True),
    ('gzip;q=0, *;q=1',False),('gzip;q=bad',False)])
def test_gzip_negotiation_respects_explicit_refusal(mod,value,expected):
    assert mod._net_accepts_gzip({'Accept-Encoding':value}) is expected

def test_json_compression_keeps_all_fields_and_records_idempotency_before_write(mod):
    data={'success':True,'products':[{'id':str(i),'name':'منتج Premium ✨','description':'Description & وصف '*100} for i in range(40)]}
    h=Handler('gzip');seen=[]
    h._idem_hook=lambda code,value:(seen.append((code,value)),h.events.append('idempotency'))
    mod._json_resp(h,200,data)
    encoded=h.wfile.getvalue()
    assert json.loads(gzip.decompress(encoded))==data
    assert seen==[(200,data)] and h.events[0]=='idempotency'
    assert h.sent['Content-Encoding']=='gzip' and int(h.sent['Content-Length'])==len(encoded)
    raw=json.dumps(data,ensure_ascii=False,separators=(',',':')).encode()
    assert len(encoded)<len(raw)*.3
    metric('sample_api_json',{'uncompressed_bytes':len(raw),'gzip_bytes':len(encoded),
        'saved_percent':round(100*(1-len(encoded)/len(raw)),2),'representative':'repeated bilingual test descriptions'})

@pytest.mark.parametrize('encoding',['','gzip;q=0'])
def test_uncompressed_clients_receive_identical_json(mod,encoding):
    h=Handler(encoding);data={'text':'data '*1000}
    mod._json_resp(h,200,data)
    assert 'Content-Encoding' not in h.sent and json.loads(h.wfile.getvalue())==data

def test_304_has_no_body_and_does_not_reuse_an_encoded_payload(mod):
    h=Handler('gzip');h._net_etag='W/"tag"'
    mod._json_resp(h,304,None)
    assert h.wfile.getvalue()==b'' and 'Content-Length' not in h.sent
    assert h.sent['ETag']=='W/"tag"'

def api_raw(mod,server,uid=1001,headers=None):
    key='sk_'+str(uid)+'k'*44
    mod.db.api_keys.update_one({'api_key':key},{'$set':{'user_id':uid,'is_active':True}},upsert=True)
    c=http.client.HTTPConnection(*server,timeout=8)
    c.request('GET','/'+mod._get_api_gateway()+'/products',headers={'Authorization':'Bearer '+key,**(headers or {})})
    r=c.getresponse();data=r.read();result=(r.status,dict(r.getheaders()),data);c.close();return result

def test_api_compression_etag_auth_and_invalidation_over_real_local_http(mod,server):
    product(mod,desc_en='A real long description '*300)
    st,headers,body=api_raw(mod,server,headers={'Accept-Encoding':'gzip'})
    assert st==200 and headers['Content-Encoding']=='gzip'
    data=json.loads(gzip.decompress(body));assert data['products'][0]['id']=='101'
    tag=headers['ETag']
    st,headers,body=api_raw(mod,server,headers={'If-None-Match':tag})
    assert st==304 and body==b''
    # Another API account never reuses the first account's representation.
    mod.db.api_hidden.insert_one({'api_user_id':1002,'product_id':'101'})
    st,headers,body=api_raw(mod,server,uid=1002,headers={'If-None-Match':tag})
    assert st==200 and headers['ETag']!=tag and json.loads(body)['products']==[]
    mod.db.products.update_one({'id':'101'},{'$set':{'price':7}})
    mod._invalidate_products_cache()
    st,headers,body=api_raw(mod,server,headers={'If-None-Match':tag})
    assert st==200 and headers['ETag']!=tag and json.loads(body)['products'][0]['store_price']==7
    c=http.client.HTTPConnection(*server,timeout=5)
    c.request('GET','/'+mod._get_api_gateway()+'/products',headers={'If-None-Match':headers['ETag']})
    r=c.getresponse();r.read();assert r.status==401;c.close()

def test_multiple_api_users_keep_their_own_cached_prices(mod,monkeypatch):
    product(mod)
    mod.db.api_pricing.insert_one({'api_user_id':1001,'product_id':'101','name_en':'First private name','sell_price':9})
    mod.db.api_pricing.insert_one({'api_user_id':1002,'product_id':'101','name_en':'Second private name','sell_price':11})
    find=Mock(wraps=mod.db.products.find);monkeypatch.setattr(mod.db.products,'find',find)
    one=mod._net_api_products_payload(1001);two=mod._net_api_products_payload(1002)
    again=mod._net_api_products_payload(1001)
    assert one==again and find.call_count==2
    assert one['products'][0]['your_price']==9 and two['products'][0]['your_price']==11
    assert one['products'][0]['name_en']=='First private name' and two['products'][0]['name_en']=='Second private name'

def test_simultaneous_catalog_misses_build_once(mod,monkeypatch):
    product(mod)
    find=Mock(wraps=mod.db.products.find);monkeypatch.setattr(mod.db.products,'find',find)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results=list(pool.map(lambda _:mod._net_api_products_payload(1001),range(6)))
    assert all(v==results[0] for v in results) and find.call_count==1

@pytest.mark.parametrize('change',['pricing','hidden'])
def test_private_api_profile_changes_invalidate_only_authorized_catalog_results(mod,change):
    product(mod)
    assert mod._net_api_products_payload(1001)['products']
    if change=='pricing':
        mod.db.api_pricing.insert_one({'api_user_id':1001,'product_id':'101','sell_price':8})
        assert mod._net_api_products_payload(1001)['products'][0]['your_price']==8
        assert mod._net_api_products_payload(1002)['products'][0]['your_price'] is None
    else:
        mod.db.api_hidden.insert_one({'api_user_id':1001,'product_id':'101'})
        assert not mod._net_api_products_payload(1001)['products']
        assert mod._net_api_products_payload(1002)['products']

def test_api_cache_ttl_memory_bounds_and_generation_guard(mod,monkeypatch):
    now=[time.time()];monkeypatch.setattr(mod.time,'time',lambda:now[0])
    generation=mod._NET_PRODUCTS_EPOCH
    for uid in range(20):mod._net_products_put(uid,[{'data':'x'*100000}],generation)
    assert len(mod._PRODUCTS_CACHE['_users'])<=16
    assert sum(v['bytes'] for v in mod._PRODUCTS_CACHE['_users'].values())<=8*1024**2
    assert mod._net_products_get(19) is not None
    now[0]+=31;assert mod._net_products_get(19) is None
    mod._invalidate_products_cache()
    mod._net_products_put(19,[{'stale':True}],generation)
    assert mod._net_products_get(19) is None

def test_batch_stock_counts_preserve_numeric_string_and_float_aliases(mod,monkeypatch):
    product(mod,stock=1);product(mod,'102',stock=2)
    mod.db.product_stock.insert_many([{'product_id':101,'is_sold':False},
        {'product_id':101.0,'is_sold':False},{'product_id':'101','is_sold':True}])
    prods=list(mod.db.products.find())
    expected={str(p['id']):mod.get_product_stock_count(p['id']) for p in prods}
    aggregate=Mock(wraps=mod.db.product_stock.aggregate);monkeypatch.setattr(mod.db.product_stock,'aggregate',aggregate)
    assert mod._net_stock_counts(prods)==expected and aggregate.call_count==1

def test_catalog_version_acknowledges_unchanged_content_and_fresh_stock_changes(mod):
    product(mod,desc_en='Description '*300)
    s,first=mod._wa_catalog_ep({'id':1001},{})
    assert s==200 and first['products']
    s,unchanged=mod._wa_catalog_ep({'id':1001},{'catalog_version':first['catalog_version']})
    assert s==200 and unchanged['not_modified'] and 'products' not in unchanged and 'vip' in unchanged
    metric('sample_unchanged_catalog',{'full_bytes':len(json.dumps(first).encode()),
        'unchanged_bytes':len(json.dumps(unchanged).encode())})
    mod.db.product_stock.insert_one({'product_id':'101','is_sold':False,'code_line':'LOCAL-ONLY-ADDED'})
    mod._wa_invalidate_catalog()
    s,changed=mod._wa_catalog_ep({'id':1001},{'catalog_version':first['catalog_version']})
    assert not changed.get('not_modified') and changed['catalog_version']!=first['catalog_version']
    assert changed['products'][0]['stock']==first['products'][0]['stock']+1

def test_root_prefix_success_is_persisted_and_stops_rediscovery(mod,monkeypatch):
    store=supplier(mod);data={'products':[{'id':'1','stock':2,'price':1}]};calls=[]
    def get(url,**kwargs):
        calls.append(url)
        return response(200,data) if url=='https://supplier.example/products' else response(404,{})
    monkeypatch.setattr(mod.requests,'get',get)
    baseline=original_function(mod,'_ext_api_get')
    baseline(dict(store),'/products');baseline(dict(store),'/products')
    assert len(calls)==16
    calls.clear();mod._ext_api_get(store,'/products');first=len(calls)
    reloaded=mod.db.ext_stores.find_one({'_id':store['_id']})
    assert reloaded['api_prefix']=='' and reloaded['api_prefix_known'] is True
    mod._ext_api_get(reloaded,'/products')
    assert first==8 and len(calls)==9
    metric('root_prefix_steady_sync_requests',{'before':8,'after':1,'reduction_percent':87.5})

@pytest.mark.parametrize('status',[401,403,429,500,503])
def test_auth_rate_limits_and_outages_are_not_retried_on_eight_prefixes(mod,monkeypatch,status):
    store=supplier(mod);get=Mock(return_value=response(status,{}));monkeypatch.setattr(mod.requests,'get',get)
    assert mod._ext_api_get(store,'/products') is None and get.call_count==1

def test_unattended_failure_backoff_bypasses_manual_requests_and_resets_after_key_change(mod,monkeypatch):
    store=supplier(mod);now=[time.time()];monkeypatch.setattr(mod.time,'time',lambda:now[0])
    get=Mock(return_value=response(503,{}));monkeypatch.setattr(mod.requests,'get',get)
    assert mod._net_sync_fetch(store) is None and get.call_count==1
    now[0]+=1;assert mod._net_sync_fetch(store) is None and get.call_count==1
    assert mod._ext_api_get(store,'/products') is None and get.call_count==2
    store['api_key']='CHANGED-FAKE-KEY'
    mod._net_sync_fetch(store);assert get.call_count==3

@pytest.mark.parametrize('base',['https://supplier.example','https://api.insightxpro.store'])
def test_conditional_supplier_sync_reuses_304_but_purchase_catalog_reads_stay_fresh(mod,monkeypatch,base):
    store=supplier(mod,base_url=base,api_prefix='/api/v1',api_prefix_known=True)
    data={'products':[{'id':'1','stock':3,'price':1}]};calls=[]
    def get(*args,**kwargs):
        headers=kwargs.get('headers') or {};calls.append(dict(headers))
        return response(304) if headers.get('If-None-Match')=='"version-1"' else response(200,data,{'ETag':'"version-1"'})
    monkeypatch.setattr(mod.requests,'get',get);monkeypatch.setattr(mod.requests,'request',get)
    assert mod._net_sync_fetch(store)==data and mod._net_sync_fetch(store)==data
    assert 'If-None-Match' not in calls[0] and calls[1]['If-None-Match']=='"version-1"'
    assert mod._ext_api_get(store,'/products')==data and 'If-None-Match' not in calls[2]

def test_large_or_invalid_remote_catalogs_are_never_reused_for_conditional_reads(mod):
    store=supplier(mod);mod._NET_SYNC_CONTEXT.active=True
    try:
        mod._net_catalog_remember(store,'/products',response(headers={'ETag':'x'}),{'unexpected':[]})
        assert not mod._NET_REMOTE
        data={'products':[{'id':'1','stock':1,'desc':'x'*600000}]}
        mod._net_catalog_remember(store,'/products',response(headers={'ETag':'x'}),data)
        assert not mod._NET_REMOTE
    finally:mod._NET_SYNC_CONTEXT.active=False

def test_supplier_sync_uses_one_product_collection_read_instead_of_one_per_row(mod,monkeypatch):
    store=supplier(mod);sid=str(store['_id']);n=150
    data={'products':[{'id':str(i),'name':'Product','stock':2,'price':1} for i in range(n)]}
    mod.db.ext_products.insert_many([{'store_id':sid,'ext_id':str(i),'stock':2,
        'cost_price':1,'base_price':1,'sell_price':1,'_supplier_available':True,'unlimited':False} for i in range(n)])
    monkeypatch.setattr(mod,'_ext_api_get',lambda *a,**k:data)
    find=Mock(wraps=mod.db.ext_products.find);monkeypatch.setattr(mod.db.ext_products,'find',find)
    original_function(mod,'_auto_sync_ext_stores')();before=find.call_count
    find.reset_mock();mod._auto_sync_ext_stores();after=find.call_count
    assert before==151 and after==1
    assert mod.db.ext_products.count_documents({'stock':2})==n
    metric('supplier_product_reads_per_sync',{'products':n,'before':before,'after':after})

def test_idle_payment_checks_wake_immediately_when_a_deposit_is_created(mod):
    assert mod._net_payment_interval(12)==60
    record=mod.register_pending_deposit(1001,5,5.001,'USDT')
    assert record and all(event.is_set() for event in mod._NET_PAY_EVENTS.values())
    assert mod._net_payment_interval(12)==12
    assert mod.db.users.find_one({'user_id':1001})['balance']==100

def test_expired_deposits_do_not_keep_idle_polling_at_the_fast_rate(mod):
    mod.db.pending_deposits.insert_one({'status':'pending','expires_at':int(time.time())-1})
    assert mod._net_payment_interval(12)==60

def test_binance_remark_detection_is_still_called_without_a_pending_deposit(mod,monkeypatch):
    monkeypatch.setattr(mod,'BINANCE_API_KEY','FAKE-KEY');monkeypatch.setattr(mod,'BINANCE_API_SECRET','FAKE-SECRET')
    get=Mock(return_value=[]);monkeypatch.setattr(mod,'_binance_pay_request',get)
    mod.check_binance_pay_auto()
    assert get.call_count==1 and mod._net_payment_interval(12)==60

def test_payment_state_is_shared_without_five_duplicate_preflight_queries(mod,monkeypatch):
    find=Mock(wraps=mod.db.pending_deposits.find_one);monkeypatch.setattr(mod.db.pending_deposits,'find_one',find)
    assert [mod._net_payment_interval(12) for _ in range(5)]==[60]*5
    assert find.call_count==1

def test_payment_database_read_failure_keeps_fast_recovery_interval(mod,monkeypatch):
    monkeypatch.setattr(mod.db.pending_deposits,'find_one',Mock(side_effect=OSError('offline')))
    assert mod._net_payment_interval(12)==12

@pytest.mark.parametrize('left_checks',[0,2])
def test_healthy_referrals_only_write_a_reset_when_it_changes(mod,monkeypatch,left_checks):
    mod.db.referrals_v2.insert_one({'invited_id':1001,'referrer_id':1002,'status':'active',
        'dv':'ok','dv_at':1,'left_checks':left_checks})
    monkeypatch.setattr(mod,'check_forced_sub',lambda *a,**k:True)
    update=Mock(wraps=mod.db.referrals_v2.update_one);monkeypatch.setattr(mod.db.referrals_v2,'update_one',update)
    assert mod._ref_checker_tick()==1
    resets=[c for c in update.call_args_list if 'left_checks' in c.args[1].get('$set',{})]
    assert len(resets)==int(left_checks!=0)
    assert mod.db.referrals_v2.find_one({'invited_id':1001})['left_checks']==0
    assert mod._ref_checker_tick()==0


def test_outbound_http_measurement_records_sizes_without_tokens_or_payloads(mod,monkeypatch):
    transport=Mock(return_value=response(200,{}, {'Content-Length':'321'}))
    def fake_transport(self,prepared,*args,**kwargs):return transport(prepared,*args,**kwargs)
    monkeypatch.setattr(mod.requests.adapters.HTTPAdapter,'send',fake_transport)
    mod._net_install_clients()
    prepared=mod.requests.Request('POST','https://api.telegram.org/bot123:FAKE-TOKEN/sendMessage?password=FAKE-PASSWORD',
        headers={'Authorization':'Bearer FAKE-SECRET'},data='FAKE-CUSTOMER-CODE').prepare()
    returned=mod.requests.adapters.HTTPAdapter().send(prepared)
    assert returned.status_code==200
    report=mod._NET_METER.snapshot();encoded=json.dumps(report)
    assert not any(v in encoded for v in ('FAKE-TOKEN','FAKE-PASSWORD','FAKE-SECRET','FAKE-CUSTOMER-CODE'))
    assert 'api.telegram.org/sendMessage' in encoded
    row=report['rows'][0];assert row['requests']==1 and row['received']==321 and row['sent']>0

def test_mongo_listener_records_command_sizes_without_documents(mod):
    listener=mod._NET_MONGO_LISTENER
    listener.started(types.SimpleNamespace(command_name='update',command={'password':'HIDDEN-MONGO-VALUE','q':{'id':123}}))
    listener.succeeded(types.SimpleNamespace(command_name='update',reply={'ok':1,'value':'HIDDEN-RESPONSE-VALUE'}))
    text=json.dumps(mod._NET_METER.snapshot())
    assert 'HIDDEN-' not in text
    row=mod._NET_METER.snapshot()['rows'][0]
    assert row['requests']==1 and row['sent']>0 and row['received']>0

@pytest.mark.parametrize('path,expected',[('/SECRET-GATEWAY/products?api_key=SECRET','/api/products'),
    ('/SECRET-GATEWAY/product/SECRET-PRODUCT','/api/product'),('/webapp/cart/status?token=SECRET','/webapp/cart'),
    ('/app/emoji/SECRET-FILE.webp','/app/emoji'),('/','/')])
def test_server_metric_labels_never_store_secret_paths_or_queries(mod,path,expected):
    assert mod._net_server_route(path)==expected

def test_admin_network_report_is_private_and_available_in_console(mod):
    mod._NET_METER.record('http_client','supplier.example',sent=1024,received=2048)
    dispatch(mod,'ad_network',1001)
    assert not any('supplier.example' in text for _,text,_ in MESSAGES)
    MESSAGES.clear();dispatch(mod,'ad_network',9001)
    assert any('supplier.example' in text and 'فاتورة' in text for _,text,_ in MESSAGES)
    assert 'ad_network' in mod._ADM_OPEN_OK and not mod._wa_bot_only('ad_network')
