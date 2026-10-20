"""Signed admin UI fixture. Fake credentials, memory DB and permanent network isolation."""
import datetime as dt
import json,os,socket,re
from http.server import ThreadingHTTPServer
from bson import ObjectId
from harness import load,APP,TOKEN,offline
from test_integration import signed
m=load()
assert m.bot.token==TOKEN and m.MONGO_URI=='mongodb://localhost/test'
m.requests.sessions.Session.request=offline
m.telebot.apihelper._make_request=offline
m.cffi_requests.get=offline;m.cffi_requests.post=offline;m.cffi_requests.delete=offline
connect,connect_ex=socket.socket.connect,socket.socket.connect_ex
def local_only(sock,address):
    if isinstance(address,tuple) and address[0] not in ('127.0.0.1','::1','localhost'):
        raise RuntimeError('External sockets disabled in local fixture')
    return connect(sock,address)
def local_only_ex(sock,address):
    if isinstance(address,tuple) and address[0] not in ('127.0.0.1','::1','localhost'):
        raise RuntimeError('External sockets disabled in local fixture')
    return connect_ex(sock,address)
socket.socket.connect=local_only;socket.socket.connect_ex=local_only_ex
if os.getenv('LARA_TEST_NETWORK_PROOF')=='1':
    blocked=0
    with socket.socket() as probe:
        for fn,args in [(m.telebot.apihelper._make_request,(TOKEN,'getMe')),
                        (m.requests.get,('https://example.invalid',)),
                        (probe.connect,(('203.0.113.1',443),))]:
            try:fn(*args)
            except RuntimeError:blocked+=1
    assert blocked==3
    print('PASS fake credentials; Telegram, HTTP and external sockets blocked')
    raise SystemExit(0)
for uid,name,admin in [(9001,'Owner',1),(1001,'Inspection Customer',0),(1002,'Other Customer',0)]:
    m.db.users.insert_one({'user_id':uid,'name':name,'username':'test_'+str(uid),
        'balance':100.,'lang':'en','is_admin':admin,'is_banned':0})
m.db.products.insert_one({'id':'101','name_en':'Netflix Premium','name_ar':'Netflix Premium','price':5.,'is_manual':False})
for n in range(16):
    m.db.orders.insert_one({'user_id':1001,'product_id':'101','order_id':'NETFLIX-'+str(n),
        'price':5.,'qty':1,'created_at':1700000000+n,'code_delivered':'account'+str(n)+'@example.test\nPassword: TEST-'+str(n),
        'via':'webapp' if n%2 else 'bot'})
cg=ObjectId()
m.db.cgpt_products.insert_one({'_id':cg,'name':'ChatGPT Business','durations':[]})
m.db.orders.insert_one({'user_id':1001,'product_id':'cgpt_'+str(cg),'order_id':'CHATGPT-RENEW',
    'total_price':7.,'qty':1,'code_delivered':'chatgpt_seat:member@example.test','cgpt_email':'member@example.test',
    'cgpt_expires_at':'2026-01-01T00:00:00','type':'cgpt_renewal','via':'webapp'})
expiry=(dt.datetime.now(dt.timezone.utc)+dt.timedelta(days=7)).isoformat()
m.db.cgpt_invites_data.insert_one({'_id':'Workspace A','private_cookie':'NEVER-EXPOSE',
    'data':{'invites':{'member@example.test':{'telegram_uid':1001,'status':'active','expires_at':expiry}}}})
m.db.ext_orders.insert_one({'user_id':1001,'ext_product_id':str(ObjectId()),'order_id':'EXTERNAL-ORDER',
    'product_name':'Canva Pro Supplier','quantity':1,'price':3.,'codes':['supplier@example.test:TEST-PASS'],
    'status':'success','via':'webapp','created_at':1700000019})
m.db.orders.insert_one({'user_id':1002,'product_id':'101','order_id':'OTHER-PRIVATE',
    'price':5.,'code_delivered':'OTHER-USER-PRIVATE-ACCOUNT'})
script='<script>window.Telegram={WebApp:{colorScheme:"light",initData:'+json.dumps(signed(9001))+',initDataUnsafe:{user:{id:9001,language_code:"en"}},ready(){},expand(){},onEvent(){},offEvent(){},setHeaderColor(){},setBackgroundColor(){},enableClosingConfirmation(){},isVersionAtLeast(){return false;},BackButton:{show(){},hide(){},onClick(){},offClick(){}},HapticFeedback:{impactOccurred(){},notificationOccurred(){},selectionChanged(){}}}};</script>'
class Handler(m.APIHandler):
    def do_GET(self):
        path=self.path.split('?')[0]
        if path=='/__user_test__/':
            content=(APP/'webapp/index.html').read_text().replace('<head>','<head><base href="/app/">'+script,1)
            content=re.sub(r'<!--TG-SDK-->.*?<!--/TG-SDK-->','',content,flags=re.S)
            return m._wa_send(self,200,content,'text/html; charset=utf-8')
        if path=='/__user_test__/diagnostics':
            m._error_drain()
            records=[{k:d.get(k) for k in ('message','location','trace','operation','user_id')} for d in m.db.system_errors.find({'source':'browser'})]
            return m._wa_send(self,200,json.dumps(records),'application/json')
        if path=='/__user_test__/state':
            return m._wa_send(self,200,json.dumps({'orders':m.db.orders.count_documents({}),
                'balance':m.db.users.find_one({'user_id':1001})['balance'],
                'balance_logs':m.db.balance_logs.count_documents({})}),'application/json')
        return super().do_GET()
print('LOCAL USER INSPECTION SERVER',flush=True)
ThreadingHTTPServer(('127.0.0.1',int(os.getenv('LARA_TEST_PORT','8782'))),Handler).serve_forever()
