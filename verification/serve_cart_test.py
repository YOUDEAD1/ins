"""Local cart UI fixture: fake identity, memory-only DB, no real integrations."""
import json, os, socket
from http.server import ThreadingHTTPServer
from harness import load, APP, TOKEN, offline
from test_integration import signed
m=load()
assert m.bot.token == TOKEN and m.MONGO_URI == 'mongodb://localhost/test'
# Permanent isolation after import, for the entire lifetime of this test server.
# Incoming localhost HTTP remains available; all outbound integrations are disabled.
m.requests.sessions.Session.request = offline
m.telebot.apihelper._make_request = offline
m.cffi_requests.get = offline
m.cffi_requests.post = offline
m.cffi_requests.delete = offline
_connect = socket.socket.connect
_connect_ex = socket.socket.connect_ex
def local_only_connect(sock, address):
 if isinstance(address, tuple) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
  raise RuntimeError('Non-local network is forbidden in this test server')
 return _connect(sock, address)
def local_only_connect_ex(sock, address):
 if isinstance(address, tuple) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
  raise RuntimeError('Non-local network is forbidden in this test server')
 return _connect_ex(sock, address)
socket.socket.connect = local_only_connect
socket.socket.connect_ex = local_only_connect_ex
if os.getenv('LARA_TEST_NETWORK_PROOF') == '1':
 blocked = 0
 with socket.socket() as probe:
  for fn,args in [(m.telebot.apihelper._make_request,(TOKEN,'getMe')),
                  (m.requests.get,('https://example.invalid',)),
                  (probe.connect,(('203.0.113.1',443),))]:
   try:fn(*args)
   except RuntimeError:blocked += 1
 assert blocked == 3
 print('PASS: fake credentials asserted; Telegram, HTTP integrations and external sockets blocked')
 raise SystemExit(0)
m.db.users.insert_one({'user_id':1001,'name':'Integration Test','username':'local_test','balance':100.,'lang':'en','is_admin':0,'is_banned':0})
for pid,name,price,stock in [('101','Netflix Premium · 1 Month',5.,20),('102','Spotify Premium · 1 Month',3.,20)]:
 m.db.products.insert_one({'id':pid,'name_en':name,'name_ar':name,'price':price,'is_manual':False,
    'discount_tiers':[{'min_qty':3,'price':4.}] if pid=='101' else []})
 for i in range(stock):m.db.product_stock.insert_one({'product_id':pid,'is_sold':False,'code_line':f'LOCAL-CART-{pid}-{i}'})
m.db.products.insert_one({'id':'103','name_en':'Canva Pro · Invitation','name_ar':'Canva Pro · Invitation','price':2.,'is_manual':True,'requires_email':True})
script="<script>window.Telegram={WebApp:{colorScheme:'light',initData:"+json.dumps(signed())+",initDataUnsafe:{user:{id:1001,language_code:'en'}},ready(){},expand(){},onEvent(){},offEvent(){},setHeaderColor(){},setBackgroundColor(){},enableClosingConfirmation(){},isVersionAtLeast(){return false;},BackButton:{show(){},hide(){},onClick(){},offClick(){}},HapticFeedback:{impactOccurred(){},notificationOccurred(){},selectionChanged(){}}}};</script>"
class Handler(m.APIHandler):
 def do_GET(self):
  path=self.path.split('?')[0]
  if path=='/__cart_test__/':
   content=(APP/'webapp/index.html').read_text().replace('<head>','<head><base href="/app/">'+script,1)
   return m._wa_send(self,200,content,'text/html; charset=utf-8')
  if path=='/__cart_test__/state':
   return m._wa_send(self,200,json.dumps({'balance':m._wa_balance(1001),'orders':m.db.orders.count_documents({}),
      'checkouts':m.db.cart_checkouts.count_documents({'status':'completed'}),'errors':m.db.system_errors.count_documents({})}),'application/json')
  return super().do_GET()
print('LOCAL CART SERVER',flush=True)
ThreadingHTTPServer(('127.0.0.1',int(os.getenv('LARA_TEST_PORT','8769'))),Handler).serve_forever()
