"""Temporary localhost-only server. Test identities and in-memory DB only."""
import json, os
from http.server import ThreadingHTTPServer
from harness import load, ROOT, APP
from test_integration import signed
m=load()
m.db.users.insert_one({'user_id':1001,'name':'Integration Test','username':'local_test','balance':100.,'lang':'en','is_admin':0,'is_banned':0})
m.db.products.insert_one({'id':'101','name_en':'Integration Test Product','name_ar':'منتج الاختبار','price':5.,'is_manual':False})
for i in range(8):m.db.product_stock.insert_one({'product_id':'101','is_sold':False,'code_line':f'LOCAL-ONLY-{i}'})
for key,value in {'usdt_address':'TEST-ONLY-NOT-A-REAL-WALLET','amount_protection':'on'}.items():m.db.settings.update_one({'key':key},{'$set':{'value':value}},upsert=True)
# Frontend runs in its normal non-demo mode with a fake Telegram wrapper. It
# talks to the original signed API; no production token or account is involved.
script="<script>window.Telegram={WebApp:{initData:"+json.dumps(signed())+",initDataUnsafe:{user:{id:1001,language_code:'en'}},ready(){},expand(){},onEvent(){},offEvent(){},setHeaderColor(){},setBackgroundColor(){},enableClosingConfirmation(){},isVersionAtLeast(){return false;},BackButton:{show(){},hide(){},onClick(){},offClick(){}},HapticFeedback:{impactOccurred(){},notificationOccurred(){},selectionChanged(){}}}};</script>"
class Handler(m.APIHandler):
    def do_GET(self):
        if self.path.split('?')[0]=='/__integration_test__/':
            html=(APP/'webapp/index.html').read_text().replace('<head>','<head><base href="/app/">'+script,1)
            return m._wa_send(self,200,html,'text/html; charset=utf-8')
        return super().do_GET()
print('LOCAL TEST SERVER http://127.0.0.1:8765/__integration_test__/',flush=True)
ThreadingHTTPServer(('127.0.0.1',int(os.getenv('LARA_TEST_PORT','8765'))),Handler).serve_forever()
