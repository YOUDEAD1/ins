"""Loopback-only catalogue/locale UI fixture; memory DB and fake Telegram identity."""
import json
import os
import socket
from http.server import ThreadingHTTPServer
from harness import load, APP, TOKEN, offline
from test_integration import signed

m = load()
assert m.bot.token == TOKEN and m.MONGO_URI == 'mongodb://localhost/test'
m.requests.sessions.Session.request = offline
m.telebot.apihelper._make_request = offline
m.cffi_requests.get = m.cffi_requests.post = m.cffi_requests.delete = offline
connect, connect_ex = socket.socket.connect, socket.socket.connect_ex
def local_connect(sock, address):
    if isinstance(address, tuple) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
        raise RuntimeError('Non-local connections forbidden in this fixture')
    return connect(sock, address)
def local_connect_ex(sock, address):
    if isinstance(address, tuple) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
        raise RuntimeError('Non-local connections forbidden in this fixture')
    return connect_ex(sock, address)
socket.socket.connect, socket.socket.connect_ex = local_connect, local_connect_ex
m.db.users.insert_one({'user_id': 1001, 'name': 'Local Test', 'balance': 100, 'lang': 'ar', 'is_admin': 0})
p = {'id': '101', 'name_en': 'Visible Product', 'name_ar': 'منتج ظاهر', 'price': 5}
m.db.products.insert_one(p)
m.db.product_stock.insert_one({'product_id': '101', 'is_sold': False, 'code_line': 'LOCAL-TEST'})
m.db.products.insert_one({'id': '102', 'name_en': 'Hidden Product', 'name_ar': 'منتج مخفي', 'price': 2, 'is_hidden': True})
cat = {'name_en': 'Visible Folder', 'name_ar': 'مجلد ظاهر', 'product_ids': ['101', 101, p['_id'], str(p['_id']), '102', 'deleted']}
m.db.catalogs.insert_one(cat)
m.db.catalogs.insert_many([
    {'name_en': 'Empty Folder', 'name_ar': 'مجلد فارغ', 'product_ids': []},
    {'name_en': 'Hidden-only Folder', 'name_ar': 'كلها مخفية', 'product_ids': ['102']}])
script = '<script>window.Telegram={WebApp:{initData:' + json.dumps(signed()) + \
    ",initDataUnsafe:{user:{id:1001,language_code:'en'}},ready(){},expand(){},onEvent(){},offEvent(){},setHeaderColor(){},setBackgroundColor(){},isVersionAtLeast(){return false;},BackButton:{show(){},hide(){},onClick(){},offClick(){}},HapticFeedback:{impactOccurred(){},notificationOccurred(){},selectionChanged(){}}}};</script>"

class Handler(m.APIHandler):
    def do_GET(self):
        path = self.path.split('?')[0]
        if path == '/__round37_test__/':
            content = (APP / 'webapp/index.html').read_text().replace('<head>', '<head><base href="/app/">' + script, 1)
            anchor = 'let lastCat = 0, catalogFlight = null;'
            assert anchor in content
            content = content.replace(anchor, anchor + '\nwindow.__round37Test={refresh:()=>refreshCatalog()};', 1)
            return m._wa_send(self, 200, content, 'text/html; charset=utf-8')
        if path in ('/__round37_test__/hide', '/__round37_test__/show'):
            m.db.catalogs.update_one({'_id': cat['_id']}, {'$set': {'is_hidden': path.endswith('/hide')}})
            return m._wa_send(self, 200, '{}', 'application/json')
        if path == '/__round37_test__/state':
            data = {'user': {'lang': m.db.users.find_one({'user_id': 1001}).get('lang'),
                             'ui_lang': m.db.users.find_one({'user_id': 1001}).get('ui_lang')},
                    'balance': m._wa_balance(1001), 'orders': m.db.orders.count_documents({})}
            return m._wa_send(self, 200, json.dumps(data), 'application/json')
        return super().do_GET()

print('ROUND37 LOCAL SERVER', flush=True)
ThreadingHTTPServer(('127.0.0.1', int(os.getenv('LARA_TEST_PORT', '8794'))), Handler).serve_forever()
