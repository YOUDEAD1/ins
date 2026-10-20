"""Signed referral UI fixture: fake identities, memory DB and loopback sockets only."""
import json
import os
import re
import socket
import urllib.parse
from http.server import ThreadingHTTPServer

from harness import load, APP, TOKEN, offline
from test_integration import signed

m = load()
assert m.bot.token == TOKEN and m.MONGO_URI == 'mongodb://localhost/test'
m.requests.sessions.Session.request = offline
m.telebot.apihelper._make_request = offline
m.cffi_requests.get = offline
m.cffi_requests.post = offline
m.cffi_requests.delete = offline
connect, connect_ex = socket.socket.connect, socket.socket.connect_ex


def local_only(sock, address):
    if isinstance(address, tuple) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
        raise RuntimeError('External sockets disabled in local referral fixture')
    return connect(sock, address)


def local_only_ex(sock, address):
    if isinstance(address, tuple) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
        raise RuntimeError('External sockets disabled in local referral fixture')
    return connect_ex(sock, address)


socket.socket.connect = local_only
socket.socket.connect_ex = local_only_ex
for uid, debt in [(1001, .75), (5001, 0)]:
    m.db.users.insert_one({'user_id': uid, 'name': 'Referral Test', 'balance': 0., 'ref_debt': debt,
        'lang': 'en', 'lang_chosen': True, 'is_admin': 0, 'is_banned': 0})
m.db.referrals_v2.insert_one({'referrer_id': 1001, 'invited_id': 5001, 'status': 'pending',
                            'created_at': 1, 'ref_policy_version': 30})


class Handler(m.APIHandler):
    def do_POST(self):
        if self.path == '/__ref_test__/settle':
            m._ref_money(1001, .75, 'ref_reward', 'Fake test reward', event_key='browser-test-debt-settlement')
            return m._wa_send(self, 200, json.dumps({'ok': True}), 'application/json')
        return super().do_POST()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == '/__ref_test__/':
            uid = int(urllib.parse.parse_qs(parsed.query).get('uid', ['5001'])[0])
            assert uid in (1001, 5001)
            script = '<script>window.Telegram={WebApp:{colorScheme:"light",platform:"web",version:"8.0",initData:' + json.dumps(signed(uid)) + ',initDataUnsafe:{user:{id:' + str(uid) + ',language_code:"en"}},ready(){},expand(){},onEvent(){},offEvent(){},setHeaderColor(){},setBackgroundColor(){},enableClosingConfirmation(){},isVersionAtLeast(){return false;},BackButton:{show(){},hide(){},onClick(){},offClick(){}},HapticFeedback:{impactOccurred(){},notificationOccurred(){},selectionChanged(){}}}};</script>'
            content = (APP / 'webapp/index.html').read_text().replace('<head>', '<head><base href="/app/">' + script, 1)
            content = re.sub(r'<!--TG-SDK-->.*?<!--/TG-SDK-->', '', content, flags=re.S)
            return m._wa_send(self, 200, content, 'text/html; charset=utf-8')
        if parsed.path == '/__ref_test__/state':
            rec = m._ref_find(5001)
            owner = m.db.users.find_one({'user_id': 1001})
            return m._wa_send(self, 200, json.dumps({'status': rec['status'],
                'verified': m._ref_web_verified(rec), 'proof_version': rec.get('web_verified_version'),
                'used_challenges': m.db.ref_verification_challenges.count_documents({'used': True}),
                'balance': owner['balance'], 'debt': owner['ref_debt'],
                'money_events': m.db.ref_money_events.count_documents({})}), 'application/json')
        return super().do_GET()


print('LOCAL REFERRAL SERVER', flush=True)
ThreadingHTTPServer(('127.0.0.1', int(os.getenv('LARA_TEST_PORT', '8785'))), Handler).serve_forever()
