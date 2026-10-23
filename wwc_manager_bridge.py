"""Authenticated manager bridge. NSS remains the only wallet authority.

No arbitrary queries, incoming prices, bearer database keys or balance setters.
Every call has a signed body, a five-minute timestamp and a durable unique nonce.
"""
import datetime as dt
import hashlib
import hmac
import json
import re
import time
import urllib.parse
from contextlib import contextmanager
from functools import wraps
from pymongo.errors import DuplicateKeyError


class ManagerBridge:
    prefix = '/wwc/manager/v1/'

    def __init__(self, platform):
        self.platform = platform
        self.indexed = False

    @contextmanager
    def capture(self):
        """Only this request thread captures the old UI; other bot users are unaffected."""
        delivery = self.platform.delivery
        previous = getattr(delivery, 'capture', None)
        messages = []
        delivery.capture = messages
        try:
            yield messages
        finally:
            delivery.capture = previous

    def authenticate(self, path, headers, body):
        secret = self.platform.client.secret
        stamp = str(headers.get('X-WWC-Time', ''))
        nonce = str(headers.get('X-WWC-Nonce', ''))
        actual = str(headers.get('X-WWC-Signature', ''))
        if (len(secret) < 32 or not re.fullmatch(r'\d{10}', stamp)
                or abs(time.time() - int(stamp)) > 300
                or not re.fullmatch(r'[A-Za-z0-9_-]{20,80}', nonce)
                or not re.fullmatch(r'[a-f0-9]{64}', actual)):
            return 401
        payload = '\n'.join(('POST', path, stamp, nonce, hashlib.sha256(body).hexdigest()))
        expected = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, actual):
            return 401
        if not self.indexed:
            self.platform.db.wwc_manager_nonces.create_index('expires_at', expireAfterSeconds=0)
            self.indexed = True
        try:
            self.platform.db.wwc_manager_nonces.insert_one({
                '_id': nonce, 'expires_at': dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=11)})
        except DuplicateKeyError:
            return 409
        return 200

    def account(self, actor):
        p = self.platform
        user = p.db.users.find_one({'user_id': actor}, {'balance': 1, 'lang': 1})
        if not user:
            raise RuntimeError('NSS_ACCOUNT_REQUIRED')
        if p.g.get('is_user_banned', lambda _: False)(actor):
            raise RuntimeError('ACCOUNT_RESTRICTED')
        # Display only. All debit decisions still use the atomic wallet functions.
        from wwc_platform import money_text
        return {'balance': money_text(user.get('balance', 0)), 'currency': 'USD',
                'language': p.g['get_lang'](actor)}

    def checkout(self, actor, token):
        p = self.platform
        quote = p.db.wwc_subscriptions.find_one({
            '_id': 'wwcwallet:' + token, 'payer_id': actor, 'payment_method': 'nss_wallet'})
        if not quote:
            raise RuntimeError('CHECKOUT_NOT_FOUND')
        receipt = quote.get('wallet_receipt') or {}
        from wwc_platform import money_text
        return dict(token=token, shop_id=quote['shop_id'], bot_username=quote['bot_username'],
                    state=quote['state'], amount=quote['amount_usd'], currency='USD', days=quote['days'],
                    expires_at=quote['expires_at'].replace(tzinfo=dt.timezone.utc).isoformat(),
                    expired=quote['expires_at'].replace(tzinfo=dt.timezone.utc) <= dt.datetime.now(dt.timezone.utc),
                    balance=self.account(actor)['balance'],
                    balance_after=money_text(receipt['balance_after']) if receipt else None,
                    reference='NSS-' + token if receipt else None)

    def execute(self, action, data):
        p = self.platform
        actor = data.get('actor_id')
        if isinstance(actor, bool) or not isinstance(actor, int) or not 0 < actor < 2**53:
            raise RuntimeError('INVALID_ACTOR')
        account = self.account(actor)
        if action == 'account':
            return dict(ok=True, **account)
        if action == 'source':
            url, key = p.source_connection(actor)
            return dict(ok=True, source_url=url, source_key=key)
        if action not in ('wallet_quote', 'wallet_confirm', 'wallet_cancel', 'wallet_status'):
            raise RuntimeError('NOT_FOUND')
        token = str(data.get('token', ''))
        with self.capture():
            if action == 'wallet_quote':
                shop_id = str(data.get('shop_id', ''))
                if not re.fullmatch(r's_[a-f0-9]{24}', shop_id):
                    raise RuntimeError('INVALID_SHOP')
                p.wallet_subscribe(actor, shop_id)
                pointer = p.db.wwc_wallet_checkouts.find_one({'_id': shop_id, 'payer_id': actor})
                if not pointer:
                    raise RuntimeError('WALLET_CHECKOUT_DISABLED')
                token = pointer['checkout_id'].split(':')[-1]
            elif not re.fullmatch(r'[a-f0-9]{32}', token):
                raise RuntimeError('INVALID_CHECKOUT')
            elif action == 'wallet_confirm':
                p.wallet_confirm(actor, token)
            elif action == 'wallet_cancel':
                p.wallet_cancel(actor, token)
        return dict(ok=True, checkout=self.checkout(actor, token))

    def install(self):
        bridge = self
        handler = self.platform.g['APIHandler']
        original = handler.do_POST

        @self.platform.g['_api_http_guard']
        def endpoint(h):
            path = urllib.parse.urlsplit(h.path)
            if path.query or not re.fullmatch(bridge.prefix + r'[a-z_]+', path.path):
                return bridge.platform.g['_json_resp'](h, 404, {'ok': False, 'error': 'NOT_FOUND'})
            try:
                length = int(h.headers.get('Content-Length', '0'))
            except ValueError:
                length = 0
            if not 0 < length <= 16384 or h.headers.get('Transfer-Encoding'):
                return bridge.platform.g['_json_resp'](h, 413, {'ok': False, 'error': 'INVALID_BODY_LENGTH'})
            body = h.rfile.read(length)
            code = bridge.authenticate(path.path, h.headers, body)
            if code != 200:
                return bridge.platform.g['_json_resp'](h, code, {'ok': False, 'error': 'REPLAY_REJECTED' if code == 409 else 'INVALID_SIGNATURE'})
            try:
                data = json.loads(body)
                if not isinstance(data, dict):
                    raise ValueError()
                value = bridge.execute(path.path[len(bridge.prefix):], data)
                return bridge.platform.g['_json_resp'](h, 200, value)
            except (ValueError, TypeError, UnicodeError):
                return bridge.platform.g['_json_resp'](h, 400, {'ok': False, 'error': 'INVALID_JSON'})
            except RuntimeError as exc:
                reason = str(exc)
                if not re.fullmatch(r'[A-Z_0-9]{1,100}', reason):
                    reason = 'SERVICE_UNAVAILABLE'
                code = 404 if reason == 'NOT_FOUND' else 409
                return bridge.platform.g['_json_resp'](h, code, {'ok': False, 'error': reason})

        @wraps(original)
        def post(h):
            if urllib.parse.urlsplit(h.path).path.startswith(bridge.prefix):
                return endpoint(h)
            return original(h)
        handler.do_POST = post


def install(platform):
    bridge = ManagerBridge(platform)
    bridge.install()
    return bridge
