"""Single active bot process across overlapping Render deploys.

This module runs before bot.py imports or starts any payment workers. A waiting
instance answers only health probes; it never imports the application. The
MongoDB record is deliberately retained after exit, including its expiry, so a
replacement cannot take ownership while shutdown work might still be running.
"""
import hashlib
import json
import logging
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dotenv import load_dotenv
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError
from pymongo.write_concern import WriteConcern

LOG = logging.getLogger('lara.runtime')
LEASE_SECONDS = 45
RENEW_SECONDS = 5
STOP_AFTER_SECONDS = 20


def lease_key(token):
    identity = token.partition(':')[0]
    if not identity.isascii() or not identity.isdigit():
        raise ValueError('Invalid Telegram bot identity')
    # Token rotation must not create a second independent lock for the same bot.
    return 'bot:' + hashlib.sha256(identity.encode()).hexdigest()


class RuntimeLease:
    def __init__(self, collection, key, *, owner=None, clock=time.monotonic):
        self.collection = collection
        self.key = key
        self.owner = owner or uuid.uuid4().hex
        self.clock = clock
        self.last_confirmed = None

    def initialize(self, initial_wait_seconds):
        # All lease and migration times use MongoDB's clock, not host clocks.
        # Never attach a TTL index: deleting the record would repeat migration.
        try:
            self.collection.update_one({'_id': self.key}, [{'$set': {
                'expires_at': {'$ifNull': ['$expires_at', '$$NOW']},
                'bootstrap_until': {'$ifNull': ['$bootstrap_until',
                    {'$add': ['$$NOW', initial_wait_seconds * 1000]}]}
            }}], upsert=True)
        except DuplicateKeyError:
            pass  # Another candidate created this same coordination record.

    def _write(self, acquiring):
        began = self.clock()
        condition = ({'$and': [
            {'$lte': ['$expires_at', '$$NOW']},
            {'$lte': ['$bootstrap_until', '$$NOW']}
        ]} if acquiring else {'$gt': ['$expires_at', '$$NOW']})
        query = {'_id': self.key, '$expr': condition}
        if not acquiring:
            query['owner'] = self.owner
        row = self.collection.find_one_and_update(query, [{'$set': {
            'owner': self.owner,
            'expires_at': {'$add': ['$$NOW', LEASE_SECONDS * 1000]},
            'revision': os.environ.get('RENDER_GIT_COMMIT', '')[:40]
        }}], return_document=ReturnDocument.AFTER)
        if not row or row.get('owner') != self.owner:
            return False
        self.last_confirmed = began
        return True

    def acquire(self):
        return self._write(True)

    def renew(self):
        return self._write(False)

    def safe_to_run(self):
        return self.last_confirmed is not None and self.clock() - self.last_confirmed < STOP_AFTER_SECONDS


_WEBAPP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'webapp')


def _site_host(handler):
    """The store's own domain (not the hosting address, not an IP)."""
    host = (handler.headers.get('Host') or '').split(':')[0].strip().lower()
    if not host or '.' not in host or host.endswith(('.onrender.com', '.render.com')):
        return ''
    if host.replace('.', '').isdigit() or host in ('localhost',):
        return ''
    return host


def _site_page(handler):
    """While waiting, the store's domain still answers like the site: link previews
    (Telegram, WhatsApp) get the real title and cover, and visitors reload in a few seconds,
    instead of a JSON status that apps cache as "file.json 160 B"."""
    host = _site_host(handler)
    path = handler.path.split('?', 1)[0]
    if not host:
        return False
    if path == '/og.png':
        try:
            with open(os.path.join(_WEBAPP_DIR, 'og-cover.png'), 'rb') as f:
                data = f.read()
        except OSError:
            return False
        handler.send_response(200)
        handler.send_header('Content-Type', 'image/png')
        handler.send_header('Content-Length', str(len(data)))
        handler.send_header('Cache-Control', 'public, max-age=3600')
        handler.end_headers()
        handler.wfile.write(data)
        return True
    if not (path in ('', '/') or path.startswith('/p/')):
        return False
    store = (os.environ.get('WEBAPP_STORE_NAME') or 'LARA STOCK').strip() or 'LARA STOCK'
    from html import escape as e
    desc = 'Digital subscriptions and codes with instant delivery, 24/7.'
    base = 'https://' + host
    body = ('<!doctype html><html lang="ar" dir="rtl"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="5">'
            f'<title>{e(store)}</title>'
            f'<meta name="description" content="{e(desc)}">'
            '<meta property="og:type" content="website">'
            f'<meta property="og:site_name" content="{e(store)}">'
            f'<meta property="og:title" content="{e(store)} — Digital subscriptions &amp; codes">'
            f'<meta property="og:description" content="{e(desc)}">'
            f'<meta property="og:url" content="{e(base)}/">'
            f'<meta property="og:image" content="{e(base)}/og.png?v=6">'
            '<meta property="og:image:type" content="image/png">'
            '<meta property="og:image:width" content="1200"><meta property="og:image:height" content="630">'
            '<meta name="twitter:card" content="summary_large_image">'
            '</head><body style="font-family:sans-serif;text-align:center;padding:40px">'
            '<p>جار تحديث المتجر تلقائيًا. يرجى الانتظار قليلًا.</p></body></html>').encode()
    handler.send_response(200)
    handler.send_header('Content-Type', 'text/html; charset=utf-8')
    handler.send_header('Content-Length', str(len(body)))
    handler.send_header('Cache-Control', 'no-store')
    handler.send_header('Retry-After', '5')
    handler.end_headers()
    handler.wfile.write(body)
    return True


class WaitingHealth:
    """Keep Render's port available while waiting for the old owner to stop."""
    def __init__(self, port, *, host='0.0.0.0'):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.split('?', 1)[0].rstrip('/') == '/app':
                    self.send_response(200)
                    self.send_header('Retry-After', '5')
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                    self.send_header('Cache-Control', 'no-store')
                    self.end_headers()
                    self.wfile.write(('<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="5">'
                                      '<title>جار التحديث</title><p dir="rtl">جار تحديث المتجر تلقائيًا. يرجى الانتظار قليلًا.</p>').encode())
                    return
                path = self.path.split('?', 1)[0]
                if path.startswith(('/webapp/blogo', '/webapp/ce/', '/webapp/img/', '/webapp/dl/')) or \
                        path.lower().endswith(('.png', '.webp', '.jpg', '.jpeg', '.svg', '.gif', '.ico', '.js', '.css', '.woff2')):
                    # images/files during the short handoff: a clear "try again" (never a JSON/HTML body that the
                    # browser would show as a broken image), and the page retries them by itself.
                    self.send_response(503)
                    self.send_header('Retry-After', '5')
                    self.send_header('Cache-Control', 'no-store')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                if _site_page(self):
                    return
                body = json.dumps({'status': 'waiting-for-old-instance',
                                   'runtime_handoff': 'single-active-v1',
                                   'bot_active': False, 'ok': False, 'success': False,
                                   'error': 'deployment_handoff'}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Cache-Control', 'no-store')
                self.send_header('Retry-After', '5')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                self.send_response(503)
                self.send_header('Retry-After', '5')
                self.end_headers()

            def log_message(self, *args):
                pass
        self.server = ThreadingHTTPServer((host, port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True,
                                       name='deployment-wait-health')

    def start(self):
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


def start_watchdogs(lease, *, terminate=os._exit, stop_event=None):
    stop = stop_event or threading.Event()
    def renew():
        while not stop.wait(RENEW_SECONDS):
            try:
                if not lease.renew():
                    LOG.error('Runtime ownership lost; stopping this process before another owner starts')
                    stop.set()
                    terminate(1)
                    return
            except Exception:
                # The independent watchdog bounds how long DB outages are tolerated.
                LOG.warning('Runtime ownership renewal unavailable')

    def watchdog():
        while not stop.is_set():
            if not lease.safe_to_run():
                LOG.error('Runtime ownership could not be confirmed; stopping all bot workers')
                stop.set()
                terminate(1)
                return
            stop.wait(0.5)

    threading.Thread(target=renew, daemon=True, name='runtime-lease-renew').start()
    threading.Thread(target=watchdog, daemon=True, name='runtime-lease-watchdog').start()
    return stop


def wait_for_ownership(lease, *, sleep=time.sleep):
    while True:
        try:
            if lease.acquire():
                return
        except Exception:
            LOG.warning('Waiting for database coordination; bot workers remain stopped')
        sleep(2)


def acquire_runtime():
    load_dotenv()
    token = os.environ.get('TOKEN', '').strip()
    uri = os.environ.get('MONGO_URI', '').strip()
    if not token or not uri:
        raise RuntimeError('TOKEN and MONGO_URI are required for coordinated startup')
    # Connect before opening the health port. A broken DB connection must not
    # make a new deployment appear ready and cause Render to remove the old one.
    client = MongoClient(uri, serverSelectionTimeoutMS=3000, connectTimeoutMS=3000,
                         socketTimeoutMS=3000, retryWrites=False, appname='lara-runtime-handoff')
    client.admin.command('ping')
    database = client[os.environ.get('MONGO_DB_NAME', 'shop_db').strip()]
    collection = database.get_collection('runtime_leases',
        write_concern=WriteConcern(w='majority', wtimeout=3000))
    key = lease_key(token)
    lease = RuntimeLease(collection, key)
    on_render = bool(os.environ.get('RENDER') or os.environ.get('RENDER_SERVICE_ID'))
    # One-time transition from the pre-lock version: Render waits 60s before
    # SIGTERM, then defaults to a 30s shutdown delay. Keep a 30s margin.
    initial_wait = max(120, int(os.environ.get('LARA_INITIAL_HANDOFF_SECONDS', '120'))) if on_render else 0
    lease.initialize(initial_wait)
    health = WaitingHealth(int(os.environ.get('PORT', '10000')))
    health.start()
    LOG.warning('Deployment handoff: waiting for the previous bot process; payment workers have not started')
    try:
        wait_for_ownership(lease)
        if not lease.safe_to_run():
            raise RuntimeError('Runtime lease confirmation took too long; refusing to start workers')
        lease.stop_event = start_watchdogs(lease)
    finally:
        health.close()
    # Retain the Mongo client through the lease for the entire process lifetime.
    lease.client = client
    LOG.warning('Deployment handoff: ownership acquired; starting one active bot process')
    return lease
