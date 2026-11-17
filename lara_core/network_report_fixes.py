"""Fix repeated work identified in the owner's process-local network report.

Public catalogue reads only: authentication, balances and purchases stay live.
All caches are bounded, single-flight and invalidated by local writes.
"""
import collections
import copy
import functools
import hashlib
import threading
import time

from bson import json_util


class NetworkReportFixes:
    READ_COLLECTIONS = frozenset(('products', 'ext_products', 'cgpt_products'))
    MAX_ENTRIES = 96
    MAX_BYTES = 8 * 1024 * 1024
    TTL = 30

    def __init__(self, namespace):
        self.ns = namespace
        self.db = namespace['db']
        self.context = threading.local()
        self.lock = threading.RLock()
        self.read_locks = [threading.RLock() for _ in range(32)]
        self.entries = collections.OrderedDict()
        self.epochs = collections.Counter()
        self.counters = collections.Counter()
        self.seen_contacts = collections.OrderedDict()
        self.http_statuses = collections.Counter()
        self.rpc_health = {}
        self.binance_lock = threading.Lock()
        self.binance_health = {'credentials': None, 'failures': 0, 'retry_at': 0.}

    def owns(self, collection):
        return (collection.database.name == self.db.name and
                collection.database.client is self.db.client)

    def invalidate(self, *groups):
        # Never wait for an in-flight database/network read before a wallet write.
        with self.lock:
            for group in groups:
                self.epochs[group] += 1
            for key, entry in list(self.entries.items()):
                if set(groups).intersection(entry['groups']):
                    self.entries.pop(key, None)

    def changed(self, collection, method, args, kwargs):
        if not self.owns(collection):
            return
        name = collection.name
        update = args[1] if len(args) > 1 else kwargs.get('update')
        touched = {key for op, values in (update or {}).items()
                   if op.startswith('$') and isinstance(values, dict) for key in values} if isinstance(update, dict) else set()
        metadata = {'stock_oos_since', 'stock_auto_hidden', 'stock_auto_hidden_at',
                    'stock_visibility_override', 'stock_history_version',
                    'stock_last_available_at', 'stock_oos_source'}
        if name in self.READ_COLLECTIONS and not (touched and touched <= metadata):
            self.invalidate(name)
        elif name == 'custom_buttons':
            self.invalidate('notice_buttons')
        elif name == 'product_stock':
            self.invalidate('stock_counts')
        if name in ('cgpt_accounts', 'cgpt_cookies', 'cgpt_invites_data'):
            self.invalidate('seat_count')
            self.ns['_invalidate_products_cache']()

    def deadline(self, expires):
        if getattr(self.context, 'api', False):
            self.context.catalogue_deadline = min(getattr(self.context, 'catalogue_deadline', expires), expires)

    def cached(self, key, groups, load, ttl=TTL, valid=None):
        with self.read_locks[hash(key) % len(self.read_locks)]:
            with self.lock:
                now = time.monotonic()
                epoch = tuple(self.epochs[group] for group in groups)
                entry = self.entries.get(key)
                if entry and entry['epoch'] == epoch and entry['expires'] > now:
                    self.entries.move_to_end(key)
                    self.counters['shared_read_hits'] += 1
                    self.deadline(entry['expires'])
                    return copy.deepcopy(entry['value'])
            value = load()  # Failed reads are never remembered as empty inventory.
            if valid is not None and not valid(value):
                return value
            size = len(json_util.dumps(value, separators=(',', ':')).encode())
            with self.lock:
                self.counters['shared_read_misses'] += 1
                if epoch == tuple(self.epochs[group] for group in groups) and size <= self.MAX_BYTES:
                    for old_key, old in list(self.entries.items()):
                        if old['expires'] <= now or old_key == key:
                            self.entries.pop(old_key, None)
                    while self.entries and (len(self.entries) >= self.MAX_ENTRIES or
                            sum(old['bytes'] for old in self.entries.values()) + size > self.MAX_BYTES):
                        self.entries.popitem(last=False)
                    expires = time.monotonic() + (ttl(value) if callable(ttl) else ttl)
                    self.entries[key] = {'groups': groups, 'epoch': epoch, 'value': copy.deepcopy(value),
                                         'expires': expires, 'bytes': size}
                    self.deadline(expires)
            return value

    def api_payload(self, original, uid):
        previous = vars(self.context).copy()
        self.context.api = True
        self.context.uid = uid
        self.context.hidden = None
        self.context.catalogue_deadline = float('inf')
        try:
            return original(uid)
        finally:
            vars(self.context).clear()
            vars(self.context).update(previous)

    def products_put(self, original, uid, data, generation):
        result = original(uid, data, generation)
        deadline = getattr(self.context, 'catalogue_deadline', float('inf'))
        if deadline != float('inf'):
            with self.ns['_NET_CACHE_LOCK']:
                entry = (self.ns['_PRODUCTS_CACHE'] or {}).get('_users', {}).get(uid)
                if entry:
                    entry['expires'] = min(entry['expires'], time.time() + max(0, deadline - time.monotonic()))
        return result

    def read(self, original, collection, method, args, kwargs):
        if getattr(self.context, 'read_passthrough', False):
            return original(collection, *args, **kwargs)
        if not self.owns(collection):
            return original(collection, *args, **kwargs)
        query = args[0] if args else kwargs.get('filter', {})
        if getattr(self.context, 'fanout', False) and collection.name == 'users' and method == 'find' and not query:
            rows = list(original(collection, {'is_banned': {'$ne': 1}, 'stock_notice_unreachable': {'$ne': True}},
                {'user_id': 1, 'lang': 1, 'lang_chosen': 1, 'is_banned': 1}).batch_size(200))
            self.context.languages = {row['user_id']: self.language(row) for row in rows if 'user_id' in row}
            self.counters['broadcast_recipient_batches'] += 1
            return iter(rows)
        if not getattr(self.context, 'api', False):
            return original(collection, *args, **kwargs)
        if collection.name == 'api_hidden':
            if method == 'find' and query == {'api_user_id': self.context.uid}:
                rows = list(original(collection, *args, **kwargs))
                self.context.hidden = rows
                return iter(rows)
            if (method == 'find_one' and len(args) == 1 and not kwargs and isinstance(query, dict) and
                    set(query) == {'api_user_id', 'product_id'} and query['api_user_id'] == self.context.uid and
                    isinstance(query['product_id'], dict) and set(query['product_id']) == {'$in'} and
                    self.context.hidden is not None):
                self.counters['hidden_checks_reused'] += 1
                return next((copy.deepcopy(row) for row in self.context.hidden
                             if row.get('product_id') in query['product_id']['$in']), None)
        if (collection.name not in self.READ_COLLECTIONS or len(args) > 2 or
                set(kwargs) - {'filter', 'projection'}):
            return original(collection, *args, **kwargs)
        # BSON-aware keys keep ObjectId and string identifiers distinct.
        key = ('catalogue', collection.name, method,
               json_util.dumps([args, kwargs], sort_keys=True, separators=(',', ':')))
        def load():
            previous = getattr(self.context, 'read_passthrough', False)
            self.context.read_passthrough = True
            try:
                value = original(collection, *args, **kwargs)
                return list(value) if method == 'find' else value
            finally:
                self.context.read_passthrough = previous
        value = self.cached(key, (collection.name,), load)
        return iter(value) if method == 'find' else value

    def stock_counts(self, original, products):
        if not getattr(self.context, 'api', False):
            return original(products)
        key = ('stock_counts', tuple((str(p.get('id', p.get('_id', ''))), bool(p.get('is_manual'))) for p in products))
        return self.cached(key, ('products', 'stock_counts'), lambda: original(products),
                           valid=lambda value: value is not None)

    def seats(self, original, fresh=False):
        if fresh:
            self.invalidate('seat_count')
            return original(fresh=True)
        def load():
            value = original(fresh=False)
            return {'value': value, 'known': bool(getattr(self.ns['_CGPT_SEAT_PROOF'], 'known', False))}
        hit = self.cached(('seat_count',), ('seat_count',), load,
                          ttl=lambda result: self.TTL if result['known'] else 5)
        self.ns['_CGPT_SEAT_PROOF'].known = hit['known']
        return hit['value']

    @staticmethod
    def language(user):
        lang = user.get('lang', 'ar') if user.get('lang_chosen') else 'en'
        return lang if lang in ('ar', 'en') else 'en'

    def fanout(self, original, *args):
        previous = vars(self.context).copy()
        self.context.fanout = True
        self.context.languages = {}
        self.counters['broadcast_runs'] += 1
        try:
            return original(*args)
        finally:
            vars(self.context).clear()
            vars(self.context).update(previous)

    def in_fanout(self):
        return bool(getattr(self.context, 'fanout', False))

    def get_lang(self, original, uid):
        if self.in_fanout() and uid in getattr(self.context, 'languages', {}):
            return self.context.languages[uid]
        return original(uid)

    def button(self, original, lang, key):
        if not self.in_fanout() or key not in ('btn_notice_site', 'btn_notice_bot'):
            return original(lang, key)
        rows = self.cached(('notice_buttons',), ('notice_buttons',), lambda: list(self.db.custom_buttons.find(
            {'key': {'$in': ['btn_notice_site', 'btn_notice_bot']}, 'lang': {'$in': ['ar', 'en']}})))
        return next((row for row in rows if row.get('key') == key and row.get('lang') == lang), None)

    def send(self, original, *args, **kwargs):
        uid = args[0] if args else kwargs.get('chat_id')
        if not self.in_fanout() or uid not in getattr(self.context, 'languages', {}):
            return original(*args, **kwargs)
        try:
            result = self.ns['_broadcast_deliver'](original, *args, **kwargs)
            self.counters['broadcast_delivered'] += 1
            return result
        except Exception as exc:
            kind = self.ns['_telegram_delivery_kind'](exc)
            self.counters['broadcast_failed_' + kind] += 1
            if kind == 'unreachable':
                try:
                    self.db.users.update_one({'user_id': uid}, {'$set': {
                        'stock_notice_unreachable': True, 'stock_notice_unreachable_at': int(time.time())}})
                    with self.lock:
                        self.seen_contacts.pop(uid, None)
                except Exception:
                    self.ns['logger'].debug('Broadcast reachability marker will retry on a later rejected send')
            raise

    def contact_seen(self, uid):
        try:
            uid = int(uid)
            if uid <= 0:
                return
            with self.lock:
                if self.seen_contacts.get(uid, 0) > time.monotonic():
                    return
            self.db.users.update_one({'user_id': uid, 'stock_notice_unreachable': True}, {'$unset': {
                'stock_notice_unreachable': '', 'stock_notice_unreachable_at': ''}})
            with self.lock:
                self.seen_contacts[uid] = time.monotonic() + 86400
                self.seen_contacts.move_to_end(uid)
                while len(self.seen_contacts) > 8192:
                    self.seen_contacts.popitem(last=False)
        except Exception:
            self.ns['logger'].debug('Inbound contact could not reset the stock-notification marker')

    def incoming(self, original, events, member=False):
        for event in events or ():
            if member:
                chat = getattr(event, 'chat', None)
                status = getattr(getattr(event, 'new_chat_member', None), 'status', None)
                if getattr(chat, 'type', None) == 'private' and status == 'member':
                    self.contact_seen(chat.id)
            else:
                user = getattr(event, 'from_user', None)
                if user is not None and not getattr(user, 'is_bot', False):
                    self.contact_seen(user.id)
        return original(events)

    def binance(self, original):
        credentials = hashlib.sha256((self.ns['BINANCE_API_KEY'] + '\0' + self.ns['BINANCE_API_SECRET']).encode()).digest()
        with self.binance_lock:
            state = self.binance_health
            if state['credentials'] != credentials:
                state.update(credentials=credentials, failures=0, retry_at=0.)
            if state['retry_at'] > time.monotonic():
                self.counters['binance_failed_cycles_deferred'] += 1
                return None
            result = original()
            if result is None:
                failures = min(4, state['failures'] + 1)
                state.update(failures=failures, retry_at=time.monotonic() + min(120, 15 * 2 ** (failures - 1)))
            else:
                state.update(failures=0, retry_at=0.)
            return result

    def rpc(self, method, params):
        nodes = list(self.ns['BSC_PUBLIC_RPCS'])
        self.ns['random'].shuffle(nodes)
        with self.lock:
            health = {node: dict(self.rpc_health.get((node, method), {})) for node in nodes}
        nodes.sort(key=lambda node: health[node].get('failures', 0))
        payload = {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}
        for node in nodes:
            if health[node].get('retry_at', 0) > time.monotonic():
                self.counters['rpc_failed_nodes_deferred'] += 1
                continue
            response = self.ns['_http_post_json'](node, payload, timeout=8)
            answered = isinstance(response, dict) and 'result' in response and not response.get('error')
            ok = answered and response['result'] is not None
            with self.lock:
                key = (node, method)
                if key not in self.rpc_health and len(self.rpc_health) >= 128:
                    self.rpc_health.pop(next(iter(self.rpc_health)))
                state = self.rpc_health.setdefault(key, {'failures': 0, 'retry_at': 0.})
                failures = min(5, state['failures'] + 1)
                state.update(failures=0 if answered else failures,
                             retry_at=0. if answered else time.monotonic() + min(300, 30 * 2 ** (failures - 1)))
            if ok:
                return response['result']
        return None  # Unavailable remains unknown; never turn it into an empty payment history.

    def http_result(self, handler):
        route = self.ns['_net_server_route'](getattr(handler, 'path', ''))
        status = int(getattr(handler, '_net_status', 0) or 0)
        with self.lock:
            key = (route, status)
            if key not in self.http_statuses and len(self.http_statuses) >= 256:
                key = ('other', status)
            if key in self.http_statuses or len(self.http_statuses) < 280:
                self.http_statuses[key] += 1

    def snapshot(self, original, meter):
        result = original(meter)
        with self.lock:
            result['network_report_fixes'] = {'version': 1, 'counters': dict(self.counters),
                'shared_cache_entries': len(self.entries),
                'shared_cache_bytes': sum(entry['bytes'] for entry in self.entries.values()),
                'maximum_cache_bytes': self.MAX_BYTES, 'catalogue_ttl_seconds': self.TTL,
                'http_statuses': [{'route': route, 'status': status, 'requests': count}
                                  for (route, status), count in self.http_statuses.items()]}
        return result


def install(namespace):
    if namespace.get('_NETWORK_REPORT_FIXES') is not None:
        return namespace['_NETWORK_REPORT_FIXES']
    service = NetworkReportFixes(namespace)

    def wrap(name, method):
        original = namespace[name]
        @functools.wraps(original)
        def wrapped(*args, **kwargs):
            return method(original, *args, **kwargs)
        namespace[name] = wrapped

    wrap('_net_api_products_payload', service.api_payload)
    wrap('_net_products_put', service.products_put)
    wrap('_net_stock_counts', service.stock_counts)
    wrap('_cgpt_total_available_seats', service.seats)
    wrap('get_lang', service.get_lang)
    wrap('_custom_button_for_view', service.button)
    wrap('_binance_pay_request', service.binance)
    original_invalidate = namespace['_cgpt_stock_invalidate']
    @functools.wraps(original_invalidate)
    def invalidate_seats(*args, **kwargs):
        service.invalidate('seat_count')
        namespace['_invalidate_products_cache']()
        return original_invalidate(*args, **kwargs)
    namespace['_cgpt_stock_invalidate'] = invalidate_seats
    namespace['_bsc_rpc_call'] = service.rpc

    classes = [__import__('pymongo.collection', fromlist=['Collection']).Collection]
    mock = namespace['sys'].modules.get('mongomock')
    if mock is not None:
        classes.append(mock.Collection)
    for cls in classes:
        if getattr(cls, '_lara_report_read_hook', False):
            continue
        for method in ('find', 'find_one'):
            original = getattr(cls, method)
            def make_read(original, method):
                @functools.wraps(original)
                def cached_read(collection, *args, **kwargs):
                    return service.read(original, collection, method, args, kwargs)
                return cached_read
            setattr(cls, method, make_read(original, method))
        cls._lara_report_read_hook = True

    original_send = namespace['bot'].send_message
    @functools.wraps(original_send)
    def send(*args, **kwargs):
        return service.send(original_send, *args, **kwargs)
    namespace['bot'].send_message = send
    for name in ('process_new_messages', 'process_new_callback_query', 'process_new_my_chat_member'):
        original = getattr(namespace['bot'], name, None)
        if not callable(original):
            continue
        def make_incoming(original, member):
            @functools.wraps(original)
            def incoming(events):
                return service.incoming(original, events, member)
            return incoming
        setattr(namespace['bot'], name, make_incoming(original, name == 'process_new_my_chat_member'))

    cls = namespace['_NetworkMeter']
    original_snapshot = cls.snapshot
    @functools.wraps(original_snapshot)
    def snapshot(meter):
        return service.snapshot(original_snapshot, meter)
    cls.snapshot = snapshot
    traffic = namespace.get('_TRAFFIC_EFFICIENCY')
    if traffic is not None:
        traffic.SOURCE_FILES = traffic.SOURCE_FILES | {'network_report_fixes.py'}
        traffic.SKIP = traffic.SKIP | {'cached_read', 'read', 'load', 'cached', 'send', 'seats', 'api_payload', 'fanout', 'get_lang', 'button'}
    namespace['_NETWORK_REPORT_FIXES'] = service
    return service
