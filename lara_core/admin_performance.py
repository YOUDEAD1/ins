"""Fast admin navigation and shared, bounded dashboard statistics.

Uses the original profit calculation and authorization. Only menu metadata is
included in an administrator's bootstrap; no reports run during bootstrap.
"""
import copy
import functools
import threading
import time


class AdminPerformance:
    TTL = 30
    PRODUCT_FIELDS = {key: 1 for key in (
        'id', 'name_ar', 'name_en', 'price', 'is_manual', 'is_hidden',
        'product_type', 'custom_emoji_id', 'discount_tiers')}

    def __init__(self, namespace):
        self.ns = namespace
        self.db = namespace['db']
        self.epoch = 0
        self.profit_epoch = 0
        self.cache = None
        self.state_lock = threading.Lock()
        self.compute_lock = threading.Lock()
        self.local = threading.local()

    def invalidate(self, profit=True):
        # This lock never covers database I/O or report computation.
        with self.state_lock:
            self.epoch += 1
            self.cache = None
            if profit:
                self.profit_epoch += 1
                self.ns['_PROFIT_CACHE'].update(v=None, t=0)

    def changed(self, collection, method, args, kwargs):
        """Cheap local invalidation, including writes originating in the bot/API."""
        try:
            if collection.database.name != self.db.name:
                return
            name = collection.name
            always = {'orders', 'ext_orders', 'purchase_rewards'}
            if name in always:
                self.invalidate()
                return
            if name == 'used_transactions':
                self.invalidate(profit=False)
                return
            if name == 'balance_logs':
                document = args[0] if args else kwargs.get('document', {})
                if method == 'insert_one' and isinstance(document, dict) and document.get('type') not in (
                        'ref_reward', 'ref_clawback', 'ref_refund', 'compensation'):
                    return
                self.invalidate()
                return
            broad = not any(word in method for word in ('update', 'replace'))
            if name == 'users':
                # Changing an existing wallet/name cannot change join counts.
                if broad or 'replace' in method or kwargs.get('upsert') is True:
                    self.invalidate(profit=False)
                return
            if name not in ('products', 'ext_products', 'settings'):
                return
            query = args[0] if args else kwargs.get('filter', {})
            if name == 'settings':
                if not isinstance(query, dict) or query.get('key') in (None, 'report_tz'):
                    self.invalidate()
                return
            update = args[1] if len(args) > 1 else kwargs.get('update', {})
            fields = set()
            if isinstance(update, dict):
                for key, value in update.items():
                    if key.startswith('$') and isinstance(value, dict):
                        fields.update(field.split('.', 1)[0] for field in value)
                    else:
                        fields.add(key.split('.', 1)[0])
            if broad or 'replace' in method or not isinstance(update, dict) or fields & {'id', 'cost', 'base_price'}:
                self.invalidate()
        except Exception:
            # Invalidating a read cache must never interfere with a real write.
            self.ns['logger'].debug('Admin statistics cache invalidation deferred')

    def shell(self, uid):
        return {'ok': True, 'stats': {}, 'alerts': {}, 'top': [],
            'protection': self.ns['is_amount_protection_enabled'](),
            'vip_enabled': bool(self.ns['vip_config']()['enabled']),
            'is_owner': int(uid) == self.ns['OWNER_ID'],
            'sections': [{'k': k, 'e': e, 'ar': ar, 'en': en, 'cb': cb}
                         for k, e, ar, en, cb in self.ns['_ADM_SECTIONS']]}

    def refresh(self, body):
        if isinstance(body, dict) and body.get('refresh') is True:
            self.invalidate()

    def products_raw(self):
        stock = self.ns['_adm_stock_map']()
        classes = self.ns['vip_classes']()
        rows = []
        plain, number = self.ns['_wa_plain'], self.ns['_wa_fnum']
        for product in self.db.products.find({}, self.PRODUCT_FIELDS):
            kind = product.get('product_type') or 'standard'
            if kind == 'cgpt_main':
                continue
            raw = product.get('id') if product.get('id') is not None else product.get('_id')
            pid = str(raw)
            rows.append({'id': pid,
                'name': plain(product.get('name_ar') or product.get('name_en') or pid, 120),
                'name_en': plain(product.get('name_en') or product.get('name_ar') or pid, 120),
                'price': round(number(product.get('price')), 4), 'stock': stock.get(pid, 0),
                'manual': bool(product.get('is_manual')), 'hidden': bool(product.get('is_hidden')),
                'type': kind, 'vip': classes.get(self.ns['vip_key'](pid), 'none'),
                'emoji_id': str(product['custom_emoji_id']) if product.get('custom_emoji_id') else None,
                'tiers': len(product.get('discount_tiers') or [])})
        return rows

    def _windows(self, periods):
        oid = self.ns['ObjectId']
        cutoffs = {key: oid.from_datetime(since) for key, _ar, _en, since, _ps in periods}
        out = {key: {'sales': 0., 'orders': 0, 'buyers': 0, 'deposits': 0.,
                     'dep_count': 0, 'new_users': 0} for key in cutoffs}
        if not cutoffs:
            return out
        earliest = min(cutoffs.values())
        order_group = {'_id': None, 'n': {'$sum': 1}, 'u': {'$addToSet': '$user_id'},
            's': {'$sum': {'$ifNull': ['$total_price', {'$ifNull': ['$price', 0]}]}}}
        pipelines = (
            ('orders', [
                {'$group': order_group},
                {'$project': {'n': 1, 's': 1, 'buyers': {'$size': '$u'}}}]),
            ('used_transactions', [{'$group': {'_id': '$method', 'n': {'$sum': 1}, 's': {'$sum': '$amount'}}}]),
            ('users', [{'$count': 'n'}]))
        for collection, tail in pipelines:
            facets = {key: [{'$match': {'_id': {'$gte': cutoff}}}] + tail
                      for key, cutoff in cutoffs.items()}
            try:
                result = next(iter(self.db[collection].aggregate([
                    {'$match': {'_id': {'$gte': earliest}}}, {'$facet': facets}], maxTimeMS=5000)), {})
            except Exception:
                # Preserve the original best-effort report behavior per source.
                self.local.partial = True
                self.ns['logger'].debug('Admin statistics source unavailable: %s', collection)
                continue
            for key in cutoffs:
                for row in result.get(key, []):
                    if collection == 'orders':
                        out[key].update(sales=round(self.ns['_wa_fnum'](row.get('s')), 2),
                                        orders=int(row.get('n') or 0), buyers=int(row.get('buyers') or 0))
                    elif collection == 'users':
                        out[key]['new_users'] = int(row.get('n') or 0)
                    elif 'gift' not in str(row.get('_id') or '').lower():
                        amount = self.ns['_wa_fnum'](row.get('s'))
                        if amount < 1e7:
                            out[key]['deposits'] += amount
                            out[key]['dep_count'] += int(row.get('n') or 0)
        for window in out.values():
            window['deposits'] = round(window['deposits'], 2)
        return out

    def _key(self):
        return self.epoch, id(self.ns['_PROFIT_CACHE'].get('v'))

    def _cached(self):
        with self.state_lock:
            cached = self.cache
            if cached and cached['key'] == self._key() and time.monotonic() < cached['expires']:
                self.local.generated_at = cached['generated_at']
                self.local.partial = False
                return copy.deepcopy(cached['data'])
        return None

    def stats_all(self):
        self.local.partial = False
        hit = self._cached()
        if hit is not None:
            return hit
        with self.compute_lock:
            hit = self._cached()
            if hit is not None:
                return hit
            with self.state_lock:
                revision = self.epoch
                profit_revision = self.profit_epoch
            try:
                periods = self.ns['profit_all']()
            except Exception:
                self.local.partial = True
                periods = [(key, ar, en, since, None)
                           for key, ar, en, since in self.ns['profit_periods']()]
            data = self._windows(periods)
            for key, ar, en, _since, profit in periods:
                data[key].update(label_ar=ar, label_en=en)
                if profit is not None:
                    try:
                        data[key].update(sales=profit['sales'], orders=profit['orders'], cost=profit['cost'],
                            profit=profit['profit'], net=profit['net'], margin=profit['margin'],
                            no_cost=profit['no_cost'], refs=round(profit['refs'] + profit['comp'], 2))
                    except (KeyError, TypeError):
                        self.ns['logger'].debug('Admin profit summary unavailable')
            generated_at = time.time()
            self.local.generated_at = generated_at
            with self.state_lock:
                if profit_revision != self.profit_epoch:
                    self.ns['_PROFIT_CACHE'].update(v=None, t=0)
                if revision == self.epoch and not self.partial():
                    self.cache = {'key': self._key(), 'data': copy.deepcopy(data),
                                  'expires': time.monotonic() + self.TTL, 'generated_at': generated_at}
            return data

    def generated_at(self):
        return getattr(self.local, 'generated_at', time.time())

    def partial(self):
        return bool(getattr(self.local, 'partial', False))


def install(namespace):
    if namespace.get('_ADMIN_PERFORMANCE') is not None:
        return namespace['_ADMIN_PERFORMANCE']
    service = AdminPerformance(namespace)
    for name, method in (('_adm_stats_all', service.stats_all), ('_adm_products_raw', service.products_raw)):
        original = namespace[name]
        namespace[name] = functools.wraps(original)(lambda method=method: method())
    namespace['_ADMIN_PERFORMANCE'] = service
    return service
