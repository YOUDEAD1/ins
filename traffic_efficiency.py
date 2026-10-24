"""Bounded request diagnostics and safe reductions to repeated read-only work.

Never caches an unpaid transaction or changes wallet/deposit matching rules.
Only the existing background poller uses the wakeup floor.
"""
import collections
import functools
import os
import re
import sys
import time


class TrafficEfficiency:
    BUCKETS = 96
    ORIGINS = 24
    SOURCE_FILES = frozenset(('bot.py', 'admin_performance.py', 'referral_auto_rewards.py',
                              'channel_broadcast_upgrade.py', 'translation_repair.py',
                              'wwc_platform.py', 'wwc_manager_bridge.py', 'traffic_efficiency.py'))
    SKIP = frozenset(('started', 'succeeded', 'failed', 'record', 'measured_send', 'measured_curl',
        'observed', 'snapshot', 'trace_origin', 'write', '_w', 'wrapper', 'wrapped', 'guarded',
        '__init__', '_vc_send_message', '_vc_edit_message_text', '_broadcast_deliver', 'request'))

    def __init__(self, namespace):
        self.ns = namespace
        self.db = namespace['db']
        self.counters = collections.Counter()

    def known_used(self, rows, provider):
        if not isinstance(rows, (list, tuple)):
            return frozenset()  # Never consume a streaming provider response before its original matcher.
        ids = set()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            try:
                if provider == 'binance':
                    order = str(row.get('orderId') or '').strip()
                    raw = order if order.isdigit() else str(row.get('transactionId') or row.get('bizOrderNo') or '').strip()
                else:
                    raw = self.ns['_bybit_row_txid'](row)
                if raw:
                    ids.add(self.ns['normalize_tx_id'](raw))
            except Exception:
                continue
            if len(ids) >= 512:
                break
        if not ids:
            return frozenset()
        try:
            # Providers already bound batches to 100/200 rows. Only positive DB
            # evidence is used, and only for this one provider response.
            used = frozenset(str(row['transaction_id']) for row in self.db.used_transactions.find(
                {'transaction_id': {'$in': list(ids)}}, {'transaction_id': 1, '_id': 0}))
            self.counters['bulk_used_reads'] += 1
            self.counters['known_paid_rows'] += len(used)
            return used
        except Exception:
            self.ns['logger'].debug('Bulk transaction read unavailable; original checks remain active')
            return frozenset()

    def bybit_kinds(self):
        try:
            coins = set(self.db.pending_deposits.distinct('coin', {'status': 'pending',
                'coin': {'$in': self.ns['BYBIT_COINS'] + ['USDT', 'USDT_BEP20']},
                'expires_at': {'$gt': int(time.time())}}))
        except Exception:
            return frozenset(('internal', 'onchain'))
        kinds = set()
        if 'BYBIT_UID' in coins:
            kinds.add('internal')
        if coins - {'BYBIT_UID'}:
            kinds.add('onchain')
        return frozenset(kinds)

    def wait(self, check_func, interval):
        event = self.ns['_NET_PAY_EVENTS'].get(getattr(check_func, '__name__', ''))
        if event is None:
            self.ns['time'].sleep(interval)
            return
        started = time.monotonic()
        delay = self.ns['_net_payment_interval'](interval)
        woke = event.wait(delay)
        event.clear()
        if woke:
            remaining = max(0., interval - (time.monotonic() - started))
            if remaining:
                self.counters['early_wakes_coalesced'] += 1
                self.ns['time'].sleep(remaining)

    def trace_origin(self):
        names = []
        frame = sys._getframe(1)
        for _ in range(80):
            if frame is None:
                break
            name = frame.f_code.co_name
            if (os.path.basename(frame.f_code.co_filename) in self.SOURCE_FILES and name not in self.SKIP
                    and re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', name) and name not in names):
                names.append(name)
                if len(names) == 4:
                    break
            frame = frame.f_back
        return ' <- '.join(names) or 'background'

    def observe(self, meter, group, label, sent, received, count, errors, origin):
        if not origin:
            return
        group, label, origin = str(group)[:24], str(label)[:90], str(origin)[:200]
        key = (group, label)
        with meter.lock:
            buckets = getattr(meter, '_traffic_origins', None)
            if buckets is None:
                buckets = meter._traffic_origins = {}
            if key not in buckets:
                if len(buckets) >= self.BUCKETS:
                    key = (group, 'other')
                if key not in buckets and len(buckets) >= self.BUCKETS + 4:
                    return
                buckets.setdefault(key, {})
            bucket = buckets[key]
            if origin not in bucket:
                # Adaptive heavy hitters: frequent late-arriving sources can
                # enter even after an early burst filled a diagnostic bucket.
                estimate = 0
                if len(bucket) >= self.ORIGINS:
                    victim = min(bucket, key=lambda name: bucket[name]['estimated_requests'])
                    estimate = bucket.pop(victim)['estimated_requests']
                bucket[origin] = {'requests': 0, 'estimated_requests': estimate,
                    'admission_error_bound': estimate, 'sent': 0, 'received': 0, 'errors': 0}
            row = bucket[origin]
            for field, value in (('requests', count), ('estimated_requests', count),
                                 ('sent', sent), ('received', received), ('errors', errors)):
                row[field] += max(0, int(value or 0))

    def hotspots(self, meter):
        with meter.lock:
            return [dict(group=group, label=label, origin=origin, **row)
                    for (group, label), bucket in getattr(meter, '_traffic_origins', {}).items()
                    for origin, row in bucket.items()]

    def snapshot(self, meter, original):
        result = original(meter)
        result['traffic_efficiency_version'] = 1
        result['hotspots'] = self.hotspots(meter)
        result['hotspot_limits'] = {'maximum_method_buckets': self.BUCKETS + 4,
            'origins_per_method': self.ORIGINS,
            'counts': 'requests/bytes are observed since admission; estimates/error bounds identify adaptive replacement; main totals remain exact'}
        result['optimizations'] = dict(self.counters)
        result['http_server_inbound_measured'] = False
        return result

    def show_network(self, call):
        if call.id:
            self.ns['bot'].answer_callback_query(call.id)
        data = self.ns['_NET_METER'].snapshot()
        size = self.ns['_net_size_text']
        esc = self.ns['html'].escape
        titles = [('http_server', 'ردود الموقع وAPI'), ('http_client', 'HTTP الصادر'),
                  ('curl_client', 'اتصالات ChatGPT'), ('mongo', 'MongoDB — قبل ضغط الاتصال')]
        lines = ['📡 <b>استهلاك الشبكة — قياس المصادر المحسّن</b>',
                 f'الفترة: {data["seconds"] / 3600:.2f} ساعة | نسخة <code>{data["instance"]}</code>', '']
        for group, title in titles:
            rows = [row for row in data['rows'] if row['group'] == group]
            incoming = 'غير مقاس' if group == 'http_server' else size(sum(row['received'] for row in rows))
            lines += [f'<b>{title}</b>: {sum(row["requests"] for row in rows):,} محاولة',
                f'إرسال {size(sum(row["sent"] for row in rows))} | استقبال {incoming}']
            for row in sorted(rows, key=lambda row: row['sent'], reverse=True)[:4]:
                lines.append(f'• <code>{esc(row["label"])}</code>: {row["requests"]:,}، '
                             f'فشل {row["errors"]:,}، إرسال {size(row["sent"])}')
        for group, title in [('http_client', 'مصادر HTTP المرصودة'), ('mongo', 'مصادر MongoDB المرصودة'),
                             ('http_server', 'سحب المنتجات حسب حساب API')]:
            hot = [row for row in data.get('hotspots', []) if row['group'] == group]
            lines += ['', '<b>' + title + '</b>']
            for row in sorted(hot, key=lambda row: row['requests'], reverse=True)[:2]:
                prefix = '≥' if row['admission_error_bound'] else ''
                lines.append(f'• <code>{esc(row["label"])} ← {esc(row["origin"][:95])}</code>: '
                             f'{prefix}{row["requests"]:,}، فشل {row["errors"]:,}')
        lines += ['', 'محاولات Telegram تشمل الناجح والفاشل؛ ليست كلها رسائل مستلمة.',
            'استقبال MongoDB منفصل عن الإرسال. أحجامه قبل الضغط ولا تُجمع بوصفها فاتورة Render.',
            'المجاميع دقيقة ضمن نطاق القياس؛ المصادر ذات ≥ مرصودة منذ دخولها قائمة التتبّع المحدودة.',
            'الرقم المفوتر: Render ← Metrics ← Outbound Bandwidth ثم Billing.']
        markup = self.ns['InlineKeyboardMarkup']()
        button = self.ns['InlineKeyboardButton']
        markup.add(button('🔄 تحديث القياس', callback_data='ad_network'))
        markup.add(button('📄 تقرير تفصيلي JSON', callback_data='ad_network_export'))
        markup.add(button('🔙 لوحة الإدارة', callback_data='admin_panel_main'))
        # Bound the visible Telegram message; full hotspots stay in the export.
        text = '\n'.join(lines)
        while len(text) > 3900:
            indices = [i for i, line in enumerate(lines) if line.startswith('•')]
            if not indices:
                break
            lines.pop(indices[-1])
            text = '\n'.join(lines)
        self.ns['bot'].send_message(call.message.chat.id, text, parse_mode='HTML', reply_markup=markup)


def install(namespace):
    if namespace.get('_TRAFFIC_EFFICIENCY') is not None:
        return namespace['_TRAFFIC_EFFICIENCY']
    service = TrafficEfficiency(namespace)
    cls = namespace['_NetworkMeter']
    original_record, original_snapshot = cls.record, cls.snapshot

    @functools.wraps(original_record)
    def observed(meter, group, label, sent=0, received=0, count=1, errors=0, detail=None):
        original_record(meter, group, label, sent, received, count, errors, detail)
        try:
            service.observe(meter, group, label, sent, received, count, errors, detail)
        except Exception:
            pass

    @functools.wraps(original_snapshot)
    def snapshot(meter):
        return service.snapshot(meter, original_snapshot)

    cls.record, cls.snapshot = observed, snapshot
    namespace['_net_trace_origin'] = service.trace_origin
    namespace['_net_wait'] = service.wait
    namespace['_TRAFFIC_EFFICIENCY'] = service
    return service
