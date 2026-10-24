"""Prospective website referrals, isolated from historical/manual batches.

Uses the shop's existing atomic wallet and editable reward notifications. No
network polling, new wallet, balance migration, or historical repricing.
"""
import functools
import hashlib
import secrets
import time

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError


VERSION = 1
MODE = 'website_auto_v1'
THRESHOLD = 10
OPEN_STATES = ('auto_pending', 'auto_paying')


def is_new(record):
    return bool(record and record.get('ref_auto_version') == VERSION)


def website_proof(record):
    stamp = (record or {}).get('web_verified_at')
    return bool(record and record.get('web_verified_version') == 2 and
                isinstance(stamp, (int, float)) and not isinstance(stamp, bool) and
                stamp > 0 and record.get('dv') in ('ok', 'ok_admin', 'flag'))


def eligible(record):
    return bool(is_new(record) and website_proof(record) and
                record.get('dv') in ('ok', 'ok_admin'))


class AutomaticReferralRewards:
    def __init__(self, namespace):
        self.ns = namespace
        self.db = namespace['db']
        self.log = namespace['logger']
        self.original = {}
        self.db.referrals_v2.create_index([
            ('ref_auto_version', 1), ('status', 1), ('auto_reward_pending', 1), ('auto_next_check_at', 1)])
        self.db.referrals_v2.create_index([
            ('referrer_id', 1), ('ref_auto_version', 1), ('ms_batch', 1), ('status', 1)])
        self.db.referral_milestones.create_index([
            ('reward_mode', 1), ('status', 1), ('auto_retry_at', 1)])
        self.db.ref_auto_locks.create_index('expires_at', expireAfterSeconds=0)

    def live_query(self, rid=None, **extra):
        query = {'ref_auto_version': VERSION, 'status': 'active',
                 'web_verified_version': 2, 'web_verified_at': {'$gt': 0},
                 'dv': {'$in': ['ok', 'ok_admin']}}
        if rid is not None:
            query['referrer_id'] = int(rid)
        query.update(extra)
        return query

    def fresh_query(self, rid=None):
        return self.live_query(rid, auto_reward_pending=True, ms_batch={'$exists': False})

    def _operation(self, milestone):
        return 'milestone:%s:%s' % (milestone['referrer_id'], milestone['batch_no'])

    def _event(self, milestone):
        key = hashlib.sha256(self._operation(milestone).encode()).hexdigest()[:32]
        return self.db.ref_money_events.find_one({'_id': key})

    def handles_event(self, event):
        """Automatic intents must resume under the same database lease as payment.

        The historical outbox remains unchanged for all historical operations.
        Without this routing a second process could race the active payer.
        """
        parts = str(event.get('operation_key') or '').split(':')
        if event.get('kind') != 'ref_reward' or len(parts) != 3 or parts[0] != 'milestone':
            return False
        try:
            rid, batch = int(parts[1]), int(parts[2])
        except ValueError:
            return False
        return bool(rid == event.get('user_id') and self.db.referral_milestones.find_one(
            {'referrer_id': rid, 'batch_no': batch, 'reward_mode': MODE}, {'_id': 1}))

    def _now(self):
        return self.ns['datetime'].datetime.now(self.ns['datetime'].timezone.utc).replace(tzinfo=None)

    def _lease(self, rid):
        token = secrets.token_hex(24)
        now = self._now()
        try:
            row = self.db.ref_auto_locks.find_one_and_update(
                {'_id': rid, '$or': [{'expires_at': {'$lte': now}},
                                    {'expires_at': {'$exists': False}}]},
                {'$set': {'token': token, 'expires_at': now + self.ns['datetime'].timedelta(seconds=120)}},
                upsert=True, return_document=ReturnDocument.AFTER)
        except DuplicateKeyError:
            return None
        return token if row and row.get('token') == token else None

    def _renew(self, rid, token):
        now = self._now()
        row = self.db.ref_auto_locks.find_one_and_update(
            {'_id': rid, 'token': token, 'expires_at': {'$gt': now}},
            {'$set': {'expires_at': now + self.ns['datetime'].timedelta(seconds=120)}},
            return_document=ReturnDocument.AFTER)
        if not row:
            raise RuntimeError('Automatic referral lease expired; saved batch will be retried')

    def _reserve(self, milestone, token):
        rid, batch = milestone['referrer_id'], milestone['batch_no']
        self._renew(rid, token)
        # A saved payment intent has immutable members and an immutable price.
        if milestone['status'] != 'auto_pending':
            return milestone
        rows = list(self.db.referrals_v2.find(
            {'referrer_id': rid, 'ref_auto_version': VERSION, 'ms_batch': batch}))
        for row in rows:
            if row.get('status') != 'active' or not eligible(row):
                self.db.referrals_v2.update_one({'_id': row['_id'], 'ms_batch': batch},
                    {'$unset': {'ms_batch': ''}, '$set': {'auto_reward_pending': True, 'auto_next_check_at': 0}})
        # Recover an insertion interrupted before any/all member assignments.
        wanted = [int(x) for x in milestone.get('members', [])]
        if wanted:
            self.db.referrals_v2.update_many(self.live_query(rid,
                invited_id={'$in': wanted}, ms_batch={'$exists': False}),
                {'$set': {'ms_batch': batch, 'auto_reward_pending': False}})
        pinned = list(self.db.referrals_v2.find(self.live_query(rid, ms_batch=batch)).sort('_id', 1))
        needed = THRESHOLD - len(pinned)
        if needed > 0:
            candidates = list(self.db.referrals_v2.find(self.fresh_query(rid)).sort('_id', 1).limit(needed))
            ids = [int(row['invited_id']) for row in candidates]
            if ids:
                self.db.referrals_v2.update_many(self.live_query(rid,
                    invited_id={'$in': ids}, ms_batch={'$exists': False}),
                    {'$set': {'ms_batch': batch, 'auto_reward_pending': False}})
        pinned = list(self.db.referrals_v2.find(self.live_query(rid, ms_batch=batch)).sort('_id', 1))
        ids = [int(row['invited_id']) for row in pinned]
        self._renew(rid, token)
        self.db.referral_milestones.update_one({'_id': milestone['_id'], 'status': 'auto_pending'},
            {'$set': {'members': ids, 'auto_retry_at': int(time.time()) + 60}})
        milestone['members'] = ids
        return milestone

    def _reconcile(self, milestone):
        ok = True
        for row in self.db.referrals_v2.find({'referrer_id': milestone['referrer_id'],
                'ref_auto_version': VERSION, 'ms_batch': milestone['batch_no'],
                'invited_id': {'$in': milestone.get('members', [])}}):
            if row.get('status') != 'active' or not eligible(row):
                self.ns['_ref_clawback'](row)
                latest = self.db.referrals_v2.find_one({'_id': row['_id']}) or {}
                ok = ok and latest.get('cb_status') == 'applied'
        if ok:
            self.db.referral_milestones.update_one({'_id': milestone['_id'], 'status': 'paid'},
                {'$set': {'auto_reconcile_pending': False}})

    def _notify(self, milestone):
        now = int(time.time())
        claim = self.db.referral_milestones.find_one_and_update(
            {'_id': milestone['_id'], 'status': 'paid', 'auto_notify_pending': True,
             '$or': [{'auto_notify_claim_until': {'$exists': False}},
                     {'auto_notify_claim_until': {'$lte': now}}]},
            {'$set': {'auto_notify_claim_until': now + 120}}, return_document=ReturnDocument.AFTER)
        if not claim:
            return
        try:
            event = self._event(claim) or {}
            receipt = event.get('receipt')
            if not receipt:
                raise RuntimeError('Automatic reward receipt not yet available')
            rid = claim['referrer_id']
            msg = self.ns['_notification_text'](rid, 'ref_milestone_dm', THRESHOLD,
                    f"{claim['reward']:.2f}", f"{receipt['balance_after']:.2f}")
            if receipt.get('debt_delta', 0) < 0:
                msg += self.ns['_notification_text'](rid, 'ref_settled_dm',
                    -receipt['debt_delta'], receipt['balance_delta'])
            self.ns['bot'].send_message(rid, msg, parse_mode='HTML')
        except Exception:
            # Keep a bounded retry in the existing minute loop; never re-credit.
            self.log.debug('Automatic referral reward notification deferred')
            return
        self.db.referral_milestones.update_one({'_id': claim['_id'], 'auto_notify_pending': True},
            {'$set': {'auto_notify_pending': False, 'auto_notified_at': now},
             '$unset': {'auto_notify_claim_until': ''}})

    def _pay(self, milestone, token):
        rid, batch = milestone['referrer_id'], milestone['batch_no']
        self._renew(rid, token)
        if milestone['status'] == 'auto_pending':
            owner = self.db.users.find_one({'user_id': rid}) or {}
            if not owner or owner.get('is_banned') == 1:
                return False
            ids = milestone.get('members', [])
            if len(set(ids)) != THRESHOLD or self.db.referrals_v2.count_documents(
                    self.live_query(rid, invited_id={'$in': ids}, ms_batch=batch)) != THRESHOLD:
                return False
            changed = self.db.referral_milestones.find_one_and_update(
                {'_id': milestone['_id'], 'status': 'auto_pending', 'members': ids},
                {'$set': {'status': 'auto_paying', 'paid_by': 'automatic_website',
                          'paid_at': int(time.time()), 'auto_retry_at': int(time.time()) + 60}},
                return_document=ReturnDocument.AFTER)
            if not changed:
                return False
            milestone = changed
        operation = self._operation(milestone)
        note = f"مكافأة إحالات متحققة تلقائيًا · الدفعة #{batch} ({THRESHOLD} إحالة)"
        key = hashlib.sha256(operation.encode()).hexdigest()[:32]
        # Mark only new automatic events for the wallet's stale-reader fence.
        self.db.ref_money_events.update_one({'_id': key}, {'$setOnInsert': {
            'user_id': rid, 'amount': self.ns['_ref_units'](milestone['reward']) / 10000,
            'kind': 'ref_reward', 'note': note[:500], 'operation_key': operation,
            'status': 'prepared', 'created_at': int(time.time()), 'reward_mode': MODE}}, upsert=True)
        credited = self.ns['_ref_money'](rid, milestone['reward'], 'ref_reward', note, event_key=operation)
        if not credited:
            return False
        self._renew(rid, token)
        self.db.referral_milestones.update_one({'_id': milestone['_id'], 'status': 'auto_paying'},
            {'$set': {'status': 'paid', 'paid_amount': milestone['reward'],
                      'money_event': credited['_ref_event_id'], 'auto_notify_pending': True,
                      'auto_reconcile_pending': True}})
        milestone = self.db.referral_milestones.find_one({'_id': milestone['_id']})
        self._reconcile(milestone)
        self._notify(milestone)
        return True

    def process(self, rid, max_batches=20):
        rid = int(rid)
        paid = 0
        with self.ns['_get_referrer_lock'](rid):
            pending = {'referrer_id': rid, 'reward_mode': MODE, 'status': {'$in': list(OPEN_STATES)}}
            if not self.db.referral_milestones.find_one(pending) and not self.db.referrals_v2.find_one(self.fresh_query(rid)):
                return 0
            token = self._lease(rid)
            if not token:
                return 0
            try:
                for _ in range(max(1, min(20, int(max_batches)))):
                    milestone = self.db.referral_milestones.find_one(pending, sort=[('batch_no', 1)])
                    if not milestone:
                        fresh = list(self.db.referrals_v2.find(self.fresh_query(rid)).sort('_id', 1).limit(THRESHOLD))
                        if len(fresh) < THRESHOLD:
                            break
                        self._renew(rid, token)
                        reward = float(self.ns['get_referral_reward']())
                        if self.ns['_ref_units'](reward) <= 0:
                            raise ValueError('Automatic referral reward must be positive and finite')
                        last = self.db.referral_milestones.find_one({'referrer_id': rid}, sort=[('batch_no', -1)])
                        milestone = {'referrer_id': rid, 'batch_no': int((last or {}).get('batch_no') or 0) + 1,
                            'reward_mode': MODE, 'status': 'auto_pending', 'threshold': THRESHOLD,
                            'reward': reward, 'members': [int(row['invited_id']) for row in fresh],
                            'created_at': int(time.time()), 'auto_retry_at': int(time.time()) + 60}
                        try:
                            milestone['_id'] = self.db.referral_milestones.insert_one(milestone).inserted_id
                        except DuplicateKeyError:
                            continue
                    milestone = self._reserve(milestone, token)
                    if not self._pay(milestone, token):
                        break
                    paid += 1
                # The tenth referral always bypasses this wait through process().
                self.db.referrals_v2.update_many(self.fresh_query(rid),
                    {'$set': {'auto_next_check_at': int(time.time()) + 300}})
            except Exception:
                self.log.debug('Automatic referral batch saved for retry', exc_info=True)
            finally:
                self.db.ref_auto_locks.delete_one({'_id': rid, 'token': token})
        return paid

    def tick(self, limit=25):
        limit = max(1, min(100, int(limit)))
        now = int(time.time())
        due = {'$or': [{'auto_retry_at': {'$exists': False}}, {'auto_retry_at': {'$lte': now}}]}
        rows = list(self.db.referral_milestones.find(dict(due, reward_mode=MODE,
            status={'$in': list(OPEN_STATES)})).sort('auto_retry_at', 1).limit(limit))
        owners = {int(row['referrer_id']) for row in rows}
        query = self.fresh_query()
        query['$or'] = [{'auto_next_check_at': {'$exists': False}}, {'auto_next_check_at': {'$lte': now}}]
        owners.update(int(row['referrer_id']) for row in self.db.referrals_v2.find(query,
            {'referrer_id': 1}).sort('auto_next_check_at', 1).limit(limit * THRESHOLD))
        for rid in sorted(owners)[:limit]:
            self.process(rid)
        # Reconciliation and message retries use the existing loop, no new job.
        for row in self.db.referral_milestones.find({'reward_mode': MODE, 'status': 'paid',
                '$or': [{'auto_notify_pending': True}, {'auto_reconcile_pending': True}]}).limit(limit):
            if row.get('auto_reconcile_pending'):
                self._reconcile(row)
            if row.get('auto_notify_pending'):
                self._notify(row)

    def install(self):
        names = ('_ref_web_verified', '_ref_policy_requires_web', '_ref_countable', '_ref_verified_query',
                 '_ms_fresh_q', '_notify_admin_referral_milestone', 'ref_progress', '_ref_resume_financial')
        self.original = {name: self.ns[name] for name in names}

        def web_verified(record):
            return website_proof(record) if is_new(record) else self.original['_ref_web_verified'](record)

        def requires_web(record):
            return is_new(record) or self.original['_ref_policy_requires_web'](record)

        def countable(record):
            return eligible(record) if is_new(record) else self.original['_ref_countable'](record)

        def verified_query():
            return {'$and': [self.original['_ref_verified_query'](), {'$or': [
                {'ref_auto_version': {'$ne': VERSION}},
                {'ref_auto_version': VERSION, 'web_verified_version': 2, 'web_verified_at': {'$gt': 0},
                 'dv': {'$in': ['ok', 'ok_admin']}}]}]}

        def legacy_fresh(rid):
            return dict(self.original['_ms_fresh_q'](rid), ref_auto_version={'$ne': VERSION})

        def notify(rid):
            self.process(rid)
            return self.original['_notify_admin_referral_milestone'](rid)

        def progress(rid):
            if self.db.referrals_v2.find_one({'referrer_id': int(rid), 'ref_auto_version': VERSION}, {'_id': 1}):
                fresh = self.db.referrals_v2.count_documents(self.fresh_query(rid))
                return fresh, THRESHOLD, max(0, THRESHOLD - fresh)
            return self.original['ref_progress'](rid)

        def resume():
            result = self.original['_ref_resume_financial']()
            self.tick()
            return result

        replacements = (web_verified, requires_web, countable, verified_query, legacy_fresh, notify, progress, resume)
        for name, replacement in zip(names, replacements):
            self.ns[name] = functools.wraps(self.original[name])(replacement)
        return self


def install(namespace):
    existing = namespace.get('_REF_AUTO_REWARDS')
    if existing is not None:
        return existing
    service = AutomaticReferralRewards(namespace).install()
    namespace['_REF_AUTO_REWARDS'] = service
    return service
