"""New-referral website gates and real wallet/HTTP regressions; offline only."""
import copy
import datetime as dt
import threading
import types
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from harness import MESSAGES
from test_integration import mod, isolate, request, server
from test_referral_integrity import challenge, referral, verify_payload
from test_integration import _ref_fp


@pytest.fixture(autouse=True)
def joined(mod, monkeypatch):
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: True)


def fresh(mod, uid=6001, proof=True, status='pending', **extra):
    mod.db.users.update_one({'user_id': uid}, {'$setOnInsert': {
        'user_id': uid, 'name': 'Invitee', 'balance': 0., 'lang': 'en',
        'lang_chosen': True, 'is_banned': 0}}, upsert=True)
    assert mod.register_new_referral(uid, 1001)
    changes = dict(extra, status=status)
    if proof:
        changes.update(dv='ok', web_verified_version=2, web_verified_at=100, dv_at=100)
    mod.db.referrals_v2.update_one({'invited_id': uid}, {'$set': changes})
    return mod._ref_find(uid)


def ready(mod, count=10, start=6001, activate=False):
    for uid in range(start, start + count):
        fresh(mod, uid, status='pending' if activate else 'active')
        if activate:
            mod.mark_referral_status(uid, 'active')


def balance(mod):
    return mod.db.users.find_one({'user_id': 1001})['balance']


def auto_batches(mod):
    return list(mod.db.referral_milestones.find({'reward_mode': 'website_auto_v1'}).sort('batch_no', 1))


def retry(mod):
    mod.db.referral_milestones.update_many({'reward_mode': 'website_auto_v1'}, {'$set': {'auto_retry_at': 0}})
    mod._ref_resume_financial()


def test_registration_labels_only_new_rows_and_does_not_relabel_a_duplicate(mod):
    old = referral(mod, invited=5001, proof=False, status='pending')
    assert not mod.register_new_referral(5001, 1002)
    assert mod._ref_find(5001) == old
    row = fresh(mod, proof=False)
    assert row['ref_auto_version'] == 1 and row['ref_policy_version'] == 30
    assert not mod.register_new_referral(6001, 1002)
    assert mod._ref_find(6001)['referrer_id'] == 1001


@pytest.mark.parametrize('changes', [
    {}, {'dv': 'ok_admin'}, {'dv': 'ok', 'dv_at': 100},
    {'dv': 'ok', 'web_verified_version': 1, 'web_verified_at': 100},
    {'dv': 'ok', 'web_verified_version': 2, 'web_verified_at': 0},
    {'dv': 'rejected', 'web_verified_version': 2, 'web_verified_at': 100}])
def test_joining_or_an_admin_verdict_without_server_website_proof_never_activates(mod, changes):
    fresh(mod, proof=False, **changes)
    mod.mark_referral_status(6001, 'active')
    mod.update_referrer_balance(1001)
    assert mod._ref_find(6001)['status'] == 'pending'
    assert mod.get_ref_counts(1001)[:2] == (1, 0)
    assert balance(mod) == 100 and not auto_batches(mod)


def test_subscription_button_and_start_cannot_replace_the_site_step(mod, server):
    fresh(mod, proof=False)
    assert request(server, '/webapp/forced/check', uid=6001)[0] == 200
    msg = mod.types.Message.de_json({'message_id': 1, 'date': 1,
        'from': {'id': 6001, 'is_bot': False, 'first_name': 'Invitee'},
        'chat': {'id': 6001, 'type': 'private'}, 'text': '/start'})
    mod.start_handler(msg)
    assert mod._ref_find(6001)['status'] == 'pending'
    assert mod.ref_dv_pending_for(6001)
    assert any(kw.get('reply_markup') for cid, _, kw in MESSAGES if cid == 6001)
    assert balance(mod) == 100


def test_ninth_active_referral_waits_and_the_real_tenth_website_verification_pays(mod, server):
    ready(mod, 9, activate=True)
    fresh(mod, 6010, proof=False)
    assert mod.ref_progress(1001) == (9, 10, 1)
    assert balance(mod) == 100 and not auto_batches(mod)
    issued = challenge(mod, server, uid=6010)
    status, result, _ = request(server, '/webapp/verify', verify_payload(issued), uid=6010)
    assert status == 200 and result['ok']
    assert mod._ref_find(6010)['status'] == 'active'
    assert balance(mod) == 100.1
    row = auto_batches(mod)[0]
    assert row['status'] == 'paid' and row['threshold'] == 10 and row['paid_amount'] == .1
    assert len(set(row['members'])) == 10
    assert mod.db.balance_logs.count_documents({'type': 'ref_reward'}) == 1
    assert mod.ref_progress(1001) == (0, 10, 10)
    assert request(server, '/webapp/verify', verify_payload(issued), uid=6010)[0] == 200
    mod.update_referrer_balance(1001); retry(mod)
    assert balance(mod) == 100.1 and len(auto_batches(mod)) == 1
    assert sum('Referral reward!' in txt for cid, txt, _ in MESSAGES if cid == 1001) == 1
    response = request(server, '/webapp/referrals')[1]
    assert response['threshold'] == 10 and response['progress'] == 0
    assert response['active'] == 10 and response['balance'] == 100.1


@pytest.mark.parametrize('subscribed,expected', [(False, 200), (None, 503)])
def test_failed_membership_is_not_success_and_does_not_consume_a_valid_challenge(mod, server, monkeypatch, subscribed, expected):
    fresh(mod, proof=False)
    issued = challenge(mod, server, uid=6001)
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: subscribed)
    status, result, _ = request(server, '/webapp/verify', verify_payload(issued), uid=6001)
    assert status == expected and not result['ok']
    assert not mod._ref_web_verified(mod._ref_find(6001))
    assert not mod.db.ref_verification_challenges.find_one({'_id': issued['challenge']})['used']
    assert balance(mod) == 100


@pytest.mark.parametrize('change', [
    {'challenge': ''}, {'ch': {'ok': False}},
    {'ch': {'ok': True, 'trusted': False, 'ms': 1000}},
    {'ch': {'ok': True, 'trusted': True, 'ms': 10}},
    {'ch': {'ok': True, 'trusted': True, 'ms': 1000, 'answer': 'wrong'}}])
def test_failed_human_check_cannot_activate_or_pay_new_referrals(mod, server, change):
    fresh(mod, proof=False)
    issued = challenge(mod, server, uid=6001)
    assert request(server, '/webapp/verify', verify_payload(issued, **change), uid=6001)[0] == 400
    assert mod._ref_find(6001)['status'] == 'pending' and not auto_batches(mod)


def test_existing_referral_threshold_cannot_reprice_or_shorten_new_ten_person_batches(mod):
    mod.db.settings.insert_one({'key': 'referral_threshold', 'value': 3})
    ready(mod, 9, activate=True)
    assert balance(mod) == 100 and not auto_batches(mod)
    fresh(mod, 6010)
    mod.mark_referral_status(6010, 'active')
    assert balance(mod) == 100.1 and auto_batches(mod)[0]['threshold'] == 10


def test_twenty_new_referrals_get_two_disjoint_automatic_rewards(mod):
    ready(mod, 20, activate=True)
    assert balance(mod) == 100.2
    batches = auto_batches(mod)
    assert len(batches) == 2 and all(row['status'] == 'paid' for row in batches)
    assert not set(batches[0]['members']) & set(batches[1]['members'])
    assert mod.db.balance_logs.count_documents({'type': 'ref_reward'}) == 2
    assert mod.db.referrals_v2.count_documents({'ms_batch': {'$exists': True}}) == 20


def test_legacy_and_new_referrals_are_not_mixed_into_an_automatic_reward(mod):
    for uid in range(5001, 5010):
        referral(mod, invited=uid)
    before = list(mod.db.referrals_v2.find({}))
    ready(mod, 1, activate=True)
    assert balance(mod) == 100 and not auto_batches(mod)
    assert before == list(mod.db.referrals_v2.find({'invited_id': {'$lt': 6000}}))
    assert mod.ref_progress(1001) == (1, 10, 9)


def test_ten_legacy_referrals_keep_the_existing_manual_reward_workflow(mod):
    for uid in range(5001, 5011):
        referral(mod, invited=uid)
    mod.update_referrer_balance(1001)
    row = mod.db.referral_milestones.find_one({})
    assert row['status'] == 'pending' and 'reward_mode' not in row
    retry(mod)
    assert balance(mod) == 100 and not auto_batches(mod)


def test_existing_pending_milestone_and_legacy_wallet_are_not_rewritten(mod):
    legacy = {'referrer_id': 1001, 'batch_no': 1, 'status': 'pending',
              'threshold': 5, 'reward': 7., 'members': [5001, 5002, 5003, 5004, 5005]}
    legacy['_id'] = mod.db.referral_milestones.insert_one(legacy).inserted_id
    ready(mod, 10, activate=True)
    assert mod.db.referral_milestones.find_one({'_id': legacy['_id']}) == legacy
    assert auto_batches(mod)[0]['batch_no'] == 2 and balance(mod) == 100.1


def test_device_flags_wait_for_existing_admin_review_then_count_and_pay_automatically(mod):
    ready(mod, 9, activate=True)
    row = fresh(mod, 6010)
    mod.db.referrals_v2.update_one({'_id': row['_id']}, {'$set': {'dv': 'flag'}})
    mod.mark_referral_status(6010, 'active')
    assert mod._ref_web_verified(mod._ref_find(6010))
    assert not mod.ref_dv_pending_for(6010)
    assert mod.get_ref_counts(1001)[:2] == (1, 9) and balance(mod) == 100
    assert mod.ref_progress(1001) == (9, 10, 1)
    mod._refdv_apply(6010, 'ok', 9001)
    assert mod._ref_find(6010)['status'] == 'active' and balance(mod) == 100.1


def test_admin_cannot_bypass_the_website_even_when_the_device_setting_is_off(mod):
    row = fresh(mod, proof=False, dv_at=100, dv='ok_admin')
    mod.db.settings.insert_one({'key': 'ref_dv', 'value': 'off'})
    assert 'التحقق' in mod._refdv_apply(row['invited_id'], 'ok', 9001)
    assert mod._ref_find(6001)['status'] == 'pending' and balance(mod) == 100


def test_suspicious_real_website_verification_waits_for_review_before_the_tenth_reward(mod, server):
    ready(mod, 9, activate=True)
    fresh(mod, 6010, proof=False)
    issued = challenge(mod, server, uid=6010)
    payload = verify_payload(issued, fp=_ref_fp('a' * 32, wd=True))
    status, result, _ = request(server, '/webapp/verify', payload, uid=6010)
    assert status == 200 and result['state'] == 'done'
    row = mod._ref_find(6010)
    assert row['dv'] == 'flag' and row['status'] == 'pending'
    assert mod._ref_web_verified(row) and balance(mod) == 100
    mod._refdv_apply(6010, 'ok', 9001)
    assert mod._ref_find(6010)['status'] == 'active' and balance(mod) == 100.1


def test_consumed_or_expired_website_challenge_does_not_activate_another_new_referral(mod, server):
    fresh(mod, proof=False)
    issued = challenge(mod, server, uid=6001)
    mod.db.ref_verification_challenges.update_one({'_id': issued['challenge']}, {'$set': {
        'expires_at': dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - dt.timedelta(seconds=1)}})
    assert request(server, '/webapp/verify', verify_payload(issued), uid=6001)[0] == 400
    assert mod._ref_find(6001)['status'] == 'pending' and balance(mod) == 100


def test_parallel_activation_credits_one_batch_and_one_ledger_entry(mod):
    ready(mod, 10)
    mod.db.referrals_v2.update_many({'ref_auto_version': 1}, {'$set': {'status': 'pending'}})
    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(lambda uid: mod.mark_referral_status(uid, 'active'), range(6001, 6011)))
    assert balance(mod) == 100.1 and len(auto_batches(mod)) == 1
    assert mod.db.ref_money_events.count_documents({'kind': 'ref_reward'}) == 1
    assert mod.db.balance_logs.count_documents({'type': 'ref_reward'}) == 1


def test_independent_workers_share_a_database_lease_and_never_share_members(mod):
    ready(mod, 20)
    services = []
    for _ in range(4):
        scope = dict(mod.__dict__)
        locks = {}
        scope['_get_referrer_lock'] = lambda rid, locks=locks: locks.setdefault(rid, threading.RLock())
        services.append(mod._ref_auto_module.AutomaticReferralRewards(scope))
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda service: service.process(1001), services))
    assert balance(mod) == 100.2
    assert len(auto_batches(mod)) == 2
    assert len({uid for row in auto_batches(mod) for uid in row['members']}) == 20


def test_an_existing_live_lease_waits_and_an_expired_lease_is_recovered(mod):
    ready(mod)
    mod.db.ref_auto_locks.insert_one({'_id': 1001, 'token': 'other-instance',
        'expires_at': dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) + dt.timedelta(minutes=2)})
    mod.update_referrer_balance(1001)
    assert balance(mod) == 100 and not auto_batches(mod)
    mod.db.ref_auto_locks.update_one({'_id': 1001}, {'$set': {
        'expires_at': dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - dt.timedelta(seconds=1)}})
    mod.update_referrer_balance(1001)
    assert balance(mod) == 100.1


def test_interruption_after_batch_insert_recovers_without_a_second_batch(mod, monkeypatch):
    ready(mod)
    original = mod._REF_AUTO_REWARDS._reserve
    monkeypatch.setattr(mod._REF_AUTO_REWARDS, '_reserve', Mock(side_effect=RuntimeError('Simulated interruption')))
    mod.update_referrer_balance(1001)
    assert balance(mod) == 100 and auto_batches(mod)[0]['status'] == 'auto_pending'
    monkeypatch.setattr(mod._REF_AUTO_REWARDS, '_reserve', original)
    retry(mod); retry(mod)
    assert balance(mod) == 100.1 and len(auto_batches(mod)) == 1


def test_partial_member_reservation_recovers_and_preserves_the_saved_reward_price(mod, monkeypatch):
    ready(mod)
    original = mod.db.referrals_v2.update_many
    interrupted = []
    def reserve_some(query, update, *args, **kwargs):
        ids = query.get('invited_id', {}).get('$in', [])
        if update.get('$set', {}).get('ms_batch') and not interrupted:
            interrupted.append(True)
            original(dict(query, invited_id={'$in': ids[:4]}), update, *args, **kwargs)
            raise RuntimeError('Lost connection during member reservation')
        return original(query, update, *args, **kwargs)
    monkeypatch.setattr(mod.db.referrals_v2, 'update_many', reserve_some)
    mod.update_referrer_balance(1001)
    assert mod.db.referrals_v2.count_documents({'ms_batch': 1}) == 4 and balance(mod) == 100
    mod.db.settings.insert_one({'key': 'referral_reward', 'value': 2})
    monkeypatch.setattr(mod.db.referrals_v2, 'update_many', original)
    retry(mod)
    assert balance(mod) == 100.1 and auto_batches(mod)[0]['reward'] == .1
    ready(mod, 10, start=6011, activate=True)
    assert balance(mod) == 102.1 and auto_batches(mod)[1]['reward'] == 2


@pytest.mark.parametrize('committed', [False, True])
def test_lost_wallet_response_is_recovered_with_one_credit(mod, monkeypatch, committed):
    ready(mod)
    original = mod._ref_money
    def lost_response(*args, **kwargs):
        if committed:
            original(*args, **kwargs)
        raise RuntimeError('Wallet response unavailable')
    monkeypatch.setattr(mod, '_ref_money', lost_response)
    mod.update_referrer_balance(1001)
    assert balance(mod) == (100.1 if committed else 100)
    assert auto_batches(mod)[0]['status'] == 'auto_paying'
    monkeypatch.setattr(mod, '_ref_money', original)
    retry(mod); retry(mod)
    assert balance(mod) == 100.1 and auto_batches(mod)[0]['status'] == 'paid'
    assert mod.db.balance_logs.count_documents({'type': 'ref_reward'}) == 1


def test_paid_status_failure_and_receipt_retry_never_repeat_the_wallet_credit(mod, monkeypatch):
    ready(mod)
    original = mod.db.referral_milestones.update_one
    def fail_status(query, update, *args, **kwargs):
        if update.get('$set', {}).get('status') == 'paid':
            raise RuntimeError('Final status write unavailable')
        return original(query, update, *args, **kwargs)
    monkeypatch.setattr(mod.db.referral_milestones, 'update_one', fail_status)
    mod.update_referrer_balance(1001)
    assert balance(mod) == 100.1 and auto_batches(mod)[0]['status'] == 'auto_paying'
    monkeypatch.setattr(mod.db.referral_milestones, 'update_one', original)
    retry(mod); retry(mod)
    assert balance(mod) == 100.1 and auto_batches(mod)[0]['status'] == 'paid'
    assert sum('Referral reward!' in txt for cid, txt, _ in MESSAGES if cid == 1001) == 1


def test_receipt_storage_failure_recovers_with_the_existing_wallet_outbox(mod, monkeypatch):
    ready(mod)
    original = mod.db.balance_logs.update_one
    monkeypatch.setattr(mod.db.balance_logs, 'update_one', Mock(side_effect=RuntimeError('Receipt storage unavailable')))
    mod.update_referrer_balance(1001)
    assert balance(mod) == 100.1 and auto_batches(mod)[0]['status'] == 'auto_paying'
    monkeypatch.setattr(mod.db.balance_logs, 'update_one', original)
    retry(mod); retry(mod)
    assert balance(mod) == 100.1 and mod.db.balance_logs.count_documents({'type': 'ref_reward'}) == 1
    assert auto_batches(mod)[0]['status'] == 'paid'


def test_automatic_wallet_outbox_is_resumed_only_by_the_database_lease_holder(mod, monkeypatch):
    ready(mod)
    original = mod.db.users.find_one_and_update
    def interrupted(query, update, *args, **kwargs):
        if any(name.startswith('_ref_ops.') for name in query):
            raise RuntimeError('Wallet write unavailable')
        return original(query, update, *args, **kwargs)
    monkeypatch.setattr(mod.db.users, 'find_one_and_update', interrupted)
    mod.update_referrer_balance(1001)
    event = mod.db.ref_money_events.find_one({})
    assert event['status'] == 'prepared' and mod._REF_AUTO_REWARDS.handles_event(event)
    pay = Mock(side_effect=RuntimeError('Must be called only by the leased payer'))
    monkeypatch.setattr(mod, '_ref_money', pay)
    tick = Mock()
    monkeypatch.setattr(mod._REF_AUTO_REWARDS, 'tick', tick)
    mod._ref_resume_financial()
    pay.assert_not_called()
    tick.assert_called_once()
    assert balance(mod) == 100


def test_a_competing_wallet_finisher_cannot_recredit_after_clearing_its_temporary_receipt(mod, monkeypatch):
    ready(mod)
    original = mod.db.users.find_one
    injected = []
    def another_finisher(query, *args, **kwargs):
        if query.get('user_id') == 1001 and not injected:
            event = mod.db.ref_money_events.find_one({'reward_mode': 'website_auto_v1', 'status': 'prepared'})
            if event:
                injected.append(True)
                mod._ref_money(event['user_id'], event['amount'], event['kind'], event['note'],
                               event_key=event['operation_key'])
        return original(query, *args, **kwargs)
    monkeypatch.setattr(mod.db.users, 'find_one', another_finisher)
    mod.update_referrer_balance(1001)
    assert injected and balance(mod) == 100.1
    assert auto_batches(mod)[0]['status'] == 'paid'
    assert mod.db.balance_logs.count_documents({'type': 'ref_reward'}) == 1
    assert not mod.db.users.find_one({'user_id': 1001}).get('_ref_ops')


def test_an_unpaid_member_who_left_is_replaced_before_the_saved_batch_is_paid(mod, monkeypatch):
    ready(mod)
    original = mod._REF_AUTO_REWARDS._pay
    monkeypatch.setattr(mod._REF_AUTO_REWARDS, '_pay', Mock(return_value=False))
    mod.update_referrer_balance(1001)
    mod.mark_referral_status(6001, 'left')
    assert balance(mod) == 100
    monkeypatch.setattr(mod._REF_AUTO_REWARDS, '_pay', original)
    fresh(mod, 6011)
    mod.mark_referral_status(6011, 'active')
    row = auto_batches(mod)[0]
    assert balance(mod) == 100.1 and 6001 not in row['members'] and 6011 in row['members']
    assert 'ms_batch' not in mod._ref_find(6001)


def test_departure_during_a_lost_payment_response_is_reconciled_without_negative_cash(mod, monkeypatch):
    ready(mod)
    original = mod._ref_money
    def committed_then_lost(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError('Receipt response lost')
    monkeypatch.setattr(mod, '_ref_money', committed_then_lost)
    mod.update_referrer_balance(1001)
    monkeypatch.setattr(mod, '_ref_money', original)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': 0.}})
    # The member can leave while another process is recovering the wallet intent.
    mod.db.referrals_v2.update_one({'invited_id': 6001}, {'$set': {'status': 'left'}})
    retry(mod); retry(mod)
    user = mod.db.users.find_one({'user_id': 1001})
    assert user['balance'] == 0 and user['ref_debt'] == .01
    assert user['ref_v2_earned'] == .09
    assert mod.db.ref_money_events.count_documents({'kind': 'ref_reward'}) == 1
    assert mod.db.ref_money_events.count_documents({'kind': 'ref_clawback'}) == 1


def test_departure_and_return_reuse_the_original_paid_value_without_a_new_batch(mod):
    ready(mod, activate=True)
    mod.db.settings.insert_one({'key': 'referral_reward', 'value': 5})
    mod.mark_referral_status(6001, 'left')
    assert balance(mod) == 100.09
    mod.mark_referral_status(6001, 'active'); mod.mark_referral_status(6001, 'active')
    assert balance(mod) == 100.1 and len(auto_batches(mod)) == 1
    assert auto_batches(mod)[0]['paid_amount'] == .1
    assert mod.db.ref_money_events.count_documents({'kind': 'ref_refund'}) == 1


def test_banned_referrer_does_not_receive_an_automatic_reward(mod):
    ready(mod)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'is_banned': 1}})
    mod.update_referrer_balance(1001)
    assert balance(mod) == 100
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'is_banned': 0}})
    retry(mod)
    assert balance(mod) == 100.1


def test_legacy_negative_balance_is_not_rewritten_or_migrated_by_the_new_policy(mod):
    ready(mod)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -50., 'ref_v2_earned': 7.}})
    mod.update_referrer_balance(1001); retry(mod)
    user = mod.db.users.find_one({'user_id': 1001})
    assert user['balance'] == -50 and user['ref_v2_earned'] == 7
    assert not mod.db.ref_balance_reconciliations.count_documents({})
    assert not mod.db.balance_logs.count_documents({})
    assert auto_batches(mod)[0]['status'] == 'auto_paying'


@pytest.mark.parametrize('language,phrase', [('ar', 'مكافأة الإحالات'), ('en', 'Referral reward!'),
                                         ('ru', 'Реферальный бонус'), ('zh', '邀请奖励')])
def test_automatic_reward_uses_the_current_site_language_and_existing_cms(mod, language, phrase):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'ui_lang': language, 'lang': 'ar'}})
    ready(mod, activate=True)
    assert any(phrase in text for cid, text, _ in MESSAGES if cid == 1001)
    assert language in mod.LANG and 'ref_auto_rules' in mod.LANG[language]
    mod.db.custom_texts.insert_one({'key': 'ref_auto_rules', 'lang': language, 'value': 'CUSTOM RULES'})
    assert mod._notification_text(1001, 'ref_auto_rules') == 'CUSTOM RULES'


def test_saved_reward_template_is_used_without_a_new_translation_request(mod):
    mod.db.custom_texts.insert_one({'key': 'ref_milestone_dm', 'lang': 'en',
        'value': 'CUSTOM {0} | {1} | {2}'})
    ready(mod, activate=True)
    assert any(text == 'CUSTOM 10 | 0.10 | 100.10' for cid, text, _ in MESSAGES if cid == 1001)


def test_notification_failure_retries_the_message_without_recrediting_or_spamming(mod, monkeypatch):
    ready(mod)
    original = mod.bot.send_message
    fail = Mock(side_effect=RuntimeError('Telegram unavailable'))
    monkeypatch.setattr(mod.bot, 'send_message', fail)
    mod._REF_AUTO_REWARDS.process(1001)
    assert balance(mod) == 100.1 and auto_batches(mod)[0]['auto_notify_pending']
    mod._REF_AUTO_REWARDS.tick(); mod._REF_AUTO_REWARDS.tick()
    assert fail.call_count == 1
    monkeypatch.setattr(mod.bot, 'send_message', original)
    mod.db.referral_milestones.update_many({}, {'$set': {'auto_notify_claim_until': 0}})
    retry(mod); retry(mod)
    assert balance(mod) == 100.1 and not auto_batches(mod)[0]['auto_notify_pending']
    assert sum('Referral reward!' in text for cid, text, _ in MESSAGES if cid == 1001) == 1


def test_new_pending_referrals_produce_no_membership_polling_or_outbound_http(mod, monkeypatch):
    mod.db.referrals_v2.insert_many([{'invited_id': 7000 + i, 'referrer_id': 1001,
        'ref_auto_version': 1, 'ref_policy_version': 30, 'status': 'pending'} for i in range(2000)])
    membership = Mock(return_value=True)
    monkeypatch.setattr(mod, 'check_forced_sub', membership)
    mod._REF_AUTO_REWARDS.tick()
    assert mod._ref_checker_tick() == 0
    membership.assert_not_called()
    assert not auto_batches(mod) and balance(mod) == 100


def test_new_archived_referral_keeps_the_requirement_when_restored(mod):
    row = fresh(mod, proof=False)
    mod.db.referrals_v2.delete_one({'_id': row['_id']})
    mod.db.referrals_archived.insert_one({key: value for key, value in row.items() if key != '_id'})
    restored = mod._ref_find(6001)
    assert restored['ref_auto_version'] == 1 and mod.ref_dv_required(restored)
    mod.mark_referral_status(6001, 'active')
    assert mod._ref_find(6001)['status'] == 'left' and balance(mod) == 100


def test_installation_is_idempotent_and_preserves_legacy_records_and_money(mod):
    referral(mod, paid=True)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': 3., 'ref_debt': 1.2}})
    before = {name: copy.deepcopy(list(mod.db[name].find({}))) for name in mod.db.list_collection_names()}
    assert mod._ref_auto_module.install(mod.__dict__) is mod._REF_AUTO_REWARDS
    assert before == {name: list(mod.db[name].find({})) for name in before}


def test_anonymous_or_other_identity_cannot_verify_a_new_referral(mod, server):
    fresh(mod, proof=False)
    fresh(mod, 6002, proof=False)
    issued = challenge(mod, server, uid=6001)
    assert request(server, '/webapp/verify', verify_payload(issued), uid=6001, auth=False)[0] == 401
    assert request(server, '/webapp/verify', verify_payload(issued), uid=6002)[0] == 400
    assert mod.get_ref_counts(1001)[:2] == (2, 0)


def test_new_instruction_is_accessible_in_the_existing_admin_text_buttons(mod):
    call = types.SimpleNamespace(id='test', data='edit_txt_ref_auto_rules',
        from_user=types.SimpleNamespace(id=9001),
        message=types.SimpleNamespace(chat=types.SimpleNamespace(id=9001), message_id=1))
    mod.ad_edit_txt_prompt(call)
    assert any('10' in txt and 'التحقق' in txt for _, txt, _ in MESSAGES)
