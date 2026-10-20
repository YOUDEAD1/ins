"""Website-gated referrals and audited wallet reversals, with fake identities and memory DB."""
import datetime as dt
import hashlib
import json
import types
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from harness import MESSAGES
from test_integration import mod, isolate, server, request, _ref_fp, _verified_ref_request
from test_error_monitor import empty_monitor


def referral(mod, invited=5001, *, proof=True, status='active', paid=False, amount=1., batch=1):
    if not mod.db.users.find_one({'user_id': invited}):
        mod.db.users.insert_one({'user_id': invited, 'name': 'Test invitee', 'balance': 0., 'is_admin': 0,
                                'is_banned': 0, 'lang': 'en', 'lang_chosen': True})
    row = {'invited_id': invited, 'referrer_id': 1001, 'status': status, 'created_at': 1,
           'ref_policy_version': 30}
    if proof:
        row.update(dv='ok', dv_at=1, web_verified_at=1, web_verified_version=2)
    if paid:
        row['ms_batch'] = batch
        mod.db.referral_milestones.insert_one({'referrer_id': 1001, 'batch_no': batch, 'status': 'paid',
            'threshold': 1, 'reward': amount, 'paid_amount': amount, 'members': [invited]})
        mod.db.users.update_one({'user_id': 1001}, {'$set': {'ref_v2_earned': amount}})
    mod.db.referrals_v2.insert_one(row)
    return mod.db.referrals_v2.find_one({'invited_id': invited})


def challenge(mod, server, uid=5001):
    status, body, _ = request(server, '/webapp/verify/challenge', uid=uid)
    assert status == 200
    mod.db.ref_verification_challenges.update_one({'_id': body['challenge']}, {'$inc': {'issued_at': -1}})
    return body


def verify_payload(issued, **changes):
    payload = {'fp': _ref_fp('d' * 32), 'challenge': issued['challenge'],
        'ch': {'ok': True, 'trusted': True, 'ms': 1000, 'answer': issued['target']}}
    payload.update(changes)
    return payload


@pytest.mark.parametrize('state,dv', [('pending', None), ('left', None), ('active', None),
                                     ('pending', 'ok_admin'), ('pending', 'flag')])
@pytest.mark.parametrize('device_check', ['on', 'off'])
def test_bot_cannot_activate_or_count_a_referral_without_website_proof(mod, monkeypatch, state, dv, device_check):
    rec = referral(mod, proof=False, status=state)
    mod.db.settings.update_one({'key': 'ref_dv'}, {'$set': {'value': device_check}}, upsert=True)
    if dv:
        mod.db.referrals_v2.update_one({'_id': rec['_id']}, {'$set': {'dv': dv}})
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: True)
    mod.mark_referral_status(5001, 'active')
    assert not mod._ref_web_verified(mod._ref_find(5001))
    assert mod.ref_progress(1001)[0] == 0 and mod.get_ref_counts(1001)[1] == 0
    assert not mod.award_purchase_referral_reward(5001, 'Test product', 10)
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_admin_approval_and_a_missing_url_cannot_bypass_the_website(mod, monkeypatch):
    referral(mod, proof=False, status='pending')
    monkeypatch.setattr(mod, '_wa_public_url', lambda: None)
    note = mod._refdv_apply(5001, 'ok', 9001)
    assert 'التحقق' in note and mod._ref_find(5001)['status'] == 'pending'
    assert mod.ref_dv_required(mod._ref_find(5001))
    assert mod.ref_progress(1001)[0] == 0


def test_admin_cannot_pay_a_milestone_whose_members_never_verified(mod):
    referral(mod, proof=False)
    mod.db.referral_milestones.insert_one({'referrer_id': 1001, 'batch_no': 1, 'status': 'pending',
        'threshold': 1, 'reward': 1., 'members': [5001]})
    call = types.SimpleNamespace(id='test', data='refwd_ok_1001_1', from_user=types.SimpleNamespace(id=9001),
        message=types.SimpleNamespace(chat=types.SimpleNamespace(id=9001)))
    mod.ref_milestone_approve(call)
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    assert mod.db.referral_milestones.find_one({})['status'] == 'pending'


def test_milestone_counts_require_website_proof_and_members_belonging_to_the_referrer(mod):
    referral(mod, proof=False)
    mod.db.settings.update_one({'key': 'referral_threshold'}, {'$set': {'value': 1}}, upsert=True)
    assert mod.ref_ms_ready_count() == 0 and mod.ref_ms_scan() == 0
    mod.db.referrals_v2.update_one({'invited_id': 5001}, {'$set': {'dv': 'ok', 'dv_at': 1}})
    assert mod._ms_active_members({'referrer_id': 1002, 'members': [5001]}) == 0
    assert mod._ms_active_members({'referrer_id': 1001, 'members': [5001]}) == 1


def test_old_milestone_cannot_pay_again_using_members_of_an_already_paid_batch(mod):
    referral(mod, paid=True)
    mod.db.referral_milestones.insert_one({'referrer_id': 1001, 'batch_no': 2, 'status': 'pending',
        'threshold': 1, 'reward': 1., 'active_count': 1})
    call = types.SimpleNamespace(id='test', data='refwd_ok_1001_2', from_user=types.SimpleNamespace(id=9001),
        message=types.SimpleNamespace(chat=types.SimpleNamespace(id=9001)))
    mod.ref_milestone_approve(call)
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    assert mod.db.referral_milestones.find_one({'batch_no': 2})['status'] == 'pending'


def test_old_milestone_pins_verified_unpaid_members_before_crediting(mod):
    referral(mod)
    mod.db.referral_milestones.insert_one({'referrer_id': 1001, 'batch_no': 1, 'status': 'pending',
        'threshold': 1, 'reward': 1., 'active_count': 1})
    call = types.SimpleNamespace(id='test', data='refwd_ok_1001_1', from_user=types.SimpleNamespace(id=9001),
        message=types.SimpleNamespace(chat=types.SimpleNamespace(id=9001)))
    mod.ref_milestone_approve(call)
    assert mod.db.referral_milestones.find_one({'batch_no': 1})['members'] == [5001]
    assert mod._ref_find(5001)['ms_batch'] == 1
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 101


def test_signed_website_challenge_activates_once_and_records_server_proof(mod, server):
    referral(mod, proof=False, status='pending')
    c = challenge(mod, server)
    status, body, _ = request(server, '/webapp/verify', verify_payload(c), uid=5001)
    assert status == 200 and body['ok']
    rec = mod._ref_find(5001)
    assert rec['status'] == 'active' and rec['web_verified_version'] == 2 and rec['web_verified_at'] > 1
    assert mod.db.ref_verification_challenges.find_one({'_id': c['challenge']})['used']
    request(server, '/webapp/verify', verify_payload(c), uid=5001)
    assert mod.ref_progress(1001)[0] == 1 and mod.db.users.find_one({'user_id': 1001})['balance'] == 100


@pytest.mark.parametrize('change', [
    {'challenge': ''}, {'ch': {'ok': False}}, {'ch': {'ok': True, 'trusted': False, 'ms': 1000}},
    {'ch': {'ok': True, 'trusted': True, 'ms': 10}},
    {'ch': {'ok': True, 'trusted': True, 'ms': 1000, 'answer': 'wrong'}}])
def test_failed_human_verification_cannot_become_a_counted_flag(mod, server, change):
    referral(mod, proof=False, status='pending')
    c = challenge(mod, server)
    status, _, _ = request(server, '/webapp/verify', verify_payload(c, **change), uid=5001)
    assert status == 400 and mod._ref_find(5001)['status'] == 'pending'
    assert not mod._ref_web_verified(mod._ref_find(5001))


def test_challenge_is_bound_to_signed_identity_and_expiry(mod, server):
    referral(mod, proof=False, status='pending')
    referral(mod, invited=5002, proof=False, status='pending')
    c = challenge(mod, server, 5001)
    assert request(server, '/webapp/verify', verify_payload(c), uid=5002)[0] == 400
    mod.db.ref_verification_challenges.update_one({'_id': c['challenge']}, {'$set': {
        'expires_at': dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - dt.timedelta(seconds=1)}})
    assert request(server, '/webapp/verify', verify_payload(c), uid=5001)[0] == 400
    assert mod.get_ref_counts(1001)[1] == 0
    assert request(server, '/webapp/verify/challenge', auth=False)[0] == 401


def test_membership_timeout_does_not_count_as_successful_website_verification(mod, server, monkeypatch):
    referral(mod, proof=False, status='pending')
    c = challenge(mod, server)
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: None)
    assert request(server, '/webapp/verify', verify_payload(c), uid=5001)[0] == 503
    assert not mod._ref_web_verified(mod._ref_find(5001))
    monkeypatch.setattr(mod, '_wa_forced_info', lambda *a: None)
    assert not mod._wa_forced_check({'id': 5001}, {})[1]['subscribed']


def test_reversal_cannot_make_spendable_balance_negative_and_return_restores_exact_value(mod):
    referral(mod, paid=True)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': .25}})
    mod.mark_referral_status(5001, 'left')
    user = mod.db.users.find_one({'user_id': 1001})
    assert user['balance'] == 0 and user['ref_debt'] == .75
    assert user['ref_v2_earned'] == 0
    mod.mark_referral_status(5001, 'left')
    assert mod.db.users.find_one({'user_id': 1001})['ref_debt'] == .75
    mod.mark_referral_status(5001, 'active')
    user = mod.db.users.find_one({'user_id': 1001})
    assert user['balance'] == .25 and user['ref_debt'] == 0 and user['ref_v2_earned'] == 1
    assert mod.db.ref_money_events.count_documents({}) == 2


def test_future_rewards_settle_recorded_liability_without_creating_cash(mod):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': 0., 'ref_debt': .75}})
    first = mod._ref_money(1001, .5, 'ref_reward', 'First reward', event_key='reward-1')
    assert first['balance'] == 0 and first['ref_debt'] == .25
    second = mod._ref_money(1001, .5, 'ref_reward', 'Second reward', event_key='reward-2')
    assert second['balance'] == .25 and second['ref_debt'] == 0 and second['ref_v2_earned'] == 1


def test_replayed_reward_in_parallel_changes_wallet_and_ledger_once(mod):
    def pay(_): return mod._ref_money(1001, .1, 'ref_reward', 'Test award', event_key='same-award')
    with ThreadPoolExecutor(max_workers=6) as pool:
        assert all(pool.map(pay, range(12)))
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100.1
    assert mod.db.ref_money_events.count_documents({}) == mod.db.balance_logs.count_documents({}) == 1
    assert not mod.db.users.find_one({'user_id': 1001}).get('_ref_ops')


@pytest.mark.parametrize('committed', [False, True])
def test_uncertain_wallet_write_is_resumed_without_double_credit(mod, monkeypatch, committed):
    original = mod.db.users.find_one_and_update
    def unavailable(query, update, *a, **k):
        if any(key.startswith('_ref_ops.') for key in query):
            if committed: original(query, update, *a, **k)
            raise RuntimeError('Simulated connection lost')
        return original(query, update, *a, **k)
    monkeypatch.setattr(mod.db.users, 'find_one_and_update', unavailable)
    with pytest.raises(RuntimeError):
        mod._ref_money(1001, .5, 'ref_reward', 'Test', event_key='uncertain-award')
    monkeypatch.setattr(mod.db.users, 'find_one_and_update', original)
    mod._ref_resume_financial(); mod._ref_resume_financial()
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100.5
    assert mod.db.ref_money_events.count_documents({}) == mod.db.balance_logs.count_documents({}) == 1


def test_receipt_storage_failure_does_not_repeat_a_completed_wallet_credit(mod, monkeypatch):
    original = mod.db.balance_logs.update_one
    monkeypatch.setattr(mod.db.balance_logs, 'update_one', Mock(side_effect=RuntimeError('Receipt unavailable')))
    with pytest.raises(RuntimeError):
        mod._ref_money(1001, .2, 'ref_reward', 'Test', event_key='receipt-outage')
    monkeypatch.setattr(mod.db.balance_logs, 'update_one', original)
    assert mod.db.ref_money_events.find_one({})['ledger_pending']
    mod._ref_resume_financial(); mod._ref_resume_financial()
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100.2
    assert mod.db.balance_logs.count_documents({}) == 1
    assert not mod.db.ref_money_events.find_one({})['ledger_pending']


def test_interrupted_clawback_resumes_the_saved_cycle_without_repeating_it(mod, monkeypatch):
    referral(mod, paid=True)
    original = mod._ref_money
    monkeypatch.setattr(mod, '_ref_money', Mock(side_effect=RuntimeError('Temporary wallet outage')))
    mod.mark_referral_status(5001, 'left')
    assert mod._ref_find(5001)['cb_status'] == 'processing'
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    monkeypatch.setattr(mod, '_ref_money', original)
    mod._ref_resume_financial(); mod._ref_resume_financial()
    assert mod._ref_find(5001)['cb_status'] == 'applied'
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 99
    assert mod.db.ref_money_events.count_documents({}) == mod.db.balance_logs.count_documents({}) == 1


def test_milestone_credit_survives_a_failed_final_status_write_without_a_second_credit(mod, monkeypatch):
    referral(mod)
    mod.db.referral_milestones.insert_one({'referrer_id': 1001, 'batch_no': 1, 'status': 'pending',
        'threshold': 1, 'reward': 1., 'members': [5001]})
    original = mod.db.referral_milestones.update_one
    def fail_paid(query, update, *a, **k):
        if update.get('$set', {}).get('status') == 'paid':
            raise RuntimeError('Final status unavailable')
        return original(query, update, *a, **k)
    monkeypatch.setattr(mod.db.referral_milestones, 'update_one', fail_paid)
    call = types.SimpleNamespace(id='test', data='refwd_ok_1001_1', from_user=types.SimpleNamespace(id=9001),
        message=types.SimpleNamespace(chat=types.SimpleNamespace(id=9001)))
    with pytest.raises(RuntimeError):
        mod.ref_milestone_approve(call)
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 101
    assert mod.db.referral_milestones.find_one({})['status'] == 'paying'
    monkeypatch.setattr(mod.db.referral_milestones, 'update_one', original)
    mod._ref_resume_financial(); mod._ref_resume_financial()
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 101
    assert mod.db.referral_milestones.find_one({})['status'] == 'paid'
    assert mod.db.ref_money_events.count_documents({}) == 1


def test_concurrent_deposit_is_not_overwritten_by_reward_compare_and_set(mod, monkeypatch):
    original = mod.db.users.find_one_and_update
    injected = []
    def deposit(query, update, *a, **k):
        if not injected and any(key.startswith('_ref_ops.') for key in query):
            injected.append(True)
            mod.db.users.update_one({'user_id': 1001}, {'$inc': {'balance': 5}})
        return original(query, update, *a, **k)
    monkeypatch.setattr(mod.db.users, 'find_one_and_update', deposit)
    mod._ref_money(1001, 1, 'ref_reward', 'Test', event_key='alongside-deposit')
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 106


def test_inferred_legacy_batch_and_unconfirmed_payment_are_not_charged_back(mod):
    rec = referral(mod, paid=True)
    mod.db.referral_milestones.update_one({}, {'$unset': {'paid_amount': ''}})
    assert mod._ref_clawback(rec) == 0
    mod.db.referrals_v2.update_one({'invited_id': 5001}, {'$set': {'ms_batch': 0}})
    assert mod._ref_clawback(mod._ref_find(5001)) == 0
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_documented_negative_referral_balance_is_reclassified_with_identical_net_position(mod):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -.4, 'ref_debt': .2}})
    mod.db.balance_logs.insert_one({'user_id': 1001, 'type': 'ref_clawback', 'amount': -.5,
        'new_balance': -.4, 'date': dt.datetime.now(), 'note': 'إحالة غادرت (5001)'})
    row = mod._ref_repair_user(1001)
    assert row['corrected'] and row['net_before'] == row['net_after'] == -.6
    user = mod.db.users.find_one({'user_id': 1001})
    assert user['balance'] == 0 and user['ref_debt'] == .6
    assert not mod._ref_repair_user(1001)['corrected']
    assert mod.db.ref_balance_reconciliations.count_documents({}) == 1
    assert mod.db.balance_logs.find_one({'type': 'ref_debt_reclass'})['net_change'] == 0


@pytest.mark.parametrize('kind,after', [('deduct', -2.), ('ref_clawback', -1.5)])
def test_unproven_negative_balance_is_reported_for_review_and_never_fabricated(mod, kind, after):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -2.}})
    mod.db.balance_logs.insert_one({'user_id': 1001, 'type': kind, 'amount': -3.,
        'new_balance': after, 'date': dt.datetime.now()})
    report = mod._ref_repair_batch()
    assert report['review'] == 1 and report['corrected'] == 0
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == -2
    assert not mod.db.ref_balance_reconciliations.count_documents({})


def test_initialization_never_erases_existing_referrals_money_or_custom_text(mod):
    referral(mod, proof=False, status='pending')
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'ref_v2_earned': 7.}})
    mod.db.custom_texts.insert_one({'key': 'invite_txt', 'lang': 'en', 'value': 'Keep this text'})
    mod.db.settings.delete_many({'key': mod.REF_V2_INIT_KEY})
    mod.initialize_referrals_v2()
    assert mod.db.referrals_v2.count_documents({}) == 1
    assert mod.db.users.find_one({'user_id': 1001})['ref_v2_earned'] == 7
    assert mod.db.custom_texts.find_one({'key': 'invite_txt'})['value'] == 'Keep this text'


def test_an_archived_referral_can_verify_and_recover_its_proven_original_deduction(mod, server):
    referral(mod, proof=False, status='pending')
    mod.db.referrals_v2.delete_one({'invited_id': 5001})
    mod.db.referrals_archived.insert_one({'invited_id': 5001, 'referrer_id': 1001, 'status': 'left',
        'cb_amount': .4, 'cb_at': 100, 'ms_batch': 0, 'archived_at': 100, 'ref_policy_version': 30})
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': 99.6}})
    mod.db.ref_clawbacks.insert_one({'invited_id': 5001, 'referrer_id': 1001, 'kind': 'clawback', 'amount': .4, 'open': True})
    mod.db.balance_logs.insert_one({'user_id': 1001, 'type': 'ref_clawback', 'amount': -.4,
        'new_balance': 99.6, 'date': dt.datetime.now(), 'note': 'إحالة غادرت (5001)'})
    assert mod.ref_dv_pending_for(5001)
    c = challenge(mod, server)
    assert request(server, '/webapp/verify', verify_payload(c), uid=5001)[0] == 200
    assert mod._ref_find(5001)['status'] == 'active'
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    assert mod.get_ref_counts(1001) == (0, 1, 0, 1)


def test_already_active_referral_with_a_stale_charge_is_refunded_only_once(mod):
    referral(mod, paid=True)
    mod.mark_referral_status(5001, 'left')
    mod.db.referrals_v2.update_one({'invited_id': 5001}, {'$set': {'status': 'active'}})
    mod.mark_referral_status(5001, 'active'); mod.mark_referral_status(5001, 'active')
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100
    assert mod.db.ref_money_events.count_documents({'kind': 'ref_refund'}) == 1


def test_reconciliation_and_liability_details_are_private_and_visible_to_their_owner(mod, server):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'ref_debt': .75}})
    status, result, _ = request(server, '/webapp/me')
    assert status == 200 and result['ref_debt'] == .75
    status, result, _ = request(server, '/webapp/referrals')
    assert status == 200 and result['ref_debt'] == .75
    assert request(server, '/webapp/me', uid=1002)[1]['ref_debt'] == 0
    mod._ref_audit_cmd(types.SimpleNamespace(from_user=types.SimpleNamespace(id=1001), chat=types.SimpleNamespace(id=1001)))
    assert not MESSAGES
