"""Prospective website policy and read-only historical balance diagnosis."""
import datetime as dt
import importlib.util
import json
from unittest.mock import Mock

import pytest
from harness import MESSAGES
from test_integration import mod, isolate, server, request
from test_referral_integrity import referral


def legacy(mod, **kwargs):
    rec = referral(mod, **kwargs)
    mod.db.referrals_v2.update_one({'_id': rec['_id']}, {'$unset': {'ref_policy_version': ''}})
    return mod._ref_find(rec['invited_id'])


def message(mod, uid=5001, text='/start'):
    return mod.types.Message.de_json({'message_id': 5, 'date': 1,
        'from': {'id': uid, 'is_bot': False, 'first_name': 'Test'},
        'chat': {'id': uid, 'type': 'private'}, 'text': text})


def test_registration_marks_only_new_referrals_with_the_new_requirement(mod):
    old = legacy(mod, proof=False)
    assert mod.register_new_referral(6001, 1001)
    new = mod._ref_find(6001)
    assert new['ref_policy_version'] == 30 and mod.ref_dv_required(new)
    assert not mod.ref_dv_required(old) and 'ref_policy_version' not in mod._ref_find(5001)


@pytest.mark.parametrize('balance', [10., 20., -50.])
def test_one_thousand_existing_referrals_are_not_demoted_repriced_or_changed_by_the_new_gate(mod, balance):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': balance, 'ref_v2_earned': 60}})
    rows = [{'referrer_id': 1001, 'invited_id': 6000 + i, 'status': 'active', 'created_at': 1,
             'ms_batch': 0} for i in range(1000)]
    mod.db.referrals_v2.insert_many(rows)
    assert mod.get_ref_counts(1001) == (0, 1000, 0, 1000)
    assert mod.ref_progress(1001)[0] == 0
    assert not any(mod.ref_dv_pending_for(6000 + i) for i in range(1000))
    user = mod.db.users.find_one({'user_id': 1001})
    assert user['balance'] == balance and user['ref_v2_earned'] == 60 and not user.get('ref_debt')
    assert mod.db.referrals_v2.count_documents({'status': 'active'}) == 1000
    assert not mod.db.balance_logs.count_documents({})


def test_existing_referral_opens_the_site_without_the_new_gate(mod, server):
    legacy(mod, proof=False)
    status, data, _ = request(server, '/webapp/bootstrap', uid=5001)
    assert status == 200 and not data['verify']
    assert not mod.ref_dv_gate(5001, 5001, mod._ref_find(5001), 'en')
    assert mod.ref_progress(1001)[0] == 1


def test_existing_pending_and_rejected_records_are_not_mass_activated(mod):
    legacy(mod, proof=False, status='pending')
    legacy(mod, invited=5002, proof=False, status='active')
    mod.db.referrals_v2.update_one({'invited_id': 5002}, {'$set': {'dv': 'rejected'}})
    assert mod.get_ref_counts(1001) == (2, 0, 0, 2)
    assert mod._ref_find(5001)['status'] == 'pending'
    assert not mod._ref_countable(mod._ref_find(5002))


def test_old_archive_keeps_its_original_policy_on_restore(mod):
    legacy(mod, proof=False)
    old = mod._ref_find(5001)
    mod.db.referrals_v2.delete_one({'_id': old['_id']})
    mod.db.referrals_archived.insert_one({k: v for k, v in old.items() if k != '_id'})
    assert not mod.ref_dv_required(mod._ref_find(5001))
    assert 'ref_policy_version' not in mod._ref_find(5001)


@pytest.mark.parametrize('new', [False, True])
def test_start_membership_timeout_never_marks_a_referral_left_or_changes_its_referrers_money(mod, monkeypatch, new):
    (referral if new else legacy)(mod, paid=True)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': 10}})
    monkeypatch.setattr(mod, 'check_forced_sub', lambda *a, **k: None)
    mod.start_handler(message(mod))
    assert mod._ref_find(5001)['status'] == 'active'
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 10
    assert not mod.db.users.find_one({'user_id': 1001}).get('ref_debt')
    assert not mod.db.ref_money_events.count_documents({}) and not mod.db.ref_clawbacks.count_documents({})
    assert any('Unable to verify' in text for _, text, _ in MESSAGES)


def test_background_startup_only_reports_existing_negative_balances_without_reclassifying(mod, monkeypatch):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -50}})
    repair = Mock(); monkeypatch.setattr(mod, '_ref_repair_batch', repair)
    monkeypatch.setattr(mod.time, 'sleep', lambda *a: None)
    monkeypatch.setattr(mod, '_ref_resume_financial', lambda: None)
    monkeypatch.setattr(mod, '_ref_checker_tick', lambda: 0)
    class StopLoop(Exception):
        pass
    monkeypatch.setattr(mod._REF_SCAN_WAKE, 'wait', Mock(side_effect=StopLoop))
    with pytest.raises(StopLoop):
        mod.background_referral_checker_v2()
    repair.assert_not_called()
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == -50
    assert not mod.db.ref_balance_reconciliations.count_documents({})


def test_a_negative_balance_requires_review_before_a_new_automatic_referral_movement(mod):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -50}})
    mod.db.balance_logs.insert_one({'user_id': 1001, 'type': 'ref_clawback', 'amount': -60,
        'new_balance': -50, 'date': dt.datetime.now()})
    with pytest.raises(ValueError, match='ledger review'):
        mod._ref_money(1001, -1, 'ref_clawback', 'Saved reversal', event_key='await-review')
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == -50
    assert mod.db.ref_money_events.find_one({})['status'] == 'prepared'
    assert not mod.db.ref_balance_reconciliations.count_documents({})
    # owner's rule (rounds 35/41): a reward credit is never blocked — it only reduces the deficit
    mod._ref_money(1001, 1, 'ref_reward', 'Batch reward', event_key='reward-on-negative')
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == -49


def test_balance_diagnosis_accepts_username_and_id_without_writing_or_exposing_private_fields(mod):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {
        'username': 'Nazawalai_helmandi', 'balance': -50, 'private_cookie': 'NEVER-EXPOSE'}})
    legacy(mod, proof=False)
    mod.db.balance_logs.insert_one({'user_id': 1001, 'type': 'ref_clawback', 'amount': -60,
        'new_balance': -50, 'date': dt.datetime.now(), 'note': 'Stored left reason'})
    mod.db.ref_clawbacks.insert_one({'referrer_id': 1001, 'kind': 'clawback', 'reason': 'left', 'amount': 60})
    snapshot = {n: list(mod.db[n].find({})) for n in mod.db.list_collection_names()}
    report = mod._ref_balance_diagnosis('@NAZAWALAI_HELMANDI')
    assert report['read_only'] and report['account']['balance'] == -50
    assert report['legacy_referrals'] == 1 and report['ledger_totals'][0]['cash_amount'] == -60
    assert report['stored_clawback_reasons'][0]['_id'] == 'left'
    assert mod._ref_balance_diagnosis('1001')['account'] == report['account']
    assert 'NEVER-EXPOSE' not in json.dumps(report, default=str)
    assert snapshot == {n: list(mod.db[n].find({})) for n in mod.db.list_collection_names()}


@pytest.mark.parametrize('target,state', [('@unknown_user', 'not_found'), ('bad<script>', 'invalid_target')])
def test_diagnosis_does_not_guess_a_missing_or_invalid_account(mod, target, state):
    assert mod._ref_balance_diagnosis(target)['state'] == state


def test_duplicate_stale_usernames_need_a_numeric_id(mod):
    mod.db.users.update_many({'user_id': {'$in': [1001, 1002]}}, {'$set': {'username': 'same_user'}})
    assert mod._ref_balance_diagnosis('@same_user')['state'] == 'ambiguous_username'


def test_only_admin_can_request_the_readonly_diagnosis(mod, monkeypatch):
    spy = Mock(wraps=mod._ref_balance_diagnosis); monkeypatch.setattr(mod, '_ref_balance_diagnosis', spy)
    mod._ref_why_cmd(message(mod, uid=1001, text='/refwhy 1002'))
    spy.assert_not_called(); assert not MESSAGES
    mod._ref_why_cmd(message(mod, uid=9001, text='/refwhy 1001'))
    spy.assert_called_once_with('1001')
    assert any('فحص فقط' in text for _, text, _ in MESSAGES)


def test_standalone_report_matches_bot_readonly_report_and_never_starts_the_bot(mod, monkeypatch, tmp_path):
    import dotenv
    import pymongo
    from harness import APP
    spec = importlib.util.spec_from_file_location('local_referral_report', APP / 'referral_report.py')
    report_module = importlib.util.module_from_spec(spec); spec.loader.exec_module(report_module)
    monkeypatch.setattr(dotenv, 'load_dotenv', lambda *a, **k: None)
    monkeypatch.setenv('MONGO_URI', 'mongodb://localhost/unused-test')
    monkeypatch.setenv('MONGO_DB_NAME', mod.db.name)
    monkeypatch.setattr(pymongo, 'MongoClient', lambda *a, **k: mod.db.client)
    monkeypatch.chdir(tmp_path)
    snapshot = {n: list(mod.db[n].find({})) for n in mod.db.list_collection_names()}
    assert report_module.build_report(mod.db, '1001') == mod._ref_balance_diagnosis('1001')
    assert report_module.main(['1001']) == 0
    saved = json.loads((tmp_path / 'referral-report-1001.json').read_text())
    assert saved['read_only'] and saved['account']['balance'] == 100
    assert snapshot == {n: list(mod.db[n].find({})) for n in mod.db.list_collection_names()}


def test_standalone_report_without_configuration_does_not_connect(mod, monkeypatch):
    import dotenv
    import pymongo
    from harness import APP
    spec = importlib.util.spec_from_file_location('local_referral_report_no_config', APP / 'referral_report.py')
    report_module = importlib.util.module_from_spec(spec); spec.loader.exec_module(report_module)
    monkeypatch.setattr(dotenv, 'load_dotenv', lambda *a, **k: None)
    monkeypatch.delenv('MONGO_URI', raising=False)
    connect = Mock(); monkeypatch.setattr(pymongo, 'MongoClient', connect)
    assert report_module.main(['1001']) == 2
    connect.assert_not_called()


def test_extended_report_marks_the_actual_negative_balance_gap_without_guessing_a_refund(mod):
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': -56.70000000000004}})
    mod.db.balance_logs.insert_one({'user_id': 1001, 'type': 'compensation', 'amount': 1,
        'new_balance': 3.4, 'date': dt.datetime(2026, 9, 30), 'by_admin': 9001})
    mod.db.orders.insert_one({'user_id': 1001, 'price': 8, 'code_delivered': 'PRIVATE-ACCOUNT-PASSWORD'})
    mod.db.orders.insert_one({'user_id': 1002, 'price': 200, 'code_delivered': 'OTHER-USER-PRIVATE'})
    mod.db.purchase_rewards.insert_one({'referrer_id': 1001, 'amount': .1, 'buyer_id': 1002})
    mod.db.settings.insert_one({'key': 'private_cookie', 'value': 'NEVER-EXPOSE'})
    snapshot = {n: list(mod.db[n].find({})) for n in mod.db.list_collection_names()}
    result = mod._ref_balance_diagnosis('1001')
    assert result['report_version'] == 2 and result['assessment']['change_since_latest_balance_log'] == -60.1
    assert result['assessment']['requires_review'] and result['recent_changes'][0]['by_admin'] == 9001
    assert result['financial_tables']['orders']['summary']['count'] == 1
    assert result['financial_tables']['orders']['summary']['recorded_price'] == 8
    assert result['financial_tables']['purchase_rewards']['summary']['recorded_amount'] == .1
    assert 'refund_amount' not in result
    raw = json.dumps(result, default=str)
    assert all(secret not in raw for secret in ('PRIVATE-ACCOUNT-PASSWORD','OTHER-USER-PRIVATE','NEVER-EXPOSE'))
    assert snapshot == {n: list(mod.db[n].find({})) for n in mod.db.list_collection_names()}
