"""Financial/referral notices follow the persisted app choice without changing ledger semantics."""
import re
from unittest.mock import Mock

import pytest
from harness import MESSAGES
from test_integration import mod, isolate, server, request
from test_products_menu import callback
from test_referral_integrity import referral


def choose(mod, lang):
    # Keep a deliberately stale Arabic bot cache to catch old get_lang-only paths.
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'lang': 'ar'}})
    mod._invalidate_user_cache(1001); mod.get_user_data_full(1001)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'ui_lang': lang}})


def dm():
    return '\n'.join(text for uid, text, _ in MESSAGES if uid == 1001)


def assert_language(text, lang):
    assert text, 'Customer notification was not sent'
    if lang == 'en':
        assert not re.search('[\u0600-\u06ff]', text), text
        assert re.search('[a-zA-Z]', text)
    elif lang == 'ar': assert re.search('[\u0600-\u06ff]', text)
    elif lang == 'ru': assert re.search('[\u0400-\u04ff]', text) and not re.search('[\u0600-\u06ff]', text)
    else: assert re.search('[\u4e00-\u9fff]', text) and not re.search('[\u0600-\u06ff]', text)


@pytest.mark.parametrize('lang', ['ar', 'en', 'ru', 'zh'])
def test_site_choice_is_saved_and_exposed_on_new_device_without_breaking_bot_menus(mod, server, lang):
    old = mod.db.users.find_one({'user_id': 1001})['lang']
    status, data, _ = request(server, '/webapp/lang', {'lang': lang})
    assert status == 200 and data['ok'] and data['ui_lang'] == lang
    user = mod.db.users.find_one({'user_id': 1001})
    assert user['ui_lang'] == lang and user['lang_chosen']
    assert user['lang'] == (lang if lang in ('ar', 'en') else old)
    status, bootstrap, _ = request(server, '/webapp/bootstrap')
    assert status == 200 and bootstrap['user']['ui_lang'] == lang
    assert mod._notification_language(1001) == lang


@pytest.mark.parametrize('lang', ['ar', 'en', 'ru', 'zh'])
@pytest.mark.parametrize('kind', ['purchase', 'progress', 'gift', 'compensation', 'deduction', 'deposit'])
def test_customer_notices_use_latest_app_language_despite_stale_base_cache(mod, monkeypatch, lang, kind):
    choose(mod, lang)
    if kind == 'purchase': mod._ref_purchase_dm(1001, 1002, 1.5, 101.5)
    elif kind == 'progress':
        monkeypatch.setattr(mod, 'ref_progress', lambda uid: (1, 5, 4))
        mod.send_progress_log_notification(1001)
    elif kind in ('gift', 'compensation', 'deduction'):
        mod.notify_balance_gift(1001, -2 if kind == 'deduction' else 2,
                                gift_type='compensation' if kind == 'compensation' else 'manual')
    else: mod.credit_user(1001, 2, 'FIXTURE-LANG-' + lang, 'ar', 'Fixture', trusted_txid=True)
    assert_language(dm(), lang)
    expected = 102 if kind == 'deposit' else 100
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == expected


def test_exact_cms_locale_is_used_without_falling_back_to_a_different_language(mod):
    choose(mod, 'en')
    mod.db.custom_texts.insert_one({'lang': 'ar', 'key': 'ref_purchase_dm', 'value': 'نص عربي {0}'})
    mod._ref_purchase_dm(1001, 1002, 1, 101)
    assert_language(dm(), 'en')
    MESSAGES.clear()
    mod.db.custom_texts.insert_one({'lang': 'en', 'key': 'ref_purchase_dm', 'value': 'CUSTOM REWARD ${1} BALANCE ${2}'})
    mod._ref_purchase_dm(1001, 1002, 1, 101)
    assert 'CUSTOM REWARD $1.00 BALANCE $101.00' in dm()


@pytest.mark.parametrize('lang', ['ar', 'en', 'ru', 'zh'])
def test_broken_custom_format_uses_default_in_the_same_language(mod, lang):
    choose(mod, lang)
    mod.db.custom_texts.insert_one({'lang': lang, 'key': 'ref_progress_dm', 'value': 'broken {999}'})
    assert_language(mod._notification_text(1001, 'ref_progress_dm', 1, 4, '1.00'), lang)


@pytest.mark.parametrize('lang', ['ar', 'en', 'ru', 'zh'])
def test_paid_milestone_notice_and_idempotent_wallet_follow_ui_language(mod, lang):
    choose(mod, lang); ref = referral(mod)
    mod.db.referrals_v2.update_one({'_id': ref['_id']}, {'$set': {'ms_batch': 1}})
    mod.db.referral_milestones.insert_one({'referrer_id': 1001, 'batch_no': 1, 'status': 'pending',
        'threshold': 1, 'reward': 1, 'members': [5001]})
    mod.ref_milestone_approve(callback(mod, 'refwd_ok_1001_1', 9001))
    assert_language(dm(), lang)
    mod.ref_milestone_approve(callback(mod, 'refwd_ok_1001_1', 9001))
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 101
    assert mod.db.ref_money_events.count_documents({}) == 1


@pytest.mark.parametrize('lang', ['ar', 'en', 'ru', 'zh'])
def test_reversal_and_refund_both_follow_app_locale_and_refund_exact_value(mod, lang):
    choose(mod, lang); rec = referral(mod, paid=True, amount=.5)
    assert mod._ref_clawback(rec) == .5
    assert_language(dm(), lang)
    MESSAGES.clear()
    assert mod._ref_refund(mod._ref_find(5001)) == .5
    assert_language(dm(), lang)
    assert mod.db.users.find_one({'user_id': 1001})['balance'] == 100


def test_balance_gift_reads_fresh_balance_and_escapes_admin_note(mod):
    choose(mod, 'en'); mod.get_user_data_full(1001)
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': 123}})
    mod.notify_balance_gift(1001, 2, note='<test & note>')
    assert '$123.00' in dm() and '&lt;test &amp; note&gt;' in dm()


def test_bot_language_button_overrides_previous_app_language(mod, monkeypatch):
    choose(mod, 'ru')
    monkeypatch.setattr(mod, 'start_handler', Mock())
    mod.toggle_lang(callback(mod, 'toggle_language'))
    user = mod.db.users.find_one({'user_id': 1001})
    assert user['lang'] == user['ui_lang'] == 'en'


def test_unknown_language_and_banned_account_do_not_update_preferences(mod, server):
    before = mod.db.users.find_one({'user_id': 1001})
    request(server, '/webapp/lang', {'lang': 'xx'})
    assert mod.db.users.find_one({'user_id': 1001}) == before
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'is_banned': 1}})
    mod._wa_set_lang({'id': 1001}, {'lang': 'ru'})
    assert 'ui_lang' not in mod.db.users.find_one({'user_id': 1001})
