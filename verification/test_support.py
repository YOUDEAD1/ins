"""Support-button regression tests. All identities/network calls are mocked."""
import ast
import time
from unittest.mock import Mock
import pytest
from test_integration import mod, isolate
from harness import MESSAGES, APP


def callback(mod,data='open_support',uid=1001):
    return mod.types.CallbackQuery.de_json({'id':'test-support-query','from':{'id':uid,'is_bot':False,'first_name':'Test'},
        'chat_instance':'test-only','data':data,'message':{'message_id':10,'date':1,'chat':{'id':uid,'type':'private'},'text':'menu'}})


def dispatch(mod,data='open_support',uid=1001):
    return mod.bot._run_middlewares_and_handler(callback(mod,data,uid),handlers=mod.bot.callback_query_handlers,
                                               middlewares=None,update_type='callback_query')


def buttons(record):
    markup=record[2].get('reply_markup')
    return [b for row in markup.to_dict()['inline_keyboard'] for b in row] if markup else []


@pytest.mark.parametrize('value',[
    'local_test_support','@local_test_support','@@local_test_support',
    '  @local_test_support  ','https://t.me/local_test_support',
    'https://t.me/local_test_support/','t.me/local_test_support',
    'https://telegram.me/local_test_support','https://telegram.dog/local_test_support',
    'tg://resolve?domain=local_test_support','https://t.me/local_test_support?profile',
    '\u200f@local_test_support\u200b'])
def test_normalized_targets(mod,monkeypatch,value):
    monkeypatch.setattr(mod,'OWNER_USER',value)
    assert mod._support_contact_links()=={'username':'local_test_support',
        'native':'tg://resolve?domain=local_test_support','web':'https://t.me/local_test_support'}


@pytest.mark.parametrize('value',['','Not Set','bad username','https://evil.example/local_test_support',
    'https://t.me/joinchat/not-a-user','https://[broken','@<script>bad</script>'])
def test_bad_username_falls_back_to_owner_without_invalid_urls(mod,monkeypatch,value):
    monkeypatch.setattr(mod,'OWNER_USER',value)
    assert mod._support_contact_links()=={'username':'','native':'tg://user?id=9001','web':None}


def test_start_shows_callback_instead_of_single_external_link(mod):
    mod.db.users.update_one({'user_id':1001},{'$set':{'lang_chosen':True}})
    message=mod.types.Message.de_json({'message_id':1,'date':1,'from':{'id':1001,'is_bot':False,'first_name':'Test'},
        'chat':{'id':1001,'type':'private'},'text':'/start'})
    mod.start_handler(message)
    found=[b for record in MESSAGES for b in buttons(record) if b.get('callback_data')=='open_support']
    assert len(found)==1
    assert 'url' not in found[0]
    assert found[0]['text']


@pytest.mark.parametrize('uid',[1001,1002])
def test_dispatch_contact_native_and_https_copyable_handle(mod,monkeypatch,uid):
    ack=Mock();monkeypatch.setattr(mod.bot,'answer_callback_query',ack)
    dispatch(mod,uid=uid)
    ack.assert_called_once_with('test-support-query')
    assert len(MESSAGES)==1
    cid,text,opts=MESSAGES[-1]
    assert cid==uid and '<code>@local_test_support</code>' in text
    assert opts['parse_mode']=='HTML'
    bs=buttons(MESSAGES[-1])
    assert [b['url'] for b in bs]==['tg://resolve?domain=local_test_support','https://t.me/local_test_support']
    assert all('style' not in b and 'icon_custom_emoji_id' not in b and 'callback_data' not in b for b in bs)


def test_support_while_payment_pending_preserves_lock_and_money(mod):
    mod.db.users.update_one({'user_id':1001},{'$set':{'deposit_locked':True,'deposit_lock_expires':int(time.time())+600}})
    assert mod._check_locked(1001)
    dispatch(mod)
    assert buttons(MESSAGES[-1])[0]['url'].startswith('tg://resolve?')
    assert mod.is_deposit_locked(1001)
    assert mod.db.users.find_one({'user_id':1001})['balance']==100
    assert mod.db.orders.count_documents({})==0


@pytest.mark.parametrize('data',['open_shop','open_deposit','admin_panel_main'])
def test_payment_guard_still_blocks_other_actions(mod,data):
    mod.db.users.update_one({'user_id':1001},{'$set':{'deposit_locked':True,'deposit_lock_expires':int(time.time())+600}})
    dispatch(mod,data)
    assert not any(b.get('url','').startswith('tg://resolve?') for record in MESSAGES for b in buttons(record))
    if data == 'admin_panel_main':
        # Version 20 rejects unauthorized admin callbacks before payment guards.
        assert mod.db.unauthorized_attempts.count_documents({'user_id':1001}) == 1
    else:
        assert any(b.get('callback_data')=='cancel_deposit' for record in MESSAGES for b in buttons(record))
    assert mod.is_deposit_locked(1001)


def test_banned_user_can_contact_support_for_appeal(mod):
    mod.db.users.update_one({'user_id':1001},{'$set':{'is_banned':1}})
    dispatch(mod)
    assert buttons(MESSAGES[-1])[0]['url'].startswith('tg://resolve?')
    assert mod.db.users.find_one({'user_id':1001})['is_banned']==1


def test_support_still_replies_if_callback_is_old(mod,monkeypatch):
    monkeypatch.setattr(mod.bot,'answer_callback_query',Mock(side_effect=RuntimeError('old callback')))
    mod.show_support(callback(mod))
    assert buttons(MESSAGES[-1])


def test_customized_main_button_appearance_preserved(mod):
    mod.db.custom_buttons.insert_one({'lang':'en','key':'btn_support','text':'Contact our team',
                                      'emoji_id':'5271619747891388291','style':'success'})
    btn=mod.create_btn(1001,'btn_support',callback_data='open_support').to_dict()
    assert btn['text']=='Contact our team' and btn['style']=='success'
    assert btn['icon_custom_emoji_id']=='5271619747891388291'
    assert btn['callback_data']=='open_support' and 'url' not in btn


def test_owner_id_fallback_has_no_broken_https_link(mod,monkeypatch):
    monkeypatch.setattr(mod,'OWNER_USER','')
    dispatch(mod)
    assert [b['url'] for b in buttons(MESSAGES[-1])]==['tg://user?id=9001']


def test_empty_config_has_safe_message(mod,monkeypatch):
    monkeypatch.setattr(mod,'OWNER_USER','')
    monkeypatch.setattr(mod,'OWNER_ID',0)
    dispatch(mod)
    assert len(MESSAGES)==1 and 'not configured' in MESSAGES[-1][1]
    assert not buttons(MESSAGES[-1])
