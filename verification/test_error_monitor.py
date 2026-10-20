"""Private error records and alerts; all identities, DB and delivery are mocked."""
import json
import logging
from unittest.mock import Mock
import pytest
from harness import MESSAGES
from test_integration import mod, isolate, server, request, product, _api
from test_products_menu import dispatch, buttons


@pytest.fixture(autouse=True)
def empty_monitor(mod,isolate):
    while True:
        try:mod._ERROR_QUEUE.get_nowait();mod._ERROR_QUEUE.task_done()
        except mod._error_queue_module.Empty:break
    mod._ERROR_MEMORY.clear()
    yield


def test_errors_include_cause_user_product_operation_and_stack(mod,server,monkeypatch):
    def broken(identity,body):raise ValueError('Supplier returned a malformed response')
    monkeypatch.setitem(mod._WA_POST_ROUTES,'/webapp/test/error',broken)
    s,r,_=request(server,'/webapp/test/error',{'product_id':'101'})
    assert s==500 and r=={'error':'server_error'}
    mod._error_drain()
    rec=mod.db.system_errors.find_one({})
    assert rec['user_id']==1001 and rec['product_id']=='101' and rec['source']=='webapp'
    assert rec['operation']=='/webapp/test/error' and rec['exception']=='ValueError'
    assert 'malformed response' in rec['cause'] and 'broken' in rec['trace']
    assert rec['request_id'] and rec['expires_at']>rec['last_at']


def test_auth_api_errors_have_authenticated_user_without_secret_gateway(mod,server,monkeypatch):
    key='sk_'+'f'*48
    mod.db.api_keys.insert_one({'user_id':1001,'api_key':key,'is_active':True})
    monkeypatch.setattr(mod.db.products,'find',Mock(side_effect=RuntimeError('catalog failed')))
    s,r=_api(server,'GET','/'+mod._get_api_gateway()+'/products',key)
    assert s==503
    mod._error_drain();rec=mod.db.system_errors.find_one({})
    assert rec and rec['user_id']==1001 and rec['source']=='api'
    raw=json.dumps(rec,default=str)
    assert key not in raw and mod._get_api_gateway() not in raw


def test_callback_exception_is_captured_with_customer_identity(mod,monkeypatch):
    from test_products_menu import callback
    call=callback(mod,'deliberate-test',1001)
    def broken(call):raise RuntimeError('callback failed')
    handler={'function':broken,'pass_bot':False,'filters':{'func':lambda c:True}}
    with pytest.raises(RuntimeError):
        mod.bot._run_middlewares_and_handler(call,handlers=[handler],middlewares=None,update_type='callback_query')
    mod._error_drain();rec=mod.db.system_errors.find_one({})
    assert rec['user_id']==1001 and rec['source']=='bot' and 'deliberate-test' in rec['operation']
    assert rec['exception']=='RuntimeError' and 'broken' in rec['trace']


def test_warning_and_error_are_saved_and_only_errors_alert(mod,monkeypatch):
    notices=Mock();monkeypatch.setattr(mod,'notify_admins',notices)
    with mod._error_scope(user_id=1001,source='bot',operation='test'):
        mod.logger.warning('A recoverable issue')
        mod.logger.error('A technical failure')
    mod._error_drain()
    assert mod.db.system_errors.count_documents({})==2 and notices.call_count==1
    assert 'technical failure' in notices.call_args.args[0]
    assert 'ad_error_' in json.dumps(notices.call_args.kwargs['buttons'].to_dict())


def test_repeat_errors_increment_count_and_deduplicate_alerts_persistently(mod,monkeypatch):
    notices=Mock();monkeypatch.setattr(mod,'notify_admins',notices)
    with mod._error_scope(user_id=1001,operation='same'):
        a=mod._error_capture('Same failure');b=mod._error_capture('Same failure')
    mod._error_drain()
    assert a==b and mod.db.system_errors.find_one({'_id':a})['count']==2 and notices.call_count==1
    mod._error_capture('Same failure')
    mod._error_drain()
    # Different users/background contexts remain distinct records.
    assert mod.db.system_errors.count_documents({})==2


def test_secrets_are_redacted_before_memory_database_and_notifications(mod,monkeypatch):
    secret='FAKE_PRIVATE_TOKEN_123456789'
    monkeypatch.setenv('TOKEN',secret)
    notices=Mock();monkeypatch.setattr(mod,'notify_admins',notices)
    with mod._error_scope(user_id=1001):
        mod._error_capture('Problem '+secret+' mongodb+srv://tester:PRIVATE_PASS@cluster.example/db?auth=PRIVATE_QUERY\nCookie=FAKE_COOKIE\nAuthorization: Bearer FAKE_HEADER\napi_key=FAKE_KEY')
    mod._error_drain()
    raw=json.dumps(list(mod._ERROR_MEMORY),default=str)+json.dumps(list(mod.db.system_errors.find()),default=str)+str(notices.call_args)
    assert not any(x in raw for x in [secret,'PRIVATE_PASS','PRIVATE_QUERY','FAKE_COOKIE','FAKE_HEADER','FAKE_KEY'])
    assert '[hidden]' in raw


def test_alert_switch_does_not_stop_recording(mod,monkeypatch):
    mod.db.settings.insert_one({'key':'error_alerts_enabled','value':'off'})
    notices=Mock();monkeypatch.setattr(mod,'notify_admins',notices)
    mod._error_capture('Failure while alerts disabled');mod._error_drain()
    assert mod.db.system_errors.count_documents({})==1 and not notices.called


def test_browser_reports_are_authenticated_and_cannot_spoof_owner(mod,server):
    s,r,_=request(server,'/webapp/client/error',{'message':'Runtime failed','operation':'cart-review','user_id':9001},auth=False)
    assert s==401
    s,r,_=request(server,'/webapp/client/error',{'message':'Runtime failed','operation':'cart-review','user_id':9001})
    assert s==200 and r['reference']
    mod._error_drain();rec=mod.db.system_errors.find_one({})
    assert rec['user_id']==1001 and rec['source']=='browser' and rec['operation']=='browser:cart-review'
    results=[request(server,'/webapp/client/error',{'message':'repeat'})[0] for _ in range(12)]
    assert 429 in results


@pytest.mark.parametrize('data',['ad_errors','ad_errors_all_0','ad_error_alerts_toggle','ad_error_'+'a'*32,'ad_err_resolve_'+'a'*32])
def test_customer_cannot_read_or_change_error_center(mod,data):
    dispatch(mod,data,1001)
    assert not MESSAGES and mod.db.settings.find_one({'key':'error_alerts_enabled'}) is None


def test_admin_reads_resolves_error_and_new_occurrence_reopens_it(mod,monkeypatch):
    monkeypatch.setitem(mod._VC_ORIG,'edit_message_text',lambda text,*a,**kw:mod.bot.send_message(9001,text,**kw))
    with mod._error_scope(user_id=1001,username='customer',operation='purchase',product_id='101'):
        key=mod._error_capture('Unavailable supplier')
    mod._error_drain();MESSAGES.clear()
    dispatch(mod,'ad_errors',9001)
    assert any(b.get('callback_data')=='ad_error_'+key for row in buttons(MESSAGES[-1]) for b in row)
    dispatch(mod,'ad_error_'+key,9001)
    assert '1001' in MESSAGES[-1][1] and 'customer' in MESSAGES[-1][1] and 'Unavailable supplier' in MESSAGES[-1][1]
    dispatch(mod,'ad_err_resolve_'+key,9001)
    assert mod.db.system_errors.find_one({'_id':key})['resolved']
    with mod._error_scope(user_id=1001,username='customer',operation='purchase',product_id='101'):
        assert mod._error_capture('Unavailable supplier')==key
    mod._error_drain();assert not mod.db.system_errors.find_one({'_id':key})['resolved']


def test_monitor_failure_does_not_interrupt_caller_and_memory_fallback_is_visible(mod,monkeypatch):
    key=mod._error_capture('Database is temporarily down')
    original=mod.db.system_errors.update_one
    original_find=mod.db.system_errors.find
    monkeypatch.setattr(mod.db.system_errors,'update_one',Mock(side_effect=RuntimeError('DB unavailable')))
    assert mod._error_drain()==1
    monkeypatch.setattr(mod.db.system_errors,'find',Mock(side_effect=RuntimeError('DB unavailable')))
    assert mod._error_recent()[0]['_id']==key
    monkeypatch.setattr(mod.db.system_errors,'update_one',original)
    monkeypatch.setattr(mod.db.system_errors,'find',original_find)
    queued=mod._ERROR_QUEUE.get_nowait();mod._ERROR_QUEUE.task_done()
    queued['_retry_at']=0;mod._ERROR_QUEUE.put_nowait(queued)
    mod._error_drain()
    assert mod.db.system_errors.find_one({'_id':key})['count']==1


def test_error_text_with_html_metacharacters_has_safe_bounded_format(mod):
    key=mod._error_capture('<>&'*1000)
    mod._error_drain()
    record=mod._error_detail(key)
    rendered=mod._error_notice(record)
    assert len(rendered)<3000 and '<>&' not in rendered and '&lt;' in rendered
