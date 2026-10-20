"""CMS partial-translation and useful, redacted browser error regressions."""
import json
from unittest.mock import Mock
import pytest
from test_integration import mod,isolate,server,request
from test_error_monitor import empty_monitor
from harness import MESSAGES

def message(mod,text):
    return mod.types.Message.de_json({'message_id':1,'date':1,'chat':{'id':9001,'type':'private'},
        'from':{'id':9001,'is_bot':False,'first_name':'Owner'},'text':text})

@pytest.mark.parametrize('text',['Search، type،','Search؟','Text ١٢٣؛','Search \u061c',
    '<code>مثال</code> Search','<tg-emoji emoji-id="123">ب</tg-emoji> Search',
    'Search {اسم}','Search https://example.test/بحث'])
def test_only_unprotected_arabic_letters_count_as_translation_residue(mod,text):
    assert mod._cms_arabic_count(text)==0

def test_real_untranslated_words_are_detected(mod):
    assert mod._cms_arabic_count('Search عن')==2

def test_arabic_commas_in_english_do_not_retry_or_create_error(mod,monkeypatch):
    retry=Mock(side_effect=AssertionError('Punctuation must not trigger a translation retry'))
    monkeypatch.setattr(mod,'safe_translate_for_cms',lambda text,target:'Search، type a product،')
    monkeypatch.setattr(mod,'_translate_single_line',retry)
    mod.ad_save_custom_text(message(mod,'ابحث عن اسم المنتج الذي تريد شراءه'),'shop_search_prompt')
    mod._error_drain()
    assert not retry.called and mod.db.system_errors.count_documents({})==0
    assert mod.db.custom_texts.find_one({'lang':'en','key':'shop_search_prompt'})['value']=='Search، type a product،'

def test_protected_arabic_emoji_and_code_do_not_trigger_failed_translation(mod,monkeypatch):
    source='ابحث عن المنتج <code>مثال</code> <tg-emoji emoji-id="123">ب</tg-emoji>'
    translated='Search <code>مثال</code> <tg-emoji emoji-id="123">ب</tg-emoji>'
    monkeypatch.setattr(mod,'safe_translate_for_cms',lambda *a:translated)
    mod.ad_save_custom_text(message(mod,source),'shop_search_prompt')
    mod._error_drain()
    assert mod.db.system_errors.count_documents({})==0
    assert mod.db.custom_texts.find_one({'lang':'en','key':'shop_search_prompt'})['value']==translated

def test_successful_line_recovery_is_not_an_open_warning(mod,monkeypatch):
    monkeypatch.setattr(mod,'safe_translate_for_cms',lambda *a:'Search عن')
    retry=Mock(return_value='Search products');monkeypatch.setattr(mod,'_translate_single_line',retry)
    mod.ad_save_custom_text(message(mod,'ابحث عن اسم المنتج الذي تريد شراءه'),'shop_search_prompt')
    mod._error_drain()
    assert retry.call_count==1 and mod.db.system_errors.count_documents({})==0
    assert mod.db.custom_texts.find_one({'lang':'en','key':'shop_search_prompt'})['value']=='Search products'

def test_failed_retry_keeps_existing_english_and_reports_real_warning(mod,monkeypatch):
    mod.db.custom_texts.insert_one({'lang':'en','key':'shop_search_prompt','value':'Previous safe English'})
    monkeypatch.setattr(mod,'safe_translate_for_cms',lambda *a:'Search عن')
    monkeypatch.setattr(mod,'_translate_single_line',lambda line,*a:line)
    text='ابحث عن اسم المنتج الذي تريد شراءه'
    mod.ad_save_custom_text(message(mod,text),'shop_search_prompt')
    assert mod.db.custom_texts.find_one({'lang':'en','key':'shop_search_prompt'})['value']=='Previous safe English'
    assert mod.db.custom_texts.find_one({'lang':'ar','key':'shop_search_prompt'})['value']==text
    mod._error_drain();rec=mod.db.system_errors.find_one({})
    assert rec['level']=='WARNING' and 'remains incomplete' in rec['cause']
    assert 'تحذير' in mod._error_notice(rec)

def test_invalid_previous_translation_uses_valid_language_default(mod,monkeypatch):
    mod.db.custom_texts.insert_one({'lang':'en','key':'shop_search_prompt','value':'Untranslated عن'})
    monkeypatch.setattr(mod,'safe_translate_for_cms',lambda *a:'Untranslated عن')
    monkeypatch.setattr(mod,'_translate_single_line',lambda line,*a:line)
    mod.ad_save_custom_text(message(mod,'ابحث عن اسم المنتج الذي تريد شراءه'),'shop_search_prompt')
    saved=mod.db.custom_texts.find_one({'lang':'en','key':'shop_search_prompt'})['value']
    assert saved==mod.LANG['en']['shop_search_prompt'] and mod._cms_arabic_count(saved)==0

def test_browser_stack_and_diagnostics_are_saved_with_authentication_and_redaction(mod,server):
    body={'message':'ReferenceError: catalogProof is not defined','operation':'runtime',
        'location':'https://local.example/app/index.html:123:45','build':'lara-27.1',
        'platform':'android','sdk_version':'9.1','last_action':'up-record',
        'user_agent':'TestBrowser','stack':'ReferenceError\n at proof (https://local.example/app/app.js?token=PRIVATE_QUERY)\npassword=PRIVATE_PASSWORD',
        'user_id':9001,'cookie':'NOT-A-DIAGNOSTIC','form_values':'DO-NOT-STORE'}
    s,r,_=request(server,'/webapp/client/error',body)
    assert s==200 and r['reference']
    mod._error_drain();rec=mod.db.system_errors.find_one({'_id':r['reference']})
    assert rec['user_id']==1001 and rec['operation']=='browser:runtime'
    assert 'build: lara-27.1' in rec['trace'] and 'last_action: up-record' in rec['trace']
    raw=json.dumps(rec,default=str)
    assert all(value not in raw for value in ['PRIVATE_QUERY','PRIVATE_PASSWORD','NOT-A-DIAGNOSTIC','DO-NOT-STORE'])

def test_masked_script_event_is_explicit_not_a_fabricated_source(mod,server):
    s,r,_=request(server,'/webapp/client/error',{'message':'Script error.','operation':'runtime-masked','location':'0','masked':True})
    assert s==200
    mod._error_drain();rec=mod.db.system_errors.find_one({'_id':r['reference']})
    assert rec['location']=='Browser location unavailable' and 'cannot be identified' in rec['trace']
    assert 'telegram-web-app.js' not in rec['trace']

def test_browser_stack_objects_are_not_serialized_as_diagnostic_content(mod,server):
    s,r,_=request(server,'/webapp/client/error',{'message':'Test','stack':{'password':'DONT_SERIALIZE'},'platform':{'cookie':'DONT_SERIALIZE'}})
    assert s==200
    mod._error_drain();rec=mod.db.system_errors.find_one({'_id':r['reference']})
    assert 'DONT_SERIALIZE' not in json.dumps(rec,default=str)

