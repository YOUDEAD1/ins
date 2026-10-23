"""Store copy uses the original NSS CMS, handlers and collections."""
from types import SimpleNamespace as NS
from unittest.mock import Mock
import pytest
from test_wwc_platform import mod, isolate, platform, message, SHOP
from harness import MESSAGES, reply

@pytest.fixture
def native(mod,platform,monkeypatch):
    p=platform[0]
    monkeypatch.setattr(p,'customization_migrated',False)
    monkeypatch.setattr(mod.bot,'register_next_step_handler',Mock())
    monkeypatch.setattr(mod.bot,'register_next_step_handler_by_chat_id',Mock())
    monkeypatch.setitem(mod._VC_ORIG,'edit_message_text',lambda text,*args,**kw:
        reply(kw.get('chat_id',args[0] if args else 9001),text,**kw))
    monkeypatch.setattr(mod,'safe_translate_for_cms',Mock(side_effect=AssertionError('unexpected translation')))
    return p

def click(mod,data,uid=9001):
    routes={'ad_texts_main':'ad_texts_main_ui','ad_cms_msgs':'ad_cms_msgs_ui',
            'ad_cms_btns_cats':'ad_cms_btns_cats_ui'}
    name=routes.get(data)
    if name is None:
        name=next(name for prefix,name in (
            ('ad_cms_wwc_','ad_cms_wwc_texts'),('ad_cms_b_','ad_cms_btns_list'),
            ('edit_txt_','ad_edit_txt_prompt'),('edit_btn_','ad_edit_btn_prompt'),
            ('rst_txt_','ad_reset_text_to_default'),('btnstyle_','ad_set_btn_style')) if data.startswith(prefix))
    call=mod.types.CallbackQuery.de_json({'id':'native-cms-fixture','data':data,
        'chat_instance':'native-cms-test','from':{'id':uid,'is_bot':False,'first_name':'Test'},
        'message':{'message_id':7,'date':1,'chat':{'id':uid,'type':'private'},'text':'CMS'}})
    handler=next(h for h in mod.bot.callback_query_handlers if h['function'].__name__==name)
    assert handler['filters']['func'](call)
    mod.bot._run_middlewares_and_handler(call,handlers=mod.bot.callback_query_handlers,
                                        middlewares=None,update_type='callback_query')

def buttons():
    return [b for row in MESSAGES[-1][2]['reply_markup'].keyboard for b in row]

def test_existing_cms_contains_all_defaults_without_a_second_editor(mod,native):
    defaults=type(native).text.__wrapped__.__globals__['TEXT']
    assert set(mod.WWC_CMS_TEXTS)=={'wwc.'+key for key in defaults['en']}
    assert len(mod.WWC_CMS_BUTTONS)==18
    assert 'wwc.manager_redirect' in mod.WWC_CMS_TEXTS
    for lang,rows in defaults.items():
        for key,value in rows.items():
            nk='wwc.'+key
            assert mod.LANG[lang][nk]==value
            mod.WWC_CMS_VALIDATE(value,defaults['en'][key],button=nk in mod.WWC_CMS_BUTTONS,key=nk)
            if nk in mod.WWC_CMS_BUTTONS:assert mod.DEFAULT_BUTTONS[lang][nk]==value
    assert not hasattr(native,'cms')
    assert not any(h['filters'].get('commands')==['storetexts'] for h in mod.bot.message_handlers)

def test_native_folders_use_native_edit_callbacks(mod,native):
    click(mod,'ad_texts_main')
    assert {'ad_cms_msgs','ad_cms_btns_cats','ad_cms_cgpt','ad_cms_shop','ad_cms_wwc_0'}<={b.callback_data for b in buttons()}
    click(mod,'ad_cms_msgs')
    assert {'edit_txt_welcome','edit_txt_dep_msg_binance','ad_cms_wwc_0'}<={b.callback_data for b in buttons()}
    keys=set()
    for page in range(5):
        click(mod,'ad_cms_wwc_'+str(page))
        keys|={b.callback_data[9:] for b in buttons() if b.callback_data.startswith('edit_txt_')}
    assert keys==set(mod.WWC_CMS_TEXTS)-set(mod.WWC_CMS_BUTTONS)
    click(mod,'ad_cms_btns_cats')
    assert 'ad_cms_b_wwc' in {b.callback_data for b in buttons()}
    click(mod,'ad_cms_b_wwc')
    assert {b.callback_data[9:] for b in buttons() if b.callback_data.startswith('edit_btn_')}==set(mod.WWC_CMS_BUTTONS)

def test_prompts_register_original_handlers(mod,native):
    click(mod,'edit_txt_wwc.error:en')
    assert mod.bot.register_next_step_handler.call_args.args[1:]==(mod.ad_save_custom_text,'wwc.error','en')
    click(mod,'edit_btn_wwc.open:zh')
    assert mod.bot.register_next_step_handler.call_args.args[1:]==(mod.ad_save_custom_btn,'wwc.open','zh')

@pytest.mark.parametrize('lang',['ar','en','vi','zh'])
def test_native_text_save_changes_only_selected_language_and_live_message(mod,native,lang):
    mod.db.users.update_one({'user_id':1001},{'$set':{'lang':lang}});mod._USER_CACHE.clear()
    value='<b>Custom</b>: {reason}'
    mod.ad_save_custom_text(message(value,uid=9001,entities=[]),'wwc.error',lang)
    assert mod.db.custom_texts.find_one({'lang':lang,'key':'wwc.error'})['value']==value
    assert mod.db.custom_texts.count_documents({'key':'wwc.error'})==1
    native.error(1001,RuntimeError('TEST_REASON'))
    assert MESSAGES[-1][1]=='<b>Custom</b>: TEST_REASON'
    assert MESSAGES[-1][2]['parse_mode']=='HTML'
    mod.safe_translate_for_cms.assert_not_called()

@pytest.mark.parametrize('bad',['No reason','Error {reason.__class__}','<script>{reason}</script>','<b>{reason}','{reason!r}','{reason:>20}'])
def test_native_save_rejects_invalid_templates(mod,native,bad):
    mod.ad_save_custom_text(message(bad,uid=9001,entities=[]),'wwc.error','en')
    assert not mod.db.custom_texts.find_one({'key':'wwc.error'})
    assert mod.bot.register_next_step_handler_by_chat_id.call_args.args[1:]==(mod.ad_save_custom_text,'wwc.error','en')

@pytest.mark.parametrize('lang',['ar','en','vi','zh'])
def test_native_premium_color_and_language_reset(mod,native,lang):
    mod.db.users.update_one({'user_id':1001},{'$set':{'lang':lang,'lang_chosen':True}});mod._USER_CACHE.clear()
    entity=NS(type='custom_emoji',offset=3,length=2,custom_emoji_id='5375338737028841420')
    mod.ad_save_custom_btn(message('🚀 🏪 My store',uid=9001,entities=[entity]),'wwc.open',lang)
    controls={b.callback_data for b in buttons() if b.callback_data.startswith('btnstyle_')}
    assert controls=={f'btnstyle_wwc.open_{color}:{lang}' for color in ('default','primary','success','danger')}
    click(mod,'btnstyle_wwc.open_success:'+lang)
    data=native.button(1001,'open','wwc:create').to_dict()
    assert data['text'].replace('  ',' ')=='🚀 My store' and data['style']=='success'
    assert data['icon_custom_emoji_id']==entity.custom_emoji_id
    assert mod.db.custom_buttons.count_documents({'key':'wwc.open'})==1
    click(mod,'rst_txt_wwc.open:'+lang)
    assert not mod.db.custom_buttons.find_one({'key':'wwc.open'})
    assert native.text(1001)['open']==mod.DEFAULT_BUTTONS[lang]['wwc.open']
    assert native.button(1001,'open','wwc:create').to_dict().get('icon_custom_emoji_id') is None

def test_premium_only_edit_retains_selected_language(mod,native):
    entity=NS(type='custom_emoji',offset=0,length=2,custom_emoji_id='5375338737028841420')
    mod.ad_save_custom_btn(message('🏪',uid=9001,entities=[entity]),'wwc.open','en')
    label=mod.db.custom_buttons.find_one({'key':'wwc.open','lang':'en'})['text']
    assert 'store' in label and 'متجر' not in label

def test_regular_emoji_and_native_premium_settings_reach_actual_start_menu(mod,native,monkeypatch):
    mod.ad_save_custom_btn(message('✅ My stores',uid=9001,entities=[]),'wwc.mine','en')
    entity=NS(type='custom_emoji',offset=0,length=2,custom_emoji_id='5375338737028841420')
    mod.ad_save_custom_btn(message('🏪 Create here',uid=9001,entities=[entity]),'wwc.open','en')
    click(mod,'btnstyle_wwc.open_success:en')
    mod.db.users.update_one({'user_id':1001},{'$set':{'lang_chosen':True}});mod._USER_CACHE.clear()
    monkeypatch.setattr(mod,'check_forced_sub',lambda *a,**kw:True)
    mod.start_handler(message('/start'))
    keys=[b for _,_,kw in MESSAGES if kw.get('reply_markup') for row in kw['reply_markup'].keyboard for b in row]
    create=next(b for b in reversed(keys) if b.callback_data=='wwc:create')
    mine=next(b for b in reversed(keys) if b.callback_data=='wwc:list')
    assert create.text=='Create here' and create.to_dict()['icon_custom_emoji_id']==entity.custom_emoji_id
    assert create.to_dict()['style']=='success' and mine.text=='✅ My stores'

def test_wallet_price_template_and_live_button(mod,native):
    mod.ad_save_custom_btn(message('Pay now',uid=9001,entities=[]),'wwc.wallet_pay','en')
    assert not mod.db.custom_buttons.find_one({'key':'wwc.wallet_pay'})
    mod.ad_save_custom_btn(message('Pay ${amount}',uid=9001,entities=[]),'wwc.wallet_pay','en')
    click(mod,'btnstyle_wwc.wallet_pay_success:en')
    native.subscribe(1001,SHOP)
    pay=next(b for b in buttons() if (b.callback_data or '').startswith('wwc:walletpay:'))
    assert pay.text=='Pay $5.00' and pay.to_dict()['style']=='success'
    assert mod.db.users.find_one({'user_id':1001})['balance']==100

def test_native_settings_persist_without_parallel_collections(mod,native):
    mod.ad_save_custom_text(message('Unavailable: {reason}',uid=9001,entities=[]),'wwc.error','en')
    restarted=type(native)(native.g)
    mod._invalidate_custom_texts_cache()
    assert restarted.text(1001)['error']=='Unavailable: {reason}'
    assert not mod.db.wwc_cms_drafts.find_one({}) and not mod.db.wwc_cms_buttons.find_one({})

def test_legacy_values_migrate_once_and_do_not_resurrect_after_reset(mod,native):
    mod.db.custom_texts.insert_one({'lang':'en','key':'wwc.open','value':'Old saved store'})
    mod.db.wwc_cms_buttons.insert_one({'lang':'en','key':'open','emoji_id':'5375338737028841420','style':'success'})
    native.migrate_customization()
    data=native.button(1001,'open','wwc:create').to_dict()
    assert data['text']=='Old saved store' and data['style']=='success' and data['icon_custom_emoji_id']=='5375338737028841420'
    mod.ad_save_custom_btn(message('Native saved store',uid=9001,entities=[]),'wwc.open','en')
    assert type(native)(native.g).text(1001)['open']=='Native saved store'
    click(mod,'rst_txt_wwc.open:en')
    assert type(native)(native.g).text(1001)['open']==mod.DEFAULT_BUTTONS['en']['wwc.open']

def test_customers_cannot_change_copy_or_colors(mod,native):
    click(mod,'edit_txt_wwc.error:en',uid=1001)
    click(mod,'btnstyle_wwc.open_danger:en',uid=1001)
    mod.ad_save_custom_text(message('Bad {reason}',uid=1001,entities=[]),'wwc.error','en')
    mod.ad_save_custom_btn(message('Bad',uid=1001,entities=[]),'wwc.open','en')
    assert not mod.db.custom_texts.find_one({}) and not mod.db.custom_buttons.find_one({})

@pytest.mark.parametrize('route',['ad_texts_main','ad_cms_msgs','ad_cms_btns_cats','ad_cms_b_wwc','ad_cms_wwc_0'])
def test_native_back_clears_pending_entry(mod,native,route):
    click(mod,'edit_txt_wwc.error:en')
    mod.bot.clear_step_handler_by_chat_id.reset_mock()
    click(mod,route)
    mod.bot.clear_step_handler_by_chat_id.assert_called_with(chat_id=9001)

def test_one_native_button_snapshot_per_dashboard(mod,native,monkeypatch):
    native.migrate_customization();native.text(1001)
    find=Mock(wraps=mod.db.custom_buttons.find)
    monkeypatch.setattr(mod.db.custom_buttons,'find',find)
    monkeypatch.setattr(mod.db.custom_buttons,'find_one',Mock(side_effect=AssertionError('per-button query')))
    monkeypatch.setattr(mod.db.custom_texts,'find',Mock(side_effect=AssertionError('uncached copy')))
    native.dashboard(1001,SHOP)
    assert find.call_count==1

def test_long_html_retains_content_and_only_last_keyboard(mod,native):
    native.send(9001,'<b>'+'line &amp; 🏪\n'*600+'</b>',[[('Back','wwc:list')]])
    chunks=[(body,kw) for uid,body,kw in MESSAGES if uid==9001]
    parser_type=mod.WWC_CMS_VALIDATE.__globals__['TemplateHTML']
    for body,kw in chunks:
        parser=parser_type();parser.feed(body);parser.close();assert not parser.stack
        assert len(body.encode('utf-16-le'))//2<=3500
    assert sum(body.count('line') for body,_ in chunks)==600
    assert chunks[-1][1].get('reply_markup')
    assert not any(kw.get('reply_markup') for _,kw in chunks[:-1])
