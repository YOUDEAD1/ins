"""Payment copy controls; offline Telegram and in-memory MongoDB only."""
import pytest
from harness import MESSAGES
from test_integration import mod, isolate, server, request


def message(mod,uid=1001):
    return mod.types.Message.de_json({'message_id':9,'date':1,'chat':{'id':uid,'type':'private'},
        'from':{'id':uid,'is_bot':False,'first_name':'Test'},'text':'10'})


def controls():
    return [(text,kw['reply_markup'].to_dict()['inline_keyboard'])
        for cid,text,kw in MESSAGES if kw.get('reply_markup')]


def copies(rows):
    return [b for row in rows for b in row if 'copy_text' in b]


@pytest.mark.parametrize('uid',[1001,1002])
def test_binance_amount_and_pay_id_share_first_row_with_correct_values(mod,uid):
    mod.ask_binance_deposit_amount(message(mod,uid))
    text,rows=controls()[-1]
    pending=mod.db.pending_deposits.find_one({'user_id':uid,'coin':'BINANCE'})
    assert [b['copy_text']['text'] for b in rows[0]]==[f"{pending['unique_amount_usd']:.4f}",'12345678']
    assert 'Binance Pay ID' in rows[0][1]['text']
    assert all(b['text'].startswith('📋 ') for b in rows[0])
    # Telegram's numeric ID remains a separate copy control for the transfer note.
    assert rows[1][0]['copy_text']['text']==str(uid) and 'Telegram' in rows[1][0]['text']
    assert any(b.get('callback_data')==f'binance_check_{uid}' for row in rows for b in row)
    assert any(b.get('callback_data')=='cancel_deposit' for row in rows for b in row)


@pytest.mark.parametrize('uid',[1001,1002])
def test_binance_copy_controls_share_custom_premium_emoji(mod,uid):
    lang='en' if uid==1001 else 'ar'
    mod.db.custom_texts.insert_many([
        {'lang':lang,'key':'dep_btn_copy_amount','value':'<tg-emoji emoji-id="5271604874419647061">📋</tg-emoji> Amount'},
        {'lang':lang,'key':'dep_btn_copy_binance_id','value':'<tg-emoji emoji-id="5271619747891388291">💰</tg-emoji> Binance Pay ID'}])
    mod.ask_binance_deposit_amount(message(mod,uid))
    buttons=copies(controls()[-1][1])
    assert len(buttons)==3
    assert all(b['icon_custom_emoji_id']=='5271604874419647061' for b in buttons)
    assert all('<' not in b['text'] and '📋' not in b['text'] and '💰' not in b['text'] for b in buttons)
    assert buttons[1]['copy_text']['text']=='12345678'


def test_copy_controls_share_custom_regular_emoji(mod):
    mod.db.custom_texts.insert_one({'lang':'en','key':'dep_btn_copy_amount','value':'💎 Copy Amount'})
    mod.ask_binance_deposit_amount(message(mod))
    assert all(b['text'].startswith('💎 ') for b in copies(controls()[-1][1]))


@pytest.mark.parametrize('value',['','Not Set','   '])
def test_unconfigured_binance_never_creates_an_unusable_payment(mod,monkeypatch,value):
    original=mod.get_setting
    monkeypatch.setattr(mod,'get_setting',lambda key,*a,**kw:value if key=='wallet_address' else original(key,*a,**kw))
    mod.ask_binance_deposit_amount(message(mod))
    assert mod.db.pending_deposits.count_documents({'user_id':1001})==0
    assert not any(copies(rows) for text,rows in controls())
    assert any(b.get('callback_data')=='cancel_deposit' for text,rows in controls() for row in rows for b in row)


def test_binance_pay_id_is_trimmed_without_using_telegram_id(mod,monkeypatch):
    original=mod.get_setting
    monkeypatch.setattr(mod,'get_setting',lambda key,*a,**kw:'  90807060  ' if key=='wallet_address' else original(key,*a,**kw))
    mod.ask_binance_deposit_amount(message(mod))
    assert controls()[-1][1][0][1]['copy_text']['text']=='90807060'


@pytest.mark.parametrize('coin,address,unit,amount',[
    ('USDT','TTESTADDRESS','USDT','10.012345'),
    ('USDT_BEP20','0xTESTADDRESS','USDT','10.012345'),
    ('LTC','ltc1TESTADDRESS','LTC','0.10012345'),
    ('TON','EQTESTADDRESS','TON','2.002469')])
@pytest.mark.parametrize('uid',[1001,1002])
def test_crypto_has_one_amount_control_in_native_units(mod,server,monkeypatch,coin,address,unit,amount,uid):
    monkeypatch.setattr(mod,'generate_unique_amount_for_user',lambda *a,**kw:10.012345)
    mod.ask_deposit_amount(message(mod,uid),coin)
    text,rows=controls()[-1]
    buttons=copies(rows)
    assert len(buttons)==2
    assert [b['copy_text']['text'] for b in buttons]==[amount,address]
    assert text.find(amount)>=0 and unit in text
    assert all(b['text'].startswith('📋 ') for b in buttons)
    # The existing website pending-deposit screen shows this same exact quote,
    # including after market-price functions change.
    monkeypatch.setattr(mod,'get_ton_price_usd',lambda:50.)
    monkeypatch.setattr(mod,'get_ltc_price_usd',lambda:1000.)
    st,data,_=request(server,'/webapp/deposit/status',uid=uid)
    assert st==200 and data['pending']['pay_amount']==amount
    assert data['pending']['pay_unit']==unit and data['pending']['address']==address


@pytest.mark.parametrize('coin',['USDT','USDT_BEP20','LTC','TON'])
def test_crypto_copy_pair_uses_same_premium_emoji(mod,coin):
    mod.db.custom_texts.insert_one({'lang':'en','key':'dep_btn_copy_amount',
        'value':'<tg-emoji emoji-id="5271604874419647061">📋</tg-emoji> Copy Amount'})
    mod.ask_deposit_amount(message(mod),coin)
    buttons=copies(controls()[-1][1])
    assert len(buttons)==2
    assert all(b['icon_custom_emoji_id']=='5271604874419647061' for b in buttons)


@pytest.mark.parametrize('coin',['USDT','USDT_BEP20'])
def test_site_created_usdt_deposit_keeps_all_unique_digits(mod,server,monkeypatch,coin):
    monkeypatch.setattr(mod,'generate_unique_amount_for_user',lambda *a,**kw:10.012345)
    st,data,_=request(server,'/webapp/deposit/create',{'method':coin,'amount':10})
    assert st==200
    assert data['pending']['pay_amount']=='10.012345'
    pending=mod.db.pending_deposits.find_one({'user_id':1001,'coin':coin})
    assert float(data['pending']['pay_amount'])==pending['unique_amount_usd']


@pytest.mark.parametrize('coin,price_function',[('LTC','get_ltc_price_usd'),('TON','get_ton_price_usd')])
@pytest.mark.parametrize('rate',[0,None,float('inf')])
def test_failed_rate_never_offers_usd_as_native_coin_amount(mod,monkeypatch,coin,price_function,rate):
    monkeypatch.setattr(mod,price_function,lambda:rate)
    mod.ask_deposit_amount(message(mod),coin)
    assert mod.db.pending_deposits.count_documents({'user_id':1001})==0
    assert not any(copies(rows) for text,rows in controls())
    assert any(b.get('callback_data')=='cancel_deposit' for text,rows in controls() for row in rows for b in row)
