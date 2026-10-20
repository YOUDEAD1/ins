"""Isolated integration harness: all DB writes stay in memory; no external calls."""
import importlib.util
import json
import os
import pathlib
import threading
import types
from unittest.mock import patch
import mongomock
import requests
import pymongo
import telebot
import dotenv
import curl_cffi.requests

ROOT = pathlib.Path(__file__).resolve().parents[1]
APP = ROOT if (ROOT/'bot.py').is_file() else ROOT/'output'
TOKEN = '123456789:LOCAL_TEST_TOKEN_DO_NOT_USE'
MESSAGES = []
client = mongomock.MongoClient()
_loaded_app = None

def offline(*args, **kwargs):
    raise RuntimeError('External network is disabled in the integration harness')

def reply(chat_id, text='', *args, **kwargs):
    MESSAGES.append((chat_id, text, kwargs))
    return telebot.types.Message.de_json({'message_id': len(MESSAGES), 'date': 1,
        'chat': {'id': int(chat_id), 'type': 'private'}, 'from': {'id':123456789,'is_bot':True,'first_name':'Test'},
        'text': str(text)})

def load():
    global _loaded_app
    # All imported session fixtures and the HTTP server must use the same app.
    # Multiple module imports would otherwise install duplicate global hooks.
    if _loaded_app is not None:
        return _loaded_app
    spec = importlib.util.spec_from_file_location('lara_test_bot', APP/'bot.py')
    m = importlib.util.module_from_spec(spec)
    env = {'TOKEN':TOKEN,'OWNER_ID':'9001','OWNER_USER':'local_test_support','MONGO_URI':'mongodb://localhost/test',
           'MONGO_DB_NAME':'lara_integration_test','BINANCE_API_KEY':'','BINANCE_API_SECRET':'','GITHUB_API_KEY':'',
           'WEBAPP_URL':'https://local.example/app/','STARS_RATE':'120'}
    with patch.dict(os.environ, env), patch.object(dotenv,'load_dotenv',return_value=False), \
         patch.object(pymongo,'MongoClient',return_value=client), \
         patch.object(threading.Thread,'start',return_value=None), \
         patch.object(requests.sessions.Session,'request',side_effect=offline), \
         patch.object(curl_cffi.requests,'get',side_effect=offline), \
         patch.object(curl_cffi.requests,'post',side_effect=offline), \
         patch.object(curl_cffi.requests,'delete',side_effect=offline):
        spec.loader.exec_module(m)
    m._API_READY.set()
    m._VC_ORIG['send_message'] = reply
    m.bot.reply_to = lambda msg,text,*a,**kw: reply(msg.chat.id,text,*a,**kw)
    m._VC_ORIG['send_document'] = lambda cid,doc,*a,**kw:reply(cid,'[document]',*a,**kw)
    m._VC_ORIG['answer_callback_query'] = lambda *a,**kw: True
    m._VC_ORIG['delete_message'] = lambda *a,**kw: True
    m._VC_ORIG['edit_message_text'] = lambda text,*a,**kw:reply(kw.get('chat_id',a[0] if a else 9001),text)
    m._VC_ORIG['edit_message_reply_markup'] = lambda *a,**kw: True
    m.bot.get_me = lambda: types.SimpleNamespace(username='lara_local_test_bot',id=123456789)
    m.bot.get_chat = lambda cid: types.SimpleNamespace(id=cid,first_name='Test',username='test_user')
    m.bot.get_chat_member = lambda *a,**kw: types.SimpleNamespace(status='member')
    m.bot.create_invoice_link = lambda **kw: 'https://t.me/$local-test-invoice'
    m._cgpt_display_seats = lambda: 8
    m._cgpt_get_seats_cached = lambda:8
    m._wa_prefetch_emoji = lambda ids:None
    _loaded_app = m
    return m

if __name__=='__main__':
    m=load()
    print('FULL MODULE IMPORT OK',len(m._WA_POST_ROUTES),'web routes',len(m.bot.callback_query_handlers),'callback handlers')
