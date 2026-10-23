"""The standalone manager link works without the retired controller connection."""
from types import SimpleNamespace as NS
from unittest.mock import Mock
import pytest
from test_integration import mod, isolate
from test_wwc_platform import platform, message, SHOP
from harness import MESSAGES


@pytest.fixture
def external(platform, monkeypatch):
    p, client, _, _ = platform
    monkeypatch.setattr(p, 'manager_username', 'fixture_manager_bot')
    monkeypatch.setattr(client, 'enabled', False)
    monkeypatch.setattr(p, 'cipher', None)
    return p, client


def urls():
    return [getattr(button, 'url', '') for _, _, options in MESSAGES
            for row in getattr(options.get('reply_markup'), 'keyboard', []) for button in row]


def test_external_menu_needs_only_username_and_does_not_duplicate(mod, external):
    p, client = external
    markup = mod.types.InlineKeyboardMarkup()
    markup.row(mod.types.InlineKeyboardButton('Products', callback_data='open_shop'))
    markup.row(mod.types.InlineKeyboardButton('Settings', callback_data='open_settings'))
    mod.bot.send_message(1001, 'Home', reply_markup=markup)
    mod.bot.send_message(1001, 'Home', reply_markup=markup)
    links = [getattr(b, 'url', '') for row in markup.keyboard for b in row]
    assert links.count('https://t.me/fixture_manager_bot?start=open') == 1
    client.call.assert_not_called()
    assert mod.db.wwc_drafts.count_documents({}) == 0


@pytest.mark.parametrize(('command', 'target'), [
    ('/createstore', 'open'), ('/openstore', 'open'), ('/myshops', 'shops'),
    ('/stores', 'shops'), ('/shop_sql', 'shops'),
    ('/start wwc_create', 'open'), ('/start wwc_' + SHOP, 'wwc_' + SHOP),
    ('/start wwcsub_' + SHOP, 'wwcsub_' + SHOP),
])
def test_external_commands_do_not_contact_old_controller(external, command, target):
    p, client = external
    p.command(message(command))
    assert 'https://t.me/fixture_manager_bot?start=' + target in urls()
    client.call.assert_not_called()


@pytest.mark.parametrize(('data', 'target'), [
    ('wwc:create', 'open'), ('wwc:list', 'shops'),
    ('wwc:show:' + SHOP, 'wwc_' + SHOP),
    ('wwc:subscribe:' + SHOP, 'wwcsub_' + SHOP),
    ('wwc:sql:' + SHOP, 'wwc_' + SHOP),
    ('wwc:activate:' + SHOP, 'wwc_' + SHOP),
    ('wwc:subscribe', 'shops'),
])
def test_external_callbacks_work_without_legacy_key(external, data, target):
    p, client = external
    p.callback(NS(from_user=NS(id=1001), message=message(), id='fixture', data=data))
    assert 'https://t.me/fixture_manager_bot?start=' + target in urls()
    client.call.assert_not_called()


def test_external_redirect_obeys_ban_and_private_chat(mod, external):
    p, client = external
    mod.db.users.update_one({'user_id':1001}, {'$set':{'is_banned':1}})
    mod._USER_CACHE.clear()
    p.command(message('/createstore'))
    p.callback(NS(from_user=NS(id=1001), message=message(), id='fixture', data='wwc:create'))
    group = message('/createstore', uid=1002)
    group.chat.type = 'group'
    p.command(group)
    p.callback(NS(from_user=NS(id=1002), message=group, id='fixture', data='wwc:create'))
    assert not urls()
    client.call.assert_not_called()


def test_external_link_keeps_existing_wallet_confirmation_handler(platform, monkeypatch):
    p, _, _, _ = platform
    monkeypatch.setattr(p, 'manager_username', 'fixture_manager_bot')
    confirm = Mock()
    monkeypatch.setattr(p, 'wallet_confirm', confirm)
    token = 'a' * 32
    p.callback(NS(from_user=NS(id=1001), message=message(), id='fixture', data='wwc:walletpay:' + token))
    confirm.assert_called_once_with(1001, token)
