"""Resolve actual bot settings without exposing credentials or mutating configuration."""
import builtins
import importlib.util
import json
import os
from pathlib import Path
from unittest.mock import Mock

import pytest
from harness import APP
from test_integration import mod, isolate


@pytest.fixture
def report(mod, monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location('local_report_config', APP / 'referral_report.py')
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    folder = tmp_path / 'app'; folder.mkdir()
    monkeypatch.setattr(m, '__file__', str(folder / 'referral_report.py'))
    monkeypatch.chdir(folder)
    monkeypatch.delenv('MONGO_URI', raising=False)
    monkeypatch.delenv('MONGO_DB_NAME', raising=False)
    return m


def settings(path, db='same_bot_database', uri='mongodb://fake.test/db?appName=TEST_PRIVATE_VALUE'):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'MONGO_URI="{uri}"\nMONGO_DB_NAME="{db}"\n', encoding='utf-8')
    return path


def test_process_settings_remain_authoritative_without_explicit_file(report, monkeypatch):
    settings(Path('.env'), db='file_database')
    monkeypatch.setenv('MONGO_URI', 'mongodb://process.test/db')
    monkeypatch.setenv('MONGO_DB_NAME', 'process_database')
    result = report.resolve_connection()
    assert result['source'] == 'process environment' and result['database'] == 'process_database'
    assert result['uri'] == 'mongodb://process.test/db'


def test_empty_process_uri_does_not_shadow_a_filled_file(report, monkeypatch):
    settings(Path('.env'))
    monkeypatch.setenv('MONGO_URI', '')
    result = report.resolve_connection()
    assert result['source'] == '.env' and result['database'] == 'same_bot_database'


def test_windows_env_txt_is_found_when_env_is_a_blank_template(report):
    Path('.env').write_text('MONGO_URI=""\nMONGO_DB_NAME="shop_db"\n')
    settings(Path('.env.txt'), db='real_bot_db')
    assert report.resolve_connection()['database'] == 'real_bot_db'


def test_utf8_bom_quotes_and_spaces_in_path_are_supported(report, tmp_path):
    p = tmp_path / 'settings with spaces.env'
    p.write_text('\ufeffMONGO_URI="mongodb://fake.test/db"\nMONGO_DB_NAME="custom db"\n', encoding='utf-8')
    result = report.resolve_connection('"' + str(p) + '"')
    assert result['uri'] == 'mongodb://fake.test/db' and result['database'] == 'custom db'


def test_explicit_file_keeps_its_database_and_does_not_use_process_database(report, monkeypatch, tmp_path):
    p = settings(tmp_path / 'selected.env', db='selected_database')
    monkeypatch.setenv('MONGO_URI', 'mongodb://unrelated.test/db')
    monkeypatch.setenv('MONGO_DB_NAME', 'unrelated_database')
    result = report.resolve_connection(p)
    assert result['database'] == 'selected_database' and 'fake.test' in result['uri']


def test_explicit_empty_file_does_not_silently_fall_back_to_another_database(report, monkeypatch, tmp_path):
    p = tmp_path / 'empty.env'; p.write_text('MONGO_URI=""\n')
    monkeypatch.setenv('MONGO_URI', 'mongodb://unrelated.test/db')
    with pytest.raises(ValueError, match='empty or missing'):
        report.resolve_connection(p)


def test_missing_explicit_file_is_reported_without_using_other_settings(report, monkeypatch):
    monkeypatch.setenv('MONGO_URI', 'mongodb://unrelated.test/db')
    with pytest.raises(ValueError, match='does not exist'):
        report.resolve_connection('missing.env')


def test_parent_bot_settings_are_found_without_recursive_device_search(report):
    settings(Path('../.env'), db='parent_bot_database')
    assert report.resolve_connection()['database'] == 'parent_bot_database'


def test_working_directory_settings_are_supported(report, monkeypatch, tmp_path):
    folder = tmp_path / 'working'; folder.mkdir(); settings(folder / '.env')
    monkeypatch.chdir(folder)
    assert report.resolve_connection()['source'] == '.env'


def test_missing_configuration_offers_the_settings_selector(report, monkeypatch, tmp_path):
    selected = settings(tmp_path / 'chosen-settings.env')
    choose = Mock(return_value=str(selected)); monkeypatch.setattr(report, '_choose_connection_file', choose)
    assert report.resolve_connection(allow_prompt=True)['source'] == selected.name
    choose.assert_called_once()


def test_cancelled_selector_does_not_connect_or_create_settings(report, monkeypatch):
    monkeypatch.setattr(report, '_choose_connection_file', lambda: None)
    before = set(Path.cwd().iterdir())
    with pytest.raises(ValueError, match='was not found'):
        report.resolve_connection(allow_prompt=True)
    assert set(Path.cwd().iterdir()) == before


def test_configuration_values_are_never_changed_or_printed(report, monkeypatch, tmp_path, capsys):
    p = settings(tmp_path / 'selected.env')
    original = p.read_bytes(); environment = dict(os.environ)
    result = report.resolve_connection(p)
    assert 'TEST_PRIVATE_VALUE' in result['uri']
    assert p.read_bytes() == original and dict(os.environ) == environment
    assert 'TEST_PRIVATE_VALUE' not in capsys.readouterr().out


def test_bad_uri_error_does_not_expose_its_value(report):
    with pytest.raises(ValueError) as error:
        report._connection_values({'MONGO_URI': 'https://fake.test/TEST_PRIVATE_VALUE'}, 'selected.env')
    assert 'TEST_PRIVATE_VALUE' not in str(error.value)


def test_missing_settings_are_handled_before_asking_for_the_username(report, monkeypatch):
    import pymongo
    connect = Mock(); prompt = Mock(side_effect=AssertionError('No target prompt should occur'))
    monkeypatch.setattr(pymongo, 'MongoClient', connect); monkeypatch.setattr(builtins, 'input', prompt)
    assert report.main(['--no-input']) == 2
    connect.assert_not_called(); prompt.assert_not_called()


def test_selected_settings_produce_the_report_with_readonly_calls_only(report, mod, monkeypatch, tmp_path, capsys):
    import pymongo
    p = settings(tmp_path / 'bot settings.env', db=mod.db.name)
    connect = Mock(return_value=mod.db.client); monkeypatch.setattr(pymongo, 'MongoClient', connect)
    snapshot = {n: list(mod.db[n].find({})) for n in mod.db.list_collection_names()}
    assert report.main(['1001', '--env', str(p), '--no-input']) == 0
    assert connect.call_args.args[0].startswith('mongodb://fake.test/')
    saved = json.loads(Path('referral-report-1001.json').read_text())
    assert saved['read_only'] and saved['account']['balance'] == 100
    assert snapshot == {n: list(mod.db[n].find({})) for n in mod.db.list_collection_names()}
    assert 'TEST_PRIVATE_VALUE' not in capsys.readouterr().out


@pytest.mark.parametrize('kind,delta', [('compensation', -30.), ('manual', .001)])
def test_admin_balance_log_uses_the_atomic_wallet_result_instead_of_a_stale_snapshot(mod, monkeypatch, kind, delta):
    from test_referral_policy import message
    mod.db.users.update_one({'user_id': 1001}, {'$set': {'balance': 10.}})
    assert mod.get_user_data_full(1001)['balance'] == 10
    monkeypatch.setattr(mod, 'show_user_admin_profile', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'vip_after_admin_balance', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'notify_balance_gift', lambda *a, **k: None)
    if kind == 'compensation':
        mod.ad_gift_comp_exec(message(mod, uid=9001, text=str(delta)), 1001)
    else:
        mod.ad_gift_note_exec(message(mod, uid=9001, text='Test manual adjustment'), 1001, delta)
    row = mod.db.balance_logs.find_one({'user_id': 1001, 'type': kind})
    assert row['new_balance'] == 10 + delta and row['old_balance'] == 10
    assert row['balance_source'] == 'atomic_wallet_update' and row['by_admin'] == 9001
    assert mod.get_user_data_full(1001)['balance'] == 10 + delta
