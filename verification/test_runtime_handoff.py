"""Offline deployment coordination: atomic ownership and bounded loss handling."""
import ast
import copy
import importlib.util
import json
from pathlib import Path
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('handoff_under_test', ROOT / 'runtime_handoff.py')
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


class Clock:
    def __init__(self):
        self.now = 0
    def __call__(self):
        return self.now


class AtomicCollection:
    """Tiny atomic server-clock fixture, independent of host clock or MongoDB."""
    def __init__(self, clock):
        self.clock, self.row, self.lock = clock, None, threading.Lock()
    def update_one(self, query, pipeline, upsert):
        with self.lock:
            if self.row is None:
                delay = pipeline[0]['$set']['bootstrap_until']['$ifNull'][1]['$add'][1] / 1000
                self.row = {'_id': query['_id'], 'expires_at': self.clock(),
                            'bootstrap_until': self.clock() + delay}
    def find_one_and_update(self, query, pipeline, return_document):
        with self.lock:
            assert '$expr' in query
            if query['_id'] != self.row['_id']:
                return None
            if 'owner' in query:
                assert query['$expr'] == {'$gt': ['$expires_at', '$$NOW']}
                if self.row.get('owner') != query['owner'] or self.row['expires_at'] <= self.clock():
                    return None
            else:
                assert query['$expr'] == {'$and': [
                    {'$lte': ['$expires_at', '$$NOW']}, {'$lte': ['$bootstrap_until', '$$NOW']}]}
                if self.row['expires_at'] > self.clock() or self.row['bootstrap_until'] > self.clock():
                    return None
            fields = pipeline[0]['$set']
            self.row.update(owner=fields['owner'], expires_at=self.clock() + fields['expires_at']['$add'][1] / 1000)
            return copy.deepcopy(self.row)


def leases(count=2, delay=0):
    clock = Clock()
    collection = AtomicCollection(clock)
    owners = [runtime.RuntimeLease(collection, 'same-bot', clock=clock) for _ in range(count)]
    owners[0].initialize(delay)
    return clock, collection, owners


def test_twenty_competing_instances_start_only_one_owner():
    _, _, owners = leases(20)
    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(lambda lease: lease.acquire(), owners))
    assert sum(results) == 1


def test_new_deploy_waits_for_old_expiry_and_old_cannot_renew_after_handoff():
    clock, _, (old, new) = leases()
    assert old.acquire() and not new.acquire()
    clock.now = 5
    assert old.renew()
    clock.now = 49
    assert not new.acquire()
    clock.now = 50
    assert new.acquire() and not old.renew()
    assert new.safe_to_run() and not old.safe_to_run()


def test_expired_owner_cannot_resurrect_its_lease():
    clock, _, (old, new) = leases()
    assert old.acquire()
    clock.now = runtime.LEASE_SECONDS
    assert not old.renew()
    assert new.acquire()


def test_first_render_migration_wait_is_shared_and_never_reset_by_redeploy():
    clock, collection, (old, new) = leases(delay=120)
    assert not old.acquire()
    clock.now = 119
    new.initialize(120)
    assert collection.row['bootstrap_until'] == 120 and not new.acquire()
    clock.now = 120
    assert new.acquire()


def test_lost_database_confirmation_stops_workers_before_lease_expires():
    clock, collection, (owner, _) = leases()
    assert owner.acquire()
    clock.now = runtime.STOP_AFTER_SECONDS
    stopped = threading.Event()
    codes = []
    def terminate(code):
        codes.append(code)
        stopped.set()
    stop = runtime.start_watchdogs(owner, terminate=terminate)
    try:
        assert stopped.wait(timeout=2)
        assert codes == [1] and clock.now < collection.row['expires_at']
    finally:
        stop.set()


def test_waiter_never_proceeds_on_database_failure_or_busy_owner():
    attempts, sleeps = [], []
    class Lease:
        def acquire(self):
            attempts.append(1)
            if len(attempts) == 1:
                raise OSError('fixture outage')
            return len(attempts) == 3
    runtime.wait_for_ownership(Lease(), sleep=sleeps.append)
    assert len(attempts) == 3 and sleeps == [2, 2]


def test_waiting_instance_serves_health_but_cannot_accept_financial_requests():
    health = runtime.WaitingHealth(0, host='127.0.0.1')
    health.start()
    base = f'http://127.0.0.1:{health.server.server_port}'
    try:
        with urllib.request.urlopen(base + '/', timeout=2) as response:
            assert response.status == 200
            assert json.load(response)['bot_active'] is False
        # Render may use a configured health-check path instead of '/'. It must
        # observe this waiting instance so it can terminate the previous one.
        with urllib.request.urlopen(base + '/custom-health-path', timeout=2) as response:
            assert json.load(response)['bot_active'] is False
        for path, data in [('/webapp/buy', b'{}'), ('/webapp/deposit/check', b'{}')]:
            with pytest.raises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(urllib.request.Request(base + path, data=data), timeout=2)
            assert error.value.code == 503
    finally:
        health.close()


def test_token_rotation_keeps_the_same_bot_lock_and_distinct_bots_are_independent():
    assert runtime.lease_key('123:old-secret') == runtime.lease_key('123:new-secret')
    assert runtime.lease_key('123:secret') != runtime.lease_key('456:secret')
    assert 'secret' not in runtime.lease_key('123:secret')


def test_runtime_gate_runs_before_telegram_imports_and_all_background_workers():
    tree = ast.parse((ROOT / 'bot.py').read_text(encoding='utf-8'))
    gate = next(n for n in tree.body if isinstance(n, ast.If) and any(
        isinstance(x, ast.Call) and isinstance(x.func, ast.Name) and x.func.id == 'acquire_runtime'
        for x in ast.walk(n)))
    assert '__main__' in ast.unparse(gate.test)
    telegram_import = next(n for n in ast.walk(tree) if isinstance(n, ast.Import) and any(a.name == 'telebot' for a in n.names))
    assert gate.lineno < telegram_import.lineno
    assert all(gate.lineno < n.lineno for n in ast.walk(tree) if isinstance(n, ast.Call)
               and isinstance(n.func, ast.Attribute) and n.func.attr == 'start')


def test_complete_render_bootstrap_waits_for_migration_then_starts_watchdogs(monkeypatch):
    server_clock = Clock()
    collection = AtomicCollection(server_clock)
    client = Mock()
    client.__getitem__ = Mock(return_value=Mock(get_collection=Mock(return_value=collection)))
    monkeypatch.setattr(runtime, 'MongoClient', Mock(return_value=client))
    monkeypatch.setattr(runtime, 'load_dotenv', lambda: None)
    monkeypatch.setenv('TOKEN', '123:fixture-secret')
    monkeypatch.setenv('MONGO_URI', 'mongodb://fixture.invalid/test')
    monkeypatch.setenv('RENDER', 'true')
    monkeypatch.setenv('PORT', '0')
    monkeypatch.delenv('LARA_INITIAL_HANDOFF_SECONDS', raising=False)
    original = runtime.wait_for_ownership
    def skip_wall_time(lease):
        def sleep(seconds):
            server_clock.now += seconds
        original(lease, sleep=sleep)
    monkeypatch.setattr(runtime, 'wait_for_ownership', skip_wall_time)
    lease = runtime.acquire_runtime()
    try:
        assert server_clock.now == 120
        assert collection.row['owner'] == lease.owner
        assert lease.safe_to_run() and lease.client is client
        client.admin.command.assert_called_once_with('ping')
    finally:
        lease.stop_event.set()
