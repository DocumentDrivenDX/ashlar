import hashlib
import json
import unittest
from types import SimpleNamespace

from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64, ROLES
from ashlar_host.managed_current_genesis import (
    MANIFEST_PROFILE, PROGRESS_PROFILE, ManagedCurrentGenesisManifestOwner,
    admit_truss_source_genesis_progress,
)
from ashlar_host.r04_schedule import R04Schedule, PROFILE, SLOT_ORDER, STAGES


def _schedule():
    slots = []
    for n, (name, stage) in enumerate(zip(SLOT_ORDER, STAGES)):
        source = name[0]
        slots.append({'ordinal': n, 'slot': name, 'source': source, 'stage': stage,
            'entity_ref': f'entity:{source}',
            'predecessor_slot': n - 2 if stage in ('update', 'delete') else None,
            'mutation': {'stage': stage}, 'expected_transition': {'prefix': n + 1}})
    return R04Schedule.from_document({'profile': PROFILE,
        'source_bindings': {source: {'feed': f'feed-{source}', 'epoch': f'epoch-{source}',
            'consumer_id': f'consumer-{source}', 'registration_id': f'consumer-reg-{source}',
            'installation_id': f'install-{source}'} for source in ('A', 'B')},
        'accepted_model_pins': {'A': {'selected': 'model-A'}, 'B': {'selected': 'model-B'}},
        'slots': slots, 'prefix_oracle': [{'prefix': n} for n in range(9)]})


def _progress(source):
    cursor = json.dumps({'profile': 'truss-caller-asserted-feed/0.1', 'nextXid': '0',
        'installationId': f'install-{source}', 'sourceEpoch': f'epoch-{source}',
        'consumerId': f'consumer-{source}', 'registrationId': f'consumer-reg-{source}',
        'x-extension': 'preserved'}, separators=(',', ':')).encode()
    return admit_truss_source_genesis_progress(feed=f'feed-{source}', epoch=f'epoch-{source}',
        cursor_bytes=cursor, cursor_sha256=hashlib.sha256(cursor).hexdigest(),
        catalog_registration_id=f'catalog-reg-{source}',
        catalog_registration_bytes=f'original registration {source}'.encode(),
        consumer_id=f'consumer-{source}', consumer_registration_id=f'consumer-reg-{source}',
        installation_id=f'install-{source}')


class _Executor:
    sql_table = '`catalog`.`schema`.`publication_manifest`'
    def __init__(self): self.rows = []; self.queries = []
    def query(self, sql, parameters):
        self.queries.append((sql, parameters))
        if 'ORDER BY recorded_at DESC' in sql: return SimpleNamespace(rows=self.rows[-2:][::-1])
        if 'WHERE publication_id=:id' in sql:
            return SimpleNamespace(rows=[r for r in self.rows if r['publication_id'] == parameters['id']])
        raise AssertionError(sql)


class _Store:
    def __init__(self, executor): self.executor = executor; self.sql_table = executor.sql_table; self.commits = 0
    def commit(self, row, *, context): self.commits += 1; self.executor.rows.append(dict(row))


class _Targets:
    def __init__(self):
        self.created_fresh = True
        self.carriers = SimpleNamespace(physical_profile=CARRIER_PROFILE_XXHASH64)
        self.names = {role: ('catalog', 'schema', role) for role in ROLES}
        self.tables = {role: '.'.join(name) for role, name in self.names.items()}
        self.inventory = {}
        for role in ROLES:
            self.inventory[role] = SimpleNamespace(name=self.tables[role], uuid=f'uuid-{role}',
                version=0, schema=(("field", "string", False),), protocol={'profile': 'test'},
                detail_digest='d' * 64)
        self.opening = dict(self.inventory)
        self.transport = SimpleNamespace(query=lambda sql, params: SimpleNamespace(rows=[]))
    def observe(self): return dict(self.inventory)


class TestManagedCurrentGenesis(unittest.TestCase):
    def test_genesis_progress_is_distinct_from_page_checkpoint(self):
        item = _progress('A')
        value = item.document()
        self.assertEqual(value['profile'], PROGRESS_PROFILE)
        self.assertEqual(value['page'], {'profile': 'ashlar-source-page-absent/0.1'})
        self.assertNotIn('previous', value)
        self.assertNotIn('payload_digest', value)
        self.assertNotIn('batch_id', value)
        self.assertNotEqual(item.catalog_registration_id, item.consumer_registration_id)

    def test_cursor_must_be_exact_and_remain_at_genesis(self):
        cursor = json.dumps({'profile': 'truss-caller-asserted-feed/0.1', 'nextXid': '1',
            'installationId': 'install-A', 'sourceEpoch': 'epoch-A',
            'consumerId': 'consumer-A', 'registrationId': 'consumer-reg-A'}).encode()
        with self.assertRaises(ValueError):
            admit_truss_source_genesis_progress(feed='feed-A', epoch='epoch-A',
                cursor_bytes=cursor, cursor_sha256=hashlib.sha256(cursor).hexdigest(),
                catalog_registration_id='catalog-reg-A', catalog_registration_bytes=b'registration',
                consumer_id='consumer-A', consumer_registration_id='consumer-reg-A',
                installation_id='install-A')

    def test_owner_writes_once_and_reopens_exact_row(self):
        executor = _Executor(); store = _Store(executor); targets = _Targets()
        owner = ManagedCurrentGenesisManifestOwner(targets, store)
        args = {'schedule_bytes': _schedule().raw,
            'source_progress': {'A': _progress('A'), 'B': _progress('B')},
            'selected_model_evidence': {'A': b'model evidence A', 'B': b'model evidence B'},
            'schema_revisions': {'model-A': 'sha-a', 'model-B': 'sha-b'},
            'recorded_at': '1762000000000000', 'context': object()}
        row = owner.initialize(**args)
        replay = owner.initialize(**args)
        self.assertEqual(row, replay)
        self.assertEqual(store.commits, 1)
        report = json.loads(row['validation_report_json'])
        self.assertEqual(report['publication_plan_profile'], MANIFEST_PROFILE)
        self.assertFalse(report['source_page_accepted'])
        self.assertFalse(report['source_acknowledged'])
        self.assertEqual(len(report['six_role_inventory']), 6)
        self.assertTrue(all(w['row_count'] == 0 for w in report['empty_data_role_witnesses'].values()))

    def test_owner_refuses_reopened_or_nonempty_target_and_other_manifest(self):
        schedule = _schedule().raw
        args = {'schedule_bytes': schedule,
            'source_progress': {'A': _progress('A'), 'B': _progress('B')},
            'selected_model_evidence': {'A': b'model evidence A', 'B': b'model evidence B'},
            'schema_revisions': {'model-A': 'sha-a', 'model-B': 'sha-b'},
            'recorded_at': '1762000000000000', 'context': object()}
        targets = _Targets(); targets.created_fresh = False
        with self.assertRaises(ValueError):
            ManagedCurrentGenesisManifestOwner(targets, _Store(_Executor())).initialize(**args)
        targets = _Targets(); targets.transport = SimpleNamespace(query=lambda sql, params:
            SimpleNamespace(rows=[{'unexpected': 'data'}]))
        with self.assertRaises(ValueError):
            ManagedCurrentGenesisManifestOwner(targets, _Store(_Executor())).initialize(**args)
        executor = _Executor(); executor.rows.append({'publication_id': 'other-manifest'})
        with self.assertRaises(ValueError):
            ManagedCurrentGenesisManifestOwner(_Targets(), _Store(executor)).initialize(**args)


if __name__ == '__main__': unittest.main()
