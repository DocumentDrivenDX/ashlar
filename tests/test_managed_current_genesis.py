import hashlib
import json
import unittest
from types import SimpleNamespace

from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64, ROLES
from ashlar_host.managed_current_genesis import (
    MANIFEST_PROFILE, PROGRESS_PROFILE, ManagedCurrentGenesisManifestOwner,
    ManagedCurrentGenesisPredecessor,
    admit_truss_source_genesis_progress, verify_managed_current_genesis_predecessor,
    transition_managed_current_first_a_page,
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
        'accepted_model_pins': {
            'A': {'documentRevision': 'model-A', 'modelSha256': hashlib.sha256(b'model evidence A').hexdigest()},
            'B': {'documentRevision': 'model-B', 'modelSha256': hashlib.sha256(b'model evidence B').hexdigest()}},
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


def _model_evidence(source):
    raw = f'model evidence {source}'.encode()
    return {'pin': {'documentRevision': f'model-{source}',
                    'modelSha256': hashlib.sha256(raw).hexdigest()}, 'bytes': raw}


def _catalog_acceptance():
    result = {}
    for source, revision in (('A', '1'), ('B', '2')):
        installation_id = f'install-{source}'
        result[source] = {
            'installation_id': installation_id,
            'revision': revision,
            'report_bytes': json.dumps({'installationId': installation_id,
                                        'revision': revision}, separators=(',', ':')).encode(),
            'catalog_bytes': json.dumps({'catalogRevision': revision},
                                        separators=(',', ':')).encode(),
        }
    return result


def _initialize_args():
    return {'schedule_bytes': _schedule().raw,
        'source_progress': {'A': _progress('A'), 'B': _progress('B')},
        'selected_model_evidence': {'A': _model_evidence('A'), 'B': _model_evidence('B')},
        'catalog_acceptance_revisions': _catalog_acceptance(),
        'schema_revisions': {'install-A': '1', 'install-B': '2'},
        'recorded_at': '1762000000000000', 'context': object()}


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

    def test_owner_writes_one_genesis_row(self):
        executor = _Executor(); store = _Store(executor); targets = _Targets()
        owner = ManagedCurrentGenesisManifestOwner(targets, store)
        args = _initialize_args()
        row = owner.initialize(**args)
        self.assertEqual(store.commits, 1)
        report = json.loads(row['validation_report_json'])
        self.assertEqual(report['publication_plan_profile'], MANIFEST_PROFILE)
        self.assertFalse(report['source_page_accepted'])
        self.assertFalse(report['source_acknowledged'])
        self.assertEqual(len(report['six_role_inventory']), 6)
        self.assertEqual(report['fresh_target_creation']['initial_role_heads'], {role: 0 for role in ROLES})
        self.assertTrue(all(w['row_count'] == 0 for w in report['empty_data_role_witnesses'].values()))
        self.assertEqual(json.loads(row['schema_revisions_json']),
                         {'install-A': '1', 'install-B': '2'})
        self.assertEqual(report['catalog_acceptance_revisions']['A']['revision'], '1')

    def test_owner_refuses_reopened_or_nonempty_target_and_other_manifest(self):
        args = _initialize_args()
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

    def test_open_existing_reopens_exact_row_without_commit(self):
        executor = _Executor(); store = _Store(executor); targets = _Targets()
        args = _initialize_args()
        first = ManagedCurrentGenesisManifestOwner(targets, store).initialize(**args)
        targets.inventory['publication_manifest'].version = 1
        targets.inventory['publication_manifest'].detail_digest = 'z' * 64
        targets.opening = dict(targets.inventory)
        targets.created_fresh = False  # same state set by ManagedCurrentTargetOwner.open_existing()
        reopened = ManagedCurrentGenesisManifestOwner(targets, store).initialize(**args)
        self.assertEqual(first, reopened)
        self.assertEqual(store.commits, 1)

    def test_open_existing_refuses_when_genesis_row_is_missing(self):
        targets = _Targets()
        targets.inventory['publication_manifest'].version = 1
        targets.opening = dict(targets.inventory)
        targets.created_fresh = False
        store = _Store(_Executor())
        args = _initialize_args()
        with self.assertRaises(ValueError):
            ManagedCurrentGenesisManifestOwner(targets, store).initialize(**args)
        self.assertEqual(store.commits, 0)

    def test_model_evidence_must_match_frozen_pin_and_revision_digest(self):
        targets = _Targets(); store = _Store(_Executor())
        args = _initialize_args()
        args['selected_model_evidence']['A'] = {
            'pin': dict(_model_evidence('A')['pin']), 'bytes': b'unrelated evidence'}
        with self.assertRaises(ValueError):
            ManagedCurrentGenesisManifestOwner(targets, store).initialize(**args)

    def test_schema_revisions_bind_catalog_acceptance_not_model_document_hashes(self):
        targets = _Targets(); store = _Store(_Executor())
        args = _initialize_args()
        args['schema_revisions'] = {
            'install-A': hashlib.sha256(b'model evidence A').hexdigest(),
            'install-B': hashlib.sha256(b'model evidence B').hexdigest()}
        with self.assertRaises(ValueError):
            ManagedCurrentGenesisManifestOwner(targets, store).initialize(**args)

    def test_verified_genesis_predecessor_transitions_only_a_to_actual_page_checkpoint(self):
        from dataclasses import replace
        from ashlar.source_checkpoint import truss_checkpoint
        from ashlar.truss_native_feed import native_feed_batch

        args = _initialize_args()
        row = ManagedCurrentGenesisManifestOwner(_Targets(), _Store(_Executor())).initialize(**args)
        predecessor = verify_managed_current_genesis_predecessor(
            row, selected_sources=args['source_progress'])
        source_a = args['source_progress']['A']
        prior = source_a.cursor_bytes
        following_doc = json.loads(prior)
        following_doc['nextXid'] = '2'
        following = json.dumps(following_doc, separators=(',', ':')).encode()
        page = json.dumps({
            'interfaceVersion': 'truss-native-feed/0.1',
            'prior': json.loads(prior), 'next': json.loads(following),
            'catalogPrerequisites': [], 'revisions': [], 'changes': [],
            'sources': [], 'reservations': [], 'configuration': {}, 'writerXid': '1',
        }, separators=(',', ':')).encode()
        batch = native_feed_batch(page, sha256=hashlib.sha256(page).hexdigest(),
                                  prior=prior, next=following, feed=source_a.feed)
        with self.assertRaisesRegex(ValueError, 'Batch differs from original source transaction'):
            transition_managed_current_first_a_page(
                predecessor,
                source_owner_admitted_batch=replace(batch, batch_id='forged-batch-id'))

        updated = transition_managed_current_first_a_page(
            predecessor, source_owner_admitted_batch=batch)
        self.assertEqual(set(updated), {args['source_progress']['A'].feed,
                                        args['source_progress']['B'].feed})
        self.assertEqual(updated[source_a.feed], json.loads(truss_checkpoint(batch)))
        self.assertEqual(updated[args['source_progress']['B'].feed],
                         args['source_progress']['B'].document())
        self.assertNotIn('position_sha256', updated[source_a.feed])
        self.assertNotIn('batch_id', args['source_progress']['A'].document())

    def test_incremental_driver_admits_genesis_predecessor_only_with_explicit_a_batch(self):
        from ashlar.source_checkpoint import truss_checkpoint
        from ashlar.truss_native_feed import native_feed_batch
        from ashlar_host.managed_current_publication import ManagedCurrentIncrementalPublicationDriver

        args = _initialize_args()
        row = ManagedCurrentGenesisManifestOwner(_Targets(), _Store(_Executor())).initialize(**args)
        predecessor = verify_managed_current_genesis_predecessor(
            row, selected_sources=args['source_progress'])
        source_a = args['source_progress']['A']
        prior = source_a.cursor_bytes
        following_doc = json.loads(prior); following_doc['nextXid'] = '2'
        following = json.dumps(following_doc, separators=(',', ':')).encode()
        page = json.dumps({'interfaceVersion': 'truss-native-feed/0.1',
            'prior': json.loads(prior), 'next': json.loads(following),
            'catalogPrerequisites': [], 'revisions': [], 'changes': [],
            'sources': [], 'reservations': [], 'configuration': {}, 'writerXid': '1'},
            separators=(',', ':')).encode()
        batch = native_feed_batch(page, sha256=hashlib.sha256(page).hexdigest(),
                                  prior=prior, next=following, feed=source_a.feed)
        checkpoint = truss_checkpoint(batch)

        class _ManifestTransport:
            def __init__(self): self.rows = [row]
            def query(self, _sql, _parameters): return SimpleNamespace(rows=self.rows)

        heads = {name: SimpleNamespace(version=0) for name in (
            'object_current', 'edge_current', 'tombstone', 'whole_source_history')}
        driver = object.__new__(ManagedCurrentIncrementalPublicationDriver)
        driver.transport = _ManifestTransport()
        driver.names = {**{role: 'catalog.schema.' + role for role in heads},
                        'publication_manifest': 'catalog.schema.publication_manifest'}
        driver.table_roles = tuple(heads)
        driver.targets = SimpleNamespace(observe=lambda: heads,
            carriers=SimpleNamespace(physical_profile=CARRIER_PROFILE_XXHASH64))
        driver.managed_genesis_predecessor = predecessor
        driver.source_owner_admitted_batch = batch
        driver.admission = SimpleNamespace(batch=batch)
        request = {'source_checkpoint_json': checkpoint, 'predecessor': row['publication_id']}
        progress = driver._previous_progress(request)
        self.assertEqual(progress, json.loads(row['source_progress_json']))
        self.assertEqual(driver.predecessor_manifest, row)

        driver.source_owner_admitted_batch = None
        with self.assertRaisesRegex(ValueError, 'separately admitted source owner batch'):
            driver._previous_progress(request)

        driver.source_owner_admitted_batch = batch
        driver.transport.rows = []
        with self.assertRaisesRegex(ValueError, 'exact durable genesis manifest predecessor'):
            driver._previous_progress(request)

        other = dict(row)
        other_report = json.loads(other['validation_report_json'])
        other_report['publication_plan_profile'] = 'ashlar-managed-current-incremental-plan/0.1'
        other['validation_report_json'] = json.dumps(other_report, sort_keys=True, separators=(',', ':'))
        driver.transport.rows = [other]
        with self.assertRaisesRegex(ValueError, 'cannot advance a non-genesis latest manifest'):
            driver._previous_progress(request)

    def test_genesis_transition_refuses_unbound_predecessor_or_noncontiguous_a_page(self):
        from ashlar.truss_native_feed import native_feed_batch

        args = _initialize_args()
        row = ManagedCurrentGenesisManifestOwner(_Targets(), _Store(_Executor())).initialize(**args)
        with self.assertRaisesRegex(ValueError, 'publication ID'):
            verify_managed_current_genesis_predecessor(
                {**row, 'publication_id': 'unbound'}, selected_sources=args['source_progress'])
        with self.assertRaisesRegex(ValueError, 'two-feed'):
            verify_managed_current_genesis_predecessor(
                row, selected_sources={'A': args['source_progress']['A'],
                                       'B': args['source_progress']['A']})

        predecessor = verify_managed_current_genesis_predecessor(
            row, selected_sources=args['source_progress'])
        with self.assertRaisesRegex(ValueError, 'Exact selected genesis manifest row'):
            transition_managed_current_first_a_page(
                ManagedCurrentGenesisPredecessor(None, {}, {}),
                source_owner_admitted_batch=None)
        predecessor.source_progress[args['source_progress']['B'].feed]['position'] = 'tampered'
        with self.assertRaisesRegex(ValueError, 'match its verified manifest row'):
            transition_managed_current_first_a_page(
                predecessor, source_owner_admitted_batch=None)
        predecessor = verify_managed_current_genesis_predecessor(
            row, selected_sources=args['source_progress'])
        source_a = args['source_progress']['A']
        wrong_prior_doc = json.loads(source_a.cursor_bytes)
        wrong_prior_doc['x-other'] = 'different cursor bytes'
        wrong_prior = json.dumps(wrong_prior_doc, separators=(',', ':')).encode()
        following_doc = json.loads(wrong_prior)
        following_doc['nextXid'] = '2'
        following = json.dumps(following_doc, separators=(',', ':')).encode()
        page = json.dumps({
            'interfaceVersion': 'truss-native-feed/0.1',
            'prior': json.loads(wrong_prior), 'next': json.loads(following),
            'catalogPrerequisites': [], 'revisions': [], 'changes': [],
            'sources': [], 'reservations': [], 'configuration': {}, 'writerXid': '1',
        }, separators=(',', ':')).encode()
        batch = native_feed_batch(page, sha256=hashlib.sha256(page).hexdigest(),
                                  prior=wrong_prior, next=following, feed=source_a.feed)
        with self.assertRaisesRegex(ValueError, 'genesis cursor'):
            transition_managed_current_first_a_page(
                predecessor, source_owner_admitted_batch=batch)
        selected_next_doc = json.loads(source_a.cursor_bytes)
        selected_next_doc['nextXid'] = '2'
        selected_next = json.dumps(selected_next_doc, separators=(',', ':')).encode()
        selected_page = json.dumps({
            'interfaceVersion': 'truss-native-feed/0.1',
            'prior': json.loads(source_a.cursor_bytes), 'next': json.loads(selected_next),
            'catalogPrerequisites': [], 'revisions': [], 'changes': [],
            'sources': [], 'reservations': [], 'configuration': {}, 'writerXid': '1',
        }, separators=(',', ':')).encode()
        wrong_feed = native_feed_batch(selected_page, sha256=hashlib.sha256(selected_page).hexdigest(),
                                       prior=source_a.cursor_bytes, next=selected_next,
                                       feed='unselected-feed')
        with self.assertRaisesRegex(ValueError, 'selected genesis cursor'):
            transition_managed_current_first_a_page(
                predecessor, source_owner_admitted_batch=wrong_feed)


if __name__ == '__main__': unittest.main()
