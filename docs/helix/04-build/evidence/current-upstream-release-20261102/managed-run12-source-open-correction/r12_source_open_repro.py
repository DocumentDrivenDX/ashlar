#!/usr/bin/env python3
"""Read-only local R12 A/B genesis-to-open reproduction; never ACKs."""
import base64
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import time

from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
from ashlar_host.managed_current_truss053_genesis_owner import ManagedCurrentTruss053SourceSelection
from ashlar_host.r04_schedule import R04Schedule
from ashlar_host.truss053_source_custody import (
    Truss053A1SourceHolder, _genesis_matches_with_monotonic_watermarks,
)
from ashlar_host.truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot
from truss import LocalPostgres
import psycopg

BASE = Path('/private/tmp/ashlar-a1-borrowed-config-20261103')
PG_ROOT = Path('/private/tmp/ashlar-a1-recovery-3tfog49n/truss-source')
ORIGINAL_GENESIS = BASE / 'run12-original-genesis.bin'
MANIFEST_PATH = BASE / 'run12-native-genesis-manifest.json'
TRIAL_PATH = BASE / 'trial-run12.json'
RESULT_PATH = BASE / 'r12-local-prepublication-open-repro.json'
INITIALIZER_SECONDS = 127


def main():
    original = ORIGINAL_GENESIS.read_bytes()
    saved = decode_detached_a1_genesis_snapshot(original)
    manifest = json.loads(MANIFEST_PATH.read_text())
    trial = json.loads(TRIAL_PATH.read_text())['trial']
    schedule = R04Schedule(saved.schedule_bytes)
    stack = ExitStack()
    holder = None
    try:
        runtimes = {source: stack.enter_context(
            LocalPostgres(PG_ROOT / f'pgdata-{source}')) for source in ('A', 'B')}
        selections = {}
        for source in ('A', 'B'):
            uri = psycopg.conninfo.make_conninfo(
                runtimes[source].info.connection_uri, user='ashlar_note_consumer')
            identity = saved.source_evidence(source)['identity']

            def connection_factory(uri=uri):
                return psycopg.connect(uri, autocommit=False, connect_timeout=5)

            selections[source] = ManagedCurrentTruss053SourceSelection(
                source, identity['feed'], identity['epoch'], identity['installation_id'],
                identity['consumer_id'], identity['consumer_registration_id'],
                identity['catalog_registration_id'], identity['catalog_revision'],
                connection_factory)
        holder = Truss053A1SourceHolder(
            schedule=schedule, source_selections=selections, context=object())
        holder._bootstrap_runtimes = stack.pop_all()
        current_genesis = holder.select_genesis()
        if not _genesis_matches_with_monotonic_watermarks(original, current_genesis):
            raise RuntimeError('Retained R12 source differs beyond monotonic safe watermarks')
        # Reproduce the actual prepublication idle interval between source
        # selection and the end of the remote initializer, without cloud work.
        time.sleep(INITIALIZER_SECONDS)
        plan = admit_detached_a1_plan(genesis_snapshot_bytes=original,
            genesis_manifest=manifest, stream=trial['stream'],
            materialized_at=trial['materialized_at'], context=object())
        request = json.dumps(plan.request, sort_keys=True, separators=(',', ':'),
            ensure_ascii=False, allow_nan=False).encode('utf-8')
        held = holder.open_prepublication(request_bytes=request,
            destination_id=trial['destination_id'], publication_id=plan.publication_id)
        refreshed = decode_detached_a1_genesis_snapshot(current_genesis)
        result = {
            'profile': 'ashlar-managed-a1-local-open-reproduction/0.1',
            'status': 'opened_and_closed_before_ack',
            'cloud_actions': False,
            'source_acknowledged': False,
            'initial_genesis_sha256': hashlib.sha256(original).hexdigest(),
            'refreshed_genesis_sha256': hashlib.sha256(current_genesis).hexdigest(),
            'held_snapshot_sha256': hashlib.sha256(held).hexdigest(),
            'held_snapshot_bytes': len(held),
            'request_sha256': hashlib.sha256(request).hexdigest(),
            'publication_id': plan.publication_id,
            'phase': holder.phase,
            'initializer_idle_seconds': INITIALIZER_SECONDS,
            'safe_watermarks': {source: refreshed.source_evidence(source)['page']['safe_watermark_xid']
                                for source in ('A', 'B')},
        }
        holder.close_before_ack()
        holder = None
        stack.close()
        fd = os.open(RESULT_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(result, stream, sort_keys=True, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        print(json.dumps(result, sort_keys=True))
    finally:
        if holder is not None:
            holder.close_before_ack()
        stack.close()


if __name__ == '__main__':
    main()
