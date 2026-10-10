"""Fixed original-commerce evolution admission; no writer/publication/ACK authority.

Fresh selected public UMF calls verify source/data semantics. The source and
runtime custody profile is fixed by this module, never by caller inventory flags.
"""
from dataclasses import dataclass
import hashlib
from importlib.resources import files
import json
import os
import re
from pathlib import Path
import tempfile
import zlib
from types import MappingProxyType
from ashlar.commerce_evolution import PreparedEvolution, prepare, qualify, independent_oracle, PRESERVATION_SHA
from ashlar.whole_entity import changes_from_batch
from ashlar.apply import EntityKey, EntityState
from .config import EvolutionAdmissionConfig, HostError
from .evolution_producer import capture, snapshot

REVISION = 'e44cd15f336dfb33db35acf20eee13dd120a1a28'
RESOURCE_HASHES = MappingProxyType({
    'source-closure.json': 'ab05e309c867e8e65eda93d0bec234cf4c815a077d0f6959a0a03a5759c389e6',
    'check_evolution.ts': '30ec82f34bd06eae9a23fb852b98482142e2fba1dcb12d9dbf370f2d8bafd3d2',
    'seed-candidate.json': '1c210a1bb3a5ef510d832715b15d238cdcb5126aa23b3f93d01d9be9c4c9d692',
    'seed-public.json.gz': '392b8a20395b53566fc9c73ed10cd37d714ce2d45330fd465bb31f4a43a6a6a1',
    'candidate.json': '450c82ef4b76fd73b095dc4617d0a9d2a9f9d06c15b9e402fc757be31b510aba',
    'public-presence.json.gz': '333c812a8dba69495bfe6c7bb1dca17094764e4e1c2b1bea00a92c25c074dfae',
    'original-model.json': '51d87c554df36846e41378cfaafc81277fcab61f6c891a6c64f34dba8fd9ac2a',
})


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def packaged_inputs() -> tuple[tuple[str, bytes], ...]:
    """Return fixed bounded package resource bytes; no checkout references."""
    root = files('ashlar_host.resources').joinpath('evolution')
    values = []
    for name, expected in RESOURCE_HASHES.items():
        raw = snapshot(Path(str(root.joinpath(name))), 4 * 1024 * 1024)
        if digest(raw) != expected:
            raise HostError('evolution-admission-refused')
        values.append((name, raw))
    baseline = snapshot(Path(str(files('ashlar_host.resources').joinpath('sql/ashlar-delta-v03/01-baseline.sql'))), 1048576)
    if digest(baseline) != '391889023e61dd8f63e3a1f94c78638145087ba9b326459fc98a95fcbe714060':
        raise HostError('evolution-admission-refused')
    values.append(('baseline.sql', baseline))
    return tuple(values)


def expand_receipt(raw: bytes, maximum: int) -> bytes:
    """Exactly one bounded gzip member, without trailing or concatenated data."""
    decoder = zlib.decompressobj(31)
    try:
        result = decoder.decompress(raw, maximum + 1)
        if len(result) > maximum or not decoder.eof or decoder.unused_data:
            raise HostError('evolution-admission-refused')
        return result
    except zlib.error:
        raise HostError('evolution-admission-refused') from None


def environment(temporary: Path) -> dict[str, str]:
    return {'TMPDIR': str(temporary), 'GIT_CONFIG_NOSYSTEM': '1',
            'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_TERMINAL_PROMPT': '0',
            'GIT_CONFIG_COUNT': '3', 'GIT_CONFIG_KEY_0': 'core.fsmonitor',
            'GIT_CONFIG_VALUE_0': 'false', 'GIT_CONFIG_KEY_1': 'core.hooksPath',
            'GIT_CONFIG_VALUE_1': '/dev/null', 'GIT_CONFIG_KEY_2': 'core.pager',
            'GIT_CONFIG_VALUE_2': 'cat'}


def custody(config: EvolutionAdmissionConfig, resources: tuple[tuple[str, bytes], ...],
            temporary: Path) -> bytes:
    """Verify developer-fixed selected bytes and clean exact Git identity."""
    closure = json.loads(dict(resources)['source-closure.json'])
    for name in ('bun', 'git'):
        raw = snapshot(getattr(config, name).resolve(), 128 * 1024 * 1024)
        expected = closure['executables'][name]
        if len(raw) != expected['bytes'] or digest(raw) != expected['sha256']:
            raise HostError('evolution-admission-refused')
    total = 0
    for entry in closure['files']:
        raw = snapshot(config.source / entry['path'], 4 * 1024 * 1024)
        total += len(raw)
        if total > 96 * 1024 * 1024 or len(raw) != entry['bytes'] or digest(raw) != entry['sha256']:
            raise HostError('evolution-admission-refused')
    for args, expected in ((('rev-parse', 'HEAD'), (REVISION + '\n').encode()),
                           (('status', '--porcelain', '--untracked-files=all'), b'')):
        stdout, stderr = capture((str(config.git), '-C', str(config.source), *args),
                                 cwd=temporary, environment=environment(temporary),
                                 timeout_seconds=config.timeout_seconds,
                                 maximum_output_bytes=config.maximum_output_bytes)
        if stdout != expected or stderr:
            raise HostError('evolution-admission-refused')
    # Recheck packaged bytes as well as selected external source resources.
    if packaged_inputs() != resources:
        raise HostError('evolution-admission-refused')
    return json.dumps({'revision': REVISION, 'source_closure_sha256': digest(dict(resources)['source-closure.json']),
                       'files': len(closure['files']), 'bytes': total,
                       'runtime_scope': closure['scope']}, sort_keys=True, separators=(',', ':')).encode()


def original_transitions(raw: dict, original: PreparedEvolution, scoped: PreparedEvolution) -> tuple:
    """Exact R3 prior carriers paired with surviving R4 changes; not model compatibility inference."""
    baseline = raw['baseline.sql']
    columns = {}
    for role in ('object_current', 'edge_current', 'tombstone'):
        body = re.search(r'CREATE TABLE ' + role + r' \((.*?)\) USING', baseline.decode(), re.S).group(1)
        columns[role] = tuple((match.group(1), match.group(2)) for item in body.split(',')
                              for match in [re.match(r'\s*(\w+) (STRING|BIGINT|TIMESTAMP)', item)] if match)
    columns['whole_source_history'] = tuple((name, 'STRING') for name in ('feed','epoch','delivery_id','digest','change_json','raw_base64'))
    prior_rows = independent_oracle(raw['candidate.json'], expand_receipt(raw['public-presence.json.gz'], 4194304),
                                   raw['original-model.json'], original, columns, prefix=3,
                                   materialized_at='2026-10-09T00:00:00+00:00')
    prior = {}
    for role, kind, type_name in (('object_current','object','type_id'), ('edge_current','edge','rel_type_id')):
        for row in prior_rows[role]:
            key = EntityKey(row['source_system'], kind, int(row[type_name]), int(row['id']))
            endpoints = tuple(EntityKey(key.source, 'object', int(row[name+'_type']), int(row[name+'_id']))
                              for name in ('source', 'target')) if kind == 'edge' else None
            prior[key] = EntityState(key, int(row['entity_version']), row['schema_revision'], row['props_json'], row['retained_json'], endpoints)
    result = tuple((prior[change.state.key], change) for change in changes_from_batch(scoped.batches[3]) if change.state.key in prior)
    if len(result) != 19:
        raise HostError('evolution-admission-refused')
    return result


@dataclass(frozen=True)
class EvolutionAdmission:
    """Owned finite source facts; caller still supplies independent authority ports."""
    prepared: PreparedEvolution
    receipt_bytes: bytes
    custody_bytes: bytes
    changes: tuple
    transitions: tuple

    def metadata(self) -> dict:
        """Bounded copied semantic/custody facts, never the full resource inventory."""
        return {'profile': 'ashlar-commerce-evolution-private-source-admission/0.2',
                'qualification': 'Fixed public source/data semantics only; no source, writer, publication or ACK authority.',
                'umf_revision': REVISION, 'public_receipt_sha256': digest(self.receipt_bytes),
                'custody': json.loads(self.custody_bytes),
                'source_system': self.prepared.source_system, 'epoch': self.prepared.epoch,
                'batch_identity_profile': 'source-epoch-qualified/0.1',
                'batch_ids': [batch.batch_id for batch in self.prepared.batches],
                'registry_sha256': digest(self.prepared.registry_json.encode()),
                'schema_revisions': list(self.prepared.schema_revisions)}

    def admit(self, change: object) -> None:
        if change not in self.changes:
            raise HostError('evolution-admission-refused')

    def admit_transition(self, previous: EntityState, change: object) -> None:
        self.admit(change)
        if (previous, change) not in self.transitions:
            raise HostError('evolution-admission-refused')


def run_admission(config: EvolutionAdmissionConfig, *, source_system: str,
                             epoch: str) -> EvolutionAdmission:
    """Freshly verify selected public semantics and source-qualified four batches."""
    if type(config) is not EvolutionAdmissionConfig:
        raise HostError('evolution-admission-refused')
    resources = packaged_inputs()
    raw = dict(resources)
    proof = expand_receipt(raw['public-presence.json.gz'], config.maximum_receipt_bytes)
    original = prepare(raw['candidate.json'], proof, raw['original-model.json'],
                       source_system=source_system, epoch=epoch)
    prepared = qualify(original)
    transitions = original_transitions(raw, original, prepared)
    primary = None
    opening = None
    temporary = tempfile.TemporaryDirectory(prefix='ashlar-evolution-admission-')
    directory = Path(temporary.name)
    try:
        opening = custody(config, resources, directory)
        for name in ('seed-candidate.json', 'seed-public.json.gz', 'check_evolution.ts'):
            fd = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            write_primary = None
            try:
                value = raw[name]
                offset = 0
                while offset < len(value):
                    written = os.write(fd, value[offset:])
                    if written <= 0:
                        raise HostError('evolution-admission-refused')
                    offset += written
            except BaseException as exc:
                write_primary = exc
                raise
            finally:
                try:
                    os.close(fd)
                except BaseException as cleanup:
                    if write_primary is None or isinstance(write_primary, Exception) and not isinstance(cleanup, Exception):
                        raise
                    try:
                        write_primary.cleanup_failed = True
                    except BaseException:
                        pass
        capture((str(config.bun), str(directory / 'check_evolution.ts'), str(config.source),
                 str(directory / 'seed-candidate.json'), str(directory / 'seed-public.json.gz'),
                 str(directory / 'receipt.json')), cwd=directory, environment=environment(directory),
                timeout_seconds=config.timeout_seconds, maximum_output_bytes=config.maximum_output_bytes)
        receipt = snapshot(directory / 'receipt.json', config.maximum_receipt_bytes)
        if digest(receipt) != PRESERVATION_SHA or receipt != proof:
            raise HostError('evolution-admission-refused')
        return EvolutionAdmission(prepared, receipt, opening,
                                  tuple(change for batch in prepared.batches for change in changes_from_batch(batch)), transitions)
    except BaseException as exc:
        primary = exc
        if isinstance(exc, Exception):
            raise HostError('evolution-admission-refused') from None
        raise
    finally:
        closing_failure = None
        if opening is not None:
            try:
                if custody(config, resources, directory) != opening:
                    raise HostError('evolution-admission-refused')
            except BaseException as exc:
                closing_failure = exc
        cleanup_failure = None
        try:
            temporary.cleanup()
        except BaseException as exc:
            cleanup_failure = exc
        cancellations = [failure for failure in (primary, closing_failure, cleanup_failure)
                         if failure is not None and not isinstance(failure, Exception)]
        owner = cancellations[0] if cancellations else (primary if primary is not None else closing_failure)
        if owner is not None:
            if cleanup_failure is not None or primary is not None and closing_failure is not None:
                try:
                    owner.cleanup_failed = True
                except BaseException:
                    pass
            if primary is None or owner is not primary:
                raise owner
        elif cleanup_failure is not None:
            raise cleanup_failure


def admit_commerce_evolution(config: EvolutionAdmissionConfig, *, source_system: str,
                             epoch: str) -> EvolutionAdmission:
    """Admit exact finite public semantics; ordinary failures are payload-free."""
    try:
        return run_admission(config, source_system=source_system, epoch=epoch)
    except Exception:
        raise HostError('evolution-admission-refused') from None
