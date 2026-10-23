# SPDX-License-Identifier: MIT OR Apache-2.0
"""Versioned custody for the current-core two-source R04 schedule.

This journal is deliberately separate from the historical commerce evolution
profile. It records intent and evidence supplied by real Truss/driver owners;
it does not implement either provider or grant execution authority.
"""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import base64
from urllib.parse import quote

PROFILE = 'ashlar-current-core-r04-schedule/0.2'
MAXIMUM_SCHEDULE_BYTES = 2 * 1024 * 1024
MAXIMUM_RECORD_BYTES = 16 * 1024 * 1024
SLOT_ORDER = ('A1', 'B1', 'A2', 'B2', 'A3', 'B3', 'A4', 'B4')
STAGES = ('create', 'create', 'update', 'update', 'delete', 'delete',
          'additive-candidate', 'additive-candidate')


class R04CustodyError(ValueError):
    """Original R04 custody is incomplete, changed or out of order."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise R04CustodyError('Duplicate R04 JSON member')
        result[key] = value
    return result


def _decode(raw, limit=MAXIMUM_RECORD_BYTES):
    if type(raw) is not bytes or not 0 < len(raw) <= limit:
        raise R04CustodyError('Bounded original R04 bytes required')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(
                              R04CustodyError('Non-finite R04 JSON value')))
    except (UnicodeError, ValueError, RecursionError) as error:
        raise R04CustodyError('Invalid original R04 JSON') from error


def _mapping(value):
    return type(value) is dict and bool(value)


@dataclass(frozen=True)
class R04Schedule:
    """Closed semantic schedule, including source facts and prefix oracle."""
    raw: bytes

    def __post_init__(self):
        value = _decode(self.raw, MAXIMUM_SCHEDULE_BYTES)
        if canonical(value) != self.raw:
            raise R04CustodyError('Canonical original R04 schedule bytes required')
        if (type(value) is not dict or set(value) != {
                'profile', 'source_bindings', 'accepted_model_pins', 'slots',
                'prefix_oracle'} or value['profile'] != PROFILE):
            raise R04CustodyError('Closed current-core R04 schedule required')
        bindings = value['source_bindings']
        pins = value['accepted_model_pins']
        slots = value['slots']
        oracle = value['prefix_oracle']
        if (type(bindings) is not dict or set(bindings) != {'A', 'B'}
                or any(not _mapping(bindings[source]) for source in ('A', 'B'))
                or type(pins) is not dict or set(pins) != {'A', 'B'}
                or any(not _mapping(pins[source]) for source in ('A', 'B'))
                or type(slots) is not list or len(slots) != 8
                or type(oracle) is not list or len(oracle) != 9
                or any(not _mapping(item) or type(item.get('prefix')) is not int or item['prefix'] != n
                       for n, item in enumerate(oracle))):
            raise R04CustodyError('Both source authorities, model pins and all prefix oracles required')
        entity_refs = {'A': None, 'B': None}
        for ordinal, (slot, stage) in enumerate(zip(slots, STAGES)):
            if (type(slot) is not dict or set(slot) != {
                    'ordinal', 'slot', 'source', 'stage', 'entity_ref',
                    'predecessor_slot', 'mutation', 'expected_transition'}
                    or type(slot['ordinal']) is not int or slot['ordinal'] != ordinal
                    or slot['slot'] != SLOT_ORDER[ordinal]
                    or slot['source'] != SLOT_ORDER[ordinal][0]
                    or slot['stage'] != stage or type(slot['entity_ref']) is not str
                    or not slot['entity_ref'] or not _mapping(slot['mutation'])
                    or not _mapping(slot['expected_transition'])):
                raise R04CustodyError('Exact closed ordered R04 semantic slot required')
            source = slot['source']
            expected_predecessor = ordinal - 2 if stage in ('update', 'delete') else None
            if slot['predecessor_slot'] != expected_predecessor:
                raise R04CustodyError('R2/R3 require prior same-source identity; other stages are explicit')
            if stage == 'create':
                entity_refs[source] = slot['entity_ref']
            elif stage in ('update', 'delete') and slot['entity_ref'] != entity_refs[source]:
                raise R04CustodyError('R2/R3 must reference the same-source R1-created entity')

    @classmethod
    def from_document(cls, value):
        return cls(canonical(value))

    def document(self):
        return _decode(self.raw, MAXIMUM_SCHEDULE_BYTES)

    @property
    def sha256(self):
        return hashlib.sha256(self.raw).hexdigest()


class R04SchedulePolicy:
    """Required current Truss, native registry and authority admission seam."""
    def admit_schedule(self, schedule, context):
        raise NotImplementedError

    def admit_request(self, schedule, ordinal, request_bytes, predecessor_ids, context):
        """Prove exact bytes implement this frozen semantic slot and model pin."""
        raise NotImplementedError

    def admit_before_source_effect(self, schedule, ordinal, request_record,
                                   prior_publication, prior_ack, context):
        """Renew selected source authority and the immediately prior slot custody."""
        raise NotImplementedError

    def admit_source_result(self, schedule, slot, request, result, readback, context):
        """Verify the original public commit/readback; return resolved IDs."""
        raise NotImplementedError

    def admit_page(self, schedule, slot, request, result, page, context):
        """Bind the actual complete Truss page and cursors to the retained request."""
        raise NotImplementedError

    def admit_retained_page(self, schedule, slot, request, result, page_record, context):
        """Re-admit original retained page facts without reading a future cursor."""
        raise NotImplementedError

    def admit_publication(self, schedule, slot, page, publication, slot_plan, context):
        """Independently verify oracle, complete vector and both progress entries."""
        raise NotImplementedError

    def admit_before_graph_effect(self, schedule, ordinal, request_record,
                                  source_result, page_record, slot_plan, context):
        """Renew the staged page and complete retained publication plan before effects."""
        raise NotImplementedError

    def admit_slot_plan(self, schedule, ordinal, slot_plan, context):
        """Independently admit the complete current-core publication plan before effects."""
        raise NotImplementedError

    def admit_retained_slot_plan(self, schedule, ordinal, slot_plan, context):
        """Re-admit retained plan bindings against current source/native owners on resume."""
        raise NotImplementedError

    def admit_before_ack_effect(self, schedule, ordinal, publication, context):
        """Renew complete vector/progress and source-specific ACK authority."""
        raise NotImplementedError

    def admit_ack(self, schedule, slot, publication, ack, context):
        """Verify durable source-specific protected ACK after publication."""
        raise NotImplementedError


class R04Journal:
    """Append-only FULL-sync SQLite custody for schedule and per-slot evidence.

    An event marked started is never silently retried. Recovery may append a
    verified public result for that original intent; it may not issue the effect
    again. Callbacks are explicit ports and must be the actual selected owners.
    """
    def __init__(self, path, connection, schedule, policy, context, identity):
        self.path, self.connection = path, connection
        self.schedule, self.policy, self.context, self.identity = schedule, policy, context, identity

    @classmethod
    def create(cls, path, schedule, *, policy, context):
        if (type(path) is not type(Path()) or not path.is_absolute() or type(schedule) is not R04Schedule
                or not isinstance(policy, R04SchedulePolicy) or context is None):
            raise R04CustodyError('Absolute path, R04 schedule, policy and context required')
        if policy.admit_schedule(schedule, context) is not None:
            raise R04CustodyError('Independent current source/ACK schedule admission incomplete')
        parent = path.parent.lstat()
        if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
                or parent.st_mode & 0o022 or path.parent.resolve() != path.parent):
            raise R04CustodyError('Private owned R04 journal directory required')
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(path, flags, 0o600)
        try:
            os.fchmod(fd, 0o600)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                raise R04CustodyError('Private owned R04 journal file required')
            connection = sqlite3.connect('file:' + quote(str(path)) + '?mode=rw', uri=True)
            connection.execute('PRAGMA synchronous=FULL')
            connection.execute('PRAGMA journal_mode=DELETE')
            with connection:
                connection.execute('CREATE TABLE schedule(id INTEGER PRIMARY KEY CHECK(id=1), raw BLOB NOT NULL, digest TEXT NOT NULL)')
                connection.execute('CREATE TABLE event(seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, slot INTEGER, body BLOB NOT NULL, previous TEXT NOT NULL, digest TEXT NOT NULL)')
                connection.execute("CREATE TRIGGER schedule_no_update BEFORE UPDATE ON schedule BEGIN SELECT RAISE(ABORT,'immutable schedule'); END")
                connection.execute("CREATE TRIGGER schedule_no_delete BEFORE DELETE ON schedule BEGIN SELECT RAISE(ABORT,'immutable schedule'); END")
                connection.execute("CREATE TRIGGER event_no_update BEFORE UPDATE ON event BEGIN SELECT RAISE(ABORT,'append-only event'); END")
                connection.execute("CREATE TRIGGER event_no_delete BEFORE DELETE ON event BEGIN SELECT RAISE(ABORT,'append-only event'); END")
                connection.execute('INSERT INTO schedule VALUES(1,?,?)', (schedule.raw, schedule.sha256))
            dirfd = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
            try:
                os.fsync(dirfd)
            finally:
                os.close(dirfd)
            return cls(path, connection, schedule, policy, context, (info.st_dev, info.st_ino))
        except BaseException:
            try:
                connection.close()
            except (UnboundLocalError, sqlite3.Error):
                pass
            os.close(fd)
            raise
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    def _events(self):
        path_info = self.path.lstat()
        if ((path_info.st_dev, path_info.st_ino) != self.identity or not stat.S_ISREG(path_info.st_mode)
                or path_info.st_uid != os.getuid() or path_info.st_mode & 0o077):
            raise R04CustodyError('Original private R04 journal identity changed')
        schedule_rows = self.connection.execute('SELECT raw,digest FROM schedule WHERE id=1').fetchall()
        if len(schedule_rows) != 1 or schedule_rows[0] != (self.schedule.raw, self.schedule.sha256):
            raise R04CustodyError('Original frozen R04 schedule changed')
        rows = self.connection.execute('SELECT seq,kind,slot,body,previous,digest FROM event ORDER BY seq').fetchall()
        previous = '0' * 64
        events = []
        for seq, kind, slot, body, old_previous, digest in rows:
            payload = _decode(body)
            calculated = hashlib.sha256(previous.encode() + b'\0' + kind.encode() + b'\0'
                + str(slot).encode() + b'\0' + body).hexdigest()
            if seq != len(events) + 1 or old_previous != previous or digest != calculated:
                raise R04CustodyError('R04 append-only event chain changed')
            events.append((kind, slot, payload))
            previous = digest
        return events

    @classmethod
    def open_existing(cls, path, *, policy, context):
        """Resume exact existing custody; never initialize or replace it."""
        if type(path) is not type(Path()) or not path.is_absolute() or not isinstance(policy, R04SchedulePolicy) or context is None:
            raise R04CustodyError('Existing absolute R04 path, policy and context required')
        parent, info = path.parent.lstat(), path.lstat()
        if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or parent.st_mode & 0o022
                or path.parent.resolve() != path.parent or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid() or info.st_mode & 0o077):
            raise R04CustodyError('Private owned existing R04 journal required')
        connection = sqlite3.connect('file:' + quote(str(path)) + '?mode=rw', uri=True)
        try:
            connection.execute('PRAGMA synchronous=FULL')
            if connection.execute('PRAGMA journal_mode').fetchone() != ('delete',):
                raise R04CustodyError('Existing R04 DELETE-journal profile required')
            rows = connection.execute('SELECT raw,digest FROM schedule WHERE id=1').fetchall()
            if len(rows) != 1:
                raise R04CustodyError('Original frozen R04 schedule missing')
            schedule = R04Schedule(rows[0][0])
            if rows[0][1] != schedule.sha256 or policy.admit_schedule(schedule, context) is not None:
                raise R04CustodyError('Original schedule or current authority admission differs')
            owner = cls(path, connection, schedule, policy, context, (info.st_dev, info.st_ino))
            owner._events()
            owner._revalidate_retained(owner._state())
            after = path.lstat()
            if (after.st_dev, after.st_ino) != owner.identity:
                raise R04CustodyError('Existing R04 journal identity changed while opening')
            return owner
        except BaseException:
            connection.close()
            raise

    def _append(self, kind, ordinal, payload):
        body = canonical(payload)
        if len(body) > MAXIMUM_RECORD_BYTES:
            raise R04CustodyError('R04 evidence record exceeds bound')
        events = self._events()
        previous = self.connection.execute('SELECT digest FROM event ORDER BY seq DESC LIMIT 1').fetchone()
        previous = previous[0] if previous else '0' * 64
        seq = len(events) + 1
        digest = hashlib.sha256(previous.encode() + b'\0' + kind.encode() + b'\0'
            + str(ordinal).encode() + b'\0' + body).hexdigest()
        with self.connection:
            self.connection.execute('INSERT INTO event VALUES(?,?,?,?,?,?)',
                (seq, kind, ordinal, body, previous, digest))
        self.connection.execute('PRAGMA wal_checkpoint(FULL)')
        fd = os.open(self.path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return self._events()

    def _state(self):
        state = [{'request': None, 'source_started': False, 'source_result': None,
                  'page': None, 'slot_plan': None, 'graph_started': False, 'publication': None,
                  'ack_started': False, 'ack': None}
                 for _ in SLOT_ORDER]
        for kind, ordinal, body in self._events():
            if type(ordinal) is not int or not 0 <= ordinal < 8:
                raise R04CustodyError('Invalid original R04 slot ordinal')
            row = state[ordinal]
            if kind == 'request':
                if row['request'] is not None:
                    raise R04CustodyError('Duplicate retained R04 request')
                spec = self.schedule.document()['slots'][ordinal]
                try:
                    request = base64.b64decode(body['request_base64'], validate=True)
                    valid = (body['schedule_sha256'] == self.schedule.sha256
                             and body['ordinal'] == ordinal and body['slot'] == spec['slot']
                             and body['source'] == spec['source']
                             and body['request_sha256'] == hashlib.sha256(request).hexdigest())
                except (KeyError, TypeError, ValueError):
                    valid = False
                if not valid:
                    raise R04CustodyError('Retained R04 request binding/digest differs')
                row['request'] = body
            elif kind == 'source_started':
                next_slot = next((n for n, item in enumerate(state) if item['ack'] is None), 8)
                if row['request'] is None or row['source_started'] or ordinal != next_slot:
                    raise R04CustodyError('Invalid R04 source-effect ordering')
                self._check_binding(row, ordinal, body)
                row['source_started'] = True
            elif kind == 'source_result':
                if not row['source_started'] or row['source_result'] is not None:
                    raise R04CustodyError('Invalid R04 source-result transition')
                self._check_binding(row, ordinal, body)
                try:
                    result = base64.b64decode(body['result_base64'], validate=True)
                    readback = base64.b64decode(body['readback_base64'], validate=True)
                    ids = body['resolved_ids']
                    spec = self.schedule.document()['slots'][ordinal]
                    if (not _mapping(ids) or not result or not readback
                            or any(type(k) is not str or type(v) is not str or not v for k, v in ids.items())
                            or spec['stage'] != 'additive-candidate' and spec['entity_ref'] not in ids):
                        raise ValueError('source result schema')
                except (KeyError, TypeError, ValueError) as error:
                    raise R04CustodyError('Retained source result/readback malformed') from error
                row['source_result'] = body
            elif kind == 'page':
                if row['source_result'] is None or row['page'] is not None:
                    raise R04CustodyError('Invalid R04 feed-page transition')
                self._check_binding(row, ordinal, body)
                self._validate_page_body(ordinal, body)
                row['page'] = body
            elif kind == 'slot_plan':
                if row['page'] is None or row['slot_plan'] is not None or row['graph_started']:
                    raise R04CustodyError('Invalid R04 complete slot-plan transition')
                self._check_binding(row, ordinal, body)
                try:
                    from .r04_slot_plan import R04SlotPlan
                    raw_plan = base64.b64decode(body['plan_base64'], validate=True)
                    plan = R04SlotPlan(raw_plan).document()
                    if (body['plan_sha256'] != hashlib.sha256(raw_plan).hexdigest()
                            or plan['schedule_sha256'] != self.schedule.sha256
                            or plan['ordinal'] != ordinal
                            or plan['mutation_request_sha256'] != row['request']['request_sha256']
                            or plan['page']['sha256'] != row['page']['page']['sha256']
                            or plan['page']['event_inventory_sha256'] != row['page']['page']['event_inventory_sha256']):
                        raise ValueError('plan lineage')
                except (KeyError, TypeError, ValueError) as error:
                    raise R04CustodyError('Retained complete R04 publication plan changed') from error
                row['slot_plan'] = body
            elif kind == 'graph_started':
                if row['page'] is None or row['slot_plan'] is None or row['graph_started'] or row['publication'] is not None:
                    raise R04CustodyError('Invalid R04 graph-effect transition')
                self._check_binding(row, ordinal, body)
                row['graph_started'] = True
            elif kind == 'publication':
                if (not row['graph_started'] or row['slot_plan'] is None or row['publication'] is not None
                        or not _mapping(body.get('publication'))
                        or body.get('slot_plan_sha256') != row['slot_plan'].get('plan_sha256')):
                    raise R04CustodyError('Invalid R04 publication transition')
                self._check_binding(row, ordinal, body)
                row['publication'] = body
            elif kind == 'ack_started':
                if row['publication'] is None or row['ack_started'] or row['ack'] is not None:
                    raise R04CustodyError('Invalid R04 ACK transition')
                self._check_binding(row, ordinal, body)
                row['ack_started'] = True
            elif kind == 'ack':
                if not row['ack_started'] or row['ack'] is not None or 'ack' not in body:
                    raise R04CustodyError('Invalid R04 ACK transition')
                self._check_binding(row, ordinal, body)
                row['ack'] = body
            else:
                raise R04CustodyError('Unknown R04 custody event')
        return state

    def _check_binding(self, state, ordinal, body):
        request = state['request']
        if (type(body) is not dict or body.get('schedule_sha256') != self.schedule.sha256
                or body.get('ordinal') != ordinal or request is None
                or body.get('request_sha256') != request.get('request_sha256')):
            raise R04CustodyError('R04 slot record is not bound to its frozen schedule/request')

    def _validate_page_body(self, ordinal, body):
        try:
            stored = body['page']
            content = base64.b64decode(stored['content_base64'], validate=True)
            prior = base64.b64decode(stored['prior_base64'], validate=True)
            following = base64.b64decode(stored['next_base64'], validate=True)
            payload = _decode(content, 8 * 1024 * 1024)
            if (hashlib.sha256(content).hexdigest() != stored['sha256']
                    or payload.get('writerXid') != stored['writer_xid']
                    or payload.get('prior') != _decode(prior, 16384)
                    or payload.get('next') != _decode(following, 16384)
                    or stored['outcome'] != 'transaction'):
                raise ValueError('page identity')
            changes = payload['changes']
            inventory = []
            for index, change in enumerate(changes):
                wrapper = change['new_value']
                raw = base64.b64decode(wrapper['bytesBase64'], validate=True)
                digest = wrapper['bytesSha256']
                if wrapper.get('component') != 'event' or hashlib.sha256(raw).hexdigest() != digest:
                    raise ValueError('event digest')
                inventory.append({'ordinal': index, 'bytes': len(raw), 'sha256': digest})
            if (inventory != stored['event_inventory']
                    or hashlib.sha256(canonical(inventory)).hexdigest() != stored['event_inventory_sha256']):
                raise ValueError('event inventory')
        except (KeyError, TypeError, ValueError) as error:
            raise R04CustodyError('Retained actual Truss page/cursor/event inventory changed') from error

    def _revalidate_retained(self, state):
        """Recheck original receipts and current native custody on every resume."""
        for ordinal, row in enumerate(state):
            request, result, page_record = row['request'], row['source_result'], row['page']
            if request is not None:
                raw_request = base64.b64decode(request['request_base64'], validate=True)
                ids = request['predecessor_ids']
                if self.policy.admit_request(self.schedule, ordinal, raw_request, ids, self.context) is not None:
                    raise R04CustodyError('Retained request no longer matches semantic slot')
            if result is not None:
                raw_result = base64.b64decode(result['result_base64'], validate=True)
                raw_readback = base64.b64decode(result['readback_base64'], validate=True)
                ids = self.policy.admit_source_result(self.schedule, ordinal, request,
                    raw_result, raw_readback, self.context)
                if ids != result['resolved_ids']:
                    raise R04CustodyError('Retained public source result identity changed')
                spec = self.schedule.document()['slots'][ordinal]
                if spec['stage'] in ('update', 'delete'):
                    first = ordinal - (2 if spec['stage'] == 'update' else 4)
                    source_id = state[first]['source_result']['resolved_ids'].get(spec['entity_ref'])
                    if result['resolved_ids'].get(spec['entity_ref']) != source_id:
                        raise R04CustodyError('R2/R3 result changed the R1-created source identity')
            if page_record is not None:
                if self.policy.admit_retained_page(self.schedule, ordinal, request,
                        result, page_record['page'], self.context) is not None:
                    raise R04CustodyError('Retained Truss page admission changed')
            if row['slot_plan'] is not None:
                try:
                    from .r04_slot_plan import R04SlotPlan
                    plan_raw = base64.b64decode(row['slot_plan']['plan_base64'], validate=True)
                    plan = R04SlotPlan(plan_raw)
                    if (row['slot_plan']['plan_sha256'] != hashlib.sha256(plan_raw).hexdigest()
                            or self.policy.admit_retained_slot_plan(self.schedule, ordinal,
                                plan, self.context) is not None):
                        raise ValueError('plan admission')
                except (KeyError, TypeError, ValueError) as error:
                    raise R04CustodyError('Retained R04 publication plan admission changed') from error
            if row['publication'] is not None:
                from .r04_slot_plan import R04SlotPlan
                plan_raw = base64.b64decode(row['slot_plan']['plan_base64'], validate=True)
                plan = R04SlotPlan(plan_raw)
                if self.policy.admit_publication(self.schedule, ordinal, page_record,
                        row['publication']['publication'], plan, self.context) is not None:
                    raise R04CustodyError('Retained complete native publication changed')
            if row['ack'] is not None:
                if self.policy.admit_ack(self.schedule, ordinal, row['publication'],
                        row['ack']['ack'], self.context) is not None:
                    raise R04CustodyError('Retained source-specific ACK changed')
        return state

    def freeze_request(self, ordinal, request, *, predecessor_ids=None):
        state = self._state()
        if type(ordinal) is not int or not 0 <= ordinal < 8 or state[ordinal]['request'] is not None:
            raise R04CustodyError('Unfrozen original R04 slot required')
        spec = self.schedule.document()['slots'][ordinal]
        refs = predecessor_ids if predecessor_ids is not None else {}
        if type(refs) is not dict:
            raise R04CustodyError('Resolved predecessor identity mapping required')
        if spec['predecessor_slot'] is not None:
            source_previous = state[spec['predecessor_slot']]['source_result']
            entity = spec['entity_ref']
            if (source_previous is None or set(refs) != {entity}
                    or refs.get(entity) != source_previous.get('resolved_ids', {}).get(entity)):
                raise R04CustodyError('Dependent request requires verified predecessor identity')
            if spec['stage'] == 'delete':
                create_result = state[ordinal - 4]['source_result']
                if (create_result is None or refs.get(entity)
                        != create_result.get('resolved_ids', {}).get(entity)):
                    raise R04CustodyError('R3 must retain the original R1-created identity')
        elif refs:
            raise R04CustodyError('Unexpected predecessor identity in independent request')
        if type(request) is not bytes or not 0 < len(request) <= 4 * 1024 * 1024:
            raise R04CustodyError('Exact bounded original Truss request bytes required')
        if self.policy.admit_request(self.schedule, ordinal, request, refs, self.context) is not None:
            raise R04CustodyError('Exact request does not implement frozen semantic slot')
        value = {'schedule_sha256': self.schedule.sha256, 'ordinal': ordinal,
                 'slot': spec['slot'], 'source': spec['source'],
                 'predecessor_ids': refs,
                 'request_base64': base64.b64encode(request).decode('ascii'),
                 'request_sha256': hashlib.sha256(request).hexdigest()}
        self._append('request', ordinal, value)
        return value['request_sha256']

    def begin_source_effect(self, ordinal):
        state = self._state()
        next_slot = next((n for n, row in enumerate(state) if row['ack'] is None), 8)
        if (type(ordinal) is not int or not 0 <= ordinal < 8 or ordinal != next_slot
                or state[ordinal]['request'] is None or state[ordinal]['source_started']):
            raise R04CustodyError('Retained request and prior durable ACK required before source effect')
        prior_publication = state[ordinal - 1]['publication'] if ordinal else None
        prior_ack = state[ordinal - 1]['ack'] if ordinal else None
        if self.policy.admit_before_source_effect(self.schedule, ordinal, state[ordinal]['request'],
                prior_publication, prior_ack, self.context) is not None:
            raise R04CustodyError('Current source authority or immediately prior slot is not admitted')
        self._append('source_started', ordinal, {'schedule_sha256': self.schedule.sha256,
            'ordinal': ordinal, 'request_sha256': state[ordinal]['request']['request_sha256']})

    def retain_source_result(self, ordinal, result, readback):
        state = self._state()
        if not state[ordinal]['source_started'] or state[ordinal]['source_result'] is not None:
            raise R04CustodyError('Original started source effect awaiting one verified result')
        if type(result) is not bytes or type(readback) is not bytes:
            raise R04CustodyError('Original public result and readback bytes required')
        resolved = self.policy.admit_source_result(self.schedule, ordinal,
            state[ordinal]['request'], result, readback, self.context)
        if not _mapping(resolved) or any(type(k) is not str or type(v) is not str or not v for k, v in resolved.items()):
            raise R04CustodyError('Public source result/readback did not resolve exact identities')
        spec = self.schedule.document()['slots'][ordinal]
        if spec['stage'] in ('update', 'delete'):
            create_ordinal = ordinal - (2 if spec['stage'] == 'update' else 4)
            create_result = state[create_ordinal]['source_result']
            if (create_result is None or resolved.get(spec['entity_ref'])
                    != create_result.get('resolved_ids', {}).get(spec['entity_ref'])):
                raise R04CustodyError('R2/R3 result changed the R1-created source identity')
        request_sha = state[ordinal]['request']['request_sha256']
        self._append('source_result', ordinal, {'schedule_sha256': self.schedule.sha256,
            'ordinal': ordinal, 'request_sha256': request_sha, 'resolved_ids': resolved,
            'result_base64': base64.b64encode(result).decode('ascii'),
            'readback_base64': base64.b64encode(readback).decode('ascii')})

    def bind_page(self, ordinal, page):
        state = self._state()
        if state[ordinal]['source_result'] is None or state[ordinal]['page'] is not None:
            raise R04CustodyError('Verified source result required before actual feed page binding')
        if self.policy.admit_page(self.schedule, ordinal, state[ordinal]['request'],
                                  state[ordinal]['source_result'], page, self.context) is not None:
            raise R04CustodyError('Actual complete feed page/request binding incomplete')
        try:
            cursor_before, cursor_after = page.prior.content, page.next.content
            content = page.content
            fields = {'consumer_id': page.consumer_id, 'registration_id': page.registration_id,
                      'writer_xid': page.writer_xid, 'outcome': page.outcome,
                      'sha256': page.sha256}
        except AttributeError as error:
            raise R04CustodyError('Actual complete public Truss FeedPage required') from error
        if (type(content) is not bytes or type(cursor_before) is not bytes
                or type(cursor_after) is not bytes or hashlib.sha256(content).hexdigest() != page.sha256
                or page.outcome != 'transaction'):
            raise R04CustodyError('Complete original Truss page content/hash/cursors required')
        payload = _decode(content, 8 * 1024 * 1024)
        changes = payload.get('changes') if type(payload) is dict else None
        if (type(changes) is not list or len(changes) > 4096
                or payload.get('writerXid') != page.writer_xid
                or payload.get('prior') != _decode(cursor_before, 16384)
                or payload.get('next') != _decode(cursor_after, 16384)):
            raise R04CustodyError('Actual complete Truss writer transaction required')
        inventory = []
        for event_ordinal, change in enumerate(changes):
            try:
                wrapper = change['new_value']
                raw = base64.b64decode(wrapper['bytesBase64'], validate=True)
                digest = wrapper['bytesSha256']
                if wrapper.get('component') != 'event' or hashlib.sha256(raw).hexdigest() != digest:
                    raise ValueError('event byte digest')
            except (KeyError, TypeError, ValueError) as error:
                raise R04CustodyError('Original Truss event-byte inventory incomplete') from error
            inventory.append({'ordinal': event_ordinal, 'bytes': len(raw), 'sha256': digest})
        stored = {**fields, 'content_base64': base64.b64encode(content).decode('ascii'),
                  'prior_base64': base64.b64encode(cursor_before).decode('ascii'),
                  'next_base64': base64.b64encode(cursor_after).decode('ascii'),
                  'event_inventory': inventory,
                  'event_inventory_sha256': hashlib.sha256(canonical(inventory)).hexdigest()}
        self._append('page', ordinal, {'schedule_sha256': self.schedule.sha256,
            'ordinal': ordinal, 'request_sha256': state[ordinal]['request']['request_sha256'],
            'page': stored})

    def retain_slot_plan(self, ordinal, plan_bytes):
        state = self._state()
        if (type(ordinal) is not int or not 0 <= ordinal < 8 or state[ordinal]['page'] is None
                or state[ordinal]['slot_plan'] is not None or state[ordinal]['graph_started']):
            raise R04CustodyError('Original retained page awaiting one complete publication plan')
        try:
            from .r04_slot_plan import R04SlotPlan
            plan = R04SlotPlan(plan_bytes)
            value = plan.document()
        except (TypeError, ValueError) as error:
            raise R04CustodyError('Canonical versioned R04 publication plan required') from error
        if (value['schedule_sha256'] != self.schedule.sha256 or value['ordinal'] != ordinal
                or value['mutation_request_sha256'] != state[ordinal]['request']['request_sha256']
                or value['page']['sha256'] != state[ordinal]['page']['page']['sha256']
                or value['page']['event_inventory_sha256'] != state[ordinal]['page']['page']['event_inventory_sha256']
                or self.policy.admit_slot_plan(self.schedule, ordinal, plan, self.context) is not None):
            raise R04CustodyError('Complete plan must bind this exact retained slot/page before effects')
        body = {'schedule_sha256': self.schedule.sha256, 'ordinal': ordinal,
            'request_sha256': state[ordinal]['request']['request_sha256'],
            'plan_base64': base64.b64encode(plan_bytes).decode('ascii'),
            'plan_sha256': hashlib.sha256(plan_bytes).hexdigest()}
        self._append('slot_plan', ordinal, body)

    def begin_graph_effect(self, ordinal):
        state = self._state()
        if state[ordinal]['page'] is None or state[ordinal]['slot_plan'] is None or state[ordinal]['graph_started']:
            raise R04CustodyError('Durably bound actual feed page and complete publication plan required before graph effects')
        from .r04_slot_plan import R04SlotPlan
        plan = R04SlotPlan(base64.b64decode(state[ordinal]['slot_plan']['plan_base64'], validate=True))
        if self.policy.admit_before_graph_effect(self.schedule, ordinal, state[ordinal]['request'],
                state[ordinal]['source_result'], state[ordinal]['page'],
                plan, self.context) is not None:
            raise R04CustodyError('Current actual page/request/plan admission incomplete before graph effects')
        self._append('graph_started', ordinal, {'schedule_sha256': self.schedule.sha256,
            'ordinal': ordinal, 'request_sha256': state[ordinal]['request']['request_sha256']})

    def retain_publication(self, ordinal, publication):
        state = self._state()
        if not state[ordinal]['graph_started'] or state[ordinal]['publication'] is not None:
            raise R04CustodyError('Original graph effect awaiting independent publication verification')
        from .r04_slot_plan import R04SlotPlan
        plan = R04SlotPlan(base64.b64decode(state[ordinal]['slot_plan']['plan_base64'], validate=True))
        if self.policy.admit_publication(self.schedule, ordinal, state[ordinal]['page'],
                                         publication, plan, self.context) is not None:
            raise R04CustodyError('Independent prefix oracle/vector/progress verification incomplete')
        self._append('publication', ordinal, {'schedule_sha256': self.schedule.sha256,
            'ordinal': ordinal, 'request_sha256': state[ordinal]['request']['request_sha256'],
            'slot_plan_sha256': state[ordinal]['slot_plan']['plan_sha256'],
            'publication': publication})

    def retain_ack(self, ordinal, ack):
        state = self._state()
        if (state[ordinal]['publication'] is None or not state[ordinal]['ack_started']
                or state[ordinal]['ack'] is not None):
            raise R04CustodyError('Verified publication required before source-specific ACK')
        if self.policy.admit_ack(self.schedule, ordinal, state[ordinal]['publication'],
                                 ack, self.context) is not None:
            raise R04CustodyError('Durable source-specific protected ACK incomplete')
        self._append('ack', ordinal, {'schedule_sha256': self.schedule.sha256,
            'ordinal': ordinal, 'request_sha256': state[ordinal]['request']['request_sha256'],
            'ack': ack})

    def begin_ack_effect(self, ordinal):
        state = self._state()
        if state[ordinal]['publication'] is None or state[ordinal]['ack_started']:
            raise R04CustodyError('Verified publication and one original ACK attempt required')
        if self.policy.admit_before_ack_effect(self.schedule, ordinal,
                state[ordinal]['publication']['publication'], self.context) is not None:
            raise R04CustodyError('Current complete publication/source ACK authority not admitted')
        self._append('ack_started', ordinal, {'schedule_sha256': self.schedule.sha256,
            'ordinal': ordinal, 'request_sha256': state[ordinal]['request']['request_sha256']})

    def state(self):
        return tuple(dict(row) for row in self._state())

    def close(self):
        self.connection.close()
