"""Isolated local Spark development custody, not UC/ABAC/Subject authority.

This profile observes actual process/session and local Delta state. It does not
connect a CQP to a published Ashlar source or establish production per-use action
or disclosure permissions. Only direct disclosed/original local values qualify.
"""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import importlib
import json
import os
from pathlib import Path
from urllib.parse import unquote, urlsplit
from .lifecycle import finish
from .path_capture import PathCaptureConfig, capture_string_frame

PROFILE = 'ashlar.private-local-current-development/0.1'
SCHEMA_PROFILE = 'ashlar.local-spark-schema-json/0.1'


@dataclass(frozen=True)
class LocalCurrentDevelopmentContext:
    uid: int
    pid: int
    profile: str = PROFILE


def local_current_development_context():
    """Actual local OS principal/process observation; never an application Subject."""
    return LocalCurrentDevelopmentContext(os.getuid(), os.getpid())


def _json(raw):
    if type(raw) is not str or len(raw.encode()) > 4_000_000:
        raise ValueError('Bounded exact original local binding required')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('Duplicate local binding member')
            result[key] = value
        return result
    def reject(_): raise ValueError('Exact local metadata JSON required')
    return json.loads(raw, object_pairs_hook=pairs, parse_float=reject, parse_constant=reject)


def _sha(raw): return hashlib.sha256(raw.encode()).hexdigest()


def _identifier(value):
    if type(value) is not str or not value or len(value.encode()) > 255 or '\0' in value:
        raise ValueError('Exact bounded local identifier required')
    return '`' + value.replace('`', '``') + '`'


class LocalCurrentDevelopmentOwner:
    """Explicit original CQP/local mapping owner; supplies no production permit.

    Binding schemaSha256 uses SCHEMA_PROFILE: SHA256 of the exact UTF-8 Spark
    StructType.json() string at the selected version. This is a local convention,
    not a Weft-defined or Unity Catalog schema fingerprint.
    """
    def __init__(self, spark, original_child, original_binding, context):
        native = importlib.import_module('weft')
        if type(original_child) is not native.CurrentQueryPreparation:
            raise TypeError('Original installed native current-query child required')
        parent = native.CurrentQueryPreparation.source_preparation(original_child)
        if type(parent) is not native.ApplicationLineagePreparation:
            raise TypeError('Original installed native parent required')
        self.spark = spark; self._original_spark = spark; self._original_spark_context = spark.sparkContext
        self.child = original_child; self.parent = parent
        self.context = context; self.native = native; self.original_binding = original_binding
        self.packet = native.CurrentQueryPreparation.native_request_json(original_child)
        self.request = native.ApplicationLineagePreparation.source_request_json(parent)
        self.designation = native.CurrentQueryPreparation.designation_json(original_child)
        binding = _json(original_binding); packet = _json(self.packet)
        if (binding.get('sourceRequestSha256') != _sha(self.request)
                or binding.get('designationSha256') != _sha(self.designation)
                or binding.get('sourceBindingSha256') != packet['binding']['sha256']
                or [(v['scan'], v['target']) for v in binding['scans']] !=
                   [(v['scan'], v['record']) for v in packet['scans']]):
            raise ValueError('Original local binding/source correspondence differs')
        if any(value.get('mode') not in (None, 'disclosed')
               or value.get('originalAction') is not None
               or value.get('exposure') not in (None, 'original') for value in binding['values']):
            raise PermissionError('Local development does not establish action/transform authority')
        self.master = spark.sparkContext.master
        self.application_id = spark.sparkContext.applicationId
        if self.master != 'local[1]': raise PermissionError('Explicit isolated local[1] Spark required')
        self.active = False
        self._require_identity(original_child, original_binding, context)
        self.original_observation = self._observe()

    def _require_identity(self, child, binding, context):
        if (child is not self.child or binding != self.original_binding or context is not self.context
                or type(context) is not LocalCurrentDevelopmentContext
                or context.profile != PROFILE or type(context.uid) is not int or type(context.pid) is not int
                or context.uid != os.getuid() or context.pid != os.getpid()
                or self.spark is not self._original_spark
                or self.spark.sparkContext is not self._original_spark_context
                or self.spark.sparkContext.master != self.master
                or self.spark.sparkContext.applicationId != self.application_id
                or self.native.CurrentQueryPreparation.source_preparation(child) is not self.parent
                or self.native.CurrentQueryPreparation.native_request_json(child) != self.packet
                or self.native.ApplicationLineagePreparation.source_request_json(self.parent) != self.request
                or self.native.CurrentQueryPreparation.designation_json(child) != self.designation):
            raise PermissionError('Actual original local process/session/query changed')

    def _observe(self):
        observations = []
        for scan in _json(self.original_binding)['scans']:
            relation = scan['relation']
            name = '.'.join(_identifier(relation[k]) for k in ('catalog', 'schema', 'table'))
            detail = self.spark.sql('DESCRIBE DETAIL ' + name).selectExpr(
                'CAST(id AS STRING) AS id', 'CAST(format AS STRING) AS format',
                'CAST(location AS STRING) AS location')
            limits = PathCaptureConfig(1, 65536, 131072)
            rows = capture_string_frame(detail, config=limits)['rows']
            if len(rows) != 1 or rows[0][0] != relation['uuid'] or rows[0][1].lower() != 'delta':
                raise ValueError('Actual native Delta table identity differs')
            location = rows[0][2]; parsed = urlsplit(location)
            if parsed.scheme != 'file' or parsed.netloc not in ('', 'localhost'):
                raise PermissionError('Only original local filesystem Delta tables supported')
            path = Path(unquote(parsed.path)).resolve()
            if not path.is_relative_to(Path('/private/tmp')):
                raise PermissionError('Explicit private temporary development table required')
            version = relation['version']
            if type(version) is not str or not version.isascii() or not version.isdecimal() or len(version) > 19 or str(int(version)) != version or int(version) > 2**63-1:
                raise ValueError('Exact native Delta version required')
            history = self.spark.sql('DESCRIBE HISTORY ' + name + ' LIMIT 1').selectExpr('CAST(version AS STRING) AS version')
            if capture_string_frame(history, config=limits)['rows'] != [[version]]:
                raise ValueError('Local current Delta version changed')
            source = self.spark.read.format('delta').option('versionAsOf', version).table(name)
            schema = source.schema.json()
            if _sha(schema) != relation['schemaSha256']:
                raise ValueError('Actual local Spark schema JSON differs')
            observations.append((scan['scan'], rows[0][0], location, version, schema))
        return tuple(observations)

    def backend(self, *, capture_limits, source_limits, mode='contributors'):
        """Compose only the exact observed Spark session with its local owner."""
        from .databricks_current_spark import SparkCurrentQueryBackend
        self._require_identity(self.child, self.original_binding, self.context)
        return SparkCurrentQueryBackend(self._original_spark, self,
            capture_limits, source_limits, mode)

    @contextmanager
    def open_current_query(self, original_child, authenticated_context):
        if self.active: raise PermissionError('Local development owner already held')
        self._require_identity(original_child, self.original_binding, authenticated_context)
        if self._observe() != self.original_observation:
            raise PermissionError('Local native cut changed before opening')
        self.active = True; primary = None
        try: yield self
        except BaseException as error: primary = error
        def close():
            try: self.renew(original_child, self.original_binding, authenticated_context)
            finally: self.active = False
        finish(primary, [close])

    def binding(self):
        if not self.active: raise PermissionError('Original local owner is not held')
        return self.original_binding

    def admit(self, child, binding, context):
        self.renew(child, binding, context)

    def renew(self, child, binding, context):
        if not self.active: raise PermissionError('Original local owner is not held')
        self._require_identity(child, binding, context)
        if self._observe() != self.original_observation:
            raise PermissionError('Original local Delta identity/version/schema changed')
