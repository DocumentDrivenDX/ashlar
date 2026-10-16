"""Explicit Spark current-query mechanics; injected owners retain all authority.

No Spark session or Unity Catalog authority is discovered or created. The caller
owns Spark lifetime; the authority context owns its authenticated cut and cleanup.
"""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import importlib
import json
from typing import Protocol, Iterator
from ashlar.schema import decode_retained_json
from .lifecycle import owned_context, finish
from .path_capture import PathCaptureConfig, capture_string_frame


def _text_json(raw, maximum):
    if type(raw) is not str or len(raw.encode('utf-8')) > maximum:
        raise ValueError('Bounded original current-query JSON required')
    return decode_retained_json(raw.encode('utf-8'))


def _encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _sha(raw):
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


class CurrentSparkAuthority(Protocol):
    """Trusted owner of actual caller, original binding and current native cut."""
    def open_current_query(self, original_child, authenticated_context): ...


@dataclass(frozen=True, repr=False)
class CurrentSparkCapture:
    """Detached native positional capture, accepted only by its creating held owner."""
    native_json: str


@dataclass(frozen=True, repr=False)
class SparkCurrentQueryBackend:
    spark: object
    authority_owner: CurrentSparkAuthority
    capture_limits: PathCaptureConfig
    source_limits: PathCaptureConfig
    mode: str = 'contributors'

    def __post_init__(self):
        if (not callable(getattr(self.spark, 'sql', None))
                or not callable(getattr(self.authority_owner, 'open_current_query', None))
                or type(self.capture_limits) is not PathCaptureConfig
                or type(self.source_limits) is not PathCaptureConfig
                or type(self.mode) is not str or self.mode not in ('contributors', 'sum')):
            raise ValueError('Explicit Spark current-query owners and limits required')
        if self.source_limits.maximum_cell_bytes > 65536:
            raise ValueError('Source capture cell ceiling is 64 KiB')
        for limits in (self.capture_limits, self.source_limits):
            limits.__post_init__()
            if (limits.maximum_rows > 10000 or limits.maximum_cell_bytes > 1048576
                    or limits.maximum_total_cell_bytes > 16777216):
                raise ValueError('Finite current-query capture ceilings required')

    @contextmanager
    def open_current_query(self, original_child: object, authenticated_context: object) -> Iterator[object]:
        self.__post_init__()
        native = importlib.import_module('weft')
        if type(original_child) is not native.CurrentQueryPreparation or authenticated_context is None:
            raise ValueError('Actual original native child and explicit context required')
        if not callable(getattr(type(original_child), 'check_databricks_current_'+self.mode+'_candidate_json', None)):
            raise ValueError('Actual native refinement checker required before capture')
        parent = original_child.source_preparation()
        if type(parent) is not native.ApplicationLineagePreparation:
            raise ValueError('Actual original native parent required')
        with owned_context(self.authority_owner.open_current_query(original_child, authenticated_context)) as authority:
            held = _HeldSparkCurrent(self, authority, original_child, parent, authenticated_context)
            primary = None
            try: yield held
            except BaseException as error: primary = error
            finish(primary, [lambda: held.renew(original_child, held.binding(), authenticated_context), held.close])


class _HeldSparkCurrent:
    def __init__(self, backend, authority, child, parent, context):
        self.backend = backend; self.authority = authority
        self.child = child; self.parent = parent; self.context = context
        self.original_binding = authority.binding()
        _text_json(self.original_binding, 4000000)
        self.original_artifact = None; self.captured = None; self.frame = None
        self.captured_json = None
        self.capture_started = False
        self.active = True

    def close(self):
        self.active = False

    def binding(self):
        if not self.active:
            raise ValueError('Current-query owner is closed')
        if (self.child.source_preparation() is not self.parent
                or self.authority.binding() != self.original_binding):
            raise ValueError('Held original binding or native parent changed')
        return self.original_binding

    def _same(self, child, binding, context):
        if child is not self.child or context is not self.context or binding != self.binding():
            raise ValueError('Exact held current-query invocation required')

    def admit(self, child, binding, context):
        self._same(child, binding, context)
        if self.authority.admit(child, binding, context) is not None:
            raise ValueError('Authority admission must complete or raise')

    def renew(self, child, binding, context):
        self._same(child, binding, context)
        if self.authority.renew(child, binding, context) is not None:
            raise ValueError('Authority renewal must complete or raise')

    def capture(self, original_artifact):
        self.binding()
        if self.capture_started:
            raise ValueError('One original query capture per held invocation required')
        self.capture_started = True
        expected = getattr(type(self.child), 'lower_databricks_current_'+self.backend.mode+'_candidate_json')(
            self.child, self.binding())
        if type(original_artifact) is not str or original_artifact != expected:
            raise ValueError('Unchanged actual original compiler artifact required')
        artifact = _text_json(original_artifact, 16777216)
        parameters = artifact['parameters']; slots = {}
        if type(parameters) is not list or len(parameters) > 1024:
            raise ValueError('Bounded ordered original parameters required')
        for ordinal, parameter in enumerate(parameters, 1):
            if (type(parameter) is not dict or type(parameter.get('ordinal')) is not int
                    or parameter['ordinal'] != ordinal or parameter.get('marker') != 'wft_'+str(ordinal)
                    or parameter.get('kind') not in ('source-value', 'internal-lineage-label')
                    or parameter.get('nativeType') not in ('STRING', 'BIGINT')
                    or type(parameter.get('value')) is not str):
                raise ValueError('Exact original native parameter inventory required')
            slots[parameter['marker']] = parameter['value']
        public = artifact['resultColumns']; private = artifact['privateResultColumns']
        if (type(public) is not list or type(private) is not list or not public or len(private) != 1
                or any(type(x) is not dict or type(x.get('position')) is not int or type(x.get('name')) is not str for x in public+private)
                or [x['position'] for x in public] != list(range(len(public)))
                or private[0]['position'] != len(public)
                or private[0]['name'] != '__weft_contributors'
                or private[0].get('purpose') != 'source-row-contributor-bag'
                or private[0].get('encoding') != 'json-text:weft.stage-contributor-bag/0.1-candidate'
                or private[0].get('multiplicity') != 'bag'
                or private[0].get('maxKeyUtf8Bytes') != 256
                or (len(public)+1)*self.backend.capture_limits.maximum_rows > 100000):
            raise ValueError('Bounded complete positional result descriptor required')
        self.admit(self.child, self.binding(), self.context)
        self.renew(self.child, self.binding(), self.context)
        captured = capture_string_frame(self.backend.spark.sql(artifact['sql'], args=slots),
            config=self.backend.capture_limits)
        if [x[0] for x in captured['schema']] != ['output_'+str(i) for i in range(len(public))]+[private[0]['name']]:
            raise ValueError('Exact complete positional result schema required')
        self.original_artifact = original_artifact
        self.captured_json = _encoded(captured)
        self.captured = CurrentSparkCapture(self.captured_json)
        return self.captured

    def _capture(self, child, artifact, captured, context):
        self._same(child, self.original_binding, context)
        if (artifact is not self.original_artifact or captured is not self.captured or captured is None
                or captured.native_json != self.captured_json):
            raise ValueError('Original invocation artifact and capture identity required')
        return _text_json(captured.native_json, 33554432)

    def refinement_input(self, child, artifact, captured, context):
        native = self._capture(child, artifact, captured, context)
        if self.frame is None:
            from .databricks_current_source_capture import capture_current_query_sources, CurrentSourceCapture
            self.renew(child, self.binding(), context)
            source = capture_current_query_sources(self.backend.spark, self.binding(), config=self.backend.source_limits)
            if type(source) is not CurrentSourceCapture or source.original_binding != self.binding():
                raise ValueError('Complete same-binding original source capture required')
            compiled = _text_json(artifact, 16777216)
            self.frame = _encoded({'version':'weft.current-query.refinement-input/0.1-candidate',
                'sourceRequestSha256':compiled['sourceRequestSha256'],
                'designationSha256':compiled['designationSha256'],
                'materializationBindingSha256':_sha(self.binding()),
                'compiledArtifactSha256':_sha(artifact),
                'scans':_text_json(source.scans_json, 16777216), 'resultRows':native['rows']})
            _text_json(self.frame, 16777216)
            self.renew(child, self.binding(), context)
        return self.frame

    def verify(self, child, artifact, captured, context):
        native = self._capture(child, artifact, captured, context)
        frame = self.refinement_input(child, artifact, captured, context)
        method = getattr(type(child), 'check_databricks_current_'+self.backend.mode+'_candidate_json')
        report = _text_json(method(child, self.binding(), frame), 16777216)
        compiled = _text_json(artifact, 16777216)
        anchors = {'sourceRequestSha256':compiled['sourceRequestSha256'],
            'designationSha256':compiled['designationSha256'],
            'materializationBindingSha256':_sha(self.binding()),
            'compiledArtifactSha256':_sha(artifact),'refinementInputSha256':_sha(frame)}
        if (type(report) is not dict or set(report) != set(anchors)|{'version','matched','authority','securityReady','nativeAdmission'}
                or report['version'] != 'weft.current-query.refinement-result/0.1-candidate'
                or report['matched'] is not True or report['authority'] is not False
                or report['securityReady'] is not False or report['nativeAdmission'] != 'required'
                or any(report[k] != value for k,value in anchors.items())):
            raise ValueError('Closed original conditional refinement match required')
        self.renew(child, self.binding(), context)
        positions = [column['position'] for column in compiled['resultColumns']]
        return _encoded({'columns':[column['name'] for column in compiled['resultColumns']],
            'rows':[[row[position] for position in positions] for row in native['rows']]})
