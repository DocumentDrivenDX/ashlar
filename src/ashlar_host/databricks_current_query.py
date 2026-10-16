"""Conditional native-current composition; no actual Unity Catalog provider exists here.

A caller must explicitly supply a trusted backend owner. There is no default
provider, declaration-based authority or SQL repair path.
"""
from __future__ import annotations
from dataclasses import dataclass, field
import hashlib
import importlib
import json
from typing import ContextManager, Protocol
from .lifecycle import finish, owned_context


class HeldCurrentQuery(Protocol):
    """Backend-owned authenticated cut and independent result/source verifier."""
    def binding(self) -> str: ...
    def admit(self, original_child, original_binding: str, authenticated_context) -> None: ...
    def renew(self, original_child, original_binding: str, authenticated_context) -> None: ...
    def capture(self, original_artifact: str): ...
    def verify(self, original_child, original_artifact: str, captured,
               authenticated_context) -> str: ...


class RefinedHeldCurrentQuery(HeldCurrentQuery, Protocol):
    def refinement_input(self, original_child, original_artifact: str, captured,
                         authenticated_context) -> str: ...


class CurrentQueryBackend(Protocol):
    def open_current_query(self, original_child, authenticated_context) -> ContextManager[HeldCurrentQuery]: ...


@dataclass(frozen=True)
class CurrentQueryResult:
    original_binding: str
    original_artifact: str
    verified_result: str
    refinement_report: str | None = field(default=None, repr=False)
    # Compiler candidate metadata never becomes an authority grant.
    authority: bool = field(default=False, init=False)
    security_ready: bool = field(default=False, init=False)


def _json(raw, maximum):
    if type(raw) is not str or len(raw.encode('utf-8')) > maximum:
        raise ValueError('Bounded original JSON text required')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('Duplicate JSON member')
            result[key] = value
        return result
    def constant(_): raise ValueError('Finite JSON required')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant, parse_float=constant)


def execute_databricks_current_query(original_child, authenticated_context,
                                      backend: CurrentQueryBackend, *, mode='contributors',
                                      maximum_result_bytes=16_777_216, refinement=False):
    """Use a trusted backend owner; absent native authority never has a fallback.

    The injected backend must authenticate the actual caller/current cut, admit
    the original child and binding independently, verify complete actual schemas,
    populations, cells and bags, and revalidate all authority at owner closing.
    JSON/compiler declarations are never substitutes for those backend checks.
    """
    if type(refinement) is not bool:
        raise ValueError('Exact refinement opt-in required')
    if type(maximum_result_bytes) is not int or not 1 <= maximum_result_bytes <= 16_777_216:
        raise ValueError('Exact positive bounded result-byte limit required')
    if type(mode) is not str or mode not in ('contributors', 'sum') or authenticated_context is None or backend is None:
        raise ValueError('Explicit current-query mode, context and backend required')
    native = importlib.import_module('weft')
    cls = native.CurrentQueryPreparation
    if type(original_child) is not cls:
        raise TypeError('Original installed native CurrentQueryPreparation required')
    source = cls.source_preparation(original_child)
    if type(source) is not native.ApplicationLineagePreparation:
        raise TypeError('Original installed native parent required')
    lower = getattr(cls, 'lower_databricks_current_'+mode+'_candidate_json')
    checker = None
    if refinement:
        checker = getattr(cls, 'check_databricks_current_'+mode+'_candidate_json', None)
        if not callable(checker):
            raise ValueError('Installed native refinement capability required')
    result = None
    with owned_context(backend.open_current_query(original_child, authenticated_context)) as held:
        binding = None
        primary = None
        def check():
            if cls.source_preparation(original_child) is not source or held.binding() != binding:
                raise ValueError('Original child or materialization binding changed')
        def require_none(action):
            if action() is not None:
                raise ValueError('Backend admission/renewal must complete or raise')
        def admit():
            require_none(lambda: held.admit(original_child, binding, authenticated_context))
        def renew():
            require_none(lambda: held.renew(original_child, binding, authenticated_context))
        try:
            binding = held.binding()
            _json(binding, 4_000_000)
            admit()
            renew()
            artifact = lower(original_child, binding)
            compiled = _json(artifact, 16_777_216)
            version = 'weft.databricks.current-scalar/' + ('0.1' if mode == 'contributors' else '0.2') + '-candidate'
            if (type(compiled) is not dict or compiled.get('version') != version
                    or compiled.get('authority') is not False
                    or compiled.get('securityReady') is not False
                    or compiled.get('nativeAdmission') != 'required'
                    or compiled.get('materializationBindingSha256') != hashlib.sha256(binding.encode()).hexdigest()
                    or type(compiled.get('sql')) is not str or not compiled['sql']
                    or type(compiled.get('parameters')) is not list):
                raise ValueError('Original conditional compiler artifact required')
            check()
            if refinement and not callable(getattr(held, 'refinement_input', None)):
                raise ValueError('Backend-owned refinement input port required')
            captured = held.capture(artifact)
            refinement_report = None
            if refinement:
                frame = held.refinement_input(original_child, artifact, captured, authenticated_context)
                _json(frame, 16_777_216)
                refinement_report = checker(original_child, binding, frame)
                report = _json(refinement_report, 16_777_216)
                anchors = {'sourceRequestSha256': compiled.get('sourceRequestSha256'),
                    'designationSha256': compiled.get('designationSha256'),
                    'materializationBindingSha256': hashlib.sha256(binding.encode()).hexdigest(),
                    'compiledArtifactSha256': hashlib.sha256(artifact.encode()).hexdigest(),
                    'refinementInputSha256': hashlib.sha256(frame.encode()).hexdigest()}
                keys = {'version', 'matched', 'authority', 'securityReady', 'nativeAdmission'} | set(anchors)
                if (type(report) is not dict or set(report) != keys
                        or report.get('version') != 'weft.current-query.refinement-result/0.1-candidate'
                        or report.get('matched') is not True or report.get('authority') is not False
                        or report.get('securityReady') is not False or report.get('nativeAdmission') != 'required'
                        or any(type(value) is not str or len(value) != 64
                            or any(c not in '0123456789abcdef' for c in value)
                            or report[key] != value for key, value in anchors.items())):
                    raise ValueError('Exact same-original refinement report required')
                check()
            verified = held.verify(original_child, artifact, captured, authenticated_context)
            _json(verified, maximum_result_bytes)
            result = CurrentQueryResult(binding, artifact, verified, refinement_report)
        except BaseException as error:
            primary = error
        # Independently attempt every closing observation, including on refusal
        # and cancellation; context exit additionally owns native cleanup.
        finish(primary, [renew, admit, check])
    return result
