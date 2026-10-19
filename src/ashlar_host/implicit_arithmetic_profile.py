# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed additive 0.4.2 host settings; package and execution authority are separate.

The owning reviewed public Weft schemas define this metadata subset. This module
imports no admission, compiler, engine or host-composition owner. Callers cannot
select another backend/version/schema set through these configuration types.
"""
from dataclasses import dataclass
import hashlib
from types import MappingProxyType
from typing import Callable, Mapping, Optional
from ashlar.weft_path_decode import PathDecodeConfig
from .path_capture import PathCaptureConfig

class ImplicitArithmeticProfileError(ValueError):
    """Payload-free explicit profile/configuration refusal."""

@dataclass(frozen=True)
class ImplicitArithmeticDescriptor:
    name: str
    schemas: tuple
    backend: tuple
    compile_version: str
    dialect: str
    ir_version: str
    compiler_entrypoint: str
    capabilities: frozenset

PROFILE = ImplicitArithmeticDescriptor(
    'implicit-arithmetic',
    (('compile-request-v0.4.2.schema.json', '8a0da7aa576b0fc08fd65621cf99e70b47a26828c719dee1c1d8d9c974298de8'),
     ('compile-response-v0.4.2.schema.json', '1967e1cabee8dc80119376ad15d0e58e16c916962d414d78fdc0247603b42da3'),
     ('logical-plan-v0.4.2.schema.json', 'a70ff489be0a9621b43623f1d69f0a1490345c8a1a4bf654121b88f0c7dd8ac1')),
    (('backendId', 'ashlar.databricks.paths-keys'),
     ('backendVersion', '0.4.2-implicit-arithmetic-labels-candidate'),
     ('interfaceVersion', 'weft-backend/0.3.0'),
     ('targetProfile', 'spark4-delta4-paths-keys-candidate')),
    'weft-compile/0.4.2', 'weft-sql/0.4.2', 'weft-ir/0.4.2',
    'compile_implicit_arithmetic_json',
    frozenset(('aggregate', 'aggregate.count', 'aggregate.countDistinct', 'aggregate.countDistinct.optional', 'aggregate.havingCountDistinctGreater', 'aggregate.havingCountStarGreater', 'aggregate.pathTargetDistinctCount', 'and', 'arithmetic.*', 'arithmetic.+', 'arithmetic.-', 'arithmetic.compareExact', 'arithmetic.exact.decimal', 'arithmetic.exact.integer', 'arithmetic.negate', 'compare.greaterEqual', 'compare.less', 'compare.lessEqual', 'compare.lexicographicGreater', 'compare.notEqual', 'compare.nullAwareStringEqual', 'compare.scalarJoin', 'equal', 'filter', 'group', 'innerJoin', 'join.left', 'limit', 'order.asc', 'parameter.named', 'predicate.nativeNull', 'predicate.stringIn', 'project', 'project.distinct', 'project.implicitArithmeticLabels', 'project.positionedOutputs', 'relationship.boundedKeys', 'relationship.inverse', 'relationship.pathExpansion', 'relationship.twoHopPaths', 'result.pathOccurrences', 'scan', 'type.boolean', 'type.decimal', 'type.integer', 'type.integer.unbounded', 'type.string', 'value.nativeNull', 'value.outerJoinPresence', 'value.presence')))

@dataclass(frozen=True)
class ImplicitArithmeticSchemaValidation:
    schemas: Mapping[str, bytes]
    validate: Callable[[Mapping[str, bytes], str, dict], None]

    def __post_init__(self):
        if (type(self.schemas) is not dict and type(self.schemas) is not type(MappingProxyType({}))) or len(self.schemas) != 3 or any(type(k) is not str for k in self.schemas) or not callable(self.validate):
            raise ImplicitArithmeticProfileError('Complete trusted offline schema port required')
        expected = dict(PROFILE.schemas)
        if set(self.schemas) != set(expected):
            raise ImplicitArithmeticProfileError('Exact selected schema inventory required')
        owned = {}
        for name, digest in PROFILE.schemas:
            raw = self.schemas[name]
            if type(raw) is not bytes or hashlib.sha256(raw).hexdigest() != digest:
                raise ImplicitArithmeticProfileError('Exact selected public schema bytes required')
            owned[name] = raw
        object.__setattr__(self, 'schemas', MappingProxyType(owned))

@dataclass(frozen=True)
class ImplicitArithmeticAdmissionConfig:
    maximum_artifact_bytes: int
    schema_validation: ImplicitArithmeticSchemaValidation
    profile: str = 'implicit-arithmetic'

    def __post_init__(self):
        if type(self.profile) is not str or self.profile != PROFILE.name:
            raise ImplicitArithmeticProfileError('Explicit implicit-arithmetic selection required')
        if type(self.maximum_artifact_bytes) is not int or not 1 <= self.maximum_artifact_bytes <= 16777216:
            raise ImplicitArithmeticProfileError('Explicit finite artifact bound required')
        if type(self.schema_validation) is not ImplicitArithmeticSchemaValidation:
            raise ImplicitArithmeticProfileError('Selected trusted offline schema port required')
        self.schema_validation.__post_init__()

@dataclass(frozen=True)
class ImplicitArithmeticExecutionConfig:
    admission: ImplicitArithmeticAdmissionConfig
    capture: PathCaptureConfig
    decoder: PathDecodeConfig
    public_source: Optional[Callable]
    public_source_revision: Optional[str]
    native_observer: Optional[Callable] = None
    maximum_public_source_bytes: Optional[int] = None

    def __post_init__(self):
        if type(self.admission) is not ImplicitArithmeticAdmissionConfig or type(self.capture) is not PathCaptureConfig or type(self.decoder) is not PathDecodeConfig:
            raise ImplicitArithmeticProfileError('Explicit typed execution settings required')
        self.admission.__post_init__(); self.capture.__post_init__(); self.decoder.__post_init__()
        if self.public_source is not None and (not callable(self.public_source) or type(self.public_source_revision) is not str or not self.public_source_revision):
            raise ImplicitArithmeticProfileError('Explicit source producer and revision required')
        if self.native_observer is not None and not callable(self.native_observer):
            raise ImplicitArithmeticProfileError('Callable native observation port required')
        if self.maximum_public_source_bytes is not None and (type(self.maximum_public_source_bytes) is not int or not 0 < self.maximum_public_source_bytes <= 16777216):
            raise ImplicitArithmeticProfileError('Explicit bounded whole public source receipt required')
