# SPDX-License-Identifier: MIT OR Apache-2.0
"""Provisional original two-hop execution through existing real public owners.

This port opens no session, installs no compiler and publishes no report. Its
caller must close the outer reader, source, installation and native session.
"""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

from ashlar.weft_path_decode import PathDecodeConfig
from ashlar.weft_paths_distribution import (PathsDistributionPaths,
    compile_paths_distribution, installed_paths_schema_bundle)
from .delta_custody import encoded
from .lifecycle import finish
from .path_admission import PathAdmissionConfig
from .path_capture import PathCaptureConfig
from .path_execution import PathExecutionConfig, execute_commerce_path
from .path_schema import make_offline_path_schema_validation
from .supply_chain_common_requests import prepare_supply_chain_two_hop
from .supply_chain_finite_source import FiniteSupplyChainSource, UMF_REVISION
from .supply_chain_publication import FiniteSupplyChainDriver
from .supply_chain_query import finite_public_source
from .supply_chain_reader import FiniteSupplyChainProvider
from .supply_chain_request import source_snapshot


@dataclass(frozen=True)
class SupplyChainTwoHopQueryConfig:
    distribution: PathsDistributionPaths
    maximum_artifact_bytes: int
    capture: PathCaptureConfig
    decoder: PathDecodeConfig

    def __post_init__(self):
        if (type(self.distribution) is not PathsDistributionPaths
                or self.distribution.package is not None
                or any(not isinstance(p, Path) or not p.is_absolute()
                       for p in (self.distribution.index, self.distribution.output))
                or type(self.maximum_artifact_bytes) is not int
                or not 1 <= self.maximum_artifact_bytes <= 16 * 1024 * 1024
                or type(self.capture) is not PathCaptureConfig
                or type(self.decoder) is not PathDecodeConfig):
            raise ValueError('Explicit existing Paths installation and typed finite bounds required')


@dataclass(frozen=True)
class _OpenedFinitePath:
    """Pass through real owner/context only; this adapter grants no authority."""
    provider: FiniteSupplyChainProvider
    context: object
    model: bytes
    graph: bytes
    bindings: dict
    source: FiniteSupplyChainSource
    original_native_files: dict

    def native_files(self):
        self.source.metadata()
        return {name: hashlib.sha256(raw).hexdigest() for name, raw in
                (('model', self.model), ('graph', self.graph),
                 ('finite_source_receipt', self.source.receipt),
                 ('source_transaction', self.source.raw))}


def query_supply_chain_two_hop(driver, pack, aliases, config, *, observe):
    """Run complete twelve-pair bags; any failure withholds combined evidence."""
    if (type(config) is not SupplyChainTwoHopQueryConfig
            or type(driver) is not FiniteSupplyChainDriver
            or type(driver.source) is not FiniteSupplyChainSource
            or driver.manifest is None or not callable(observe)):
        raise ValueError('Actual committed finite driver and typed query configuration required')
    source = driver.source
    source.renew()
    original = source_snapshot([source.bindings, driver.manifest, aliases])
    registry = [{'table': t.table, 'uuid': t.uuid} for t in driver.transport.targets.values()]
    prepared = prepare_supply_chain_two_hop(pack, source.model, source.graph,
                                            original[0], original[1], registry, original[2])
    requests = []
    for ordinal, query in enumerate(prepared['queries']):
        raw = (encoded(query['request']) + '\n').encode()
        if len(raw) > config.maximum_artifact_bytes:
            raise ValueError('Complete original request exceeds artifact bound')
        requests.append((ordinal, query, raw))
    schemas = None
    results = []
    primary = None
    try:
        schemas = dict(installed_paths_schema_bundle(config.distribution))
        selected = {name: schemas[name] for name in
                    ('compile-request-v0.4.schema.json', 'compile-response-v0.4.schema.json',
                     'logical-plan-v0.4.schema.json')}
        execution = PathExecutionConfig(
            PathAdmissionConfig(config.maximum_artifact_bytes,
                                make_offline_path_schema_validation(selected), 'paths'),
            config.capture, config.decoder,
            finite_public_source(source, config.maximum_artifact_bytes), UMF_REVISION)
        for ordinal, query, raw in requests:
            responses = []
            for iteration in range(2):
                response = compile_paths_distribution(config.distribution, raw)
                if type(response) is not bytes or len(response) > config.maximum_artifact_bytes:
                    raise ValueError('Complete original compiler response exceeds artifact bound')
                observe(ordinal, iteration, raw, response)
                responses.append(response)
            if responses[0] != responses[1]:
                raise ValueError('Original independently recompiled protocol differs')
            request = query['request']
            provider = FiniteSupplyChainProvider(driver, original[2], json.loads(request['target']['bindingJson']))
            fingerprints = {name: hashlib.sha256(value).hexdigest() for name, value in
                (('model', source.model), ('graph', source.graph),
                 ('finite_source_receipt', source.receipt), ('source_transaction', source.raw))}
            opened = _OpenedFinitePath(provider, driver.context, source.model,
                                      source.graph, original[0], source, fingerprints)
            def oracle(model, graph, request_copy, q=query):
                if model != source.model or graph != source.graph or request_copy != q['request']:
                    raise ValueError('Original independent oracle scope differs')
                return {'rows': q['expected_rows'],
                        'witnesses': {'relationships': q['relationships'], 'complete_pairs': 12},
                        'scope': 'Complete original occurrence bag and empty source rows; no native authority'}
            evidence = execute_commerce_path(opened, request, json.loads(responses[0]),
                json.loads(responses[1]), config=execution, original_oracle=oracle)
            results.append({'ordinal': ordinal, 'relationships': query['relationships'],
                            'requestBytes': raw, 'responseBytes': responses[0], 'evidence': evidence})
    except BaseException as error:
        primary = error
    def close_metadata():
        if source_snapshot([source.bindings, driver.manifest, aliases]) != original:
            raise ValueError('Original closing source/publication metadata differs')
    def close_installation():
        if schemas is not None and dict(installed_paths_schema_bundle(config.distribution)) != schemas:
            raise ValueError('Original installed schema bundle differs')
    finish(primary, [source.renew, close_metadata, close_installation])
    return {'case': prepared['case'], 'queries': results,
            'qualification': 'Provisional complete original two-hop evidence only; outer reader/source/installation/native cleanup required. Other six common cases and ACK remain separate.'}
