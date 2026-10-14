"""Original seventeen analytic queries through the owned public runtime port.

The caller owns the actual source/export intervals and releases a final CLI
report only after those close. This composition closes its native interval
before returning; it creates no publication or source authority.
"""
import hashlib
import json
from .lifecycle import finish, owned_context
from .pack_scenario_queries import MAP_PUPPY, carrier_check, execute
from .puppy_finite_local import (PuppyFiniteLoopbackConfig,
    PuppyGremlinLoopbackConfig, open_puppy_scenario_local)
from .truss_preparation import json_bytes


class ScenarioQueryTrace:
    """Bounded original requests retained before submission, without credentials."""
    def __init__(self, native):
        self.native = native
        self.requests = []
        self.stage = None
        self.bytes = 0

    def set_stage(self, stage):
        # Detach the renderer's description from subsequent caller mutation.
        self.stage = json.loads(json.dumps(stage, allow_nan=False))

    def __call__(self, script, bindings):
        request = {'stage': self.stage, 'query': script, 'bindings': bindings}
        raw = json.dumps(request, ensure_ascii=False, sort_keys=True,
                         separators=(',', ':'), allow_nan=False).encode()
        if len(self.requests) >= 128 or self.bytes + len(raw) > 1048576:
            raise ValueError('Bounded original scenario request inventory required')
        self.requests.append(json.loads(raw))
        self.bytes += len(raw)
        return self.native.query_original(script, bindings, self.native.context)


def execute_owned_puppy_scenarios(config, *, pack_bytes, sources, exports,
                                  preparation, native_map_profile,
                                  maximum_rows=1000, maximum_bytes=1048576,
                                  failure_observer=None, creation_observer=None,
                                  invocation_nonce=None):
    """Run every original query and full opening/closing carrier bags once.

    Preparation is a description; the factory independently admits its exact
    bytes, four table bags, model and actual owners before engine effects.
    Failure does not expose a partial successful report.
    """
    if type(config) not in (PuppyFiniteLoopbackConfig, PuppyGremlinLoopbackConfig):
        raise ValueError('Exact original scenario runtime configuration required')
    language = 'Gremlin' if type(config) is PuppyGremlinLoopbackConfig else 'Cypher'
    if native_map_profile != (MAP_PUPPY if language == 'Gremlin' else None):
        raise ValueError('Protocol-specific original native map profile required')
    preparation = json.loads(json_bytes(preparation, 16777216))
    result = None
    query = None
    def failure(value):
        if failure_observer is not None:
            failure_observer({**value, 'original_requests':
                [] if query is None else list(query.requests)})
    with owned_context(open_puppy_scenario_local(config, pack_bytes=pack_bytes,
            sources=sources, exports=exports, preparation=preparation,
            native_map_profile=native_map_profile,
            maximum_rows=maximum_rows, maximum_bytes=maximum_bytes,
            creation_observer=creation_observer, invocation_nonce=invocation_nonce,
            failure_observer=failure if failure_observer is not None else None)) as native:
        query = ScenarioQueryTrace(native)
        reports = []
        with owned_context(native.hold(native.context)):
            pairs = list(zip(sources, preparation['reports']))
            closings = [{} for _ in pairs]
            primary = None
            try:
                for index, (source, item) in enumerate(pairs):
                    profile = item['profile']
                    _, bindings = source.definition.build(source.model, source.graph)
                    opening = {}
                    for kind in ('node', 'edge'):
                        opening[kind] = carrier_check(profile, kind, language, query,
                            native_map_profile, accepted_bindings=bindings)
                    cases = execute(profile, language, query)
                    reports.append({'pack': profile['pack'], 'opening': opening,
                                    'cases': cases, 'closing': closings[index]})
                if [len(report['cases']) for report in reports] != [8, 9]:
                    raise ValueError('All seventeen original scenario results required')
            except BaseException as error:
                primary = error
            def close_carrier(index, source, item, kind):
                _, bindings = source.definition.build(source.model, source.graph)
                closings[index][kind] = carrier_check(item['profile'], kind,
                    language, query, native_map_profile, accepted_bindings=bindings)
            # Each required closing is attempted independently while held, even
            # when opening or an authored query fails. Authored queries do not retry.
            finish(primary, [lambda index=index, source=source, item=item, kind=kind:
                close_carrier(index, source, item, kind)
                for index, (source, item) in enumerate(pairs)
                for kind in ('node', 'edge')])
            result = {'profile': 'ashlar-original17-puppygraph-native-result/0.1',
                'protocol': language, 'native_map_profile': native_map_profile,
                'image': config.image, 'container_id': native.container_id,
                'database_sha256': preparation['database_sha256'],
                'model_sha256': preparation['model_sha256'],
                'release_sha256s': list(preparation['release_sha256s']),
                'custody_sha256s': list(preparation['custody_sha256s']),
                'reports': reports, 'original_requests': query.requests,
                'scope': 'Original finite analytic queries only; no publication, protected ACK or canonical source grant.'}
    # Capture after held/runtime closing, including the final schema reads.
    # Repeated byte-identical bodies share storage; every occurrence is retained.
    observations = []
    blobs = {}
    for name, raw in native.observations:
        digest = hashlib.sha256(raw).hexdigest()
        if digest in blobs and blobs[digest] != raw:
            raise ValueError('Distinct original observation bytes share a digest')
        blobs[digest] = raw
        observations.append({'name': name, 'sha256': digest, 'bytes': len(raw)})
    result['native_model_observations'] = observations
    result['native_model_blobs'] = blobs
    return result
