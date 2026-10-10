"""Installed explicit local composition; source and native authority remain separate."""
import hashlib
import importlib.metadata
import sys
import sqlite3
from pathlib import Path
from .config import HostError, PublishCommerceConfig, QueryCommerceConfig
from .source import original_inputs

PUBLISH_JARS = {
 'io.delta_delta-spark_2.12-3.2.1.jar':'088e187da689a347a6a8556dcb22318e3dfcfb995d807f5e2c19b4d0a7ee9499',
 'io.delta_delta-storage-3.2.1.jar':'4dcc179fc4076bda5060a4038f979c53e1f5916cf04971e28f9441db390763c7',
 'io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar':'8bbd2bbb4c7a3b137e51b4f32f49a30a2ccf70eeb07b452cffa20090dd481b96',
 'io.graphframes_graphframes-spark3_2.12-0.12.3.jar':'69d7909628caf42bcbe1c7e6e5a5a0901040d00583738d931af41bd72af1d3dc',
}
QUERY_JARS = {
 'delta-spark_2.13-4.0.0.jar':'538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e',
 'delta-storage-4.0.0.jar':'9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d',
}


def runtime_paths(config, publish: bool, *, require_fresh: bool = True) -> list[Path]:
    expected = PUBLISH_JARS if publish else QUERY_JARS
    versions = {'pyspark':'3.5.3','delta-spark':'3.2.1','graphframes-py':'0.12.3'} if publish else {'pyspark':'4.0.1','delta-spark':'4.0.0'}
    original_inputs(config.model, config.graph)
    if sys.version_info[:2] != (3,11): raise HostError('qualified-python-required')
    if not publish and sqlite3.sqlite_version != '3.53.1': raise HostError('qualified-sqlite-required')
    if require_fresh and config.output.exists(): raise HostError('fresh-output-required')
    if config.jars.is_symlink() or not config.jars.is_dir(): raise HostError('qualified-jars-required')
    paths = sorted(config.jars.iterdir())
    if {p.name for p in paths} != set(expected): raise HostError('qualified-jars-required')
    for p in paths:
        if p.is_symlink() or not p.is_file(): raise HostError('qualified-jars-required')
        digest = hashlib.sha256()
        with p.open('rb') as stream:
            while chunk := stream.read(1024*1024): digest.update(chunk)
        if digest.hexdigest() != expected[p.name]: raise HostError('qualified-jars-required')
    if any(importlib.metadata.version(k) != v for k,v in versions.items()): raise HostError('qualified-runtime-required')
    return paths


def publish_commerce(config: PublishCommerceConfig) -> dict:
    """Publish the declared original development source; no Truss authority claim."""
    if not isinstance(config, PublishCommerceConfig): raise HostError('invalid-configuration')
    try:
        runtime_paths(config, True)
        from .delta_publication import publish
        return publish(config)
    except Exception as error:
        if isinstance(error, HostError): raise
        raise HostError('publication-refused') from None


def query_commerce(config: QueryCommerceConfig) -> dict:
    """Use the fixed indexed component under original publication and ACK holds."""
    if not isinstance(config, QueryCommerceConfig): raise HostError('invalid-configuration')
    try:
        runtime_paths(config, False)
        from .delta_query import query
        return query(config)
    except Exception as error:
        if isinstance(error, HostError): raise
        raise HostError('query-refused') from None
