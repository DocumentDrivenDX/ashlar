"""Separate indexed 5dd/041 count-star installation and bounded string transport.

Availability commits at the final no-clobber ready link. This is cooperating
local-writer scope, not crash durability, native query or publication proof.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
from pathlib import Path
import subprocess

from . import _weft_installation_mechanics as mechanics

from .weft_count_star_package import (
    CountStarInstallationConfig, CountStarInstallationError, VerifiedCountStarPackage,
    inspect_package, read_snapshot, encode_document, decode_document,
    verify_trusted_index, verify_exact_tree, validate_manifest,
    JSON_LIMIT, FILE_LIMIT, PROTOCOL_LIMIT, INSTALLED_SCHEMAS, SCHEMA_BASE, VERSION,
)


def _refuse():
    raise CountStarInstallationError('ASHLAR-WEFT-COUNT-STAR-REFUSED')


def _sha(raw): return hashlib.sha256(raw).hexdigest()


def _marker(primary):
    mechanics.mark_cleanup(primary)


@dataclass(frozen=True)
class CountStarInstallation:
    config: CountStarInstallationConfig
    ready_bytes: bytes
    cleanup_pending: bool = False


def _write_owned(path, raw):
    mechanics.write_owned(path, raw, refuse=_refuse)


def _resource_paths():
    return ('backend-manifest.json',) + tuple('schemas/' + name for name in INSTALLED_SCHEMAS)


def _verify(config, ready_raw, *, ready_present):
    index_raw, entry = verify_trusted_index(config)
    ready = decode_document(ready_raw)
    if type(ready) is not dict or set(ready) != {'format','indexRevision','indexSha256','realizationId','target','observedOS','executable','provenance','resources'}: _refuse()
    if ready['format'] != 'ashlar-weft-count-star-ready/0.1' or ready['indexRevision'] != config.index_revision or ready['indexSha256'] != config.index_sha256 or ready['realizationId'] != config.realization_id or ready['target'] != config.observed_target or ready['observedOS'] != config.observed_os: _refuse()
    expected_exe = {**entry['executable'], 'path':'weft-paths-keys'}
    if ready['executable'] != expected_exe: _refuse()
    binary = read_snapshot(config.output/'weft-paths-keys')
    if len(binary) != expected_exe['bytes'] or _sha(binary) != expected_exe['sha256'] or (config.output/'weft-paths-keys').stat().st_mode & 0o777 != 0o555: _refuse()
    provenance_raw = read_snapshot(config.output/'provenance.json', JSON_LIMIT)
    if (config.output/'provenance.json').stat().st_mode & 0o777 != 0o444: _refuse()
    if ready_present and (config.output/'ready.json').stat().st_mode & 0o777 != 0o444: _refuse()
    if ready['provenance'] != {'path':'provenance.json','sha256':_sha(provenance_raw),'bytes':len(provenance_raw)}: _refuse()
    provenance = decode_document(provenance_raw)
    if type(provenance) is not dict or set(provenance) != {'format','indexRevision','indexSha256','realizationId','manifestHex','custodyHex','resources','qualification'}: _refuse()
    if provenance['format'] != 'ashlar-weft-count-star-provenance/0.1' or provenance['indexRevision'] != config.index_revision or provenance['indexSha256'] != config.index_sha256 or provenance['realizationId'] != config.realization_id: _refuse()
    manifest_raw = bytes.fromhex(provenance['manifestHex']); custody_raw = bytes.fromhex(provenance['custodyHex'])
    for raw, desc in ((manifest_raw,entry['manifest']),(custody_raw,entry['assemblyCustody'])):
        if len(raw) != desc['bytes'] or _sha(raw) != desc['sha256']: _refuse()
    manifest = decode_document(manifest_raw); validate_manifest(manifest)
    if manifest['realizationId'] != config.realization_id or manifest['build']['platform']['observedOS'] != config.observed_os or manifest['executable'] != entry['executable']: _refuse()
    custody = decode_document(custody_raw)
    artifacts = {d['path']:d for d in custody['artifacts']}
    expected_resources = []
    for name in _resource_paths():
        original = manifest['backendManifests'][0]['path'] if name == 'backend-manifest.json' else SCHEMA_BASE + name[len('schemas/'):]
        expected_resources.append({**artifacts[original], 'path':name})
    if ready['resources'] != expected_resources or provenance['resources'] != expected_resources: _refuse()
    for desc in expected_resources:
        raw = read_snapshot(config.output/desc['path'], JSON_LIMIT)
        if len(raw) != desc['bytes'] or _sha(raw) != desc['sha256'] or (config.output/desc['path']).stat().st_mode & 0o777 != 0o444: _refuse()
    verify_exact_tree(config.output, ('weft-paths-keys','provenance.json') + _resource_paths() + (('ready.json',) if ready_present else ()))
    if read_snapshot(config.index_path, JSON_LIMIT) != index_raw: _refuse()
    return CountStarInstallation(config, ready_raw)


def open_installation(config: CountStarInstallationConfig) -> CountStarInstallation:
    """Trust gate precedes caller-controlled installed files; package may be None."""
    try:
        verify_trusted_index(config)
        return _verify(config, read_snapshot(config.output/'ready.json', JSON_LIMIT), ready_present=True)
    except (OSError, ValueError, TypeError, KeyError, RecursionError): _refuse()


def install(config: CountStarInstallationConfig) -> CountStarInstallation:
    """Reverify all package bytes; final ready publication commits availability."""
    verified = inspect_package(config)
    return mechanics.publish_installation(
        config, verified, layout=mechanics.installation_layout('count-star'),
        write=_write_owned, read=read_snapshot, encode=encode_document,
        verify=_verify, refuse=_refuse)


def installed_schema_bundle(installation: CountStarInstallation) -> tuple[tuple[str, bytes], ...]:
    """Owned exact public schemas; caller still owns actual offline validation."""
    opening=open_installation(installation.config)
    if opening.ready_bytes!=installation.ready_bytes:_refuse()
    result=tuple((name,read_snapshot(installation.config.output/'schemas'/name,JSON_LIMIT)) for name in INSTALLED_SCHEMAS)
    if open_installation(installation.config).ready_bytes!=opening.ready_bytes:_refuse()
    return result


def compile_request(installation: CountStarInstallation, request: bytes) -> bytes:
    """Bounded original string transport, no compiler result/native authority."""
    if not isinstance(installation,CountStarInstallation) or type(request)is not bytes or len(request)>PROTOCOL_LIMIT:_refuse()
    opening=open_installation(installation.config)
    if opening.ready_bytes!=installation.ready_bytes:_refuse()
    def validate(raw):
        if len(raw)>JSON_LIMIT:_refuse()
        response=decode_document(raw)
        if type(response)is not dict or response.get('interfaceVersion')not in ('weft-compile/0.4.0','weft-compile/0.4.1') or response.get('status')not in ('compiled','blocked'):_refuse()
        if response['status']=='compiled' and (response['interfaceVersion']!='weft-compile/0.4.1' or response.get('backend',{}).get('backendVersion')!=VERSION or response.get('logicalPlan',{}).get('irVersion')!='weft-ir/0.4.1'):_refuse()
    def closing():
        if open_installation(installation.config).ready_bytes!=opening.ready_bytes:_refuse()
    return mechanics.compile_transport(installation.config.output/'weft-paths-keys',request,
                                       refuse=_refuse,validate_response=validate,closing_verify=closing)
