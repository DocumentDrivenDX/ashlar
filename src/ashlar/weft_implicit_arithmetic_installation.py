"""Fixed 042 readonly installation and original string transport; no native grant."""
from __future__ import annotations
from dataclasses import dataclass
from . import _weft_installation_mechanics as mechanics
from .weft_implicit_arithmetic_package import (
    ImplicitArithmeticInstallationConfig, ImplicitArithmeticInstallationError,
    inspect_package,read_snapshot,encode_document,decode_document,verify_trusted_index,
    verify_exact_tree,validate_manifest,sha,refuse,JSON_LIMIT,PROTOCOL_LIMIT,
    INSTALLED_SCHEMAS,SCHEMA_BASE,VERSION,MANIFEST_SHA256,CUSTODY_SHA256)

@dataclass(frozen=True)
class ImplicitArithmeticInstallation:
    config:ImplicitArithmeticInstallationConfig
    ready_bytes:bytes
    cleanup_pending:bool=False


def _verify(config,ready_raw,*,ready_present):
    index_raw,entry=verify_trusted_index(config);layout=mechanics.installation_layout('implicit-arithmetic')
    ready=decode_document(ready_raw)
    if type(ready)is not dict or set(ready)!={'format','indexRevision','indexSha256','realizationId','target','observedOS','executable','provenance','resources'}:refuse()
    if (ready['format'],ready['indexRevision'],ready['indexSha256'],ready['realizationId'],ready['target'],ready['observedOS'])!=(layout.ready_format,config.index_revision,config.index_sha256,config.realization_id,config.observed_target,config.observed_os):refuse()
    provenance_raw=read_snapshot(config.output/'provenance.json',JSON_LIMIT);provenance=decode_document(provenance_raw)
    if type(provenance)is not dict or set(provenance)!={'format','indexRevision','indexSha256','realizationId','manifestHex','custodyHex','resources','qualification'}:refuse()
    if (provenance['format'],provenance['indexRevision'],provenance['indexSha256'],provenance['realizationId'])!=('ashlar-weft-implicit-arithmetic-provenance/0.1',config.index_revision,config.index_sha256,config.realization_id):refuse()
    for key in ('manifestHex','custodyHex'):
        if type(provenance[key])is not str:refuse()
    manifest_raw=bytes.fromhex(provenance['manifestHex']);custody_raw=bytes.fromhex(provenance['custodyHex'])
    if (len(manifest_raw),sha(manifest_raw))!=(9902,MANIFEST_SHA256)or (len(custody_raw),sha(custody_raw))!=(17079,CUSTODY_SHA256):refuse()
    manifest=decode_document(manifest_raw);validate_manifest(manifest);custody=decode_document(custody_raw)
    artifacts={item['path']:item for item in custody['artifacts']}
    resources=[{**artifacts[manifest['backendManifests'][0]['path']if name=='backend-manifest.json'else SCHEMA_BASE+name[len('schemas/'):]],'path':name}for name in layout.resources]
    if provenance['resources']!=resources:refuse()
    if mechanics.verify_installed_files(config.output,ready,layout=layout,ready_present=ready_present,
        expected_resources=resources,expected_executable=entry['executable'],read=read_snapshot,
        exact_tree=verify_exact_tree,refuse=refuse)!=provenance_raw:refuse()
    if read_snapshot(config.index_path,JSON_LIMIT)!=index_raw:refuse()
    return ImplicitArithmeticInstallation(config,ready_raw)


def open_installation(config):
    try:
        verify_trusted_index(config)
        return _verify(config,read_snapshot(config.output/'ready.json',JSON_LIMIT),ready_present=True)
    except (OSError,ValueError,TypeError,KeyError,RecursionError):refuse()


def install(config):
    verified=inspect_package(config)
    return mechanics.publish_installation(config,verified,layout=mechanics.installation_layout('implicit-arithmetic'),
        write=lambda path,raw:mechanics.write_owned(path,raw,refuse=refuse),read=read_snapshot,
        encode=encode_document,verify=_verify,refuse=refuse)


def installed_schema_bundle(installation):
    if type(installation)is not ImplicitArithmeticInstallation:refuse()
    opening=open_installation(installation.config)
    if opening.ready_bytes!=installation.ready_bytes:refuse()
    result=tuple((name,read_snapshot(installation.config.output/'schemas'/name,JSON_LIMIT))for name in INSTALLED_SCHEMAS)
    if open_installation(installation.config).ready_bytes!=opening.ready_bytes:refuse()
    return result


def compile_request(installation,request):
    if type(installation)is not ImplicitArithmeticInstallation or type(request)is not bytes or len(request)>PROTOCOL_LIMIT:refuse()
    opening=open_installation(installation.config)
    if opening.ready_bytes!=installation.ready_bytes:refuse()
    def validate(raw):
        if len(raw)>JSON_LIMIT:refuse()
        response=decode_document(raw)
        if type(response)is not dict or response.get('interfaceVersion')!='weft-compile/0.4.2'or response.get('status')not in ('compiled','blocked'):refuse()
        if response['status']=='compiled'and (response.get('backend',{}).get('backendVersion')!=VERSION or response.get('logicalPlan',{}).get('irVersion')!='weft-ir/0.4.2'):refuse()
    def closing():
        if open_installation(installation.config).ready_bytes!=opening.ready_bytes:refuse()
    return mechanics.compile_transport(installation.config.output/'weft-implicit-arithmetic',request,refuse=refuse,validate_response=validate,closing_verify=closing)
