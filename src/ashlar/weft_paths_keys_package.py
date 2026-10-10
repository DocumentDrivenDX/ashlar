"""Fixed PathsKeys3a package proof; injected trust never comes from package bytes.

Compiler Git3a, qualification producer H and historical530 receipts have separate
identities. Verification is inert and discharges no native/publication obligation.
Directory custody assumes cooperating writers; every selected byte is rechecked.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Optional
import zlib
from .weft_paths_package import (read_snapshot, decode_document, encode_document,
                                verify_exact_tree, verify_trusted_index,
                                PathsInstallationError)

JSON_LIMIT = 4 * 1024 * 1024
FILE_LIMIT = 32 * 1024 * 1024
TOTAL_LIMIT = 96 * 1024 * 1024
DECODED_LIMIT = 64 * 1024 * 1024
PROTOCOL_LIMIT = 16 * 1024 * 1024
SOURCE = '3a2a79ccc19d636f20d31116662d61365c1b4ff7'
BINARY = '471fc5dedc8f8eba3156444d19cc2161fac7a06833672dd5621ed49ead8b522c'
BACKEND = 'ead2df8de9927765775ec7267a9a1865b1c02ec21c849255ada85c36a645fb84'
INVENTORY = 'e9f1e0ac01d7b9dc1b2d61148c74274c1fed5a5593726487aa4562f2e4bf3512'
RECEIPT = 'd5f586b09499d8a099d4b270ab49f03e797dbd9e9ca38ec1bc8034aa0dbf5aa0'
CASES = 'b9661c7ff024e9696417ef5bce3ca70a28d8cd8b93b109835ab0517ededf63d0'
BUILD = '1ad88380c1c0ece419c7054c6a8d474aaaa425fdef1027798848df1f9e17ac90'
QUALIFICATION = '5be699a29f0fc67d95c9e99222c7981d86b074d615b8344983bb6bd5dba4c0b2'
HARNESS = '77517097fe5c5c3b27f90433c505c36514e409896beaa3d473f1937542eb75a5'
BRIDGE = 'fccf88deb54e04bc1c599a8dd11303669a15e8e0c71e3ae0d2c5f649c6ee68b8'
CHECKER = 'bf9523f086466fd485d1cb90b54da9d02677585f015216ad90b236daa9acc276'
BACKEND_ID = 'ashlar.databricks.paths-keys'
VERSION = '0.4.0-paths-keys-candidate'
TARGET = 'spark4-delta4-paths-keys-candidate'
FEATURE = 'ashlar-databricks-paths-keys'
SCHEMA_BASE = 'source-subset/docs/helix/02-design/contracts/'
INSTALLED_SCHEMAS = ('compile-request-v0.4.schema.json', 'compile-response-v0.4.schema.json',
                     'logical-plan-v0.4.schema.json', 'application-result-v0.4.schema.json',
                     'backend-manifest-v0.3.schema.json', 'application-result-v0.2.schema.json')
CONTROL_IDS = ('candidate-opt-out', 'unknown-backend', 'wrong-backend-version', 'wrong-profile',
               'unknown-envelope-member', 'mismatched-interface-dialect', 'binding-digest-mismatch',
               'model-digest-mismatch', 'sql-byte-limit', 'binding-byte-limit', 'duplicate-envelope-member',
               'missing-publication', 'negative-table-version', 'duplicate-table-uuid', 'missing-table-mapping',
               'inconsistent-model-pin', 'fresh-binding-0', 'fresh-binding-1', 'fresh-binding-2')
NAMESPACE_IDS = ('0.1-pair', '0.2-pair', '0.3-pair', 'reverse-mixed', 'unknown-pair')
TRANSPORT_IDS = ('oversize', 'invalid-utf8', 'split-utf8-at-limit', 'exact-limit',
                 'exact-limit-multibyte', 'malformed-json', 'directory-input', 'closed-output')
SCOPES = ('columns-native', 'application-native', 'key-refusal', 'unsigned-columns',
          'optional-native', 'relationship-native', 'compound-native',
          'compound-boundaries-native', 'compound-application-native')
FENCE = {'interfaceVersion': 'weft-compile/0.4.0', 'status': 'blocked', 'diagnostics': [
    {'code': 'WFT-VERSION', 'severity': 'error', 'message':
     'This entrypoint requires the exact 0.4 compile and dialect pair', 'phase': 'input',
     'recoverability': 'correct-input'}]}

class PathsKeysInstallationError(ValueError):
    """Payload-free selected-profile refusal; fallback is never authorized."""

def _refuse():
    raise PathsKeysInstallationError('ASHLAR-WEFT-PATHS-KEYS-REFUSED')

def _sha(raw):
    return hashlib.sha256(raw).hexdigest()

def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()

def _closed(value, keys):
    if type(value) is not dict or set(value) != set(keys): _refuse()

def _text(value, limit=4096):
    if type(value) is not str or not 1 <= len(value) <= limit or any(ord(c)<32 or ord(c)==127 for c in value): _refuse()

def _digest(value, digits=64):
    if type(value) is not str or re.fullmatch('[0-9a-f]{'+str(digits)+'}', value) is None: _refuse()

def _number(value, maximum, minimum=0):
    if type(value) is not int or not minimum <= value <= maximum: _refuse()

def _relative(value):
    _text(value)
    if value.startswith('/') or ':' in value or '\\' in value or any(x in ('', '.', '..') for x in value.split('/')): _refuse()
    return value

def _descriptor(value):
    _closed(value, ('path','sha256','bytes')); _relative(value['path']); _digest(value['sha256']); _number(value['bytes'], FILE_LIMIT)
    return value

def _inflate(raw, limit=DECODED_LIMIT):
    try:
        decoder=zlib.decompressobj(16+zlib.MAX_WBITS)
        result=decoder.decompress(raw, limit+1)
        if len(result)>limit or decoder.unconsumed_tail or not decoder.eof or decoder.unused_data: _refuse()
        return result
    except zlib.error: _refuse()

@dataclass(frozen=True)
class PathsKeysInstallationConfig:
    """Trust pins and actual host observations injected by trusted composition.

    package=None is reserved for restart; inspection requires an absolute package.
    Index digest/revision are authority inputs, never inferred from a manifest.
    """
    index_path: Path
    index_revision: str
    index_sha256: str
    package: Optional[Path]
    realization_id: str
    output: Path
    observed_target: str
    observed_os: str

    def __post_init__(self):
        for name in ('index_path','package','output'):
            value=getattr(self,name)
            if name=='package' and value is None: continue
            if not isinstance(value,Path) or not value.is_absolute(): _refuse()
            object.__setattr__(self,name,value.absolute())
        _digest(self.index_revision,40); _digest(self.index_sha256)
        _text(self.realization_id,256)
        if re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]*',self.realization_id) is None: _refuse()
        _text(self.observed_target,256); _text(self.observed_os,256)

class _Package:
    def __init__(self, root):
        self.root=root; self.raw={}; self.total=0

    def artifact(self, descriptor, limit=FILE_LIMIT):
        d=_descriptor(descriptor)
        if d['bytes']>limit or d['path'] not in self.raw and self.total+d['bytes']>TOTAL_LIMIT: _refuse()
        raw=read_snapshot(self.root/d['path'],limit)
        if len(raw)!=d['bytes'] or _sha(raw)!=d['sha256']: _refuse()
        if d['path'] in self.raw:
            if self.raw[d['path']]!=raw: _refuse()
        else: self.total+=len(raw)
        self.raw[d['path']]=raw
        return raw

    def close(self):
        for name,raw in self.raw.items():
            if read_snapshot(self.root/name)!=raw: _refuse()
        verify_exact_tree(self.root,self.raw)
        for name in self.raw:
            mode=stat.S_IMODE((self.root/name).stat().st_mode)
            if mode != (0o555 if name=='bin/weft-paths-keys' else 0o444): _refuse()

def _validate_manifest(manifest):
    """Closed fixed realization metadata; no profile inference or admission claim."""
    _closed(manifest,('format','realizationId','source','build','executable','backendManifests','publicSchemas','conformance'))
    _text(manifest['realizationId'],256)
    if re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]*',manifest['realizationId']) is None or manifest['format']!='weft-distribution/0.1': _refuse()
    _closed(manifest['source'],('commit','inventory'))
    if manifest['source']['commit']!=SOURCE: _refuse()
    inv=manifest['source']['inventory']; _closed(inv,('artifact','decodedSha256','decodedBytes','trackedFiles'))
    _descriptor(inv['artifact']); _number(inv['decodedBytes'],JSON_LIMIT,1)
    if inv['decodedSha256']!=INVENTORY or inv['trackedFiles']!=1987: _refuse()
    b=manifest['build']; _closed(b,('release','target','features','command','tools','lockfiles','toolchain','effectiveEnvironment','platform'))
    if b['release'] is not True or b['target']!='aarch64-apple-darwin' or b['features']!=[FEATURE]: _refuse()
    if type(b['command']) is not list or len(b['command'])!=15 or b['command'][1:]!=['build','--offline','--locked','--release','-j1','--target','aarch64-apple-darwin','-p','weft-runtime','--no-default-features','--features',FEATURE,'--bin','weft-paths-keys']: _refuse()
    _text(b['command'][0])
    if b['platform']!={'binaryFormat':'mach-o','machine':'arm64','minimumOS':'11.0','sdk':'27.0','observedOS':'27.0.1'}: _refuse()
    _closed(b['effectiveEnvironment'],('observed','unknowns')); _closed(b['effectiveEnvironment']['observed'],('CARGO_HOME','RUSTUP_HOME','CARGO_TARGET_DIR','PATHPrefix'))
    for value in b['effectiveEnvironment']['observed'].values(): _text(value)
    unknowns=b['effectiveEnvironment']['unknowns']
    if type(unknowns) is not list or not 1<=len(unknowns)<=32 or len(set(unknowns))!=len(unknowns): _refuse()
    for value in unknowns: _text(value)
    for name in ('tools','toolchain'): _descriptor(b[name])
    if type(b['lockfiles']) is not list or len(b['lockfiles'])!=1 or b['lockfiles'][0]['path']!='source-subset/Cargo.lock' or b['toolchain']['path']!='source-subset/rust-toolchain.toml': _refuse()
    _descriptor(b['lockfiles'][0]); _descriptor(manifest['executable'])
    if manifest['executable']!={'path':'bin/weft-paths-keys','sha256':BINARY,'bytes':8330656}: _refuse()
    for name,count in (('backendManifests',1),('publicSchemas',20)):
        values=manifest[name]
        if type(values) is not list or len(values)!=count: _refuse()
        for value in values: _descriptor(value)
        if [v['path'] for v in values]!=sorted(set(v['path'] for v in values)): _refuse()
    if manifest['backendManifests'][0]['sha256']!=BACKEND: _refuse()
    c=manifest['conformance']; _closed(c,('corpus','controls','transport'))
    for kind in ('corpus','controls'):
        value=c[kind]; _closed(value,('cases','responses','summary','custody')+(('compiled','blocked') if kind=='corpus' else ()))
        for name in ('responses','summary','custody'): _descriptor(value[name])
    if (c['corpus']['cases'],c['corpus']['compiled'],c['corpus']['blocked'],c['controls']['cases'])!=(55,36,19,19): _refuse()
    _descriptor(c['transport'])

def validate_manifest(manifest: dict) -> None:
    """Validate fixed manifest metadata for inspection and restart; no authority gain."""
    try:
        _validate_manifest(manifest)
    except (KeyError, TypeError, ValueError, RecursionError):
        _refuse()

def _inventory(raw, commit, count):
    value=decode_document(raw); _closed(value,('sourceCommit','files'))
    if value['sourceCommit']!=commit or type(value['files']) is not list or len(value['files'])!=count: _refuse()
    indexed={}
    for entry in value['files']:
        _closed(entry,('path','mode','gitBlob','sha256','bytes'))
        _relative(entry['path']); _digest(entry['gitBlob'],40); _digest(entry['sha256']); _number(entry['bytes'],256*1024*1024)
        if entry['mode'] not in ('100644','100755') or entry['path'] in indexed: _refuse()
        indexed[entry['path']]=entry
    if list(indexed)!=sorted(indexed): _refuse()
    return indexed

def _source_bytes(raw, entry):
    if len(raw)!=entry['bytes'] or _sha(raw)!=entry['sha256'] or hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()!=entry['gitBlob']: _refuse()

def _proof(package, manifest, entry):
    proof_raw=package.artifact(entry['assemblyCustody'],JSON_LIMIT); proof=decode_document(proof_raw)
    _closed(proof,('format','sourceCommit','qualification','producerSha256','artifacts','sourceSubset'))
    if proof['format']!='weft-distribution-assembly-custody/0.1' or proof['sourceCommit']!=SOURCE: _refuse()
    _text(proof['qualification']); _digest(proof['producerSha256'])
    if type(proof['artifacts']) is not list or len(proof['artifacts'])!=126: _refuse()
    descriptors={}
    for d in proof['artifacts']:
        _descriptor(d)
        if d['path'] in descriptors or d['path']=='assembly-custody.json': _refuse()
        descriptors[d['path']]=d; package.artifact(d)
    if list(descriptors)!=sorted(descriptors) or descriptors.get('manifest.json')!=entry['manifest'] or descriptors.get('bin/weft-paths-keys')!=entry['executable']: _refuse()
    inv=manifest['source']['inventory']; raw=_inflate(package.artifact(inv['artifact']),JSON_LIMIT)
    if _sha(raw)!=INVENTORY or len(raw)!=inv['decodedBytes']: _refuse()
    sources=_inventory(raw,SOURCE,1987)
    subset=sorted(p[len('source-subset/'):] for p in package.raw if p.startswith('source-subset/'))
    if len(subset)!=97 or proof['sourceSubset']!=subset: _refuse()
    for name in subset: _source_bytes(package.raw['source-subset/'+name],sources[name])
    schemas=['source-subset/'+name for name in sources if name.startswith('docs/helix/02-design/contracts/') and name.endswith('.schema.json')]
    if schemas!=[d['path'] for d in manifest['publicSchemas']]: _refuse()
    for d in manifest['publicSchemas']+manifest['backendManifests']+manifest['build']['lockfiles']+[manifest['build']['tools'],manifest['build']['toolchain'],inv['artifact']]:
        if descriptors.get(d['path'])!=d: _refuse()
    if proof['producerSha256']!=_sha(package.raw['producer/assemble-paths-distribution.py']): _refuse()
    backend=decode_document(package.raw[manifest['backendManifests'][0]['path']])
    if backend['backendId']!=BACKEND_ID or backend['backendVersion']!=VERSION or backend['interfaceVersion']!='weft-backend/0.3.0' or backend['languageProfiles']!=[{'dialectProfile':'weft-sql/0.4.0','irVersion':'weft-ir/0.4.0'}]: _refuse()
    caps=[c['id'] for c in backend['capabilities']]
    if len(caps)!=48 or len(set(caps))!=48 or 'relationship.boundedKeys' not in caps: _refuse()
    _producer(package,manifest)
    return proof_raw,descriptors,backend

def _resource_match(raw, descriptor):
    if _sha(raw)!=descriptor['sha256'] or len(raw)!=descriptor['bytes']: _refuse()

def _producer(package, manifest):
    raw=package.raw
    build_raw=raw['evidence/build-command.json']; qualification_raw=raw['evidence/qualification-command.json']
    if _sha(build_raw)!=BUILD or _sha(qualification_raw)!=QUALIFICATION: _refuse()
    build=decode_document(build_raw); qualification=decode_document(qualification_raw)
    if build['sourceCommit']!=SOURCE or qualification['sourceCommit']!=SOURCE: _refuse()
    if next(c['argv'] for c in build['commands'] if c['phase']=='cli-build')!=manifest['build']['command']: _refuse()
    if decode_document(raw['evidence/tools.json'])!=build['tools']: _refuse()
    for phase,name in (('cli-build','cli-build-outcome.json'),('source-metadata','metadata-build-outcome.json')):
        outcome=decode_document(raw['evidence/'+name])
        if outcome['commandSha256']!=BUILD or outcome['phase']!=phase or outcome['exitCode']!=0 or outcome['openingClosingCustody'] is not True or outcome['environmentInherited'] is not False: _refuse()
        if phase=='cli-build' and outcome['binary']['sha256']!=BINARY: _refuse()
    outcome=decode_document(raw['evidence/qualification-outcome.json'])
    if outcome['commandSha256']!=QUALIFICATION or outcome['completed'] is not True or outcome['openingClosingCustody'] is not True or outcome['result']['exitCode']!=0: _refuse()
    snapshot=qualification['producerProvenance']['snapshot']
    _resource_match(raw['producer/corpus-harness.py'],snapshot)
    if snapshot['sha256']!=HARNESS: _refuse()
    for name,pin in (('schema-checker',BRIDGE),('checker.py',CHECKER),('run.py',None),('bounded-launcher.py',None)):
        selected=[d for d in qualification['resources'] if d['path'].endswith('/'+name)]
        if len(selected)!=1: _refuse()
        _resource_match(raw['producer/'+name],selected[0])
        if pin is not None and _sha(raw['producer/'+name])!=pin: _refuse()
    for name in ('Cargo.toml','Cargo.lock','src/main.rs'):
        selected=[d for d in build['resources'] if d['path'].endswith('/metadata-probe/'+name)]
        if len(selected)!=1: _refuse()
        _resource_match(raw['producer/metadata-probe/'+name],selected[0])

def _historical(package, history):
    _closed(history,('legacyCases','manifest','realizationId','receipt','sourceCommit'))
    old_source='530ae3511a4a50364d3d7e26195d3883952601df'
    if history['legacyCases']!=463 or history['sourceCommit']!=old_source or history['realizationId']!='weft-530ae35-paths-aarch64-apple-darwin-candidate': _refuse()
    for key in ('manifest','receipt'): _descriptor(history[key])
    base=Path(history['manifest']['path']).parent.as_posix()
    def selected(name): return package.raw['source-subset/'+name]
    manifest_raw=selected(history['manifest']['path']); receipt_gz=selected(history['receipt']['path'])
    _resource_match(manifest_raw,history['manifest']); _resource_match(receipt_gz,history['receipt'])
    m=decode_document(manifest_raw); rraw=_inflate(receipt_gz); r=decode_document(rraw)
    if _sha(manifest_raw)!='fa801113d214078f5bbccf6aee2d9acdf184ccd7be352b7fd55e84b794d5ac4e' or _sha(rraw)!='7f80a87ac41d868a32d220fd5a5ec7f8998c264ec0eecc4455029ce3ba7caf68': _refuse()
    if m['source']['commit']!=old_source or m['realizationId']!=history['realizationId'] or r['sourceCommit']!=old_source or m['executable']['sha256']!=r['binarySha256'] or not any(d['sha256']==r['backendInput']['sha256'] and d['bytes']==r['backendInput']['bytes'] for d in m['backendManifests']): _refuse()
    inv=m['source']['inventory']; inv_raw=_inflate(selected(base+'/'+inv['artifact']['path']),JSON_LIMIT)
    if _sha(inv_raw)!=inv['decodedSha256'] or _sha(inv_raw)!=r['sourceInventorySha256'] or len(inv_raw)!=inv['decodedBytes']: _refuse()
    sources=_inventory(inv_raw,old_source,1842)
    for d in r['sources']:
        raw=selected(base+'/source-subset/'+d['path']); _resource_match(raw,d); _source_bytes(raw,sources[d['path']])
    harness=selected(base+'/source-subset/scripts/distribution/check-paths-cli.py')
    if _sha(harness)!=r['harnessSha256']: _refuse()
    prefix=base+'/source-subset/docs/helix/04-build/evidence/'
    originals=[]
    for scope in SCOPES:
        originals.extend((scope+':'+str(item['id']),item) for item in (decode_document(line) for line in selected(prefix+'B-006-'+scope+'/compile-artifacts.jsonl').splitlines() if line))
    for scope in ('scalar-native','global-native'):
        for name in sorted(p[len('source-subset/'):] for p in package.raw if p.startswith('source-subset/'+prefix+'B-006-'+scope+'/') and p.endswith('-compile.json')):
            originals.append((scope+':'+Path(name).stem,decode_document(selected(name))))
    originals.append(('cross-module',decode_document(selected(prefix+'B-006-cross-module-native/compile.json'))))
    if len(originals)!=463 or len(r['cases'])!=512: _refuse()
    for row,(identity,original) in zip(r['cases'][:463],originals):
        if row['id']!='legacy:'+identity or row['scope']!='legacy' or row['role']!='namespace' or bytes.fromhex(row['requestHex'])!=_canonical(original['request']) or bytes.fromhex(row['originalExpectedHex'])!=_canonical(original['response'])+b'\n' or bytes.fromhex(row['responseHex'])!=_canonical(FENCE)+b'\n' or type(row['exit']) is not int or row['exit']!=0 or row['stderrHex'] or row['migrations']: _refuse()

def _records(receipt, bundle, backend):
    if receipt['sourceCommit']!=SOURCE or receipt['binarySha256']!=BINARY or receipt['sourceInventorySha256']!=INVENTORY or receipt['declaredCapabilityCount']!=48 or receipt['profile']!='paths-keys': _refuse()
    _closed(bundle,('format','sourceCommit','paths','controls','namespaceFences','coverage','sources','historicalQualification'))
    if bundle['format']!='weft-paths-keys-corpus/0.1' or bundle['sourceCommit']!=SOURCE or len(bundle['paths'])!=50 or [c['id'] for c in bundle['controls']]!=list(CONTROL_IDS) or [c['id'] for c in bundle['namespaceFences']]!=list(NAMESPACE_IDS): _refuse()
    expected=[('namespace:'+c['id'],'legacy' if i<3 else 'controls',c) for i,c in enumerate(bundle['namespaceFences'])]+[('paths:'+c['id'],'paths',c) for c in bundle['paths']]+[('controls:'+c['id'],'controls',c) for c in bundle['controls']]
    rows=receipt['cases']
    if len(rows)!=74 or len({r['id'] for r in rows})!=74 or receipt['executedProtocolCases']!=74: _refuse()
    responses={}; statuses=[]
    for row,(identity,scope,case) in zip(rows,expected):
        if (row['id'],row['scope'],row['role'],row['requestHex'],row['responseHex'])!=(identity,scope,case['role'],case['requestHex'],case['responseHex']) or type(row['exit']) is not int or row['exit']!=0 or row['stderrHex'] or row['migrations']: _refuse()
        request=bytes.fromhex(row['requestHex']); raw=bytes.fromhex(row['responseHex'])
        if len(request)>PROTOCOL_LIMIT or len(raw)>4*1024*1024: _refuse()
        response=decode_document(raw); statuses.append(response['status']); responses[identity]=response
        if response['interfaceVersion']!='weft-compile/0.4.0' or response['status'] not in ('compiled','blocked'): _refuse()
        if identity.startswith('namespace:') and response!=FENCE: _refuse()
        if response['status']=='compiled' and (response['backend']!={'backendId':BACKEND_ID,'backendVersion':VERSION,'interfaceVersion':'weft-backend/0.3.0','targetProfile':TARGET} or response['targetContext']['id']!=TARGET): _refuse()
    if statuses[:5]!=['blocked']*5 or statuses[5:55].count('compiled')!=36 or statuses[55:].count('compiled')!=3 or receipt['protocolStatusCounts']!={'compiled':39,'blocked':35}: _refuse()
    if receipt['historicalQualification']!=bundle['historicalQualification'] or receipt['harnessSha256']!=HARNESS or receipt['schemaCheckerSha256']!=BRIDGE or receipt['producerProvenance']!={'bytes':35371,'harnessSha256':HARNESS,'scope':'Separately reviewed qualification producer; not asserted to belong to the compiler source inventory'}: _refuse()
    if receipt['coverage']!=bundle['coverage'] or set(bundle['coverage'])!={c['id'] for c in backend['capabilities']}: _refuse()
    for cap,item in bundle['coverage'].items():
        _closed(item,('accepted','refused','scope')); _text(item['scope'])
        if type(item['accepted']) is not list or type(item['refused']) is not list or not (item['accepted'] or item['refused']): _refuse()
        for identity in item['accepted']:
            if responses[identity]['status']!='compiled' or cap not in responses[identity]['logicalPlan']['requiredCapabilities']: _refuse()
        for identity in item['refused']:
            if responses[identity]['status']!='blocked': _refuse()
    return rows

def _conformance(package, manifest, descriptors, backend):
    receipt_raw=_inflate(package.raw['evidence/full-receipt.json.gz']); bundle_raw=package.raw['evidence/expected-cases.json']
    if _sha(receipt_raw)!=RECEIPT or _sha(bundle_raw)!=CASES: _refuse()
    receipt=decode_document(receipt_raw); bundle=decode_document(bundle_raw)
    if receipt['casesInput']!={'sha256':CASES,'bytes':len(bundle_raw)} or receipt['backendInput']!={'sha256':BACKEND,'bytes':len(package.raw[manifest['backendManifests'][0]['path']])}: _refuse()
    rows=_records(receipt,bundle,backend); _historical(package,bundle['historicalQualification'])
    for d in receipt['sources']:
        raw=package.raw['source-subset/'+d['path']]; _resource_match(raw,d)
    for kind,selected,compiled in (('corpus',rows[:55],36),('controls',rows[55:],3)):
        item=manifest['conformance'][kind]
        for name in ('responses','summary','custody'):
            if descriptors.get(item[name]['path'])!=item[name]: _refuse()
        raw=_inflate(package.artifact(item['responses'])); summary=decode_document(package.artifact(item['summary'])); custody=decode_document(package.artifact(item['custody']))
        if raw!=b''.join(_canonical(row)+b'\n' for row in selected) or len(raw)!=summary['decodedBytes'] or _sha(raw)!=summary['decodedSha256'] or summary['profile']!='weft-paths-keys-produced-corpus/0.1' or summary['cases']!=len(selected) or summary['compiled']!=compiled or summary['blocked']!=len(selected)-compiled or summary['sourceCommit']!=SOURCE or summary['binarySha256']!=BINARY: _refuse()
        if custody!={'sourceCommit':SOURCE,'binarySha256':BINARY,'fullReceiptSha256':RECEIPT,'expectedCasesSha256':CASES,'decodedSha256':_sha(raw),'decodedBytes':len(raw)}: _refuse()
    transport=decode_document(package.artifact(manifest['conformance']['transport']))
    if descriptors.get(manifest['conformance']['transport']['path'])!=manifest['conformance']['transport'] or transport!={'binarySha256':BINARY,'controls':receipt['transport']} or [r['id'] for r in receipt['transport']]!=list(TRANSPORT_IDS): _refuse()
    for row in receipt['transport']:
        raw=bytes.fromhex(row['stdoutHex']); err=bytes.fromhex(row['stderrHex'])
        if len(raw)>4*1024*1024 or len(err)>4096: _refuse()
        if row['id'] in ('oversize','invalid-utf8','split-utf8-at-limit','directory-input','closed-output'):
            marker='INPUT_LIMIT' if row['id']=='oversize' else 'UTF8' if 'utf8' in row['id'] else 'INPUT_IO' if row['id']=='directory-input' else 'OUTPUT_IO'
            if row['exit']!=2 or raw or err!=('WEFT_CLI_'+marker+'\n').encode(): _refuse()
        elif row['exit']!=0 or err or decode_document(raw)['status']!='blocked': _refuse()

@dataclass(frozen=True)
class VerifiedPathsKeysPackage:
    """Opaque immutable snapshots; installers must reverify rather than trust objects."""
    manifest_bytes: bytes
    custody_bytes: bytes
    executable_bytes: bytes
    provenance_bytes: bytes
    resources: tuple[tuple[str, bytes], ...]

def inspect_package(config: PathsKeysInstallationConfig) -> VerifiedPathsKeysPackage:
    """Verify injected index, exact127 closure and profile proofs without execution."""
    try:
        if type(config) is not PathsKeysInstallationConfig: _refuse()
        index_raw,entry=verify_trusted_index(config)
        if not isinstance(config.package,Path): _refuse()
        package=_Package(config.package)
        raw=package.artifact(entry['manifest'],JSON_LIMIT); manifest=decode_document(raw); validate_manifest(manifest)
        if manifest['realizationId']!=config.realization_id or manifest['executable']!=entry['executable'] or manifest['build']['platform']['observedOS']!=config.observed_os: _refuse()
        proof_raw,descriptors,backend=_proof(package,manifest,entry)
        _conformance(package,manifest,descriptors,backend)
        resources=[('backend-manifest.json',package.raw[manifest['backendManifests'][0]['path']])]
        for name in INSTALLED_SCHEMAS: resources.append(('schemas/'+name,package.raw[SCHEMA_BASE+name]))
        provenance={'format':'ashlar-weft-paths-keys-provenance/0.1','indexRevision':config.index_revision,'indexSha256':config.index_sha256,'realizationId':config.realization_id,'manifestHex':raw.hex(),'custodyHex':proof_raw.hex(),'resources':[{'path':name,'sha256':_sha(data),'bytes':len(data)} for name,data in resources],'qualification':'Indexed compiler bytes only; currentGit3a/producerH and historical530 remain distinct; no native/source/publication/ACK obligation discharged.'}
        provenance_raw=encode_document(provenance)+b'\n'
        if len(provenance_raw)>JSON_LIMIT: _refuse()
        executable=package.raw[manifest['executable']['path']]
        package.close()
        if read_snapshot(config.index_path,JSON_LIMIT)!=index_raw: _refuse()
        return VerifiedPathsKeysPackage(raw,proof_raw,executable,provenance_raw,tuple(resources))
    except (KeyError,TypeError,OSError,ValueError,RecursionError,StopIteration): _refuse()
