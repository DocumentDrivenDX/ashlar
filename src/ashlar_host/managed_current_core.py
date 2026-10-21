# SPDX-License-Identifier: MIT OR Apache-2.0
"""Offline binding boundary for a future managed UMF 0.10 publication.

This module prepares identity-preserving metadata only.  It does not create UC
objects, compile Weft SQL, publish Delta data, or grant source/read authority.
"""
from dataclasses import dataclass
import hashlib
import json
import re
import uuid
import base64

PROFILE = 'ashlar-managed-current-core/0.1'
BINDING_PROFILE = 'ashlar-managed-current-core-binding/0.1'
BINDING_PROFILE_045 = 'ashlar-managed-current-core-binding/0.2'
ROLES = ('object_current', 'edge_current', 'tombstone', 'whole_source_history',
         'publication_manifest', 'publication_attempt_phase')
BACKENDS = ('Backend03', 'Backend02')
_IDENT = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,127}\Z')


def _require(value, reason='managed-current-core refused'):
    if not value:
        raise ValueError(reason)


def _text(value):
    return type(value) is str and 0 < len(value.encode('utf-8')) <= 4096 and '\0' not in value


def _observed(value):
    return _text(value) and value.strip().casefold() not in {
        'unknown', 'unobserved', 'pending', 'not observed', 'not-observed', 'n/a', 'null'
    }


@dataclass(frozen=True)
class ManagedCurrentCoreConfig:
    """Explicit intended destination and named current query surfaces."""
    catalog: str
    schema: str
    backend03: dict
    backend02: dict
    preferred_cost_usd: int
    hard_cost_usd: int
    profile: str = PROFILE

    def __post_init__(self):
        _require(self.profile == PROFILE and _IDENT.fullmatch(self.catalog) is not None
                 and _IDENT.fullmatch(self.schema) is not None)
        _require(type(self.preferred_cost_usd) is int and 0 <= self.preferred_cost_usd
                 and type(self.hard_cost_usd) is int and self.preferred_cost_usd <= self.hard_cost_usd,
                 'explicit managed-current-core cost bounds required')
        for backend, name in ((self.backend03, 'Backend03'), (self.backend02, 'Backend02')):
            if backend is not None:
                _validate_target(backend, name)
        _require(self.backend03 is None or self.backend02 is None or self.backend03 != self.backend02,
                 'distinct Weft target identities required')


def _validate_target(target, expected_surface):
    _require(type(target) is dict and set(target) == {
        'surface', 'backendId', 'backendVersion', 'targetProfile', 'weftInterface', 'weftDialect'
    }, 'complete observed Weft target identity required')
    _require(target['surface'] == expected_surface and all(_text(target[k]) for k in (
        'backendId', 'backendVersion', 'targetProfile', 'weftInterface', 'weftDialect')))
    # Known targets belong to different, already-qualified or candidate profiles.
    _require((target['backendId'], target['backendVersion'], target['targetProfile']) not in {
        ('ashlar.databricks', '0.1.0-qualified', 'dbsql2026.39-qualified'),
        ('ashlar.databricks', '0.2.0-local-spark4.2-pages-candidate', 'spark4.2.0-delta4.4.1-candidate'),
        ('ashlar.spark4.2', '0.2.0-local', 'spark4.2.0-delta4.4.1-candidate'),
        ('ashlar.databricks.paths-keys-having', '0.4.1-count-star-having-spark4.2-candidate',
         'spark4.2.0-delta4.4.1-paths-keys-having-candidate'),
    }, 'historical or local candidate target cannot identify a managed current target')


@dataclass(frozen=True)
class ManagedCurrentCoreObservation:
    """Values returned by the eventual independent UC/runtime/source observers.

    Missing values remain missing and block admission.  A populated instance is
    still only a transport record: callers must construct it from verified,
    held observations before any owner grants execution authority.
    """
    installation_id: str | None = None
    runtime_name: str | None = None
    runtime_version: str | None = None
    build_id: str | None = None
    protocol: str | None = None
    principal_id: str | None = None
    production_source_receipt: str | None = None
    protected_ack_owner: str | None = None

    def require_observed(self):
        values = (self.installation_id, self.runtime_name, self.runtime_version,
                  self.build_id, self.protocol, self.principal_id,
                  self.production_source_receipt, self.protected_ack_owner)
        _require(all(_observed(value) for value in values),
                 'managed UC/runtime/build/protocol/principal/production source/ACK observations required')
        _require(_sha(self.production_source_receipt), 'verified production source receipt digest required')
        return self


def _sha(value):
    return type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value) is not None


def bind_current_core(admission, config: ManagedCurrentCoreConfig, table_inventory: dict | None = None):
    """Prepare source mapping, then optionally add a six-role install inventory.

    With no inventory this is the pre-install bundle. Supplying one validates
    the six-role installation identities and heads. Publication manifests keep
    their separate four-data-role version vector. Neither result is a
    publication or query request, and neither grants execution authority.
    """
    from .truss_graph_admission import TrussGraphAdmission, current_admission_profile
    from .truss_current_catalog import (CURRENT_TRUSS_VERSION, CURRENT_TRUSS_045_VERSION,
        CURRENT_EVIDENCE_PROFILE, CURRENT_045_EVIDENCE_PROFILE,
        CURRENT_TRUSS_045_WHEEL_SHA256, CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256)
    _require(type(config) is ManagedCurrentCoreConfig)
    config.__post_init__()
    _require(type(admission) is TrussGraphAdmission and admission.current
             and admission.current_truss_version in (CURRENT_TRUSS_VERSION, CURRENT_TRUSS_045_VERSION)
             and admission.metadata()['profile'] == current_admission_profile(admission.current_truss_version)
             and admission.batch.profile == 'truss-native-feed/0.1',
             'actual versioned current Truss / UMF 0.10 graph admission required')
    version = admission.current_truss_version
    evidence_profile = (CURRENT_EVIDENCE_PROFILE if version == CURRENT_TRUSS_VERSION
                        else CURRENT_045_EVIDENCE_PROFILE)
    _require(admission.current_catalog_evidence_profile == evidence_profile)
    if version == CURRENT_TRUSS_045_VERSION:
        _require(admission.current_runtime_wheel_sha256 == CURRENT_TRUSS_045_WHEEL_SHA256
                 and admission.current_runtime_manifest_sha256 == CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256,
                 'Truss 0.0.45 installed wheel/runtime provenance required')
    binding_profile = BINDING_PROFILE if version == CURRENT_TRUSS_VERSION else BINDING_PROFILE_045
    tables = None if table_inventory is None else _validate_installation_inventory(config, table_inventory)

    accepted = {}
    graph_revisions = {}
    for revision, selected in sorted(admission.revisions.items()):
        _require(type(revision) is str and revision.isdecimal())
        models = selected.get('models')
        _require(type(models) is dict and models)
        selected_report = selected.get('report_object')
        _require(type(selected_report) is dict)
        accepted_revisions = {item['documentId']: item['documentRevision']
                              for item in selected_report['acceptedInput']['documents']}
        for document_id, (model, raw) in sorted(models.items()):
            digest = hashlib.sha256(raw).hexdigest()
            document_revision = accepted_revisions.get(document_id)
            accepted_item=next((item for item in selected_report['acceptedInput']['documents']
                                if item['documentId']==document_id),None)
            artifact=accepted_item.get('artifact') if type(accepted_item)is dict else None
            _require(model.get('umf') == '0.10.0' and _sha(digest)
                     and _text(document_revision) and type(artifact)is dict
                     and _text(artifact.get('identity')) and artifact.get('sha256')==digest)
            accepted[revision + ':' + document_id] = {
                'documentId': document_id, 'documentRevision': document_revision,
                'artifactIdentity': artifact['identity'],
                'registrationId': selected_report.get('registrationId'),
                'catalogRevision': revision,
                'umfVersion': '0.10.0', 'modelSha256': digest,
                'sourceBytesBase64': base64.b64encode(raw).decode('ascii'),
            }
        view = json.loads(selected['view'])
        definitions = [entry['reference'] for entry in view['definitions']]
        assigned_types = {}
        for type_id, declared in sorted(selected.get('types', {}).items()):
            document_id, module_id, element_id = declared['owner']
            model = models[document_id][0]
            records = [element for module in model['modules'] if module['id'] == module_id
                       for element in module['elements'] if element['id'] == element_id]
            _require(len(records) == 1 and records[0].get('kind') == 'record')
            properties = []
            for member in records[0]['members']:
                matches = [item for item in definitions
                           if item.get('kind') == 'property' and item.get('typeId') == type_id
                           and item.get('owner') == {'documentId': document_id, 'moduleId': member['module']}
                           and item.get('authoredIdentity') == member['element']]
                _require(len(matches) == 1, 'current assigned property identity must map to one UMF field')
                item = matches[0]
                prop_id = item['propertyId']
                family, nullability = declared['domains'][prop_id]
                properties.append({'propertyId': prop_id,
                    'logical': {'documentId': document_id, 'module': member['module'],
                                'element': member['element']},
                    'definitionPin': declared['fields'][prop_id], 'family': family,
                    'nullability': nullability, 'facets': declared['facets'][prop_id]})
            assigned_types[type_id] = {
                'logical': {'documentId': document_id, 'module': module_id, 'element': element_id},
                'definitionPin': declared['pin'], 'properties': properties,
                'keys': [{'keyId': key_id, 'propertyIds': list(components)}
                         for key_id, components in sorted(declared['keys'].items())],
            }
        assigned_edges = {}
        for edge_id, declared in sorted(selected.get('edges', {}).items()):
            document_id, module_id, relationship_id = declared['owner']
            assigned_edges[edge_id] = {
                'logical': {'documentId': document_id, 'module': module_id,
                            'relationship': relationship_id},
                'definitionPin': declared['pin'],
                'endpointTypeIds': list(declared['endpoints']),
                'declaration': declared['declaration'],
            }
        graph_revisions[revision] = {'types': assigned_types, 'relationships': assigned_edges}
    _require(bool(accepted), 'at least one exact accepted UMF 0.10 source document required')
    # Retain the complete exact source, report, catalog view and native page bytes
    # in this bounded offline harness object; digests alone are not source custody.
    catalog_views = admission.catalog_views
    _require(type(catalog_views) is dict and catalog_views)
    report_digests = {}
    report_bytes = {}
    for revision, selected in admission.revisions.items():
        raw = selected['report']
        report_digests[revision] = hashlib.sha256(raw).hexdigest()
        report_bytes[revision] = base64.b64encode(raw).decode('ascii')
    view_digests = {revision: hashlib.sha256(raw).hexdigest()
                    for revision, raw in catalog_views.items() if revision in admission.revisions}
    _require(set(view_digests) == set(admission.revisions))
    view_bytes = {revision: base64.b64encode(raw).decode('ascii')
                  for revision, raw in catalog_views.items() if revision in admission.revisions}
    result = {
        'profile': binding_profile,
        'status': 'preinstallation-source-bundle' if tables is None else 'postinstallation-installation-inventory-bound-offline',
        'source': {
            'authority': 'development-only-until-production-owner-observed',
            'pageAuthority': 'unverified-caller-supplied-native-page',
            'trussVersion': version, 'umfVersion': '0.10.0',
            'feed': admission.batch.feed, 'epoch': admission.batch.epoch,
            'transactionSha256': hashlib.sha256(admission.batch.begin).hexdigest(),
            'nativePageBase64': base64.b64encode(admission.batch.begin).decode('ascii'),
            'cursorBeforeBase64': admission.batch.cursor_before,
            'cursorAfterBase64': admission.batch.cursor_after,
            'acceptedDocuments': accepted, 'reportSha256': report_digests,
            'reportBase64': report_bytes, 'catalogViewSha256': view_digests,
            'catalogViewBase64': view_bytes,
            'assignedGraphByRevision': graph_revisions,
        },
        'destination': {'catalog': config.catalog, 'schema': config.schema,
             'intendedRoleNames': {role: [config.catalog, config.schema, role] for role in ROLES},
                        'tables': tables},
        'weftTargets': {'Backend03': dict(config.backend03) if config.backend03 is not None else None,
                        'Backend02': dict(config.backend02) if config.backend02 is not None else None},
        'bounds': {'preferredCostUsd': config.preferred_cost_usd,
                   'hardCostUsd': config.hard_cost_usd},
    }
    if version == CURRENT_TRUSS_045_VERSION:
        result['source']['catalogEvidenceProfile'] = evidence_profile
        result['source']['runtimeProvenance'] = {
            'trussWheelSha256': admission.current_runtime_wheel_sha256,
            'runtimeManifestSha256': admission.current_runtime_manifest_sha256,
        }
    result['bindingSha256'] = hashlib.sha256(json.dumps(
        result, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
    return result


def _validate_installation_inventory(config, table_inventory):
    _require(type(table_inventory) is dict and set(table_inventory) == set(ROLES),
             'complete six-role observed installation inventory required')
    tables = {}
    uuids = set()
    for role in ROLES:
        item = table_inventory[role]
        _require(type(item) is dict and set(item) == {'name', 'uuid', 'version'})
        name = item['name']
        _require(type(name) is tuple and len(name) == 3
                 and name[0] == config.catalog and name[1] == config.schema
                 and name[2] == role and all(type(part) is str and _IDENT.fullmatch(part) for part in name),
                 'exact three-part Unity Catalog role name required')
        try:
            table_uuid = str(uuid.UUID(item['uuid']))
        except (ValueError, TypeError, AttributeError):
            _require(False, 'observed native Delta table UUID required')
        _require(table_uuid == item['uuid'] and table_uuid not in uuids
                 and type(item['version']) is int and item['version'] >= 0,
                 'distinct native table UUID and exact observed version required')
        uuids.add(table_uuid)
        tables[role] = {'name': list(name), 'uuid': table_uuid, 'version': item['version']}
    return tables


def bind_observed_managed_installation(prepared_binding, config: ManagedCurrentCoreConfig, table_inventory: dict):
    """Attach independently observed native identities after managed install."""
    source = prepared_binding.get('source') if type(prepared_binding) is dict else None
    version = source.get('trussVersion') if type(source) is dict else None
    expected_profile = (BINDING_PROFILE if version == '0.0.44'
                        else BINDING_PROFILE_045 if version == '0.0.45' else None)
    _require(type(prepared_binding) is dict and prepared_binding.get('profile') == expected_profile
             and prepared_binding.get('status') == 'preinstallation-source-bundle'
             and prepared_binding.get('destination') == {
                 'catalog': config.catalog, 'schema': config.schema,
                 'intendedRoleNames': {role: [config.catalog, config.schema, role] for role in ROLES},
                 'tables': None}, 'original preinstallation binding required')
    # Do not attach observed destination identities to a forged or stale source
    # bundle; reconstruct the exact versioned admission before accepting them.
    restore_managed_current_admission(prepared_binding, config)
    result = json.loads(json.dumps(prepared_binding))
    result['destination']['tables'] = _validate_installation_inventory(config, table_inventory)
    result['status'] = 'postinstallation-installation-inventory-bound-offline'
    result.pop('bindingSha256', None)
    result['bindingSha256'] = hashlib.sha256(json.dumps(
        result, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
    return result


def restore_managed_current_admission(binding, config):
    """Revalidate a digest-bound source bundle inside the injected-Spark task.

    The returned admission is reconstructed from its original Truss page,
    accepted report, complete catalog view, and exact accepted UMF 0.10 bytes.
    The outer digest is correspondence only; source authority/ACK stays with the
    external held Truss source session.
    """
    _require(type(binding)is dict and binding.get('status')=='preinstallation-source-bundle',
             'Exact preinstallation current source bundle required')
    source=binding.get('source');_require(type(source)is dict)
    from .truss_current_catalog import (CURRENT_TRUSS_VERSION, CURRENT_TRUSS_045_VERSION,
        CURRENT_EVIDENCE_PROFILE, CURRENT_045_EVIDENCE_PROFILE,
        CURRENT_TRUSS_045_WHEEL_SHA256, CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256)
    from .truss_graph_admission import current_admission_profile
    version=source.get('trussVersion')
    expected_binding_profile=(BINDING_PROFILE if version==CURRENT_TRUSS_VERSION
                              else BINDING_PROFILE_045 if version==CURRENT_TRUSS_045_VERSION else None)
    _require(expected_binding_profile is not None and binding.get('profile')==expected_binding_profile
             and source.get('umfVersion')=='0.10.0',
             'Version-matched current Truss source bundle required')
    if version==CURRENT_TRUSS_045_VERSION:
        _require(source.get('catalogEvidenceProfile')=='ashlar-truss-current-catalog-evidence/0.2'
                 and source.get('runtimeProvenance')=={
                     'trussWheelSha256':CURRENT_TRUSS_045_WHEEL_SHA256,
                     'runtimeManifestSha256':CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256},
                 'Truss 0.0.45 wheel/runtime provenance required in source bundle')
    else:
        _require('catalogEvidenceProfile' not in source and 'runtimeProvenance' not in source,
                 'Truss 0.0.44 source bundle must retain its original provenance contract')
    retained=dict(binding);digest=retained.pop('bindingSha256',None)
    _require(_sha(digest) and hashlib.sha256(json.dumps(retained,sort_keys=True,
             separators=(',',':'),ensure_ascii=False).encode()).hexdigest()==digest,
             'Original digest-bound source bundle required')
    page=base64.b64decode(source.get('nativePageBase64',''),validate=True)
    prior=base64.b64decode(source.get('cursorBeforeBase64',''),validate=True)
    following=base64.b64decode(source.get('cursorAfterBase64',''),validate=True)
    _require(hashlib.sha256(page).hexdigest()==source.get('transactionSha256'))
    from ashlar.truss_native_feed import native_feed_batch
    batch=native_feed_batch(page,sha256=source['transactionSha256'],prior=prior,next=following,feed=source['feed'])
    reports={revision:base64.b64decode(raw,validate=True) for revision,raw in source['reportBase64'].items()}
    views={revision:base64.b64decode(raw,validate=True) for revision,raw in source['catalogViewBase64'].items()}
    from types import SimpleNamespace
    source_objects={}
    for key,item in source['acceptedDocuments'].items():
        revision,document_id=key.split(':',1)
        raw=base64.b64decode(item['sourceBytesBase64'],validate=True)
        _require(hashlib.sha256(raw).hexdigest()==item['modelSha256']
                 and item['umfVersion']=='0.10.0' and item['documentId']==document_id)
        source_objects[(revision,document_id)]=SimpleNamespace(document_id=document_id,
            document_revision=item['documentRevision'],artifact_identity=item['artifactIdentity'],
            registration_id=item['registrationId'],sha256=item['modelSha256'],content=raw,
            catalog_revision=int(item['catalogRevision']))
    from .truss_current_catalog import CurrentCatalogEvidence
    from .truss_graph_admission import TrussGraphAdmission
    runtime = source.get('runtimeProvenance', {})
    admission=TrussGraphAdmission(batch,CurrentCatalogEvidence(
        version,reports,views,source_objects,
        runtime_wheel_sha256=runtime.get('trussWheelSha256'),
        runtime_manifest_sha256=runtime.get('runtimeManifestSha256')))
    _require(admission.current and admission.metadata()['profile']==current_admission_profile(version)
             and admission.current_catalog_evidence_profile==(source.get('catalogEvidenceProfile')
                 if version==CURRENT_TRUSS_045_VERSION else CURRENT_EVIDENCE_PROFILE)
             and admission.current_runtime_wheel_sha256==runtime.get('trussWheelSha256')
             and admission.current_runtime_manifest_sha256==runtime.get('runtimeManifestSha256')
             and admission.metadata()['declared_umf']=={revision:{doc:'0.10.0' for doc in value['models']}
                    for revision,value in admission.revisions.items()},
             'Reconstructed current Truss/UMF admission differs')
    rebuilt=bind_current_core(admission,config)
    _require(rebuilt['bindingSha256']==digest,'Reconstructed source binding differs from retained bundle')
    return admission


