# SPDX-License-Identifier: MIT OR Apache-2.0
"""Truss 0.44 / UMF 0.10 current-catalog source custody and interpretation."""
from dataclasses import dataclass
import hashlib
import json
import re

CURRENT_TRUSS_VERSION = '0.0.44'
CURRENT_REPORT_VERSION = 'truss-catalog-registration-report/0.6'
CURRENT_VIEW_VERSION = 'truss-catalog-view/0.2.0'
CURRENT_VIEW_PROFILE = ('truss-complete-catalog-view', '0.4')
CURRENT_VIEW_PROFILE_SHA256 = '782025c3dfcf49bd676c67df3d62269f78793a31358a18556c2c2689d5fbc436'
CURRENT_LAYOUT_PROFILE_SHA256 = 'cf417e0af5d37dd61fa5f39f818d2e7282a29e6c6c6ab7fc0b26dcdde99e7dec'
CURRENT_EVIDENCE_PROFILE = 'ashlar-truss-current-catalog-evidence/0.1'


@dataclass(frozen=True)
class CurrentCatalogEvidence:
    truss_version: str
    reports: dict
    views: dict
    sources: dict

    @property
    def profile(self):
        return CURRENT_EVIDENCE_PROFILE


def parse_current_catalog_json(raw):
    """Parse current Truss catalog/report/source JSON, refusing duplicate members."""
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate current Truss JSON member')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs)


def _sha(value):
    return type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value) is not None


def resolve_current_accepted_sources(report, revision, sources):
    """Match report 0.6 accepted references to exact public UMF 0.10 source bytes.

    `sources` is keyed by ``(catalog_revision, document_id)`` and each value
    carries its public source identity and original content bytes.
    """
    if report.get('interfaceVersion') != CURRENT_REPORT_VERSION:
        raise ValueError('Current Truss registration report 0.6 required')
    if report.get('revision') != revision:
        raise ValueError('Current Truss report revision differs')
    accepted = report.get('acceptedInput', {}).get('documents')
    validations = report.get('source', {}).get('documentValidations')
    if type(accepted) is not list or not accepted or type(validations) is not list:
        raise ValueError('Current accepted source references required')
    registration_id = report.get('registrationId')
    if type(registration_id) is not str or not registration_id:
        raise ValueError('Current report registration identity required')
    versions = report.get('source', {}).get('umf')
    if type(versions) is not list or not any(type(x) is dict and x.get('version') == '0.10.0' for x in versions):
        raise ValueError('Original UMF 0.10 source profile required')
    by_validation = {}
    for item in validations:
        if type(item) is not dict or item.get('documentId') in by_validation:
            raise ValueError('Distinct current source validation references required')
        by_validation[item.get('documentId')] = item
    models = {}
    for item in accepted:
        if type(item) is not dict:
            raise ValueError('Current accepted document reference required')
        document_id = item.get('documentId')
        document_revision = item.get('documentRevision')
        artifact = item.get('artifact')
        if (type(document_id) is not str or not document_id or type(document_revision) is not str
                or not document_revision or type(artifact) is not dict
                or type(artifact.get('identity')) is not str or not artifact['identity']
                or not _sha(artifact.get('sha256'))):
            raise ValueError('Exact current accepted document identity and digest required')
        validation = by_validation.get(document_id)
        source = sources.get((revision, document_id))
        if (type(validation) is not dict or validation.get('valid') is not True
                or validation.get('documentId') != document_id
                or validation.get('contentSha256') != artifact['sha256']
                or source is None):
            raise ValueError('Current accepted source reference differs from report')
        if (source.document_id != document_id or source.document_revision != document_revision
                or source.artifact_identity != artifact['identity']
                or getattr(source, 'registration_id', None) != registration_id
                or source.sha256 != artifact['sha256']
                or type(source.content) is not bytes or hashlib.sha256(source.content).hexdigest() != artifact['sha256']):
            raise ValueError('Public accepted source identity, revision or digest differs from report')
        if getattr(source, 'catalog_revision', int(revision)) != int(revision):
            raise ValueError('Public accepted source catalog revision differs')
        model = parse_current_catalog_json(source.content)
        if type(model) is not dict or model.get('umf') != '0.10.0' or model.get('id') != document_id:
            raise ValueError('Exact original UMF 0.10 document required')
        models[document_id] = (model, source.content, document_revision, artifact['sha256'])
    if set(by_validation) != set(models):
        raise ValueError('Current report source inventory differs from accepted documents')
    return models


def interpret_current_catalog(revision, report_raw, view_raw, sources):
    """Project only Truss-assigned current IDs onto exact accepted UMF 0.10 bytes."""
    from .truss_graph_admission import admitted_facets
    if type(revision) is not str or not revision.isascii() or not revision.isdecimal() or str(int(revision)) != revision:
        raise ValueError('Canonical current catalog revision required')
    report = parse_current_catalog_json(report_raw)
    models = resolve_current_accepted_sources(report, revision, sources)
    catalog = parse_current_catalog_json(view_raw)
    if (catalog.get('interfaceVersion') != CURRENT_VIEW_VERSION
            or catalog.get('scope') != {'kind': 'complete'}
            or catalog.get('catalogRevision') != revision):
        raise ValueError('Complete Truss catalog view 0.2.0 for the accepted revision required')
    view_profile = catalog.get('viewProfile')
    if (type(view_profile) is not dict
            or (view_profile.get('identity'), view_profile.get('version')) != CURRENT_VIEW_PROFILE
            or view_profile.get('sha256') != CURRENT_VIEW_PROFILE_SHA256):
        raise ValueError('Truss complete catalog view profile 0.4 required')
    layout = catalog.get('layoutProfile')
    if (type(layout) is not dict or layout.get('identity') != 'truss-layout'
            or layout.get('version') != '1.0' or layout.get('sha256') != CURRENT_LAYOUT_PROFILE_SHA256):
        raise ValueError('Truss layout profile 1.0 required')
    report_layout = report.get('acceptedInput', {}).get('layoutProfile')
    if (type(report_layout) is not dict or report_layout.get('identity') != layout['identity']
            or report_layout.get('version') != layout['version']
            or report_layout.get('sha256') != layout['sha256']):
        raise ValueError('Current report and catalog view layout profiles differ')
    if not _sha(catalog.get('inventorySha256')) or not _sha(catalog.get('consistencyEvidenceSha256')):
        raise ValueError('Current catalog inventory and consistency evidence digests required')
    definitions = catalog.get('definitions')
    if type(definitions) is not list or not definitions:
        raise ValueError('Complete current catalog definition inventory required')
    source_by_doc = {doc: model for doc, model in models.items()}
    elements = {}
    relationships = {}
    for document_id, (model, raw, doc_revision, digest) in models.items():
        for module in model.get('modules', []):
            if type(module) is not dict or type(module.get('id')) is not str or type(module.get('elements')) is not list:
                raise ValueError('Original UMF module and element inventory required')
            for element in module['elements']:
                if type(element) is not dict or type(element.get('id')) is not str:
                    raise ValueError('Original UMF element identity required')
                key = (document_id, module['id'], element['id'])
                if key in elements:
                    raise ValueError('Duplicate original UMF element identity')
                elements[key] = element
            for relation in module.get('relationships', []):
                if type(relation) is not dict or type(relation.get('id')) is not str:
                    raise ValueError('Original UMF relationship identity required')
                key = (document_id, module['id'], relation['id'])
                if key in relationships:
                    raise ValueError('Duplicate original UMF relationship identity')
                relationships[key] = relation
    refs = {}
    for entry in definitions:
        if type(entry) is not dict or entry.get('lifecycle') != 'active' or entry.get('resolution') != 'defined':
            raise ValueError('Active defined current catalog entries required')
        ref = entry.get('reference')
        provenance = entry.get('provenance')
        owner = ref.get('owner') if type(ref) is dict else None
        if (type(owner) is not dict or type(owner.get('documentId')) is not str
                or type(owner.get('moduleId')) is not str or owner['documentId'] not in source_by_doc):
            raise ValueError('Current catalog source ownership required')
        model, raw, doc_revision, digest = models[owner['documentId']]
        if (type(provenance) is not dict or provenance.get('kind') != 'accepted_document'
                or provenance.get('acceptedCatalogRevision') != revision
                or provenance.get('documentRevision') != doc_revision
                or provenance.get('acceptedDocumentSha256') != digest):
            raise ValueError('Current catalog provenance differs from exact accepted source')
        kind = ref.get('kind')
        authored = ref.get('authoredIdentity')
        type_id = ref.get('typeId')
        natural = (kind, owner['documentId'], owner['moduleId'], type_id, authored)
        if kind not in ('type', 'property', 'key', 'relationship') or type(natural[-1]) is not str or natural in refs:
            raise ValueError('Distinct supported current catalog definition required')
        if not _sha(ref.get('definitionPin')) or type(entry.get('definition')) is not dict or not _sha(entry['definition'].get('sha256')):
            raise ValueError('Exact current definition pin and source digest required')
        refs[natural] = ref
    types = {}
    qualified = {}
    for (kind, doc, module_id, type_id, authored), ref in refs.items():
        if kind != 'type':
            continue
        owner = (doc, module_id, authored)
        record = elements.get(owner)
        if type(record) is not dict or record.get('kind') != 'record' or type(type_id) is not str or type_id in types or owner in qualified:
            raise ValueError('Current catalog type must bind a distinct original UMF record')
        fields = {}; domains = {}; facets = {}; required = set(); authored_fields = {}
        for member in record.get('members', []):
            if type(member) is not dict:
                raise ValueError('Original UMF record members required')
            field_owner = (doc, member.get('module'), member.get('element'))
            field = elements.get(field_owner)
            if (type(field) is not dict or field.get('kind') != 'field'
                    or field.get('scalarType') not in ('string', 'integer', 'decimal', 'boolean')
                    or field.get('cardinality') != 'one'
                    or field.get('nullability') not in ('required', 'absent-allowed')):
                raise ValueError('Supported original scalar field declarations required')
            property_id = None
            property_ref = refs.get(('property', doc, member['module'], type_id, member['element']))
            if property_ref is None or type(property_ref.get('propertyId')) is not str:
                raise ValueError('Current Truss property ID and definition pin required')
            property_id = property_ref['propertyId']
            if property_id in fields:
                raise ValueError('Distinct current Truss property IDs required')
            fields[property_id] = property_ref['definitionPin']
            domains[property_id] = (field['scalarType'], field['nullability'])
            facets[property_id] = admitted_facets(field)
            authored_fields[(member['module'], member['element'])] = property_id
            if field['nullability'] == 'required':
                required.add(property_id)
        keys = {}
        for key in record.get('keys', []):
            key_id = key.get('id') if type(key) is dict else None
            key_ref = refs.get(('key', doc, module_id, type_id, key_id))
            components = tuple(authored_fields[(part['module'], part['element'])] for part in key['fields'])
            if (key_ref is None or not components or len(set(components)) != len(components)
                    or any(domains[pid][0] != 'string' for pid in components) or key_id in keys):
                raise ValueError('Current Truss key allocation must bind original STRING key fields')
            keys[key_id] = components
        types[type_id] = {'owner': owner, 'pin': ref['definitionPin'], 'fields': fields,
                          'domains': domains, 'facets': facets, 'required': required, 'keys': keys}
        qualified[owner] = type_id
    edges = {}
    for (kind, doc, module_id, type_id, authored), ref in refs.items():
        if kind != 'relationship':
            continue
        owner = (doc, module_id, authored)
        relation = relationships.get(owner)
        relation_id = ref.get('relationshipId')
        if (type(relation) is not dict or type(relation_id) is not str or relation_id in edges
                or relation.get('directed') is not True or relation.get('targetLifecycle') != 'independent'
                or len(relation.get('source', [])) != 1 or len(relation.get('target', [])) != 1):
            raise ValueError('Supported current directed independent binary relationship required')
        endpoints = []
        for role in ('source', 'target'):
            selected = relation[role][0]
            endpoint_type = qualified.get((doc, selected['module'], selected['element']))
            if endpoint_type is None or ('key' in selected and selected['key'] not in types[endpoint_type]['keys']):
                raise ValueError('Current relationship endpoint must bind an accepted record and declared key')
            endpoints.append(endpoint_type)
        edges[relation_id] = {'owner': owner, 'pin': ref['definitionPin'],
                              'endpoints': tuple(endpoints), 'declaration': relation}
    if not types:
        raise ValueError('At least one supported current UMF record required')
    return {'types': types, 'edges': edges, 'models': {doc: (item[0], item[1]) for doc, item in models.items()},
            'report': report_raw, 'report_object': report, 'view': view_raw,
            'truss_version': CURRENT_TRUSS_VERSION}
