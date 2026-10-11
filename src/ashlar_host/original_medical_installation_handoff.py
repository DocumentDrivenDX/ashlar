"""Passive installation correspondence; current lease/native owners grant authority."""
import hashlib
import json
import re
from urllib.parse import urlsplit, unquote
from .delta_custody import DeltaTarget
from .local_physical_carriers import original_local_physical_carriers, ORIGINAL_LOCAL_PHYSICAL_PROVENANCE
from .medical_installation import OriginalMedicalInstallationSelection
from .original_medical_local_configuration import (
    OriginalMedicalPublicationConfig, OriginalMedicalReplayConfig, OriginalMedicalReadConfig, OriginalMedicalRecoveryConfig)


def _closed(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        raise ValueError('Closed original medical installation correspondence required')


def _same(left, right):
    return json.dumps(left, sort_keys=True, separators=(',', ':'), allow_nan=False) == json.dumps(
        right, sort_keys=True, separators=(',', ':'), allow_nan=False)


def validate_original_medical_installation_handoff(raw, config):
    """Check unchanged selected bytes and six-role stages without native or SQL effects."""
    if type(config) not in (OriginalMedicalPublicationConfig, OriginalMedicalReplayConfig, OriginalMedicalReadConfig, OriginalMedicalRecoveryConfig):
        raise ValueError('Exact existing medical operation configuration required')
    config.__post_init__()
    if (type(raw) is not bytes or not raw or len(raw) > min(config.bounds.receipt_bytes, config.bounds.input_bytes)
            or len(raw) != config.installation.bytes or hashlib.sha256(raw).hexdigest() != config.installation.sha256):
        raise PermissionError('Original bounded installation bytes differ')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('Duplicate installation member')
            result[key] = value
        return result
    def nonfinite(value): raise ValueError('Nonfinite installation value')
    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)
    _closed(value, ('profile','installation_id','journal','targets','capacity',
                    'generated_provenance_sha256','qualification','native'))
    expected_targets = [{'table':'local.original_medical.'+target.table,
                         'path':str(target.path),'uuid':target.uuid} for target in config.targets]
    if (value['profile'] != 'ashlar-original-medical-local-installation/0.1'
            or value['installation_id'] != config.installation_id or value['journal'] != str(config.journal)
            or value['generated_provenance_sha256'] != ORIGINAL_LOCAL_PHYSICAL_PROVENANCE
            or not _same(value['targets'], expected_targets) or not _same(value['capacity'], dict(config.capacity))
            or type(value['qualification']) is not str or not 1 <= len(value['qualification'].encode()) <= 4096):
        raise PermissionError('Complete original installation selection differs')
    roles = tuple(target.table for target in config.targets)
    _closed(value['native'], roles)
    generated = original_local_physical_carriers(); registered_properties = None
    for target in config.targets:
        stages = value['native'][target.table]; _closed(stages, ('creation','registration'))
        for stage, version in (('creation',0), ('registration',1)):
            witness = stages[stage]
            _closed(witness, ('uuid','version','location','schema','properties','minReaderVersion','minWriterVersion','history'))
            location = urlsplit(witness['location']) if type(witness['location']) is str else None
            if (witness['uuid'] != target.uuid or type(witness['version']) is not int or witness['version'] != version
                    or type(witness['minReaderVersion']) is not int or witness['minReaderVersion'] != 1
                    or type(witness['minWriterVersion']) is not int or witness['minWriterVersion'] != 2
                    or location is None or location.scheme != 'file' or location.netloc or location.query or location.fragment
                    or unquote(location.path) != str(target.path)
                    or not _same(witness['schema'], json.loads(generated['carriers'][target.table]['schemaJson']))):
                raise PermissionError('Original complete native stage differs')
            properties = witness['properties']
            if stage == 'creation':
                if not _same(properties, {}): raise PermissionError('Original empty creation properties differ')
                history = [{'version':0, 'userMetadata':None}]
            else:
                _closed(properties, ('ashlar.local.installation.id','ashlar.local.registry.sha256','ashlar.local.journal.sha256'))
                if properties['ashlar.local.installation.id'] != config.installation_id or any(
                        type(properties[key]) is not str or re.fullmatch('[0-9a-f]{64}', properties[key]) is None
                        for key in ('ashlar.local.registry.sha256','ashlar.local.journal.sha256')):
                    raise PermissionError('Original registration properties differ')
                if registered_properties is not None and properties != registered_properties:
                    raise PermissionError('Mixed original registration correspondence')
                registered_properties = dict(properties)
                metadata = json.dumps({'profile':'ashlar-local-delta-installation-commit/0.1',
                    'installation_id':config.installation_id,
                    'registry_sha256':properties['ashlar.local.registry.sha256'],
                    'journal_sha256':properties['ashlar.local.journal.sha256'], 'uuid':target.uuid},
                    sort_keys=True, separators=(',', ':'))
                history = [{'version':1, 'userMetadata':metadata, 'operation':'SET TBLPROPERTIES'},
                           {'version':0, 'userMetadata':None}]
            if not _same(witness['history'], history):
                raise PermissionError('Original complete native history stage differs')
    targets = tuple(DeltaTarget(row['table'], target.path, row['uuid'])
                    for row, target in zip(expected_targets, config.targets))
    return OriginalMedicalInstallationSelection(config.installation_id, config.journal, targets, config.capacity)
