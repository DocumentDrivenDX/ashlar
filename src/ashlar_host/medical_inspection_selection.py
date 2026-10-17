"""Closed dispatch between independently qualified local inspection owners."""
from .medical_producer import decode_medical
from .medical_commit_inspection_configuration import OriginalMedicalCommitInspectionConfig,parse_original_medical_commit_inspection,inspection_selection_bytes as process_selection_bytes
from .medical_journal_inspection_configuration import OriginalMedicalJournalInspectionConfig,parse_original_medical_journal_inspection,journal_inspection_selection_bytes


def parse_medical_inspection(raw):
    value=decode_medical(raw,262144)
    if type(value)is not dict:raise ValueError('Closed medical inspection selection required')
    profile=value.get('profile')
    if profile=='ashlar-original-medical-local-commit-inspection/0.1':return parse_original_medical_commit_inspection(raw)
    if profile=='ashlar-original-medical-local-journal-inspection/0.1':return parse_original_medical_journal_inspection(raw)
    raise ValueError('Unknown medical inspection profile')


def medical_inspection_selection_bytes(config):
    if type(config)is OriginalMedicalCommitInspectionConfig:return process_selection_bytes(config)
    if type(config)is OriginalMedicalJournalInspectionConfig:return journal_inspection_selection_bytes(config)
    raise ValueError('Exact supported medical inspection configuration required')


def renew_medical_inspection_selection(config,original):
    if medical_inspection_selection_bytes(config)!=original:raise PermissionError('Complete medical inspection selection changed')


def create_medical_inspection(config):
    if type(config)is OriginalMedicalCommitInspectionConfig:
        from .medical_process_inspection import MedicalProcessInspection
        return MedicalProcessInspection(config)
    if type(config)is OriginalMedicalJournalInspectionConfig:
        from .medical_journal_inspection import MedicalJournalInspection
        return MedicalJournalInspection(config)
    raise ValueError('Exact supported medical inspection configuration required')
