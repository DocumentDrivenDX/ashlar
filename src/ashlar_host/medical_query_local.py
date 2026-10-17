"""Public finite medical query composition; one reader and one native session."""
from .medical_query_batch import MedicalQueryBatch
from .original_medical_local_read import read_original_medical_local


def query_original_medical_cases(configuration,compiler_paths,*,cases=('singleton','two-hop'),supervision=None):
    selection=MedicalQueryBatch(compiler_paths,cases)
    return read_original_medical_local(configuration,query_batch=selection,supervision=supervision)
