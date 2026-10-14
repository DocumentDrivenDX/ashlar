"""One exact authored commerce case on an existing Truss publication."""
from pathlib import Path
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths
from .evolution_cli import InvocationFile
from .commerce_original_scenarios import commerce_original_cases
from .lifecycle import owned_context,finish
from .truss_recovery import restore_applied_truss_publication
from .truss_resume_cli import run_selected_truss_command
from .truss_indexed_commerce_query import execute_truss_indexed_commerce_query


def run_query_truss_commerce_command(configuration,output,*,index,installation,pack_path,case_id,revision,document_id,maximum_rows=1000):
    pack=InvocationFile.read(Path(pack_path),262144)
    cases=dict(commerce_original_cases(pack.raw))
    if case_id not in cases or type(maximum_rows)is not int or not 1<=maximum_rows<=10000 or any(type(value)is not str or not value or len(value.encode())>4096 for value in(revision,document_id)):raise ValueError('Explicit bounded original commerce selections required')
    paths=ImplicitArithmeticDistributionPaths(Path(index),Path(installation))
    def query(driver,config):
        pack.renew(262144)
        with owned_context(driver.writer(driver.source_sessions.batch().feed,driver.context)):
            restore_applied_truss_publication(driver,config['request_digest'])
        answer=execute_truss_indexed_commerce_query(driver,paths,case_id=case_id,pack_bytes=pack.raw,revision=revision,document_id=document_id,maximum_rows=maximum_rows)
        pack.renew(262144)
        result={key:value for key,value in answer.items()if key not in('request_bytes','response_bytes','recompiled_bytes')}
        for key,label in(('request_bytes','request_original_json'),('response_bytes','response_original_json'),('recompiled_bytes','recompiled_original_json')):result[label]=answer[key].decode('utf8')
        return result
    # The shared composition closes actual source, Spark, native pins and input
    # custody before its private report write. Never commit or ACK a query.
    def write(result,destination):
        from .truss_example import private_file
        from .exact_report import report_body
        pack.renew(262144)
        result=dict(result)
        result['qualification']='Original indexed0.4.2 commerce case on a held Truss publication, truthful per-record homogeneous physical revisions, full original source/capacity guards and independent current positional occurrence bag. Owned outer source/Spark cleanup completed before report output; no production qualification.'
        private_file(destination,report_body(result,16777216));return result
    try:return run_selected_truss_command(configuration,output,query,report_writer=write)
    except BaseException as error:finish(error,[lambda:pack.renew(262144)])
