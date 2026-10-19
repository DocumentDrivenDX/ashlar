# SPDX-License-Identifier: MIT OR Apache-2.0
"""Query an existing Truss publication without publishing or acknowledging it."""
from pathlib import Path
from .weft_compatible_cli import CompatibleCompiler
from .lifecycle import owned_context
from .truss_recovery import restore_applied_truss_publication
from .truss_resume_cli import run_selected_truss_command
from .truss_weft_query import execute_truss_numeric_grouped_count_query


def run_query_truss_numeric_grouped_count_command(configuration,output,*,compiler,sql,revision,document_id,physical_revisions,maximum_rows=1000):
    paths=CompatibleCompiler(Path(compiler))
    if type(physical_revisions)is not tuple or not physical_revisions:raise ValueError('Explicit physical revisions required')
    if (type(maximum_rows)is not int or not 1<=maximum_rows<=10000
            or type(sql)is not str or not sql or len(sql.encode())>65536
            or any(type(value)is not str or not value or len(value)>4096 for value in (revision,document_id))):
        raise ValueError('Bounded explicit Truss query selections required')
    def query(driver,config):
        with owned_context(driver.writer(driver.source_sessions.batch().feed,driver.context)):
            restore_applied_truss_publication(driver,config['request_digest'])
        answer=execute_truss_numeric_grouped_count_query(driver,paths,sql,revision=revision,
            document_id=document_id,physical_revisions=physical_revisions,maximum_rows=maximum_rows)
        report={key:value for key,value in answer.items() if key not in ('request_bytes','response_bytes')}
        # Preserve the original protocol strings, including their terminal newline.
        report['request_original_json']=answer['request_bytes'].decode('utf-8')
        report['response_original_json']=answer['response_bytes'].decode('utf-8')
        return report
    return run_selected_truss_command(configuration,output,query)
