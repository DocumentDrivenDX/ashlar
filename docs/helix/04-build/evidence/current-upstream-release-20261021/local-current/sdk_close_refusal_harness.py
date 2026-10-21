"""Task-private SDK close-time setting-drift refusal; no source values changed."""
from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import sys
from umf_access.types import StrictModel
from ashlar.weft_current_distribution import CurrentWeftDistribution, CurrentWeftDistributionPaths
from ashlar_host.configured_local_cli import parse_configured_local
from ashlar_host.configured_local_runtime import page_configured_local
from ashlar_host.configured_sdk_pages import ConfiguredSdkPagesConfig
from ashlar_host.configured_local_runtime_profile import PARAMETER_SUBSTITUTION_SETTING
from ashlar_host.path_capture import PathCaptureConfig
import ashlar_host.configured_sdk_pages as sdk_pages

class Parameters(StrictModel):
    pass
class Person(StrictModel):
    person_key: str
    person_name: str

invocation=Path(sys.argv[1]); raw=invocation.read_bytes()
if len(raw)>262144: raise ValueError('invocation bound exceeded')
parsed=parse_configured_local(raw)
compiler=CurrentWeftDistribution(CurrentWeftDistributionPaths(parsed.query.compile.paths.index))
query=ConfiguredSdkPagesConfig(compiler=compiler,sql='SELECT p.person_key,p.person_name FROM Person p',
    parameters_type=Parameters,result_type=Person,parameters=Parameters(),page_size=2,
    maximum_rows=1000,timeout_seconds=90,capture=PathCaptureConfig(1000,65536,1048576))
fault_output=parsed.runtime.output.parent/'sdk-person-pages-current-sdk-close-refusal'
if fault_output.exists(): raise ValueError('fresh close-refusal output required')
fault_runtime=replace(parsed.runtime,output=fault_output,
    admission=replace(parsed.runtime.admission,output=fault_output/'admission'))
original_hold_page=sdk_pages._Owner.hold_page
page_closes=0; injected=False; restored=False
@contextmanager
def drift_at_second_page_close(owner,vector,*,context):
    global page_closes,injected,restored
    page_closes+=1; old=None
    try:
        with original_hold_page(owner,vector,context=context):
            yield vector
            if page_closes==2:
                spark=owner.scope.transport.spark
                old=spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)
                spark.conf.set(PARAMETER_SUBSTITUTION_SETTING,'false')
                injected=spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)=='false'
    finally:
        if old is not None:
            owner.scope.transport.spark.conf.set(PARAMETER_SUBSTITUTION_SETTING,old)
            restored=owner.scope.transport.spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)==old
sdk_pages._Owner.hold_page=drift_at_second_page_close
refused=False
try:
    try:
        page_configured_local(fault_runtime,ordinal=1,query=query)
    except ValueError as exc:
        if str(exc)!='Configured local runtime refused': raise
        refused=True
finally:
    sdk_pages._Owner.hold_page=original_hold_page
if page_closes!=2 or not injected or not restored or not refused:
    raise ValueError('SDK close-time runtime drift did not refuse after the continuation page')
if (fault_output/'runtime-report.json').exists():
    raise ValueError('closing refusal must withhold the buffered page result')
print(json.dumps({'status':'refused-at-enclosing-runtime-close','refusal':'Configured local runtime refused',
    'page_closes':page_closes,'setting_flipped_on_second_close':injected,
    'setting_restored':restored,'runtime_report_written':False,'buffered_result_withheld':True,
    'page_size':2,'original_sql':query.sql},ensure_ascii=False,sort_keys=True,separators=(',',':')))
