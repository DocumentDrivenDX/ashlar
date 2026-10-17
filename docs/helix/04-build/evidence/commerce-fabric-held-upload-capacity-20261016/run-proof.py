"""Review draft. No execution without parent-approved native slot and trial gate."""
from pathlib import Path
import base64, dataclasses, hashlib, json, os, sys, time, uuid
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'source/src'))

def retain(name,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False,default=lambda item: str(item) if isinstance(item,Path) else (_ for _ in ()).throw(TypeError(type(item).__name__)))+'\n').encode()
    fd=os.open(ROOT/'evidence'/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as stream: stream.write(raw);stream.flush();os.fsync(stream.fileno())

def retain_failure(method,path,status,body,headers):
    retain('onelake-operation-refused.json',{'format':'ashlar-private-onelake-refusal/0.1',
        'method':method,'path':path,'status':status,'body_base64':base64.b64encode(body).decode('ascii'),
        'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'headers':headers,
        'qualification':'Original bounded credential-checked response; no retry or creation authority'})

def source_check():
    selection=json.loads((ROOT/'source/source-selection.json').read_bytes())
    assert selection['revision']==(ROOT/'expected-source-revision.txt').read_text().strip()
    for item in selection['files']:
        path=ROOT/'source'/item['path'];raw=path.read_bytes()
        if len(raw)!=item['bytes'] or hashlib.sha256(raw).hexdigest()!=item['sha256']:
            raise PermissionError('Committed source selection changed')
    return selection

def main():
    source_check()
    from ashlar_host.commerce_export_cli import parse_commerce_export_configuration,open_commerce_export_spark,open_registered_commerce_reader
    from ashlar_host.original_commerce_local_setup import original_commerce_local_source_ports
    from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
    from ashlar_host.commerce_query_sources import CommerceQuerySourceFactory
    from ashlar_host.fabric_release_journal import FabricReleaseJournal
    from ashlar_host.fabric_deployment_http import FabricDeploymentHttpConfig,open_fabric_deployment_http
    from ashlar_host.fabric_ordinary_http import FabricOrdinaryHttpConfig,open_fabric_ordinary_http
    from ashlar_host.onelake_release_upload import OneLakeUploadConfig
    from ashlar_host.commerce_fabric_deployment import CommerceFabricDeploymentConfig,open_commerce_fabric_deployment
    from ashlar_host.commerce_fabric_admission import admit_commerce_fabric_deployment,CommerceFabricLoadedGraphPolicy
    from ashlar_host.commerce_fabric_release import execute_commerce_fabric_gql
    config=parse_commerce_export_configuration((ROOT/'selection-original.json').read_bytes())
    # Deliberate pre-execution gate: no receipt/Boolean can authorize native work.
    if sys.argv[1:]!=['--parent-approved-native-slot']:
        raise PermissionError('Draft only: independently review trial-capacity gate and supervisor before execution')
    from runpy import run_path
    run_path(str(ROOT/'preflight.py'),run_name='runtime_preflight')
    # Parent must independently review this runner before launching supervisor.
    authentication=FabricDeploymentHttpConfig(Path('/opt/homebrew/Cellar/azure-cli/2.90.0/bin/az'),Path('/Users/erik/.azure'),
        'ea295b81-083f-44a4-88b2-28ea3297dde9','e64e5956-f78c-4332-913c-b08dc07f58f8','erik.labianca@synaptiq.ai','0a4dbd9c-e1a7-4ec8-8ce9-aa51991ff793')
    from ashlar_host.evolution_producer import capture
    environment={'PATH':'/usr/bin:/bin','HOME':'/Users/erik','AZURE_CONFIG_DIR':str(authentication.azure_config_dir),'AZURE_CORE_COLLECT_TELEMETRY':'0','AZURE_CORE_NO_COLOR':'true'}
    def cloud_get(route):
        stdout,stderr=capture([str(authentication.azure_cli),'rest','--subscription',authentication.account_id,'--method','get','--url','https://api.fabric.microsoft.com/v1/'+route,'--resource','https://api.fabric.microsoft.com','--output','json','--only-show-errors'],cwd=ROOT,environment=environment,timeout_seconds=30,maximum_output_bytes=1048576)
        return json.loads(stdout)
    workspace=cloud_get('workspaces/'+authentication.workspace_id)
    capacities=cloud_get('capacities')
    if capacities.get('continuationToken') or capacities.get('continuationUri'):raise PermissionError('Unbounded capacity listing')
    selected=[item for item in capacities['value'] if item['id']==workspace.get('capacityId')]
    if len(selected)!=1 or selected[0]['id']!='c69c0329-778f-4446-b529-d43358c73251' or selected[0]['sku']!='FTL4' or selected[0]['state']!='Active':raise PermissionError('Existing active free trial capacity required; no settings changes')
    retain('trial-capacity.json',{'workspace':workspace,'selected_capacity':selected[0]})
    candidate=str(uuid.uuid4());suffix=candidate.replace('-','')
    native=Path('/private/tmp/ashlar-commerce-fabric-native-20261016-a')
    deployment_config=CommerceFabricDeploymentConfig(OneLakeUploadConfig(authentication,'ee66f2e8-9072-4a48-8364-ecd2f2967c3e'),
        native/'nodes',native/'edges','Tables/dbo/ashlar_held_'+suffix+'_nodes','Tables/dbo/ashlar_held_'+suffix+'_edges',
        'Ashlar_Held_Commerce_'+suffix,'private-cooperative-sole-creator/0.1')
    retain('selection.json',{'candidate':candidate,'configuration':dataclasses.asdict(deployment_config)},)
    journal=FabricReleaseJournal.initialize(ROOT/'journal')
    ports=original_commerce_local_source_ports(config.source);registration=ports.registration
    def connection(role):
        if role==registration.source_role:return registration.source.connection_factory(None)
        if role==registration.ack_role:return registration.metadata_factory(None)
        raise PermissionError('Unselected ordinary role')
    jars_root=Path('/private/tmp/ashlar-current-commerce-preparation-20261015-9pn8k_h7/query-jars')
    jars=tuple(jars_root/name for name in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar'))
    started=time.monotonic()
    with open_commerce_export_spark(jars,ROOT/'runtime/warehouse') as spark:
        with open_registered_commerce_reader(spark,config.publication,ordinary_session=OrdinaryQueryConnectionFactory(connection),
                source_factory=CommerceQuerySourceFactory(registration),destinations=(ROOT/'journal',ROOT/'runtime',ROOT/'evidence')) as opened:
            original_files=opened.native_files()
            with open_commerce_fabric_deployment(spark,opened,journal,deployment_config,candidate) as deployment:
                from ashlar_host.onelake_release_upload import OneLakeOperationRefused
                from ashlar_host.lifecycle import finish
                try:
                    deployment.stage()
                except OneLakeOperationRefused as primary:
                    method,path,status,body,headers=primary.response
                    finish(primary,[lambda:retain_failure(method,path,status,body,headers)])
                deployment.submit_creation()
                while True:
                    observed=deployment.observe_creation()
                    with deployment.admission_interval() as interval:
                        creation=interval.mutation.creation(candidate)
                    if creation.graph_model_id is not None:break
                    if json.loads(observed.body)['status'] in ('Failed','Cancelled'):raise PermissionError('Creation failed')
                    if time.monotonic()-started>500:raise TimeoutError('Original creation deadline')
                    time.sleep(2);deployment.renew()
                graph=creation.graph_model_id
                retain('pending-portal.json',{'graph_model_id':graph,'workspace_id':authentication.workspace_id,
                    'url':'https://app.fabric.microsoft.com/groups/'+authentication.workspace_id+'/graph/'+graph+'?experience=fabric-developer','instruction':'Actual first portal editor open required; waiting only for native observed job, never a Boolean success file'})
                while True:
                    with open_fabric_deployment_http(authentication) as http:
                        listing=json.loads(http.item_jobs(graph).body)
                    if listing.get('continuationToken') or listing.get('continuationUri'):raise PermissionError('Ambiguous native jobs')
                    if listing['value']:break
                    if time.monotonic()-started>500:raise TimeoutError('First portal load not observed')
                    time.sleep(2);deployment.renew()
                while True:
                    job=json.loads(deployment.observe_initial_load().body)
                    if job['status']=='Completed':break
                    if job['status'] in ('Failed','Cancelled','Deduped'):raise PermissionError('Initial load refused')
                    if time.monotonic()-started>500:raise TimeoutError('Original load deadline')
                    time.sleep(2);deployment.renew()
                gql=FabricOrdinaryHttpConfig(**dataclasses.asdict(authentication),graph_model_id=graph)
                with open_fabric_ordinary_http(gql) as http:
                    binding,revision=admit_commerce_fabric_deployment(deployment,transport=http,transport_context=http.context,expected_revision=0)
                from ashlar.schema import decode_retained_json
                with deployment.admission_interval() as interval:
                    expected_count=len(decode_retained_json(interval.release.payload)['nodes'])
                if expected_count!=11:raise PermissionError('Original admitted commerce node count changed')
            with open_fabric_ordinary_http(gql) as http:
                policy=CommerceFabricLoadedGraphPolicy(journal,deployment_config,http,http.context)
                result=execute_commerce_fabric_gql(opened,binding,'MATCH (n:AshlarNode) RETURN count(n) AS row_count',
                    transport=http,transport_context=http.context,loaded_graph_policy=policy,loaded_graph_context=policy.context)
            if result.columns!=(('row_count','INT64'),) or result.rows!=((expected_count,),):raise PermissionError('Native query count/columns differ from independently admitted release')
            if opened.native_files()!=original_files:raise PermissionError('Original native files changed')
            custody=opened.provider.closed_interval_custody(opened.context)
    source_check()
    retain('report.json',{'binding':dataclasses.asdict(binding),'revision':revision,'columns':result.columns,'rows':result.rows,'request_sha256':result.request_sha256,'source_ack_interval':custody})

if __name__=='__main__':main()
