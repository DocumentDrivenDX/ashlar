"""Actual original commerce publication -> CQP -> local Spark source refinement.

Read-only original source/ACK and complete publication holds; new private staging
only. Declared local query-profile documents are not application policy grants or
managed Unity Catalog authority. Caller selects every runtime/input explicitly.
"""
from collections import Counter
import argparse
import hashlib
import importlib
import json
import os
import stat
import re
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace


def execute_publication_queries(spark,weft,configuration):
    from ashlar_host.commerce_export_cli import parse_commerce_export_configuration,CommerceExportConfiguration
    from ashlar_host.original_commerce_local_setup import original_commerce_local_source_ports
    from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
    from ashlar_host.commerce_query_sources import CommerceQuerySourceFactory
    from ashlar_host.publication_reader import open_commerce_reader
    from ashlar_host.publication_current_authority import open_publication_current_interval,PublicationCurrentOwner
    from ashlar_host.publication_current_stage import commerce_current_request,commerce_current_rows,stage_commerce_current,create_publication_stage_schema
    from ashlar_host.databricks_current_query import execute_databricks_current_query
    from ashlar_host.path_capture import PathCaptureConfig
    from ashlar_host.lifecycle import owned_context
    config=parse_commerce_export_configuration(configuration.raw)
    if type(config) is not CommerceExportConfiguration:raise ValueError('Explicit existing export-only commerce selection required')
    ports=original_commerce_local_source_ports(config.source);registration=ports.registration
    def connection(role):
        if role==registration.source_role:return registration.source.connection_factory(None)
        if role==registration.ack_role:return registration.metadata_factory(None)
        raise PermissionError('Unselected ordinary role refused')
    results=[]
    with owned_context(open_commerce_reader(spark,config.publication,
            ordinary_session=OrdinaryQueryConnectionFactory(connection),
            source_factory=CommerceQuerySourceFactory(registration))) as opened:
        with open_publication_current_interval(opened) as publication:
            source_rows=commerce_current_rows(opened)
            schema_owner=create_publication_stage_schema(publication,spark,schema='publication_current')
            for case in ('count','sku'):
                raw=json.dumps(commerce_current_request(opened,case=case),ensure_ascii=False,separators=(',',':'))
                parent=weft.prepare_application_lineage(raw);child=parent.prepare_current_query()
                stage=stage_commerce_current(publication,spark,child,schema='publication_current',table='products_'+case,schema_owner=schema_owner)
                owner=PublicationCurrentOwner(publication,stage)
                backend=owner.backend(capture_limits=PathCaptureConfig(1000,65536,1048576),source_limits=PathCaptureConfig(1000,65536,1048576))
                result=execute_databricks_current_query(child,publication.context,backend,engine='spark',refinement=True)
                public=json.loads(result.verified_result)
                expected=[[str(len(source_rows))]] if case=='count' else [[value] for _,value in source_rows]
                if Counter(map(tuple,public['rows']))!=Counter(map(tuple,expected)):raise ValueError('Complete publication-derived independent result differs')
                results.append(dict(case=case,request=json.loads(raw),binding=json.loads(result.original_binding),artifact=json.loads(result.original_artifact),public=public,refinement=json.loads(result.refinement_report)))
        publication_id=opened.manifest['publication_id']
    configuration.renew(262144)
    return dict(scope='Actual original private commerce publication/source ACK; declared local profile, not UMF/UC authorization',publication_id=publication_id,cases=results)


def require_fresh_output(output):
    if (not output.is_absolute() or output.exists() or output.is_symlink()
            or output.parent.resolve()!=output.parent
            or output.parent.stat().st_uid!=os.getuid()
            or stat.S_IMODE(output.parent.stat().st_mode)!=0o700):
        raise ValueError('Fresh absolute output under canonical owned private0700 parent required')


def load_selected_weft(root, module_sha256, shim_sha256):
    if any(name=='weft' or name.startswith('weft.') for name in sys.modules):
        raise ValueError('Preloaded Weft modules refused')
    if not root.is_absolute() or root.resolve()!=root or not root.is_dir():
        raise ValueError('Explicit canonical native Weft directory required')
    paths=(root/'weft'/'weft.abi3.so',root/'weft'/'__init__.py')
    digests=(module_sha256,shim_sha256)
    for path,digest in zip(paths,digests):
        if (type(digest)is not str or re.fullmatch('[0-9a-f]{64}',digest)is None
                or path.resolve()!=path or not path.is_file()
                or hashlib.sha256(path.read_bytes()).hexdigest()!=digest):
            raise ValueError('Selected native module/shim custody differs')
    sys.path.insert(0,str(root))
    weft=importlib.import_module('weft');native=importlib.import_module('weft.weft')
    if Path(weft.__file__).resolve()!=paths[1] or Path(native.__file__).resolve()!=paths[0]:
        raise ValueError('Actual imported package/native origins differ')
    for path,digest in zip(paths,digests):
        if hashlib.sha256(path.read_bytes()).hexdigest()!=digest:
            raise ValueError('Imported native module/shim bytes changed')
    return weft


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('configuration','weft-root','java-home','jars','output'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--weft-module-sha256',required=True);parser.add_argument('--weft-shim-sha256',required=True)
    args=parser.parse_args()
    from ashlar_host.evolution_cli import InvocationFile
    from ashlar_host.commerce_export_cli import open_commerce_export_spark
    from ashlar_host.commerce import runtime_paths
    from ashlar_host.lifecycle import owned_context
    require_fresh_output(args.output)
    weft=load_selected_weft(args.weft_root,args.weft_module_sha256,args.weft_shim_sha256)
    captured=InvocationFile.read(args.configuration,262144)
    os.environ['JAVA_HOME']=str(args.java_home);os.environ['PYSPARK_PYTHON']=sys.executable
    if not hasattr(weft.CurrentQueryPreparation,'check_spark_current_contributors_candidate_json'):raise ValueError('Explicit Spark compiler feature required')
    from ashlar_host.commerce_export_cli import parse_commerce_export_configuration,CommerceExportConfiguration
    selected=parse_commerce_export_configuration(captured.raw)
    if type(selected) is not CommerceExportConfiguration:raise ValueError('Existing export-only configuration required')
    jar_paths=runtime_paths(SimpleNamespace(output=args.output,jars=args.jars,model=selected.publication/'original-ontology.json',graph=selected.publication/'original-graph.json'),False)
    args.output.mkdir(mode=0o700)
    with tempfile.TemporaryDirectory(prefix='native-',dir=args.output) as work:
        with owned_context(open_commerce_export_spark(jar_paths,Path(work)/'warehouse')) as spark:
            spark.sparkContext.setLogLevel('ERROR')
            result=execute_publication_queries(spark,weft,captured)
    captured.renew(262144)
    payload=json.dumps(result,ensure_ascii=False,separators=(',',':')).encode()
    if len(payload)>4194304:raise ValueError('Bounded final report required')
    from ashlar_host.commerce_query_lifecycle import publish_commerce_query_report
    publish_commerce_query_report(args.output,payload)

if __name__=='__main__':main()
