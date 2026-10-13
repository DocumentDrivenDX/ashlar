"""Installed bounded source-custody command; native deployment is a separate host."""
import argparse
import base64
from dataclasses import asdict
import json
import sys
from .source import jsonl_batches


def main():
    parser = argparse.ArgumentParser(prog='ashlar')
    commands = parser.add_subparsers(dest='command', required=True)
    diagnostics = commands.add_parser('diagnostics', help='Read one bounded closed diagnostic run')
    diagnostics.add_argument('--run-directory', required=True)
    diagnostics.add_argument('--attempt-id')
    diagnostics.add_argument('--min-severity', type=int, choices=(9, 13, 17), default=9)
    diagnostics.add_argument('--event-name', choices=('ashlar.operation.started',
        'ashlar.operation.phase', 'ashlar.operation.finished'))
    diagnostics.add_argument('--limit', type=int, default=50)
    install_weft = commands.add_parser('install-weft', help='Verify and install the pinned local Weft candidate package')
    for name in ('index', 'package', 'output'):
        install_weft.add_argument('--' + name, required=True)
    compile_weft = commands.add_parser('compile-weft', help='Compile original stdin through a retained indexed installation')
    for name in ('index', 'installation'):
        compile_weft.add_argument('--' + name, required=True)
    install_paths = commands.add_parser('install-weft-paths', help='Verify and install the separately pinned Paths candidate')
    for name in ('index', 'package', 'output'):
        install_paths.add_argument('--' + name, required=True)
    compile_paths = commands.add_parser('compile-weft-paths', help='Compile original stdin through a retained Paths installation')
    for name in ('index', 'installation'):
        compile_paths.add_argument('--' + name, required=True)
    for command in (install_paths, compile_paths):
        command.add_argument('--profile', default='paths', choices=('paths', 'paths-keys'))
    install_implicit = commands.add_parser('install-weft-implicit-arithmetic', help='Install the fixed indexed 0.4.2 compiler')
    for name in ('index', 'package', 'output'):
        install_implicit.add_argument('--' + name, required=True)
    compile_implicit = commands.add_parser('compile-weft-implicit-arithmetic', help='Compile original stdin through the fixed 0.4.2 installation')
    for name in ('index', 'installation'):
        compile_implicit.add_argument('--' + name, required=True)
    install_count_star = commands.add_parser('install-weft-count-star', help='Verify and install the indexed COUNT star candidate')
    for name in ('index', 'package', 'output'):
        install_count_star.add_argument('--' + name, required=True)
    compile_count_star = commands.add_parser('compile-weft-count-star', help='Compile original stdin through the selected COUNT star installation')
    for name in ('index', 'installation'):
        compile_count_star.add_argument('--' + name, required=True)
    supply_compile = commands.add_parser('compile-supply-chain-count-star', help='Compile all five unchanged original supply-chain queries with selected041; no native authority')
    for name in ('pack','model','graph','metadata','index','installation','output'):
        supply_compile.add_argument('--'+name, required=True)
    supply_compile.add_argument('--maximum-artifact-bytes',type=int,required=True)
    supply_compile.add_argument('--case-id',action='append',help='Original case ID; repeat to select cases, default all five')
    inspect = commands.add_parser('inspect-source', help='Verify committed JSONL batches from stdin; no ACK')
    inspect.add_argument('--feed', required=True)
    inspect.add_argument('--epoch', required=True)
    inspect.add_argument('--cursor-before', default='0')
    csv = commands.add_parser('inspect-csv', help='Adapt bounded UTF-8 CSV to retained source batches/checkpoints; no ACK')
    for name in ('feed','epoch','source-system','schema-revision','type-id','properties-json'):
        csv.add_argument('--'+name,required=True)
    configured=commands.add_parser('inspect-configured-source',help='Inspect pinned model/ID/source configuration and local replay; no database I/O')
    configured.add_argument('configuration')
    commerce=commands.add_parser('commerce-source',help='Build an exact original commerce candidate with development bindings')
    for name in ('ontology','graph','output'):
        commerce.add_argument('--'+name,required=True)
    commerce.add_argument('--source-system',required=True)
    commerce.add_argument('--binding-profile',choices=['ashlar-commerce-development-bindings/0.1','ashlar-commerce-development-bindings/0.2'],default='ashlar-commerce-development-bindings/0.2')
    for pack in ('archaeology','ecology'):
        command=commands.add_parser(pack+'-source',help='Build an exact original '+pack+' candidate with development bindings')
        for name in ('ontology','graph','output','source-system'):
            command.add_argument('--'+name,required=True)
    medical=commands.add_parser('medical-source',help='Build an exact historical medical candidate using explicit public admission')
    for name in ('ontology','graph','public-admission','output','source-system'):
        medical.add_argument('--'+name,required=True)
    medical.add_argument('--binding-profile',required=True,choices=['ashlar-medical-development-bindings/0.2'])
    for name in ('publish-commerce', 'query-commerce', 'query-commerce-paths', 'query-commerce-count-star', 'query-commerce-implicit-arithmetic'):
        command = commands.add_parser(name, help='Run the explicit local commerce native host profile')
        for option in ('output', 'jars', 'model', 'graph', 'umf-source', 'bun', 'git',
                       'postgres-container', 'postgres-host', 'postgres-database'):
            command.add_argument('--' + option, required=True)
        for option in ('producer-timeout-seconds', 'producer-maximum-output-bytes',
                       'producer-maximum-receipt-bytes', 'postgres-port'):
            command.add_argument('--' + option, type=int, required=True)
        if name == 'publish-commerce':
            for option in ('source-system', 'binding-profile'):
                command.add_argument('--' + option, required=True)
        else:
            for option in ('index', 'installation', 'publication'):
                command.add_argument('--' + option, required=True)
        if name in ('query-commerce-count-star','query-commerce-implicit-arithmetic'):
            command.add_argument('--case',dest='case_ids',action='append',default=[],help='Execute only these named authored queries; repeat to select multiple cases')
        if name in ('query-commerce-paths','query-commerce-count-star','query-commerce-implicit-arithmetic'):
            if name in ('query-commerce-count-star','query-commerce-implicit-arithmetic'):
                command.add_argument('--postgres-profile',default='legacy-private',choices=('legacy-private','explicit-loopback'))
                for option in ('session-provider', 'session-provider-sha256', 'pack'):
                    command.add_argument('--' + option, required=name=='query-commerce-implicit-arithmetic')
            command.add_argument('--diagnostics-config')
            if name == 'query-commerce-paths':
                command.add_argument('--profile', default='paths', choices=('paths', 'paths-keys'))
            for option in ('maximum-artifact-bytes', 'maximum-rows',
                           'maximum-cell-bytes', 'maximum-total-cell-bytes'):
                command.add_argument('--' + option, type=int, required=True)
    for name in ('publish-commerce-evolution', 'resume-commerce-evolution', 'continue-commerce-evolution'):
        command = commands.add_parser(name, help='Run the original eight-step evolution with an explicit trusted operator provider')
        for option in ('configuration', 'provider', 'provider-sha256'):
            command.add_argument('--' + option, required=True)
    truss_preparation = commands.add_parser('prepare-truss-catalog', help='Prepare original UMF through an explicitly selected Truss package; no catalog acceptance or feed authority')
    truss_preparation.add_argument('--config', required=True)
    truss_note=commands.add_parser('publish-truss-note',help='Publish a few complete Truss Note transactions into a fresh local Delta destination, then ACK')
    truss_note.add_argument('--configuration',required=True)
    truss_note.add_argument('--output',required=True)
    truss_graph=commands.add_parser('publish-truss-graph',help='Publish accepted scalar Truss records and edges to fresh local Delta tables, then ACK')
    truss_graph.add_argument('--configuration',required=True)
    truss_graph.add_argument('--output',required=True)
    truss_resume=commands.add_parser('resume-truss-graph',help='Resume an original local Truss graph publication from its existing journal')
    truss_resume.add_argument('--configuration',required=True)
    truss_resume.add_argument('--output',required=True)
    truss_resume_setup=commands.add_parser('configure-truss-resume',help='Create a private resume configuration from the original publisher configuration and saved intent; no engine execution')
    truss_resume_setup.add_argument('--configuration',required=True)
    truss_resume_setup.add_argument('--publication-intent',required=True)
    truss_resume_setup.add_argument('--output',required=True)
    truss_export=commands.add_parser('export-truss-graph',help='Export an explicitly selected existing Truss publication for graph tools; no ingestion or ACK')
    truss_export.add_argument('--configuration',required=True)
    truss_export.add_argument('--output',required=True)
    truss_record=commands.add_parser('read-truss-record',help='Read named fields and original carriers from an explicitly selected Truss object')
    for option in ('configuration','output'):
        truss_record.add_argument('--'+option,required=True)
    truss_record.add_argument('--type-id',type=int,required=True)
    truss_record.add_argument('--entity-id',type=int,required=True)
    truss_singleton=commands.add_parser('read-truss-singleton',help='Read one object or edge from an explicitly selected Truss publication; no compiler, ingestion or ACK')
    for option in ('configuration','output'):
        truss_singleton.add_argument('--'+option,required=True)
    truss_singleton.add_argument('--kind',choices=('object','edge'),required=True)
    truss_singleton.add_argument('--type-id',type=int,required=True)
    truss_singleton.add_argument('--entity-id',type=int,required=True)
    truss_batch=commands.add_parser('query-truss-graph-batch',help='Run a bounded named Weft query gallery on one original Truss publication with one owned runtime')
    for option in ('configuration','output','weft-index','weft-installation','batch'):
        truss_batch.add_argument('--'+option,required=True)
    truss_query=commands.add_parser('query-truss-graph',help='Run Weft STRING, global/grouped COUNT, HAS_RELATED or authored forward/inverse RELATED_KEYS queries against an original local Truss publication; no publication or ACK')
    for option in ('configuration','output','weft-index','weft-installation','sql','revision','document-id'):
        truss_query.add_argument('--'+option,required=True)
    truss_query.add_argument('--maximum-rows',type=int,default=1000)
    truss_compatible=commands.add_parser('query-truss-compatible',help='Run local compatible-revision candidate direct STRING projections under publication custody')
    for option in ('configuration','output','compiler','sql','revision','document-id'):
        truss_compatible.add_argument('--'+option,required=True)
    truss_compatible.add_argument('--physical-revision',action='append',required=True)
    truss_compatible.add_argument('--maximum-rows',type=int,default=1000)
    truss_scalars=commands.add_parser('query-truss-scalars',help='Run local compatible-revision candidate direct required scalar projections under publication custody')
    for option in ('configuration','output','compiler','sql','revision','document-id'):
        truss_scalars.add_argument('--'+option,required=True)
    truss_scalars.add_argument('--physical-revision',action='append',required=True)
    truss_scalars.add_argument('--maximum-rows',type=int,default=1000)
    truss_scalar_join=commands.add_parser('query-truss-scalar-join',help='Run local compatible-revision candidate required numeric equality INNER JOIN projections under publication custody')
    for option in ('configuration','output','compiler','sql','revision','document-id'):
        truss_scalar_join.add_argument('--'+option,required=True)
    truss_scalar_join.add_argument('--physical-revision',action='append',required=True)
    truss_scalar_join.add_argument('--maximum-rows',type=int,default=1000)
    truss_saved=commands.add_parser('publish-saved-truss-graph',help='Publish a selected saved Truss page whose first source plan was never retained')
    for option in ('configuration','publication-intent','publication-intent-sha256','output'):
        truss_saved.add_argument('--'+option,required=True)
    truss_target=commands.add_parser('update-delete-truss-target',help='Update the selected cohort B label, then delete its edge and target using public Truss groups')
    truss_target.add_argument('--configuration',required=True)
    truss_target.add_argument('--output',required=True)
    truss_continue=commands.add_parser('continue-truss-graph',help='Consume new contiguous Truss pages into an existing publication and journal')
    truss_continue.add_argument('--configuration',required=True)
    truss_continue.add_argument('--output',required=True)
    truss_continue.add_argument('--maximum-pages',type=int,default=1)
    truss_continue.add_argument('--wait-seconds',type=int,default=0)
    truss_continue.add_argument('--maximum-page-bytes',type=int,default=1048576)
    truss_evolve=commands.add_parser('evolve-truss-graph',help='Accept a selected record evolution and publish its two complete feed pages')
    truss_evolve.add_argument('--configuration',required=True)
    truss_evolve.add_argument('--reader-configuration',required=True)
    truss_evolve.add_argument('--output',required=True)
    truss_example=commands.add_parser('run-truss-example',help='Run a fresh local Truss to Delta example with optional streamed mutations and a query or gallery using selected runtimes')
    truss_example.add_argument('--configuration',required=True)
    truss_example.add_argument('--output',required=True)
    truss_record_example=commands.add_parser('run-truss-record-example',help='Run a fresh local typed Truss record through Delta and an exact singleton read')
    truss_record_example.add_argument('--configuration',required=True)
    truss_record_example.add_argument('--output',required=True)
    truss_setup=commands.add_parser('setup-truss-local',help='Initialize a fresh local Truss installation for the selected original UMF document and stop its owned server')
    truss_setup.add_argument('--configuration',required=True)
    truss_setup.add_argument('--output',required=True)
    truss_server=commands.add_parser('serve-truss-local',help='Run an initialized local Truss server in the foreground until Ctrl-C')
    truss_server.add_argument('--data-directory',required=True)
    truss_server.add_argument('--connection-output')
    truss_next=commands.add_parser('configure-truss-continuation',help='Select the exact last completed intent for another bounded continuation; no source connections or grants')
    truss_next.add_argument('--configuration',required=True)
    truss_next.add_argument('--continuation-result',required=True)
    truss_next.add_argument('--output',required=True)
    truss_reader=commands.add_parser('configure-truss-example-reader',help='Prepare a private ordinary reader configuration for a restarted original example; no connections or grants')
    truss_reader.add_argument('--example-directory',required=True)
    truss_reader.add_argument('--connection-configuration',required=True)
    truss_reader.add_argument('--output',required=True)
    truss_reader.add_argument('--reader-configuration',help='Explicit saved reader or continuation configuration; defaults to the original STRING demo query configuration')
    for name in ('mutate-truss-note','evolve-truss-note','evolve-truss-record','create-truss-record','update-truss-record','delete-truss-record','mutate-truss-graph','change-truss-graph','accept-truss-schema'):
        truss_operation=commands.add_parser(name,help='Apply the selected mutation or schema evolution through public Truss APIs')
        truss_operation.add_argument('--configuration',required=True)
        truss_operation.add_argument('--output',required=True)
    verification = commands.add_parser('verify-commerce-evolution-publication', help='Verify one completed original publication under explicit finite operator supervision')
    for option in ('configuration', 'provider', 'provider-sha256'):
        verification.add_argument('--' + option, required=True)
    for name in ('activate-puppy-release', 'read-puppy-release'):
        command = commands.add_parser(name, help='Activate or read one explicit original PuppyGraph release handle')
        for option in ('configuration', 'provider', 'provider-sha256'):
            command.add_argument('--' + option, required=True)
    recovery = commands.add_parser('resume-puppy-release', help='Reconcile an interrupted activation from its explicitly selected original intent')
    for option in ('configuration', 'provider', 'provider-sha256', 'original-intent',
                   'original-intent-sha256', 'attempt-directory'):
        recovery.add_argument('--' + option, required=True)
    preparation = commands.add_parser('prepare-puppy-release', help='Prepare exact local carrier tables under independent original-release admission')
    for option in ('configuration','provider','provider-sha256'):
        preparation.add_argument('--'+option,required=True)
    for name in ('publish-finite-pack','query-finite-pack'):
        command=commands.add_parser(name,help='Publish or query one original finite pack under its actual local source/publication owner')
        command.add_argument('--configuration',required=True)
        if name=='query-finite-pack':command.add_argument('--case',action='append',help='Run only this original authored case; repeat to select more')
    supply_configuration=commands.add_parser('configure-supply-chain-native',help='Print a local five-query and two-hop configuration; no engine execution')
    for option in ('output','jars','pack','model','graph','producer-source','bun','git','count-star-index','count-star-installation','paths-index','paths-installation'):
        supply_configuration.add_argument('--'+option,required=True)
    supply_common_setup=commands.add_parser('configure-supply-chain-common',help='Prepare original common query configuration without starting an engine')
    for option in ('configuration','python','java-home','output'):supply_common_setup.add_argument('--'+option,required=True)
    supply_common=commands.add_parser('query-supply-chain-common',help='Run selected unchanged common cases on an existing original supply-chain publication; no reingest')
    supply_common.add_argument('--configuration',required=True)
    supply_common.add_argument('--output',required=True)
    supply_common.add_argument('--case',dest='cases',action='append')
    supply_native=commands.add_parser('run-supply-chain-native',help='Publish, replay and query all five original supply-chain scenarios in an explicit local installation')
    supply_native.add_argument('--configuration',required=True)
    materialize_export=commands.add_parser('materialize-graph-export',help='Materialize an explicitly selected private export into fresh local Delta tables')
    for option in ('release','custody','release-sha256','custody-sha256','node-path','edge-path','jars','output'):
        materialize_export.add_argument('--'+option,required=True)
    commerce_export=commands.add_parser('export-original-commerce',help='Export an existing original commerce graph and its actual closed private custody receipt')
    for option in ('configuration','publication','output','jars'):
        commerce_export.add_argument('--'+option,required=True)
    finite_export=commands.add_parser('export-original-finite-pack',help='Export an existing original finite-file publication into immutable graph and file-custody bytes')
    finite_export.add_argument('--configuration',required=True)
    finite_export.add_argument('--output',required=True)
    fabric_notebook=commands.add_parser('prepare-fabric-notebook',help='Generate a private runnable Fabric notebook from an explicitly selected export pair')
    for option in ('release','custody','release-sha256','custody-sha256','nodes','edges','output','display-name'):
        fabric_notebook.add_argument('--'+option,required=True)
    fabric_definition=commands.add_parser('prepare-fabric-graph-definition',help='Generate local Fabric graph mappings for explicit OneLake release tables')
    for option in ('nodes','edges','output'):
        fabric_definition.add_argument('--'+option,required=True)
    fabric_definition.add_argument('--display-name',help='Also emit a complete Fabric Create Graph Model request')
    fabric=commands.add_parser('query-fabric-gql',help='Query an explicitly admitted Fabric graph through a trusted operator provider')
    for option in ('configuration','provider','provider-sha256'):
        fabric.add_argument('--'+option,required=True)
    export_setup=commands.add_parser('configure-graph-export',help='Create a private saved-export selection from its original result without loading an engine')
    for option in ('export-report','export-report-sha256','output'):
        export_setup.add_argument('--'+option,required=True)
    graphframes_setup=commands.add_parser('configure-graphframes-export',help='Create a private GraphFrames configuration from a selected immutable export report; no engine execution')
    for option in ('export-report','export-report-sha256','java-home','output'):
        graphframes_setup.add_argument('--'+option,required=True)
    graphframes_setup.add_argument('--jar',dest='jars',action='append',required=True)
    graphframes_export=commands.add_parser('run-graphframes-export',help='Run bounded traversal and count queries over an immutable local graph export')
    graphframes_export.add_argument('--configuration',required=True)
    graphframes_export.add_argument('--output',required=True)
    graphframes_export.add_argument('--maximum-hops',type=int,choices=(1,2),default=1)
    duckdb_export=commands.add_parser('query-duckdb-export',help='Run bounded topology queries over an immutable saved graph with DuckDB')
    duckdb_export.add_argument('--configuration',required=True)
    duckdb_export.add_argument('--output',required=True)
    duckdb_export.add_argument('--maximum-hops',type=int,choices=(1,2),default=1)
    graph_diff=commands.add_parser('compare-graph-exports',help='Compare two immutable local graph exports without starting an engine')
    for option in ('first-configuration','second-configuration','output'):
        graph_diff.add_argument('--'+option,required=True)
    graphframes = commands.add_parser('query-graphframes', help='Query one explicitly admitted original graph release in an isolated GraphFrames runtime')
    for option in ('configuration', 'provider', 'provider-sha256'):
        graphframes.add_argument('--' + option, required=True)
    graphframes.add_argument('--case',dest='cases',action='append',default=[],help='Run only the selected original authored case; repeat for multiple cases')
    for name in ('initialize-original-commerce','publish-original-commerce'):
        local=commands.add_parser(name,help='Explicit original-commerce local initialization or existing publication')
        local.add_argument('--configuration',required=True)
    for name in ('initialize-original-medical','publish-original-medical','replay-original-medical','read-original-medical','recover-original-medical'):
        medical=commands.add_parser(name,help='Explicit supervised original medical initialization, publication or settled replay')
        medical.add_argument('--configuration',required=True)
    args = parser.parse_args()
    if args.command in ('initialize-original-medical','publish-original-medical','replay-original-medical','read-original-medical','recover-original-medical'):
        from pathlib import Path
        from ashlar_host.original_medical_local_cli import run_original_medical_initialization_command, run_original_medical_publication_command, run_original_medical_replay_command, run_original_medical_read_command, run_original_medical_recovery_command, medical_failure_metadata
        try:
            entry={'initialize-original-medical':run_original_medical_initialization_command,'publish-original-medical':run_original_medical_publication_command,'replay-original-medical':run_original_medical_replay_command,'read-original-medical':run_original_medical_read_command,'recover-original-medical':run_original_medical_recovery_command}[args.command]
            answer=entry(Path(args.configuration))
            print(json.dumps(answer,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False))
        except Exception as error:
            print('ashlar original medical operation refused',file=sys.stderr)
            metadata=medical_failure_metadata(error)
            if metadata is not None:print(json.dumps(metadata,sort_keys=True,separators=(',',':')),file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'prepare-truss-catalog':
        from pathlib import Path
        try:
            from ashlar_host.truss_preparation import run_truss_preparation_command
            answer = run_truss_preparation_command(Path(args.config))
            payload = (json.dumps(answer, sort_keys=True, separators=(',', ':'),
                                  ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
            written = sys.stdout.buffer.write(payload)
            if type(written) is not int or written != len(payload):
                raise OSError('Incomplete preparation summary output')
            sys.stdout.buffer.flush()
        except Exception:
            print('ashlar Truss preparation command failed', file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command in ('initialize-original-commerce','publish-original-commerce'):
        from ashlar_host.original_commerce_local_cli import run_original_commerce_local_command
        try:
            answer=run_original_commerce_local_command(args.configuration,initialize=args.command=='initialize-original-commerce',
                provider=getattr(args,'provider',None),provider_sha256=getattr(args,'provider_sha256',None))
            print(json.dumps(answer,sort_keys=True,separators=(',',':')))
        except Exception:
            print('ashlar original local operation refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'configure-supply-chain-native':
        from ashlar_host.supply_chain_configuration import make_supply_chain_native_configuration
        try:
            raw=make_supply_chain_native_configuration(**{name:getattr(args,name)for name in ('output','jars','pack','model','graph','producer_source','bun','git','count_star_index','count_star_installation','paths_index','paths_installation')})
        except ValueError as error:
            parser.error(str(error))
        sys.stdout.write(raw.decode('utf-8'))
        return
    if args.command == 'configure-supply-chain-common':
        try:
            from ashlar_host.supply_chain_common_setup import run_configure_supply_chain_common_command
            print(json.dumps(run_configure_supply_chain_common_command(args.configuration,args.python,args.java_home,args.output),separators=(',',':')))
        except Exception:
            print('ashlar supply-chain common configuration refused; inspect original selections and fresh output',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'query-supply-chain-common':
        try:
            from ashlar_host.supply_chain_common_cli import run_supply_chain_common_command
            result=run_supply_chain_common_command(args.configuration,args.output,**({'selected_case_ids':tuple(args.cases)}if args.cases else {}))
            print(json.dumps(result,separators=(',',':')))
        except Exception:
            print('ashlar original supply-chain common query refused; retain partial private output and inspect original inputs',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'run-supply-chain-native':
        from ashlar_host.supply_chain_native_cli import run_supply_chain_native_command
        try:
            answer=run_supply_chain_native_command(args.configuration)
            print(json.dumps(answer,sort_keys=True,separators=(',',':')))
        except Exception:
            print('ashlar original supply-chain native operation refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'materialize-graph-export':
        from ashlar_host.graph_export_materialization_cli import run_materialize_graph_export_command
        try:
            answer=run_materialize_graph_export_command(args.release,args.custody,args.release_sha256,args.custody_sha256,args.node_path,args.edge_path,args.jars,args.output)
            print(json.dumps(answer,separators=(',',':')))
        except Exception:
            print('ashlar graph export materialization refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'export-original-finite-pack':
        from ashlar_host.finite_export_cli import run_export_original_finite_pack_command
        try:
            answer=run_export_original_finite_pack_command(args.configuration,args.output)
            print(json.dumps(answer,separators=(',',':')))
        except Exception:
            print('ashlar original finite pack export refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'export-original-commerce':
        from ashlar_host.commerce_export_cli import run_export_original_commerce_command
        try:
            answer=run_export_original_commerce_command(args.configuration,args.publication,args.output,args.jars)
            print(json.dumps(answer,separators=(',',':')))
        except Exception:
            print('ashlar commerce graph export refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command=='evolve-truss-graph':
        try:
            from ashlar_host.truss_evolve_graph import run_evolve_truss_graph_command
            result=run_evolve_truss_graph_command(args.configuration,args.reader_configuration,args.output)
            print(json.dumps({'output':str(args.output)+'/result.json','publication_id':result['publication_id']},separators=(',',':')))
        except Exception:
            print('ashlar Truss graph evolution refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command in ('mutate-truss-note','evolve-truss-note','evolve-truss-record','create-truss-record','update-truss-record','delete-truss-record','mutate-truss-graph','change-truss-graph','accept-truss-schema'):
        try:
            from pathlib import Path
            if args.command=='mutate-truss-note':
                from ashlar_host.truss_note_mutation import run_mutate_truss_note_command as run
            elif args.command=='evolve-truss-note':
                from ashlar_host.truss_note_evolution import run_evolve_truss_note_command as run
            elif args.command=='create-truss-record':
                from ashlar_host.truss_record_mutation import run_create_truss_record_command as run
            elif args.command=='update-truss-record':
                from ashlar_host.truss_record_update import run_update_truss_record_command as run
            elif args.command=='delete-truss-record':
                from ashlar_host.truss_record_delete import run_delete_truss_record_command as run
            elif args.command=='evolve-truss-record':
                from ashlar_host.truss_schema_evolution import run_evolve_truss_record_command as run
            elif args.command=='mutate-truss-graph':
                from ashlar_host.truss_graph_mutation import run_mutate_truss_graph_command as run
            elif args.command=='change-truss-graph':
                from ashlar_host.truss_graph_change import run_change_truss_graph_command as run
            else:
                from ashlar_host.truss_schema_acceptance import run_accept_truss_schema_command as run
            run(args.configuration,args.output)
            print(json.dumps({'output':str(Path(args.output))},separators=(',',':')))
        except Exception:
            print('ashlar Truss operation refused; inspect the retained report and source outcome',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'serve-truss-local':
        try:
            from ashlar_host.truss_local_server import run_serve_truss_local_command
            run_serve_truss_local_command(args.data_directory,**({'connection_output':args.connection_output}if args.connection_output else {}))
        except Exception:
            print('ashlar local Truss server refused; inspect the selected initialized directory and runtime',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'configure-truss-continuation':
        try:
            from ashlar_host.truss_continuation_setup import run_configure_truss_continuation_command
            result=run_configure_truss_continuation_command(args.configuration,args.continuation_result,args.output)
            print(json.dumps({'output':result['output']},separators=(',',':')))
        except Exception:
            print('ashlar continuation configuration refused; inspect original result and fresh output selection',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'configure-truss-example-reader':
        try:
            from ashlar_host.truss_example_reader import run_configure_truss_example_reader_command
            result=run_configure_truss_example_reader_command(args.example_directory,args.connection_configuration,args.output,reader_configuration=args.reader_configuration)
            print(json.dumps({'output':result['output']},separators=(',',':')))
        except Exception:
            print('ashlar example reader configuration refused; inspect original private inputs and fresh output selection',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'run-truss-record-example':
        try:
            from ashlar_host.truss_record_example import run_truss_record_example_command
            result=run_truss_record_example_command(args.configuration,args.output)
            print(json.dumps({'output':str(args.output)+'/result.json','publication_id':result['publication_id']},separators=(',',':')))
        except Exception:
            print('ashlar typed Truss example refused; retain partial output and inspect source state before retrying',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'run-truss-example':
        try:
            from pathlib import Path
            from ashlar_host.truss_example import run_truss_example_command
            run_truss_example_command(args.configuration,args.output)
            print(json.dumps({'output':str(Path(args.output)/'result.json')},separators=(',',':')))
        except Exception:
            print('ashlar Truss example refused; retain partial private outputs and inspect the selected runtimes',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'setup-truss-local':
        try:
            from pathlib import Path
            from ashlar_host.truss_local_setup import run_setup_truss_local_command
            run_setup_truss_local_command(args.configuration,args.output)
            print(json.dumps({'output':str(Path(args.output))},separators=(',',':')))
        except Exception:
            print('ashlar local Truss setup refused; retain any partial data and inspect the selected local runtime',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'configure-truss-resume':
        try:
            from ashlar_host.truss_resume_setup import run_configure_truss_resume_command
            run_configure_truss_resume_command(args.configuration,args.publication_intent,args.output)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar Truss resume setup refused; inspect the original configuration and saved intent',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'publish-saved-truss-graph':
        try:
            from ashlar_host.truss_saved_cli import run_publish_saved_truss_graph_command
            run_publish_saved_truss_graph_command(args.configuration,args.publication_intent,args.publication_intent_sha256,args.output)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar saved Truss publication refused; retain the original selection and use resume for retained attempts',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'update-delete-truss-target':
        try:
            from ashlar_host.truss_graph_lifecycle import run_update_delete_truss_target_command
            run_update_delete_truss_target_command(args.configuration,args.output)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar selected Truss target lifecycle refused; inspect source state and retain original request keys',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'continue-truss-graph':
        try:
            from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
            result=run_continue_truss_graph_command(args.configuration,args.output,
                maximum_pages=args.maximum_pages,wait_seconds=args.wait_seconds,maximum_page_bytes=args.maximum_page_bytes)
            print(json.dumps({'output':args.output,'publications':len(result['publications'])},separators=(',',':')))
        except Exception:
            print('ashlar Truss continuation refused; inspect the requested output if written, retain the original journal and resume any pending intent',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'export-truss-graph':
        try:
            from ashlar_host.truss_graph_export_cli import run_export_truss_graph_command
            run_export_truss_graph_command(args.configuration,args.output)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar Truss graph export refused; retain any partial output and inspect the selected publication',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command=='read-truss-record':
        try:
            from ashlar_host.truss_record_read import run_read_truss_record_command
            run_read_truss_record_command(args.configuration,args.output,type_id=args.type_id,entity_id=args.entity_id)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar Truss record read refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'read-truss-singleton':
        try:
            from ashlar_host.truss_singleton import run_read_truss_singleton_command
            run_read_truss_singleton_command(args.configuration,args.output,kind=args.kind,
                type_id=args.type_id,entity_id=args.entity_id)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar Truss singleton refused; inspect the selected publication and typed identity',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'query-truss-graph-batch':
        try:
            from ashlar_host.truss_query_batch import run_query_truss_graph_batch_command
            result=run_query_truss_graph_batch_command(args.configuration,args.output,index=args.weft_index,installation=args.weft_installation,batch=args.batch)
            print(json.dumps({'output':args.output,'publication_id':result['publication_id'],'queries':len(result['queries'])},separators=(',',':')))
        except Exception:
            print('ashlar Truss query batch refused; retain original inputs and inspect the selected publication',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'query-truss-compatible':
        try:
            from ashlar_host.truss_compatible_query_cli import run_query_truss_compatible_command
            run_query_truss_compatible_command(args.configuration,args.output,compiler=args.compiler,
                sql=args.sql,revision=args.revision,document_id=args.document_id,
                physical_revisions=tuple(args.physical_revision),maximum_rows=args.maximum_rows)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar compatible Truss query refused; inspect the original publication and selected local compiler',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'query-truss-scalars':
        try:
            from ashlar_host.truss_scalar_query_cli import run_query_truss_scalars_command
            run_query_truss_scalars_command(args.configuration,args.output,compiler=args.compiler,
                sql=args.sql,revision=args.revision,document_id=args.document_id,
                physical_revisions=tuple(args.physical_revision),maximum_rows=args.maximum_rows)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar compatible Truss query refused; inspect the original publication and selected local compiler',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'query-truss-scalar-join':
        try:
            from ashlar_host.truss_scalar_join_query_cli import run_query_truss_scalar_join_command
            run_query_truss_scalar_join_command(args.configuration,args.output,compiler=args.compiler,
                sql=args.sql,revision=args.revision,document_id=args.document_id,
                physical_revisions=tuple(args.physical_revision),maximum_rows=args.maximum_rows)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar compatible Truss query refused; inspect the original publication and selected local compiler',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'query-truss-graph':
        try:
            from ashlar_host.truss_query_cli import run_query_truss_graph_command
            run_query_truss_graph_command(args.configuration,args.output,index=args.weft_index,
                installation=args.weft_installation,sql=args.sql,revision=args.revision,
                document_id=args.document_id,maximum_rows=args.maximum_rows)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar Truss query refused; inspect the original publication and selected compiler',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'resume-truss-graph':
        try:
            from ashlar_host.truss_resume_cli import run_resume_truss_graph_command
            run_resume_truss_graph_command(args.configuration,args.output)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar Truss resume refused; retain the original journal and inspect the source checkpoint',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command in ('publish-truss-note','publish-truss-graph'):
        try:
            from pathlib import Path
            from ashlar_host.truss_note_cli import run_publish_truss_note_command,run_publish_truss_graph_command
            run=run_publish_truss_graph_command if args.command=='publish-truss-graph' else run_publish_truss_note_command
            answer=run(args.configuration,args.output)
            print(json.dumps({'output':str(Path(args.output)/'result.json'),'publications':len(answer['publications'])},separators=(',',':')))
        except Exception:
            print('ashlar Truss publication refused; inspect the retained destination and source checkpoint',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'prepare-fabric-notebook':
        from pathlib import Path
        from ashlar_host.local_graph_export import LocalGraphExportSelection
        from ashlar_host.fabric_notebook import prepare_fabric_notebook
        try:
            output=prepare_fabric_notebook(LocalGraphExportSelection(Path(args.release),Path(args.custody),args.release_sha256,args.custody_sha256),args.nodes,args.edges,args.output,args.display_name)
            print(json.dumps({'notebook':str(output),'scope':'Private local notebook only; no cloud operation or graph authority.'}))
        except (ValueError,OSError)as error:parser.error(str(error))
        return
    if args.command == 'prepare-fabric-graph-definition':
        from ashlar_host.fabric_graph_definition import prepare_fabric_graph_definition
        try:
            answer=prepare_fabric_graph_definition(args.nodes,args.edges,args.output,**({'display_name':args.display_name}if args.display_name is not None else {}))
            print(json.dumps(answer,separators=(',',':')))
        except (ValueError,OSError) as error:
            parser.error(str(error))
        return
    if args.command == 'query-fabric-gql':
        from ashlar_host.fabric_gql_cli import run_fabric_gql_command
        try:
            answer=run_fabric_gql_command(args.configuration,args.provider,args.provider_sha256)
            print(json.dumps(answer,ensure_ascii=False,separators=(',',':'),allow_nan=False))
        except Exception:
            print('ashlar Fabric GQL operation refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'configure-graph-export':
        try:
            from ashlar_host.graph_export_setup import run_configure_graph_export_command
            answer=run_configure_graph_export_command(args.export_report,args.export_report_sha256,args.output)
            print(json.dumps(answer,separators=(',',':')))
        except Exception:
            print('ashlar graph export setup refused; inspect the selected export result and digests',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'configure-graphframes-export':
        try:
            from ashlar_host.graphframes_export_setup import run_configure_graphframes_export_command
            answer=run_configure_graphframes_export_command(args.export_report,args.export_report_sha256,args.output,jars=args.jars,java_home=args.java_home)
            print(json.dumps(answer,separators=(',',':')))
        except Exception:
            print('ashlar GraphFrames setup refused; inspect the selected export report and runtime paths',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'run-graphframes-export':
        try:
            from ashlar_host.graphframes_export import run_graphframes_export_command
            run_graphframes_export_command(args.configuration,args.output,maximum_hops=args.maximum_hops)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar GraphFrames export query refused; inspect the selected export and runtime',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'query-duckdb-export':
        try:
            from ashlar_host.duckdb_graph_export import run_query_duckdb_export_command
            run_query_duckdb_export_command(args.configuration,args.output,args.maximum_hops)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar DuckDB export query refused; inspect the selected export and DuckDB dependency',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'compare-graph-exports':
        try:
            from ashlar_host.graph_export_diff import run_compare_graph_exports_command
            run_compare_graph_exports_command(args.first_configuration,args.second_configuration,args.output)
            print(json.dumps({'output':args.output},separators=(',',':')))
        except Exception:
            print('ashlar graph export comparison refused; inspect the selected immutable exports',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'query-graphframes':
        from ashlar_host.graphframes_cli import run_graphframes_command
        try:
            answer = run_graphframes_command(args.configuration, args.provider, args.provider_sha256,**({'selected_case_ids':tuple(args.cases)}if args.cases else {}))
            print(json.dumps(answer, sort_keys=True, separators=(',', ':')))
        except Exception:
            print('ashlar selected GraphFrames release refused', file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command in ('publish-finite-pack','query-finite-pack'):
        from ashlar_host.pack_native_cli import run_finite_pack_command
        try:
            mode='publish'if args.command=='publish-finite-pack'else'query'
            selected_cases=tuple(args.case)if mode=='query'and args.case else None
            answer=run_finite_pack_command(args.configuration,mode,**({'case_ids':selected_cases}if selected_cases is not None else {}))
            print(json.dumps(answer,sort_keys=True,separators=(',',':')))
        except Exception:
            print('ashlar original finite pack operation refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command == 'prepare-puppy-release':
        from ashlar_host.puppy_carrier_cli import run_puppy_preparation
        try:answer=run_puppy_preparation(args.configuration,args.provider,args.provider_sha256)
        except Exception:
            parser.exit(1,'ashlar original PuppyGraph carrier preparation refused\n')
        print(json.dumps(answer,sort_keys=True));return
    if args.command in ('activate-puppy-release','read-puppy-release','resume-puppy-release'):
        from ashlar_host.puppy_release_cli import run_puppy_command, run_puppy_recovery
        try:
            if args.command == 'resume-puppy-release':
                answer = run_puppy_recovery(args.configuration,args.provider,args.provider_sha256,
                    args.original_intent,args.original_intent_sha256,args.attempt_directory)
            else:
                answer = run_puppy_command(args.configuration,args.provider,args.provider_sha256,
                    'activate' if args.command == 'activate-puppy-release' else 'read')
            print(json.dumps(answer,sort_keys=True,separators=(',',':')))
        except Exception:
            print('ashlar selected PuppyGraph release refused',file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command == 'compile-supply-chain-count-star':
        from ashlar_host.supply_chain_cli import compile_supply_chain_command
        try:
            options={}if args.case_id is None else {'selected_case_ids':args.case_id}
            answer=compile_supply_chain_command(**{name:getattr(args,name)for name in ('pack','model','graph','metadata','index','installation','output','maximum_artifact_bytes')},**options)
            print(json.dumps(answer,sort_keys=True,separators=(',',':')))
        except Exception:
            print('ashlar supply-chain compiler refused',file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command in ('publish-commerce-evolution', 'resume-commerce-evolution', 'continue-commerce-evolution'):
        from ashlar_host.evolution_cli import run_evolution_command
        from ashlar_host.config import HostError
        try:
            receipt = run_evolution_command(args.configuration, args.provider, args.provider_sha256,
                {'publish-commerce-evolution': 'fresh', 'resume-commerce-evolution': 'resume',
                 'continue-commerce-evolution': 'continue'}[args.command])
            print(json.dumps(receipt, sort_keys=True, separators=(',', ':')))
        except Exception:
            print('ashlar commerce evolution refused', file=sys.stderr)
            raise SystemExit(2) from None
        return

    if args.command == 'verify-commerce-evolution-publication':
        from ashlar_host.evolution_verification_cli import run_verification_command
        try:
            answer = run_verification_command(args.configuration, args.provider, args.provider_sha256)
            print(json.dumps(answer, sort_keys=True, separators=(',', ':')))
        except Exception:
            print('ashlar evolution publication verification refused', file=sys.stderr)
            raise SystemExit(2) from None
        return

    if args.command == 'diagnostics':
        from pathlib import Path
        from ashlar_host.diagnostics import read_diagnostics, DiagnosticsError
        try:
            result = read_diagnostics(Path(args.run_directory), args.attempt_id,
                args.min_severity, args.event_name, args.limit)
            payload = (json.dumps(result, ensure_ascii=True, separators=(',', ':'),
                                  allow_nan=False) + '\n').encode('utf8')
            if len(payload) > 524288:
                raise DiagnosticsError('diagnostics-snapshot-invalid')
            written = sys.stdout.buffer.write(payload)
            if type(written) is not int or written != len(payload):
                raise DiagnosticsError('diagnostics-output-refused')
            sys.stdout.buffer.flush()
        except (DiagnosticsError, OSError, ValueError):
            print('ashlar diagnostics refused', file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command in ('publish-commerce', 'query-commerce', 'query-commerce-paths', 'query-commerce-count-star', 'query-commerce-implicit-arithmetic'):
        from pathlib import Path
        from ashlar_host.config import (HostError, ProducerConfig, PrivatePostgresConfig,
                                        PublishCommerceConfig, QueryCommerceConfig)
        try:
            producer = ProducerConfig(Path(args.umf_source), Path(args.bun), Path(args.git),
                                      args.producer_timeout_seconds,
                                      args.producer_maximum_output_bytes,
                                      args.producer_maximum_receipt_bytes)
            endpoint_type=PrivatePostgresConfig
            if args.command in ('query-commerce-count-star','query-commerce-implicit-arithmetic') and args.postgres_profile=='explicit-loopback':
                from ashlar_host.implicit_arithmetic_configuration import ExplicitCommercePostgresEndpoint
                endpoint_type=ExplicitCommercePostgresEndpoint
            postgres = endpoint_type(args.postgres_container, args.postgres_host,
                                     args.postgres_port, args.postgres_database)
            if args.command == 'publish-commerce':
                config = PublishCommerceConfig(Path(args.output), Path(args.jars),
                    Path(args.model), Path(args.graph), producer, postgres,
                    args.source_system, args.binding_profile)
                from ashlar_host.commerce import publish_commerce
                publish_commerce(config)
                print("ashlar-host: report " + str(config.output / "report.json"))
            elif args.command == 'query-commerce':
                config = QueryCommerceConfig(Path(args.index), Path(args.installation),
                    Path(args.publication), Path(args.output), Path(args.jars),
                    Path(args.model), Path(args.graph), producer, postgres)
                from ashlar_host.commerce import query_commerce
                query_commerce(config)
                print("ashlar-host: report " + str(config.output / "report.json"))
            else:
                from ashlar_host.config import QueryCommercePathsConfig
                from ashlar_host.path_capture import PathCaptureConfig, PathCaptureError
                from .weft_path_decode import PathDecodeConfig, PathDecodeError
                try:
                    capture = PathCaptureConfig(args.maximum_rows,
                        args.maximum_cell_bytes, args.maximum_total_cell_bytes)
                    decoder = PathDecodeConfig(args.maximum_cell_bytes)
                except (PathCaptureError, PathDecodeError):
                    raise HostError('invalid-finite-bound') from None
                if args.command == 'query-commerce-implicit-arithmetic':
                    from ashlar_host.implicit_arithmetic_configuration import QueryCommerceImplicitArithmeticConfig
                    from ashlar_host.implicit_arithmetic_command import run_implicit_arithmetic_command
                    config = QueryCommerceImplicitArithmeticConfig(Path(args.index), Path(args.installation),
                        Path(args.publication), Path(args.output), Path(args.jars),
                        Path(args.model), Path(args.graph), producer, postgres,
                        args.maximum_artifact_bytes, capture, decoder)
                    def query_commerce_paths(config, *, diagnostics=None):
                        return run_implicit_arithmetic_command(config,
                            provider_path=Path(args.session_provider),
                            provider_sha256=args.session_provider_sha256,
                            pack_path=Path(args.pack), diagnostics=diagnostics, selected_case_ids=tuple(args.case_ids))
                elif args.command == 'query-commerce-count-star':
                    from ashlar_host.count_star_configuration import QueryCommerceCountStarConfig
                    from ashlar_host.count_star_query import query_commerce_count_star
                    supplied=(args.session_provider,args.session_provider_sha256,args.pack)
                    explicit=args.postgres_profile=='explicit-loopback'
                    if (explicit and not all(supplied))or(any(supplied)and not explicit):
                        raise HostError('explicit-count-query-provider-required')
                    def query_commerce_paths(config, *, diagnostics=None):
                        if explicit:
                            from ashlar_host.count_star_command import run_count_star_command
                            return run_count_star_command(config,provider_path=Path(args.session_provider),
                                provider_sha256=args.session_provider_sha256,pack_path=Path(args.pack),
                                diagnostics=diagnostics,selected_case_ids=tuple(args.case_ids))
                        return query_commerce_count_star(config,diagnostics=diagnostics,selected_case_ids=tuple(args.case_ids))
                    config = QueryCommerceCountStarConfig(Path(args.index), Path(args.installation),
                        Path(args.publication), Path(args.output), Path(args.jars),
                        Path(args.model), Path(args.graph), producer, postgres,
                        args.maximum_artifact_bytes, capture, decoder)
                else:
                    config = QueryCommercePathsConfig(Path(args.index), Path(args.installation),
                        Path(args.publication), Path(args.output), Path(args.jars),
                        Path(args.model), Path(args.graph), producer, postgres,
                        args.maximum_artifact_bytes, capture, decoder, args.profile)
                    from ashlar_host.paths_query import query_commerce_paths
                if args.diagnostics_config is None:
                    query_commerce_paths(config)
                else:
                    from ashlar_host.config import load_diagnostics_config
                    from ashlar_host.diagnostic_composition import diagnostic_run
                    selected = load_diagnostics_config(Path(args.diagnostics_config))
                    with diagnostic_run(selected) as run:
                        query_commerce_paths(config, diagnostics=run)
                print("ashlar-host: report " + str(config.output / "report.json"))
        except (HostError, OSError):
            print('ashlar-host: refused', file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command in ('install-weft-implicit-arithmetic','compile-weft-implicit-arithmetic'):
        from pathlib import Path
        from .weft_distribution import read_request,DistributionError
        from .weft_implicit_arithmetic_distribution import (ImplicitArithmeticDistributionPaths,ImplicitArithmeticDistributionError,
            install_implicit_arithmetic_distribution,compile_implicit_arithmetic_distribution)
        try:
            if args.command == 'install-weft-implicit-arithmetic':
                result=install_implicit_arithmetic_distribution(ImplicitArithmeticDistributionPaths(Path(args.index),Path(args.output),Path(args.package)))
                print('ashlar-weft-implicit-arithmetic: '+('cleanup-pending'if result.cleanup_pending else 'installed'),file=sys.stderr)
            else:
                request=read_request(sys.stdin.buffer)
                response=compile_implicit_arithmetic_distribution(ImplicitArithmeticDistributionPaths(Path(args.index),Path(args.installation)),request)
                written=sys.stdout.buffer.write(response)
                if type(written)is not int or written!=len(response):raise ImplicitArithmeticDistributionError('output-refused')
                sys.stdout.buffer.flush()
        except (ImplicitArithmeticDistributionError,DistributionError,OSError):
            print('ashlar-weft-implicit-arithmetic: refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command in ('install-weft-count-star','compile-weft-count-star'):
        from pathlib import Path
        from .weft_distribution import read_request,DistributionError
        from .weft_count_star_distribution import (CountStarDistributionPaths,CountStarDistributionError,
            install_count_star_distribution,compile_count_star_distribution)
        try:
            if args.command == 'install-weft-count-star':
                result=install_count_star_distribution(CountStarDistributionPaths(Path(args.index),Path(args.output),Path(args.package)))
                print('ashlar-weft-count-star: '+('cleanup-pending'if result.cleanup_pending else 'installed'),file=sys.stderr)
            else:
                request=read_request(sys.stdin.buffer)
                response=compile_count_star_distribution(CountStarDistributionPaths(Path(args.index),Path(args.installation)),request)
                written=sys.stdout.buffer.write(response)
                if type(written)is not int or written!=len(response):raise CountStarDistributionError('output-refused')
                sys.stdout.buffer.flush()
        except (CountStarDistributionError,DistributionError,OSError):
            print('ashlar-weft-count-star: refused',file=sys.stderr)
            raise SystemExit(2)from None
        return
    if args.command in ('install-weft-paths', 'compile-weft-paths'):
        from pathlib import Path
        from .weft_distribution import read_request, DistributionError
        if args.profile == 'paths':
            from .weft_paths_distribution import (PathsDistributionPaths, PathsDistributionError,
                install_paths_distribution, compile_paths_distribution)
        else:
            from .weft_paths_keys_distribution import (
                PathsKeysDistributionPaths as PathsDistributionPaths,
                PathsKeysDistributionError as PathsDistributionError,
                install_paths_keys_distribution as install_paths_distribution,
                compile_paths_keys_distribution as compile_paths_distribution)
        try:
            if args.command == 'install-weft-paths':
                result = install_paths_distribution(PathsDistributionPaths(
                    Path(args.index), Path(args.output), Path(args.package)))
                outcome = 'cleanup-pending' if result.cleanup_pending else 'installed'
                print('ashlar-weft-paths: ' + outcome, file=sys.stderr)
            else:
                request = read_request(sys.stdin.buffer)
                response = compile_paths_distribution(PathsDistributionPaths(
                    Path(args.index), Path(args.installation)), request)
                sys.stdout.buffer.write(response)
                sys.stdout.buffer.flush()
        except (PathsDistributionError, DistributionError) as error:
            print('ashlar-weft-paths: ' + str(error), file=sys.stderr)
            raise SystemExit(2) from None
        except OSError:
            print('ashlar-weft-paths: io-refused', file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command in ('install-weft', 'compile-weft'):
        from pathlib import Path
        from .weft_distribution import (DistributionPaths, DistributionError,
            install_distribution, compile_distribution, read_request, diagnostic)
        try:
            if args.command == 'install-weft':
                result = install_distribution(DistributionPaths(
                    Path(args.index), Path(args.output), Path(args.package)))
                print('ashlar-weft: ' + diagnostic(result), file=sys.stderr)
            else:
                request = read_request(sys.stdin.buffer)
                response = compile_distribution(DistributionPaths(
                    Path(args.index), Path(args.installation)), request)
                sys.stdout.buffer.write(response)
                sys.stdout.buffer.flush()
        except DistributionError as error:
            print('ashlar-weft: ' + str(error), file=sys.stderr)
            raise SystemExit(2) from None
        except OSError:
            print('ashlar-weft: io-refused', file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command=='medical-source':
        from .medical_source import write_candidate
        batch,_=write_candidate(args.ontology,args.graph,args.public_admission,args.output,source_system=args.source_system,binding_profile=args.binding_profile)
        print('Created candidate historical medical transaction: '+str(len(batch.records))+' events; development bindings only; native host must recompute public admission')
        return
    if args.command in ('archaeology-source','ecology-source'):
        if args.command=='archaeology-source':
            from .archaeology_source import write_candidate
        else:
            from .ecology_source import write_candidate
        batch,_=write_candidate(args.ontology,args.graph,args.output,source_system=args.source_system)
        print('Created candidate '+args.command[:-7]+' transaction: '+str(len(batch.records))+' events; development bindings only')
        return
    if args.command=='commerce-source':
        from .commerce_source import write_candidate
        write_candidate(args.ontology,args.graph,args.output,source_system=args.source_system,binding_profile=args.binding_profile)
        print('Created candidate commerce transaction: 11 objects, 10 edges; development bindings only')
        return
    if args.command=='inspect-configured-source':
        from .source_config import load_jsonl_configuration,inspect_configured_source
        print(json.dumps(inspect_configured_source(load_jsonl_configuration(args.configuration)),separators=(',',':')))
        return
    if args.command == 'inspect-csv':
        from .csv_source import csv_batches,validate_csv_batch
        from .schema import _json
        from .staging import batch_row
        from .source_checkpoint import csv_checkpoint
        mapping=_json(args.properties_json.encode('utf-8'))
        lines=iter(lambda:sys.stdin.buffer.readline(65537),b'')
        for batch in csv_batches(lines,feed=args.feed,epoch=args.epoch,
                source_system=args.source_system,schema_revision=args.schema_revision,
                type_id=args.type_id,properties=mapping):
            validate_csv_batch(batch,feed=args.feed,epoch=args.epoch,source_system=args.source_system,
                schema_revision=args.schema_revision,type_id=args.type_id,properties=mapping)
            print(json.dumps({'format':'ashlar-csv-source-custody/0.1',
                'batch_row':batch_row(batch),'source_checkpoint_json':csv_checkpoint(batch)},
                separators=(',',':')),flush=True)
        return

    lines = iter(lambda: sys.stdin.buffer.readline(1024 * 1024 + 1), b'')
    for batch in jsonl_batches(lines, feed=args.feed, epoch=args.epoch,
                              cursor_before=args.cursor_before):
        value = asdict(batch)
        value['begin_base64'] = base64.b64encode(value.pop('begin')).decode('ascii')
        value['commit_base64'] = base64.b64encode(value.pop('commit')).decode('ascii')
        for record in value['records']:
            record['raw_base64'] = base64.b64encode(record.pop('raw')).decode('ascii')
        print(json.dumps(value, separators=(',', ':')), flush=True)
