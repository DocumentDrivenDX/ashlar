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
    install_count_star = commands.add_parser('install-weft-count-star', help='Verify and install the indexed COUNT star candidate')
    for name in ('index', 'package', 'output'):
        install_count_star.add_argument('--' + name, required=True)
    compile_count_star = commands.add_parser('compile-weft-count-star', help='Compile original stdin through the selected COUNT star installation')
    for name in ('index', 'installation'):
        compile_count_star.add_argument('--' + name, required=True)
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
    for name in ('publish-commerce', 'query-commerce', 'query-commerce-paths', 'query-commerce-count-star'):
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
        if name in ('query-commerce-paths','query-commerce-count-star'):
            command.add_argument('--diagnostics-config')
            if name == 'query-commerce-paths':
                command.add_argument('--profile', default='paths', choices=('paths', 'paths-keys'))
            for option in ('maximum-artifact-bytes', 'maximum-rows',
                           'maximum-cell-bytes', 'maximum-total-cell-bytes'):
                command.add_argument('--' + option, type=int, required=True)
    args = parser.parse_args()
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
    if args.command in ('publish-commerce', 'query-commerce', 'query-commerce-paths', 'query-commerce-count-star'):
        from pathlib import Path
        from ashlar_host.config import (HostError, ProducerConfig, PrivatePostgresConfig,
                                        PublishCommerceConfig, QueryCommerceConfig)
        try:
            producer = ProducerConfig(Path(args.umf_source), Path(args.bun), Path(args.git),
                                      args.producer_timeout_seconds,
                                      args.producer_maximum_output_bytes,
                                      args.producer_maximum_receipt_bytes)
            postgres = PrivatePostgresConfig(args.postgres_container, args.postgres_host,
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
                if args.command == 'query-commerce-count-star':
                    from ashlar_host.count_star_configuration import QueryCommerceCountStarConfig
                    from ashlar_host.count_star_query import query_commerce_count_star as query_commerce_paths
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
