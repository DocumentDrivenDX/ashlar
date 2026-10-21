# SPDX-License-Identifier: MIT OR Apache-2.0
"""Capture original public historical catalog bytes required by one native page."""
import base64,hashlib,json,re
from pathlib import Path
from dataclasses import asdict,replace
from ashlar.staging import batch_row
from .lifecycle import finish
from .evolution_producer import snapshot
from .truss_current_catalog import (CURRENT_REPORT_VERSION,
    CURRENT_TRUSS_VERSION, CurrentCatalogEvidence, parse_current_catalog_json, resolve_current_accepted_sources,
    interpret_current_catalog)



def parse_archived_acceptance_files(value):
    """Select bounded original acceptance archives and explicit expected digests."""
    if type(value)is not list or len(value)>16:raise ValueError('At most sixteen explicit acceptance archive selections required')
    selected=[]
    for item in value:
        if type(item)is not dict or set(item)!={'path','sha256'}or type(item['path'])is not str or type(item['sha256'])is not str or re.fullmatch('[0-9a-f]{64}',item['sha256'])is None:raise ValueError('Exact archive path and expected SHA256 required')
        path=Path(item['path'])
        if not path.is_absolute()or path.resolve()!=path or not path.is_file()or any(path==prior[0]for prior in selected):raise ValueError('Distinct canonical existing acceptance archive required')
        if hashlib.sha256(snapshot(path,16777216)).hexdigest()!=item['sha256']:raise ValueError('Original acceptance archive digest differs')
        selected.append((path,item['sha256']))
    return tuple(selected)


def _archived_pairs(selected):
    archives=[]
    for path,digest in selected:
        raw=snapshot(path,16777216)
        if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('Selected acceptance archive changed')
        value=json.loads(raw)
        for prefix in ('','previous_'):
            report=value.get(prefix+'report_original_base64');catalog=value.get(prefix+'catalog_original_base64')
            if report is None and catalog is None:continue
            if type(report)is not str or type(catalog)is not str:raise ValueError('Complete original report/catalog archive pair required')
            archives.append((base64.b64decode(report,validate=True),base64.b64decode(catalog,validate=True)))
    return archives


def read_historical_catalog_views(connection_factory,batch,archived_acceptance_files=()):
    batch_row(batch)
    if batch.profile!='truss-native-feed/0.1'or not callable(connection_factory):raise ValueError('Original native page and selected ordinary connection factory required')
    revisions=[];reports={}
    archives=_archived_pairs(archived_acceptance_files)
    for prerequisite in json.loads(batch.begin)['catalogPrerequisites']:
        if prerequisite.get('reportBytesBase64')is not None:
            revision=prerequisite['revision']
            if type(revision)is not str or not revision.isascii()or not revision.isdecimal()or str(int(revision))!=revision or not 1<=int(revision)<=2147483647 or revision in revisions:raise ValueError('Distinct original accepted catalog revisions required')
            revisions.append(revision);reports[revision]=base64.b64decode(prerequisite['reportBytesBase64'],validate=True)if archives else None
    if not revisions:return {}
    from truss import (Ok,CatalogRuntimeConfiguration,create_reference_assembly,
        adopt_psycopg_host,HistoricalCatalogSelection,CatalogViewAvailable,CatalogViewUnavailable)
    def value(result):
        if not isinstance(result,Ok):raise ValueError('Public historical catalog read refused')
        return result.value
    captured={}
    with connection_factory()as connection:
        if connection.autocommit or connection.info.transaction_status!=0:raise ValueError('Fresh ordinary non-autocommit connection required')
        # The public catalog extractor takes a shared schema lock and refuses READ ONLY.
        # Capture in an ordinary transaction, then roll it back without committing.
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction())
            assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host))
            seed=assembly.catalog_view_request()
            for revision in revisions:
                matches=[catalog for report,catalog in archives if report==reports[revision]]
                if matches:
                    if any(raw!=matches[0]for raw in matches):raise ValueError('Conflicting exact-report archived catalogs')
                    original=matches[0];catalog=json.loads(original);report=json.loads(reports[revision])
                    if str(catalog['catalogRevision'])!=revision or str(report['revision'])!=revision or report['installationId']!=json.loads(batch.begin)['prior']['installationId']or catalog['layoutProfile']!=asdict(seed.layout_profile)or catalog['viewProfile']!=asdict(seed.view_profile)or catalog['layoutProfile']!=report['acceptedInput']['layoutProfile']:raise ValueError('Original archived report/catalog revision/profile differs')
                    captured[revision]=original;continue
                request=replace(seed,selection=HistoricalCatalogSelection(catalog_revision=revision))
                available=value(assembly.catalog_view_in_transaction(tx,request))
                if isinstance(available,CatalogViewUnavailable):
                    reason=available.reason if available.reason in ('profile','closure','context','resource')else 'unknown'
                    raise ValueError(f'Public historical catalog view unavailable: {reason}')
                if not isinstance(available,CatalogViewAvailable)or type(available.view.original_bytes)is not bytes or str(json.loads(available.view.original_bytes)['catalogRevision'])!=revision:raise ValueError('Actual original historical catalog view required')
                captured[revision]=available.view.original_bytes
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise ValueError('Historical catalog host detach refused')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
        connection.rollback()
    return captured


def read_current_catalog_evidence(connection_factory,batch):
    """Read Truss 0.44 reports, complete views and exact accepted source bytes.

    The 0.6 report carries source references, not source bytes. Every source is
    fetched through the public accepted-document operation in this same ordinary
    READ COMMITTED transaction and matched byte-for-byte to its report reference.
    """
    batch_row(batch)
    if batch.profile!='truss-native-feed/0.1'or not callable(connection_factory):
        raise ValueError('Original native page and ordinary connection factory required')
    import importlib.metadata
    try:version=importlib.metadata.version('truss-toolkit')
    except importlib.metadata.PackageNotFoundError:raise ValueError('Installed Truss toolkit version required')from None
    if version!=CURRENT_TRUSS_VERSION:raise ValueError('Truss 0.0.44 current catalog profile required')
    payload=parse_current_catalog_json(batch.begin)
    installation=parse_current_catalog_json(base64.b64decode(batch.cursor_before,validate=True)).get('installationId')
    prerequisites=[]
    for item in payload.get('catalogPrerequisites',[]):
        if item.get('reportBytesBase64')is None:continue
        revision=item.get('revision');raw=base64.b64decode(item['reportBytesBase64'],validate=True)
        if (type(revision)is not str or not revision.isascii()or not revision.isdecimal()
                or str(int(revision))!=revision or hashlib.sha256(raw).hexdigest()!=item.get('reportSha256')):
            raise ValueError('Exact current accepted report revision and digest required')
        report=parse_current_catalog_json(raw)
        if report.get('interfaceVersion')!=CURRENT_REPORT_VERSION:
            raise ValueError('Current Truss report 0.6 required for this reader')
        if report.get('revision')!=revision or report.get('installationId')!=installation:
            raise ValueError('Current report revision and installation differ from native page')
        prerequisites.append((revision,raw,report))
    if not prerequisites or len({revision for revision,_,_ in prerequisites})!=len(prerequisites):
        raise ValueError('Distinct current accepted catalog reports required')
    if len(prerequisites) != 1:
        raise ValueError('Current Truss reader requires one accepted revision; historical source/view custody is unsupported')
    from truss import (Ok,CatalogRuntimeConfiguration,create_reference_assembly,
        adopt_psycopg_host,CatalogViewAvailable,CatalogViewUnavailable)
    def value(result,label):
        if not isinstance(result,Ok):raise ValueError('Public current Truss '+label+' operation refused')
        return result.value
    reports={};views={};sources={}
    with connection_factory()as connection:
        if connection.autocommit or connection.info.transaction_status!=0:
            raise ValueError('Fresh ordinary non-autocommit connection required')
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction(),'transaction')
            assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host),'assembly')
            seed=assembly.catalog_view_request(definition_count='65536',encoded_bytes='16777216')
            for revision,raw,report in prerequisites:
                expected=report.get('acceptedInput',{}).get('documents')
                if type(expected)is not list or not expected:
                    raise ValueError('Current report accepted document references required')
                for reference in expected:
                    document_id=reference.get('documentId')if type(reference)is dict else None
                    accepted=value(assembly.accepted_document_in_transaction(tx,document_id=document_id,maximum_bytes=1048576),'accepted source')
                    if not hasattr(accepted,'content'):
                        raise ValueError('Public accepted source is not authorized or available')
                    sources[revision,document_id]=accepted
                resolve_current_accepted_sources(report,revision,sources)
                result=value(assembly.catalog_view_in_transaction(tx,seed),'catalog view')
                if isinstance(result,CatalogViewUnavailable):
                    reason=result.reason if result.reason in ('profile','closure','context','resource','definition')else'unknown'
                    raise ValueError('Public current catalog view unavailable: '+reason)
                if not isinstance(result,CatalogViewAvailable)or type(result.view.original_bytes)is not bytes:
                    raise ValueError('Public current complete catalog view required')
                view_raw=result.view.original_bytes
                interpret_current_catalog(revision,raw,view_raw,sources)
                reports[revision]=raw;views[revision]=view_raw
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise ValueError('Current catalog host detach refused')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
        connection.rollback()
    return CurrentCatalogEvidence(version,reports,views,sources)


def read_graph_catalog_evidence(connection_factory,batch,archives=()):
    """Select live public custody for report 0.6, historical archives otherwise."""
    batch_row(batch)
    payload=parse_current_catalog_json(batch.begin)
    current=False
    for item in payload.get('catalogPrerequisites',[]):
        if type(item)is not dict or item.get('reportBytesBase64')is None:continue
        raw=base64.b64decode(item['reportBytesBase64'],validate=True)
        if parse_current_catalog_json(raw).get('interfaceVersion')==CURRENT_REPORT_VERSION:
            current=True
    if current:
        if archives:raise ValueError('Current Truss report source custody comes from the live accepted-document API')
        return read_current_catalog_evidence(connection_factory,batch)
    return read_historical_catalog_views(connection_factory,batch,archives)
