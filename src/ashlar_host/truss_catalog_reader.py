"""Capture original public historical catalog bytes required by one native page."""
import json
from dataclasses import replace
from ashlar.staging import batch_row
from .lifecycle import finish


def read_historical_catalog_views(connection_factory,batch):
    batch_row(batch)
    if batch.profile!='truss-native-feed/0.1'or not callable(connection_factory):raise ValueError('Original native page and selected ordinary connection factory required')
    revisions=[]
    for prerequisite in json.loads(batch.begin)['catalogPrerequisites']:
        if prerequisite.get('reportBytesBase64')is not None:
            revision=prerequisite['revision']
            if type(revision)is not str or not revision.isascii()or not revision.isdecimal()or str(int(revision))!=revision or not 1<=int(revision)<=2147483647 or revision in revisions:raise ValueError('Distinct original accepted catalog revisions required')
            revisions.append(revision)
    if not revisions:return {}
    from truss import (Ok,CatalogRuntimeConfiguration,create_reference_assembly,
        adopt_psycopg_host,HistoricalCatalogSelection,CatalogViewAvailable)
    def value(result):
        if not isinstance(result,Ok):raise ValueError('Public historical catalog read refused')
        return result.value
    captured={}
    with connection_factory()as connection:
        if connection.autocommit or connection.info.transaction_status!=0:raise ValueError('Fresh ordinary non-autocommit connection required')
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction(access_mode='read_only'))
            assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host))
            seed=assembly.catalog_view_request()
            for revision in revisions:
                request=replace(seed,selection=HistoricalCatalogSelection(catalog_revision=revision))
                available=value(assembly.catalog_view_in_transaction(tx,request))
                if not isinstance(available,CatalogViewAvailable)or type(available.view.original_bytes)is not bytes or str(json.loads(available.view.original_bytes)['catalogRevision'])!=revision:raise ValueError('Actual original historical catalog view required')
                captured[revision]=available.view.original_bytes
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise ValueError('Historical catalog host detach refused')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
        connection.rollback()
    return captured
