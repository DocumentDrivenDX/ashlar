# SPDX-License-Identifier: MIT OR Apache-2.0
"""Installed ordinary configuration entrypoint for the local Note workflow."""
import json
from pathlib import Path

def run_publish_truss_note_command(configuration,output):
    return _run_publication(configuration,output,graph=False)

def run_publish_truss_graph_command(configuration,output):
    return _run_publication(configuration,output,graph=True)

def _run_publication(configuration,output,*,graph):
    with Path(configuration).open('rb')as stream:raw=stream.read(65537)
    if len(raw)>65536:raise ValueError('Configuration bound exceeded')
    config=json.loads(raw)
    required={'connection_uri','administrator_uri','reader_role','consumer_id','feed','jars','maximum_pages'}
    if type(config)is not dict or not required<=set(config)<=required|{'wait_seconds','archived_acceptance_files','export_graph','destination_id','maximum_page_bytes'}:raise ValueError('Exact native configuration with optional wait_seconds required')
    page_bytes=config.get('maximum_page_bytes',1048576)
    if type(page_bytes)is not int or not 1<=page_bytes<=2097152:raise ValueError('Page byte budget must be 1..2097152')
    from .truss_feed_sessions import receipt_destination_id,require_truss_receipt_sdk
    destination_id=receipt_destination_id(config.get('destination_id'))
    if 'destination_id'in config and destination_id is None:raise ValueError('Explicit destination identity required')
    if destination_id is not None:require_truss_receipt_sdk()
    from .truss_catalog_reader import parse_archived_acceptance_files
    archives=parse_archived_acceptance_files(config.get('archived_acceptance_files',[]))
    if archives and not graph:raise ValueError('Archived catalogs apply to graph publication only')
    export_graph=config.get('export_graph',False)
    if type(export_graph)is not bool or export_graph and not graph:raise ValueError('Graph export requires graph publication and a boolean selection')
    wait=config.get('wait_seconds',0)
    if type(wait)is not int or not 0<=wait<=60:raise ValueError('Wait budget must be 0..60 seconds')
    if type(config['maximum_pages'])is not int or not 1<=config['maximum_pages']<=8 or any(type(config[key])is not str or not config[key]or '\0'in config[key]for key in ('connection_uri','administrator_uri','reader_role','consumer_id','feed')):raise ValueError('Explicit bounded ordinary source selections required')
    if type(config['jars'])is not list or len(config['jars'])!=2:raise ValueError('Select existing Delta Spark and storage JARs')
    jars=tuple(Path(value)for value in config['jars'])
    if any(not path.is_absolute()or path.resolve()!=path or not path.is_file()for path in jars)or len(set(jars))!=2:raise ValueError('Two distinct canonical installed JARs required')
    destination=Path(output)
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or destination.exists()or any(character in str(destination)for character in ('`',"'",'\\','\0','\n','\r')):raise ValueError('Fresh canonical private output required')
    if any(destination==path or destination in path.parents or path in destination.parents for path in (*jars,Path(configuration).resolve(),*(path for path,digest in archives))):raise ValueError('Separate fresh output required')
    warehouse=destination.parent/(destination.name+'-warehouse')
    if warehouse.exists():raise ValueError('Fresh separately owned Spark warehouse required')
    from .truss_runtime import require_truss_psycopg_runtime
    require_truss_psycopg_runtime()
    import psycopg
    from .commerce_export_cli import open_commerce_export_spark
    from .truss_note_publication import publish_note_pages,publish_graph_pages,register_note_consumer
    def connection():return psycopg.connect(config['connection_uri'],autocommit=False,connect_timeout=5)
    def administrator():return psycopg.connect(config['administrator_uri'],autocommit=False,connect_timeout=5)
    register_note_consumer(administrator,config['consumer_id'],config['reader_role'])
    # Existing SDK/JDK/JARs only; local[1], 512 MiB, no package installation.
    with open_commerce_export_spark(jars,warehouse)as spark:
        publish=publish_graph_pages if graph else publish_note_pages
        return publish(spark,destination,connection_factory=connection,consumer_id=config['consumer_id'],feed=config['feed'],maximum_pages=config['maximum_pages'],maximum_page_bytes=page_bytes,wait_seconds=wait,**({'destination_id':destination_id}if destination_id is not None else {}),**({'archived_acceptance_files':archives,'export_graph':export_graph}if graph and (archives or export_graph)else {}))

