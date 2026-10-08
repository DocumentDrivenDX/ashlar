"""Bounded single-line UTF-8 CSV to original-custody whole-object transactions.

Caller supplies source/epoch custody and admitted IDs/schema mapping. Each CSV
record is an explicit singleton transaction, not an inferred database commit.
No acknowledgement, catalog acceptance or property type coercion occurs here.
"""
import base64,csv,json
from .source import SourceError,jsonl_batches,records_digest
from .whole_entity import _integer


def _csv_batches(lines, *, feed, epoch, source_system, schema_revision, type_id,
                properties, max_records=1000, max_line_bytes=65536, _first_ordinal=1):
    """Consume a complete header then one object operation per complete CSV line.

    Required control columns: id, entity_version, operation. Mapped values are
    strings, including empty strings. Unmapped columns remain in exact original
    header/row bytes within the original delivery identity. Embedded-newline CSV is unsupported.
    Original-custody delivery IDs and ordinal batch IDs are stable only within independently trusted immutable
    source epoch/file custody; changed input must not reuse that custody.
    """
    for value in (feed,epoch,source_system,schema_revision):
        if not isinstance(value,str) or not value or '\x00' in value:raise SourceError('Explicit CSV source/schema custody required')
    _integer(type_id)
    if not isinstance(properties,dict) or not properties:raise SourceError('Explicit string property mappings required')
    properties=dict(properties)
    controls={'id','entity_version','operation'}
    for column,ident in properties.items():
        if not isinstance(column,str) or not column or column in controls:raise SourceError('Invalid property column')
        _integer(ident)
    if len(set(properties.values()))!=len(properties):raise SourceError('Duplicate property identity mapping')
    if type(max_records) is not int or not 1<=max_records<=1000 or type(max_line_bytes) is not int or not 1<=max_line_bytes<=65536:raise SourceError('CSV profile bounds exceeded')
    def parse(raw):
        if not isinstance(raw,bytes) or not raw.endswith(b'\n') or b'\n' in raw[:-1] or len(raw)>max_line_bytes:raise SourceError('Bounded complete single-line CSV required')
        try:
            rows=list(csv.reader([raw.decode('utf-8')],strict=True))
        except (UnicodeError,csv.Error) as exc:raise SourceError('Invalid UTF-8 CSV') from exc
        if len(rows)!=1:return []
        return rows[0]
    iterator=iter(lines)
    try:header_raw=next(iterator)
    except StopIteration:raise SourceError('CSV header required')
    header=parse(header_raw)
    if not header or any(not name or '\x00' in name for name in header) or len(set(header))!=len(header) or not controls|set(properties)<=set(header):raise SourceError('Unique complete CSV header required')
    header64=base64.b64encode(header_raw).decode('ascii')
    def encoded(value):return (json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n').encode('utf-8')
    for ordinal,raw in enumerate(iterator,_first_ordinal):
        if ordinal>max_records:raise SourceError('CSV record bound exceeded')
        values=parse(raw)
        if len(values)!=len(header):raise SourceError('CSV row width differs from header')
        row=dict(zip(header,values));_integer(row['id']);version=_integer(row['entity_version'])
        if version<0 or row['operation'] not in ('create','replace','delete'):raise SourceError('Explicit supported CSV operation/version required')
        batch_id='csv-row-'+str(ordinal)
        custody={'profile':'ashlar-single-line-csv/0.1','header_base64':header64,'row_base64':base64.b64encode(raw).decode('ascii'),'row_ordinal':str(ordinal)}
        delivery=json.dumps(custody,separators=(',',':'))
        event={'kind':'event','source_profile':'ashlar-whole-entity/0.1','source_system':source_system,
               'schema_revision':schema_revision,'delivery_id':delivery,'entity_kind':'object',
               'type_id':type_id,'id':row['id'],'entity_version':row['entity_version'],'operation':row['operation'],
               'props_json':json.dumps({ident:row[column] for column,ident in properties.items()},ensure_ascii=False,separators=(',',':')),
               'retained_json':json.dumps({'source_profile':'ashlar-single-line-csv/0.1','unmapped_columns':{column:row[column] for column in header if column not in controls and column not in properties}},ensure_ascii=False,separators=(',',':'))}
        record=encoded(event)
        transaction=[encoded({'kind':'begin','batch_id':batch_id}),record,
                     encoded({'kind':'commit','batch_id':batch_id,'record_count':1,'records_sha256':records_digest([record])})]
        # Each independently emitted transaction has its own inner byte cursor.
        # Resume native CSV progress by admitted epoch plus retained row ordinal,
        # never by concatenating these offsets or manufacturing a source ACK.
        yield next(jsonl_batches(transaction,feed=feed,epoch=epoch))


def csv_batches(lines, *, feed, epoch, source_system, schema_revision, type_id,
                properties, max_records=1000, max_line_bytes=65536):
    """Adapt original CSV lines with explicit source identity and ordered mapping."""
    return _csv_batches(lines,feed=feed,epoch=epoch,source_system=source_system,
        schema_revision=schema_revision,type_id=type_id,properties=properties,
        max_records=max_records,max_line_bytes=max_line_bytes)


def validate_csv_batch(batch, *, feed, epoch, source_system, schema_revision,
                       type_id, properties):
    """Verify original CSV-to-event correspondence under independently admitted config.

    Retains exact producer bytes/order; it does not authorize the caller, admit
    catalog IDs/schema meanings or prove original file epoch authority. The host
    must supply the same ordered original mapping, not one derived from the event.
    """
    from .source_checkpoint import csv_checkpoint
    from .schema import _json
    csv_checkpoint(batch)  # Validate full original transaction and bounded custody.
    custody=_json(batch.records[0].delivery_id.encode('utf-8'))
    originals=[base64.b64decode(custody[key],validate=True) for key in ('header_base64','row_base64')]
    expected=next(_csv_batches(originals,feed=feed,epoch=epoch,source_system=source_system,
        schema_revision=schema_revision,type_id=type_id,properties=properties,
        _first_ordinal=int(custody['row_ordinal'])))
    if expected!=batch:raise SourceError('Adapted batch differs from original CSV and admitted mapping')
