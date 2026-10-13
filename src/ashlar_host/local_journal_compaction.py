"""Lossless receipt-only maintenance under the existing finite installation lease."""
import fcntl,hashlib,json,os,re,sqlite3,stat
from pathlib import Path
from urllib.parse import quote
from .delta_custody import LocalDeltaTransport,encoded,operation_capacity,local_journal_definitions
from .local_receipt_codec import CHUNK,STORED_LIMIT,ORIGINAL_LIMIT,ReceiptStorageOverflow,encode_receipt_chunks,original_receipt_chunks
from .lifecycle import finish


def compact_original_local_delta_journal(path,installation_id,targets,*,capacity=None):
    """Preserve every logical column byte; never open native/source publication.

    Oversize admission is exclusive to this streaming maintenance operation.
    Ordinary publication retains its existing 128MiB journal cap. Receipt rows
    are bounded by that existing budget individually, with at most 1000 rows per
    known table. No scopes, original plans, SQL, receipts or digests are replaced.
    """
    from .supply_chain_native import PrivateFiniteLease
    path=Path(path)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical original journal required')
    before=path.lstat()
    if not stat.S_ISREG(before.st_mode)or before.st_uid!=os.getuid()or before.st_mode&0o077 or before.st_nlink!=1:
        raise PermissionError('Owned private original journal required')
    registry={'installation':installation_id,'targets':[{'table':target.table,'path':str(target.path),'uuid':target.uuid}for target in sorted(targets,key=lambda item:item.table)]}
    selected=operation_capacity(capacity)
    if selected is not None:registry['operation_capacity']=dict(selected)
    lease=PrivateFiniteLease(path.parent,create=False);fd=None;connection=None;primary=None;result=None
    def renew():
        lease.renew();current=path.lstat()
        if (current.st_dev,current.st_ino,current.st_mode,current.st_uid,current.st_nlink)!=(before.st_dev,before.st_ino,before.st_mode,before.st_uid,before.st_nlink):raise PermissionError('Original journal identity changed')
    def chunks(table,column,key_name,key,length):
        if type(length)is not int or not 0<=length<=ORIGINAL_LIMIT:raise ValueError('Bounded original journal column required')
        for offset in range(0,length,CHUNK):
            raw=connection.execute('SELECT substr(CAST('+column+' AS BLOB),?,?) FROM '+table+' WHERE '+key_name+'=?',(offset+1,min(CHUNK,length-offset),key)).fetchone()[0]
            if type(raw)is not bytes or len(raw)!=min(CHUNK,length-offset):raise ValueError('Complete original journal column required')
            yield raw
    def receipt_chunks(key,length):
        if length<=STORED_LIMIT:
            stored=b''.join(chunks('local_operation','receipt','operation',key,length)).decode('utf8')
            yield from original_receipt_chunks(stored)
        else:yield from chunks('local_operation','receipt','operation',key,length)
    def digest():
        answer=hashlib.sha256()
        for table in sorted(local_journal_definitions()):
            columns=connection.execute('PRAGMA table_info('+table+')').fetchall();key_name=next(column[1]for column in columns if column[5]==1)
            keys=connection.execute('SELECT '+key_name+' FROM '+table+' ORDER BY '+key_name+' LIMIT 1001').fetchall()
            if len(keys)>1000:raise ValueError('Bounded original journal row inventory required')
            for (key,)in keys:
                for column in columns:
                    name=column[1]
                    kind,length=connection.execute('SELECT typeof('+name+'),length(CAST('+name+' AS BLOB)) FROM '+table+' WHERE '+key_name+'=?',(key,)).fetchone()
                    stream=receipt_chunks(key,length)if table=='local_operation'and name=='receipt'and kind!='null'else (()if kind=='null'else chunks(table,name,key_name,key,length))
                    value_sha=hashlib.sha256();size=0
                    for raw in stream:value_sha.update(raw);size+=len(raw)
                    answer.update((encoded([table,key,name,kind,size,value_sha.hexdigest()])+'\n').encode())
        return answer.hexdigest()
    try:
        renew();fd=os.open(str(path)+'.lock',os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        connection=sqlite3.connect('file:'+quote(str(path))+'?mode=rw',uri=True)
        connection.execute('BEGIN IMMEDIATE')
        definitions=local_journal_definitions();actual=dict(connection.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"));normalize=lambda sql:re.sub(r'\s+','',sql).lower()
        if set(actual)!=set(definitions)or any(normalize(actual[name])!=normalize(sql)for name,sql in definitions.items()):raise ValueError('Complete unchanged original journal schema required')
        if connection.execute('SELECT original FROM local_installation WHERE id=1').fetchall()!=[(encoded(registry),)]:raise ValueError('Complete original journal registry differs')
        original=digest();rows=connection.execute('SELECT operation,length(CAST(receipt AS BLOB)) FROM local_operation WHERE receipt IS NOT NULL ORDER BY operation LIMIT 1001').fetchall()
        if len(rows)>1000:raise ValueError('Bounded original receipt inventory required')
        for key,length in rows:
            if type(length)is not int or not 0<length<=ORIGINAL_LIMIT:raise ValueError('Bounded original receipt required')
            stream=receipt_chunks(key,length)
            # Existing envelopes may expand beyond their storage length; retain
            # their original verified length without allocating the expansion.
            encoded_receipt=False
            if length<=STORED_LIMIT:
                stored=b''.join(chunks('local_operation','receipt','operation',key,length)).decode('utf8');parsed=json.loads(stored)
                encoded_receipt=type(parsed)is dict and parsed.get('profile')=='ashlar-local-delta-receipt-zlib/0.1'
                logical_length=parsed['original_length']if encoded_receipt else length
            else:logical_length=length
            try:compressed=encode_receipt_chunks(stream,logical_length)
            except ReceiptStorageOverflow:
                if length<=STORED_LIMIT and not encoded_receipt:continue
                raise
            connection.execute('UPDATE local_operation SET receipt=? WHERE operation=?',(compressed,key))
        if digest()!=original:raise ValueError('Original logical journal bytes changed')
        renew();connection.commit();connection.execute('VACUUM')
        checkpoint=connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
        if checkpoint is None or checkpoint[0]!=0:raise ValueError('Original journal checkpoint remains busy')
        renew()
        if digest()!=original:raise ValueError('Compacted original logical journal differs')
        if path.stat().st_size>128*1024*1024:raise ValueError('Lossless compacted journal remains above original cap')
        LocalDeltaTransport.require_existing_journal(path,installation_id,targets,capacity=capacity)
        result={'logical_sha256':original,'receipts':len(rows),'before_bytes':before.st_size,'after_bytes':path.stat().st_size,
            'scope':'Lossless original local receipt storage only; no native/source publication or ACK grant.'}
    except BaseException as error:primary=error
    callbacks=([connection.rollback,connection.close]if connection is not None else [])+([lambda:os.close(fd)]if fd is not None else [])+[lease.close]
    finish(primary,callbacks)
    return result
