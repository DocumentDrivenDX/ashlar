"""Lossless local receipt storage; exact original bytes remain authoritative."""
import base64,hashlib,json,re,zlib
PROFILE='ashlar-local-delta-receipt-zlib/0.1'
STORED_LIMIT=4194304
ORIGINAL_LIMIT=128*1024*1024
CHUNK=65536

class ReceiptStorageOverflow(ValueError):
    """Compression cannot fit the unchanged storage budget."""


def _top_level_profile(text):
    """Read only the outer profile, skipping large nested/string carriers."""
    decoder=json.JSONDecoder();index=0;size=len(text)
    def whitespace(position):
        while position<size and text[position].isspace():position+=1
        return position
    index=whitespace(index)
    if index==size or text[index]!='{':return None
    index+=1
    while True:
        index=whitespace(index)
        if index>=size or text[index]=='}':return None
        key,used=decoder.raw_decode(text[index:index+4096]);index+=used;index=whitespace(index)
        if index>=size or text[index]!=':':raise ValueError('Exact legacy receipt object required')
        index=whitespace(index+1)
        if key=='profile':return decoder.raw_decode(text[index:index+4096])[0]
        depth=0;quoted=False;escaped=False
        while index<size:
            character=text[index]
            if quoted:
                if escaped:escaped=False
                elif character=='\\':escaped=True
                elif character=='"':quoted=False
            elif character=='"':quoted=True
            elif character in '[{':depth+=1
            elif character in ']}':
                if depth==0:return None
                depth-=1
            elif character==','and depth==0:
                index+=1;break
            index+=1
        else:return None


def encode_receipt_chunks(chunks,length):
    if type(length)is not int or not 0<length<=ORIGINAL_LIMIT:raise ValueError('Bounded original receipt length required')
    compressor=zlib.compressobj(6);parts=[];digest=hashlib.sha256();observed=0;stored=0
    def add(value):
        nonlocal stored
        stored+=len(value)
        if stored>STORED_LIMIT*3//4-1024:raise ReceiptStorageOverflow('Bounded compressed receipt storage required')
        parts.append(value)
    for chunk in chunks:
        if type(chunk)is not bytes or not 0<len(chunk)<=CHUNK:raise ValueError('Bounded original receipt chunks required')
        observed+=len(chunk)
        if observed>length:raise ValueError('Original receipt length differs')
        digest.update(chunk);add(compressor.compress(chunk))
    if observed!=length:raise ValueError('Complete original receipt bytes required')
    add(compressor.flush())
    value={'profile':PROFILE,'original_length':length,'original_sha256':digest.hexdigest(),'zlib_base64':base64.b64encode(b''.join(parts)).decode('ascii')}
    result=json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    if len(result.encode())>STORED_LIMIT:raise ReceiptStorageOverflow('Bounded compressed receipt storage required')
    return result


def encode_local_receipt(original):
    if type(original)is not str:raise ValueError('Original receipt text required')
    raw=original.encode('utf8')
    try:stored=encode_receipt_chunks((raw[offset:offset+CHUNK]for offset in range(0,len(raw),CHUNK)),len(raw))
    except ReceiptStorageOverflow:
        if len(raw)<=STORED_LIMIT:return original
        raise
    return original if len(raw)<=STORED_LIMIT and len(raw)<len(stored.encode())else stored


def original_receipt_chunks(stored):
    """Hash-verified streaming original bytes, including unchanged legacy text."""
    if type(stored)is not str or not 0<len(stored)<=ORIGINAL_LIMIT:raise ValueError('Bounded stored receipt required')
    # Large legacy receipts were written by the original transport. Classify
    # the top-level profile without parsing/allocating their complete snapshot.
    # Original canonical receipt JSON uses this exact top-level profile member.
    if len(stored)>STORED_LIMIT:
        if _top_level_profile(stored)==PROFILE:raise ValueError('Bounded compressed receipt envelope required')
        observed=0
        for offset in range(0,len(stored),CHUNK//4):
            raw=stored[offset:offset+CHUNK//4].encode('utf8');observed+=len(raw)
            if observed>ORIGINAL_LIMIT:raise ValueError('Bounded original legacy receipt required')
            yield raw
        return
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate receipt storage member')
            result[key]=value
        return result
    value=json.loads(stored,object_pairs_hook=pairs)
    if type(value)is not dict or value.get('profile')!=PROFILE:
        raw=stored.encode('utf8')
        for offset in range(0,len(raw),CHUNK):yield raw[offset:offset+CHUNK]
        return
    if len(stored.encode())>STORED_LIMIT:raise ValueError('Bounded compressed receipt envelope required')
    if (set(value)!={'profile','original_length','original_sha256','zlib_base64'}or type(value['original_length'])is not int
            or not 0<value['original_length']<=ORIGINAL_LIMIT or type(value['original_sha256'])is not str
            or re.fullmatch('[0-9a-f]{64}',value['original_sha256'])is None or type(value['zlib_base64'])is not str):raise ValueError('Exact lossless receipt storage envelope required')
    packed=base64.b64decode(value['zlib_base64'],validate=True)
    if base64.b64encode(packed).decode()!=value['zlib_base64']:raise ValueError('Canonical compressed receipt carrier required')
    decoder=zlib.decompressobj();digest=hashlib.sha256();total=0
    for offset in range(0,len(packed),CHUNK):
        pending=packed[offset:offset+CHUNK]
        while pending:
            original=decoder.decompress(pending,CHUNK);pending=decoder.unconsumed_tail
            total+=len(original)
            if total>value['original_length']:raise ValueError('Receipt expansion exceeds original bound')
            digest.update(original)
            if original:yield original
            if decoder.unused_data:raise ValueError('Trailing compressed receipt bytes')
    if not decoder.eof or decoder.unconsumed_tail or total!=value['original_length']or digest.hexdigest()!=value['original_sha256']:
        raise ValueError('Complete original receipt digest/length differs')


def local_receipt_equals(stored,original):
    if type(original)is not str:raise ValueError('Exact reconstructed native receipt required')
    expected=iter(original[offset:offset+CHUNK//4].encode('utf8')for offset in range(0,len(original),CHUNK//4))
    pending=b'';equal=True
    for chunk in original_receipt_chunks(stored):
        offset=0
        while offset<len(chunk):
            if not pending:pending=next(expected,b'')
            if not pending:
                equal=False;break
            count=min(len(pending),len(chunk)-offset)
            if pending[:count]!=chunk[offset:offset+count]:equal=False
            pending=pending[count:];offset+=count
    return equal and not pending and next(expected,None)is None
