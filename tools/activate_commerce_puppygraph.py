"""Explicit local dedicated PuppyGraph schema addition preserving prior labels.

Requires independently retained full prior model and exact active label parity.
Not atomic release activation; engine schema mutation is a separate local probe.
"""
import argparse,base64,hashlib,json,os,urllib.request
from pathlib import Path

def merged_model(prior,addition):
    if set(prior)!=set(addition) or set(prior)!={'catalog','node','edge'}:raise ValueError('Complete qualified model shape required')
    result={}
    for section,key in [('catalog','name'),('node','label'),('edge','label')]:
        rows=prior[section]+addition[section]
        if len({r[key] for r in rows})!=len(rows):raise ValueError('Existing catalog/labels must not be overwritten')
        result[section]=rows
    return result

def activate(prior_bytes,addition_bytes,*,endpoint,user,password,output):
    if endpoint!='http://127.0.0.1:18881':raise ValueError('Explicit existing dedicated local endpoint required')
    out=Path(output)
    if out.exists():raise ValueError('Fresh evidence directory required')
    prior=json.loads(prior_bytes);addition=json.loads(addition_bytes);merged=merged_model(prior,addition)
    auth='Basic '+base64.b64encode((user+':'+password).encode()).decode()
    def call(path,data=None):
        request=urllib.request.Request(endpoint+path,data=data,headers={'Authorization':auth,'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=30) as response:return response.read()
    before=call('/schemajson');active=json.loads(before)
    for section in ('node','edge'):
        if active.get(section)!=prior[section]:raise ValueError('Current complete prior node/edge inventory differs')
    visibility='omitted'
    if 'catalog' in active:
        if active['catalog']!=prior['catalog']:raise ValueError('Current complete catalog inventory differs')
        visibility='present'
    request=json.dumps(merged,sort_keys=True,separators=(',',':')).encode()
    # Evidence exists before mutation; unknown HTTP outcomes must be inspected.
    out.mkdir(parents=True)
    (out/'before.json').write_bytes(before);(out/'request.json').write_bytes(request)
    response=call('/schema?postUploadBehavior=none',request);(out/'upload.json').write_bytes(response)
    if json.loads(response).get('ok') is not True:raise ValueError('Native schema upload did not succeed')
    after=call('/schemajson');(out/'after.json').write_bytes(after)
    observed=json.loads(after)
    for section in ('node','edge'):
        if observed.get(section)!=merged[section]:raise ValueError('Complete prior+commerce native schema differs')
    if 'catalog' in observed and observed['catalog']!=merged['catalog']:raise ValueError('Complete native catalog inventory differs')
    facts={'catalog_api_visibility':visibility,'prior_sha256':hashlib.sha256(prior_bytes).hexdigest(),'addition_sha256':hashlib.sha256(addition_bytes).hexdigest(),'request_sha256':hashlib.sha256(request).hexdigest()}
    (out/'custody.json').write_text(json.dumps(facts,sort_keys=True,indent=2)+'\n')
    return {'request_sha256':hashlib.sha256(request).hexdigest(),'scope':'Dedicated local schema addition; no atomic graph activation or rollback guarantee'}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('prior','addition','output'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--endpoint',required=True);a=p.parse_args()
    print(json.dumps(activate(a.prior.read_bytes(),a.addition.read_bytes(),endpoint=a.endpoint,user=os.environ['ASHLAR_PUPPY_USER'],password=os.environ['ASHLAR_PUPPY_PASSWORD'],output=a.output)))
