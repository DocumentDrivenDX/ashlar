"""Independent template-CSV commerce arithmetic; no graph or UMF admission."""
import csv
import hashlib
import io
import json
from decimal import Decimal, localcontext
from pathlib import Path
from domain_pack_inventory import read_json, safe_path

INVENTORY_SHA256='795261c25448998fdd1bf80fe42cd7df25dff72f7aca0e4149501ffc82fbe8a2'


def commerce_oracle(root, inventory):
    root=Path(root)
    if root.is_symlink():raise ValueError('Original regular source required')
    root=root.resolve();raw=Path(inventory).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=INVENTORY_SHA256:
        raise ValueError('Trusted original inventory required')
    packs=[p for p in read_json(raw)['packs'] if p['id']=='commerce']
    if len(packs)!=1:raise ValueError('Original commerce pack required')
    tables={}
    for entry in packs[0]['files']:
        relative=safe_path(entry['path']);path=root/relative
        if path.is_symlink() or any(p.is_symlink() for p in path.parents if root in p.parents):
            raise ValueError('Original regular source required')
        data=path.read_bytes()
        if hashlib.sha256(data).hexdigest()!=entry['sha256']:
            raise ValueError('Original commerce bytes differ')
        if relative.startswith('data/') and relative.endswith('.csv'):
            rows=list(csv.DictReader(io.StringIO(data.decode('utf-8'),newline='')))
            if any(None in r or any(v is None for v in r.values()) for r in rows):
                raise ValueError('Complete original CSV rows required')
            keyed={r['id']:r for r in rows}
            if len(keyed)!=len(rows):raise ValueError('Ambiguous template row identity')
            tables[path.stem]=keyed
    # These are the original authored scenario operations, not SQL evaluation.
    # Decimal context exceeds all input/output coefficients in this tiny corpus.
    with localcontext() as context:
        context.prec=128
        partial=[];over=[];settled=[];refunds=[]
        for f in tables['fulfillments'].values():
            line=tables['order_lines'][f['line_id']]
            fq,lq=int(f['quantity']),int(line['quantity'])
            if fq>lq:over.append([f['id']])
            if fq<lq:
                for r in tables['returns'].values():
                    if r['line_id']==line['id']:
                        rq=int(r['quantity']);partial.append([fq,rq,lq-fq+rq])
        for p in tables['payments'].values():
            i=tables['invoices'][p['invoice_id']]
            if Decimal(p['amount'])==Decimal(i['amount']) and p['currency']==i['currency']:
                settled.append([p['id']])
        for r in tables['refunds'].values():
            returned=tables['returns'][r['return_id']]
            line=tables['order_lines'][returned['line_id']]
            product=tables['products'][line['product_id']]
            if Decimal(r['amount'])==int(returned['quantity'])*Decimal(product['unit_price']):
                refunds.append([r['id']])
    return {'format':'ashlar-commerce-template-oracle/0.1',
            'inventory_sha256':INVENTORY_SHA256,
            'table_counts':{k:len(v) for k,v in sorted(tables.items())},
            'scenarios':{'partial-return':sorted(partial),'fulfillment':sorted(over),
                         'settlement':sorted(settled),'refund':sorted(refunds)},
            'qualification':'Independent original template-CSV expected results using exact integer/decimal arithmetic; no SQL execution, graph replay-key mapping, UMF admission or engine support proof.'}


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('commerce_root');p.add_argument('inventory');a=p.parse_args()
    print(json.dumps(commerce_oracle(a.commerce_root,a.inventory),indent=2))
