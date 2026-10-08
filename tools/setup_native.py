"""Install candidate Ashlar UC Delta carriers in an admitted existing catalog.

Owner-controlled development deployment, not production remote fencing. Retain
--journal permanently for same-handle recovery; no cleanup or overwrite exists.
"""
import argparse,fcntl,hashlib,json,re,sys,time
from pathlib import Path
from databricks.sdk import WorkspaceClient
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.authority import validate_writer_inventory
from durable_sql import DurableSQL,SQLPending
from persistent_sql import Client
from generated_carriers import load_generated_carriers

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--catalog',required=True);p.add_argument('--schema',required=True)
    p.add_argument('--journal',required=True);p.add_argument('--output',required=True)
    p.add_argument('--profile',default='aidev-cus');p.add_argument('--warehouse',default='2439e1f2e37ac563')
    args=p.parse_args()
    for name in [args.catalog,args.schema]:
        if not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',name):p.error('Safe catalog/schema identifiers required')
    w=WorkspaceClient(profile=args.profile);user=w.current_user.me();actor=user.user_name
    if not actor:raise ValueError('Authenticated owner principal unavailable')
    out=Path(args.output);c=Client(out,warehouse_id=args.warehouse,profile=args.profile)
    def observe(label,sql):
        rows=c.sql(label,sql);cols=c.records[-1]['response'].get('manifest',{}).get('schema',{}).get('columns',[])
        return [dict(zip([x['name'] for x in cols],r)) for r in rows]
    catalog=w.catalogs.get(name=args.catalog)
    validate_writer_inventory(catalog.owner,observe('catalog-authority','SHOW GRANTS ON CATALOG '+args.catalog),trusted_writers=[actor])
    transport=DurableSQL(args.journal,w.api_client,args.warehouse,user.id)
    with transport.db:transport.db.execute('CREATE TABLE IF NOT EXISTS installed_target (name TEXT PRIMARY KEY,uuid TEXT NOT NULL)')
    def execute(operation,sql):
        deadline=time.monotonic()+180
        while True:
            try:return transport.query(operation,sql,{})
            except SQLPending:
                if time.monotonic()>deadline:raise
                time.sleep(.2)
    prefix=args.catalog+'.'+args.schema
    generated,generated_sha256=load_generated_carriers(ROOT)
    definitions={name:carrier['sql'] for name,carrier in generated['carriers'].items()}
    identities={}
    try:
        with open(args.journal+'.writer-lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            execute(prefix+':schema','CREATE SCHEMA '+prefix)
            schema=w.schemas.get(full_name=prefix)
            validate_writer_inventory(schema.owner,observe('schema-authority','SHOW GRANTS ON SCHEMA '+prefix),trusted_writers=[actor])
            for short,ddl in definitions.items():
                name=prefix+'.'+short
                execute(name+':install',ddl.replace('CREATE TABLE `'+short+'`','CREATE TABLE '+name,1))
                native=w.tables.get(full_name=name)
                validate_writer_inventory(native.owner,observe('table-authority','SHOW GRANTS ON TABLE '+name),trusted_writers=[actor])
                detail=observe('target-identity','DESCRIBE DETAIL '+name)
                if len(detail)!=1:raise ValueError('Ambiguous installed identity')
                uuid=detail[0]['id']
                with transport.db:
                    transport.db.execute('INSERT OR IGNORE INTO installed_target VALUES (?,?)',(name,uuid))
                    original=transport.db.execute('SELECT uuid FROM installed_target WHERE name=?',(name,)).fetchone()[0]
                if original!=uuid:raise ValueError('Installed target replaced: '+name)
                # Verify the declared columns independently of metadata flags.
                observe('declared-schema','SELECT * FROM '+name+' LIMIT 0')
                actual=c.records[-1]['response']['manifest']['schema']['columns']
                types={'string':'STRING','long':'BIGINT','boolean':'BOOLEAN','timestamp':'TIMESTAMP'}
                expected=[(field['name'],types[field['deltaType']]) for field in generated['carriers'][short]['columns']]
                if [(x['name'],x['type_text']) for x in actual]!=expected:raise ValueError('Installed carrier schema differs: '+name)
                identities[name]={'uuid':uuid,'columns':expected,'detail':detail[0]}
            # Renew ancestors after all setup operations, before readiness.
            validate_writer_inventory(w.catalogs.get(name=args.catalog).owner,observe('final-catalog-authority','SHOW GRANTS ON CATALOG '+args.catalog),trusted_writers=[actor])
            validate_writer_inventory(w.schemas.get(full_name=prefix).owner,observe('final-schema-authority','SHOW GRANTS ON SCHEMA '+prefix),trusted_writers=[actor])
        summary={'state':'installed','namespace':prefix,'authenticated_owner':actor,'tables':identities,'generated_carriers_sha256':generated_sha256,'umf_generator':generated['generator'],'model_inputs':generated['inputs'],'qualification':'UMF-generated candidate ashlar-delta/0.3 development carriers; exact schema/UUID and fresh inherited writer inventories verified. Same-host journal/lock; platform admin trust, remote writer lifecycle, retained pins, actual schema admission and producer/publication still required. No data writes, cleanup or retention/grant changes.'}
        (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
        transport.db.row_factory=__import__('sqlite3').Row
        (out/'journal-export.json').write_text(json.dumps({'submissions':[dict(r) for r in transport.db.execute('SELECT * FROM submission ORDER BY operation')],'installed_targets':[dict(r) for r in transport.db.execute('SELECT * FROM installed_target ORDER BY name')]},indent=2)+'\n')
        print('Installed '+str(len(identities))+' candidate carriers in '+prefix)
    finally:transport.close()
if __name__=='__main__':main()
