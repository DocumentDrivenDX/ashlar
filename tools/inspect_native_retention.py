"""Read UC automatic-maintenance metadata; never change retention or grants."""
import argparse,json,sys
from pathlib import Path
from databricks.sdk import WorkspaceClient
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from ashlar.retention import RetentionError,validate_predictive_optimization_disabled

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    if args.output.exists():p.error('New output directory required')
    w=WorkspaceClient(profile='aidev-cus')
    actor=w.current_user.me().user_name
    catalog='ashlar_e2e_private_20261008';schema=catalog+'.runtime_csv'
    def observation(kind,name,value):
        full=value.as_dict()
        try:
            validate_predictive_optimization_disabled(full.get('enable_predictive_optimization'),full.get('effective_predictive_optimization_flag'))
            exclusion={'outcome':'observed_disabled','qualification':'Narrow scheduling exclusion only; outstanding operations and retained files remain unqualified.'}
        except RetentionError as exc:
            exclusion={'outcome':'refused','reason':str(exc)}
        return {'kind':kind,'name':name,'owner':full.get('owner'),'predictive_optimization_exclusion':exclusion,
            'enable_predictive_optimization':full.get('enable_predictive_optimization'),
            'effective_predictive_optimization_flag':full.get('effective_predictive_optimization_flag'),
            'retention_properties':{k:v for k,v in (full.get('properties') or {}).items() if k in ('delta.deletedFileRetentionDuration','delta.logRetentionDuration','delta.enableExpiredLogCleanup')},
            'qualification':'Fresh UC metadata observation, not an operator fence or retained-file proof. Absent properties are unspecified, never assumed disabled.'}
    observations=[observation('catalog',catalog,w.catalogs.get(name=catalog)),observation('schema',schema,w.schemas.get(full_name=schema))]
    for short in ('object_current','edge_current','tombstone','whole_source_history'):
        name=schema+'.'+short
        observations.append(observation('table',name,w.tables.get(full_name=name)))
    result={'authenticated_actor':actor,'observations':observations,'qualification':'Read-only metadata API; no SQL workload, mutations, retention changes or publication admission. Effective predictive optimization may expose an automatic maintenance lane requiring independent containment.'}
    args.output.mkdir(parents=True)
    (args.output/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
if __name__=='__main__':main()
