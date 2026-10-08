"""Fresh exact warehouse observations; never sets or resets session configuration."""
import json
from ashlar.publication import ResolutionError
from databricks_transport import sql_result

ENGINE_SQL="SELECT to_json(current_version(), map('ignoreNullFields','false')) AS __weft_warehouse"
ANSI_SQL='SET ANSI_MODE'
EXPECTED_ENGINE={'dbr_version':None,'dbsql_version':'2026.39',
    'u_build_hash':'15b447529a1f55ca1ad8c73a89b8d196f6ed4904',
    'r_build_hash':'3909d148af5cb6b560624c9255f5a727f7a17a4c'}
EXPECTED_PROFILE={
    'engineVersion':'2026.39',
    'qualification':'evidence-qualified compiler semantics; host execution and publication remain conditional',
    'requirements':[
        'verify the exact executing native engine and relevant settings before any integrity or user SQL',
        'retain the same admitted catalog/publication context across checks and buffered result publication',
        'refuse mismatched or unavailable profile evidence; never silently choose a newer engine or another profile'],
    'settings':{'ansi_mode':True,'arithmetic':'ANSI exact-or-error','comparison':'explicit UTF8_BINARY',
        'resultTransport':'JSON_ARRAY exact STRING','warehouseBuild':{k:EXPECTED_ENGINE[k] for k in ('r_build_hash','u_build_hash')}},
    'targetProfile':'dbsql2026.39-qualified'}

def verify_observations(profile,engine,ansi):
    if profile!=EXPECTED_PROFILE:raise ResolutionError('Unadmitted native profile requirements')
    if engine.columns!=(('__weft_warehouse','STRING'),) or len(engine.rows)!=1:
        raise ResolutionError('Complete exact warehouse identity observation required')
    raw=engine.rows[0].get('__weft_warehouse')
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise ResolutionError('Duplicate warehouse identity content')
            result[key]=value
        return result
    if type(raw) is not str:raise ResolutionError('Exact warehouse identity text required')
    try:identity=json.loads(raw,object_pairs_hook=unique)
    except (ValueError,TypeError) as error:raise ResolutionError('Invalid warehouse identity') from error
    if identity!=EXPECTED_ENGINE:raise ResolutionError('Native engine/build drift; no fallback')
    if ansi.columns!=(('key','STRING'),('value','STRING')) or list(ansi.rows)!=[{'key':'ANSI_MODE','value':'true'}]:
        raise ResolutionError('ANSI mode is unavailable or differs; no settings change')
    return identity

def observe_native_profile(client,profile):
    if profile!=EXPECTED_PROFILE:raise ResolutionError('Unadmitted native profile requirements')
    client.sql('weft-profile-engine',ENGINE_SQL)
    engine=sql_result(client.records[-1]['response'])
    client.sql('weft-profile-ansi-read',ANSI_SQL)
    ansi=sql_result(client.records[-1]['response'])
    return verify_observations(profile,engine,ansi)
