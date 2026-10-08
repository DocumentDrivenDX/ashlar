"""Host SDK-to-core SQLResult adapter with explicit journaled mutations.

Read queries use fresh authenticated observations; mutations must supply an
original operation ID to the durable journal. No ordinal-derived write IDs.
"""
from ashlar.native import SQLResult
from durable_sql import SQLCustodyError


def sql_result(response):
    if response.get('status',{}).get('state')!='SUCCEEDED':
        raise SQLCustodyError('No successful original SQL response')
    manifest=response.get('manifest',{});result=response.get('result',{})
    if manifest.get('truncated') or result.get('next_chunk_index') is not None or result.get('next_chunk_internal_link'):
        raise SQLCustodyError('Incomplete original SQL result')
    fields=manifest.get('schema',{}).get('columns',[])
    names=[];columns=[]
    for ordinal,field in enumerate(fields):
        if field.get('position')!=ordinal or not isinstance(field.get('name'),str) or not field['name'] or not isinstance(field.get('type_text'),str) or not field['type_text']:
            raise SQLCustodyError('Incomplete native column metadata')
        names.append(field['name']);columns.append((field['name'],field['type_text']))
    if len(set(names))!=len(names):raise SQLCustodyError('Duplicate native column names')
    rows=result.get('data_array',[])
    if not isinstance(rows,list) or any(not isinstance(row,list) or len(row)!=len(names) for row in rows):
        raise SQLCustodyError('Native result shape mismatch')
    if rows and not names:raise SQLCustodyError('Missing native result schema')
    count=manifest.get('total_row_count')
    if count is not None and (type(count) is not int or count!=len(rows)):
        raise SQLCustodyError('Original native row inventory incomplete')
    return SQLResult([dict(zip(names,row)) for row in rows],tuple(columns))

class DatabricksTransport:
    def __init__(self,read_client,journal):
        self.read_client=read_client;self.journal=journal
        if read_client.warehouse_id!=journal.warehouse:
            raise SQLCustodyError('Read/write warehouse mismatch')
        if read_client.w.api_client is not journal.api:
            raise SQLCustodyError('Read/write authenticated SDK client mismatch')
        # Application authority still requires independently checked policies.
    def query(self,sql,parameters):
        # Trusted generated SQL only; prefix checks are routing, not a sandbox.
        prefix=sql.lstrip().split(None,1)[0].upper() if sql.strip() else ''
        if prefix not in ('SELECT','DESCRIBE','SHOW'):
            raise SQLCustodyError('Mutation requires an explicit original operation ID')
        if any(not isinstance(k,str) or not isinstance(v,str) for k,v in parameters.items()):
            raise SQLCustodyError('Named string parameters required')
        self.read_client.sql('authenticated-read',sql,parameters=[{'name':k,'type':'STRING','value':v} for k,v in parameters.items()] or None)
        return sql_result(self.read_client.records[-1]['response'])
    def mutation(self,operation,sql,parameters):
        return sql_result(self.journal.query(operation,sql,parameters))

class OperationExecutor:
    """Bind one retained mutation identity while keeping all reads fresh."""
    def __init__(self,transport,operation):
        if not isinstance(operation,str) or not operation:
            raise SQLCustodyError('Explicit original mutation operation required')
        self.transport=transport;self.operation=operation
    def query(self,sql,parameters):
        prefix=sql.lstrip().split(None,1)[0].upper() if sql.strip() else ''
        if prefix in ('SELECT','SHOW','DESCRIBE'):return self.transport.query(sql,parameters)
        return self.transport.mutation(self.operation,sql,parameters)

class CompiledWeftTransport:
    """Allow exact trusted compiler statements, including WITH, without widening reads."""
    def __init__(self,transport,artifact):
        self.transport=transport
        from weft_native_profile import EXPECTED_PROFILE
        profiles=[item['parameters'] for item in artifact['obligations'] if item['id']=='ashlar.nativeProfile']
        if profiles!=[EXPECTED_PROFILE]:
            raise SQLCustodyError('Exact native execution profile required')
        self.profile=profiles[0]
        self.allowed={artifact['sql']}
        for item in artifact['obligations']:
            self.allowed.update(check['sql'] for check in item['parameters'].get('checks',[]))
    def query(self,sql,parameters):
        if sql not in self.allowed:return self.transport.query(sql,parameters)
        if any(type(k) is not str or type(v) is not str for k,v in parameters.items()):
            raise SQLCustodyError('Exact compiler parameter carriers required')
        client=self.transport.read_client
        from weft_execution_guard import guarded_statement
        client.sql('weft-compiled-read',guarded_statement(sql,self.profile),parameters=[{'name':k,'type':'STRING','value':v} for k,v in parameters.items()] or None)
        return sql_result(client.records[-1]['response'])
