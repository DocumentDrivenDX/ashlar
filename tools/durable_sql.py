"""Host-only SQLite custody for original Databricks SQL submissions.

The caller supplies an authenticated SDK api_client and trusted authority ID.
A submitting row without a retained handle is deliberately unrecoverable here:
reconcile original native query history/effects externally; never blind retry.
Use a private local journal, retain it with the publisher's recovery artifacts.
"""
import hashlib
import json
import sqlite3


class SQLCustodyError(ValueError):
    pass


class SQLPending(SQLCustodyError):
    pass


class DurableSQL:
    def __init__(self, path, api_client, warehouse_id, authority_id):
        if not isinstance(authority_id, str) or not authority_id:
            raise SQLCustodyError('Trusted authenticated authority identity required')
        import re
        if not re.fullmatch('[0-9a-f]{16}', warehouse_id):
            raise SQLCustodyError('Explicit warehouse ID required')
        self.api = api_client
        self.warehouse = warehouse_id
        self.authority = authority_id
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS submission (operation TEXT PRIMARY KEY, request TEXT NOT NULL, digest TEXT NOT NULL, handle TEXT, response TEXT)')
        self.db.commit()

    def close(self):
        self.db.close()

    def query(self, operation, statement, parameters):
        """Submit once or recover the original handle. Pending raises with handle.

        Calls never automatically cancel or create replacement statements. A
        stored terminal response is replayed, including original failures.
        Parameters use STRING types; complete inline results are bounded at1000.
        """
        if not isinstance(operation, str) or not operation or not isinstance(statement, str) or not statement:
            raise SQLCustodyError('Explicit operation and SQL required')
        if not isinstance(parameters, dict) or any(not isinstance(k,str) or not k or not isinstance(v,str) for k,v in parameters.items()):
            raise SQLCustodyError('Named string parameters required')
        body = {'warehouse_id':self.warehouse,'statement':statement,
                'parameters':[{'name':k,'type':'STRING','value':v} for k,v in sorted(parameters.items())],
                'wait_timeout':'10s','on_wait_timeout':'CONTINUE','disposition':'INLINE',
                'format':'JSON_ARRAY','row_limit':1000}
        request = json.dumps({'authority':self.authority,'body':body},sort_keys=True,separators=(',',':'))
        digest = hashlib.sha256(request.encode()).hexdigest()
        with self.db:
            cursor = self.db.execute('INSERT OR IGNORE INTO submission(operation,request,digest) VALUES (?,?,?)',(operation,request,digest))
            created = cursor.rowcount == 1
            row = self.db.execute('SELECT request,digest,handle,response FROM submission WHERE operation=?',(operation,)).fetchone()
        if row[0] != request or row[1] != digest:
            raise SQLCustodyError('Original operation request/authority conflict')
        if created:
            # The intent is durable before POST. A crash/exception before handle
            # persistence leaves an uncertain original, never permission to retry.
            response = self.api.do('POST','/api/2.0/sql/statements',body=body)
            handle = response.get('statement_id')
            if not isinstance(handle,str) or not handle:
                raise SQLCustodyError('Submission returned no original handle; reconciliation required')
            with self.db:
                self.db.execute('UPDATE submission SET handle=? WHERE operation=?',(handle,operation))
        else:
            handle = row[2]
            if not handle:
                raise SQLCustodyError('Original submission uncertain without handle; reconciliation required')
            response = json.loads(row[3]) if row[3] else self.api.do('GET','/api/2.0/sql/statements/'+handle)
        if response.get('statement_id') != handle:
            raise SQLCustodyError('Native response handle mismatch')
        state = response.get('status',{}).get('state')
        if state in ('PENDING','RUNNING'):
            raise SQLPending('Original statement still '+state+': '+handle)
        if state not in ('SUCCEEDED','FAILED','CANCELED','CLOSED'):
            raise SQLCustodyError('Unknown original statement state')
        exact = json.dumps(response,sort_keys=True,separators=(',',':'))
        with self.db:
            self.db.execute('UPDATE submission SET response=COALESCE(response,?) WHERE operation=?',(exact,operation))
        retained = self.db.execute('SELECT response FROM submission WHERE operation=?',(operation,)).fetchone()[0]
        if retained != exact:
            raise SQLCustodyError('Conflicting original terminal response')
        if state != 'SUCCEEDED':
            raise SQLCustodyError('Original statement '+state+': '+handle)
        manifest = response.get('manifest',{})
        result = response.get('result',{})
        if manifest.get('truncated') or result.get('next_chunk_internal_link') or result.get('next_chunk_index') is not None:
            raise SQLCustodyError('Original result incomplete; explicit chunk recovery required')
        return response
