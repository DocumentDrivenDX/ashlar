"""Pure original publication request/effect correspondence; no authority grant."""
import hashlib
import json
from ashlar.source_checkpoint import outbox_checkpoint,truss_checkpoint
from ashlar.staging import batch_row
from .delta_custody import encoded, sha

def local_effect_plan(generated, prior_rows, tables):
    """Elide only closed generated zero-match DELETE before any submission.

    Caller must independently verify complete original native prior rows/UUIDs
    under the held writer. This pure correspondence function grants no admission.
    """
    selected = []
    elisions = []
    for (ordinal, step) in enumerate(generated):
        found = None
        for (role, typed) in (('object_current', 'type_id'), ('edge_current', 'rel_type_id')):
            quoted = '`' + '`.`'.join(tables[role].split('.')) + '`'
            expected = 'MERGE INTO ' + quoted + " t USING (SELECT k.* FROM (SELECT explode(from_json(:keys,'ARRAY<STRUCT<source_system:STRING,type_id:STRING,id:STRING>>')) k)) s ON t.source_system=s.source_system AND t." + typed + '=cast(s.type_id AS BIGINT) AND t.id=cast(s.id AS BIGINT) WHEN MATCHED THEN DELETE'
            if step['statement'] != expected:
                continue
            if set(step['parameters']) != {'keys'}:
                raise ValueError('Closed generated delete key inventory required')
            keys = json.loads(step['parameters']['keys'])
            if not isinstance(keys, list) or not keys or any((set(k) != {'source_system', 'type_id', 'id'} or any((type(v) is not str for v in k.values())) for k in keys)):
                raise ValueError('Exact generated source-qualified delete keys required')
            matches = [row for row in prior_rows[role] if any(((row['source_system'], row[typed], row['id']) == (k['source_system'], k['type_id'], k['id']) for k in keys))]
            if not matches:
                found = {'ordinal': ordinal, 'original_step': json.loads(encoded(step)), 'table': tables[role], 'source_keys': keys, 'matched_rows': [], 'complete_prior_rows_sha256': sha(encoded(sorted(prior_rows[role], key=encoded)))}
        if found is None:
            selected.append(json.loads(encoded(step)))
        else:
            elisions.append(found)
    return (selected, elisions)


def request_for(stream, transaction, predecessor, revisions):
    row = batch_row(transaction.batch)
    request = {'stream': stream, 'batch_id': transaction.batch.batch_id, 'predecessor': predecessor, 'schema_revisions_json': encoded(revisions), 'source_batch_json': row['batch_json'], 'source_batch_digest': row['batch_digest'], 'source_checkpoint_json': outbox_checkpoint(transaction)}
    request['request_digest'] = hashlib.sha256(json.dumps(request, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return request


def truss_request_for(stream,batch,predecessor,revisions):
    """Prepare the same exact native request used by publish_truss_transaction."""
    row=batch_row(batch)
    request={'stream':stream,'batch_id':batch.batch_id,'predecessor':predecessor,
             'schema_revisions_json':encoded(revisions),'source_batch_json':row['batch_json'],
             'source_batch_digest':row['batch_digest'],'source_checkpoint_json':truss_checkpoint(batch)}
    request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return request

