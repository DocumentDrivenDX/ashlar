"""Actual UMF checks using explicit admitted bindings, never fixed example IDs."""
from pathlib import Path
from ashlar.schema import SchemaIntake
from ashlar.semantic_policy import StringRecordPolicy
from ashlar.whole_entity import changes_from_batch
from run_local_example import check_value_request


def check_bound_records(umf_source, intake, policy, batches, *, schema_path, output_dir):
    # Host-selected policy supplies source/revision and independently admitted IDs.
    # Each original Record group keeps its own untouched upstream receipt.
    if not isinstance(intake,SchemaIntake) or not isinstance(policy,StringRecordPolicy):
        raise ValueError('Original intake and explicit string Record policy required')
    recovered=SchemaIntake.read(intake.artifact,intake.document_revision,trusted_validator_revision=intake.validator_revision)
    if recovered!=intake or policy.source_sha256!=intake.source_sha256:
        raise ValueError('Binding and original intake source differ')
    groups={} 
    for batch in batches:
        changes=changes_from_batch(batch)
        if len(changes)!=len(batch.records):raise ValueError('Complete original change inventory required')
        for change,record in zip(changes,batch.records):
            selected=policy.record_value_request(change)
            if change.operation=='delete':continue
            identity=selected['identity'];key=(identity['module'],identity['element'])
            groups.setdefault(key,[]).append({'deliveryId':record.delivery_id,
                'recordSha256':record.sha256,'values':selected['values']})
    output_dir=Path(output_dir)
    if output_dir.exists():raise ValueError('Fresh original receipt directory required')
    output_dir.mkdir(parents=True)
    receipts=[]
    for ordinal,(key,records) in enumerate(sorted(groups.items()),1):
        receipts.append(check_value_request(umf_source,intake,records,
            output_dir/('record-'+str(ordinal)+'.json'),schema_path,
            identity={'module':key[0],'element':key[1]}))
    return tuple(receipts)
