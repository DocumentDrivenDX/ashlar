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
    if recovered!=intake or policy.source_sha256!=intake.source_sha256 or policy.document_revision!=intake.document_revision:
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


def check_bound_existing_records(umf_source,intake,policy,state,original_policies,*,schema_path,output_dir):
    """Check retained prestate against an explicitly selected target definition.

    Original bindings decode original IDs. Logical Field references carry values
    to the selected target; target IDs/revisions never replace original history.
    No automatic schema compatibility or migration is inferred.
    """
    if not isinstance(intake,SchemaIntake) or not isinstance(policy,StringRecordPolicy):
        raise ValueError('Original target intake and binding required')
    recovered=SchemaIntake.read(intake.artifact,intake.document_revision,trusted_validator_revision=intake.validator_revision)
    if recovered!=intake or policy.source_sha256!=intake.source_sha256 or policy.document_revision!=intake.document_revision:
        raise ValueError('Target binding and original intake differ')
    groups={}
    target_refs={ref:set(policy.property_fields[tid].values()) for tid,ref in policy.record_identities.items()}
    for entity in sorted(state.current.values(),key=lambda value:value.key):
        original=state.history.get((entity.key,entity.version))
        if original is None or original.state!=entity:raise ValueError('Original retained current history required')
        key=(entity.key.source,entity.schema_revision)
        original_policy=original_policies.get(key)
        if not isinstance(original_policy,StringRecordPolicy):raise ValueError('Explicit original schema binding required')
        selected=original_policy.record_value_request(original)
        identity=selected['identity'];ref=(identity['module'],identity['element'])
        if original_policy.source_system!=policy.source_system or ref not in target_refs:
            raise ValueError('Explicit target Record correspondence required')
        if any((v['field']['module'],v['field']['element']) not in target_refs[ref] for v in selected['values']):
            raise ValueError('Unmapped retained Field; no implicit loss')
        groups.setdefault(ref,[]).append({'deliveryId':original.delivery_id,
            'recordSha256':original.raw_digest,'values':selected['values']})
    output_dir=Path(output_dir)
    if output_dir.exists():raise ValueError('Fresh original prestate receipt directory required')
    output_dir.mkdir(parents=True)
    return tuple(check_value_request(umf_source,intake,records,output_dir/('record-'+str(ordinal)+'.json'),schema_path,
        identity={'module':ref[0],'element':ref[1]}) for ordinal,(ref,records) in enumerate(sorted(groups.items()),1))
