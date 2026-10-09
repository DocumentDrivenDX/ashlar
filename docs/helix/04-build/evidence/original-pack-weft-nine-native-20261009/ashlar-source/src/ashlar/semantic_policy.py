"""Executable selected string-record constraints, not native catalog acceptance.

Rebuild meaning from the retained UMF interpretation and require complete active
ID bindings supplied by a trusted catalog authority. No IDs are inferred here.
"""
from types import MappingProxyType
from .binding import plan_string_record_binding,BindingError
from .catalog import Identity,MappingEntry
from .schema import _json,SchemaIntake

class SemanticPolicyError(ValueError):
    pass

class StringRecordPolicy:
    @classmethod
    def from_intake(cls,intake,interpretation,entries,*,source_system,source_schema_revision=None):
        """Bind original intake custody to interpretation before host enforcement.

        The default revision is the owner document revision for raw document sources.
        A catalog source must supply its independently admitted event revision.
        This does not attest native catalog acceptance of supplied ID entries.
        """
        if not isinstance(intake,SchemaIntake):
            raise SemanticPolicyError('Complete original schema intake required')
        recovered=SchemaIntake.read(intake.artifact,intake.document_revision,
                                   trusted_validator_revision=intake.validator_revision)
        if recovered!=intake:
            raise SemanticPolicyError('Schema intake differs from original artifact')
        policy=cls(interpretation,entries,validator_revision=intake.validator_revision,
                   source_system=source_system,schema_revision=intake.document_revision if source_schema_revision is None else source_schema_revision)
        policy.document_revision=intake.document_revision
        if policy.source_sha256!=intake.source_sha256:
            raise SemanticPolicyError('Interpretation does not belong to retained original schema')
        return policy

    @classmethod
    def from_truss_catalog(cls,intake,interpretation,entries,*,source_system,catalog_revision):
        """Bind data's native accepted-catalog revision separately from owner revision.

        Caller must independently admit the original report, IDs and positive
        catalog revision. This method provides constraints, not that authority.
        """
        if type(catalog_revision) is not str or not 1<=len(catalog_revision)<=10 or not catalog_revision.isascii() or not catalog_revision.isdecimal() or str(int(catalog_revision))!=catalog_revision or not 0<int(catalog_revision)<2**31:
            raise SemanticPolicyError('Explicit positive canonical native catalog revision required')
        policy=cls.from_intake(intake,interpretation,entries,source_system=source_system,
                               source_schema_revision=catalog_revision)
        policy.catalog_revision=catalog_revision
        return policy

    def __init__(self,interpretation,entries,*,validator_revision,source_system,schema_revision):
        if any(not isinstance(s,str) or not s or '\x00' in s for s in [source_system,schema_revision]):
            raise SemanticPolicyError('Explicit admitted source/revision required')
        plan=plan_string_record_binding(interpretation,trusted_validator_revision=validator_revision)
        if plan['status']!='candidate' or plan['blocked']:
            raise SemanticPolicyError('Unsupported UMF assertions block executable binding')
        desired={Identity('type',tuple(t['identity'])) for t in plan['types']}
        desired.update(Identity('property',tuple(p['identity'])) for p in plan['properties'])
        mapping={};ids=set();identities=set()
        for entry in entries:
            if not isinstance(entry,MappingEntry):raise SemanticPolicyError('Trusted catalog entries required')
            entry.identity.validate()
            if type(entry.active) is not bool or entry.identity in identities:
                raise SemanticPolicyError('Invalid or duplicate catalog lifecycle identity')
            identities.add(entry.identity)
            if type(entry.catalog_id) is not int or not 1<=entry.catalog_id<2**31 or (entry.identity.family,entry.catalog_id) in ids:
                raise SemanticPolicyError('Invalid or ambiguous reserved catalog ID')
            ids.add((entry.identity.family,entry.catalog_id))
            if not entry.active:continue
            if entry.identity not in desired or entry.identity in mapping:
                raise SemanticPolicyError('Foreign or duplicate active binding')
            mapping[entry.identity]=entry.catalog_id
        if set(mapping)!=desired:raise SemanticPolicyError('Incomplete active catalog binding')
        types={};record_identities={};property_fields={}
        for record in plan['types']:
            identity=tuple(record['identity']);properties={}
            for prop in plan['properties']:
                if tuple(prop['identity'][:3])!=identity:continue
                pid=str(mapping[Identity('property',tuple(prop['identity']))])
                properties[pid]=prop['nullability']['nullability']
            type_id=mapping[Identity('type',identity)]
            types[type_id]=MappingProxyType(properties)
            record_identities[type_id]=(identity[1],identity[2])
            property_fields[type_id]=MappingProxyType({str(mapping[Identity('property',tuple(prop['identity']))]):tuple(prop['identity'][3:]) for prop in plan['properties'] if tuple(prop['identity'][:3])==identity})
        self.record_identities=MappingProxyType(record_identities);self.property_fields=MappingProxyType(property_fields)
        self.types=MappingProxyType(types);self.source_system=source_system;self.schema_revision=schema_revision
        self.source_sha256=plan['sourceSha256'];self.interpretation_sha256=plan['interpretationSha256']
    def __call__(self,change):
        state=change.state
        if state.key.source!=self.source_system or state.schema_revision!=self.schema_revision:
            raise SemanticPolicyError('Unadmitted source or schema revision')
        if state.key.kind!='object' or state.key.type_id not in self.types:
            raise SemanticPolicyError('Unknown Record or unsupported relationship binding')
        values=_json(state.props_json.encode());fields=self.types[state.key.type_id]
        if not isinstance(values,dict) or not set(values).issubset(fields):
            raise SemanticPolicyError('Unknown executable property ID')
        for pid,availability in fields.items():
            if pid not in values:
                if availability=='required':raise SemanticPolicyError('Missing required string property')
            elif not isinstance(values[pid],str):
                # absent-allowed means absence, never implicit JSON null or cast.
                raise SemanticPolicyError('Singleton string required; no null/type coercion')
        # retained_json remains opaque exact custody, not executable assertions.

    def record_value_request(self,change):
        """Translate an admitted ID binding into UMF's qualified Record request.

        Does not validate UMF or confer accepted native IDs. Omitted properties
        remain absent; opaque retained content is not interpreted as fields.
        """
        self(change)
        module,element=self.record_identities[change.state.key.type_id]
        fields=self.property_fields[change.state.key.type_id]
        values=_json(change.state.props_json.encode('utf-8'))
        return {'identity':{'module':module,'element':element},'values':[
            {'field':{'module':fields[pid][0],'element':fields[pid][1]},
             'state':'present','value':{'string':value}} for pid,value in values.items()]}
