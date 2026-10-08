"""Complete native snapshot checks; source authority and pin custody stay explicit."""
import json
from ashlar.effect_validation import validate_effect_snapshot
from ashlar.protocol import inspect_protocol,ReaderProtocolProfile
from ashlar.publication import Descriptor,_decode,_freeze
from ashlar.publisher import PublicationError
from ashlar.schema import _json
from ashlar.stored_publisher import _artifact
from ashlar.manifest import validate_manifest_row


class NativeArtifactValidator:
    """Callable JournaledPublisherDriver validator and retained-descriptor checker.

    Policy.admit(descriptor,targets,context), gate.check(descriptor,context), and
    pins(descriptor,context) must return None. Policy independently binds original
    source/schema/request, expected inventories and current authority. Pin service
    must cover the complete descriptor vector under actual native custody.
    This validator neither acquires a writer lane nor manufactures admission.
    """
    def __init__(self,executor,targets,policy,gate,pins,*,reader_profile,manifest_table,manifest_uuid):
        targets=_json(json.dumps(targets,sort_keys=True,separators=(',',':')).encode())
        if not isinstance(targets,dict) or not targets or len(targets)>16:
            raise PublicationError('Complete bounded independently admitted target inventory required')
        for target in targets.values():
            if not isinstance(target,dict) or set(target)!={'uuid','version','columns','rows'}:
                raise PublicationError('Exact native snapshot target interpretation required')
        if not isinstance(reader_profile,ReaderProtocolProfile):raise PublicationError('Explicit reader protocol profile required')
        self._targets_json=json.dumps(targets,sort_keys=True,separators=(',',':'))
        self.executor=executor;self.policy=policy;self.gate=gate;self.pins=pins
        self.profile=reader_profile;self.manifest_table=manifest_table;self.manifest_uuid=manifest_uuid

    def _admit(self,descriptor,context):
        copied=_json(self._targets_json.encode())
        if self.policy.admit(descriptor,copied,context) is not None:
            raise PublicationError('Complete current source/schema/writer admission required')

    def _descriptor_targets(self,descriptor):
        if not isinstance(descriptor,Descriptor):raise PublicationError('Original retained descriptor required')
        row=dict(descriptor.raw);validate_manifest_row(row)
        original=Descriptor(row['publication_id'],row['profile_version'],_freeze(_decode(row['table_versions_json'])),
            _freeze(_decode(row['schema_revisions_json'])),_freeze(_decode(row['source_progress_json'])),
            _freeze(_decode(row['validation_report_json'])),_freeze(row))
        if descriptor!=original:raise PublicationError('Descriptor fields differ from original raw manifest')
        targets=_json(self._targets_json.encode())
        if dict(descriptor.versions)!={table:target['version'] for table,target in targets.items()}:
            raise PublicationError('Descriptor differs from complete admitted version vector')
        return targets

    def validate_descriptor(self,descriptor,context):
        targets=self._descriptor_targets(descriptor)
        self._admit(descriptor,context)
        if self.gate.check(descriptor,context) is not None:raise PublicationError('Finite retention admission incomplete')
        inspect_protocol(self.executor,self.manifest_table,self.manifest_uuid,profile=self.profile)
        for table,target in targets.items():
            inspect_protocol(self.executor,table,target['uuid'],profile=self.profile)
            validate_effect_snapshot(self.executor,table,target['uuid'],target['version'],target['columns'],target['rows'])
        if self.pins(descriptor,context) is not None:raise PublicationError('Complete current pin custody required')
        if self.gate.check(descriptor,context) is not None:raise PublicationError('Closing finite retention admission incomplete')
        self._admit(descriptor,context)

    def _renew_pinned_descriptor(self,descriptor,context):
        """Internal renewal after full parity in one continuously held pin interval.

        Delta snapshots are immutable under the admitted UUID/version; cooperating
        maintenance must honor the held complete pin vector. Current admission
        remains mandatory at every renewal.
        """
        targets=self._descriptor_targets(descriptor)
        self._admit(descriptor,context)
        if self.gate.check(descriptor,context) is not None:raise PublicationError('Finite retention admission incomplete')
        inspect_protocol(self.executor,self.manifest_table,self.manifest_uuid,profile=self.profile)
        for table,target in targets.items():inspect_protocol(self.executor,table,target['uuid'],profile=self.profile)
        if self.pins(descriptor,context) is not None:raise PublicationError('Complete current pin custody required')
        if self.gate.check(descriptor,context) is not None:raise PublicationError('Closing finite retention admission incomplete')
        self._admit(descriptor,context)

    def __call__(self,request,artifact,descriptor,context):
        _,original=_artifact(artifact,dict(request))
        if descriptor!=original:raise PublicationError('Original request-bound artifact descriptor required')
        self.validate_descriptor(descriptor,context)
