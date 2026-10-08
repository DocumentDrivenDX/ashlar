"""Full parity reuse limited to one real, complete native pin interval."""
from contextlib import contextmanager
from ashlar.manifest import bind_manifest_pins
from ashlar.publisher import PublicationError
from native_artifact_validation import NativeArtifactValidator


class PinnedArtifactValidation:
    def __init__(self,pins,vector,validator,context):
        if not isinstance(validator,NativeArtifactValidator):raise PublicationError('Concrete native artifact validator required')
        self.pins=pins;self.vector=vector;self.validator=validator;self.context=context
        self.inventory=validator._targets_json
        self.active=False;self.original=None;self.poisoned=False

    @contextmanager
    def hold(self,vector,*,context):
        if self.active or vector!=self.vector or context is not self.context:
            raise PublicationError('Exact nonreentrant complete pin interval required')
        # Admission and actual native locks precede any parity reuse. No retained
        # boolean or prior process result can open this interval.
        with self.pins.hold(vector,context=context) as admitted:
            if admitted!=vector:raise PublicationError('Complete native pin hold did not yield its original vector')
            self.active=True;self.original=None;self.poisoned=False
            try:yield vector
            finally:self.active=False;self.original=None;self.poisoned=False

    def validate_descriptor(self,descriptor,context):
        if not self.active or self.poisoned or context is not self.context:
            raise PublicationError('Live admitted native pin interval required')
        try:
            if self.validator._targets_json!=self.inventory:raise PublicationError('Original parity expectations changed')
            bind_manifest_pins(descriptor,self.vector,{t:pair[0] for t,pair in self.vector.targets.items()},authority=self.vector.authority)
            if self.original is None:
                self.validator.validate_descriptor(descriptor,context)
                self.original=descriptor
            else:
                if descriptor!=self.original:raise PublicationError('Descriptor changed within original pin interval')
                self.validator._renew_pinned_descriptor(descriptor,context)
        except BaseException:
            # A caught refusal must never reactivate cached parity in this hold.
            self.poisoned=True;self.original=None
            raise
