from contextlib import contextmanager
import unittest
import test_native_artifact_validation as native
from ashlar.manifest import manifest_pin_vector
from ashlar.publisher import PublicationError
from ashlar.effect_validation import EffectValidationError
from ashlar.protocol import ProtocolError
from pinned_artifact_validation import PinnedArtifactValidation

class Pins:
    def __init__(self):self.active=False;self.deny=False;self.fail_exit=False
    @contextmanager
    def hold(self,vector,*,context):
        if self.deny:raise PermissionError('Native pin admission denied')
        self.active=True
        try:
            yield vector
            if self.fail_exit:raise PermissionError('Closing native pin custody denied')
        finally:self.active=False

class PinnedValidationTests(unittest.TestCase):
    def setup(self):
        helper=native.NativeArtifactValidatorTests();_,_,d=helper.inputs();ex=native.NativeExecutor();p=native.Policy();g=native.Gate();pins=Pins();checks=[]
        def admission(descriptor,context):
            if not pins.active:raise PermissionError('Actual guard required')
            checks.append(descriptor)
        v=helper.validator(ex,p,g,admission)
        vector=manifest_pin_vector(dict(d.raw),{'c.s.object_current':'uuid'},authority='private')
        context='held';interval=PinnedArtifactValidation(pins,vector,v,context)
        return d,ex,p,g,pins,checks,interval,context
    def test_full_parity_once_and_fresh_admission_on_every_renewal(self):
        d,ex,p,g,pins,checks,v,c=self.setup()
        with self.assertRaises(PublicationError):v.validate_descriptor(d,c)
        with v.hold(v.vector,context=c):
            v.validate_descriptor(d,c);self.assertEqual(len(ex.calls),6)
            v.validate_descriptor(d,c);self.assertEqual(len(ex.calls),8)
            self.assertEqual((p.calls,g.calls,len(checks)),(4,4,2))
        self.assertFalse(pins.active)
        with self.assertRaises(PublicationError):v.validate_descriptor(d,c)
        ex.rows=[]
        with v.hold(v.vector,context=c):
            with self.assertRaises(EffectValidationError):v.validate_descriptor(d,c)
    def test_expiry_and_protocol_refusal_poison_the_interval(self):
        for mode in ('retention','protocol','source','expectations'):
            d,ex,p,g,pins,checks,v,c=self.setup()
            with v.hold(v.vector,context=c):
                v.validate_descriptor(d,c)
                if mode=='retention':g.denied=True
                if mode=='protocol':ex.feature='["future"]'
                if mode=='source':p.denied=True
                if mode=='expectations':v.validator._targets_json+=' '
                with self.assertRaises((PermissionError,ProtocolError,PublicationError)):v.validate_descriptor(d,c)
                g.denied=False;p.denied=False;ex.feature='[]';v.validator._targets_json=v.inventory
                before=len(ex.calls)
                with self.assertRaises(PublicationError):v.validate_descriptor(d,c)
                self.assertEqual(len(ex.calls),before)
    def test_pin_failure_reentrancy_and_closing_refusal_clear_reuse(self):
        d,ex,p,g,pins,checks,v,c=self.setup();pins.deny=True
        with self.assertRaises(PermissionError):
            with v.hold(v.vector,context=c):pass
        self.assertFalse(v.active);self.assertEqual(ex.calls,[])
        pins.deny=False;pins.fail_exit=True
        with self.assertRaises(PermissionError):
            with v.hold(v.vector,context=c):
                v.validate_descriptor(d,c)
                with self.assertRaises(PublicationError):
                    with v.hold(v.vector,context=c):pass
        self.assertFalse(v.active);self.assertIsNone(v.original)

if __name__=='__main__':unittest.main()
