import unittest
from types import SimpleNamespace

from ashlar_host.managed_current_person_read import (
    ManagedCurrentPersonPageOwner, ManagedCurrentPersonPolicy,
)


class _Policy(ManagedCurrentPersonPolicy):
    def __init__(self):
        self.closed = 0
        self.pages = 0

    def open(self, **kwargs):
        return "a" * 64

    def renew(self, **kwargs):
        return "a" * 64

    def authorize_page(self, **kwargs):
        self.pages += 1

    def authorize_rows(self, **kwargs):
        return None

    def close(self, **kwargs):
        self.closed += 1


class _Frame:
    schema = SimpleNamespace(fields=[])

    def __init__(self, count):
        self.count = count
        self.bound = None

    def limit(self, bound):
        self.bound = bound
        return self

    def collect(self):
        return [SimpleNamespace(asDict=lambda recursive: {}) for _ in range(self.count)]


class PersonPageOwnerTests(unittest.TestCase):
    def test_policy_port_is_mandatory(self):
        with self.assertRaises(TypeError):
            ManagedCurrentPersonPolicy()

    def test_hold_closes_policy_even_when_body_fails(self):
        owner = object.__new__(ManagedCurrentPersonPageOwner)
        owner.context, owner.vector, owner.active = object(), object(), False
        owner.principal, owner.person_policy = "person@example.test", _Policy()
        owner.receipt = {"manifest": {"publication_id": "pub"}}
        owner.policy_revision = "a" * 64
        owner._renew = lambda: None
        with self.assertRaisesRegex(RuntimeError, "body failed"):
            with owner.hold(owner.vector, context=owner.context):
                raise RuntimeError("body failed")
        self.assertFalse(owner.active)
        self.assertEqual(owner.person_policy.closed, 1)
        with owner.hold(owner.vector, context=owner.context):
            pass
        self.assertEqual(owner.person_policy.closed, 2)
        owner.person_policy.open = lambda **kwargs: "b" * 64
        with self.assertRaises(ValueError):
            with owner.hold(owner.vector, context=owner.context):
                self.fail("changed policy must not enter held page")
        self.assertFalse(owner.active)
        self.assertEqual(owner.person_policy.closed, 3)

    def test_only_exact_compiler_sql_is_bounded_before_collect(self):
        owner = object.__new__(ManagedCurrentPersonPageOwner)
        owner._renew = lambda: None
        owner.current_artifact = {"sql": "SELECT exact", "parameters": [], "obligations": []}
        owner.maximum_rows = 2
        frame = _Frame(3)
        owner.spark = SimpleNamespace(sql=lambda sql, args: frame)
        with self.assertRaises(ValueError):
            owner.query("SELECT exact", {})
        self.assertEqual(frame.bound, 3)
        with self.assertRaises(ValueError):
            owner.query("SELECT different", {})

    def test_backend03_is_refused_before_policy_or_execution(self):
        owner = object.__new__(ManagedCurrentPersonPageOwner)
        owner.person_policy, owner.principal = _Policy(), "person@example.test"
        owner.receipt = {"manifest": {"publication_id": "pub"}}
        with self.assertRaises(ValueError):
            owner.admit_artifact({"target": {"backendVersion": "backend03"}},
                                 {}, None, None)
        self.assertEqual(owner.person_policy.pages, 0)


if __name__ == "__main__":
    unittest.main()
