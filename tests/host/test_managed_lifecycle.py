"""Custody control selftests; these deliberately do not qualify native authority."""

from time import monotonic

import pytest
from umf_access.protocols import ActionRefused

from ashlar_host.managed_lifecycle import (
    ManagedDeploymentLifecycle,
    ManagedLifecycleEnrollment,
)


class Connection:
    autocommit = True

    def close(self):
        pass


class TransportControl(ManagedDeploymentLifecycle):
    def __init__(self):
        super().__init__(
            lambda **_: None,
            ManagedLifecycleEnrollment(
                ("c", "s", "lease"), "id", "store", "epoch", "coordinator"
            ),
        )
        self.token = None
        self.failure = None
        self.calls = []

    def _connection(self, deadline):
        return Connection()

    def _query(self, connection, statement, parameters, deadline):
        self.calls.append(statement)
        if self.failure == statement:
            raise self.original
        if statement.startswith("SELECT store"):
            return [("store", "epoch", self.token)]
        if "SET token=:token" in statement:
            self.token = parameters["token"]
        if "SET token=NULL" in statement:
            self.token = None
        return []


def test_success_releases_only_original_token_after_body():
    transport = TransportControl()
    with transport._hold("action", deadline=monotonic() + 10) as original:
        assert transport.token == original.token
        assert original.committed and not original.released
    assert original.released and transport.pending is None
    assert transport.token is None
    assert transport.calls.count("COMMIT") == 2


def test_unknown_commit_retains_original_and_blocks_any_fresh_operation():
    transport = TransportControl()
    transport.failure = "COMMIT"
    transport.original = OSError("original response lost")
    with (
        pytest.raises(OSError) as failure,
        transport._hold("action", deadline=monotonic() + 10),
    ):
        pytest.fail("uncertain acquisition must not enter business body")
    assert failure.value is transport.original
    original = transport.pending
    assert original.original_error is transport.original
    assert transport.token == original.token
    count = len(transport.calls)
    with (
        pytest.raises(
            ActionRefused, match="ORIGINAL_NATIVE_LIFECYCLE_RECOVERY_REQUIRED"
        ),
        transport._hold("deployment", deadline=monotonic() + 10),
    ):
        pytest.fail("fresh deployment must be fenced")
    assert len(transport.calls) == count
    assert transport.pending is original


def test_uncertain_business_body_never_unlocks_or_expires_lease():
    transport = TransportControl()
    original_error = OSError("business settlement uncertain")
    with (
        pytest.raises(OSError) as failure,
        transport._hold("action", deadline=monotonic() + 10) as original,
    ):
        raise original_error
    assert failure.value is original_error
    assert transport.pending is original
    assert transport.token == original.token
    assert original.original_error is original_error
    assert not original.released
    assert transport.calls.count("COMMIT") == 1


def test_acquisition_loss_recovers_only_same_original_visible_token_without_business():
    transport = TransportControl()
    transport.failure = "COMMIT"
    transport.original = OSError("original acquisition acknowledgement lost")
    with pytest.raises(OSError), transport._hold("action", deadline=monotonic() + 10):
        pytest.fail("unknown acquisition cannot dispatch")
    original = transport.pending
    assert not original.body_entered
    transport.failure = None
    assert (
        transport.recover_unused_or_completed(original, deadline=monotonic() + 10)
        is original
    )
    assert original.released and transport.pending is None and transport.token is None


def test_uncertain_business_cannot_be_released_by_lease_recovery():
    transport = TransportControl()
    with pytest.raises(OSError), transport._hold("action", deadline=monotonic() + 10):
        raise OSError("original business uncertainty")
    original = transport.pending
    count = len(transport.calls)
    with pytest.raises(ActionRefused, match="ORIGINAL_BUSINESS_SETTLEMENT_REQUIRED"):
        transport.recover_unused_or_completed(original, deadline=monotonic() + 10)
    assert len(transport.calls) == count
    assert transport.pending is original and transport.token == original.token
