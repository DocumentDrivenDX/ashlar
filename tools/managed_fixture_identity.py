"""Qualification-only ephemeral UC fixture identity; secret never leaves memory.

No business resource enrollment. Callers grant only their independently owned
fixture-table privileges; search fixtures must remain SELECT-only.
"""

from contextlib import contextmanager
from dataclasses import dataclass, field

from databricks.sdk.core import Config
from databricks.sdk.service import iam, sql


@dataclass(frozen=True)
class OwnedManagedIdentity:
    principal_id: str
    application_id: str
    config: Config = field(repr=False)


class OwnedIdentityCleanupFailed(RuntimeError):
    def __init__(self, originals):
        self.originals = tuple(originals)
        super().__init__(
            "Owned native fixture identity cleanup failed; original failures retained"
        )


@contextmanager
def temporary_managed_identity(admin, *, warehouse_id, display_name):
    principal = credential = None
    body_error = None
    try:
        principal = admin.service_principals.create(
            active=True,
            display_name=display_name,
            entitlements=[iam.ComplexValue(value="databricks-sql-access")],
        )
        if not principal.id or not principal.application_id or principal.roles:
            raise RuntimeError("Original fresh non-admin fixture identity required")
        admin.warehouses.update_permissions(
            warehouse_id,
            access_control_list=[
                sql.WarehouseAccessControlRequest(
                    service_principal_name=principal.application_id,
                    permission_level=sql.WarehousePermissionLevel.CAN_USE,
                )
            ],
        )
        credential = admin.service_principal_secrets_proxy.create(
            principal.id, lifetime="3600s"
        )
        if not credential.id or not credential.secret:
            raise RuntimeError("Original one-hour in-memory credential required")
        config = Config(
            host=admin.config.host,
            client_id=principal.application_id,
            client_secret=credential.secret,
            auth_type="oauth-m2m",
            config_file="/dev/null",
        )
        yield OwnedManagedIdentity(principal.id, principal.application_id, config)
    except BaseException as error:
        body_error = error
        raise
    finally:
        errors = []
        if credential is not None and principal is not None:
            try:
                admin.service_principal_secrets_proxy.delete(
                    principal.id, credential.id
                )
            except Exception as error:  # noqa: BLE001 - collect and rethrow every cleanup failure after both cleanup attempts
                errors.append(error)
        if principal is not None:
            try:
                admin.service_principals.delete(principal.id)
            except Exception as error:  # noqa: BLE001 - collect and rethrow every cleanup failure after both cleanup attempts
                errors.append(error)
        credential = None
        if errors:
            # Preserve original body failure and all cleanup failures; none are
            # serialized or logged here because their context may carry secrets.
            if body_error is not None:
                errors.insert(0, body_error)
            raise OwnedIdentityCleanupFailed(errors) from body_error
