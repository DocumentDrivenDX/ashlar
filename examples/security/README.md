# Review the isolated administrator boundary

[The proposal](client-dev-excluded-admin.proposal.json) is review data, not an
executable installation or an approved authority configuration. No current
publication command consumes it. The selected schema must be newly created;
an existing schema with that name must refuse rather than be reused or cleaned.

The concrete classification to review is: treat the existing `client_dev`
catalog owner `db-aidev-admins` as an excluded administrator for the disposable
`ashlar_security_current_query_20261015` test scope. This acknowledges its existing
ability to administer child privileges; it grants no new access and adds nobody
to ordinary data writers or query callers. It does not put a principal in a UC
policy `EXCEPT` clause. The tests cannot claim protection against this excluded
administrator.

The primary connection observed the exact catalog locator and a workspace SCIM
list entry with the proposed name and ID. Its detailed group read returned 403.
Resource type, account/workspace identity correspondence and transitive membership
remain unresolved. A caller must not be classified as ordinary from that denial,
from an absent membership response, or from a caller-supplied Subject string.
The catalog creation observation is not an immutable catalog UUID.

## Proposed provider API

Keep administrator classification separate from `trusted_writers`:

```python
boundary = operator_policy.require_excluded_administrator_boundary(
    original_proposal_bytes, authenticated_context
)
assessment = native_owner.assess_inventory(
    boundary=boundary,
    ordinary_writers=original_admitted_writer_identities,
    original_source=original_source_custody,
    observed_catalog=current_catalog,
    effective_privileges=current_complete_privilege_inventory,
    membership=current_complete_membership_inventory,
)
```

These are proposed interfaces, not callable implementations. The operator policy
must supply a separately admitted human classification bound to the original
proposal and exact scope. A JSON `approved` field, matching digest or this file
alone cannot construct that boundary. Pending classification or unresolved native
identity must refuse registration. The original source admission remains separate.

The assessment must preserve distinct ordinary-writer and excluded-administrator
inventories. It must refuse a changed catalog/owner/group, unknown privilege or
delegation origin, missing/incomplete membership, source or binding drift, and
opening/closing inventory drift. No automatic administrator-to-writer or
administrator-to-ordinary-caller conversion is permitted. The existing strict
writer path stays unchanged until this new boundary is implemented and approved.

Original Weft `ashlar.B14` remains required: actual privileged bypass and actual
ordinary inability to obtain it need separate native evidence. Classification or
a successful primary-connection query does not prove either result. An excluded
administrator member cannot serve as the independent ordinary-caller baseline.
