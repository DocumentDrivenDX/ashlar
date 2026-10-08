"""Pure stable-ID planning. Native acceptance must serialize and revalidate it."""
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping, Sequence, Tuple

MAX_ID = 2**31 - 1
FAMILIES = ('type', 'property', 'relationship')

class CatalogPlanError(ValueError):
    pass

@dataclass(frozen=True, order=True)
class Identity:
    family: str
    # Authored, document-qualified identity; property includes owning Record.
    # Display names and document revision are never lineage components.
    parts: Tuple[str, ...]

    def validate(self):
        lengths = {'type': 3, 'property': 5, 'relationship': 4}
        if self.family not in lengths or not isinstance(self.parts, tuple) or len(self.parts) != lengths[self.family]:
            raise CatalogPlanError('Invalid qualified identity shape')
        if any(not isinstance(x,str) or not x or '\x00' in x for x in self.parts):
            raise CatalogPlanError('Invalid opaque authored identity')
        for part in self.parts:
            try: part.encode('utf-8')
            except UnicodeError as exc: raise CatalogPlanError('Invalid identity UTF8') from exc

@dataclass(frozen=True)
class MappingEntry:
    identity: Identity
    catalog_id: int
    active: bool

@dataclass(frozen=True)
class CatalogPlan:
    entries: Tuple[MappingEntry, ...]
    allocated: Tuple[MappingEntry, ...]
    retired: Tuple[MappingEntry, ...]
    reactivated: Tuple[MappingEntry, ...]
    highwater: Mapping[str, int]
    expected_head: str


def plan_catalog_ids(existing: Sequence[MappingEntry], desired: Sequence[Identity], *,
                     highwater: Mapping[str,int], expected_head: str) -> CatalogPlan:
    """Full selected catalog identity inventory, not a partial document patch.

    Inputs are a trusted complete locked-state snapshot and semantically admitted
    desired identities. This only plans IDs/lifecycle, never admits UMF meanings,
    restores privileges, rebinds data, persists or establishes accepted revision.
    """
    if not isinstance(expected_head,str) or not expected_head or not expected_head.isascii() or not expected_head.isdecimal():
        raise CatalogPlanError('Explicit native expected head required')
    water = dict(highwater)
    if set(water) != set(FAMILIES) or any(type(v) is not int or not 0<=v<=MAX_ID for v in water.values()):
        raise CatalogPlanError('Invalid complete allocation highwater')
    prior = {}; occupied = set()
    for entry in existing:
        entry.identity.validate()
        if type(entry.catalog_id) is not int or not 1<=entry.catalog_id<=water[entry.identity.family] or type(entry.active) is not bool:
            raise CatalogPlanError('Invalid prior allocation/lifecycle')
        native_key = (entry.identity.family,entry.catalog_id)
        if entry.identity in prior or native_key in occupied:
            raise CatalogPlanError('Duplicate identity or native allocation')
        prior[entry.identity] = entry
        occupied.add(native_key)
    wanted = set()
    for identity in desired:
        identity.validate()
        if identity in wanted:
            raise CatalogPlanError('Duplicate desired identity')
        wanted.add(identity)
    for identity in prior:
        if identity.family in ('property','relationship'):
            owner=Identity('type',identity.parts[:3])
            if owner not in prior:
                raise CatalogPlanError('Prior member missing complete owner identity')
    for identity in wanted:
        if identity.family in ('property','relationship'):
            owner=Identity('type',identity.parts[:3])
            if owner not in wanted:
                raise CatalogPlanError('Active member requires active owner')
    # Allocate deterministically by exact Unicode UTF8 byte tuples, never by name.
    order = lambda i:(i.family,tuple(x.encode('utf-8') for x in i.parts))
    allocated=[]; retired=[]; reactivated=[]; result=[]
    for identity in sorted(set(prior)|wanted,key=order):
        previous = prior.get(identity)
        active = identity in wanted
        if previous is None:
            if water[identity.family] == MAX_ID:
                raise CatalogPlanError('Catalog identifier exhaustion')
            water[identity.family] += 1
            entry = MappingEntry(identity,water[identity.family],True)
            allocated.append(entry)
        else:
            entry = MappingEntry(identity,previous.catalog_id,active)
            if previous.active and not active: retired.append(entry)
            if not previous.active and active: reactivated.append(entry)
        result.append(entry)
    # Preserve reservations for every retired identity. Grants are deliberately
    # absent: reactivation is not restoration of old authorization.
    return CatalogPlan(tuple(result),tuple(allocated),tuple(retired),tuple(reactivated),
                       MappingProxyType(water),expected_head)
