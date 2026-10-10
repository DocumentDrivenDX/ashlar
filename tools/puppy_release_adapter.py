"""Public native PuppyGraph staging/query adapter on supplied ordinary transports.

No credentials, engine creation or capacity management. Callers own authenticated
HTTP/Cypher transports and actual runtime inspection, all local recipe reviewed
separately. Native schema/query bytes are retained before interpretation.
"""
import hashlib
import json
from pathlib import Path
from ashlar_host.puppy_release import PuppyReleaseActivation, PuppyReleaseError
from run_graph_release_graphframes import columns


def decode(raw):
    if type(raw) is not bytes or len(raw) > 16 * 1024 * 1024:
        raise PuppyReleaseError('Bounded native schema response required')
    def obj(pairs):
        value = {}
        for key, item in pairs:
            if key in value: raise PuppyReleaseError('Duplicate native schema member')
            value[key] = item
        return value
    return json.loads(raw.decode(), object_pairs_hook=obj,
        parse_constant=lambda value: (_ for _ in ()).throw(PuppyReleaseError('Nonfinite native JSON')))


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def original_model(prior, addition):
    """Merge exact original additions; existing identical rows are replay only."""
    if (type(prior) is not dict or type(addition) is not dict
            or set(prior) != {'catalog', 'node', 'edge'} or set(addition) != set(prior)):
        raise PuppyReleaseError('Complete independently supplied original schema required')
    merged = {}
    for section, key in [('catalog', 'name'), ('node', 'label'), ('edge', 'label')]:
        entries = {}
        for value in (prior, addition):
            rows = value[section]
            if type(rows) is not list:
                raise PuppyReleaseError('Exact original schema inventory required')
            seen = set()
            for row in rows:
                if type(row) is not dict or type(row.get(key)) is not str or row[key] in seen:
                    raise PuppyReleaseError('Duplicate or malformed original schema identity')
                seen.add(row[key])
                if row[key] in entries and encoded(entries[row[key]]) != encoded(row):
                    raise PuppyReleaseError('Original schema identity conflict')
                entries[row[key]] = row
        merged[section] = list(entries.values())
    return merged


def admit_original_subset(active, prior, addition):
    """Native absence grants nothing: authorize only an exact original intent.

    Every prior row must remain; the only extra visible rows permitted are exact
    members of the independently pinned original addition. Missing catalogs are
    reported as unobservable, never inferred from the local request.
    """
    merged = original_model(prior, addition)
    if type(active) is dict and active == {}:
        # Public native unconfigured shape, not an observed empty inventory.
        # Original empty prior and independently supplied writer/source policy
        # authorize first installation; absence never authorizes prior repair.
        if all(type(prior[section]) is list and prior[section] == []
               for section in ('catalog', 'node', 'edge')):
            return merged, False
        raise PuppyReleaseError('Unconfigured native schema refuses retained prior')
    if type(active) is not dict or not {'node', 'edge'}.issubset(active):
        raise PuppyReleaseError('Complete native label inventory required')
    complete = True
    for section, key in [('catalog', 'name'), ('node', 'label'), ('edge', 'label')]:
        if section == 'catalog' and section not in active:
            continue
        rows = active[section]
        if type(rows) is not list:
            raise PuppyReleaseError('Exact observed native schema inventory required')
        observed = {}
        allowed = {row[key]: row for row in merged[section]}
        for row in rows:
            if (type(row) is not dict or type(row.get(key)) is not str or row[key] in observed
                    or row[key] not in allowed or encoded(row) != encoded(allowed[row[key]])):
                raise PuppyReleaseError('Unknown or changed native schema refuses original resume')
            observed[row[key]] = row
        if any(row[key] not in observed for row in prior[section]):
            raise PuppyReleaseError('Original prior native schema disappeared')
        if any(row[key] not in observed for row in addition[section]):
            complete = False
    return merged, complete


class PuppyNativeReleaseAdapter:
    def __init__(self, *, prepared, prior_model, http, runtime, evidence,
                 cypher=None, projection=None):
        """Explicit prepared SHA-keyed files and exact independently pinned model.

        http(path,data,context)->bytes executes /schemajson or /schema.
        cypher(query,context)->list executes real release-qualified native query.
        runtime(context)->(engine_instance_identity, exact_engine_version) uses
        actual independently pinned runtime inspection; never a local counter.
        Optional projection(kind,label,context,retain)->(query,rows) replaces only the
        query rendering/transport; identical complete observation gates apply.
        """
        if (type(prepared) is not dict or type(prior_model) is not bytes
                or any(not callable(port) for port in (http, runtime))
                or (projection is None and not callable(cypher))
                or (projection is not None and (not callable(projection) or cypher is not None))):
            raise PuppyReleaseError('Explicit prepared carriers and public native transports required')
        self.prepared = {}
        for digest, selected in prepared.items():
            if (type(digest) is not str or len(digest) != 64
                    or type(selected) is not tuple or len(selected) != 4
                    or type(selected[3]) is not str or len(selected[3]) != 64
                    or any(c not in "0123456789abcdef" for c in digest + selected[3])):
                raise PuppyReleaseError("Externally trusted prepared receipt pins required")
            self.prepared[digest] = tuple(str(Path(path)) for path in selected[:3]) + (selected[3],)
        self.model = prior_model
        self.http = http; self.cypher = cypher; self.runtime = runtime
        self.projection = projection
        self.evidence = Path(evidence)
        self.evidence.mkdir(mode=0o700, exist_ok=False)
        self.ordinal = 0
        decode(prior_model)

    def retain(self, name, raw):
        if type(raw) is not bytes or len(raw) > 16 * 1024 * 1024:
            raise PuppyReleaseError('Bounded native observation required')
        self.ordinal += 1
        with (self.evidence / (str(self.ordinal).zfill(4) + '-' + name)).open('xb') as target:
            target.write(raw)
        return raw

    def carrier(self, handle):
        selected = self.prepared.get(handle.release.sha256)
        if type(selected) is not tuple or len(selected) != 4:
            raise PuppyReleaseError('Exact prepared carrier/model/receipt location required')
        database, model_path, receipt_path = map(Path, selected[:3])
        for path in (database, model_path, receipt_path):
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 32 * 1024 * 1024:
                raise PuppyReleaseError('Bounded original regular carrier inputs required')
        receipt_bytes = receipt_path.read_bytes()
        if hashlib.sha256(receipt_bytes).hexdigest() != selected[3]:
            raise PuppyReleaseError("Externally pinned original carrier receipt changed")
        receipt = decode(receipt_bytes); model = model_path.read_bytes()
        if (receipt['release_sha256'] != handle.release.sha256
                or hashlib.sha256(model).hexdigest() != receipt['model_sha256']
                or hashlib.sha256(database.read_bytes()).hexdigest() != receipt['database_sha256']):
            raise PuppyReleaseError('Original carrier bytes changed')
        return receipt, decode(model)

    def schema(self, handle, context):
        raw = self.retain('schema.json', self.http('/schemajson', None, context))
        active = decode(raw)
        subset = {'node': [], 'edge': []}
        receipt, model = self.carrier(handle)
        visibility = 'present' if 'catalog' in active else 'omitted'
        sections = [('node', 'label'), ('edge', 'label')]
        if visibility == 'present': sections.insert(0, ('catalog', 'name'))
        for section, key in sections:
            if type(active.get(section)) is not list or any(type(row) is not dict for row in active[section]):
                raise PuppyReleaseError('Exact native schema inventory required')
            wanted = model[section][0][key]
            found = [row for row in active[section] if row.get(key) == wanted]
            if len(found) != 1: raise PuppyReleaseError('Complete immutable native schema absent')
            subset[section] = found
            if encoded(found) != encoded(model[section]):
                raise PuppyReleaseError('Native schema mapping differs from exact prepared model')
        return receipt, encoded(subset), visibility

    def stage(self, handle, context):
        receipt, addition = self.carrier(handle)
        before = decode(self.retain('before.json', self.http('/schemajson', None, context)))
        prior = decode(self.model)
        merged, complete = admit_original_subset(before, prior, addition)
        request = encoded(merged)
        # Exact intent exists before any actual mutation, including resumed
        # original staged subsets. No caller-selected replacement repair exists.
        self.retain('request.json', request)
        if not complete:
            outcome = decode(self.retain('upload.json', self.http('/schema?postUploadBehavior=none', request, context)))
            if outcome.get('ok') is not True:
                raise PuppyReleaseError('Native activation did not complete')
        receipt, schema, visibility = self.schema(handle, context)
        identity, version = self.runtime(context)
        if type(identity) is not str or not identity or type(version) is not str or not version:
            raise PuppyReleaseError('Actual pinned native runtime identity required')
        self.model = request
        return PuppyReleaseActivation(handle.release.sha256, identity,
            hashlib.sha256(schema).hexdigest(), receipt['database_sha256'], visibility)

    def observe(self, handle, context):
        receipt, schema, visibility = self.schema(handle, context)
        identity, version = self.runtime(context)
        actual = {'engine': 'PuppyGraph', 'version': version, 'engine_id': identity,
            'schema_json': schema.decode(), 'carrier_sha256': receipt['database_sha256'],
            'catalog_visibility': visibility, 'node_label': handle.node_label,
            'edge_label': handle.edge_label, 'nodes': [], 'edges': []}
        for kind, role, label in [('node', 'nodes', handle.node_label), ('edge', 'edges', handle.edge_label)]:
            names = columns(kind)
            if self.projection is None:
                field = lambda name: 'carrier_id' if name == 'id' else 'carrier_key' if name == 'graph_id' else name
                pattern = '(x:' + label + ')' if kind == 'node' else '(s)-[x:' + label + ']->(t)'
                query = 'MATCH ' + pattern + ' RETURN ' + ','.join('x.' + field(n) + ' AS ' + n for n in names) + ',id(x) AS native_id'
                if kind == 'edge': query += ',id(s) AS native_source,id(t) AS native_target'
                rows = self.cypher(query, context)
            else:
                selected = self.projection(kind, label, context, self.retain)
                if type(selected) is not tuple or len(selected) != 2:
                    raise PuppyReleaseError('Exact public native projection required')
                query, rows = selected
                if type(query) is not str or len(query.encode()) > 65536:
                    raise PuppyReleaseError('Bounded original native query required')
            self.retain('query.json', encoded({'query': query, 'rows': rows}))
            if type(rows) is not list or len(rows) > 10000:
                raise PuppyReleaseError('Bounded complete native graph rows required')
            for row in rows:
                keys = {'native_id'} | ({'native_source', 'native_target'} if kind == 'edge' else set())
                if type(row) is not dict or set(row) != set(names) | keys:
                    raise PuppyReleaseError('Exact native row/identity projection required')
                actual[role].append({'row': {name: row[name] for name in names},
                    **{name: row[name] for name in keys}})
        _, closing_schema, closing_visibility = self.schema(handle, context)
        if closing_schema != schema or closing_visibility != visibility or self.runtime(context) != (identity, version):
            raise PuppyReleaseError('Native release schema/runtime changed during read')
        self.carrier(handle)
        return self.retain('observed.json', encoded(actual))
