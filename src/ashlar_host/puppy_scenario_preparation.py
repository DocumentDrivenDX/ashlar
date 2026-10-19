# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original17 analytic preparation; no engine, publication or source authority."""
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import os
import stat
from .finite_dataset import FinitePackSource
from .finite_pack import FinitePackDefinition
from .graphframes_pack_scenarios import prepare_scenarios, bag
from .local_graph_export import LocalGraphExportUse
from .private_graph_custody import decode_custody_bytes
from .lifecycle import finish, owned_context

PROFILE = 'ashlar-original17-puppygraph-preparation/0.1'
NAMES = ('archaeology', 'ecology')

@dataclass(frozen=True)
class PuppyScenarioPreparationConfig:
    """Original pack bytes in fixed order and one fresh private destination."""
    pack_bytes: tuple[bytes, bytes]
    output: Path

    def __post_init__(self):
        if (type(self.pack_bytes) is not tuple or len(self.pack_bytes) != 2
                or any(type(b) is not bytes or not 0 < len(b) <= 1048576 for b in self.pack_bytes)
                or not isinstance(self.output, Path) or not self.output.is_absolute()
                or self.output.parent.resolve() != self.output.parent):
            raise ValueError('Exact two original packs and canonical output required')


def project_puppy_scenario_pack(name: str, model: bytes, graph: bytes, pack: bytes, release: bytes) -> dict:
    """Pure original-byte projection, not semantic or file-use admission."""
    if type(name) is not str or name not in NAMES or type(release) is not bytes or not 0 < len(release) <= 16777216:
        raise ValueError('Named bounded original scenario release required')
    definition = FinitePackDefinition(name)
    profile = prepare_scenarios(definition, model, graph, pack, decode_custody_bytes(release))
    _, bindings = definition.build(model, graph)
    from .pack_scenario_queries import entries
    nodes, edges, longs = entries(profile, bindings)
    if len(nodes) != (39 if name == 'archaeology' else 40) or len(edges) != (46 if name == 'archaeology' else 54):
        raise ValueError('Complete original scenario population required')
    return {'profile': profile, 'nodes': nodes, 'edges': edges, 'long_columns': sorted(longs)}


def _encoded(value):
    chunks = []
    size = 1
    for part in json.JSONEncoder(sort_keys=True,ensure_ascii=False,allow_nan=False).iterencode(value):
        block = part.encode('utf8'); size += len(block)
        if size > 16777216: raise ValueError('Finite preparation receipt bound exceeded')
        chunks.append(block)
    return b''.join(chunks)+b'\n'


def prepare_puppy_scenarios(config: PuppyScenarioPreparationConfig, *,
                            sources: tuple[FinitePackSource, FinitePackSource],
                            exports: tuple[LocalGraphExportUse, LocalGraphExportUse]) -> dict:
    """Prepare four exact readonly tables while authentic source/file owners renew."""
    if type(config) is not PuppyScenarioPreparationConfig:
        raise ValueError('Typed original17 preparation configuration required')
    config.__post_init__()
    if (type(sources) is not tuple or type(exports) is not tuple or len(sources) != 2 or len(exports) != 2
            or any(type(s) is not FinitePackSource for s in sources)
            or any(type(e) is not LocalGraphExportUse for e in exports)):
        raise ValueError('Two authentic original source and export owners required')
    for name, source, export, pack in zip(NAMES, sources, exports, config.pack_bytes):
        if source.definition.name != name or source.context is not export.context:
            raise ValueError('Ordered original pack owners/context required')
        source.definition.inputs(source.model, source.graph, pack)
    parent = config.output.parent
    info = parent.lstat()
    if info.st_uid != os.getuid() or not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700 or config.output.exists() or config.output.is_symlink():
        raise ValueError('Fresh output under owned private parent required')
    metadata = [source.metadata() for source in sources]
    def renew():
        def parent_same():
            named = parent.lstat()
            if (parent.resolve() != parent or (named.st_dev,named.st_ino,named.st_uid,named.st_mode) !=
                    (info.st_dev,info.st_ino,info.st_uid,info.st_mode)):
                raise PermissionError('Original private preparation parent changed')
        def source_same(source, original):
            if source.metadata() != original:
                raise PermissionError('Original source admission changed')
        callbacks = [parent_same]
        for source, export, original in zip(sources, exports, metadata):
            callbacks.append(lambda source=source, original=original: source_same(source, original))
            callbacks.append(lambda export=export: export.admit_original_export(
                export.release, export.custody_bytes, export.source, export.context))
        finish(None, callbacks)
    answer = None
    with ExitStack() as stack:
        for export in exports:
            stack.enter_context(owned_context(export.writer(export.context)))
        primary = None
        try:
            renew()
            projected = [project_puppy_scenario_pack(name, source.model, source.graph, pack, export.release.payload)
                         for name, source, pack, export in zip(NAMES, sources, config.pack_bytes, exports)]
            import duckdb
            config.output.mkdir(mode=0o700)
            database = config.output/'scenarios.duckdb'
            connection = duckdb.connect(str(database))
            failure = None
            model = {'catalog': [{'name': 'ashlar_scenarios', 'type': 'duckdb',
                                  'jdbc': {'jdbcUri': 'jdbc:duckdb:/tmp/scenarios.duckdb'}}], 'node': [], 'edge': []}
            observations = []
            try:
                connection.execute('SET threads=1'); connection.execute("SET memory_limit='64MB'")
                connection.execute('BEGIN TRANSACTION'); connection.execute('CREATE SCHEMA carrier')
                for name, item in zip(NAMES, projected):
                    label = 'Scenario'+name.title()
                    for kind in ('node', 'edge'):
                        rows = item[kind+'s']; columns = list(rows[0]); longs = set(item['long_columns']) if kind == 'node' else set()
                        table = name+'_'+kind+'s'
                        schema = {n: 'BIGINT' if n in longs else 'VARCHAR' for n in columns}
                        connection.execute('CREATE TABLE carrier.'+table+' ('+','.join('"'+n+'" '+schema[n] for n in columns)+')')
                        connection.executemany('INSERT INTO carrier.'+table+' VALUES ('+','.join('?' for _ in columns)+')', [[r[n] for n in columns] for r in rows])
                        actual = [dict(zip(columns, r)) for r in connection.execute('SELECT * FROM carrier.'+table).fetchall()]
                        if bag(actual) != bag(rows): raise ValueError('Complete prepared analytic cell bag differs')
                        attrs = [n for n in columns if n not in ('id','graph_id')]; key = kind+'_key'
                        entry = {'label': label+kind.title(), 'dataSourceGroup': {'externalDataSource': {'enabled': True,
                            'catalog': 'ashlar_scenarios', 'schema': 'carrier', 'table': table,
                            'mappedField': [{'sourceFieldName': n, 'targetFieldName': n} for n in attrs]+[{'sourceFieldName':'graph_id','targetFieldName':key}]}},
                            'id':[{'name':key,'type':'STRING'}], 'attribute':[{'name':n,'type':'LONG' if n in longs else 'STRING'} for n in attrs]}
                        if kind == 'edge': entry.update(fromNodeLabel=label+'Node',toNodeLabel=label+'Node',fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
                        model[kind].append(entry); observations.append({'table':table,'rows':len(rows),'schema':schema,'full_cell_parity':True})
                renew(); connection.execute('COMMIT'); connection.execute('CHECKPOINT')
            except BaseException as error: failure = error
            finish(failure, [connection.close])
            database.chmod(0o444)
            if not 0 < database.stat().st_size <= 33554432:
                raise ValueError('Finite prepared database bound exceeded')
            model_raw = _encoded(model)
            answer = {'model_sha256':hashlib.sha256(model_raw).hexdigest(),
                      'model_bytes':len(model_raw), 'format':PROFILE,'reports':projected,'observations':observations,
                      'source_admissions':metadata,'release_sha256s':[e.release.sha256 for e in exports],
                      'custody_sha256s':[e.custody_sha256 for e in exports],
                      'database_sha256':hashlib.sha256(database.read_bytes()).hexdigest(),
                      'database_bytes':database.stat().st_size,'duckdb_version':duckdb.__version__,
                      'engine_executed':False,'qualification':'Original17 preparation only; no native or publication authority.'}
            for name, value in (('model.json',model),('receipt.json',answer)):
                raw = model_raw if name == 'model.json' else _encoded(value)
                fd=os.open(config.output/name,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
                stream=os.fdopen(fd,'wb'); failure=None
                try:
                    if stream.write(raw)!=len(raw):raise OSError('Complete preparation bytes required')
                except BaseException as error:failure=error
                finish(failure,[stream.close]);(config.output/name).chmod(0o444)
        except BaseException as error: primary = error
        finish(primary,[renew])
    return answer
