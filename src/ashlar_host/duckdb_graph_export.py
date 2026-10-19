# SPDX-License-Identifier: MIT OR Apache-2.0
"""Finite engine-local queries of immutable local export carriers; no live authority."""
import json,os,hashlib
from collections import Counter,defaultdict
from pathlib import Path
from ashlar.graph_release import GraphRelease,release_columns
from .graph_export_diff import parse_graph_export_selection
from .private_graph_custody import decode_custody_bytes
from .local_graph_export import open_local_graph_export
from .evolution_cli import InvocationFile
from .lifecycle import owned_context,finish


def query_duckdb_export(connection,release,maximum_hops=1):
    if type(release)is not GraphRelease or type(maximum_hops)is not int or maximum_hops not in (1,2):raise ValueError('Exact release and one/two hops required')
    if hashlib.sha256(release.payload).hexdigest()!=release.sha256:raise ValueError('Original release digest differs')
    value=decode_custody_bytes(release.payload);nodes=value['nodes'];edges=value['edges']
    for kind,rows in (('node',nodes),('edge',edges)):
        columns=release_columns(kind)
        if any(type(r)is not dict or set(r)!=set(columns)or any(type(c)is not str and c is not None for c in r.values())for r in rows):raise ValueError('Full canonical STRING/null carrier inventory required')
    if len(nodes)>10000 or len(edges)>10000:raise ValueError('Finite graph required')
    ids=[r['graph_id']for r in nodes];edge_ids=[r['graph_id']for r in edges]
    set_ids=set(ids)
    if len(set_ids)!=len(ids)or len(set(edge_ids))!=len(edge_ids)or any(e['src']not in set_ids or e['dst']not in set_ids for e in edges):raise ValueError('Canonical unique identities/endpoints required')
    def select(sql):
        rows=connection.execute(sql+' LIMIT 10001').fetchall()
        if len(rows)>10000:raise ValueError('Finite query result exceeded')
        return [list(row)for row in rows]
    def same(rows,expected):
        if Counter(tuple(r)for r in rows)!=Counter(tuple(r)for r in expected):raise ValueError('Complete original graph bag mismatch')
    created=[];primary=None;result=None
    try:
        for name,kind,rows in (('ashlar_export_nodes','node',nodes),('ashlar_export_edges','edge',edges)):
            columns=release_columns(kind)
            connection.execute('CREATE TEMP TABLE '+name+' ('+','.join('"'+c+'" VARCHAR'for c in columns)+')');created.append(name)
            if rows:connection.executemany('INSERT INTO '+name+' VALUES ('+','.join('?'for _ in columns)+')',[[r[c]for c in columns]for r in rows])
            actual=select('SELECT * FROM '+name);same(actual,[[r[c]for c in columns]for r in rows])
        one=[[e['src'],e['graph_id'],e['dst']]for e in edges]
        actual=select('SELECT src,graph_id,dst FROM ashlar_export_edges');same(actual,one)
        degree_counts=Counter(e['src']for e in edges)
        degree=[[identity,degree_counts[identity]]for identity in ids]
        degrees=select('SELECT n.graph_id,COUNT(e.graph_id) FROM ashlar_export_nodes n LEFT JOIN ashlar_export_edges e ON n.graph_id=e.src GROUP BY n.graph_id');same(degrees,degree)
        native_version=connection.execute('SELECT version()').fetchone()[0]
        if type(native_version)is not str or not native_version:raise ValueError('Native DuckDB version required')
        result={'engine':{'name':'DuckDB','version':native_version},'publication_id':value['publication']['publication_id'],'nodes':nodes,'edges':edges,'one_hop':actual,'out_degrees':degrees,'maximum_hops':maximum_hops,
            'qualification':'DuckDB queries of selected immutable local STRING/null carriers; no current source, native Delta, Weft or remote graph authority.'}
        if maximum_hops==2:
            expected=[];adjacency=defaultdict(list);path_counts=Counter();destinations=defaultdict(set)
            for edge in edges:adjacency[edge['src']].append(edge)
            for a in edges:
                for b in adjacency[a['dst']]:
                    if len(expected)>=10000:raise ValueError('Finite two-hop paths exceeded')
                    expected.append([a['src'],a['graph_id'],a['dst'],b['graph_id'],b['dst']])
                    path_counts[a['src']]+=1;destinations[a['src']].add(b['dst'])
            paths=select('SELECT a.src,a.graph_id,a.dst,b.graph_id,b.dst FROM ashlar_export_edges a JOIN ashlar_export_edges b ON a.dst=b.src');same(paths,expected)
            counts=[[identity,path_counts[identity],len(destinations[identity])]for identity in ids]
            actual_counts=select('SELECT n.graph_id,COUNT(b.graph_id),COUNT(DISTINCT b.dst) FROM ashlar_export_nodes n LEFT JOIN ashlar_export_edges a ON n.graph_id=a.src LEFT JOIN ashlar_export_edges b ON a.dst=b.src GROUP BY n.graph_id');same(actual_counts,counts)
            result.update(two_hop=paths,two_hop_counts=actual_counts)
    except BaseException as error:primary=error
    finish(primary,[lambda name=name:connection.execute('DROP TABLE '+name)for name in reversed(created)])
    return result


def run_query_duckdb_export_command(configuration,output,maximum_hops=1):
    if type(maximum_hops)is not int or maximum_hops not in (1,2):raise ValueError('One/two hops required')
    config=InvocationFile.read(configuration,65536);selection=parse_graph_export_selection(config.raw);destination=Path(output)
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()or destination.exists()or destination.is_symlink():raise ValueError('Fresh canonical report required')
    if any(destination==p or p in destination.parents or destination in p.parents for p in (config.path,selection.release_path,selection.custody_path)):raise ValueError('Separate output required')
    import duckdb
    primary=None;result=None
    try:
        config.renew(65536)
        with owned_context(open_local_graph_export(selection))as owner:
            with owned_context(owner.writer(owner.context)):
                admit=lambda:owner.admit_original_export(owner.release,owner.custody_bytes,owner.source,owner.context)
                connection=duckdb.connect(':memory:',config={'threads':'1','memory_limit':'64MB','enable_external_access':'false'})
                error=None
                try:admit();result=query_duckdb_export(connection,owner.release,maximum_hops)
                except BaseException as caught:error=caught
                finish(error,[admit,connection.close])
    except BaseException as error:primary=error
    finish(primary,[lambda:config.renew(65536)])
    raw=bytearray()
    for piece in json.JSONEncoder(ensure_ascii=False,allow_nan=False,separators=(',',':')).iterencode(result):
        block=piece.encode()
        if len(raw)+len(block)+1>16777216:raise ValueError('Report exceeds16MiB')
        raw.extend(block)
    raw.extend(b'\n')
    fd=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(raw)!=len(raw):raise OSError('Complete query report write required')
        stream.flush();os.fsync(stream.fileno())
    return result
