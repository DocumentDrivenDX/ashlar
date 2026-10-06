"""Experimental strict native row-commit resolver; caller enforces pending-publication barrier."""
def resolve(client,table,snapshot,feed,epoch,position,expected_count):
 # Table identifiers are trusted fixture/config inputs; string origins are escaped SQL literals.
 quote=lambda x:"'"+x.replace("'","''")+"'"
 rows=client.sql('strict-row-commit-'+table.split('.')[-1],f"SELECT count(*),count(_metadata.row_commit_version),min(_metadata.row_commit_version),max(_metadata.row_commit_version),count(DISTINCT _metadata.row_commit_version) FROM {table} VERSION AS OF {int(snapshot)} WHERE source_feed={quote(feed)} AND source_epoch={quote(epoch)} AND source_position={int(position)}")
 assert len(rows)==1
 total,nonnull,minimum,maximum,distinct=rows[0];assert int(total)==expected_count and int(nonnull)==expected_count and minimum is not None and minimum==maximum and int(distinct)==1,rows
 return int(minimum)
