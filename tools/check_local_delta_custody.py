"""Bounded actual Spark/Delta original-commit recovery; no publication/ACK claim."""
import argparse,hashlib,importlib.metadata,json
from contextlib import contextmanager
from pathlib import Path
from local_delta_custody import DeltaTarget,LocalDeltaTransport,LocalDeltaUncertain,LocalDeltaError,encoded
from run_graph_release_graphframes import JARS,VERSIONS

class ProbePolicy:
    def __init__(self,context,target):self.context=context;self.target=target
    @contextmanager
    def writer(self,operation,context):
        if context is not self.context:raise PermissionError('Private original probe context required')
        yield
    def admit(self,intent,context):
        if intent['profile']=='ashlar-local-delta-installation/0.1':
            if context is not self.context or intent['targets'][0]['uuid']!=self.target.uuid:raise PermissionError('Original fresh installation required')
            return
        if context is not self.context or intent['table']!=self.target.table or intent['uuid']!=self.target.uuid or intent['request_digest']!='a'*64:raise PermissionError('Original explicit bounded native operation required')

def run(output,jars):
    output=Path(output)
    if output.exists():raise ValueError('Fresh private output required')
    if {p:importlib.metadata.version(p) for p in VERSIONS}!=VERSIONS:raise ValueError('Existing qualified runtime versions required')
    paths=[Path(jars)/p for p in JARS]
    if not all(p.is_file() for p in paths):raise ValueError('Explicit existing native jars required')
    output.mkdir(parents=True)
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar small original Delta custody').config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.jars',','.join(str(p) for p in paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    try:
        path=output/'items';spark.sql('CREATE TABLE delta.`'+str(path)+'` (id BIGINT,published_at TIMESTAMP,props STRING) USING DELTA')
        detail=spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').first();target=DeltaTarget('local.runtime.items',path,detail.id);context=object();policy=ProbePolicy(context,target)
        journal=output/'operations.sqlite';t=LocalDeltaTransport.initialize(spark,journal,'private-native-probe',(target,),policy,context=context)
        sql='INSERT INTO `local`.`runtime`.`items` VALUES(cast(:id AS BIGINT),timestamp_micros(cast(:time AS BIGINT)),:props)'
        params={'id':'9007199254740993','time':'-1','props':' {"decimal":-0.00,"opaque":18446744073709551615,"unicode":"雪"} '}
        original_snapshot=t._snapshot
        def crash_after_native_commit(*args):raise RuntimeError('Injected loss after actual native commit before receipt persistence')
        t._snapshot=crash_after_native_commit
        try:t.mutation('original-commit',sql,params,intent_digest='a'*64,context=context)
        except RuntimeError as error:
            if 'Injected loss' not in str(error):raise
        else:raise AssertionError('Lost receipt injection did not execute')
        if t.db.execute('SELECT state,receipt FROM local_operation').fetchone()!=('submitted',None):raise AssertionError('Original submitted custody not retained')
        t.close();fresh=LocalDeltaTransport(spark,journal,'private-native-probe',(target,),policy)
        recovered=fresh.recover('original-commit',sql,params,intent_digest='a'*64,context=context)
        replay=fresh.mutation('original-commit',sql,params,intent_digest='a'*64,context=context)
        if recovered!=replay:raise AssertionError('Original exact commit replay differs')
        proof=json.loads(fresh.db.execute('SELECT receipt FROM local_operation WHERE operation=?',('original-commit',)).fetchone()[0])
        expected=[{'id':params['id'],'published_at':params['time'],'props':params['props']}]
        if proof['snapshot']['rows']!=expected or proof['version']!=2:raise AssertionError('Independent exact int/time/property native snapshot differs')
        history_before=fresh._history(target)
        class FaultSpark:
            conf=spark.conf
            def sql(self,statement,args=None):
                if statement.startswith('INSERT'):raise RuntimeError('Injected loss after durable submitted marker before native call')
                return spark.sql(statement,args=args)
        fresh.spark=FaultSpark()
        try:fresh.mutation('uncertain-absence',sql,{**params,'id':'2'},intent_digest='a'*64,context=context)
        except RuntimeError as error:
            if 'Injected loss' not in str(error):raise
        else:raise AssertionError('Uncertain absence injection did not execute')
        fresh.close();again=LocalDeltaTransport(spark,journal,'private-native-probe',(target,),policy)
        for function in (again.recover,again.mutation):
            try:function('uncertain-absence',sql,{**params,'id':'2'},intent_digest='a'*64,context=context)
            except LocalDeltaUncertain:pass
            else:raise AssertionError('Absent submitted original commit allowed replacement')
        if again._history(target)!=history_before:raise AssertionError('Recovery submitted a replacement native mutation')
        proof_after=again._snapshot(target,2)
        if proof_after['rows']!=expected:raise AssertionError('Uncertain reconciliation changed original rows')
        again.close()
        retained_journal=output/'operations-retained.sqlite';journal.rename(retained_journal)
        for attempt in (lambda:LocalDeltaTransport(spark,journal,'private-native-probe',(target,),policy),lambda:LocalDeltaTransport.initialize(spark,journal,'private-native-probe',(target,),policy,context=context)):
            try:attempt()
            except LocalDeltaError:pass
            else:raise AssertionError('Lost journal allowed replacement installation')
        journal_history=[r.asDict() for r in spark.sql('DESCRIBE HISTORY delta.`'+str(path)+'`').collect()]
        if journal_history!=history_before:raise AssertionError('Lost-journal refusal changed native history')
        retained_journal.rename(journal)
        report={'lost_journal_reinitialization_refused':True,'format':'ashlar-local-delta-custody-check/0.1','versions':VERSIONS,'native_uuid':target.uuid,'original_commit':proof,'lost_receipt_fresh_recovery':True,'replay_version_unchanged':True,'submitted_absent_uncertain':True,'replacement_writes':0,'qualification':__doc__}
        (output/'report.json').write_text(encoded(report)+'\n');again.close();return report
    finally:spark.stop()
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--jars',required=True,type=Path);a=p.parse_args();print(json.dumps(run(a.output,a.jars)))
