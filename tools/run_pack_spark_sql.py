"""Authored-SQL baseline over views of held original local publications.

Views preserve UTF8_BINARY strings and present nulls. Numeric casts use explicit
finite DECIMAL(38,0) integer and DECIMAL(38,18) decimal subsets; all original
lexical tokens remain unchanged in the pinned native props/retained bytes.
Every view's complete typed cell bag must equal an independent original graph
projection before any authored SQL executes. This is not Weft-generated SQL or
Weft conformance, accepted Truss catalogs, UC, remote source, or scale evidence.
"""
import json,re
from collections import Counter
from decimal import Decimal,localcontext,InvalidOperation
from run_pack_publication_weft import compiler_request,original_ports
from prepare_pack_query_cases import prepare

OUTPUT_FAMILIES={
 'archaeology':{'cycle':('string','string'),'dating':('string','string'),'media':('string','integer'),'missing-media':('string',),'specialists':('integer',)*4,'lineage':('string','string'),'evidence-links':('string',)*3,'sample':('string','string')},
 'ecology':{'effort-event':('string',),'connected-measurements':('string','integer'),'censor':('string','decimal','string'),'match':('string',),'effort':('string',),'zero':('string',),'network':('string','string'),'comparability':('string',)*3,'fishing':('string',)}}


def target_value(token,family):
    """Representation check only, after genuine public source admission."""
    if family not in ('string','integer','decimal'):raise ValueError('Named finite native scalar profile required')
    if token is None:return None
    if type(token)is not str:raise ValueError('Original lexical string carrier required')
    if family=='string':return token
    if family not in ('integer','decimal'):raise ValueError('Named finite native scalar profile required')
    try:value=Decimal(token)
    except InvalidOperation as error:raise ValueError('Exact numeric lexical carrier required') from error
    if not value.is_finite():raise ValueError('Finite numeric native carrier required')
    if family=='integer':
        if not re.fullmatch(r'-?(?:0|[1-9][0-9]*)',token) or value.copy_abs()>=Decimal('1e38'):raise ValueError('Exact DECIMAL(38,0) subset required')
    else:
        if value.copy_abs()>=Decimal('1e20'):raise ValueError('DECIMAL(38,18) range exceeded')
        with localcontext() as context:
            context.prec=80
            if value.quantize(Decimal('1e-18'))!=value:raise ValueError('DECIMAL(38,18) would round original value')
    return value


def view_design(pack,model_bytes,graph_bytes,bindings):
    converter,native,_,_=original_ports(pack)
    _,expected=converter.build_transaction(model_bytes,graph_bytes,source_system=native.SOURCE_SYSTEM)
    if bindings!=expected:raise ValueError('Exact original development bindings required')
    model=json.loads(model_bytes);graph=json.loads(graph_bytes)
    elements={(module['id'],element['id']):element for module in model['modules']for element in module['elements']}
    properties={tuple(item['identity']):item['property_id'] for item in bindings['properties']}
    plans=[]
    for type in bindings['types']:
        kind,module,element=type['identity']
        if kind!='object':continue
        record=elements[(module,element)];columns=[];expressions=[]
        for ref in record['members']:
            field=elements[(ref['module'],ref['element'])];name=field['name'];family=field['scalarType']
            if not re.fullmatch('[A-Za-z_][A-Za-z_0-9]*',name):raise ValueError('Explicit simple authored Field name profile required')
            property_id=properties[(model['id'],ref['module'],ref['element'])]
            raw="get_json_object(props_json,'$."+property_id+"')"
            if family=='string':expression=raw+' COLLATE UTF8_BINARY'
            elif family=='integer':expression='CAST('+raw+' AS DECIMAL(38,0))'
            elif family=='decimal':expression='CAST('+raw+' AS DECIMAL(38,18))'
            else:raise ValueError('Unsupported named native scalar family')
            columns.append({'name':name,'field_identity':[model['id'],ref['module'],ref['element']],'family':family,'property_id':property_id})
            expressions.append(expression+' AS `'+name+'`')
        if len({column['name']for column in columns})!=len(columns) or not re.fullmatch('[A-Za-z_][A-Za-z_0-9]*',record['name']):raise ValueError('Distinct simple authored column/view names required')
        expected_rows=[]
        for obj in graph['objects']:
            if obj['type']!={'document':model['id'],'module':module,'element':element}:continue
            expected_rows.append(tuple(target_value(obj['values'][column['field_identity'][2]],column['family'])for column in columns))
        plans.append({'name':record['name'],'identity':[model['id'],module,element],'type_id':type['type_id'],'source_system':native.SOURCE_SYSTEM,'columns':columns,'expressions':expressions,'expected_rows':expected_rows})
    if len({plan['name']for plan in plans})!=len(plans):raise ValueError('Distinct original Record view names required')
    return plans


def typed_rows(rows,families):
    if any(family not in ('string','integer','decimal')for family in families):raise ValueError('Named output families required')
    result=[]
    for row in rows:
        if len(row)!=len(families):raise ValueError('Exact declared output arity required')
        converted=[]
        for value,family in zip(row,families):
            if value is None:converted.append(None);continue
            if family=='string':
                if type(value)is not str:raise ValueError('Binary string result required')
                converted.append(value)
            else:
                if type(value)not in (Decimal,int):raise ValueError('Exact numeric result; no float coercion')
                numeric=Decimal(value)
                if not numeric.is_finite() or (family=='integer' and numeric!=numeric.to_integral_value()):raise ValueError('Exact finite declared numeric result required')
                converted.append(numeric)
        result.append(tuple(converted))
    return result


def baseline(reader,pack):
    """Keep view checks, all unchanged SQL and buffered answers in one interval."""
    spark=reader.driver.transport.spark;provider=reader.provider;context=reader.context
    plans=view_design(pack,reader.model,reader.graph,reader.bindings);cases=prepare(pack)['cases']
    request=compiler_request(pack,'SELECT * FROM '+plans[0]['name'],reader.model,reader.graph,reader.bindings,reader.manifest,reader.original_report['table_registry'],reader.aliases)
    binding=json.loads(request['target']['bindingJson']);answers=[];created=[];completed=False
    try:
        with provider.interval(context):
            opening=provider.resolve(context);provider.admit_binding(binding,opening,context);provider.runtime(context)
            table=reader.driver.tables['object_current'];version=opening.snapshots[table].version;target=reader.driver.transport.targets[table]
            frame=spark.read.format('delta').option('versionAsOf',version).load(str(target.path))
            for plan in plans:
                selected=frame.where((frame.source_system==plan['source_system'])&(frame.type_id==int(plan['type_id']))).selectExpr(*plan['expressions'])
                rows=[tuple(row)for row in selected.collect()];families=[column['family']for column in plan['columns']]
                if Counter(typed_rows(rows,families))!=Counter(plan['expected_rows']):raise ValueError('Complete original scalar/null view bag differs')
                if spark.catalog.tableExists(plan['name']):raise ValueError('Exclusive original view namespace required')
                selected.createOrReplaceTempView(plan['name']);created.append(plan['name'])
            # All views are fully checked before the first authored query.
            for case in cases:
                original=case['original_scenario'];families=OUTPUT_FAMILIES[pack][original['id']]
                result=spark.sql(original['sql'])
                from pyspark.sql.types import StringType,DecimalType,LongType,IntegerType
                allowed={'string':(StringType,),'decimal':(DecimalType,),'integer':(DecimalType,LongType,IntegerType)}
                if len(result.schema.fields)!=len(families) or any(not isinstance(field.dataType,allowed[family])for field,family in zip(result.schema.fields,families)):raise ValueError('Declared original result family/schema differs')
                rows=typed_rows([tuple(row)for row in result.collect()],families)
                expected=[tuple(target_value(value,family)for value,family in zip(row,families))for row in case['original_graph_expected']]
                if Counter(rows)!=Counter(expected):raise ValueError('Original independent graph scenario result differs')
                answers.append({'id':original['id'],'original_sql':original['sql'],'rows':[[None if value is None else str(value)for value in row]for row in rows],'original_graph_expected':case['original_graph_expected'],'output_families':families})
            provider.runtime(context);closing=provider.resolve(context);provider.admit_binding(binding,closing,context)
            if dict(opening.descriptor.raw)!=dict(closing.descriptor.raw) or opening.snapshots!=closing.snapshots:raise ValueError('Whole original publication changed before baseline release')
            completed=True
    finally:
        failures=[]
        for name in reversed(created):
            try:
                if not spark.catalog.dropTempView(name):raise ValueError('Original temporary view cleanup failed')
            except Exception as error:failures.append(error)
        if failures:raise failures[0]
    if not completed:raise ValueError('Original guarded baseline closure suppressed')
    return {'pack':pack,'view_count':len(plans),'complete_scalar_rows':sum(len(plan['expected_rows'])for plan in plans),'view_scalar_profile':'UTF8_BINARY/DECIMAL(38,0)/DECIMAL(38,18)','cases':answers,'closed_interval_custody':provider.closed_interval_custody(context),'qualification':__doc__}


def run(publications,output,jars):
    from pathlib import Path
    import hashlib
    from local_delta_custody import encoded
    from run_pack_publication_weft import open_pack_reader
    if set(publications)!={'archaeology','ecology'}:raise ValueError('Both original publications required')
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    jars=Path(jars);paths=[jars/name for name in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if not all(path.is_file()for path in paths):raise ValueError('Explicit existing qualified Delta4 jars required')
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original authored SQL private baseline').config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.sql.warehouse.dir',str(output/'warehouse')).config('spark.jars',','.join(str(path)for path in paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    reports=[]
    try:
        if spark.version!='4.0.1':raise ValueError('Actual experimental Spark4.0.1 required')
        for pack in ('archaeology','ecology'):
            with open_pack_reader(spark,publications[pack],pack) as reader:
                def identifier(name):return '.'.join('`'+part.replace('`','``')+'`'for part in name.split('.'))
                for native,alias in reader.aliases.items():
                    database='.'.join(alias.split('.')[:2]);spark.sql('CREATE DATABASE IF NOT EXISTS '+identifier(database))
                    path=str(reader.driver.transport.targets[native].path).replace("'","''")
                    spark.sql('CREATE TABLE '+identifier(alias)+" USING DELTA LOCATION '"+path+"'")
                report=baseline(reader,pack)
                report['original_publication']=str(publications[pack]);report['original_manifest']=reader.manifest
            reports.append(report)
    finally:
        spark.stop()
    result={'format':'ashlar-original-pack-authored-spark-sql/0.1','runtime':'Spark4.0.1/Delta4.0.0','reports':reports,'jars':[{'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}for path in paths],'qualification':__doc__}
    (output/'report.json').write_text(encoded(result)+'\n');return result


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--archaeology',required=True);parser.add_argument('--ecology',required=True);parser.add_argument('--output',required=True);parser.add_argument('--jars',required=True);args=parser.parse_args()
    result=run({'archaeology':args.archaeology,'ecology':args.ecology},args.output,args.jars)
    print('Completed guarded authored SQL baseline:',sum(len(report['cases'])for report in result['reports']),'cases')
