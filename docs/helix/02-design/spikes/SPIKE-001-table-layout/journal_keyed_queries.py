"""Owned unique-event journal comparison; full exact fields and multiplicity refusal."""
from journal_validation_queries import COLS,TEXTS
KEYS=('source_feed','source_epoch','source_delivery_id','property_id','event_ordinal')
def keyed(left,right):
    # This scoped profile requires each side's event key to be unique.
    fields=','.join(f"hex(encode({col},'UTF-8')) AS {col}" if col in TEXTS else col for col in COLS)
    keys=','.join(KEYS)
    def side(query):return f'SELECT *,count(*) OVER (PARTITION BY {keys}) __members FROM (SELECT {fields} FROM ({query}))'
    join=' AND '.join(f'e.{col} <=> a.{col}' for col in KEYS)
    checks=' OR '.join(f'NOT(e.{col} <=> a.{col})' for col in COLS)
    return f'SELECT count(*) FROM ({side(left)}) e FULL OUTER JOIN ({side(right)}) a ON {join} WHERE e.__members IS NULL OR a.__members IS NULL OR e.__members<>1 OR a.__members<>1 OR '+checks
