"""CSV adapter over the shared original-file publication/checkpoint handoff."""
from ashlar.csv_source import csv_batches,validate_csv_batch
from ashlar.source_checkpoint import csv_checkpoint
from journaled_file_progress import JournaledFileProgress,LocalProgressOutcomeUnknown

class JournaledCsvProgress(JournaledFileProgress):
    scope_table='csv_consumer_scope'
    progress_table='csv_consumer_progress'
    checkpoint_profile='ashlar-single-line-csv/0.1'
    checkpoint=staticmethod(csv_checkpoint)
    def __init__(self,journal,source,source_sha256,policy,resolver,*,stream,feed,epoch,
                 source_system,schema_revision,type_id,properties):
        config=dict(feed=feed,epoch=epoch,source_system=source_system,schema_revision=schema_revision,type_id=type_id,properties=dict(properties))
        super().__init__(journal,source,source_sha256,policy,resolver,stream=stream,feed=feed,epoch=epoch,config=config)
    def decode(self,raw):
        batches=tuple(csv_batches(raw.splitlines(keepends=True),**self.config))
        for batch in batches:validate_csv_batch(batch,**self.config)
        return batches
