"""Complete immutable JSONL transactions over the shared durable handoff."""
from ashlar.source import jsonl_batches
from ashlar.source_checkpoint import jsonl_checkpoint
from journaled_file_progress import JournaledFileProgress

class JournaledJsonlProgress(JournaledFileProgress):
    scope_table='jsonl_consumer_scope'
    progress_table='jsonl_consumer_progress'
    checkpoint_profile='ashlar-immutable-jsonl/0.1'
    checkpoint=staticmethod(jsonl_checkpoint)
    def __init__(self,journal,source,source_sha256,policy,resolver,*,stream,feed,epoch):
        super().__init__(journal,source,source_sha256,policy,resolver,stream=stream,feed=feed,epoch=epoch,config=dict(feed=feed,epoch=epoch))
    def decode(self,raw):
        return tuple(jsonl_batches(raw.splitlines(keepends=True),**self.config))
