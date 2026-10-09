"""Replay admission that cannot prepare, apply or recommit a missing attempt."""
from ashlar.publisher import PublicationError


class CommittedReplayBackend:
    def __init__(self, backend, stream, batch_id, request_digest, descriptor):
        self.backend=backend
        self.stream=stream;self.batch_id=batch_id
        self.digest=request_digest;self.descriptor=descriptor

    def writer(self,stream,context):
        if stream!=self.stream:raise PublicationError('Original replay stream required')
        return self.backend.writer(stream,context)

    def observe(self,stream,batch_id):
        if (stream,batch_id)!=(self.stream,self.batch_id):raise PublicationError('Original replay batch required')
        original=self.backend.observe(stream,batch_id)
        if original is None or original.phase!='committed' or original.request_digest!=self.digest or original.descriptor!=self.descriptor:
            raise PublicationError('Replay requires the original committed attempt and checkpoint descriptor')
        return original

    def acknowledge(self,stream,request,descriptor,context):
        if stream!=self.stream or request['batch_id']!=self.batch_id or request['request_digest']!=self.digest or descriptor!=self.descriptor:
            raise PublicationError('Original replay acknowledgement required')
        return self.backend.acknowledge(stream,request,descriptor,context)
