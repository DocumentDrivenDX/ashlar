"""Concurrent exact-reader control; publication callback supplied explicitly.
No warehouse lifecycle operations and no automatic write retry.
"""
import time
from concurrent.futures import Future,ThreadPoolExecutor
from threading import Event
from driver_sql import DriverClient
from isolation_reader import ExactReader

def compare_readers(targets,versions,rows,publish,client_factory=DriverClient,
                    idle_reads=30,max_load_reads=250,max_load_seconds=180):
    """Targets are (role, warehouse_id, output_directory). Each owns a connection.
    publish(stop) must propagate failure and inspect stop before advancing writes.
    An in-flight write is never claimed cancelled by setting the reader event.
    """
    assert len(targets)==2 and len({t[0] for t in targets})==2
    assert len({t[1] for t in targets})==2, 'Isolation requires distinct warehouses'
    assert len({str(t[2]) for t in targets})==2, 'Evidence directories must differ'
    assert 1<=idle_reads<=1000
    stop=Event();start=Event();completed=Event();ready=[Future(),Future()]
    def lane(target,signal):
        role,warehouse,out=target;client=None
        report={'role':role,'warehouse_id':warehouse,'state':'initializing'}
        try:
            client=client_factory(out,warehouse_id=warehouse)
            reader=ExactReader(client,versions,rows);reader.preflight()
            for i in range(idle_reads):reader.read('idle',i)
            signal.set_result(True)
            if not start.wait(timeout=240):raise TimeoutError('Coordinator did not release reader')
            report['load_start_epoch']=time.time()
            report['load_reads']=reader.bounded_load(stop,max_load_reads,max_load_seconds)
            report['load_end_epoch']=time.time()
            if completed.is_set():
                for i in range(idle_reads):reader.read('post',i)
            report['state']='reader exact checks passed; final-history audit required'
            return report
        except BaseException as error:
            stop.set()
            if not signal.done():signal.set_exception(error)
            raise
        finally:
            if client is not None:
                try:client.history()
                finally:client.close()
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(lane,target,signal) for target,signal in zip(targets,ready)]
        try:
            for signal in ready:signal.result(timeout=240)
            started=time.time();start.set()
            publication=publish(stop)
            if any(f.done() and f.exception() is not None for f in futures):
                raise RuntimeError('Reader failed during publication; inspect statement evidence')
            finished=time.time();completed.set()
        finally:
            stop.set();start.set()
        reports=[f.result() for f in futures]
    return {'publication_start_epoch':started,'publication_end_epoch':finished,
            'publication':publication,'readers':reports,
            'qualification':'Reader sessions started together after independent idle cohorts; actual statement intervals determine overlap. No source or performance admission from controller success.'}
