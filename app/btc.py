import time
import datetime
import os
from multiprocessing import Manager, Process
from functools import wraps
import block_publisher
import copy
import itertools 
from modules import clustering_consumer, timeline_consumer, cassandra

#from multiprocessing import  Queue
from faster_fifo import Queue
import faster_fifo_reduction
import uuid
from queue import Full, Empty

def ensure_parent(func):
    @wraps(func)
    def inner(self, *args, **kwargs):
        if os.getpid() != self._creator_pid:
            raise RuntimeError(f"{func.__name__} can only be called in the "
                               "parent.")
        return func(self, *args, **kwargs)
    return inner

class PublishQueue(object):
    def __init__(self):
        self._queues = []
        self._creator_pid = os.getpid()

    def __getstate__(self):
        self_dict = self.__dict__
        self_dict['_queues'] = []
        return self_dict

    def __setstate__(self, state):
        self.__dict__.update(state)

    #@ensure_parent
    def register(self):
        q = Queue(10000 * 10000)
        self._queues.append(q)
        return q

    #@ensure_parent
    def publish(self, val):
        for q in self._queues:
            try: 
                q.put(val)
            except Full:
                print(f"FULL Q {q} out of {self._queues}!")
                success = False
                while success is False:
                    try:
                        q.put(val)
                        success = True
                    except Full:
                        time.sleep(2)
                
    #@ensure_parent
    def close_all(self):
        for q in self._queues:
            q.close()

NUMBER_OF_FETCHERS = 3
BLOCKS = {
    'start': 600_000,
    'end': 772_000,
}

def main():
    q = PublishQueue()
    
    db = cassandra.CassandraTimeByStage()
    db.truncate_table()

    processes = []
    producers = []
    timelineers = []
    p = clustering_consumer.spawnClusteringConsumer(4, q.register(), (BLOCKS["end"] - BLOCKS["start"] + 1))
    processes.extend(p)
    p = timeline_consumer.spawn_address_timeline_consumer(6, q.register(), (BLOCKS["end"] - BLOCKS["start"] + 1))
    timelineers.extend(p)
    p = block_publisher.spawnBlockPublishers(NUMBER_OF_FETCHERS,q,BLOCKS)
    processes.extend(p)
    producers.extend(p)
    start = time.time()

    db.insert_start(uuid.UUID('{26401d76-98ba-11ed-a8fc-0242ac120002}'))
    
    for p in producers:
        p.join()

    db.insert_end(uuid.UUID('{26401d76-98ba-11ed-a8fc-0242ac120002}'))
    q.publish(None)

    for p in timelineers:
        p.join()

    db.insert_end(uuid.UUID('{26402a28-98ba-11ed-a8fc-0242ac120002}'))
    
    for p in processes:
        p.join()
    print(f"Done in {time.time() - start}s")

if __name__ == "__main__":
    main()
