
from multiprocessing import Process
import time
from queue import Full, Empty
from modules import address_timeline, cassandra
from datetime import datetime
import os
import uuid

def spawn_address_timeline_consumer(numOfProcesses: int,queue, number_of_blocks: int):
    processes = []
    
    number_of_blocks_per_process = int(number_of_blocks / numOfProcesses)
    remaining_blocks = number_of_blocks % numOfProcesses

    print(f"ADDRESS TIMELINE:\t\tSpawning {numOfProcesses} consumers")
    tdb = cassandra.CassandraTimeByStage()
    tdb.insert_start(uuid.UUID('{26402a28-98ba-11ed-a8fc-0242ac120002}'))
    db = cassandra.CassandraAddressBalance(None)
    db.create_table()
    
    for i in range(numOfProcesses):
        if i == 0:
            num_of_blocks_per_this_process = number_of_blocks_per_process + remaining_blocks
        else:
            num_of_blocks_per_this_process = number_of_blocks_per_process
        
        p = Process(target=_address_timeline_consumer, args=(queue,num_of_blocks_per_this_process,db.table_name,i+1,))
        p.start()
        processes.append(p)

    db = None
    return processes

def _address_timeline_consumer(queue, number_of_blocks: int,table_name, index):
    
    totalBlocks = 0
    address_timelines = address_timeline.AddressTimeline()
    db = cassandra.CassandraAddressBalance(table_name)
    db.batch_init()
    while True:
        
        try:
            items = queue.get_many(max_messages_to_get=index)
        except Empty:
            continue
        
        start = time.time()
        for item in items:
            if item is None:
                queue.put(None)
                print(f"T_C: ALL DONE!")    
                return
            address_timelines.set_new_block(item)
            address_timelines.parse_transactions()
            totalBlocks += 1

            for address, change in address_timelines.address_to_change.items():
                db.batch_add((datetime.fromtimestamp(address_timelines.blocktime), address, address_timelines.blockheight, 0, change))

        end = time.time()

        dbAddingTime = end - start

        items = []
        address_timelines.clear()
        
        qSize = queue.qsize()
        if qSize > 15:
            print(f"A_B: {os.getpid()}: Done processing, {qSize=}, {totalBlocks=}/{number_of_blocks} in {dbAddingTime=}")
