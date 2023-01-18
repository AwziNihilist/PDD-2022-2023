from multiprocessing import Process, Pipe, Queue
from btc import PublishQueue
from modules import clustering, txfiltering, cassandra
import time
import datetime
from queue import Full, Empty
import uuid


def spawnClusteringConsumer(numOfProcesses: int,queue: PublishQueue, number_of_blocks: int):
    processes = []
    
    queue_superconsumer = Queue()
    
    number_of_blocks_per_process = int(number_of_blocks / numOfProcesses)
    remaining_blocks = number_of_blocks % numOfProcesses
    
    print(f"CLUSTERING:\t\t\tSpawning {numOfProcesses} consumers + 1 super consumer! {number_of_blocks_per_process}-{number_of_blocks_per_process+remaining_blocks}/each")   
    
    for i in range(numOfProcesses):
        if i == 0:
            num_of_blocks_per_this_process = number_of_blocks_per_process + remaining_blocks
        else:
            num_of_blocks_per_this_process = number_of_blocks_per_process
        
        p = Process(target=_clustering_consumer, args=(queue,num_of_blocks_per_this_process,queue_superconsumer,i,numOfProcesses,))
        p.start()
        processes.append(p)
        
    p = Process(target=_clustering_superConsumer, args=(queue_superconsumer,numOfProcesses,))
    p.start()
    processes.append(p)

    return processes

def _clustering_superConsumer(queue_superconsumer,numOfProcesses):
    
    clusters = clustering.ClusteringAliased()
    
    tdb = cassandra.CassandraTimeByStage()
    tdb.insert_start(uuid.UUID('{26402046-98ba-11ed-a8fc-0242ac120002}'))

    dbAC = cassandra.CassandraAddressCluster(None)
    dbAC.create_table()

    dbCA = cassandra.CassandraClusterAddresses(None)
    dbCA.create_table()

    while numOfProcesses > 0:
        data = None
        data = queue_superconsumer.get()
        
        if data is None:
            numOfProcesses -= 1
            continue
        clusters.cluster_sets(data)
    
    print(f"SC: Everything closed!")
    tdb.insert_end(uuid.UUID('{26402046-98ba-11ed-a8fc-0242ac120002}'))
    tdb.insert_start(uuid.UUID('{26402186-98ba-11ed-a8fc-0242ac120002}'))
    print(f"SC: Started normalization!")
    start = time.time()
    clusters.normalizeDicts()
    end = time.time()
    print(f"SC: Normalization finished in {end-start}s")
    tdb.insert_end(uuid.UUID('{26402186-98ba-11ed-a8fc-0242ac120002}'))

    print(clusters)
    clusters.write_to_file()

    NUMBER_OF_CA_WORKERS = 8
    NUMBER_OF_AC_WORKERS = 8

    print(f"SC: Spawning {NUMBER_OF_CA_WORKERS=}")

    ca_queue = Queue()
    ca_token_queue = Queue()
    ca_workers = []

    for i in range(NUMBER_OF_CA_WORKERS):
        p = Process(target=_ca_worker, args=(ca_queue, ca_token_queue, dbCA.table_name,i,))
        p.start()
        ca_workers.append(p)


    print(f"SC: Started writing CLUSTERS to DB!")
    tdb.insert_start(uuid.UUID('{2640278a-98ba-11ed-a8fc-0242ac120002}'))
    start = time.time()

    batch_size_addresses = 0
    batch_to_send = []
    batch_size_max_addresses = 500
    batch_size_too_big_threshold = 500

    #with open(file="clusters" ,mode='w') as f:
    #print(f"SC: file openend")
    for cluster_id, addresses in clusters.c_a.items():  
        
        cluster_size = 0
        unhashed_set = set()
        
        for hashed_address in addresses:
            address = clusters.hash_to_address(hashed_address)
            unhashed_set.add(address)
            cluster_size += 1
        #print(f"SC: got unhashed set")            
        clusters.c_a[cluster_id] = None
        #f.write(f"{cluster_id}:{unhashed_set}\n")
        #print(f"SC: out to file")   
        
        if cluster_size > batch_size_too_big_threshold:
            ca_token_queue.get()
            ca_queue.put((cluster_id, cluster_size, unhashed_set))

        elif batch_size_addresses + cluster_size < batch_size_max_addresses:
            batch_to_send.append((cluster_id, cluster_size, unhashed_set))
            batch_size_addresses += cluster_size
        else:
            ca_token_queue.get()
            ca_queue.put(batch_to_send)
            batch_to_send = [(cluster_id, cluster_size, unhashed_set)]
            batch_size_addresses = cluster_size
        
    if batch_size_addresses > 0:
        ca_queue.put(batch_to_send)

    ca_queue.put(None)

    for worker_process in ca_workers:
        print(f"SC: Waiting for workers")
        worker_process.join()
        

    end = time.time()
    print(f"SC: writing CLUSTERS to DB finished in {end-start}s")
    tdb.insert_end(uuid.UUID('{2640278a-98ba-11ed-a8fc-0242ac120002}'))

    dbCA = None
    clusters.c_a = dict()

    print(f"SC: Spawning {NUMBER_OF_AC_WORKERS=}")

    ac_queue = Queue()
    ac_workers = []

    for _ in range(NUMBER_OF_AC_WORKERS):
        p = Process(target=_ac_worker, args=(ac_queue, dbAC.table_name,))
        p.start()
        ac_workers.append(p)

    print(f"SC: Started writing ADDRESSES to DB!")
    tdb.insert_start(uuid.UUID('{264028f2-98ba-11ed-a8fc-0242ac120002}'))
    start = time.time()

    batch_size = 0
    batch_size_max = 500
    
    tuples_to_insert = []
    for addressHashed, cluster_id in clusters.a_c.items():        
        address = clusters.hash_to_address(addressHashed)
        tuples_to_insert.append((address, cluster_id))
        batch_size += 1
        
        clusters.a_c[addressHashed] = None
        
        if batch_size == batch_size_max:
            ac_queue.put(tuples_to_insert)
            tuples_to_insert = []
            batch_size = 0

    if batch_size > 0:
        ac_queue.put(tuples_to_insert)
    
    ac_queue.put(None)
    tuples_to_insert = []

    for p in ac_workers:
        p.join()

    end = time.time()
    print(f"SC: writing ADDRESSES to DB finished in {end-start}s")
    tdb.insert_end(uuid.UUID('{264028f2-98ba-11ed-a8fc-0242ac120002}'))

    print(clusters)
    clusters.write_to_file()

def _ca_worker(queue, token_queue, table_name, token):
    dbCA = cassandra.CassandraClusterAddresses(table_name)
    a = token
    while True:
        data = None
        #print(f"Worker published token {a}")
        token_queue.put(a)
        data = queue.get()
        #print(f"Worker received data")
        
        if data is None:
            queue.put(None)
            print("C_A WORKER: Returning!")
            return

        if not isinstance(data, list):
            #print(f"its not a list but biig")
            dbCA.insert_single(data)
            continue
        
        #print(f"list so init")
        dbCA.batch_init()
        #print(f"for each member of the list")
        for tuple in data:
            #print(f"add to batch")
            dbCA.batch_add(tuple)
        #print(f"execute batch")
        dbCA.batch_execute()
        #print(f"batch executed")

def _ac_worker(queue, table_name):

    dbAC = cassandra.CassandraAddressCluster(table_name)
    
    while True:
        data = None
        data = queue.get()
        
        if data is None:
            queue.put(None)
            print("A_C WORKER: Returning!")
            return
        
        dbAC.batch_init()
        for tuple in data:
            dbAC.batch_add(tuple)
        dbAC.batch_execute()


def _reporter(queue, total_blocks):
    db = cassandra.CassandraBlocksByTime()
    db.truncate_table()
    id = uuid.UUID('{00010203-0405-0607-0809-0a0b0c0d0e0f}')
    while True:
        data = None
        data = queue.get()
        
        if data is None:
            return
        
        ct = datetime.datetime.utcnow()
        blocks = data
        if blocks % 100 == 0:
            print(f"C_R: {ct} ~{blocks}/{total_blocks*4}")
        
        db.insert_single((id, ct, blocks))

def _clustering_consumer(queue, number_of_blocks: int, queue_superconsumer, index,numOfProcesses):
    
    totalBlocks = 0
    blocksInPackage = 0

    clusters = clustering.Clustering()
    
    if index == 0:
        reporter_queue = Queue()
        reporter_process = Process(target=_reporter, args=(reporter_queue,number_of_blocks,))
        reporter_process.start()

    
    while True:
        
        try:
            item = queue.get()
        except Empty:
            continue

        if item is None:
            if (blocksInPackage > 0):
                print("C_C: Shutting down!")
                queue_superconsumer.put(list(clusters.c_a.values()))
            queue_superconsumer.put(None)
            queue.put(None)
            if index == 0:
                reporter_queue.put(None)
            return


        #Process the new item
        start = time.time()
        
        item = txfiltering.filterDataWithAuxiliaries(item['txs'])
        end1 = time.time()

        clusters.cluster_sets(item)
        end2 = time.time()

        filteringTime = end1 - start
        clusteringTime = end2 - start

        blocksInPackage += 1
        totalBlocks += 1

        if index == 0 and totalBlocks % 100 == 0:
            #print(f"C_C: Queue size: {queue.qsize()}")
            reporter_queue.put(totalBlocks*numOfProcesses)

        if blocksInPackage == 1_000:
            queue_superconsumer.put(list(clusters.c_a.values()))
            clusters.reset_state()
            blocksInPackage = 0

        
            
        item = None
