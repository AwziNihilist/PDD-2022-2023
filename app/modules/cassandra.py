from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
from cassandra.policies import RetryPolicy
import socket
import time
from cassandra import ConsistencyLevel
#from cassandra import BatchType

from datetime import datetime


auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

class Cassandra():
    def __init__(self) -> None:
        self.cluster = Cluster(['10.0.0.106'], port=9042, auth_provider=auth_provider, executor_threads=6,control_connection_timeout=None, connect_timeout=30)
        self.session = self.cluster.connect('btc')
        self.table_name = None
        
        self.batch = None
        self.input_group = None

class CassandraBlocksByTime(Cassandra):
    def __init__(self) -> None:
        super().__init__()
        self.table_name = 'blocks_by_time'
        self.batch_size_current = 0
        self.batch_size_max = 65535
        self.batch_init()

    def truncate_table(self):
        
        truncate = f"TRUNCATE TABLE {self.table_name}"

        # Execute the query
        self.session.execute(truncate)

    def batch_init(self):
        try:
            self.batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_ONE,request_timeout=60)#, batch_type=BatchType.UNLOGGED)
            if self.input_group is None:
                self.input_group = self.session.prepare(f"INSERT INTO {self.table_name} (id, time, value) VALUES (?, ?, ?)")
        except:
            time.sleep(0.1)
            ct = datetime.utcnow()
            with open(file="errors",mode='w') as f:
                f.write(f"{ct} ERR INIT CassandraBlocksByTime\n")
            self.batch_init()
        

    def batch_add(self, values):
        self.batch_size_current += 1
        self.batch.add(self.input_group,values)
        self.batch_execute()
        self.batch_init()

    def batch_execute(self):
        self.session.execute_async(self.batch)
        
    def insert_single(self, values):
        self.session.execute_async(f"INSERT INTO {self.table_name} (id, time, value) VALUES (%s, %s, %s)", values)


class CassandraTimeByStage(Cassandra):
    def __init__(self) -> None:
        super().__init__()
        self.table_name = 'time_by_stage'

    def truncate_table(self):
        truncate = f"TRUNCATE TABLE {self.table_name}"
        # Execute the query
        self.session.execute(truncate)

    def insert_start(self, stage):
        ct = datetime.utcnow()
        self.session.execute(f"INSERT INTO {self.table_name} (id, time, value) VALUES (%s, %s, %s)", (stage, ct, 1))
        
    def insert_end(self, stage):
        ct = datetime.utcnow()
        self.session.execute(f"INSERT INTO {self.table_name} (id, time, value) VALUES (%s, %s, %s)", (stage, ct, 0))

class CassandraAddressBalance(Cassandra):
    def __init__(self, table_name) -> None:
        super().__init__()
        self.table_name = table_name
        self.batch_size_current = 0
        self.batch_size_max = 300
        #self.batch_init()

    def create_table(self):
        #now = int(datetime.now().timestamp())
        now = 1
        
        self.table_name = f"address_balance_{now}"
        
        # Define the table schema
        table_schema = f"""
            CREATE TABLE {self.table_name} (
                time timestamp,
                address text,
                blockheight int,
                value double,
                difference double,
                PRIMARY KEY (time, address)
            ) 
            """

        table_schema = f"TRUNCATE TABLE {self.table_name}"

        # Execute the query
        self.session.execute(table_schema)

    def batch_init(self):
        #try:
        self.batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_ONE, batch_type=BatchType.UNLOGGED,request_timeout=60)
        if self.input_group is None:
            self.input_group = self.session.prepare(f"INSERT INTO {self.table_name} (time, address, blockheight, value, difference) VALUES (?, ?, ?, ?, ?)")
        #except:
            #time.sleep(0.1)
            #ct = datetime.utcnow()
           # with open(file="errors",mode='w') as f:
                #f.write(f"{ct} ERR INIT ADDRESS BALANCE\n")
            #self.batch_init()

    def batch_add(self, values):
        self.batch_size_current += 1
        self.batch.add(self.input_group,values)
        if self.batch_size_current == self.batch_size_max:
            #print(f"{self.batch_size_current=} {self.batch_size_max=} {values=}")
            self.batch_execute()
            #self.input_group = self.session.prepare(f"INSERT INTO {self.table_name} (time, address, blockheight, value, difference) VALUES (?, ?, ?, ?, ?)")
            self.batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_ONE, batch_type=BatchType.UNLOGGED,request_timeout=60)
            self.batch_size_current = 0

    def batch_execute(self):
        #try:
        self.session.execute(self.batch)
        #except:
        #    print(f"ERROR! CONNECTION BUSY! RETRY")
        #    result = False
        #    while result is False:
        #        try:
        #            self.session.execute(self.batch)
        #            result = True
        #        except:
        #            time.sleep(2)
        #    print(f"RETRY SUCCESS!")
                    

class CassandraAddressCluster(Cassandra):
    def __init__(self,table_name) -> None:
        super().__init__()
        self.table_name = table_name

    def create_table(self):
        
        #now = int(datetime.now().timestamp())
        now = 1
        self.table_name = f"address_cluster_{now}"
        # Define the table schema
        table_schema = f"""
            CREATE TABLE {self.table_name} (
                address text PRIMARY KEY,
                cluster_id int
            )
            """
        table_schema = f"TRUNCATE TABLE {self.table_name}"

        # Execute the query
        self.session.execute(table_schema)

    def batch_init(self):
        try:
            self.batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_ONE,request_timeout=60)#, batch_type=BatchType.UNLOGGED)
            if self.input_group is None:
                self.input_group = self.session.prepare(f"INSERT INTO {self.table_name} (address, cluster_id) VALUES (?, ?)")
        except:
            time.sleep(0.1)
            ct = datetime.utcnow()
            with open(file="errors",mode='w') as f:
                f.write(f"{ct} ERR INIT ADDRESS BALANCE\n")
            self.batch_init()
            

    def batch_add(self, values):
        self.batch.add(self.input_group,values)

    def batch_execute(self):
        self.session.execute_async(self.batch)


class CassandraClusterAddresses(Cassandra):
    def __init__(self,table_name) -> None:
        super().__init__()
        self.table_name = table_name

    def create_table(self):
        #now = int(datetime.now().timestamp())
        now = 1
        
        self.table_name = f"cluster_addresses_{now}"
        
        # Define the table schema
        table_schema = f"""
            CREATE TABLE {self.table_name} (
                cluster_id int PRIMARY KEY,
                size int,
                addresses set<text>
            )
            """

        table_schema = f"TRUNCATE TABLE {self.table_name}"

        # Execute the query
        self.session.execute(table_schema)
    
    def insert_single(self, values):
        self.session.execute_async(f"INSERT INTO {self.table_name} (cluster_id, size, addresses) VALUES (%s, %s, %s)", values)

    def batch_init(self):
        try:
            self.batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_ONE,request_timeout=60) #batch_type=BatchType.UNLOGGED)
            if self.input_group is None:
                self.input_group = self.session.prepare(f"INSERT INTO {self.table_name} (cluster_id, size, addresses)  VALUES (?, ?, ?)")
        except:
            time.sleep(0.1)
            ct = datetime.utcnow()
            with open(file="errors",mode='w') as f:
                f.write(f"{ct} ERR INIT ADDRESS BALANCE\n")
            self.batch_init()
            

    def batch_add(self, values):
        self.batch.add(self.input_group,values)

    def batch_execute(self):
        self.session.execute_async(self.batch)