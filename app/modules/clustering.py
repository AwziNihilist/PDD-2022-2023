from typing import List, Set
import time


class Clustering():
    
    def __init__(self) -> None:
        self.a_c = dict()
        self.c_a = dict()
        self.c_cRen = dict()
        self.c_cRenTo = dict()
        self.c_ranks = dict()

        self.cluster_alias = dict()
        self._cluster_id = 0

        self._times = []

    def _get_avg_time(self) -> float:
        if len(self._times) == 0:
            return 0
        
        return sum(self._times) / len(self._times)

    def __str__(self) -> str:
        return (
            f'Clusters: \n'
            f'{len(self.c_a)=}\n'
            f'{len(self.a_c)=}\n'
            f'Avg time: {self._get_avg_time()}s\n'
            f'Number of jobs: {len(self._times)}s\n'
            )
    
    def write_to_file(self):
        with open(file="ClusteringOutput",mode='w') as f:
            f.write(self.__str__())

    def reset_state(self):
        self.a_c = dict()
        self.c_a = dict()
        self.c_cRen = dict()
        self.c_cRenTo = dict()
        self.c_ranks = dict()

        self.cluster_alias = dict()
        self._cluster_id = 0

    def cluster_sets(self, sets_of_addresses):
        start = time.time()
        
        for set_of_addresses in sets_of_addresses:
            self.clusterAddressesWithExtraMemory(set_of_addresses)
        
        end = time.time()
        self._times.append(end-start)

    def _get_cluster_with_highest_rank(self, setOfCIds: set[str]) -> int:
        max = 0
        maxcId = None
        for cId in setOfCIds:
            rank = self.c_ranks[cId]
            if rank > max:
                max = rank
                maxcId = cId
        return maxcId

    def _get_random_item_from_set(set: set) -> str:
        for e in set:
            break
        return e
    
    def _get_next_cluster_id(self):
        to_returned = self._cluster_id
        self._cluster_id += 1
        return to_returned
    
    def normalizeDicts(self):
        for addressKey in self.a_c:
            clusterRename = self.c_cRen.get(self.a_c[addressKey])
            if clusterRename is not None:
                self.a_c[addressKey] = clusterRename
        
        self.c_cRen = dict()
        self.c_cRenTo = dict()

    def clusterAddressesWithExtraMemory(self, set_of_addresses):
        
        #set of all unique clusterIds
        uniqueClusterIds = set()
        
        #set of all addresses without cluster
        addressesWithoutCluster = set()
        
        #Fill up the set
        for addr in set_of_addresses:

            clusterId = self.a_c.get(addr)
            #Address has a cluster, get ID or Alias
            if clusterId is not None:
                cluster_renamedId = self.c_cRen.get(clusterId)
                if cluster_renamedId is not None:
                    clusterId = cluster_renamedId
                uniqueClusterIds.add(clusterId)
            else:
                addressesWithoutCluster.add(addr)

        #if none of the addresses is in a cluster
        if not uniqueClusterIds:

            newClusterId = self._get_next_cluster_id()

            #Create new cluster with these addresses
            self.c_a[newClusterId] = set_of_addresses
            
            #Assing this new clusterId to each address in addr_cluster dict
            for address in set_of_addresses:
                self.a_c[address] = newClusterId
            
            self.c_ranks[newClusterId] = len(set_of_addresses)
        
        #at least one address is a cluster already
        else:
            
            mergeClusterId = self._get_cluster_with_highest_rank(uniqueClusterIds)
            
            #Iterate over all clusters to merge
            for clusterIdToMerge in uniqueClusterIds:
                if clusterIdToMerge is not mergeClusterId:
                    
                    #merge mergeClusterId and clusterIdToMerge to mergeClusterId
                    self.c_a[mergeClusterId].update(self.c_a[clusterIdToMerge])

                    #update rank
                    self.c_ranks[mergeClusterId] += self.c_ranks[clusterIdToMerge]
                    
                    #Remove merged cluster
                    del self.c_a[clusterIdToMerge]

                    #Remove its rank
                    del self.c_ranks[clusterIdToMerge]
                    
                    #Get all clusters which are renamed to cluster that was just merged as a different one
                    clustersRenamedToMe = self.c_cRenTo.get(clusterIdToMerge)

                    #If this Cluster had any clusters renaming to him
                    if clustersRenamedToMe is not None:
                        #Update all of them with the new clusterId
                        for clusterToCorrect in clustersRenamedToMe:
                            self.c_cRen[clusterToCorrect] = mergeClusterId
                        #Add myself and all cluster that were renaming to me to  mergeClusterId
                        clustersToAdd = clustersRenamedToMe
                        clustersToAdd.add(clusterIdToMerge)
                        a = self.c_cRenTo.get(mergeClusterId)
                        if a is None:
                            a = set()
                        a.update(clustersToAdd)
                        self.c_cRenTo[mergeClusterId] = a
                        #Remove my rename as noone is renaming to me anymore
                        del self.c_cRenTo[clusterIdToMerge]
                    else:
                        #Not a single cluster was renaming to me, so just add mme as renamer to mergeClusterId
                        a = self.c_cRenTo.get(mergeClusterId)
                        if a is None:
                            a = set()
                        a.add(clusterIdToMerge)
                        self.c_cRenTo[mergeClusterId] = a

                    #Declare that I merged with mergeClusterId
                    self.c_cRen[clusterIdToMerge] = mergeClusterId
            
            #Addresses which do not have any cluster should have mergeClusterId
            for addressWithoutCluster in addressesWithoutCluster:
                self.a_c[addressWithoutCluster] = mergeClusterId


class ClusteringAliased(Clustering):
    
    def __init__(self) -> None:
        super().__init__()
        
        self._start_total = time.time()
        self.hash_addr = dict()

    def __str__(self) -> str:
        return (
            f'Clusters: \n'
            f'{len(self.c_a)=}\n'
            f'{len(self.a_c)=}\n'
            f'Avg time per job: {self._get_avg_time()}s\n'
            f'Number of jobs: {len(self._times)}\n'
            f'Time active: {time.time() - self._start_total}s\n'
            )

    def hash_to_address(self, hash) -> str:
        return self.hash_addr[hash]
        
    def cluster_sets(self, sets_of_addresses):
        start = time.time()
        
        for set_of_addresses in sets_of_addresses:
            aliased_address_set = set()
            for address_in_set in set_of_addresses:
                
                addressHash = hash(address_in_set)
                
                if addressHash not in self.hash_addr:
                    self.hash_addr[addressHash] = address_in_set
                else:
                    if address_in_set != self.hash_addr[addressHash]:
                        print(f'kolize{address_in_set} vs {self.hash_addr[addressHash]}')
                        self.hash_addr[address_in_set] = address_in_set
                        addressHash = address_in_set

                aliased_address_set.add(addressHash)
            
            self.clusterAddressesWithExtraMemory(aliased_address_set)
        
        end = time.time()
        self._times.append(end-start)
        

