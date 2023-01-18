
import aiohttp
import asyncio 
import time
from multiprocessing import Manager, Process
import os
from threading import Thread
from queue import Full, Empty, Queue

class AsyncQueue():
    
    def __init__(self) -> None:
        self.task_queue = asyncio.Queue()
        self.tasks = {}
        self.completed = asyncio.Queue()
        self.lastBlocksToComplete = []
        self.pendingBlockBooks = 0

    async def _task_runner(self):
        while len(self.tasks) > 0 or self.pendingBlockBooks > 0:
            #print("loading complete", len(self.tasks), len(self.lastBlocksToComplete))
            task = await self.completed.get()
            completedBlockId = await task
            if completedBlockId in self.lastBlocksToComplete:
                self.lastBlocksToComplete.remove(completedBlockId)
            del self.tasks[completedBlockId]

    async def enqueue(self, blockHeight, coro: asyncio.coroutine):
        task = asyncio.ensure_future(coro)
        self.tasks[blockHeight] = task
        task.add_done_callback(self.completed.put_nowait)
        # await self.task_queue.put(task)
    
    async def start_runner(self):
        await self._task_runner()
        # await self.task_queue.join()

    def set_lastBlocksToComplete(self,lastBlocksToComplete):
        self.lastBlocksToComplete = lastBlocksToComplete

    def add_pendingBlockBooks(self):
        self.pendingBlockBooks += 1

    def dec_pendingBlockBooks(self):
        self.pendingBlockBooks -= 1

       
    
class BlockbookInfo():
    def __init__(self, url: str, perf: int, limit: int ) -> None:
        self.url = url
        self.perf = perf
        self.TCPlimit = limit

class BlockbookFetcher():

    def __init__(self, publisherQueue, asyncQueue: AsyncQueue, numberOfInstances: int, processIndex, blockbook: BlockbookInfo, blockstart, blockend) -> None:
        self.blockstart = blockstart
        self.blockend = blockend
        self.publisherQueue = publisherQueue
        self.asyncQueue = asyncQueue
        self.numberOfInstances = numberOfInstances
        self.blockbook = blockbook
        self.nextBlockToFetch = blockstart + processIndex

        #self.worker_q = Queue()
        #Thread(target=xd, args=(self.worker_q,publisherQueue,)).start()


        self.pubTime = 0

        asyncQueue.add_pendingBlockBooks()
        
        timeout = aiohttp.ClientTimeout(total=0)
        connector = aiohttp.TCPConnector(limit=int(blockbook.TCPlimit/numberOfInstances))
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def stop(self):
        #self.worker_q.put(None)
        await self.session.close()
    
    def __str__(self) -> str:
        return f"B_P: {self.blockbook.url}; {self.blockstart} -> {self.blockend}; next: {self.nextBlockToFetch}, timeSpendWaiting: {self.pubTime}s"

    def _get_nextBlockHeight(self):

        if self.nextBlockToFetch is None:
            return None

        nextBlockToFetch = self.nextBlockToFetch
        self.nextBlockToFetch += self.numberOfInstances
        
        if self.nextBlockToFetch > self.blockend:
            self.nextBlockToFetch = None
            print(self)
            self.asyncQueue.dec_pendingBlockBooks()

        return nextBlockToFetch

    async def _fetchPage(self, url: str):
        #print("\t", "GET", url)
        try:
            async with self.session.get(url) as response:
                return await response.json()
        except:
            print(f"B_F: ERROR! at {url}! Retrying!")
            return await self._fetchPage(url)

    
    async def _getBlock(self, blockHeight):
        
        #print(f"Fetching {blockHeight} by {os.getpid()} from {self.blockbook.url}")
        blockData = await self._fetchPage(f"{self.blockbook.url}/api/v2/block/{blockHeight}?page=1")
        totalPages = blockData["totalPages"]

        for pageId in range(2, totalPages + 1):
            pageContent = await self._fetchPage(f"{self.blockbook.url}/api/v2/block/{blockHeight}?page={pageId}")
            blockData["txs"].extend(pageContent["txs"])

        #published finished block

        start = time.time()
        #print(f"Publishing B#{blockHeight}")
        self.publisherQueue.publish(blockData)
        
        #self.async_put(blockData)
        #self.worker_q.put(blockData)
        #self.async_put(blockData)
        #t = Thread(target=self.async_put, args=('blockData'))
        #t.start()
        
        self.pubTime += time.time() - start
        #print(f"Published B#{blockHeight}")
        #print(f"Published B#{blockHeight} in {time.time() - start}")
        
        #print(f"Block {blockheight} obtained by {os.getpid()} from {self.blockbook.url}")
        nextBlock = self._get_nextBlockHeight()
        
        #If next block is not out of range
        if nextBlock is not None:
            await self.asyncQueue.enqueue(nextBlock, self._getBlock(nextBlock))

        return blockHeight

    async def start(self):
        for _ in range(int(self.blockbook.TCPlimit/self.numberOfInstances)):
            nextBlock = self._get_nextBlockHeight()
            if nextBlock is not None:
                await self.asyncQueue.enqueue(nextBlock, self._getBlock(nextBlock))

BLOCKBOOKS = [


]

def spawnBlockPublishers(numOfProcesses: int, queue, blocks: dict,):
    processes = []
    for i in range(numOfProcesses):
        p = Process(target=_blockPublisher, args=(queue,BLOCKBOOKS,blocks,i,numOfProcesses,))
        p.start()
        processes.append(p)
    return processes


async def start(fetchers: list[BlockbookFetcher], asyncQueue: AsyncQueue):
    
    #Start block fetchers
    for fetcher in fetchers:
        await fetcher.start()
    
    #Start task runner
    await asyncQueue.start_runner()

    for fetcher in fetchers:
        #print(fetcher)
        await fetcher.stop()
    


def _blockPublisher(publisherQueue, blockbooks: list[dict], blocks: dict, processIndex, numberOfProccesses):

    async def main():
        totalNumberOfBlocks = (blocks['end'] - blocks['start']) + 1
        #print(totalNumberOfBlocks)
        
        totalPerformanceOfBlockbooks = 0
        
        for blockbook in blockbooks:
            totalPerformanceOfBlockbooks += blockbook['perf']

        asyncQueue = AsyncQueue()
        arr_of_last_blocks = []

        fetcherInstances = []
        startingBlock = blocks['start']
        
        #calculate ratio of performance for each blockbook
        for index, blockbook in enumerate(blockbooks):
            numberOfBlocksForThisBlockbook = int((totalNumberOfBlocks/totalPerformanceOfBlockbooks) * blockbook['perf'])
            blockbookInfo = BlockbookInfo(blockbook['url'], blockbook['perf'], blockbook['session_limit'])
            #print(f"BLocks ill fetch: {numberOfBlocksForThisBlockbook}")
            first_block = startingBlock
            last_block = startingBlock + numberOfBlocksForThisBlockbook - 1

            if index == len(blockbooks) - 1:
                last_block = blocks['end']
                #print(f"Ive decided to fetch all until {last_block}")
            arr_of_last_blocks.append(last_block)
            fetcher = BlockbookFetcher(publisherQueue, asyncQueue, numberOfProccesses, processIndex, blockbookInfo, first_block, last_block)
            fetcherInstances.append(fetcher)
            startingBlock += numberOfBlocksForThisBlockbook

        #for fetcher in fetcherInstances:
            #print(fetcher)
            #a = fetcher._get_nextBlockToFetch()
            #print(f"--Blocks ill fetch--")
            #while a is not None:
            #    print(a)
            #    a = fetcher._get_nextBlockToFetch()
            #del fetcher
        asyncQueue.set_lastBlocksToComplete(arr_of_last_blocks)
        #return 
        #for fetcher in fetcherInstances:
        #   fetcher.start()

        await start(fetcherInstances, asyncQueue)
    
    asyncio.get_event_loop().run_until_complete(main())

    #print("CLOSING")
    #publisherQueue.publish(None)
    #publisherQueue.close_all()

    #publisherQueue.endItAll()

    # asyncio.run(start(fetcherInstances, asyncQueue)) #TODO PREDELAT

    #asyncQueue.start()

    #TODO HOW TO DEAL WITH END? RUNNER HAS TO RECEIVE A STOP FROM ALL FETCHERS
    #TODO CALL DEL ON EVERYTHING
