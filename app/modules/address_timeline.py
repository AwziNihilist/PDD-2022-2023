class AddressTimeline():
    
    def __init__(self) -> None:
        self.address_to_change = dict()
        self.blocktime = 0
        self.blockheight = 0
        self._block_data = None
        
    def clear(self) -> None:
        self.address_to_change = dict()
        self.blocktime = 0
        self.blockheight = 0
        self._block_data = None

    def set_new_block(self, block):
        self.address_to_change = dict()
        self.blocktime = block['time']
        self.blockheight = block['height']
        self._block_data = block

    def get_blockheight(self) -> int:
        return self.blockheight

    def get_blocktime(self) -> int:
        return self.blocktime

    def parse_transactions(self):
        for tx in self._block_data['txs']:
            for input in tx['vin']:
                if not input['isAddress']:
                    continue
                address = input['addresses'][0]
                value = input['value']
                self.address_to_change[address] = self.address_to_change.get(address,0) - int(value)
            
            for output in tx['vout']:
                if not output['isAddress']:
                    continue
                address = output['addresses'][0]
                value = output['value']
                self.address_to_change[address] = self.address_to_change.get(address,0) + int(value)