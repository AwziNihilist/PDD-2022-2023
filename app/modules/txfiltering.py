from typing import List, Set

def _aux_get_vins(blockTx: dict) -> dict:
    return blockTx["vin"]

def _isAddressInner(input: dict) -> bool:
    return input["isAddress"]

def _aux_get_isAddresses(inputs: List[dict]) -> List[dict]:
    return filter(_isAddressInner, inputs)

def _get_addressInner(input: dict) -> str:
    return input["addresses"][0]

def _aux_get_addresses(inputs: list) -> List[str]:
    return map(_get_addressInner, inputs)

def _aux_get_addressesSet(addresses: List[str]) -> set:
    return set(addresses)

def _aux_filter_hasMoreThanOne(addressSet: set) -> bool:
    return len(addressSet) > 1

#def filterDataComplex(blockTxs) -> List[List[str]]:
#    return list(filter(lambda j: len(set(j)) > 1, map(lambda y: [item for sublist in [z["addresses"] for z in y if z["isAddress"]==True] for item in sublist], [x["vin"] for x in blockTxs])))

def filterDataWithAuxiliaries(blockTxs: list) -> List[Set[str]]:
    data = map(_aux_get_vins, blockTxs)
    data = map(_aux_get_isAddresses, data)
    data = map(_aux_get_addresses, data)
    data = map(_aux_get_addressesSet, data)
    data = filter(_aux_filter_hasMoreThanOne, data)
    return list(data)