from abc import ABC, abstractmethod

# dcs -> racks -> nodes -> roles
ProvisioningTopology = dict[str, dict[str, dict[str, list[str]]]]

class TopologyAssigner(ABC):
    
    @classmethod
    @abstractmethod
    def constructTopology(cls, dcs: int, racks_per_dc: int, nodes_per_rack: int) -> ProvisioningTopology:
        pass
