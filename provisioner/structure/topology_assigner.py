from abc import ABC, abstractmethod

# dcs -> racks -> nodes -> roles
ProvisioningTopology = dict[str, dict[str, dict[str, list[str]]]]
# nodes -> (roles, dc, rack)
InverseProvisioningTopology = dict[str, tuple[list[str], str, str]]

def findNodesWithRole(inverse_topology: InverseProvisioningTopology,
                      role: str,
                      short_circuit_first: bool = False) -> list[str]:
    result = []
    for node, (roles, _dc, _rack) in inverse_topology.items():
        if (not role in roles):
            continue
        result.append(node)
        if (short_circuit_first):
            break
    return result

class TopologyAssigner(ABC):
    
    @classmethod
    @abstractmethod
    def constructTopology(cls, dcs: int, racks_per_dc: int, nodes_per_rack: int) -> tuple[ProvisioningTopology, InverseProvisioningTopology]:
        pass
