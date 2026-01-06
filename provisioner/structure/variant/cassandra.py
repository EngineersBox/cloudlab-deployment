from enum import Enum

from provisioner.structure.topology_assigner import InverseProvisioningTopology, ProvisioningTopology, TopologyAssigner

class CassandraNodeRole(Enum):
    Data = "data"

    def __str__(self) -> str:
        return "%s" % self.value

class CassandraTopologyAssigner(TopologyAssigner):

    @classmethod
    def constructTopology(cls, dcs: int, racks_per_dc: int, nodes_per_rack: int) -> tuple[ProvisioningTopology, InverseProvisioningTopology]:
        topology: ProvisioningTopology = {}
        inverse_topology: InverseProvisioningTopology = {}
        node_id = 0
        for dc_id in range(dcs):
            dc_name = f"dc-{dc_id}"
            dc = topology.setdefault(dc_name, {})
            for rack_id in range(racks_per_dc):
                rack_name = f"rack-{rack_id}"
                rack = dc.setdefault(rack_name, {})
                for _ in range(nodes_per_rack):
                    node_name = f"D{dc_id}R{rack_id}N{node_id}"
                    roles = rack.setdefault(node_name, [str(CassandraNodeRole.Data)])
                    inverse_topology[node_name] = (roles, dc_name, rack_name)
                    node_id += 1
        return (topology, inverse_topology)
