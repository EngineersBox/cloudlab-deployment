import geni.portal as portal
from dataclasses import dataclass, field
from typing import Iterator
from provisioner import topology
from provisioner.structure.node import Node
from provisioner.structure.rack import Rack
from provisioner.structure.datacentre import DataCentre
from provisioner.parameters import Parameter, ParameterGroup
from provisioner.structure.topology_assigner import InverseProvisioningTopology, ProvisioningTopology

@dataclass
class Cluster:
    topology: ProvisioningTopology = field(default_factory=dict)
    inverse_topology: InverseProvisioningTopology = field(default_factory=dict)
    datacentres: dict[str, DataCentre] = field(default_factory=dict)

    def racksGenerator(self) -> Iterator[Rack]:
        for dc in self.datacentres.values():
            for rack in dc.racks.values():
                yield rack

    def nodesGenerator(self) -> Iterator[Node]:
        for rack in self.racksGenerator():
            for node in rack.nodes.values():
                yield node

class ClusterParameterGroup(ParameterGroup):

    @classmethod
    def name(cls) -> str:
        return "Cluster"

    @classmethod
    def id(cls) -> str:
        return "cluster"

    def __init__(self):
        super().__init__(
            parameters=[
                Parameter(
                    name="dc_count",
                    description="Number of datacentres",
                    typ=portal.ParameterType.INTEGER,
                    defaultValue=1
                ),
                Parameter(
                    name="racks_per_dc",
                    description="Number of racks in each datacentre",
                    typ=portal.ParameterType.INTEGER,
                    defaultValue=1
                ),
                Parameter(
                    name="nodes_per_rack",
                    description="Number of nodes in each rack",
                    typ=portal.ParameterType.INTEGER,
                    defaultValue=1
                ),
                Parameter(
                    name="node_size",
                    description="Instance type to use for the nodes (Absent implies autoselect)",
                    typ=portal.ParameterType.STRING,
                    defaultValue=None
                ),
                Parameter(
                    name="node_disk_image",
                    description="Node disk image",
                    typ=portal.ParameterType.IMAGE,
                    defaultValue="urn:publicid:IDN+utah.cloudlab.us+image+cassandramulti7-PG0:ubuntu22-docker-java"
                ),
                Parameter(
                    name="vlan_type",
                    description="Shared VLAN used between experiments to expose traffic between the experiments",
                    typ=portal.ParameterType.STRING,
                    defaultValue=None
                )
            ]
        )

    def validate(self,params: portal.Namespace) -> None:
        super().validate(params)
        total_nodes = params.dc_count * params.racks_per_dc * params.nodes_per_rack
        if total_nodes < 1 or total_nodes > 9:
            portal.context.reportError(portal.ParameterError(
                "Node count must be in range [1,9]",
                ["node_count"]
            ))

CLUSTER_PARAMETERS: ParameterGroup = ClusterParameterGroup()
