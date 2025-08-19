import geni.portal as portal
from typing import Tuple
from geni.rspec import pg
from provisioner.application.app import AbstractApplication, ApplicationVariant
from provisioner.docker import DockerConfig
from provisioner.structure.cluster import Cluster
from provisioner.structure.datacentre import DataCentre
from provisioner.structure.node import Node
from provisioner.structure.rack import Rack
from provisioner.topology import TopologyProperties

class HBaseApplication(AbstractApplication):
    all_ips: list[pg.Interface] = []
    topology: dict[Node, Tuple[DataCentre, Rack]]
    
    def __init__(self, version: str, docker_config: DockerConfig):
        super().__init__(version, docker_config)

    @classmethod
    def variant(cls) -> ApplicationVariant:
        return ApplicationVariant.HBASE

    def preConfigureClusterLevelProperties(self,
                                           cluster: Cluster,
                                           params: portal.Namespace,
                                           topology_properties: TopologyProperties) -> None:
        super().preConfigureClusterLevelProperties(
            cluster,
            params,
            topology_properties
        )
        self.cluster = cluster

    def nodeInstallApplication(self, node: Node) -> None:
        super().nodeInstallApplication(node)
        # TODO: Implement this
