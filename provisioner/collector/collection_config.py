from abc import ABC, abstractmethod
from provisioner.structure.cluster import Cluster
from provisioner.structure.node import Node
from provisioner.provisioner import TopologyProperties
import geni.portal as portal

class CollectionConfiguration(ABC):

    @classmethod
    @abstractmethod
    def writeJMXCollectionConfig(cls,
                                 node: Node,
                                 topology_properties: TopologyProperties,
                                 otel_collection_interval: int,
                                 otel_container_local_path) -> None:
        pass

    @classmethod
    @abstractmethod
    def createYCSBBaseProfileProperties(cls,
                                        node: Node,
                                        cluster: Cluster,
                                        topology_properties: TopologyProperties) -> str:
        pass

    @classmethod
    @abstractmethod
    def createBenchmarkingProperties(cls,
                                    node: Node,
                                    cluster: Cluster,
                                    params: portal.Namespace,
                                    topology_properties: TopologyProperties) -> dict[str, str]:
        pass
