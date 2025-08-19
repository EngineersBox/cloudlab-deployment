from abc import ABC, abstractmethod
from provisioner.structure.node import Node
from provisioner.provisioner import TopologyProperties

class CollectionConfiguration(ABC):

    @classmethod
    @abstractmethod
    def writeJMXCollectionConfig(cls,
                                 node: Node,
                                 otel_topology_properties: TopologyProperties,
                                 otel_collection_interval: int,
                                 otel_container_local_path) -> None:
        pass

    @classmethod
    @abstractmethod
    def createYCSBBaseProfileProperties(cls,
                                        node: Node,
                                        otel_topology_properties: TopologyProperties) -> str:
        pass
