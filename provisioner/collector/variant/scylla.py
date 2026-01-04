from provisioner.structure.cluster import Cluster
from provisioner.structure.node import Node
from provisioner.provisioner import TopologyProperties
from provisioner.utils import catToFile, chmod
from provisioner.application.app import ApplicationVariant, LOCAL_PATH
from provisioner.collector.collection_config import CollectionConfiguration

class ScyllaCollectionConfig(CollectionConfiguration):

    @classmethod
    def writeJMLCollectionConfig(cls,
                                 npode: Node,
                                 otel_topology_properties: TopologyProperties,
                                 otel_collection_interval: int,
                                 otel_container_local_path) -> None:
        pass

    @classmethod
    def createYCSBBaseProfileProperties(cls,
                                        node: Node,
                                        cluster: Cluster,
                                        topology_properties: TopologyProperties) -> str:
        all_ips: list[str] = []
        for cluster_node in topology_properties.db_nodes.values():
            all_ips.append(cluster_node.getInterfaceAddress())
        return f"""
        scylla.hosts={",".join(all_ips)}
        port=9042
        """

    @classmethod
    def createBenchmarkingProperties(cls,
                                    node: Node,
                                    cluster: Cluster,
                                    topology_properties: TopologyProperties) -> dict[str, str]:
        return {}
