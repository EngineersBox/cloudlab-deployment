from provisioner.application import app
from provisioner.application.app import AbstractApplication, ApplicationVariant, LOCAL_PATH
from provisioner.application.variant.cassandra import CassandraApplication
from provisioner.application.variant.scylla import ScyllaApplication
from provisioner.application.variant.mongodb import MongoDBApplication
from provisioner.application.variant.elasticsearch import ElasticsearchApplication
from provisioner.application.variant.hbase import HBaseApplication
from provisioner.collector.collection_config import CollectionConfiguration
from provisioner.collector.variant.cassandra import CassandraCollectionConfig
from provisioner.collector.variant.elasticsearch import ElasticsearchCollectionConfig
from provisioner.collector.variant.hbase import HBaseCollectionConfig
from provisioner.collector.variant.mongodb import MonogDBCollectionConfig
from provisioner.collector.variant.scylla import ScyllaCollectionConfig
from provisioner.docker import DockerConfig
from provisioner.structure.cluster import Cluster
from provisioner.structure.node import Node
from provisioner.provisioner import TopologyProperties
from provisioner.utils import catToFile, chmod, mkdir
import geni.portal as portal

OTEL_JMX_COLLECTION_INTERVAL_MS = 500
OTEL_CONTAINER_LOCAL_PATH = "/otel-lgtm"

COLLECTION_CONFIGS: dict[ApplicationVariant, type[CollectionConfiguration]] = {
    CassandraApplication.variant(): CassandraCollectionConfig,
    ElasticsearchApplication.variant(): ElasticsearchCollectionConfig,
    HBaseApplication.variant(): HBaseCollectionConfig,
    MongoDBApplication.variant(): MonogDBCollectionConfig,
    ScyllaApplication.variant(): ScyllaCollectionConfig,
}

class OTELCollector(AbstractApplication):
    ycsb_repository: str
    ycsb_commit_like: str
    cluster_application: str

    def __init__(self, version: str, docker_config: DockerConfig):
        super().__init__(version, docker_config)

    @classmethod
    def variant(cls) -> ApplicationVariant:
        return ApplicationVariant.OTEL_COLLECTOR

    def preConfigureClusterLevelProperties(self,
                                           cluster: Cluster,
                                           params: portal.Namespace,
                                           topology_properties: TopologyProperties) -> None:
        super().preConfigureClusterLevelProperties(
            cluster,
            params,
            topology_properties
        )
        self.ycsb_repository = params.ycsb_repository
        self.ycsb_commit_like = params.ycsb_commit_like
        self.cluster_application = params.application

    def writeYCSBBenchmarkingConfiguration(self, node: Node) -> dict[str, str]:
        base_profile_path=f"{LOCAL_PATH}/ycsb/base_profile.dat"
        app_variant: ApplicationVariant = ApplicationVariant(ApplicationVariant._member_map_[str(self.cluster_application).upper()])
        profile_content = COLLECTION_CONFIGS[app_variant].createYCSBBaseProfileProperties(
            node,
            self.cluster,
            self.topology_properties
        )
        catToFile(node, base_profile_path, profile_content)
        return COLLECTION_CONFIGS[app_variant].createBenchmarkingProperties(
            node,
            self.cluster,
            self.topology_properties
        )

    def writeTargetAppCollectionConfigs(self, node: Node) -> None:
        app_variant: ApplicationVariant = ApplicationVariant(ApplicationVariant._member_map_[str(self.cluster_application).upper()])
        COLLECTION_CONFIGS[app_variant].writeJMXCollectionConfig(
            node,
            self.topology_properties,
            OTEL_JMX_COLLECTION_INTERVAL_MS,
            OTEL_CONTAINER_LOCAL_PATH
        )

    def createDirectories(self, node: Node) -> None:
        dirs = ["hostmetrics", "kernel"]
        for dir in dirs:
            mkdir(node, f"/var/log/otel/{dir}", create_parent=True)
        chmod(node, f"/var/log/otel", 0o766, recursive=True)

    def nodeInstallApplication(self, node: Node) -> None:
        # TODO: Need to update the collector config with 
        #       JMX consumers for each of the cluster nodes
        super().nodeInstallApplication(node)
        self.unpackTar(node, use_pg_install=False)
        self.cloneRepo(
            node,
            self.ycsb_repository,
            f"{LOCAL_PATH}/ycsb",
            self.ycsb_commit_like
        )
        self.writeTargetAppCollectionConfigs(node)
        node_ips = []
        for cluster_node in self.topology_properties.db_nodes.values():
            node_ips.append(cluster_node.getInterfaceAddress())
        properties = {
            "INVOKE_INIT": True,
            "CLUSTER_APPLICATION_VARIANT": self.cluster_application,
            "NODE_IPS": node_ips
        }
        properties.update(self.writeYCSBBenchmarkingConfiguration(node))
        self.bootstrapNode(
            node,
            properties,
            [
                ".*opentelemetry.*"
            ]
        )
