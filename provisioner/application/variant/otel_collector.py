from provisioner.application.app import AbstractApplication, ApplicationVariant, ServiceStartTiming, LOCAL_PATH
from provisioner.docker import DockerConfig
from provisioner.structure.cluster import Cluster
from provisioner.structure.node import Node
from provisioner.provisioner import TopologyProperties
from provisioner.utils import catToFile, chmod
import geni.portal as portal
import geni.rspec.pg as pg

OTEL_JMX_COLLECTION_INTERVAL_MS = 500
OTEL_CONTAINER_LOCAL_PATH = "/otel-lgtm"

class OTELCollector(AbstractApplication):
    ycsb_version: str
    cluster_application: str

    def __init__(self, version: str, docker_config: DockerConfig):
        super().__init__(version, docker_config)

    @classmethod
    def variant(cls) -> ApplicationVariant:
        return ApplicationVariant.OTEL_COLLECTOR

    def preConfigureClusterLevelProperties(self,
                                           cluster: Cluster,
                                           params: portal.Namespace,
                                           topologyProperties: TopologyProperties) -> None:
        super().preConfigureClusterLevelProperties(
            cluster,
            params,
            topologyProperties
        )
        self.ycsb_version = params.ycsb_version
        self.cluster_application = params.application

    def createYCSBBaseProfile(self, node: Node) -> None:
        base_profile_path=f"{LOCAL_PATH}/ycsb-{self.ycsb_version}/base_profile.dat"
        app_variant: ApplicationVariant = ApplicationVariant(ApplicationVariant._member_map_[str(self.cluster_application).upper()])
        all_ips: list[str] = []
        for cluster_node in self.topologyProperties.db_nodes:
            all_ips.append(cluster_node.getInterfaceAddress())
        profile_content: str = ""
        if app_variant == ApplicationVariant.CASSANDRA:
            profile_content = f"""
            hosts={",".join(all_ips)}
            port=9042
            """
        elif app_variant == ApplicationVariant.ELASTICSEARCH:
            # TODO: Complete this
            raise NotImplementedError("Elasticsearch YCSB profile not implemented")
        elif app_variant == ApplicationVariant.MONGO_DB:
            # TODO: Complete this
            raise NotImplementedError("MongoDB YCSB profile not implemented")
        elif app_variant == ApplicationVariant.SCYLLA:
            profile_content = f"""
            scylla.hosts={",".join(all_ips)}
            port=9042
            """
        else:
            raise RuntimeError(f"Invalid application variant: {self.cluster_application}")
        catToFile(node, base_profile_path, profile_content)

    def writeCassandraJMXCollectionConfig(self, node: Node) -> None:
        jmx_services = """#!/usr/bin/env bash
JMX_PORT=7199

declare -A JMX_NAMES
declare -A JMX_PATHS
declare -A JMX_IPS
"""
        i = 0
        for cluster_node in self.topologyProperties.db_nodes:
            node_addr = cluster_node.getInterfaceAddress()
            jmx_config = f"""# OTEL JMX Collection Config
otel.metrics.exporter=otlp
otel.exporter.otlp.endpoint=http://{node.getInterfaceAddress()}:4318
otel.jmx.groovy.script={OTEL_CONTAINER_LOCAL_PATH}/jmx.groovy
otel.jmx.service.url=service:jmx:rmi://{node_addr}/jndi/rmi://{node_addr}:7199/jmxrmi
otel.jmx.remote.registry.ssl=false
otel.jmx.interval.milliseconds={OTEL_JMX_COLLECTION_INTERVAL_MS}
otel.exporter.otlp.protocol=http/protobuf
otel.service.name={ApplicationVariant.CASSANDRA}-{cluster_node.id}
otel.resource.attributes=application={ApplicationVariant.CASSANDRA},node={cluster_node.id}
"""
            instance_path = f"{LOCAL_PATH}/config/otel/jmx_configs/jmx_{cluster_node.id}.properties"
            container_path = f"{OTEL_CONTAINER_LOCAL_PATH}/jmx_configs/jmx_{cluster_node.id}.properties"

            jmx_services += f"\nJMX_NAMES[{i}]=\"{cluster_node.id}\""
            jmx_services += f"\nJMX_PATHS[{i}]=\"{container_path}\""
            jmx_services += f"\nJMX_IPS[{i}]=\"{node_addr}\""
            
            catToFile(node, instance_path, jmx_config)
            chmod(node, instance_path, 0o777)
            i += 1
        catToFile(node, f"{LOCAL_PATH}/config/otel/jmx_services", jmx_services)
        chmod(node, f"{LOCAL_PATH}/config/otel/jmx_services", 0o777)

    def writeTargetAppCollectionConfigs(self, node: Node) -> None:
        app_variant: ApplicationVariant = ApplicationVariant(ApplicationVariant._member_map_[str(self.cluster_application).upper()])
        if app_variant == ApplicationVariant.CASSANDRA:
            self.writeCassandraJMXCollectionConfig(node)
        elif app_variant == ApplicationVariant.ELASTICSEARCH:
            # TODO: Implement this
            raise NotImplementedError("Elasticsearch collection configs not implementated")
        elif app_variant == ApplicationVariant.MONGO_DB:
            # TODO: Implement this
            raise NotImplementedError("MongoDB collection configs not implementated")
        elif app_variant == ApplicationVariant.SCYLLA:
            # TODO: Implement this
            raise NotImplementedError("Scylla collection configs not implementated")
        else:
            raise RuntimeError(f"Invalid application variant: {self.cluster_application}")

    def nodeInstallApplication(self, node: Node) -> None:
        # TODO: Need to update the collector config with 
        #       JMX consumers for each of the cluster nodes
        super().nodeInstallApplication(node)
        self.unpackTar(node)
        self.unpackTar(
            node,
            f"https://github.com/brianfrankcooper/YCSB/releases/download/{self.ycsb_version}/ycsb-{self.ycsb_version}.tar.gz"
        )
        self.writeTargetAppCollectionConfigs(node)
        node_ips = []
        for cluster_node in self.topologyProperties.db_nodes:
            node_ips.append(cluster_node.getInterfaceAddress())
        self.bootstrapNode(
            node,
            {
                "INVOKE_INIT": "true",
                "CLUSTER_APPLICATION_VARIANT": self.cluster_application,
                "CASSANDRA_NODE_IPS": "'" + ",".join(node_ips) + "'"
            },
            ServiceStartTiming.AFTER_INIT
        )
        self.createYCSBBaseProfile(node)
