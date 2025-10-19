from provisioner.structure.node import Node
from provisioner.provisioner import TopologyProperties
from provisioner.utils import catToFile, chmod
from provisioner.application.app import ApplicationVariant, LOCAL_PATH
from provisioner.collector.collection_config import CollectionConfiguration

class CassandraCollectionConfig(CollectionConfiguration):

    @classmethod
    def writeJMXCollectionConfig(cls,
                                 node: Node,
                                 otel_topology_properties: TopologyProperties,
                                 otel_collection_interval: int,
                                 otel_container_local_path) -> None:
        jmx_services = """#!/usr/bin/env bash
JMX_PORT=7199

declare -A JMX_NAMES
declare -A JMX_PATHS
declare -A JMX_IPS
"""
        i = 0
        for cluster_node in otel_topology_properties.db_nodes.values():
            node_addr = cluster_node.getInterfaceAddress()
            jmx_config = f"""# OTEL JMX Collection Config
otel.metrics.exporter=otlp
otel.exporter.otlp.endpoint=http://{node.getInterfaceAddress()}:4318
otel.jmx.groovy.script={otel_container_local_path}/jmx.groovy
otel.jmx.service.url=service:jmx:rmi://{node_addr}/jndi/rmi://{node_addr}:7199/jmxrmi
otel.jmx.remote.registry.ssl=false
otel.jmx.interval.milliseconds={otel_collection_interval}
otel.exporter.otlp.protocol=http/protobuf
otel.service.name={ApplicationVariant.CASSANDRA}-{cluster_node.id}
otel.resource.attributes=application={ApplicationVariant.CASSANDRA},node={cluster_node.id}
"""
            instance_path = f"{LOCAL_PATH}/config/otel/jmx_configs/jmx_{cluster_node.id}.properties"
            container_path = f"{otel_container_local_path}/jmx_configs/jmx_{cluster_node.id}.properties"

            jmx_services += f"\nJMX_NAMES[{i}]=\"{cluster_node.id}\""
            jmx_services += f"\nJMX_PATHS[{i}]=\"{container_path}\""
            jmx_services += f"\nJMX_IPS[{i}]=\"{node_addr}\""
            
            catToFile(node, instance_path, jmx_config)
            chmod(node, instance_path, 0o777)
            i += 1
        catToFile(node, f"{LOCAL_PATH}/config/otel/jmx_services", jmx_services)
        chmod(node, f"{LOCAL_PATH}/config/otel/jmx_services", 0o777)

    @classmethod
    def createYCSBBaseProfileProperties(cls,
                                        node: Node,
                                        otel_topology_properties: TopologyProperties) -> str:
        all_ips: list[str] = []
        for cluster_node in otel_topology_properties.db_nodes.values():
            all_ips.append(cluster_node.getInterfaceAddress())
        return f"""
        hosts={",".join(all_ips)}
        port=9042
        """
