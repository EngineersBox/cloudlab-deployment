from provisioner.structure.node import Node
from provisioner.structure.variant.hbase import HBaseNodeRole
from provisioner.provisioner import TopologyProperties
from provisioner.utils import catToFile, chmod, sed
from provisioner.application.app import ApplicationVariant, LOCAL_PATH
from provisioner.collector.collection_config import CollectionConfiguration

HBASE_ROLE_PORT_MAPPINGS: dict[HBaseNodeRole, list[tuple[str, int]]] = {
    HBaseNodeRole.HBASE_MASTER: [("otel_master.properties", 0)],
    HBaseNodeRole.HBASE_ZOOKEEPER: [("otel_zookeeper.properties", 0)],
    HBaseNodeRole.HDFS_NAME: [("otel_regionserver.properties", 0)]
}

class HBaseCollectionConfig(CollectionConfiguration):

    @classmethod
    def writeJMLCollectionConfig(cls,
                                 node: Node,
                                 otel_topology_properties: TopologyProperties,
                                 otel_collection_interval: int,
                                 otel_container_local_path: str) -> None:
        pass
#         jmx_port = 7199
#
#         jmx_services = f"""#!/usr/bin/env bash
# JMX_PORT={jmx_port}
#
# declare -A JMX_NAMES
# declare -A JMX_PATHS
# declare -A JMX_IPS
# """
#         i = 0
#         for cluster_node in otel_topology_properties.db_nodes.values():
#             node_addr = cluster_node.getInterfaceAddress()
#             for role in cluster_node.roles:
#                 base_role = HBaseNodeRole.__members__[role.upper()]
#                 mappings = HBASE_ROLE_PORT_MAPPINGS[base_role]
#                 if mappings == None:
#                     continue
#                 for entry in mappings:
#                     sed(
#                         cluster_node,
#                         {
#                             "@@JMX_PORT@@": f"{entry[1]}"
#                         },
#                         entry[0]
#                     )
#             jmx_config = f"""# OTEL JMX Collection Config
# otel.metrics.exporter=otlp
# otel.exporter.otlp.endpoint=http://{node.getInterfaceAddress()}:4318
# otel.jmx.groovy.script={otel_container_local_path}/jmx.groovy
# otel.jmx.service.url=service:jmx:rmi://{node_addr}/jndi/rmi://{node_addr}:{jmx_port}/jmxrmi
# otel.jmx.remote.registry.ssl=false
# otel.jmx.interval.milliseconds={otel_collection_interval}
# otel.exporter.otlp.protocol=http/protobuf
# otel.service.name={ApplicationVariant.HBASE}-{cluster_node.id}
# otel.resource.attributes=application={ApplicationVariant.HBASE},node={cluster_node.id}
# """
#             instance_path = f"{LOCAL_PATH}/config/otel/jmx_configs/jmx_{cluster_node.id}.properties"
#             container_path = f"{otel_container_local_path}/jmx_configs/jmx_{cluster_node.id}.properties"
#
#             jmx_services += f"\nJMX_NAMES[{i}]=\"{cluster_node.id}\""
#             jmx_services += f"\nJMX_PATHS[{i}]=\"{container_path}\""
#             jmx_services += f"\nJMX_IPS[{i}]=\"{node_addr}\""
#
#             catToFile(node, instance_path, jmx_config)
#             chmod(node, instance_path, 0o777)
#             i += 1
#         catToFile(node, f"{LOCAL_PATH}/config/otel/jmx_services", jmx_services)
#         chmod(node, f"{LOCAL_PATH}/config/otel/jmx_services", 0o777)

    @classmethod
    def createYCSBBaseProfileProperties(cls,
                                        node: Node,
                                        otel_topology_properties: TopologyProperties) -> str:
        return ""
