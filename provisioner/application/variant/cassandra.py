import math
import geni.portal as portal
from typing import Optional
from geni.rspec import pg
from provisioner.application.app import AbstractApplication, ApplicationVariant, LOCAL_PATH, USERNAME, GROUPNAME
from provisioner.docker import DockerConfig
from provisioner.parameters import Parameter, ParameterGroup
from provisioner.structure.node import Node
from provisioner.structure.rack import Rack
from provisioner.structure.cluster import Cluster
from provisioner.provisioner import TopologyProperties
from provisioner.utils import catToFile, chmod, chown, mkdir, ifaceForIp, sed

# CASSANDRA_YAML_DEFAULT_PROPERTIES: dict[str, Any] = {
#     "cluster_name": "Cassandra Cluster",
#     "num_tokens": 16,
#     "prepared_statements_cache_size": "",
#     "row_cache_size": "0MiB",
#     "row_cache_save_period": "0s",
#     "commit_log_segment_size": "32MiB",
#     "commit_log_disk_access_mode": "auto",
#     "commit_log_total_space": "8192MiB",
#     "seed_node_ips": "",
#     "concurrent_reads": 32,
#     "concurrent_writes": 32,
#     "concurrent_counter_writes": 32,
#     "networking_cache_size": "128MiB",
#     "file_cache_enabled": "false",
#     "file_cache_size": "512MiB",
#     "buffer_pool_use_heap_if_exhausted": "false",
#     "memtable_heap_space": "",
#     "memtable_offheap_space": "",
#     "memtable_allocation_type": "heap_buffers",
#     "memtable_flush_writers": 2,
#     "trickle_fsync": "false",
#     "trickle_fsync_interval": "10240KiB",
#     "listen_address": "127.0.0.1",
#     "boradcast_address": "127.0.0.1",
#     "rpc_address": "0.0.0.0",
#     "broadcast_rpc_address": "127.0.0.1",
#     "sstable_scaling_parameters": "T4",
#     "sstable_target_size": "1GiB",
#     "concurrent_compactors": 16,
#     "compaction_throughput": "64MiB/s",
#     "sstable_preemptive_open_interval": "50MiB",
#     "endpoint_snitch": "GossipingPropertyFileSnitch",
#     "internode_compression": "dc",
#     "internode_dc_tcp_nodelay": "false"
# }

class CassandraApplication(AbstractApplication):
    all_ips: list[pg.Interface] = []
    # Node Ids to node interfaces
    seeds: dict[str, pg.Interface] = {}
    has_init = False
    ycsb_rf: int = 0
    heap_size: Optional[str] = None

    def __init__(self, version: str, docker_config: DockerConfig):
        super().__init__(version, docker_config)

    @classmethod
    def variant(cls) -> ApplicationVariant:
        return ApplicationVariant.CASSANDRA

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
        self.determineSeedNodes(cluster, params)
        self.ycsb_rf = params.cassandra_ycsb_rf
        self.heap_size = params.application_heap_size

    def determineSeedNodes(self, cluster: Cluster, params: portal.Namespace) -> None:
        # Spread seeds across DCs to ensure at least 1 per DC.
        # Seeds within DCs should be spread across racks too.
        self.all_ips = [node.interface for node in cluster.nodesGenerator()]
        nodes_per_dc: int = params.racks_per_dc * params.nodes_per_rack
        seeds_per_dc: int = int(math.log2(nodes_per_dc))
        for dc in cluster.datacentres.values():
            racks: list[Rack] = list(dc.racks.values())
            for i in range(seeds_per_dc):
                rack: Rack = racks[i % len(racks)]
                for node in rack.nodes.values():
                    if (node.id in self.seeds):
                        continue
                    self.seeds[node.id] = node.interface
                    break

    def writeRackDcProperties(self, node: Node) -> None:
        _, dc, rack = self.cluster.inverse_topology[node.id]
        properties = f"""# DC and Rack specification of this node
dc={dc}
rack={rack}
"""
        catToFile(
            node,
            f"{LOCAL_PATH}/config/cassandra/cassandra-rackdc.properties",
            properties
        )

    def writeTopologyProperties(self, node: Node) -> None:
        _, default_dc, default_rack = list(self.cluster.inverse_topology.values())[0]
        properties = f"""# Mappings of Node IP=DC:Rack
# Default mapping for unknown nodes
default={default_dc}:{default_rack}
"""
        for node1, (_, dc, rack) in self.cluster.inverse_topology.items():
            properties += f"\n{self.topology_properties.db_nodes[node1].getInterfaceAddress()}={dc}:{rack}"
        catToFile(
            node,
            f"{LOCAL_PATH}/config/cassandra/cassandra-topology.properties",
            properties
        )

    def writeCassandraEnvProperties(self, node: Node) -> None:
        sed(
            node,
            {
                "@@RMI_HOSTNAME@@": node.getInterfaceAddress(),
                "@@HEAP_SIZE@@": f"{self.heap_size}"
            },
            f"{LOCAL_PATH}/config/cassandra/cassandra-env.sh"
        )

    def writeCassandraYamlProperties(self, node: Node) -> None:
        formatted_seeds = []
        for seed in self.seeds.values():
            seed_address = seed.addresses[0].address
            formatted_seeds.append(f"{seed_address}:7000")
        csv_seeds = ",".join(formatted_seeds)
        sed(
            node,
            {
                "@@SEED_IPS@@": csv_seeds,
                "@@NODE_IFACE@@": f"$({ifaceForIp(node.getInterfaceAddress())})",
                "@@NODE_ADDRESS@@": node.getInterfaceAddress()
            },
            f"{LOCAL_PATH}/config/cassandra/cassandra.yaml"
        )

    def writeCassandraOTELProperties(self, node: Node) -> None:
        sed(
            node,
            {
                "OTEL_SERVICE_NAME": f"{self.variant()}-{node.id}",
                "NODE_ID": node.id
            },
            f"{LOCAL_PATH}/config/cassandra/otel.properties"
        )

    def createDirectories(self, node: Node) -> None:
        dirs = ["data", "logs"]
        for dir in dirs:
            mkdir(node, f"/var/lib/cluster/{dir}", True)
        chmod(node, f"/var/lib/cluster", 0o777, recursive=True)
        chown(node, f"/var/lib/cluster", USERNAME, GROUPNAME, recursive=True)

    def nodeInstallApplication(self, node: Node) -> None:
        super().nodeInstallApplication(node)
        self.unpackTar(node)
        self.writeRackDcProperties(node)
        self.writeTopologyProperties(node)
        self.writeCassandraEnvProperties(node)
        self.writeCassandraYamlProperties(node)
        self.writeCassandraOTELProperties(node)
        self.createDirectories(node)
        invoke_init_script = False
        if node.id in self.seeds and not self.has_init:
            invoke_init_script = True
        self.bootstrapNode(
            node,
            {
                "NODE_ALL_IPS": [f"{iface.addresses[0].address}" for iface in self.all_ips],
                "SEED_NODE": node in self.seeds,
                "INVOKE_INIT": invoke_init_script,
                "DC_COUNT": len(self.cluster.datacentres),
                "YCSB_RF": self.ycsb_rf,
                # FIXME: the docker-entrypoint.sh script that is run when the Cassandra container starts
                #        will try to replace all the ip fields (seeds, listen_address, etc) with the first
                #        non-localhost IP it can find (which will always be wrong for us). It won't do this
                #        if the env vars corresponding to those fields are set. Either set those or just
                #        publish a new image that doesn't do this stuff on startup.
                "CASSANDRA_RPC_ADDRESS": node.getInterfaceAddress(),
                "CASSANDRA_LISTEN_ADDRESS": node.getInterfaceAddress(),
                "CASSANDRA_BROADCAST_ADDRESS": node.getInterfaceAddress(),
                "CASSANDRA_BROADCAST_RPC_ADDRESS": node.getInterfaceAddress(),
                "CASSANDRA_SEEDS": ",".join([f"{seed.addresses[0].address}:7000" for seed in self.seeds.values()])
            },
            [
                ".*cassandra.*"
            ]
        )

class CassandraParameters(ParameterGroup):

    def __init__(self):
        super().__init__(parameters=[
            Parameter(
                name="cassandra_ycsb_rf",
                description="Replication factor for YCSB keyspace",
                typ=portal.ParameterType.INTEGER,
                required=False,
                defaultValue=0
            ),
        ])

    def validate(self, params: portal.Namespace) -> None:
        super().validate(params)
        nodes_per_dc = params.racks_per_dc * params.nodes_per_rack
        if params.cassandra_ycsb_rf == 0:
            params.cassandra_ycsb_rf = nodes_per_dc
        elif params.cassandra_ycsb_rf > nodes_per_dc:
            portal.context.reportError(portal.ParameterError(
                f"Replication factor {params.cassandra_ycsb_rf} must be less than or equal to number of nodes in dc {nodes_per_dc}",
                ["cassandra_ycsb_rf"]
            ))

    @classmethod
    def name(cls) -> str:
        return "Cassandra"

    @classmethod
    def id(cls) -> str:
        return "cassandra"

CASSANDRA_PARAMETERS: ParameterGroup = CassandraParameters()
