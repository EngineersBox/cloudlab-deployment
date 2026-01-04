from typing import Tuple
import geni.portal as portal
import geni.rspec.pg as pg
import ipaddress
from provisioner.application.variant.hbase import HBaseApplication
from provisioner.net.network import NetworkManager
from provisioner.application.app import *
from provisioner.structure.node import Node
from provisioner.structure.cluster import Cluster
from provisioner.structure.rack import Rack
from provisioner.structure.datacentre import DataCentre
from provisioner.application.variant.cassandra import CassandraApplication
from provisioner.application.variant.mongodb import MongoDBApplication
from provisioner.application.variant.elasticsearch import ElasticsearchApplication
from provisioner.application.variant.scylla import ScyllaApplication
from provisioner.collector.collector import Collector
from provisioner.application.variant.otel_collector import OTELCollector
from provisioner.structure.topology_assigner import TopologyAssigner, ProvisioningTopology, InverseProvisioningTopology
from provisioner.structure.variant.cassandra import CassandraTopologyAssigner
from provisioner.structure.variant.hbase import HBaseTopologyAssigner
from provisioner.topology import TopologyProperties

APPLICATION_BINDINGS: dict[ApplicationVariant, type[AbstractApplication]] = {
    CassandraApplication.variant(): CassandraApplication,
    ElasticsearchApplication.variant(): ElasticsearchApplication,
    HBaseApplication.variant(): HBaseApplication,
    MongoDBApplication.variant(): MongoDBApplication,
    ScyllaApplication.variant(): ScyllaApplication,
}

APPLICATION_TOPOLOGY_ASSIGNERS: dict[ApplicationVariant, type[TopologyAssigner]] = {
    CassandraApplication.variant(): CassandraTopologyAssigner,
    HBaseApplication.variant(): HBaseTopologyAssigner,
}

class Provisioner:
    request: pg.Request
    params: portal.Namespace
    docker_config: DockerConfig

    __node_idx = 0

    def __init__(self, request: pg.Request, params: portal.Namespace):
        self.request = request
        self.params = params
        self.docker_config: DockerConfig = DockerConfig(
            username=self.params.github_username,
            token=self.params.github_token
        )

    def nodeProvision(self, name: str, roles: list[str]) -> Node:
        self.__node_idx += 1
        node_vm = pg.RawPC(name)
        node_vm.hardware_type = self.params.node_size
        node_vm.disk_image = self.params.node_disk_image
        self.request.addResource(node_vm)
        iface: pg.Interface = node_vm.addInterface(NetworkManager.CURRENT_PHYSICAL_INTERFACE)
        # iface.component_id = Provisioner.NODE_PHYSICAL_INTERFACE_FORMAT % i
        net_address: ipaddress.IPv4Address = NetworkManager.nextAddress()
        address: pg.IPv4Address = pg.IPv4Address(
            str(net_address),
            str(NetworkManager.ADDRESS_NETWORK.netmask)
        )
        iface.addAddress(address)
        return Node(
            id=name,
            instance=node_vm,
            size=self.params.node_size,
            interface=iface,
            roles=roles
        )

    def rackProvision(self, name: str) -> Rack:
        return Rack(
            name=name,
            nodes={}
        )

    def datacentreProvision(self, name: str) -> DataCentre:
        return DataCentre(
            name=name,
            racks={}
        )

    def partitionDataCentres(self, app_variant: ApplicationVariant) -> tuple[dict[str, DataCentre], ProvisioningTopology, InverseProvisioningTopology]:
        print("Partitioning nodes into datacentres and racks")
        datacentres: dict[str, DataCentre] = {}
        assigner = APPLICATION_TOPOLOGY_ASSIGNERS[app_variant]
        (topology, inverse_topology) = assigner.constructTopology(self.params.dc_count, self.params.racks_per_dc, self.params.nodes_per_rack)
        dc_idx: int = 0
        rack_idx: int = 0
        for (dc, racks) in topology.items():
            new_dc = self.datacentreProvision(dc)
            datacentres[dc] = new_dc
            for (rack, nodes) in racks.items():
                new_rack = self.rackProvision(rack)
                new_dc.racks[rack] = new_rack
                for (node, roles) in nodes.items():
                    new_rack.nodes[node] = self.nodeProvision(node, roles)
                rack_idx += 1
            dc_idx += 1
            rack_idx = 0
        return (datacentres, topology, inverse_topology)

    def bootstrapDB(self,
                    cluster: Cluster,
                    topology_properties: TopologyProperties) -> None:
        print("Bootstrapping cluster")
        app_variant: ApplicationVariant = ApplicationVariant[str(self.params.application).upper()]
        app: AbstractApplication = APPLICATION_BINDINGS[app_variant](
            self.params.application_version,
            self.docker_config
        )
        app.preConfigureClusterLevelProperties(
            cluster,
            self.params,
            topology_properties
        )
        # Addresses are assigned in previous loop, we need to know
        # them all before installing as each node should know the
        # addresses of all other nodes
        for node in topology_properties.db_nodes.values():
            print(f"Installing {self.params.application} on node {node.id}")
            app.nodeInstallApplication(node)

    def bootstrapCollector(self,
                           cluster: Cluster,
                           collector: Collector,
                           topology_properties: TopologyProperties) -> None:
        print("Bootstrapping collector")
        app: OTELCollector = OTELCollector(
            self.params.collector_version,
            self.docker_config
        )
        app.preConfigureClusterLevelProperties(
            cluster,
            self.params,
            topology_properties
        )
        print(f"Installing {app.variant()} on node {collector.node.id}")
        app.nodeInstallApplication(collector.node)
    
    def clusterProvisionHardware(self) -> Cluster:
        print("Provisioning cluster hardware")
        app_variant: ApplicationVariant = ApplicationVariant[str(self.params.application).upper()]
        datacentres, topology, inverse_topology = self.partitionDataCentres(app_variant)
        return Cluster(
            topology,
            inverse_topology,
            datacentres
        )

    def collectorProvisionHardware(self) -> Collector:
        print("Provisioning collector hardware")
        return Collector(self.nodeProvision("collector", []))

    def bindNodesViaLAN(self,
                        cluster: Cluster,
                        collector: Optional[Collector]) -> pg.LAN:
        print("Constructing VLAN and binding node interfaces")
        lan: pg.LAN = pg.LAN("LAN")
        for node in cluster.nodesGenerator():
            print(
                "Binding node address {} to LAN".format(
                    node.interface.addresses[0].address
                )
            );
            lan.addInterface(node.interface)
        if collector != None:
            lan.addInterface(collector.node.interface)
            print(
                "Binding allocator interface {} to LAN".format(
                    collector.node.interface.addresses[0].address
                )
            )
        if (self.params.vlan_type != None):
            lan.connectSharedVlan(self.params.vlan_type)
        return lan

    def provision(self) -> Tuple[Cluster, Optional[Collector]]:
        # Pre-allocate interface to share across nodes in LAN
        NetworkManager.nextPhysicalInterface()
        cluster: Cluster = self.clusterProvisionHardware()
        collector: Optional[Collector] = self.collectorProvisionHardware()
        lan: pg.LAN = self.bindNodesViaLAN(cluster, collector)
        self.request.addResource(lan)
        db_nodes = {}
        for node in cluster.nodesGenerator():
            db_nodes[node.id] = node
        topology_properties: TopologyProperties = TopologyProperties(
            collector.node.interface if collector != None else None,
            db_nodes
        )
        self.bootstrapDB(cluster, topology_properties)
        self.bootstrapCollector(cluster, collector, topology_properties)
        return cluster, collector
