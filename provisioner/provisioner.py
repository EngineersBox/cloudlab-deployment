from typing import Iterator, Tuple
import geni.portal as portal
import geni.rspec.pg as pg
import uuid, ipaddress
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
from provisioner.topology import TopologyProperties


APPLICATION_BINDINGS: dict[ApplicationVariant, type[AbstractApplication]] = {
    CassandraApplication.variant(): CassandraApplication,
    MongoDBApplication.variant(): MongoDBApplication,
    ElasticsearchApplication.variant(): ElasticsearchApplication,
    ScyllaApplication.variant(): ScyllaApplication
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

    def nodeProvision(self) -> Node:
        id = f"node{self.__node_idx}"
        self.__node_idx += 1
        node_vm = pg.RawPC(id)
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
            id=id,
            instance=node_vm,
            size=self.params.node_size,
            interface=iface
        )

    def rackProvision(self, i: int) -> Rack:
        return Rack(
            name="rack-%d" % i,
            nodes=[]
        )

    def datacentreProvision(self, i: int) -> DataCentre:
        return DataCentre(
            name="dc-%d" % i,
            racks={}
        )

    def partitionDataCentres(self) -> dict[str, DataCentre]:
        print("Partitioning nodes into datacentres and racks")
        datacentres: dict[str, DataCentre] = {}
        dc_idx: int = 0
        rack_idx: int = 0
        for _ in range(self.params.dc_count):
            dc: DataCentre = self.datacentreProvision(dc_idx)
            datacentres[dc.name] = dc
            for _ in range(self.params.racks_per_dc):
                rack: Rack = self.rackProvision(rack_idx)
                dc.racks[rack.name] = rack
                for _ in range(self.params.nodes_per_rack):
                    rack.nodes.append(self.nodeProvision())
                rack_idx += 1
            dc_idx += 1
            rack_idx = 0
        return datacentres

    def bootstrapDB(self,
                    cluster: Cluster,
                    topologyProperties: TopologyProperties) -> None:
        print("Bootstrapping cluster")
        app_variant: ApplicationVariant = ApplicationVariant[str(self.params.application).upper()]
        app: AbstractApplication = APPLICATION_BINDINGS[app_variant](
            self.params.application_version,
            self.docker_config
        )
        app.preConfigureClusterLevelProperties(
            cluster,
            self.params,
            topologyProperties
        )
        # Addresses are assigned in previous loop, we need to know
        # them all before installing as each node should know the
        # addresses of all other nodes
        for node in topologyProperties.db_nodes:
            print(f"Installing {self.params.application} on node {node.id}")
            app.nodeInstallApplication(node)

    def bootstrapCollector(self,
                           cluster: Cluster,
                           collector: Collector,
                           topologyProperties: TopologyProperties) -> None:
        print("Bootstrapping collector")
        app: OTELCollector = OTELCollector(
            self.params.collector_version,
            self.docker_config
        )
        app.preConfigureClusterLevelProperties(
            cluster,
            self.params,
            topologyProperties
        )
        print(f"Installing {app.variant()} on node {collector.node.id}")
        app.nodeInstallApplication(collector.node)
    
    def clusterProvisionHardware(self) -> Cluster:
        print("Provisioning cluster hardware")
        cluster: Cluster = Cluster()
        cluster.datacentres = self.partitionDataCentres()
        return cluster

    def collectorProvisionHardware(self) -> Collector:
        print("Provisioning collector hardware")
        return Collector(self.nodeProvision())

    def bindNodesViaLAN(self,
                        cluster: Cluster,
                        collector: Collector) -> pg.LAN:
        print("Constructing VLAN and binding node interfaces")
        lan: pg.LAN = pg.LAN("LAN")
        for node in cluster.nodesGenerator():
            print(
                "Binding node address {} to LAN".format(
                    node.interface.addresses[0].address
                )
            );
            lan.addInterface(node.interface)
        lan.addInterface(collector.node.interface)
        print(
            "Binding allocator interface {} to LAN".format(
                collector.node.interface.addresses[0].address
            )
        )
        if (self.params.vlan_type != None):
            lan.connectSharedVlan(self.params.vlan_type)
        return lan

    def provision(self) -> Tuple[Cluster, Collector]:
        # Pre-allocate interface to share across nodes in LAN
        NetworkManager.nextPhysicalInterface()
        cluster: Cluster = self.clusterProvisionHardware()
        collector: Collector = self.collectorProvisionHardware()
        lan: pg.LAN = self.bindNodesViaLAN(cluster, collector)
        self.request.addResource(lan)
        topologyProperties: TopologyProperties = TopologyProperties(
            collector.node.interface,
            [node for node in cluster.nodesGenerator()]
        )
        self.bootstrapDB(cluster, topologyProperties)
        self.bootstrapCollector(cluster, collector, topologyProperties)
        return cluster, collector
