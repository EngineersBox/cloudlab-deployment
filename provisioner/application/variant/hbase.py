import geni.portal as portal
from typing import Tuple
from geni.rspec import pg
from provisioner.application.app import LOCAL_PATH, AbstractApplication, ApplicationVariant
from provisioner.docker import DockerConfig
from provisioner.structure.cluster import Cluster
from provisioner.structure.datacentre import DataCentre
from provisioner.structure.node import Node
from provisioner.structure.rack import Rack
from provisioner.topology import TopologyProperties
from provisioner.utils import catToFile, sedReplaceMappings

class HBaseApplication(AbstractApplication):
    all_ips: list[pg.Interface] = []
    topology: dict[Node, Tuple[DataCentre, Rack]]
    client_max_total_tasks: int = 100
    client_max_perserver_tasks: int = 2
    client_max_perregion_tasks: int = 1
    
    def __init__(self, version: str, docker_config: DockerConfig):
        super().__init__(version, docker_config)

    @classmethod
    def variant(cls) -> ApplicationVariant:
        return ApplicationVariant.HBASE

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
        self.client_max_total_tasks = params.hbase_client_max_total_tasks
        self.client_max_perserver_tasks = params.hbase_client_max_perserver_tasks
        self.client_max_perregion_tasks = params.hbase_client_max_perregion_tasks

    def writeHBaseSiteProperties(self, node: Node) -> None:
        all_ips_prop: str = ",".join([f"\"{iface.addresses[0].address}\"" for iface in self.all_ips])
        sedReplaceMappings(
            node,
            {
                "@@ZK_NODE_IPS@@": all_ips_prop,
                "@@CLIENT_MAX_TOTAL_TASKS@@": f"{self.client_max_total_tasks}",
                "@@CLIENT_MAX_PER_SERVER_TASKS@@": f"{self.client_max_perserver_tasks}",
                "@@CLIENT_MAS_PER_REGION_TASKS@@": f"{self.client_max_perregion_tasks}"
            },
            f"{LOCAL_PATH}/config/hbase/hbase-site.xml"
        )

    def writeRegionServersConfig(self, node: Node) -> None:
        # TODO: Saturate config
        config = f"""
        """
        catToFile(
            node,
            f"{LOCAL_PATH}/config/hbase/regionservers",
            config
        )

    def writeBackupMastersConfig(self, node: Node) -> None:
        # TODO: Saturate config
        config = f"""
        """
        catToFile(
            node,
            f"{LOCAL_PATH}/config/hbase/regionservers",
            config
        )

    def writeSSHKeys(self, node: Node) -> None:
        pass

    def nodeInstallApplication(self, node: Node) -> None:
        super().nodeInstallApplication(node)
        self.unpackTar(node)
        all_ips_prop: str = " ".join([f"\"{iface.addresses[0].address}\"" for iface in self.all_ips])
        self.writeHBaseSiteProperties(node)
        self.writeRegionServersConfig(node)
        self.writeBackupMastersConfig(node)
        self.bootstrapNode(
            node,
            {
                "NODE_ALL_IPS": "({})".format(all_ips_prop),
                "INVOKE_INIT": "true",
            }
        )
