import geni.portal as portal
from geni.rspec import pg
from provisioner.application.app import LOCAL_PATH, AbstractApplication, ApplicationVariant
from provisioner.docker import DockerConfig
from provisioner.parameters import Parameter, ParameterGroup
from provisioner.structure.cluster import Cluster
from provisioner.structure.node import Node
from provisioner.topology import TopologyProperties
from provisioner.utils import catToFile, sed
from provisioner.list_utils import takeSpread

class HBaseApplication(AbstractApplication):
    all_ips: list[pg.Interface] = []
    zk_nodes: list[pg.Interface] = []
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
        super().constructTopology(cluster)
        self.cluster = cluster
        self.client_max_total_tasks = params.hbase.client_max_total_tasks
        self.client_max_perserver_tasks = params.hbase.client_max_perserver_tasks
        self.client_max_perregion_tasks = params.hbase.client_max_perregion_tasks
        self.zk_nodes = self.determineZookeeperNodes()

    def determineZookeeperNodes(self) -> list[pg.Interface]:
        num_nodes = len(self.all_ips)
        zk_count = 3
        if (num_nodes < 15):
            zk_count = min(3, num_nodes)
        elif (num_nodes < 21):
            zk_count = 5
        else:
            zk_count = 7
        result: list[pg.Interface] = []
        for node in takeSpread(list(self.topology.keys()), zk_count):
            result.append(node.interface)
        return result

    def writeHBaseSiteProperties(self, node: Node) -> None:
        all_ips_prop: str = ",".join([f"\"{iface.addresses[0].address}\"" for iface in self.all_ips])
        sed(
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

    def writeEnvConfig(self, node: Node) -> None:
        # TODO: Saturate config
        config = f"""
        HBASE_MANAGES_ZK=true
        """
        catToFile(
            node,
            f"{LOCAL_PATH}/config/hbase/hbase-env.sh",
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
        self.writeEnvConfig(node)
        self.writeSSHKeys(node)
        self.bootstrapNode(
            node,
            {
                "NODE_ALL_IPS": "({})".format(all_ips_prop),
                "INVOKE_INIT": "true",
            }
        )

class HBaseParameters(ParameterGroup):

    def __init__(self):
        super().__init__(parameters=[
            Parameter(
                name="client_max_total_tasks",
                description="Maximum number of concurrent mutation tasks a single HTable instance will send to the cluster",
                typ=portal.ParameterType.INTEGER,
                required=False,
                defaultValue=100,
            ),
            Parameter(
                name="client_max_perserver_tasks",
                description="Maximum number of concurrent mutation tasks a single HTable instance wil send to a single region server",
                typ=portal.ParameterType.INTEGER,
                required=False,
                defaultValue=2,
            ),
            Parameter(
                name="client_max_perregion_tasks",
                description="Maximum number of concurrent mutation tasks that the client will maintain to a single region. if there is already this many writes in progress for this region, new puts won't be sent to this region until some writes finish",
                typ=portal.ParameterType.INTEGER,
                required=False,
                defaultValue=1,
            ),
        ])

    @classmethod
    def name(cls) -> str:
        return "HBase"

    @classmethod
    def id(cls) -> str:
        return "hbase"

HBASE_PARAMETERS = HBaseParameters()
