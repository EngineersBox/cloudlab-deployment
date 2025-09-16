import geni.portal as portal
from geni.rspec import pg
from provisioner.application.app import LOCAL_PATH, AbstractApplication, ApplicationVariant
from provisioner.docker import DockerConfig
from provisioner.parameters import Parameter, ParameterGroup
from provisioner.structure.cluster import Cluster
from provisioner.structure.node import Node
from provisioner.structure.topology_assigner import findNodesWithRole
from provisioner.structure.variant.hbase import HBaseAppType, HBaseNodeRole
from provisioner.topology import TopologyProperties
from provisioner.utils import appendToFile, catToFile, sed

class HBaseApplication(AbstractApplication):
    all_ips: list[pg.Interface] = []
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
        self.all_ips = [node.interface for node in cluster.nodesGenerator()]
        self.cluster = cluster
        self.client_max_total_tasks = params.hbase.client_max_total_tasks
        self.client_max_perserver_tasks = params.hbase.client_max_perserver_tasks
        self.client_max_perregion_tasks = params.hbase.client_max_perregion_tasks
        found = False
        for node, (roles, _dc, _rack) in self.cluster.inverse_topology.items():
            if (HBaseNodeRole.HBaseMaster in roles):
                self.master = topology_properties.db_nodes[node].interface
                found = True
                break
        if (not found):
            raise ValueError("No nodes has the HBaseMaster role assigned")

    def writeHBaseSiteProperties(self, node: Node) -> None:
        zk_nodes = [
            self.topology_properties.db_nodes[node].interface
            for node in findNodesWithRole(self.cluster.inverse_topology, str(HBaseNodeRole.HBaseZooKeeper))
        ]
        zk_ips_prop: str = ",".join([f"\"{iface.addresses[0].address}\"" for iface in zk_nodes])
        sed(
            node,
            {
                "@@ZK_NODE_IPS@@": zk_ips_prop,
                "@@CLIENT_MAX_TOTAL_TASKS@@": f"{self.client_max_total_tasks}",
                "@@CLIENT_MAX_PER_SERVER_TASKS@@": f"{self.client_max_perserver_tasks}",
                "@@CLIENT_MAS_PER_REGION_TASKS@@": f"{self.client_max_perregion_tasks}"
            },
            f"{LOCAL_PATH}/config/hbase/hbase-site.xml"
        )

    def writeRegionServersConfig(self, node: Node) -> None:
        config = "\n".join([iface.addresses[0].address for iface in self.all_ips])
        catToFile(
            node,
            f"{LOCAL_PATH}/config/hbase/regionservers",
            config
        )

    def writeBackupMastersConfig(self, node: Node) -> None:
        backup_masters = [
            self.topology_properties.db_nodes[node].interface 
            for node in findNodesWithRole(self.cluster.inverse_topology, str(HBaseNodeRole.HBaseBackupMaster))
        ]
        config = "\n".join([iface.addresses[0].address for iface in backup_masters])
        catToFile(
            node,
            f"{LOCAL_PATH}/config/hbase/backup-masters",
            config
        )

    def writeZookeeperConfig(self, node: Node) -> None:
        pass

    def writeHBaseConfiguration(self, node: Node, role: HBaseNodeRole) -> None:
        self.writeRegionServersConfig(node)
        self.writeBackupMastersConfig(node)
        if (role == HBaseNodeRole.HBaseZooKeeper):
            self.writeZookeeperConfig(node)

    def writeHDFSYarnConfiguraton(self, node: Node) -> None:
        pass

    def writeHDFSMapReduceConfiguration(self, node: Node) -> None:
        pass

    def writeHDFSEnvConfiguration(self, node: Node) -> None:
        config = f"""
        export HADOOP_HOME={LOCAL_PATH}/hdfs
        """
        appendToFile(
            node,
            "/etc/profile.d",
            config
        )
        config = f"""

        """

    def writeHDFSConfiguration(self, node: Node) -> None:
        pass

    def nodeInstallApplication(self, node: Node) -> None:
        super().nodeInstallApplication(node)
        self.unpackTar(node)
        all_ips_prop: str = " ".join([f"\"{iface.addresses[0].address}\"" for iface in self.all_ips])
        self.writeHBaseSiteProperties(node)
        self.writeRegionServersConfig(node)
        self.writeBackupMastersConfig(node)
        for role in node.roles:
            hbase_role = HBaseNodeRole[role]
            app_type = hbase_role.appType()
            if (app_type == HBaseAppType.HBase):
                self.writeHBaseConfiguration(node, hbase_role)
            elif (app_type == HBaseAppType.HDFS):
                self.writeHDFSConfiguration(node)
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

HBASE_PARAMETERS: ParameterGroup = HBaseParameters()
