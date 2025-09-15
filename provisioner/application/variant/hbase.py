from enum import Enum
from typing import Optional
import geni.portal as portal
from geni.rspec import pg
from provisioner.application.app import LOCAL_PATH, AbstractApplication, ApplicationVariant
from provisioner.docker import DockerConfig
from provisioner.parameters import Parameter, ParameterGroup
from provisioner.structure.cluster import Cluster
from provisioner.structure.datacentre import DataCentre
from provisioner.structure.node import Node
from provisioner.topology import TopologyProperties
from provisioner.utils import appendToFile, catToFile, sed
from provisioner.list_utils import takeSpread

class AppType(Enum):
    HDFS = "hdfs"
    HBase = "hbase"

class NodeRole(Enum):
    HBaseData = "hbase_data", AppType.HBase
    HBaseZooKeeper = "hbase_zookeeper", AppType.HBase
    HBaseMaster = "hbase_master", AppType.HBase
    HBaseBackupMaster = "hbase_backup_master", AppType.HBase
    HDFSName = "hdfs_name", AppType.HDFS
    HDFSData = "hdfs_data", AppType.HDFS
    HDFSResourceManager = "hdfs_resource_manager", AppType.HDFS
    HDFSNodeManager = "hdfs_node_manager", AppType.HDFS,
    HDFSWebProxy = "hdfs_web_proxy", AppType.HDFS,
    HDFSMapRedHstory = "hdfs_mapred_history", AppType.HDFS

    def __str__(self) -> str:
        return "%s" % self.value[0]

    def appType(self) -> AppType:
        return self.value[1]

class HBaseApplication(AbstractApplication):
    all_ips: list[pg.Interface] = []
    roles: dict[pg.Interface, list[NodeRole]] = {}
    # HBase
    zk_nodes: list[pg.Interface] = []
    master: pg.Interface
    backup_masters: list[pg.Interface] = []
    client_max_total_tasks: int = 100
    client_max_perserver_tasks: int = 2
    client_max_perregion_tasks: int = 1
    # HDFS
    name_nodes: list[pg.Interface] = []
    data_nodes: list[pg.Interface] = []
    resource_manager_nodes: list[pg.Interface] = []
    data_manager_nodes: list[pg.Interface] = []
    
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
        self.master = list(self.topology.keys())[0].interface
        self.zk_nodes = self.determineHBaseZookeeperNodes()
        self.backup_masters = self.determineHBaseBackupMasterNodes()

    def assignNodeRoles(self) -> None:
        self.roles.setdefault(list(self.topology.keys())[0].interface, []).extend([
            NodeRole.HBaseMaster,
            NodeRole.HDFSName
        ])
        for iface in self.determineHBaseZookeeperNodes():
            self.roles.setdefault(iface, []).append(NodeRole.HBaseZooKeeper)
        for iface in self.determineHBaseBackupMasterNodes():
            self.roles.setdefault(iface, []).append(NodeRole.HBaseBackupMaster)
        for iface in self.determineHDFSDataNodes():
            self.roles.setdefault(iface, []).extend([
                NodeRole.HDFSData,
                NodeRole.HDFSNodeManager
            ])
        for iface in self.determineHDFSNameNodes():
            self.roles.setdefault(iface, []).append(NodeRole.HDFSName)
        for iface in self.determineHDFSResourceManagerNodes():
            self.roles.setdefault(iface, []).append(NodeRole.HDFSResourceManager)

    def determineHBaseZookeeperNodes(self) -> list[pg.Interface]:
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

    def _findHBaseNodeWithoutRole(self, dc: DataCentre) -> Optional[Node]:
        for rack in dc.racks.values():
            for node in rack.nodes:
                existing = self.roles.setdefault(node.interface, [])
                if (NodeRole.HBaseMaster in existing
                    or NodeRole.HBaseBackupMaster in existing
                    or NodeRole.HBaseZooKeeper in existing):
                    continue;
                return node
        return None

    def determineHBaseBackupMasterNodes(self) -> list[pg.Interface]:
        result = []
        backup_count = min(3, len(self.cluster.datacentres))
        for dc in self.cluster.datacentres.values():
            node = self._findHBaseNodeWithoutRole(dc)
            if (node == None):
                continue
            result.append(node.interface)
            backup_count -= 1
        if (backup_count != 0):
            raise ValueError("Unable to allocate backup masters")
        return result

    def determineHDFSNameNodes(self) -> list[pg.Interface]:
        result = []
        for dc in self.cluster.datacentres.values():
            result.append(list(dc.racks.values())[-1].nodes[0].interface)
        return result

    def determineHDFSDataNodes(self) -> list[pg.Interface]:
        return self.all_ips

    def determineHDFSResourceManagerNodes(self) -> list[pg.Interface]:
        result = []
        for dc in self.cluster.datacentres.values():
            racks = list(dc.racks.values())
            result.append(racks[min(1, len(racks) - 1)].nodes[0].interface)
        return result

    def writeHBaseSiteProperties(self, node: Node) -> None:
        zk_ips_prop: str = ",".join([f"\"{iface.addresses[0].address}\"" for iface in self.zk_nodes])
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
        config = "\n".join([iface.addresses[0].address for iface in self.backup_masters])
        catToFile(
            node,
            f"{LOCAL_PATH}/config/hbase/backup-masters",
            config
        )

    def writeZookeeperConfig(self, node: Node) -> None:
        pass

    def writeHBaseConfiguration(self, node: Node, role: NodeRole) -> None:
        self.writeRegionServersConfig(node)
        self.writeBackupMastersConfig(node)
        if (role == NodeRole.HBaseZooKeeper):
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
        for role in self.roles[node.interface]:
            if (role.appType() == AppType.HBase):
                self.writeHBaseConfiguration(node, role)
            elif (role.appType() == AppType.HDFS):
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
