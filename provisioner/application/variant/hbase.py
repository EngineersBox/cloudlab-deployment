import math
import geni.portal as portal
from typing import Optional, Tuple
from geni.rspec import pg
from provisioner import parameters
from provisioner.application.app import LOCAL_PATH, AbstractApplication, ApplicationVariant
from provisioner.docker import DockerConfig
from provisioner.parameters import Parameter, ParameterGroup
from provisioner.structure.cluster import Cluster
from provisioner.structure.datacentre import DataCentre
from provisioner.structure.node import Node
from provisioner.structure.rack import Rack
from provisioner.topology import TopologyProperties
from provisioner.utils import catToFile, sedReplaceMappings

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
        super().constructTopology(cluster)
        self.cluster = cluster
        self.client_max_total_tasks = params.hbase.client_max_total_tasks
        self.client_max_perserver_tasks = params.hbase.client_max_perserver_tasks
        self.client_max_perregion_tasks = params.hbase.client_max_perregion_tasks

    def allocateZookeeperNodes(self) -> list[pg.Interface]:
        # TODO: Test this logic and verify it splits ZK nodes
        #       between DCs and racks correctly
        num_nodes = len(self.all_ips)
        zk_count = 3
        if (num_nodes < 5):
            zk_count = min(3, num_nodes)
        elif (num_nodes < 7):
            zk_count = 5
        else:
            zk_count = 7
        num_dcs = len(self.cluster.datacentres)
        per_dc = max(1, int(math.floor(zk_count / float(num_dcs))))
        allocated = 0
        i = 0
        dcs  = list(self.cluster.datacentres.items())
        result: list[pg.Interface] = []
        while (allocated < zk_count):
            dc_to_allocate = min(per_dc, zk_count - allocated)
            dc: DataCentre = dcs[i][1]
            racks = list(dc.racks.items())
            per_rack = int(math.floor(dc_to_allocate / float(len(racks))))
            rack_allocated = 0
            j = 0
            while (rack_allocated < per_dc):
                rack_to_allocate = min(per_rack, dc_to_allocate - rack_allocated)
                rack: Rack = racks[i][1]
                count = min(len(rack.nodes), rack_to_allocate)
                result.extend([node.interface for node in rack.nodes[0:count]])
                rack_allocated += count
                j += 1
            i += 1
            allocated += rack_allocated
        return result

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
