from enum import Enum
from provisioner.structure.topology_assigner import InverseProvisioningTopology, TopologyAssigner, ProvisioningTopology, addOrUpdateNode
from provisioner.list_utils import takeSpread

class HBaseAppType(Enum):
    HDFS = "hdfs"
    HBase = "hbase"

class HBaseNodeRole(Enum):
    HBASE_REGION_SERVER = "hbase_region_server", HBaseAppType.HBase
    HBASE_ZOOKEEPER = "hbase_zookeeper", HBaseAppType.HBase
    HBASE_MASTER = "hbase_master", HBaseAppType.HBase
    HBASE_BACKUP_MASTER = "hbase_backup_master", HBaseAppType.HBase
    HDFS_NAME = "hdfs_name", HBaseAppType.HDFS
    HDFS_DATA = "hdfs_data", HBaseAppType.HDFS
    HDFS_RESOURCE_MANAGER = "hdfs_resource_manager", HBaseAppType.HDFS
    HDFS_NODE_MANAGER = "hdfs_node_manager", HBaseAppType.HDFS,
    HDFS_WEB_PROXY = "hdfs_web_proxy", HBaseAppType.HDFS,
    HDFS_MAPRED_HISTORY = "hdfs_mapred_history", HBaseAppType.HDFS

    def __str__(self) -> str:
        return "%s" % self.value[0]

    def appType(self) -> HBaseAppType:
        return self.value[1]

class HBaseTopologyAssigner(TopologyAssigner):

    @classmethod
    def createHBaseMasterNode(cls,
                              node_id: int,
                              topology: ProvisioningTopology,
                              inverse_topology: InverseProvisioningTopology) -> None:
        dc = list(topology.items())[0]
        rack = list(dc[1].keys())[0]
        addOrUpdateNode(
            topology,
            inverse_topology,
            dc[0],
            rack,
            f"node-{node_id}",
            [str(HBaseNodeRole.HBASE_MASTER)]
        )

    @classmethod
    def determineHBaseZookeeperNodes(cls,
                                     num_nodes: int,
                                     topology: ProvisioningTopology,
                                     inverse_topology: InverseProvisioningTopology) -> None:
        
        zk_count = 1
        if (num_nodes <= 3):
            zk_count = 1
        elif (num_nodes < 15):
            zk_count = 3
        elif (num_nodes < 21):
            zk_count = 5
        else:
            zk_count = 7
        for (node, (_, dc, rack)) in takeSpread(list(inverse_topology.items()), zk_count):
            addOrUpdateNode(
                topology,
                inverse_topology,
                dc,
                rack,
                node,
                [str(HBaseNodeRole.HBASE_ZOOKEEPER)]
            )

    @classmethod
    def createHDFSAuxiliaryNode(cls,
                                node_id: int,
                                topology: ProvisioningTopology,
                                inverse_topology: InverseProvisioningTopology) -> None:
        dc = list(topology.items())[0]
        rack = list(dc[1].keys())[0]
        addOrUpdateNode(
            topology,
            inverse_topology,
            dc[0],
            rack,
            f"node-{node_id}",
            [
                str(HBaseNodeRole.HDFS_NAME),
                str(HBaseNodeRole.HDFS_RESOURCE_MANAGER),
                # str(HBaseNodeRole.HDFS_WEB_PROXY),
                str(HBaseNodeRole.HDFS_MAPRED_HISTORY)
            ]
        )

    @classmethod
    def constructTopology(cls, dcs: int, racks_per_dc: int, nodes_per_rack: int) -> tuple[ProvisioningTopology, InverseProvisioningTopology]:
        topology: ProvisioningTopology = {}
        inverse_topology: InverseProvisioningTopology = {}
        node_id = 0
        for dc_id in range(dcs):
            dc_name = f"dc-{dc_id}"
            for rack_id in range(racks_per_dc):
                rack_name = f"rack-{rack_id}"
                for _ in range(nodes_per_rack):
                    node_name = f"node-{node_id}"
                    addOrUpdateNode(
                        topology,
                        inverse_topology,
                        dc_name,
                        rack_name,
                        node_name,
                        [
                            str(HBaseNodeRole.HBASE_REGION_SERVER),
                            str(HBaseNodeRole.HDFS_DATA),
                            str(HBaseNodeRole.HDFS_NODE_MANAGER)
                        ]
                    )
                    node_id += 1
        cls.determineHBaseZookeeperNodes(
            node_id,
            topology,
            inverse_topology
        )
        cls.createHBaseMasterNode(node_id, topology, inverse_topology)
        cls.createHDFSAuxiliaryNode(node_id, topology, inverse_topology)
        return (topology, inverse_topology)
