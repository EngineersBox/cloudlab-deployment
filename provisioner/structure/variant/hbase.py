from enum import Enum
from provisioner.structure.topology_assigner import TopologyAssigner, ProvisioningTopology
from provisioner.list_utils import takeSpread

class HBaseAppType(Enum):
    HDFS = "hdfs"
    HBase = "hbase"

class HBaseNodeRole(Enum):
    HBaseData = "hbase_data", HBaseAppType.HBase
    HBaseZooKeeper = "hbase_zookeeper", HBaseAppType.HBase
    HBaseMaster = "hbase_master", HBaseAppType.HBase
    HBaseBackupMaster = "hbase_backup_master", HBaseAppType.HBase
    HDFSName = "hdfs_name", HBaseAppType.HDFS
    HDFSData = "hdfs_data", HBaseAppType.HDFS
    HDFSResourceManager = "hdfs_resource_manager", HBaseAppType.HDFS
    HDFSNodeManager = "hdfs_node_manager", HBaseAppType.HDFS,
    HDFSWebProxy = "hdfs_web_proxy", HBaseAppType.HDFS,
    HDFSMapRedHstory = "hdfs_mapred_history", HBaseAppType.HDFS

    def __str__(self) -> str:
        return "%s" % self.value[0]

    def appType(self) -> HBaseAppType:
        return self.value[1]

class HBaseTopologyAssigner(TopologyAssigner):

    @classmethod
    def createHBaseMasterNode(cls, node_id: int, topology: ProvisioningTopology) -> None:
        dc = list(topology.values())[0]
        rack = list(dc.values())[0]
        rack.setdefault(f"node-{node_id}", []).append(str(HBaseNodeRole.HBaseMaster))

    @classmethod
    def determineHBaseZookeeperNodes(cls,
                                     num_nodes: int,
                                     topology: ProvisioningTopology,
                                     inverse_topology: dict[str, tuple[str, str]]) -> None:
        zk_count = 3
        if (num_nodes < 15):
            zk_count = min(3, num_nodes)
        elif (num_nodes < 21):
            zk_count = 5
        else:
            zk_count = 7
        for (node, (dc, rack)) in takeSpread(list(inverse_topology.items()), zk_count):
            topology[dc][rack][node].append(str(HBaseNodeRole.HBaseZooKeeper))

    @classmethod
    def createHDFSAuxiliaryNodes(cls, node_id: int, topology: ProvisioningTopology) -> None:
        dc = list(topology.values())[0]
        rack = list(dc.values())[0]
        rack.setdefault(f"node-{node_id}", []).extend([
            str(HBaseNodeRole.HDFSName),
            str(HBaseNodeRole.HDFSWebProxy),
            str(HBaseNodeRole.HDFSMapRedHstory)
        ])

    @classmethod
    def constructTopology(cls, dcs: int, racks_per_dc: int, nodes_per_rack: int) -> ProvisioningTopology:
        topology = {}
        inverse_topology = {}
        node_id = 0
        for dc_id in range(dcs):
            dc = topology.setdefault(f"dc-{dc_id}", {})
            for rack_id in range(racks_per_dc):
                rack = dc.setdefault(f"rack-{rack_id}", {})
                for _ in range(nodes_per_rack):
                    node_name = f"node-{node_id}"
                    rack.setdefault(node_name, []).extend([
                        str(HBaseNodeRole.HBaseData),
                        str(HBaseNodeRole.HDFSData),
                        str(HBaseNodeRole.HDFSNodeManager)
                    ])
                    inverse_topology[node_name] = (dc_id, rack_id)
                    node_id += 1
        cls.determineHBaseZookeeperNodes(
            node_id + 1,
            topology,
            inverse_topology
        )
        node_id += 1
        cls.createHBaseMasterNode(node_id, topology)
        node_id += 1
        cls.createHDFSAuxiliaryNodes(node_id, topology)
        return topology
