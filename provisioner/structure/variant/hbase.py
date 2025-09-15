from enum import Enum
from provisioner.structure.topology_assigner import TopologyAssigner, ProvisioningTopology

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

    def constructTopology(self, dcs: int, racks_per_dc: int, nodes_per_rack: int) -> ProvisioningTopology:
        topology = {}
        for dc_id in range(dcs):
            dc = topology.setdefault(f"dc-{dc_id}", {})
            for rack_id in range(racks_per_dc):
                rack = dc.setdefault(f"rack-{rack_id}", {})
                for node_id in range(nodes_per_rack):
                    rack.setdefault(f"node-{node_id}", []).extend([
                        HBaseNodeRole.HBaseData,
                        HBaseNodeRole.HDFSData,
                        HBaseNodeRole.HDFSNodeManager
                    ])
        # TODO: Assign rest of roles
        return topology
