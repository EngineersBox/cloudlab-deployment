from provisioner.application.app import AbstractApplication, ApplicationVariant, LOCAL_PATH
from provisioner.docker import DockerConfig
from provisioner.structure.cluster import Cluster
from provisioner.structure.node import Node
from provisioner.provisioner import TopologyProperties
from provisioner.utils import catToFile
import geni.portal as portal
import geni.rspec.pg as pg

class OTELCollector(AbstractApplication):
    ycsb_version: str
    cluster_application: str

    def __init__(self, version: str, docker_config: DockerConfig):
        super().__init__(version, docker_config)

    @classmethod
    def variant(cls) -> ApplicationVariant:
        return ApplicationVariant.OTEL_COLLECTOR

    def preConfigureClusterLevelProperties(self,
                                           cluster: Cluster,
                                           params: portal.Namespace,
                                           topologyProperties: TopologyProperties) -> None:
        super().preConfigureClusterLevelProperties(
            cluster,
            params,
            topologyProperties
        )
        self.ycsb_version = params.ycsb_version
        self.cluster_application = params.application

    def createYCSBBaseProfile(self, node: Node) -> None:
        base_profile_path=f"{LOCAL_PATH}/ycsb-{self.ycsb_version}/base_profile.dat"
        app_variant: ApplicationVariant = ApplicationVariant(ApplicationVariant._member_map_[str(self.cluster_application).upper()])
        all_ips: list[str] = []
        for iface in self.topologyProperties.nodeInterfaces:
            all_ips.append(iface.addresses[0].address)
        profile_content: str = ""
        if app_variant == ApplicationVariant.CASSANDRA:
            profile_content = f"""
            hosts={",".join(all_ips)}
            port=9042
            """
        elif app_variant == ApplicationVariant.ELASTICSEARCH:
            # TODO: Complete this
            raise NotImplementedError("Elasticsearch YCSB profile not implemented")
        elif app_variant == ApplicationVariant.MONGO_DB:
            # TODO: Complete this
            raise NotImplementedError("MongoDB YCSB profile not implemented")
        elif app_variant == ApplicationVariant.SCYLLA:
            profile_content = f"""
            scylla.hosts={",".join(all_ips)}
            port=9042
            """
        else:
            raise RuntimeError(f"Invalid application variant: {self.cluster_application}")
        node.instance.addService(catToFile(base_profile_path, profile_content))

    def nodeInstallApplication(self, node: Node) -> None:
        super().nodeInstallApplication(node)
        self.unpackTar(node)
        self.unpackTar(
            node,
            f"https://github.com/brianfrankcooper/YCSB/releases/download/{self.ycsb_version}/ycsb-{self.ycsb_version}.tar.gz"
        )
        self.bootstrapNode(node, {})
        self.createYCSBBaseProfile(node)
