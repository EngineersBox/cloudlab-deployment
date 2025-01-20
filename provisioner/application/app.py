from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional
from provisioner.collector.collector import OTELFeature
from provisioner.docker import DockerConfig
from provisioner.structure.cluster import Cluster
from provisioner.parameters import ParameterGroup, Parameter
from provisioner.structure.node import Node
from provisioner.topology import TopologyProperties
from provisioner.utils import catToFile
import geni.portal as portal
from geni.rspec import pg

class ApplicationVariant(Enum):
    CASSANDRA = "cassandra", True
    MONGO_DB = "mongodb", True
    SCYLLA = "scylla", True
    ELASTICSEARCH = "elasticsearch", True,
    OTEL_COLLECTOR = "otel_collector", False

    def __str__(self) -> str:
        return "%s" % self.value[0]

    @staticmethod
    def provsionableMembers() -> list["ApplicationVariant"]:
        return list(filter(
            lambda e: e.value[1],
            ApplicationVariant._member_map_.values()
        ))

VAR_LIB_PATH = "/var/lib"
LOCAL_PATH = f"{VAR_LIB_PATH}/cluster"
USERNAME = "cluster"

def cluster_user_context(func):
    def wrapper(self, *args, **kwargs):
        print("Before method execution")
        node: Optional[Node] = None
        for arg in args:
            if isinstance(arg, Node):
                node = arg
        if node == None:
            raise ValueError("Expected a node instance in arguments")
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo su -u {USERNAME}"
        ))
        res = func(self, *args, **kwargs)
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command="exit"
        ))
        return res
    return wrapper

class AbstractApplication(ABC):
    version: str
    docker_config: DockerConfig
    topologyProperties: TopologyProperties
    collectorFeatures: set[OTELFeature]

    @abstractmethod
    def __init__(self, version: str, docker_config: DockerConfig):
        self.version = version
        self.docker_config = docker_config

    @classmethod
    @abstractmethod
    def variant(cls) -> ApplicationVariant:
        pass

    @abstractmethod
    def preConfigureClusterLevelProperties(self,
                                           cluster: Cluster,
                                           params: portal.Namespace,
                                           topologyProperties: TopologyProperties) -> None:
        self.topologyProperties = topologyProperties
        self.collectorFeatures = params.collector_features


    def unpackTar(self,
                  node: Node,
                  url: Optional[str] = None,
                  path: Optional[str] = None) -> None:
        if url == None:
            url=f"https://github.com/EngineersBox/cassandra-benchmarking/releases/download/{self.variant()}-{self.version}/{self.variant()}.tar.gz"
        if path == None:
            path = VAR_LIB_PATH
        node.instance.addService(pg.Install(
            url=url,
            path=path
        ))

    def _writeEnvFile(self,
                      node: Node,
                      properties: dict[str, str]) -> None:
        collector_address: str = self.topologyProperties.collectorInterface.addresses[0].address
        # Ensure the collector exports data for enabled features
        for feat in self.collectorFeatures:
            properties[f"OTEL_{str(feat).upper()}_EXPORTER"] = "otlp"
        if OTELFeature.TRACES in self.collectorFeatures:
            properties["OTEL_TRACES_EXPORTER"] = "always_on"
        # Bash env file
        env_file_content = f"""# Node configuration properties
INSTALL_PATH={LOCAL_PATH}
APPLICATION_VARIANT={self.variant()}
APPLICATION_VERSION={self.version}

EBPF_NET_INTAKE_HOST={collector_address}
EBPF_NET_INTAKE_PORT=8000

OTEL_EXPORTER_OTLP_ENDPOINT=http://{collector_address}:4318
OTEL_SERVICE_NAME={node.id}
OTEL_RESOURCE_ATTRIBUTES=application={self.variant()}

NODE_IP={node.getInterfaceAddress()}
"""
        for (k,v) in properties.items():
            env_file_content += f"\n{k}={v}"
        # Bash sourcable configuration properties that the
        # bootstrap script uses as well as docker containers
        node.instance.addService(catToFile(
            f"{LOCAL_PATH}/node_env",
            env_file_content
        ))
        # Replace template var for pushing logs
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo sed -i \"s/@@COLLECTOR_ADDRESS@@/{collector_address}/g\" {LOCAL_PATH}/config/otel/otel-instance-config.yaml"
        ))

    @cluster_user_context
    def _bootstrapInUserContext(self,
                                 node: Node,
                                 properties: dict[str, str]) -> None:
        # Login to docker registry
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            # NOTE: Yes this is unsafe for wider usage, but given
            #       it is a read-only token within a private environment
            #       it's not the worst. I don't think it's worth setting
            #       up an external credentials provider to manage this.
            command=f"echo \"{self.docker_config.token}\" | sudo docker login ghcr.io -u {self.docker_config.username} --password-stdin",
        ))
        # Install bootstrap systemd unit and run it
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo ln -s {LOCAL_PATH}/units/bootstrap.service /etc/systemd/system/bootstrap.service && sudo systemctl start bootstrap.service"
        ))

    def bootstrapNode(self,
                      node: Node,
                      properties: dict[str, str]) -> None:
        # Create cluster user and switch to it
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo useradd {USERNAME} -u 1000 -g 1000 -m -G sudo"
        ))
        self._writeEnvFile(
            node,
            properties
        )
        self._bootstrapInUserContext(node, properties)

    @abstractmethod
    def nodeInstallApplication(self, node: Node) -> None:
        pass

class ApplicationParameterGroup(ParameterGroup):

    @classmethod
    def name(cls) -> str:
        return "Application"

    @classmethod
    def id(cls) -> str:
        return "application"

    def __init__(self):
        super().__init__(
            parameters=[
                Parameter(
                    name="application",
                    description="Database application to install",
                    typ=portal.ParameterType.STRING,
                    defaultValue=str(ApplicationVariant.CASSANDRA),
                    legalValues=[(str(app), app.name.title()) for app in ApplicationVariant.provsionableMembers()],
                    required=True
                ),
                Parameter(
                    name="application_version",
                    description="Version of the application",
                    typ=portal.ParameterType.STRING,
                    required=True
                ),
                Parameter(
                    name="cassandra_ycsb_rf",
                    description="Replication factor for YCSB keyspace",
                    typ=portal.ParameterType.INTEGER,
                    required=False,
                    defaultValue=0
                ),
            ]
        )

    def validate(self, params: portal.Namespace) -> None:
        super().validate(params)
        nodes_per_dc = params.racks_per_dc * params.nodes_per_rack
        if params.cassandra_ycsb_rf == 0:
            params.cassandra_ycsb_rf = nodes_per_dc
        elif params.cassandra_ycsb_rf > nodes_per_dc:
            portal.context.reportError(portal.ParameterError(
                f"Replication factor {params.cassandra_ycsb_rf} must be less than or equal to number of nodes in dc {nodes_per_dc}",
                ["cassandra_ycsb_rf"]
            ))


APPLICATION_PARAMETERS: ParameterGroup = ApplicationParameterGroup()
