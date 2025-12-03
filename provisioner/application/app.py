from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Optional
from provisioner.application.config import bashEncoder, jsonEncoder
from provisioner.collector.collector import OTELFeature
from provisioner.docker import DockerConfig
from provisioner.structure.cluster import Cluster
from provisioner.parameters import ParameterGroup, Parameter
from provisioner.structure.node import Node
from provisioner.topology import TopologyProperties
from provisioner.utils import catToFile, sed
import geni.portal as portal
from geni.rspec import pg
import string, random

class ApplicationVariant(Enum):
    CASSANDRA = "cassandra", True
    ELASTICSEARCH = "elasticsearch", True,
    HBASE = "hbase", True,
    MONGO_DB = "mongodb", True
    SCYLLA = "scylla", True
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
GROUPNAME = "cluster"

class AbstractApplication(ABC):
    version: str
    docker_config: DockerConfig
    cluster = Cluster
    collector_features: set[OTELFeature]

    @abstractmethod
    def __init__(self, version: str, docker_config: DockerConfig):
        self.version = version
        self.docker_config = docker_config

    @classmethod
    @abstractmethod
    def variant(cls) -> ApplicationVariant:
        raise NotImplementedError(f"No ApplicationVariant specified for {cls.__name__}")

    @abstractmethod
    def preConfigureClusterLevelProperties(self,
                                           cluster: Cluster,
                                           params: portal.Namespace,
                                           topology_properties: TopologyProperties) -> None:
        self.topology_properties = topology_properties
        self.cluster = cluster
        self.collector_features = params.collector_features

    def unpackTar(self,
                  node: Node,
                  url: Optional[str] = None,
                  path: Optional[str] = None,
                  use_pg_install: bool = True) -> None:
        if url == None:
            url=f"https://github.com/EngineersBox/database-benchmarking/releases/download/{self.variant()}-{self.version}/{self.variant()}.tar.gz"
        if path == None:
            path = LOCAL_PATH
        if (use_pg_install):
            node.instance.addService(pg.Install(
                url=url,
                path=path
            ))
        else:
            archive_name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
            commands=[
                f"sudo wget {url} -O {archive_name}.tar.gz",
                f"sudo mkdir -p {path}",
                f"sudo tar -xzf {archive_name}.tar.gz --directory={path}",
                f"sudo rm {archive_name}.tar.gz"
            ]
            for command in commands:
                node.instance.addService(pg.Execute(
                    shell="/bin/bash",
                    command=command
                ))

    def _writeEnvFile(self,
                      node: Node,
                      properties: dict[str, Any]) -> None:
        # Bash env file
        env_file_content = "# Node configuration properties\n"
        env_file_content += bashEncoder(properties)
        # Bash sourcable configuration properties that the
        # bootstrap script uses as well as docker containers
        catToFile(
            node,
            f"{LOCAL_PATH}/node_env",
            env_file_content
        )

    def _writeBootstrapConfigFile(self,
                                  node: Node,
                                  properties: dict[str, Any]) -> None:
        catToFile(
            node,
            f"{LOCAL_PATH}/init/bootstrap_config.json",
            jsonEncoder(properties)
        )

    def createClusterUser(self, node: Node) -> None:
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo groupadd -g 1000 {GROUPNAME} && sudo useradd -u 1000 -g 1000 -m -G sudo,docker {USERNAME}"
        ))

    def bootstrapNode(self,
                      node: Node,
                      properties: dict[str, Any],
                      process_regexes: list[str]) -> None:
        collector_address: str = ""
        if self.topology_properties.collectorInterface != None:
            collector_address = self.topology_properties.collectorInterface.addresses[0].address
        # Ensure the collector exports data for enabled features
        for feat in self.collector_features:
            properties[f"OTEL_{str(feat).upper()}_EXPORTER"] = "otlp"
        if OTELFeature.TRACES in self.collector_features:
            properties["OTEL_TRACES_SAMPLER"] = "always_on"
        properties["INSTALL_PATH"] = LOCAL_PATH
        properties["APPLICATION_VARIANT"] = str(self.variant())
        properties["APPLICATION_VERSION"] = self.version
        properties["NODE_IP"] = node.getInterfaceAddress()
        properties["EBPF_NET_INTAKE_HOST"] = collector_address
        properties["EBPF_NET_INTAKE_PORT"] = 8000
        if (self.variant() != ApplicationVariant.OTEL_COLLECTOR):
            properties["OTEL_EXPORTER_OTLP_ENDPOINT"] = f"http://{collector_address}:4318"
            properties["OTEL_SERVICES_NAME"] = f"{self.variant()}-{node.id}"
            properties["OTEL_RESOURCE_ATTRIBUTES"] = f"application={self.variant()},node={node.id}"
        properties["LD_LIBRARY_PATH"] = "/var/lib/kairos/lib:$LD_LIBRARY_PATH"
        self._writeEnvFile(
            node,
            properties
        )
        self._writeBootstrapConfigFile(
            node,
            properties
        )
        # Replace template var for pushing logs
        regexes = ",".join([f"\"{regex}\"" for regex in process_regexes])
        sed(
            node,
            {
                "@@COLLECTOR_ADDRESS@@": collector_address,
                "@@PROCESS_REGEXES@@": regexes 
            },
            f"{LOCAL_PATH}/config/otel/otel-instance-config.yaml"
        )
        # Unpack kairos libraries
        self.unpackTar(
            node,
            url="https://github.com/EngineersBox/database-benchmarking/releases/download/kairos-0.1.0/kairos-0.1.0-x86_64-unknown-linux-gnu.tar.gz",
            path="/var/lib/kairos",
            use_pg_install=False
        )
        # Login to docker registry
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            # NOTE: Yes this is unsafe for wider usage, but given
            #       it is a read-only token within a private environment
            #       it's not the worst. I don't think it's worth setting
            #       up an external credentials provider to manage this.
            command=f"sudo su {USERNAME} -c 'docker login ghcr.io -u {self.docker_config.username} -p {self.docker_config.token}'",
        ))
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"{LOCAL_PATH}/init/setup.sh"
        ))
        # Install bootstrap systemd unit and run it
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo ln -s {LOCAL_PATH}/init/bootstrap.service /etc/systemd/system/bootstrap.service && sudo systemctl start bootstrap.service"
        ))

    @abstractmethod
    def nodeInstallApplication(self, node: Node) -> None:
        self.createClusterUser(node)

    @abstractmethod
    def writeJMXCollectionConfig(self, node: Node) -> None:
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
                    name="application_heap_size",
                    description="Amount of memory to allocate as heap for the application",
                    typ=portal.ParameterType.STRING,
                    defaultValue="4G",
                    required=False
                ),
            ]
        )

    def validate(self, params: portal.Namespace) -> None:
        super().validate(params)

APPLICATION_PARAMETERS: ParameterGroup = ApplicationParameterGroup()
