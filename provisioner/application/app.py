from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Optional
from provisioner.application.config import bashEncoder, jsonEncoder
from provisioner.collector.collector import OTELFeature
from provisioner.docker import DockerConfig
from provisioner.structure.cluster import Cluster
from provisioner.parameters import ParameterGroup, Parameter
from provisioner.structure.node import Node
from provisioner.structure.topology_assigner import InverseProvisioningTopology, ProvisioningTopology
from provisioner.topology import TopologyProperties
from provisioner.utils import catToFile, sed
import geni.portal as portal
from geni.rspec import pg
import json

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

class ServiceStartTiming(Enum):
    BEFORE_INIT = "\"BEFORE_INIT\""
    AFTER_INIT = "\"AFTER_INIT\""

    def toBashLiteral(self) -> str:
        return self.value

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
            path = VAR_LIB_PATH
        if (use_pg_install):
            node.instance.addService(pg.Install(
                url=url,
                path=path
            ))
        else:
            node.instance.addService(pg.Execute(
                shell="/bin/bash",
                command=f"sudo wget {url} && sudo tar -xzf {self.variant()}.tar.gz -C /var/lib/. && sudo rm {self.variant()}.tar.gz"
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
            f"{LOCAL_PATH}/init/bootstrap/bootstrap_config.json",
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
                      service_start_timing: ServiceStartTiming = ServiceStartTiming.BEFORE_INIT) -> None:
        collector_address: str = self.topology_properties.collectorInterface.addresses[0].address
        # Ensure the collector exports data for enabled features
        for feat in self.collector_features:
            properties[f"OTEL_{str(feat).upper()}_EXPORTER"] = "otlp"
        if OTELFeature.TRACES in self.collector_features:
            properties["OTEL_TRACES_SAMPLER"] = "always_on"
        properties["SERVICE_START_TIMING"] = service_start_timing.toBashLiteral()
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
        self._writeEnvFile(
            node,
            properties
        )
        self._writeBootstrapConfigFile(
            node,
            properties
        )
        # Replace template var for pushing logs
        sed(
            node,
            {
                "@@COLLECTOR_ADDRESS@@": collector_address
            },
            f"{LOCAL_PATH}/config/otel/otel-instance-config.yaml"
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
            command=f"sudo bash {LOCAL_PATH}/init/bootstrap/setup.sh"
        ))
        # Install bootstrap systemd unit and run it
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo ln -s {LOCAL_PATH}/init/bootstrap/bootstrap.service /etc/systemd/system/bootstrap.service && sudo systemctl start bootstrap.service"
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
