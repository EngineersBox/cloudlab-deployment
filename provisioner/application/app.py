from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional
from provisioner.collector.collector import OTELFeature
from provisioner.docker import DockerConfig
from provisioner.structure.cluster import Cluster
from provisioner.parameters import ParameterGroup, Parameter
from provisioner.structure.node import Node
from provisioner.topology import TopologyProperties
from provisioner.utils import catToFile, sedReplaceMappings
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
GROUPNAME = "cluster"

class ServiceStartTiming(Enum):
    BEFORE_INIT = "\"BEFORE_INIT\""
    AFTER_INIT = "\"AFTER_INIT\""

    def toBashLiteral(self) -> str:
        return self.value

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
                  path: Optional[str] = None,
                  use_pg_install: bool = True) -> None:
        if url == None:
            url=f"https://github.com/EngineersBox/cassandra-benchmarking/releases/download/{self.variant()}-{self.version}/{self.variant()}.tar.gz"
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
                      properties: dict[str, str]) -> None:
        collector_address: str = self.topologyProperties.collectorInterface.addresses[0].address
        # Ensure the collector exports data for enabled features
        for feat in self.collectorFeatures:
            properties[f"OTEL_{str(feat).upper()}_EXPORTER"] = "otlp"
        if OTELFeature.TRACES in self.collectorFeatures:
            properties["OTEL_TRACES_SAMPLER"] = "always_on"
        # Bash env file
        env_file_content = f"""# Node configuration properties
INSTALL_PATH={LOCAL_PATH}
APPLICATION_VARIANT={self.variant()}
APPLICATION_VERSION={self.version}
NODE_IP={node.getInterfaceAddress()}

EBPF_NET_INTAKE_HOST={collector_address}
EBPF_NET_INTAKE_PORT=8000
"""
        if (self.variant() != ApplicationVariant.OTEL_COLLECTOR):
            env_file_content += f"""
OTEL_EXPORTER_OTLP_ENDPOINT=http://{collector_address}:4318
OTEL_SERVICE_NAME={self.variant()}-{node.id}
OTEL_RESOURCE_ATTRIBUTES=application={self.variant()},node={node.id}
"""

        for (k,v) in properties.items():
            env_file_content += f"\n{k}={v}"
        # Bash sourcable configuration properties that the
        # bootstrap script uses as well as docker containers
        catToFile(
            node,
            f"{LOCAL_PATH}/node_env",
            env_file_content
        )
        # Replace template var for pushing logs
        sedReplaceMappings(
            node,
            {
                "@@COLLECTOR_ADDRESS@@": collector_address
            },
            f"{LOCAL_PATH}/config/otel/otel-instance-config.yaml"
        )

    def createClusterUser(self, node: Node) -> None:
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo groupadd -g 1000 {GROUPNAME} && sudo useradd -u 1000 -g 1000 -m -G sudo,docker {USERNAME}"
        ))

    def bootstrapNode(self,
                      node: Node,
                      properties: dict[str, str],
                      service_start_timing: ServiceStartTiming = ServiceStartTiming.BEFORE_INIT) -> None:
        properties["SERVICE_START_TIMING"] = service_start_timing.toBashLiteral()
        self._writeEnvFile(
            node,
            properties
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
        # Install bootstrap systemd unit and run it
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo ln -s {LOCAL_PATH}/units/bootstrap.service /etc/systemd/system/bootstrap.service && sudo systemctl start bootstrap.service"
        ))

    @abstractmethod
    def nodeInstallApplication(self, node: Node) -> None:
        self.createClusterUser(node)

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
