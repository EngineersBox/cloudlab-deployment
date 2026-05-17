import re
import shutil
import uuid
import xml.etree.ElementTree as ET
import sys, os, logging, logging.config, coloredlogs, caseconverter, urllib.request
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse, unquote
from dataclasses import dataclass, field
from typing import Optional, Tuple
from enum import Enum
from jinja2 import Environment, FileSystemLoader, select_autoescape
from uuid import UUID, uuid4

env = Environment(
    loader = FileSystemLoader("aws_prov/templates"),
    autoescape = select_autoescape()
)

IPV4_PATTERN = re.compile(r"(\d{1,3}\d.){3}\d{1,3}")

class ColoredFormatter(coloredlogs.ColoredFormatter):
    def __init__(self, fmt=None, datefmt=None, style='%'):
        '''Match coloredlogs.ColoredFormatter arguments with logging.Formatter'''
        coloredlogs.ColoredFormatter.__init__(self, fmt=fmt, datefmt=datefmt)
        coloredlogs.DEFAULT_FIELD_STYLES = {
            "asctime": {
                "color": "green"
            },
            "hostname": {
                "color": "magenta"
            },
            "levelname": {
                "faint": True,
                "color": 214
            },
            "name": {
                "color": "blue"
            },
            "programname": {
                "color": "cyan"
            },
            "username": {
                "color": "yellow"
            }
        }
        coloredlogs.DEFAULT_LEVEL_STYLES = {
            "critical": {
                "bold": True,
                "color": "red"
            },
            "logging": {
                "color": "magenta"
            },
            "error": {
                "color": "red"
            },
            "info": {
                "color": "blue"
            },
            "notice": {
                "color": "magenta"
            },
            "spam": {
                "color": "green",
                "faint": True
            },
            "success": {
                "bold": True,
                "color": "green"
            },
            "verbose": {
                "color": "blue"
            },
            "warning": {
                "color": "yellow"
            }
        }

logging.config.fileConfig(fname="aws_prov/logger.ini", disable_existing_loggers=False)
LOGGER = logging.getLogger("AWS Provisioner")

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

@dataclass
class Port:
    to_port: int
    from_port: int
    protocol: str
    cidr_blocks: list[str] = field(default_factory=list)

APPLICATION_PORTS: dict[ApplicationVariant, list[Port]] = {
    ApplicationVariant.HBASE: [
        # HBase Master
        Port(16000, 16000, "tcp"),
        Port(16010, 16010, "tcp"),
        Port(16100, 16100, "tcp"),
        # HBase RegionServer
        Port(16020, 16020, "tcp"),
        Port(16030, 16030, "tcp"),
        Port(8080, 8080, "tcp"),
        Port(8085, 8085, "tcp"),
        Port(9090, 9090, "tcp"),
        Port(9095, 9095, "tcp"),
        # Zookeeper
        Port(2181, 2181, "tcp"),
        Port(2888, 2888, "tcp"),
        Port(3888, 3888, "tcp"),
    ]
}

@dataclass
class CloudinitConfigPart:
    filename: str
    content_type: str
    content: Optional[str] = None
    content_file: Optional[str] = None

def servicesToShellScript(services: ET.Element) -> str:
    script_lines = ["#!/bin/bash"]
    for service in services:
        if (service.tag == "install"):
            install_path = service.attrib["install_path"]
            script_lines.append(f"mkdir -p \"{install_path}\"")
            url = service.attrib["url"]
            script_lines.append(f"wget -P \"{install_path}\" \"{url}\"")            
            unpack_cmd = "tar"
            if (url.endswith(".tar.gz")):
                unpack_cmd = "tar -xf"
            elif (url.endswith(".zip")):
                unpack_cmd = "unzip"
            else:
                LOGGER.error(f"Unknown unpack command for resource at URL: {url}")
            url_parsed = urlparse(url)
            filename = unquote(PurePosixPath(url_parsed.path).name)
            script_lines.append(f"{unpack_cmd} \"{install_path}/{filename}\"")
            script_lines.append(f"rm -f \"{install_path}/{filename}\"")
        elif (service.tag == "execute"):
            shell = service.attrib["shell"]
            command = service.attrib["command"]
            script_lines.append(f"{shell} -c {command}")
        else:
            LOGGER.warning(f"Unknown service type '{service.tag}', skipping")
    return "\n".join(script_lines)

def provisionNode(cluster_id: UUID,
                  node: ET.Element,
                  app_variant: ApplicationVariant,
                  output_dir: str) -> str:
    node_id = caseconverter.snakecase(node.attrib["client_id"])
    services = node.find("./services")
    if (services == None):
        LOGGER.error("Expected <services></services> tags in node but found none")
        exit(1)
    with open(f"{output_dir}/{app_variant}_{node_id}_boot_run.sh", "w") as f:
        f.write(servicesToShellScript(services))
    LOGGER.info(f"Written {node_id} Cloudinit Script to {output_dir}/{app_variant}_{node_id}_boot_run.sh")
    parts: list[CloudinitConfigPart] = [
        CloudinitConfigPart(
            "/etc/boot_run.sh",
            "text/x-shellscript",
            content_file=f"{app_variant}_{node_id}_boot_run.sh"
        ),
        CloudinitConfigPart(
            "cloud-config.yaml",
            "text/cloud-config",
            content="runcmd:\n - [ \"/bin/bash\", \"/etc/boot_run.sh\" ]"
        )
    ]
    return env.get_template("aws_node.tf.j2").render({
        "cluster_id": str(cluster_id),
        "node_id": node_id,
        "node_name": node_id,
        "application": app_variant,
        "node_dependecies": [],
        "cloudinit_config_parts": parts
    })

def provisionNetworking(cluster_id: UUID,
                        node: ET.Element,
                        app_variant: ApplicationVariant,
                        node_ips: list[str]) -> str:
    external_ip = urllib.request.urlopen('https://v4.ident.me').read().decode('utf8')
    if (IPV4_PATTERN.match(external_ip) == None):
        LOGGER.error("External IP is not a vlaid IPv4 address: {external_ip}")
        exit(1)
    ssh_ingess_ips = list(map(lambda ip: f"{ip}/32", node_ips)) + [f"{external_ip}/32"]
    cluster_ports = []
    for port in APPLICATION_PORTS[app_variant]:
        cluster_ports.append(Port(
            to_port=port.to_port,
            from_port=port.from_port,
            protocol=port.protocol,
            cidr_blocks=list(map(lambda ip: f"{ip}/32", node_ips)) + [f"{external_ip}/32"]
        ))
    return env.get_template("security_group.tf.j2").render({
        "cluster_id": str(cluster_id),
        "cluster_ports": cluster_ports,
        "ssh_ingess_ips": ssh_ingess_ips,
        "application": app_variant,
        "node_id": caseconverter.snakecase(node.attrib["client_id"])
    })

def removeNamespace(doc, namespace):
    ns = u'{%s}' % namespace
    nsl = len(ns)
    for elem in doc.iter():
        if elem.tag.startswith(ns):
            elem.tag = elem.tag[nsl:]

def main(profile_xml_path: str, app_variant: ApplicationVariant, output_dir: str) -> None:
    cluster_id = uuid4()
    tree = ET.parse(profile_xml_path)
    root = tree.getroot()
    removeNamespace(root, "http://www.geni.net/resources/rspec/3")
    removeNamespace(root, "http://www.protogeni.net/resources/rspec/ext/client/1")
    removeNamespace(root, "http://www.protogeni.net/resources/rspec/ext/emulab/1")
    node_content: dict[str, str] = {}
    node_ips: dict[str, str] = {}
    for child in root.iterfind("node"):
        node_id = child.attrib["client_id"]
        ip_node = child.find("./interface/ip")
        if (ip_node == None):
            LOGGER.error(f"Failed to extract ip for node {node_id}")
            exit(1)
        ip_addr = ip_node.attrib["address"]
        LOGGER.info(f"Extracted node ip: {ip_addr}")
        node_ips[node_id] = ip_addr
    network_content: dict[str, str] = {}
    for child in root:
        if (child.tag == "node"):
            node_id = child.attrib["client_id"]
            LOGGER.info(f"Provisioning node {node_id}")
            node_content[child.attrib["client_id"]] = provisionNode(
                cluster_id,
                child,
                app_variant,
                output_dir
            )
        elif (child.tag == "link"):
            LOGGER.info("Provisioning network link")
            network_content["link"] = provisionNetworking(
                cluster_id,
                child,
                app_variant,
                list(node_ips.values())
            )
    main_content = env.get_template("main.tf.j2").render({
        "nodes": list(node_content.values()),
        "networking": list(network_content.values())
    })
    with open(f"{output_dir}/main.tf", "w") as f:
        f.write(main_content)
    LOGGER.info(f"Written content to {output_dir}/main.tf")
    shutil.copyfile(
        "aws_prov/templates/variables.tf",
        f"{output_dir}/variables.tf"
    )
    LOGGER.info(f"Copied variables to {output_dir}/variables.tf")

if __name__ == "__main__":
    if (len(sys.argv) != 4):
        LOGGER.error(f"Usage: {sys.argv[0]} <cloudlab profile.xml path> <application type> <output dir>")
        exit(1)
    app_variant: ApplicationVariant = ApplicationVariant[sys.argv[2].upper()]
    main(sys.argv[1], app_variant, sys.argv[3])
