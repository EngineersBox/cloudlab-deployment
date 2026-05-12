import xml.etree.ElementTree as ET
import sys, os, logging, logging.config, coloredlogs, caseconverter, urllib.request
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse, unquote
from dataclasses import dataclass
from typing import Tuple
from enum import Enum
from jinja2 import Environment, FileSystemLoader, select_autoescape

env = Environment(
    loader = FileSystemLoader("templates"),
    autoescape = select_autoescape()
)

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

logging.config.fileConfig(fname="logger.ini", disable_existing_loggers=False)
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
    cidr_blocks: list[str] = []

APPLICATION_PORTS: dict[ApplicationVariant, list[Port]] = {
}

@dataclass
class CloudinitConfigPart:
    filename: str
    content_type: str
    content: str

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
        elif (service.tag == "command"):
            shell = service.attrib["shell"]
            command = service.attrib["command"]
            script_lines.append(f"{shell} -c {command}")
        else:
            LOGGER.warning(f"Unknown service type '{service.tag}', skipping")
    return "\n".join(script_lines)

def provisionNode(node: ET.Element,
                  app_variant: ApplicationVariant,
                  node_ips: dict[str, str]) -> str:
    node_id = caseconverter.snakecase(node.attrib["client_id"])
    services = node.find("./services")
    if (services == None):
        LOGGER.error("Expected <services></services> tags in node but found none")
        exit(1)
    parts: list[CloudinitConfigPart] = [
        CloudinitConfigPart(
            "/etc/boot_run.sh",
            "text/x-shellscript",
            servicesToShellScript(services)
        ),
        CloudinitConfigPart(
            "cloud-config.yaml",
            "text/cloud-config",
            "runcmd:\n - [ \"/bin/bash\", \"/etc/boot_run.sh\" ]"
        )
    ]
    return env.get_template("templates/aws_node.tf.j2").render({
        "node_id": node_id,
        "node_name": node_id,
        "application": app_variant,
        "node_dependecies": [],
        "cloudinit_config_parts": parts
    })

def provisionNetworking(node: ET.Element,
                        app_variant: ApplicationVariant,
                        node_ips: list[str]) -> str:
    external_ip = urllib.request.urlopen('https://v4.ident.me').read().decode('utf8')
    ssh_ingess_ips = list(map(lambda ip: f"{ip}/32", node_ips)) + [f"{external_ip}/32"]
    cluster_ports = []
    for port in APPLICATION_PORTS[app_variant]:
        cluster_ports.append(Port(
            to_port=port.to_port,
            from_port=port.from_port,
            protocol=port.protocol,
            cidr_blocks=list(map(lambda ip: f"{ip}/32", node_ips)) + [f"{external_ip}/32"]
        ))
    return env.get_template("templates/security_group.tf.j2").render({
        "cluster_ports": cluster_ports,
        "ssh_ingess_ips": ssh_ingess_ips
    })

def main(profile_xml_path: str, app_variant: ApplicationVariant, output_dir: str) -> None:
    tree = ET.parse(profile_xml_path)
    root = tree.getroot()
    node_content: dict[str, str] = {}
    node_ips: dict[str, str] = {}
    for child in root:
        if (child.tag != "node"):
            continue
        node_id = child.attrib["client_id"]
        ip_node = child.find("./interface[@client_id='{node_id}:eth0']/ip")
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
            node_content[child.attrib["client_id"]] = provisionNode(child, app_variant, node_ips)
        elif (child.tag == "link"):
            LOGGER.info("Provisioning network link")
            network_content["link"] = provisionNetworking(
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

if __name__ == "__main__":
    if (len(sys.argv) != 4):
        LOGGER.error(f"Usage: {sys.argv[0]} <cloudlab profile.xml path> <application type> <output dir>")
        exit(1)
    app_variant: ApplicationVariant = ApplicationVariant[sys.argv[2].upper()]
    main(sys.argv[1], app_variant, sys.argv[3])
