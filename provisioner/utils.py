import geni.rspec.pg as pg

from provisioner.structure.node import Node

def catToFile(node: Node, path: str, content: str) -> None:
    if not content.endswith("\n"):
        content += "\n"
    node.instance.addService(pg.Execute(
        shell="/bin/bash",
        command=f"sudo cat > {path} <<-EOF\n{content}EOF"
    ))

def appendToFile(node: Node, path: str, content: str) -> None:
    if not content.endswith("\n"):
        content += "\n"
    node.instance.addService(pg.Execute(
        shell="/bin/bash",
        command=f"sudo cat >> {path} <<-EOF\n{content}EOF"
    ))

def chmod(node: Node, path: str, permissions: int, recursive: bool = False) -> None:
    node.instance.addService(pg.Execute(
        shell="/bin/bash",
        command=f"sudo chmod {'-R ' if recursive else ''}0{permissions:o} {path}"
    ))

def chown(node: Node, path: str, user: str, group: str) -> None:
    node.instance.addService(pg.Execute(
        shell="/bin/bash",
        command=f"sudo chown {user}:{group} {path}"
    ))

def mkdir(node: Node, path: str, create_parent: bool = True) -> None:
    node.instance.addService(pg.Execute(
        shell="/bin/bash",
        command=f"sudo mkdir {'-p ' if create_parent else ''} {path}"
    ))

def ifaceForIp(ip: str) -> str:
    return f"sudo ifconfig | grep -B1 {ip} | grep -o '^\\w*'"

def sed(node: Node, mappings: dict[str, str], path: str) -> None:
    for key, value in mappings.items():
        node.instance.addService(pg.Execute(
            shell="/bin/bash",
            command=f"sudo sed -i \"s/{key}/{value}/g\" {path}"
        ))
