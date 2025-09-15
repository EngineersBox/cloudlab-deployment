from dataclasses import dataclass
from .node import Node

@dataclass
class Rack:
    name: str
    # role mapped nodes
    nodes: dict[str, Node]
