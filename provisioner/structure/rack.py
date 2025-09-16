from dataclasses import dataclass
from .node import Node

@dataclass
class Rack:
    name: str
    # name mapped nodes
    nodes: dict[str, Node]
