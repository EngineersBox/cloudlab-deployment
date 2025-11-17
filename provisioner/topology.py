from dataclasses import dataclass, field
from typing import Optional
import geni.rspec.pg as pg

from provisioner.structure.node import Node

@dataclass
class TopologyProperties:
    collectorInterface: Optional[pg.Interface]
    db_nodes: dict[str, Node] = field(default_factory=dict)
