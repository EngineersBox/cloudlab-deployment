from dataclasses import dataclass, field
import geni.rspec.pg as pg

from provisioner.structure.node import Node

@dataclass
class TopologyProperties:
    collectorInterface: pg.Interface
    db_nodes: list[Node] = field(default_factory=list)
