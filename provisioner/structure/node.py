from dataclasses import dataclass, field
from typing import Optional
import geni.rspec.pg as pg 

@dataclass
class Node:
    id: str
    instance: pg.RawPC
    size: str
    interface: pg.Interface
    config: Optional[str] = None
    roles: list[str] = field(default_factory=list)

    def __hash__(self) -> int:
        return self.id.__hash__()

    def getInterfaceAddress(self) -> str:
        return self.interface.addresses[0].address
