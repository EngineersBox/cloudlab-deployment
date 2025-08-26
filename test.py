import math

class Node:
    interface: str

    def __init__(self, interface: str):
        self.interface = interface

class Rack:
    nodes: list[Node]

    def __init__(self, nodes: list[Node]):
        self.nodes = nodes

class DataCentre:
    racks: dict[str, Rack]

    def __init__(self, racks: dict[str, Rack]):
        self.racks = racks

class Test:
    all_ips: list[str]
    datacentres: dict[str, DataCentre]

    def __init__(self, dcs: dict[str, DataCentre]):
        self.datacentres = dcs
        self.all_ips = []
        for dc in dcs.values():
            for rack in dc.racks.values():
                self.all_ips.extend([node.interface for node in rack.nodes])

    def allocateZookeeperNodes(self) -> list[str]:
        # TODO: Test this logic and verify it splits ZK nodes
        #       between DCs and racks correctly
        num_nodes = len(self.all_ips)
        zk_count = 3
        if (num_nodes < 5):
            zk_count = min(3, num_nodes)
        elif (num_nodes < 7):
            zk_count = 5
        else:
            zk_count = 7
        num_dcs = len(self.datacentres)
        per_dc = max(1, int(math.floor(zk_count / float(num_dcs))))
        allocated = 0
        i = 0
        dcs  = list(self.datacentres.items())
        result: list[str] = []
        while (allocated < zk_count):
            dc_to_allocate = min(per_dc, zk_count - allocated)
            dc: DataCentre = dcs[i][1]
            racks = list(dc.racks.items())
            per_rack = int(math.floor(dc_to_allocate / float(len(racks))))
            rack_allocated = 0
            j = 0
            while (rack_allocated < per_dc):
                rack_to_allocate = min(per_rack, dc_to_allocate - rack_allocated)
                rack: Rack = racks[i][1]
                count = min(len(rack.nodes), rack_to_allocate)
                result.extend([node.interface for node in rack.nodes[0:count]])
                rack_allocated += count
                j += 1
            i += 1
            allocated += rack_allocated
        return result

if __name__ == '__main__':
    test = Test({
        'dc1': DataCentre(racks={
            'rack1': Rack(nodes=[
                Node('1'),
                Node('2'),
                Node('3'),
            ]),
            'rack2': Rack(nodes=[
                Node('4'),
                Node('5'),
                Node('6'),
            ])
        }),
        'dc2': DataCentre(racks={
            'rack1': Rack(nodes=[
                Node('7'),
                Node('8'),
                Node('9'),
            ]),
            'rack2': Rack(nodes=[
                Node('10'),
                Node('11'),
                Node('12'),
            ])
        })
    })
    result = test.allocateZookeeperNodes()
    print(result)
