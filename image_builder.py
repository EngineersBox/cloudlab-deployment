import geni.portal as portal
import geni.rspec.pg as pg 

def main():
    request: pg.Request = portal.context.makeRequestRSpec()
    node_vm = pg.RawPC("node")
    node_vm.hardware_type = "m400"
    node_vm.disk_image = "urn:publicid:IDN+utah.cloudlab.us+image+emulab-ops:UBUNTU22-64-ARM"
    request.addResource(node_vm)
    request.writeXML("./image_builder_profile.xml")

if __name__ == "__main__":
    main()
