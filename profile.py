import geni.portal as portal
import geni.rspec.pg as pg 
from provisioner.docker import DOCKER_PARAMETERS
from provisioner.structure.cluster import CLUSTER_PARAMETERS
from provisioner.application.app import APPLICATION_PARAMETERS
from provisioner.parameters import ParameterGroup
from provisioner.provisoner import Provisioner
from provisioner.collector.collector import COLLECTOR_PARAMETERS

DEBUG_OUTPUT: bool = True
PARAMETER_GROUPS: list[ParameterGroup] = [
    CLUSTER_PARAMETERS,
    APPLICATION_PARAMETERS,
    COLLECTOR_PARAMETERS,
    DOCKER_PARAMETERS
]

def bindAndValidateParameters() -> portal.Namespace:
    for parameterGroup in PARAMETER_GROUPS:
        parameterGroup.bind()
    params: portal.Namespace = portal.context.bindParameters()
    for parameterGroup in PARAMETER_GROUPS:
        parameterGroup.validate(params)
    portal.context.verifyParameters()
    return params

def main() -> None:
    params: portal.Namespace = bindAndValidateParameters()
    request: pg.Request = portal.context.makeRequestRSpec()
    provisioner: Provisioner = Provisioner(request, params)
    cluster, collector = provisioner.provision()
    if DEBUG_OUTPUT:
        request.writeXML("./test.xml")
    else:
        portal.context.printRequestRSpec()

if __name__ == "__main__":
    main()
