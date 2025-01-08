from dataclasses import dataclass
import geni.portal as portal
from provisioner.parameters import Parameter, ParameterGroup

@dataclass
class DockerConfig:
    username: str
    token: str

class DockerParameterGroup(ParameterGroup):

    @classmethod
    def name(cls) -> str:
        return "Docker"

    @classmethod
    def id(cls) -> str:
        return "docker"

    def __init__(self):
        super().__init__(
            parameters=[
                Parameter(
                    name="github_username",
                    description="GitHub Username",
                    typ=portal.ParameterType.STRING,
                    required=True
                ),
                Parameter(
                    name="github_token",
                    description="Read-only token",
                    typ=portal.ParameterType.STRING,
                    required=True
                )
            ]
        )

DOCKER_PARAMETERS: ParameterGroup = DockerParameterGroup()
