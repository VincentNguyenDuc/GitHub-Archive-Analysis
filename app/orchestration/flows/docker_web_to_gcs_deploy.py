from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from etl_web_to_gcs import main_gcs_flow
from constants import PrefectConstants

docker_block = DockerContainer.load(PrefectConstants.DOCKER_BLOCK)
docker_dep = Deployment.build_from_flow(
    flow=main_gcs_flow,
    name="docker-web-to-gcs-flow",
    infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep.apply()
