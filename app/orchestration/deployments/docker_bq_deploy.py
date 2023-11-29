from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from ..flows.etl_gcs_to_bq import main_bq_flow
from constants import PrefectConstants

docker_block = DockerContainer.load(PrefectConstants.DOCKER_BLOCK)
docker_dep = Deployment.build_from_flow(
    flow=main_bq_flow,
    name="docker-bq-flow",
    infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep.apply()
