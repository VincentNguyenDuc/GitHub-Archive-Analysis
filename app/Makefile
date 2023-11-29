INFRA_DIR = ./infrastructure
ORCHES_DIR = ./orchestration

terraform_build:
	terraform -chdir=${INFRA_DIR} init
	terraform -chdir=${INFRA_DIR} plan
	terraform -chdir=${INFRA_DIR} apply

terraform_destroy:
	terraform -chdir=${INFRA_DIR} destroy