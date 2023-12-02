INFRA_DIR = ./infrastructure
ORCHES_DIR = ./orchestration
EXPLOR_DIR = ./exploration

terraform_build:
	terraform -chdir=${INFRA_DIR} init
	terraform -chdir=${INFRA_DIR} plan
	terraform -chdir=${INFRA_DIR} apply

terraform_destroy:
	terraform -chdir=${INFRA_DIR} destroy

clean_directory:
	find . -type d -name __pycache__ -prune -exec rm -rf {} \;
	find . -type d -name tmp -prune -exec rm -rf {} \;
	rm -rf ${EXPLOR_DIR}/data/*.json
	rm -rf ${EXPLOR_DIR}/data/*.gz
