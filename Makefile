INFRA_DIR = ./app/infra
FLOWS_DIR = ./app/flows

terraform_build:
	terraform -chdir=${INFRA_DIR} init
	terraform -chdir=${INFRA_DIR} plan
	terraform -chdir=${INFRA_DIR} apply

terraform_destroy:
	terraform -chdir=${INFRA_DIR} destroy

run_flows:
	python3 ${FLOWS_DIR}/etl_web_to_gcs.py
	python3 ${FLOWS_DIR}/etl_gcs_to_bq.py