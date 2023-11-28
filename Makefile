INFRA_DIR = ./app/infra
FLOWS_DIR = ./app/flows
FLOWS_DEPLOYMENT_DIR = ${FLOWS_DIR}/deploy

terraform_build:
	terraform -chdir=${INFRA_DIR} init
	terraform -chdir=${INFRA_DIR} plan
	terraform -chdir=${INFRA_DIR} apply

terraform_destroy:
	terraform -chdir=${INFRA_DIR} destroy

run_flows:
	python3 ${FLOWS_DIR}/etl_web_to_gcs.py
	python3 ${FLOWS_DIR}/etl_gcs_to_bq.py

prefect_build_web_to_gcs:
	prefect deployment build ${FLOWS_DIR}/etl_web_to_gcs.py:main_gcs_flow -n "Build Deployment from Web to GCS"
	mv .prefectignore ${FLOWS_DEPLOYMENT_DIR}
	mv main_gcs_flow-deployment.yaml ${FLOWS_DEPLOYMENT_DIR}

prefect_build_gcs_to_bq:
	prefect deployment build ${FLOWS_DIR}/etl_gcs_to_bq.py:main_bq_flow -n "Build Deployment from GCS to BQ"
	mv .prefectignore ${FLOWS_DEPLOYMENT_DIR}
	mv main_bq_flow-deployment.yaml ${FLOWS_DEPLOYMENT_DIR}

prefect_apply_web_to_gcs:
	prefect deployment apply ${FLOWS_DEPLOYMENT_DIR}/main_gcs_flow-deployment.yaml

prefect_apply_gcs_to_bq:
	prefect deployment apply ${FLOWS_DEPLOYMENT_DIR}/main_bq_flow-deployment.yaml