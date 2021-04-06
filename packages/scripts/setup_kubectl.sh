source scripts/set_env.sh

composer=$(gcloud composer environments describe ${COMPOSER_ENV_NAME} --location ${LOCATION} | grep  ${CLUSTER} | cut -d '/' -f 6-)

gcloud container clusters get-credentials ${composer} --zone ${ZONE} --project ${GCP_PROJECT}