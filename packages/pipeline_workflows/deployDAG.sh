#!/bin/bash

#Fetch current Composer environment details
COMPOSER_ENV="composer"
PROJECT_NAME="ekstepspeechrecognition"
LOCATION="us-east1"
for env in  $(gcloud composer environments list --project=$PROJECT_NAME --locations=$LOCATION --format="value(NAME)")
do
  if [ -z "$env" ]; then 
  	echo "ERROR: There is no existing Composer environment, hence exiting..." >&2
  	exit -1
  fi		
  COMPOSER_ENV=$env
  echo "Composer environment name: $COMPOSER_ENV" 
done
#Upload env variables
variables_json="./src/main/python/resources/airflow_config_file.json"
echo "$variables_json file"
echo "$PATH path "
sudo -E env "PATH=$PATH" gcloud --quiet components update kubectl
sudo chmod 757 /home/ubuntu/.config/gcloud/logs -R
gcloud container clusters get-credentials $COMPOSER_ENV --zone us-east1-b --project $PROJECT_NAME
# gcloud components update
# sudo gcloud components install kubectl

gcloud composer environments storage data import --environment $COMPOSER_ENV --location $LOCATION --source ./src/main/python/resources/airflow_config_file.json
gcloud composer environments run composer --location $LOCATION variables -- --import /home/airflow/gcs/data/airflow_config_file.json

# Upload DAG files to Composer bucket
for file in ./src/main/python/dags/*.py
do
	echo "Uploading DAG file: $file to Composer Bucket"
	gcloud composer environments storage dags import \
	    --environment ${COMPOSER_ENV} \
	    --location ${LOCATION} \
	    --source $file
done
