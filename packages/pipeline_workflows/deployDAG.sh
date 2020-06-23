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

# Upload DAG files to Composer bucket
for file in ./src/main/python/dags/*.py
do
	echo "Uploading DAG file: $file to Composer Bucket"
	gcloud composer environments storage dags import \
	    --environment ${COMPOSER_ENV} \
	    --location ${LOCATION} \
	    --source $file
done
