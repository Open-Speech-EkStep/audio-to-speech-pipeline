#!/bin/bash
. ./env-config.cfg
#Fetch current Composer environment details
echo $ENV
for env in  $(gcloud composer environments list --project=$PROJECT_NAME --locations=$LOCATION --format="value(NAME)")
do
  echo "The composer is : " $env
  if [ -z "$env" ]; then 
  	echo "ERROR: There is no existing Composer environment, hence exiting..." >&2
  	exit 1
  fi

  case "$env" in
    *_"$ENV"_*) COMPOSER_ENV=$env;variables_json="airflow_config_file_${ENV}.json";echo "Composer environment name: $COMPOSER_ENV"  ;;
    *)              echo 'False'
  esac



done
#Upload env variables

echo "$variables_json file"
echo "$PATH path "
echo "$(python -V)"
pyenv install --list
sudo -E env "PATH=$PATH" gcloud --quiet components update
sudo -E env "PATH=$PATH" gcloud --quiet components update kubectl
sudo chmod 757 /home/ubuntu/.config/gcloud/logs -R
# pyenv local 3.7.0
composer=$(gcloud composer environments describe $COMPOSER_ENV --location $LOCATION | grep  "gkeCluster" | cut -d '/' -f 6-)
gcloud container clusters get-credentials $composer --zone us-east1-b --project $PROJECT_NAME
# gcloud components update
# sudo gcloud components install kubectl
echo $variables_json
ls ./src/main/python/resources/${variables_json}
gcloud composer environments storage data import --environment $COMPOSER_ENV --location $LOCATION --source ./src/main/python/resources/${variables_json}
gcloud composer environments run $COMPOSER_ENV --location $LOCATION variables -- --import /home/airflow/gcs/data/${variables_json}

# Upload DAG files to Composer bucket
for file in ./src/main/python/dags/*.py
do
	echo "Uploading DAG file: $file to Composer Bucket"
	gcloud composer environments storage dags import \
	    --environment ${COMPOSER_ENV} \
	    --location ${LOCATION} \
	    --source $file
done
