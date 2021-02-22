#!/usr/bin/env bash

REGION=us-central1
DB_NAME=crowdsourcedb
GOOGLE_AUTH=$1

cd ${HOME}
wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-265.0.0-linux-x86_64.tar.gz
tar -zxf google-cloud-sdk-*
cd google-cloud-sdk
pwd
./install.sh --quiet
echo ${GOOGLE_AUTH} > ${HOME}/gcp-key.json
./bin/gcloud auth activate-service-account --key-file ${HOME}/gcp-key.json
./bin/gcloud --quiet config set project ${GCP_PROJECT}
export GOOGLE_APPLICATION_CREDENTIALS=${HOME}/gcp-key.json
echo $GOOGLE_APPLICATION_CREDENTIALS
wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy
chmod +x cloud_sql_proxy
nohup ./cloud_sql_proxy -dir=./cloudsql -instances=${GCP_PROJECT}:${REGION}:${DB_NAME}=tcp:5432 &
sleep 25s
cat nohup.out
