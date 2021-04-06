source scripts/set_env.sh

nohup .workspace/cloud_sql_proxy -dir=./workspace/cloudsql -instances=${GCP_PROJECT}:${GCP_REGION}:${DB_NAME}=tcp:5432 &