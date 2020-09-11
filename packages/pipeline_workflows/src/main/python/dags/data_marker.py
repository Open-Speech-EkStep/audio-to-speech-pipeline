import datetime
import json
import time
from airflow import models
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator

downloaded_catalog_config = json.loads(Variable.get("downloadcatalogconfig"))
# downloaded_source_audio_format = json.loads(Variable.get("downloadedsourceaudioformat"))
composer_namespace = Variable.get("composer_namespace")
bucket_name = Variable.get("bucket")
# source_audio_format = downloaded_catalog_config["audioformat"]

default_args = {
    'email': ['gaurav.gupta@thoughtworks.com']
}

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

secret_file = secret.Secret(
    deploy_type='volume',
    deploy_target='/tmp/secrets/google',
    secret='gc-storage-rw-key',
    key='key.json')



with models.DAG(
        dag_id='data_marker_pipeline',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY) as dag:
    kubernetes_list_bucket_pod = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='data-marker',
        name='data-marker',
        cmds=["python", "invocation_script.py" ,"-a", "data_marking", "-rc", "data/audiotospeech/config/data_marker/config.yaml"],
        # namespace='composer-1-10-4-airflow-1-10-6-3b791e93',
        namespace = composer_namespace,
        startup_timeout_seconds=300,
        secrets=[secret_file],
        image='us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:1.0.0',
        image_pull_policy='Always')


