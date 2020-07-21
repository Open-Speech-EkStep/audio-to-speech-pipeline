import datetime
import json
import time
from airflow import models
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
source_batch_count = json.loads(Variable.get("sourcebatchcountforcataloguing"))
composer_namespace = Variable.get("composer_namespace")

default_args = {
    'email': ['gaurav.gupta@thoughtworks.com']
}

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

secret_file = secret.Secret(
    deploy_type='volume',
    deploy_target='/tmp/secrets/google',
    secret='gc-storage-rw-key',
    key='key.json')
dag_id='data_tagger_pipeline'
dag_number = dag_id

def create_dag(dag_id,
               dag_number,
               default_args,
               source,
               batch_count):

    dag =  models.DAG(
            dag_id,
            schedule_interval=datetime.timedelta(days=1),
            default_args=default_args,
            start_date=YESTERDAY)
    with dag:
        downloaded_data_cataloguer = kubernetes_pod_operator.KubernetesPodOperator(
            task_id='data-tagger',
            name='data-tagger',
            cmds=["python", "-m", "src.scripts.downloaded_data_cataloguer", "cluster", "ekstepspeechrecognition-dev", "data/audiotospeech/config/downloaded_data_cataloguer/config.yaml",source,batch_count],

            namespace=composer_namespace,
            startup_timeout_seconds=300,
            secrets=[secret_file],
            image='us.gcr.io/ekstepspeechrecognition/downloaded_data_cataloguer:1.0.0',
            image_pull_policy='Always')

        downloaded_data_cataloguer

    return dag

for source, batch_count in source_batch_count.items():
    dag_id = f"cataloguing_{source}_{batch_count}"

    default_args = {
        'email': ['gaurav.gupta@thoughtworks.com']
    }

    # schedule = '@daily'

    dag_number = dag_id + str(batch_count)

    globals()[dag_id] = create_dag(dag_id,
                                   dag_number,
                                   default_args,
                                   source,
                                   batch_count)
