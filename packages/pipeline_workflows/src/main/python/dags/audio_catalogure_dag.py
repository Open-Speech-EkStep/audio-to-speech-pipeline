# [START composer_kubernetespodoperator]
import datetime
from airflow import models
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.models import Variable

composer_namespace = Variable.get("composer_namespace")
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")
default_args = {
    'email': ['gaurav.gupta@thoughtworks.com']
}

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# [START composer_kubernetespodoperator_secretobject]
# First define a secret from a file
secret_file = secret.Secret(
    deploy_type='volume',
    deploy_target='/tmp/secrets/google',
    secret='gc-storage-rw-key',
    key='key.json')
# [END composer_kubernetespodoperator_secretobject]

# If a Pod fails to launch, or has an error occur in the container, Airflow
# will show the task as failed, as well as contain all of the task logs
# required to debug.
with models.DAG(
        dag_id='data_prep_cataloguer_pipeline',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY) as dag:
    kubernetes_list_bucket_pod = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='data-normalizer',
        name='data-normalizer',
        cmds=["python", "invocation_script.py", "-b",bucket_name, "-a", "audio_cataloguer", "-rc",f"data/audiotospeech/config/config.yaml"],
        namespace = composer_namespace,
        startup_timeout_seconds=300,
        secrets=[secret_file],
        image=f'us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:{env_name}_1.0.0',
        image_pull_policy='Always')
