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
        dag_id='datadownload_youtube_pipeline',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY) as dag:
    # Only name, namespace, image, and task_id are required to create a
    # KubernetesPodOperator. In Cloud Composer, currently the operator defaults
    # to using the config file found at `/home/airflow/composer_kube_config if
    # no `config_file` parameter is specified. By default it will contain the
    # credentials for Cloud Composer's Google Kubernetes Engine cluster that is
    # created upon environment creation.

    # [START composer_kubernetespodoperator_minconfig]
    kubernetes_list_bucket_pod = kubernetes_pod_operator.KubernetesPodOperator(
        # The ID specified for the task.
        task_id='datadownload-youtube',
        # Name of task you want to run, used to generate Pod ID.    
        name='datadownload-youtube',
        # Entrypoint of the container, if not specified the Docker container's
        # entrypoint is used. The cmds parameter is templated.
        # cmds=['echo'],
        cmds=["python", "-m", "src.scripts.download", "cluster", bucket_name, "data/audiotospeech/config/datadownload/config.yaml"],
        #cmds=["python", "src/scripts/pipeline.py","cluster", bucket_name, "data/texttospeech/config/curation/config.yaml"],
        # The namespace to run within Kubernetes, default namespace is
        # `default`. There is the potential for the resource starvation of
        # Airflow workers and scheduler within the Cloud Composer environment,
        # the recommended solution is to increase the amount of nodes in order
        # to satisfy the computing requirements. Alternatively, launching pods
        # into a custom namespace will stop fighting over resources.

        # namespace='composer-1-10-4-airflow-1-10-6-6928624a',
        namespace=composer_namespace,
        startup_timeout_seconds=300,
        secrets=[secret_file],
        image=f'us.gcr.io/ekstepspeechrecognition/datacollector_youtube_{env_name}:2.0.0',
        image_pull_policy='Always')
    # config_file="{{ conf.get('core', 'kube_config') }}")
    # Docker image specified. Defaults to hub.docker.com, but any fully
    # qualified URLs will point to a custom repository. Supports private
    # gcr.io images if the Composer Environment is under the same
    # project-id as the gcr.io images and the service account that Composer
    # uses has permission to access the Google Container Registry
    # (the default service account has permission)

    # [END composer_kubernetespodoperator_minconfig]
