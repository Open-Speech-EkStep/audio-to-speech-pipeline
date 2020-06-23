# [START composer_kubernetespodoperator]
import datetime
import json
import time
from airflow import models
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.python_operator import PythonOperator
from move_exp_data_dag_processor import count_utterances_file_chunks, copy_utterances


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
dag_id='data_tagger_pipeline'
dag_number = dag_id
# If a Pod fails to launch, or has an error occur in the container, Airflow
# will show the task as failed, as well as contain all of the task logs
# required to debug.
# dag = DAG(dag_id,
#           schedule_interval=datetime.timedelta(days=1),
#           default_args=default_args,
#           start_date=YESTERDAY)

dag =  models.DAG(
        dag_id,
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY)
    # Only name, namespace, image, and task_id are required to create a
    # KubernetesPodOperator. In Cloud Composer, currently the operator defaults
    # to using the config file found at `/home/airflow/composer_kube_config if
    # no `config_file` parameter is specified. By default it will contain the
    # credentials for Cloud Composer's Google Kubernetes Engine cluster that is
    # created upon environment creation.

    # [START composer_kubernetespodoperator_minconfig]
with dag:
    kubernetes_list_bucket_pod = kubernetes_pod_operator.KubernetesPodOperator(
        # The ID specified for the task.
        task_id='data-tagger',
        # Name of task you want to run, used to generate Pod ID.    
        name='data-tagger',
        # Entrypoint of the container, if not specified the Docker container's
        # entrypoint is used. The cmds parameter is templated.
        # cmds=['echo'],
        cmds=["python", "-m", "src.scripts.data_tagger", "cluster", "ekstepspeechrecognition-dev", "data/audiotospeech/config/datatagger/config.yaml"],
        # The namespace to run within Kubernetes, default namespace is
        # `default`. There is the potential for the resource starvation of
        # Airflow workers and scheduler within the Cloud Composer environment,
        # the recommended solution is to increase the amount of nodes in order
        # to satisfy the computing requirements. Alternatively, launching pods
        # into a custom namespace will stop fighting over resources.
        namespace='composer-1-10-4-airflow-1-10-6-3b791e93',
        startup_timeout_seconds=300,
        secrets=[secret_file],
        image='us.gcr.io/ekstepspeechrecognition/data_tagger:1.0.0',
        image_pull_policy='Always')


    count_utterances_chunks_list = PythonOperator(
        task_id=dag_id + "_count_utterances_file_chunks",
        python_callable=count_utterances_file_chunks,
        op_kwargs={'source': dag_id},
        dag_number=dag_number)

    kubernetes_list_bucket_pod >> count_utterances_chunks_list

    utterances_chunks_list = json.loads(Variable.get("utteranceschunkslist"))
    utterances_batch_count = Variable.get("utterancesconcurrentbatchcount")
    # print(utterances_chunks_list,type(utterances_chunks_list))

    for index, utterances_chunk in enumerate(utterances_chunks_list['utteranceschunkslist']):
        if index > (int(utterances_batch_count)-1):
            break

        else:
            copy_utterance_files = PythonOperator(
                task_id=dag_id + "_copy_utterances_" + str(index),
                python_callable=copy_utterances,
                op_kwargs={'src_file_name': utterances_chunk},
                dag_number=dag_number)
            count_utterances_chunks_list >> copy_utterance_files
    
    # config_file="{{ conf.get('core', 'kube_config') }}")
    # Docker image specified. Defaults to hub.docker.com, but any fully
    # qualified URLs will point to a custom repository. Supports private
    # gcr.io images if the Composer Environment is under the same
    # project-id as the gcr.io images and the service account that Composer
    # uses has permission to access the Google Container Registry
    # (the default service account has permission)

    # [END composer_kubernetespodoperator_minconfig]