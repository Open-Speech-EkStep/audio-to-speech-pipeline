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

secret_file = secret.Secret(
    deploy_type='volume',
    deploy_target='/tmp/secrets/google',
    secret='gc-storage-rw-key',
    key='key.json')
dag_id='data_tagger_pipeline'
dag_number = dag_id

dag =  models.DAG(
        dag_id,
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY)
with dag:
    kubernetes_list_bucket_pod = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='data-tagger',
        name='data-tagger',
        cmds=["python", "-m", "src.scripts.data_tagger", "cluster", "ekstepspeechrecognition-dev", "data/audiotospeech/config/datatagger/config.yaml"],
       
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
    
