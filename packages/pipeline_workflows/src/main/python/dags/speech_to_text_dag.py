import json
import datetime
import math
import time

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.python_operator import PythonOperator
from speech_to_text_dag_processor import get_audio_ids, get_files_from_landing_zone, move_raw_to_processed
from constants import DEFAULT_TRANSLATION_API

sourceinfo = json.loads(Variable.get("sourceinfo"))
tobe_processed_path = Variable.get("tobeprocessedpath")
processed_path = Variable.get("rawprocessedpath")
bucket_name = Variable.get("bucket")
stt_config_path = Variable.get("sttconfigpath")
composer_namespace = Variable.get("composer_namespace")
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

secret_file = secret.Secret(
    deploy_type='volume',
    deploy_target='/tmp/secrets/google',
    secret='gc-storage-rw-key',
    key='key.json')


def create_dag(dag_id,
               dag_number,
               default_args,
               args,
               batch_count):
    dag = DAG(dag_id,
              schedule_interval=datetime.timedelta(days=1),
              default_args=default_args,
              start_date=YESTERDAY)

    with dag:
        copy_files_in_buckets = PythonOperator(
            task_id=dag_id + "_copy_landing_tobeprocessed",
            python_callable=get_files_from_landing_zone,
            op_kwargs={'source': dag_id},
            dag_number=dag_number)

        fetch_audio_ids = PythonOperator(
            task_id=dag_id + "_fetch_audio_ids",
            python_callable=get_audio_ids,
            op_kwargs={'source': dag_id},
            dag_number=dag_number)

        copy_files_in_buckets >> fetch_audio_ids

        parallelism = json.loads(Variable.get("parallelism"))

        audio_file_ids = json.loads(Variable.get("audiofileids"))[dag_id]
        chunk_size = math.ceil(len(audio_file_ids) / parallelism)
        batches = [audio_file_ids[i:i + chunk_size] for i in range(0, len(audio_file_ids), chunk_size)]

        if len(audio_file_ids) > 0:
            data_prep_cataloguer = kubernetes_pod_operator.KubernetesPodOperator(
                task_id='data-prep-cataloguer',
                name='data-prep-cataloguer',
                cmds=["python", "-m", "src.scripts.db_normalizer", "cluster", "ekstepspeechrecognition-dev",
                      "data/audiotospeech/config/datacataloguer-prep/config.yaml"],
                # namespace='composer-1-10-4-airflow-1-10-6-3b791e93',
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image='us.gcr.io/ekstepspeechrecognition/data_prep_cataloguer:1.0.0',
                image_pull_policy='Always')

        for batch_audio_file_ids in batches:
            data_prep_task = kubernetes_pod_operator.KubernetesPodOperator(
                task_id=dag_id + "_data_prep_" + batch_audio_file_ids[0],
                name='data-prep-stt',
                cmds=["python", "-m", "src.scripts.pipeline_v2", "cluster", bucket_name, stt_config_path, dag_id,
                      batch_audio_file_ids, args.get('file_format'), args.get('translation_source')],
                # namespace='composer-1-10-4-airflow-1-10-6-3b791e93',
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image='us.gcr.io/ekstepspeechrecognition/dataprocessor:1.0.0',
                image_pull_policy='Always')

            move_to_processed = PythonOperator(
                task_id=dag_id + "_move_raw_to_processed_" + batch_audio_file_ids[0],
                python_callable=move_raw_to_processed,
                op_kwargs={'source': dag_id, 'batch_audio_file_ids': batch_audio_file_ids},
                dag_number=dag_number)

            fetch_audio_ids >> data_prep_task >> move_to_processed >> data_prep_cataloguer

    return dag


for source in sourceinfo.keys():
    source_info = sourceinfo.get(source)

    batch_count = source_info.get('count')
    parallelism = source_info.get('parallelism', batch_count)
    file_format = source_info.get('format')
    translation_source = source_info.get('api', DEFAULT_TRANSLATION_API)

    dag_id = source

    dag_args = {
        'email': ['gaurav.gupta@thoughtworks.com'],
    }

    args = {
        'file_format': file_format,
        'translation_source': translation_source,
        'parallelism': parallelism
    }

    # schedule = '@daily'

    dag_number = dag_id + str(batch_count)

    globals()[dag_id] = create_dag(dag_id,
                                   dag_number,
                                   dag_args,
                                   args,
                                   batch_count)
