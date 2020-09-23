import json
import datetime
import math
import time

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.python_operator import PythonOperator
from helper_dag import get_audio_ids, get_files_from_landing_zone, move_raw_to_processed, get_require_audio_id

sourceinfo = json.loads(Variable.get("sourceinfo"))
source_path_for_snr = Variable.get("sourcepathforsnr")
stt_source_path = Variable.get("sttsourcepath")
snr_done_path = Variable.get("snrdonepath")
bucket_name = Variable.get("bucket")

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
    dag = DAG(f'{dag_id}_stt',
              schedule_interval=datetime.timedelta(days=1),
              default_args=default_args,
              start_date=YESTERDAY)

    with dag:

        fetch_audio_ids = PythonOperator(
            task_id=dag_id + "_fetch_audio_ids",
            python_callable=get_require_audio_id,
            op_kwargs={'source': dag_id,
                       'stt_source_path': stt_source_path, "batch_count": batch_count},
            dag_number=dag_number)

        fetch_audio_ids

        parallelism = args.get("parallelism")
        stt = args.get("stt")

        audio_file_ids = json.loads(Variable.get("audioidsforstt"))[dag_id]

        batches = []

        if len(audio_file_ids) > 0:
            chunk_size = math.ceil(len(audio_file_ids) / parallelism)
            batches = [audio_file_ids[i:i + chunk_size]
                       for i in range(0, len(audio_file_ids), chunk_size)]

        for batch_audio_file_ids in batches:
            data_prep_task = kubernetes_pod_operator.KubernetesPodOperator(
                task_id=dag_id + "_data_stt_" + batch_audio_file_ids[0],
                name='data-prep-stt',
                cmds=["python", "invocation_script.py", "-b", bucket_name, "-a", "audio_transcription", "-rc",
                      "data/audiotospeech/config/audio_processing/config.yaml",
                      "-ai", ','.join(batch_audio_file_ids), "-as", dag_id, "-stt", stt],
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image='us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:1.0.0',
                image_pull_policy='Always')

            fetch_audio_ids >> data_prep_task

    return dag


for source in sourceinfo.keys():
    source_info = sourceinfo.get(source)

    batch_count = source_info.get('count')
    parallelism = source_info.get('parallelism', batch_count)
    api = source_info.get('stt')

    dag_id = source

    dag_args = {
        'email': ['gaurav.gupta@thoughtworks.com'],
    }

    args = {
        'parallelism': parallelism,
        'stt': api
    }

    # schedule = '@daily'

    dag_number = dag_id + str(batch_count)

    globals()[dag_id] = create_dag(dag_id,
                                   dag_number,
                                   dag_args,
                                   args,
                                   batch_count)
