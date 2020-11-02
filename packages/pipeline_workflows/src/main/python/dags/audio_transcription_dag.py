import json
import datetime
import math

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.python_operator import PythonOperator
from helper_dag import get_require_audio_id

sourceinfo = json.loads(Variable.get("sourceinfo"))
stt_source_path = Variable.get("sttsourcepath")
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")
composer_namespace = Variable.get("composer_namespace")
resource_limits = json.loads(Variable.get("stt_resource_limits"))
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
LANGUAGE_CONSTANT = "{language}"

secret_file = secret.Secret(
    deploy_type='volume',
    deploy_target='/tmp/secrets/google',
    secret='gc-storage-rw-key',
    key='key.json')


def interpolate_language_paths(language):
    stt_source_path_set = stt_source_path.replace(LANGUAGE_CONSTANT, language)
    return stt_source_path_set


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

        language = args.get('language')
        print(args)
        print(f"Language for source is {language}")
        stt_source_path_set = interpolate_language_paths(language)

        fetch_audio_ids = PythonOperator(
            task_id=dag_id + "_fetch_audio_ids",
            python_callable=get_require_audio_id,
            op_kwargs={'source': dag_id,
                       'stt_source_path': stt_source_path_set, "batch_count": batch_count, 'bucket_name': bucket_name},
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
                      f"data/audiotospeech/config/config.yaml",
                      "-ai", ','.join(batch_audio_file_ids), "-as", dag_id, "-stt", stt,"-l",language],
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image=f'us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:{env_name}_1.0.0',
                image_pull_policy='Always',
                resources=resource_limits)

            fetch_audio_ids >> data_prep_task

    return dag


for source in sourceinfo.keys():
    source_info = sourceinfo.get(source)

    batch_count = source_info.get('count')
    parallelism = source_info.get('parallelism', batch_count)
    api = source_info.get('stt')
    language = source_info.get('language').lower()

    dag_id = source

    dag_args = {
        'email': ['gaurav.gupta@thoughtworks.com'],
    }

    args = {
        'parallelism': parallelism,
        'stt': api,
        'language': language

    }

    dag_number = dag_id + str(batch_count)

    globals()[dag_id] = create_dag(dag_id,
                                   dag_number,
                                   dag_args,
                                   args,
                                   batch_count)
