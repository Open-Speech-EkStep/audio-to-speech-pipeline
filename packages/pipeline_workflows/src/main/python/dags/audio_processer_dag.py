import json
import datetime
import math

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.python_operator import PythonOperator
from helper_dag import get_file_path_from_bucket

snr_catalogue_source = json.loads(Variable.get("snrcatalogue"))
source_path_for_snr = Variable.get("sourcepathforsnr")
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")
composer_namespace = Variable.get("composer_namespace")
resource_limits = json.loads(Variable.get("snr_resource_limits"))
meta_file_extension = Variable.get("metafileextension")

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
LANGUAGE_CONSTANT = "{language}"

secret_file = secret.Secret(
    deploy_type='volume',
    deploy_target='/tmp/secrets/google',
    secret='gc-storage-rw-key',
    key='key.json')


def interpolate_language_paths(language):
    source_path_for_snr_set = source_path_for_snr.replace(LANGUAGE_CONSTANT, language)
    return source_path_for_snr_set


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

        audio_format = args.get('audio_format')
        language = args.get('language')
        print(args)
        print(f"Language for source is {language}")
        source_path_for_snr_set = interpolate_language_paths(
            language)

        get_file_path_from_gcp_bucket = PythonOperator(
            task_id=dag_id + "_get_file_path",
            python_callable=get_file_path_from_bucket,
            op_kwargs={'source': dag_id, 'source_landing_path': source_path_for_snr_set,'batch_count': batch_count ,'audio_format': audio_format, 'meta_file_extension': meta_file_extension, 'bucket_name': bucket_name},
            dag_number=dag_number)


        get_file_path_from_gcp_bucket

        parallelism = args.get("parallelism")

        file_path_list = json.loads(Variable.get("audiofilelist"))[dag_id]


        if len(file_path_list) > 0:

            chunk_size = math.ceil(len(file_path_list) / parallelism)
            batches = [file_path_list[i:i + chunk_size] for i in range(0, len(file_path_list), chunk_size)]
            data_prep_cataloguer = kubernetes_pod_operator.KubernetesPodOperator(
                task_id='data-catalogure',
                name='data-catalogure',
                cmds=["python", "invocation_script.py", "-b",bucket_name, "-a", "audio_cataloguer", "-rc",f"data/audiotospeech/config/config.yaml"],
                namespace = composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image=f'us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:{env_name}_1.0.0',
                image_pull_policy='Always',
                resources=resource_limits)

        else:
            batches = []

        for batch_file_path_list in batches:
            data_prep_task = kubernetes_pod_operator.KubernetesPodOperator(
                task_id=dag_id + "_data_snr_" + batch_file_path_list[0],
                name='data-prep-snr',
                cmds=["python", "invocation_script.py", "-b", bucket_name, "-a", "audio_processing", "-rc",
                      f"data/audiotospeech/config/config.yaml",
                      "-fl", ','.join(batch_file_path_list), "-af", args.get('audio_format'), "-as", dag_id,"-l",language],
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image=f'us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:{env_name}_1.0.0',
                image_pull_policy='Always',
                resources=resource_limits)

            get_file_path_from_gcp_bucket >> data_prep_task >> data_prep_cataloguer

    return dag


for source in snr_catalogue_source.keys():
    source_info = snr_catalogue_source.get(source)

    batch_count = source_info.get('count')
    parallelism = source_info.get('parallelism', batch_count)
    audio_format = source_info.get('format')
    language = source_info.get('language').lower()

    dag_id = source

    dag_args = {
        'email': ['gaurav.gupta@thoughtworks.com'],
    }

    args = {
        'audio_format': audio_format,
        'parallelism': parallelism,
        'language': language
    }

    # schedule = '@daily'

    dag_number = dag_id + str(batch_count)

    globals()[dag_id] = create_dag(dag_id,
                                   dag_number,
                                   dag_args,
                                   args,
                                   batch_count)
