import json
import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.python_operator import PythonOperator
from helper_dag import audio_analysis_start

audio_analysis_config = json.loads(Variable.get("audio_analysis_config"))
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")
composer_namespace = Variable.get("composer_namespace")
resource_limits = json.loads(Variable.get("audio_analysis_resource_limits"))
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

secret_file = secret.Secret(
    deploy_type='volume',
    deploy_target='/tmp/secrets/google',
    secret='gc-storage-rw-key',
    key='key.json')


def create_dag(data_marker_config, default_args):
    dag = DAG(dag_id='audio-analysis-pipeline',
              schedule_interval=datetime.timedelta(days=1),
              default_args=default_args,
              start_date=YESTERDAY)

    with dag:
        before_start = PythonOperator(
            task_id="audio_analysis_start",
            python_callable=audio_analysis_start,
            op_kwargs={},
        )

        before_start

        for source in audio_analysis_config.keys():
            source_config = audio_analysis_config.get(source)
            language = source_config.get('language').lower()
            print(f"Language for source is {language}")
            data_marker_task = kubernetes_pod_operator.KubernetesPodOperator(
                task_id=f'data-audio-analysis-{source}',
                name='data-audio-analysis',
                cmds=["python", "invocation_script.py", "-b", bucket_name, "-a", "audio_analysis", "-rc",
                      f"data/audiotospeech/config/audio_processing/config_{language}.yaml",
                          "-as", source],
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image=f'us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:{env_name}_1.0.0',
                image_pull_policy='Always',
                resources=resource_limits
            )

            before_start >> data_marker_task

    return dag


dag_args = {
    'email': ['srajat@thoughtworks.com'],
}

globals()['audio_analysis_start'] = create_dag(audio_analysis_config, dag_args)
