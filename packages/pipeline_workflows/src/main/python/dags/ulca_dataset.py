import json
import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator


ulca_dataset_config = json.loads(Variable.get("ulca_dataset_config"))
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")
composer_namespace = Variable.get("composer_namespace")
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

secret_file = secret.Secret(
    deploy_type="volume",
    deploy_target="/tmp/secrets/google",
    secret="gc-storage-rw-key",
    key="key.json",
)


def create_dag(ulca_dataset_config, default_args):
    dag = DAG(
        dag_id="ulca_dataset_pipeline",
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY,
    )

    with dag:

        for source in ulca_dataset_config.keys():
            source_config = ulca_dataset_config.get(source)
            language = source_config.get("language").lower()
            print(f"Language for source is {language}")
            ulca_dataset_task = kubernetes_pod_operator.KubernetesPodOperator(
                task_id=f"ulca-dataset-{source}",
                name="data-dataset",
                cmds=[
                    "python",
                    "invocation_script.py",
                    "-b",
                    bucket_name,
                    "-a",
                    "ulca_dataset",
                    "-rc",
                    "data/audiotospeech/config/config.yaml",
                    "-as",
                    source,
                    "-ulca_config",
                    json.dumps(source_config),
                    "-l",
                    language,
                ],
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image=f"us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:{env_name}_1.0.0",
                image_pull_policy="Always",
                )

        ulca_dataset_task

    return dag


dag_args = {
    "email": ["rajat.singhal@thoughtworks.com"],
}

globals()["ulca_dataset_pipeline"] = create_dag(ulca_dataset_config, dag_args)
