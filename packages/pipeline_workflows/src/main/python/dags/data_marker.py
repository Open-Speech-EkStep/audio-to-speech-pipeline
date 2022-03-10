import json
import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import TriggerDagRunOperator
from helper_dag import data_marking_start

data_marker_config = json.loads(Variable.get("data_filter_config"))
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")
composer_namespace = Variable.get("composer_namespace")
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
project = Variable.get("project")
sourceinfo = json.loads(Variable.get("sourceinfo"))

secret_file = secret.Secret(
    deploy_type="volume",
    deploy_target="/tmp/secrets/google",
    secret="gc-storage-rw-key",
    key="key.json",
)


def create_dag(data_marker_config, default_args):
    dag = DAG(
        dag_id="data_marker_pipeline",
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY,
    )

    with dag:
        before_start = PythonOperator(
            task_id="data_marking_start",
            python_callable=data_marking_start,
            op_kwargs={},
        )

        before_start

        for source in data_marker_config.keys():

            filter_by_config = data_marker_config.get(source)
            language = filter_by_config.get("language").lower()
            print(f"Language for source is {language}")

            sourceinfo_dict = sourceinfo.get(source)

            api = sourceinfo_dict.get("stt", 'azure')

            next_dag_id = source + '_stt_' + api + '_' + language
            trigger_dependent_dag = TriggerDagRunOperator(
                task_id="trigger_dependent_dag_" + next_dag_id,
                trigger_dag_id=next_dag_id,
            )
            data_marker_task = kubernetes_pod_operator.KubernetesPodOperator(
                task_id=f"data-marker-{source}-{language}",
                name="data-marker",
                cmds=[
                    "python",
                    "invocation_script.py",
                    "-b",
                    bucket_name,
                    "-a",
                    "data_marking",
                    "-rc",
                    "data/audiotospeech/config/config.yaml",
                    "-as",
                    source,
                    "-fs",
                    json.dumps(filter_by_config),
                    "-l",
                    language,
                ],
                namespace=composer_namespace,
                startup_timeout_seconds=300,
                secrets=[secret_file],
                image=f"us.gcr.io/{project}/ekstep_data_pipelines:{env_name}_1.0.0",
                image_pull_policy="Always",
            )

            before_start >> data_marker_task >> trigger_dependent_dag

    return dag


dag_args = {
    "email": ["gaurav.gupta@thoughtworks.com"],
}

globals()["data_marker_pipeline"] = create_dag(data_marker_config, dag_args)
