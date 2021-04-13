import datetime

from airflow import DAG
from airflow.contrib.kubernetes import secret
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from data_validation_report_processor import report_generation_pipeline

secret_file = secret.Secret(
    deploy_type="volume",
    deploy_target="/tmp/secrets/google",
    secret="gc-storage-rw-key",
    key="key.json",
)

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")


def create_dag(dag_id, stage, default_args):
    dag = DAG(
        dag_id + "_" + stage,
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY,
    )
    with dag:
        report_generation_pipeline_task = PythonOperator(
            task_id=dag_id + stage + "_report_generation_and_upload",
            python_callable=report_generation_pipeline,
            op_kwargs={"stage": stage, "bucket": bucket_name},
            dag_number=1234,
        )

        report_generation_pipeline_task
    return dag


for source in range(0, 1):
    dag_id = "report_generation_pipeline"

    dag_args = {
        "email": ["soujyo.sen@thoughtworks.com"],
    }

    # schedule = '@daily'

    # dag_number = dag_id + str(batch_count)

    globals()[dag_id + "pre-transcription"] = create_dag(
        dag_id, "pre-transcription", default_args=dag_args
    )
    globals()[dag_id + "post-transcription"] = create_dag(
        dag_id, "post-transcription", default_args=dag_args
    )
