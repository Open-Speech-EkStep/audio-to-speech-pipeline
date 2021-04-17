import json
import datetime
import math

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.python_operator import PythonOperator
from helper_dag import generate_splitted_batches_for_audio_analysis

audio_analysis_config = json.loads(Variable.get("audio_analysis_config"))
source_path = Variable.get("sourcepathforaudioanalysis")
destination_path = Variable.get("destinationpathforaudioanalysis")
bucket_name = Variable.get("bucket")
env_name = Variable.get("env")
composer_namespace = Variable.get("composer_namespace")
resource_limits = json.loads(Variable.get("audio_analysis_resource_limits"))

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
LANGUAGE_CONSTANT = "{language}"

secret_file = secret.Secret(
    deploy_type="volume",
    deploy_target="/tmp/secrets/google",
    secret="gc-storage-rw-key",
    key="key.json",
)


def interpolate_language_paths(path, language):
    path_set = path.replace(LANGUAGE_CONSTANT, language)
    return path_set


def create_dag(dag_id, dag_number, default_args, args, max_records_threshold_per_pod):
    dag = DAG(
        dag_id,
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args,
        start_date=YESTERDAY,
    )

    with dag:
        audio_format = args.get("audio_format")
        language = args.get("language")
        parallelism = args.get("parallelism")
        source = args.get("source")
        print(args)
        print(f"Language for source is {language}")
        source_path_set = interpolate_language_paths(source_path, language)
        destination_path_set = interpolate_language_paths(destination_path, language)
        generate_batch_files = PythonOperator(
            task_id=dag_id + "_generate_batches",
            python_callable=generate_splitted_batches_for_audio_analysis,
            op_kwargs={
                "source": source,
                "source_path": source_path_set,
                "destination_path": destination_path_set,
                "max_records_threshold_per_pod": max_records_threshold_per_pod,
                "audio_format": audio_format,
                "bucket_name": bucket_name,
            },
            dag_number=dag_number,
        )

        generate_batch_files

        data_audio_analysis_task = kubernetes_pod_operator.KubernetesPodOperator(
            task_id=f"data-audio-analysis-{source}",
            name="data-audio-analysis",
            cmds=[
                "python",
                "invocation_script.py",
                "-b",
                bucket_name,
                "-a",
                "audio_analysis",
                "-rc",
                f"data/audiotospeech/config/config.yaml",
                "-as",
                source,
                "-l",
                language,
            ],
            namespace=composer_namespace,
            startup_timeout_seconds=300,
            secrets=[secret_file],
            image=f"us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:{env_name}_1.0.0",
            image_pull_policy="Always",
            resources=resource_limits,
        )

        batch_file_path_dict = json.loads(Variable.get("embedding_batch_file_list"))
        list_of_batch_files = batch_file_path_dict[source]
        if len(list_of_batch_files) > 0:
            total_phases = math.ceil(len(list_of_batch_files) / parallelism)
            task_dict = {}
            for phase in range(0, total_phases):
                for pod in range(1, parallelism + 1):
                    if len(list_of_batch_files) > 0:
                        batch_file = list_of_batch_files.pop()
                        task_dict[
                            f"create_embedding_task_{pod}_{phase}"] = kubernetes_pod_operator.KubernetesPodOperator(
                            task_id=source + "_create_embedding_" + str(pod) + '_' + str(phase),
                            name="create-embedding",
                            cmds=[
                                "python",
                                "invocation_script.py",
                                "-b",
                                bucket_name,
                                "-a",
                                "audio_embedding",
                                "-rc",
                                f"data/audiotospeech/config/config.yaml",
                                "-fp",
                                batch_file,
                                "-as",
                                source,
                                "-l",
                                language,
                            ],
                            namespace=composer_namespace,
                            startup_timeout_seconds=300,
                            secrets=[secret_file],
                            image=f"us.gcr.io/ekstepspeechrecognition/ekstep_data_pipelines:{env_name}_1.0.0",
                            image_pull_policy="Always",
                            resources=resource_limits,
                        )
                        if phase == 0:
                            generate_batch_files >> task_dict[
                                f"create_embedding_task_{pod}_{phase}"] >> data_audio_analysis_task
                        # elif phase == total_phases - 1:
                        #     task_dict[f"create_embedding_task_{pod}_{phase - 1}"] >> task_dict[
                        #         f"create_embedding_task_{pod}_{phase}"] >> data_audio_analysis_task
                        else:
                            task_dict[f"create_embedding_task_{pod}_{phase - 1}"] >> task_dict[
                                f"create_embedding_task_{pod}_{phase}"] >> data_audio_analysis_task
            # if phase == total_phases - 1:
            #     task_dict[f"create_embedding_task_{last_pod_no}_{phase}"] >> data_audio_analysis_task
        else:
            generate_batch_files >> data_audio_analysis_task

    return dag


for source in audio_analysis_config.keys():
    source_info = audio_analysis_config.get(source)

    max_records_threshold_per_pod = source_info.get("batch_size")
    parallelism = source_info.get("parallelism", 5)
    audio_format = source_info.get("format")
    language = source_info.get("language").lower()

    dag_id = source + '_' + 'audio_embedding_analysis'

    dag_args = {
        "email": ["soujyo.sen@thoughtworks.com"],
    }

    args = {
        "audio_format": audio_format,
        "parallelism": parallelism,
        "language": language,
        "source": source
    }

    # schedule = '@daily'

    dag_number = dag_id + str(max_records_threshold_per_pod)

    globals()[dag_id] = create_dag(dag_id, dag_number, dag_args, args, max_records_threshold_per_pod)
