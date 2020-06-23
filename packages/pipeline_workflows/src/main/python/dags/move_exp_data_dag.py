# import json
# import datetime
# import time

# from airflow import DAG
# from airflow.models import Variable
# from airflow.contrib.kubernetes import secret
# from airflow.contrib.operators import kubernetes_pod_operator
# from airflow.operators.python_operator import PythonOperator
# from move_exp_data_dag_processor import count_utterances_file_chunks, copy_utterances

# secret_file = secret.Secret(
#     deploy_type='volume',
#     deploy_target='/tmp/secrets/google',
#     secret='gc-storage-rw-key',
#     key='key.json')

# dag_id = "Data_Catalog_Orchestration"
# dag_number = dag_id
# default_args = {
#     'email': ['gaurav.gupta@thoughtworks.com']
# }
# YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# dag = DAG(dag_id,
#           schedule_interval=datetime.timedelta(days=1),
#           default_args=default_args,
#           start_date=YESTERDAY)

# with dag:
#     count_utterances_chunks_list = PythonOperator(
#         task_id=dag_id + "_count_utterances_file_chunks",
#         python_callable=count_utterances_file_chunks,
#         op_kwargs={'source': dag_id},
#         dag_number=dag_number)

#     count_utterances_chunks_list

#     utterances_chunks_list = json.loads(Variable.get("utteranceschunkslist"))
#     utterances_batch_count = Variable.get("utterancesconcurrentbatchcount")
#     # print(utterances_chunks_list,type(utterances_chunks_list))

#     for index, utterances_chunk in enumerate(utterances_chunks_list['utteranceschunkslist']):
#         if index > (int(utterances_batch_count)-1):
#             break

#         else:
#             copy_utterance_files = PythonOperator(
#                 task_id=dag_id + "_copy_utterances_" + str(index),
#                 python_callable=copy_utterances,
#                 op_kwargs={'src_file_name': utterances_chunk},
#                 dag_number=dag_number)
#             count_utterances_chunks_list >> copy_utterance_files
