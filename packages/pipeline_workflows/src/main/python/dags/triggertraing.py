import datetime
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import BashOperator


trigger_training_config = json.loads(Variable.get("trigger_training_config"))
command = trigger_training_config.get("command")
zone = trigger_training_config.get("zone")
project = trigger_training_config.get("project")
vm_name = trigger_training_config.get("vm_name")

#from airflow.utils import trigger_rule
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': Variable.get('gcp_project')
}
bash_cmd=f'gcloud compute ssh --zone {zone} {vm_name}  --project {project} --command "{command}"'

with DAG(dag_id="trigger_training", schedule_interval='@daily', start_date=yesterday, default_args=default_dag_args, catchup=False) as dag:
     start = DummyOperator(task_id='start')
    
     end = DummyOperator(task_id='end')
         
     bash_remote_gcp_machine = BashOperator(task_id='bash_remote_gcp_machine_task',bash_command=bash_cmd)
	
start >> bash_remote_gcp_machine >> end