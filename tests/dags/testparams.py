from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'start_date': datetime.utcnow(),
    'owner': 'airflow',
}

dag = DAG(
    dag_id='example_dag_conf',
    default_args=args,
    schedule_interval=None,
)

def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag,
)

# You can also access the DagRun object in templates
bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Here is the message: '
                 '{{ dag_run.conf["message"] if dag_run else "" }}" ',
    dag=dag,
)

start = DummyOperator(task_id="start", dag=dag)

start >> run_this >> bash_task