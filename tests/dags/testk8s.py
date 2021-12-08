from datetime import datetime, timedelta

from kubernetes.client import models as k8s
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod import Resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

dag_id = 'kubernetes-dag'

task_default_args = {
    'owner': 'bomwo',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 11, 21),
    'depends_on_past': False,
    'email': ['bomwo25@mgmail.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    'execution_timeout': timedelta(hours=1)
}

dag = DAG(
    dag_id=dag_id,
    description='kubernetes pod operator',
    default_args=task_default_args,
    schedule_interval='5 16 * * *',
    max_active_runs=1
)

env = Secret(
    'env',
    'TEST',
    'test_env',
    'TEST',
)

pod_resources = Resources()
pod_resources.request_cpu = '1000m'
pod_resources.request_memory = '2048Mi'
pod_resources.limit_cpu = '2000m'
pod_resources.limit_memory = '4096Mi'


configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='secret')),
]

start = DummyOperator(task_id="start", dag=dag)

run = KubernetesPodOperator(
    task_id="kubernetespodoperator",
    namespace='airflow',
    image='hello-world:latest',
    # secrets=[
    #     env
    # ],
    image_pull_secrets=[k8s.V1LocalObjectReference('image_credential')],
    name="job",
    is_delete_operator_pod=True,
    get_logs=True,
    resources=pod_resources,
    # env_from=configmaps,
    dag=dag,
)

write_xcom = KubernetesPodOperator(
    namespace='airflow',
    image='alpine',
    cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '{{ dag_run.conf.get('message', '[1,2,3,4]') if dag_run else '[1,2,3,4]' }}' > /airflow/xcom/return.json"],
    name="write-xcom",
    do_xcom_push=True,
    is_delete_operator_pod=True,
    in_cluster=True,
    task_id="write-xcom",
    get_logs=True,
    dag=dag,
)

pod_task_xcom_result = BashOperator(
    bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"",
    task_id="pod_task_xcom_result",
    dag=dag,
)

start >> run
start >> write_xcom >> pod_task_xcom_result
