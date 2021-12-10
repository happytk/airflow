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

volume = k8s.V1Volume(
    name='workspace-3-volume',
    # persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='workspace-volume-3-claim'),
    host_path=k8s.V1HostPathVolumeSource(path='/workspace'),
)

volume_mounts = [
    k8s.V1VolumeMount(
        mount_path='/workspace', name='workspace-3-volume', sub_path=None,
        read_only=False
    )
]


# docker pull harbor.accuinsight.net/accutuning/accutuning/modeler-common@sha256:8366e7d63ad086972790ae25fc37f48b4e8ed5bfb55479c0a7e3d7f6bde3cef1
run = KubernetesPodOperator(
    task_id="kubernetespodoperator",
    namespace='airflow',
    image='harbor.accuinsight.net/accutuning/accutuning/modeler-common@sha256:8366e7d63ad086972790ae25fc37f48b4e8ed5bfb55479c0a7e3d7f6bde3cef1',
    volumes=[volume],
    volume_mounts=volume_mounts,
    # image='hello-world:latest',
    # secrets=[
    #     env
    # ],
    image_pull_secrets=[k8s.V1LocalObjectReference('image_credential')],
    name="job",
    is_delete_operator_pod=True,
    get_logs=True,
    resources=pod_resources,
    # env_from=configmaps,
    env_vars={'ACCUTUNING_WORKSPACE': '/workspace'},
    dag=dag,
)

write_xcom = KubernetesPodOperator(
    namespace='airflow',
    image='alpine',
    # cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '{{ dag_run.conf.get('message', '[1,2,3,4]') if dag_run else '[1,2,3,4]' }}' > /airflow/xcom/return.json"],
    cmds=["sh", "-c", "mkdir -p /airflow/xcom/;ls -al /workspace/* > /airflow/xcom/return.json"],
    name="write-xcom",
    do_xcom_push=True,
    volumes=[volume],
    volume_mounts=volume_mounts,
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
