import os
import sys
# import json
from datetime import datetime
# from airflow.decorators import dag, task
# from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import DAG, BaseOperator


def setup_django_for_airflow():
    # Add Django project root to path
    # sys.path.append('/home/ubuntu/roamapp/roamsloth')
    sys.path.append('/home/ubuntu/accutuning-roadmap.wagtail/roadmap')
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "roadmap.settings.dev")

    import django
    django.setup()


class DjangoOperator(BaseOperator):

    def pre_execute(self, *args, **kwargs):
        setup_django_for_airflow()


class DjangoExampleOperator(DjangoOperator):

    def execute(self, context):
        from django.core import management
        management.call_command('accujira')


dag = DAG(
    dag_id='jira-team-slack',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1),
    catchup=False, tags=['accuinsight', 'jira']
)

start = DummyOperator(task_id='start')
accujira = DjangoExampleOperator(task_id='hello_accujira', dag=dag)
end = DummyOperator(task_id='end')
start >> accujira >> end
