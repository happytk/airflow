import json
from datetime import datetime

from airflow.decorators import dag, task
@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['example'])
def test_docker():
    @task.docker(image='smizy/scikit-learn', multiple_outputs=True)
    def load(order_data_dict: dict):
        from sklearn.datasets import load_iris
        data = load_iris()
        return data.target_names
    load()
testd = test_docker()