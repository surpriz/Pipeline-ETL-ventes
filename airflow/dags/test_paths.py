# dags/test_paths.py
from airflow.decorators import dag, task
from datetime import datetime
import os

@dag(
    dag_id='test_paths',
    schedule_interval=None,
    start_date=datetime(2024, 12, 25),
    catchup=False
)
def test_paths():
    @task()
    def print_paths():
        print("Current working directory:", os.getcwd())
        print("Directory contents:", os.listdir())
        print("Python path:", sys.path)
        return "Paths printed"

dag = test_paths()