# dags/test_connection_dag.py
from airflow.decorators import dag, task
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dag(
    dag_id='test_connection',
    schedule_interval=None,
    start_date=datetime(2024, 12, 25),
    catchup=False
)
def test_connection():
    @task()
    def test_postgresql():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            result = cur.fetchone()
            cur.close()
            conn.close()
            return "Connexion PostgreSQL réussie!"
        except Exception as e:
            logger.error(f"Erreur de connexion: {str(e)}")
            raise  # Ceci fera échouer la tâche

    test_postgresql()

dag = test_connection()