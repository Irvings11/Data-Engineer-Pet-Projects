from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/scripts')  # путь к скриптам
from extract_countries import extract_countries  # импорт функции

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=10)
}

with DAG(
    dag_id='rest_countries_etl',
    default_args=default_args,
    start_date=datetime(2025, 12, 30),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_countries',
        python_callable=extract_countries
    )
