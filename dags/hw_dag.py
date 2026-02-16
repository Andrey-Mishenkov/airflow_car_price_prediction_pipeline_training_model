import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator

# path = os.path.expanduser('~/airflow_hw')
path = "/opt/airflow/plugins"

# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path

# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.pipeline import pipeline
from modules.predict import predict

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        # schedule_interval="00 15 * * *",
        schedule="00 15 * * *",
        default_args=args,
) as dag:

    # служебный оператор
    start_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Task 33 start!"',
        dag=dag,
    )

    # запись модели
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
        dag=dag
    )

    # предсказание
    predict = PythonOperator(
        task_id='predict',
        python_callable=predict,
        dag=dag
    )

    # Порядок выполнения задач
    start_task >> pipeline >> predict