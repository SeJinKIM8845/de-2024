from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'drone_data_analysis',
    default_args=default_args,
    description='A DAG for drone data analysis and anomaly detection',
    schedule_interval=timedelta(days=1),
)

collect_data = BashOperator(
    task_id='download_drone_data',
    bash_command="bash /opt/airflow/jobs/drone-data.sh",
    dag=dag
)

filter_data = BashOperator(
    task_id='filter-data',
    bash_command="bash /opt/airflow/jobs/spark-submit.sh /opt/airflow/jobs/main.py",
    dag=dag
)

collect_data >> filter_data